/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use std::{
    borrow::Borrow,
    io::{self, Cursor},
    net::{Ipv4Addr, UdpSocket},
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use cameleon_device::gige::protocol::stream::{
    ImageLeader, ImageTrailer, PacketHeader, PacketType, PayloadType, PayloadTypeKind,
};
use futures_channel::oneshot;
use tracing::{error, warn};

use crate::{
    payload::{Payload, PayloadSender},
    DeviceControl, PayloadStream, StreamError, StreamResult,
};

/// How long a receive waits before the streaming loop comes back to check whether it has been
/// asked to stop. A camera that delivers nothing — a packet size too large for a hop on the
/// path, a link that went down — would otherwise leave the receive blocked with no arrival to
/// wake it, and the stop waiting on a loop that never looks.
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Keeps a socket that fails every receive from spinning the loop.
const RECV_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Trait for a generic stream UDP socket
pub trait StreamUdpSocket: Send + Sync + 'static {
    /// Receives a single datagram message. Same as UdpSocket::recv.
    fn recv(&self, buf: &mut [u8]) -> io::Result<usize>;

    /// Bounds how long [`Self::recv`] blocks, after which it fails with [`io::ErrorKind::WouldBlock`]
    /// or [`io::ErrorKind::TimedOut`] depending on the platform.
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;

    /// Returns the port number associated with this socket.
    fn port(&self) -> u16;
}

impl StreamUdpSocket for UdpSocket {
    fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.recv(buf)
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.set_read_timeout(timeout)
    }

    fn port(&self) -> u16 {
        self.local_addr().unwrap().port()
    }
}

fn is_recv_timeout(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

#[derive(Debug)]
/// Host-side parameters needed to configure GigE streaming.
pub struct StreamParams {
    /// Host IPv4 address used as stream destination.
    pub host_addr: Ipv4Addr,
    /// Host UDP port used as stream destination.
    pub host_port: u16,
}

/// Stream channel handle for receiving GigE GVSP payloads.
pub struct StreamHandle<S: StreamUdpSocket = UdpSocket> {
    completion: Option<Arc<(Mutex<bool>, Condvar)>>,
    cancellation_tx: Option<oneshot::Sender<()>>,
    sock: Arc<S>,
}

impl<S: StreamUdpSocket> StreamHandle<S> {
    /// Creates a stream handle from a bound UDP socket.
    ///
    /// # Errors
    ///
    /// Fails when the socket refuses the receive timeout the streaming loop needs to stay
    /// interruptible.
    pub fn new(sock: S) -> StreamResult<Self> {
        sock.set_read_timeout(Some(RECV_TIMEOUT))
            .map_err(StreamError::from)?;

        Ok(Self {
            completion: None,
            cancellation_tx: None,
            sock: Arc::new(sock),
        })
    }

    /// Returns the local UDP port of the stream socket.
    pub fn port(&self) -> u16 {
        self.sock.port()
    }

    fn spawn_streaming_loop(&mut self, sender: PayloadSender) -> StreamResult<()> {
        if self.is_loop_running() {
            return Err(StreamError::InStreaming);
        }
        let (cancellation_tx, cancellation_rx) = oneshot::channel();
        self.cancellation_tx = Some(cancellation_tx);
        let completion = Arc::new((Mutex::new(false), Condvar::new()));
        self.completion = Some(completion.clone());

        let strm_loop = StreamingLoop {
            // 65536 is the max UDP packet size
            // but the actual packet size is available
            // only after control handle is created
            // so we go the safe way here
            buffer: vec![0u8; 65536],
            cancellation_rx,
            completion,
            sock: self.sock.clone(),
            sender,
        };

        thread::spawn(move || {
            strm_loop.run();
        });
        Ok(())
    }
}

impl<S: StreamUdpSocket> PayloadStream for StreamHandle<S> {
    fn open(&mut self) -> StreamResult<()> {
        // TODO:
        Ok(())
    }

    fn close(&mut self) -> StreamResult<()> {
        // TODO:
        Ok(())
    }

    fn start_streaming_loop(
        &mut self,
        sender: PayloadSender,
        _ctrl: &mut dyn DeviceControl,
    ) -> StreamResult<()> {
        self.spawn_streaming_loop(sender)
    }

    fn stop_streaming_loop(&mut self) -> StreamResult<()> {
        if let Some(cancel) = self.cancellation_tx.take() {
            if cancel.send(()).is_err() {
                return Err(StreamError::Disconnected);
            }
            match self.completion.take().as_ref().map(Borrow::borrow) {
                Some((completion, condvar)) => {
                    let guard = completion.lock().unwrap();
                    if *guard {
                        return Ok(());
                    }
                    drop(condvar.wait_while(guard, |g| !*g).unwrap());
                    Ok(())
                }
                None => Err(StreamError::Disconnected),
            }
        } else {
            Ok(())
        }
    }

    fn is_loop_running(&self) -> bool {
        if let Some((completed, _)) = self.completion.as_ref().map(|v| v.as_ref()) {
            if !*completed.lock().unwrap() {
                return true;
            }
        }
        false
    }
}

struct StreamingLoop<S: StreamUdpSocket> {
    buffer: Vec<u8>,
    cancellation_rx: oneshot::Receiver<()>,
    completion: Arc<(Mutex<bool>, Condvar)>,
    sock: Arc<S>,
    sender: PayloadSender,
}

impl<S: StreamUdpSocket> StreamingLoop<S> {
    /// Whether the loop has been asked to stop, which a dropped sender also means.
    fn is_cancelled(&mut self) -> bool {
        !matches!(self.cancellation_rx.try_recv(), Ok(None))
    }

    /// Releases whoever waits on the loop having stopped.
    fn signal_completion(&self) {
        *self.completion.0.lock().unwrap() = true;
        self.completion.1.notify_all();
    }

    fn run(mut self) {
        macro_rules! unwrap_or_continue {
            ($expr:expr) => {
                match $expr {
                    Err(err) => {
                        use tracing::error;
                        error!(?err);
                        continue;
                    }
                    Ok(v) => v,
                }
            };
        }
        macro_rules! ensure_or_continue {
            ($expr:expr, $($tt:tt)*) => {
                if !($expr) {
                    use tracing::error;
                    error!($($tt)*);
                    continue;
                }
            };
        }
        let mut payload = Vec::new();
        let mut builder = None;

        macro_rules! handle_packet_mismatch {
            ($expr:expr) => {
                match $expr {
                    Ok(p) => p,
                    // if we get an old packet,
                    // we just ignore it and keep building
                    // the new frame
                    Err(PacketMismatch::TooOld) => continue,
                    Err(PacketMismatch::TooNew) => {
                        tracing::warn!("Packet loss occured, frame skipped");
                        builder = None;
                        continue;
                    }
                }
            };
        }

        loop {
            if self.is_cancelled() {
                break;
            }

            let length = match self.sock.recv(&mut self.buffer) {
                Ok(length) => length,
                Err(err) if is_recv_timeout(&err) => continue,
                Err(err) => {
                    error!(?err);
                    thread::sleep(RECV_ERROR_BACKOFF);
                    continue;
                }
            };
            let mut cursor = Cursor::new(&self.buffer[..]);
            let header = unwrap_or_continue!(PacketHeader::parse(&mut cursor));
            match header.packet_type {
                PacketType::Leader => {
                    let payload_type =
                        unwrap_or_continue!(PayloadType::parse_generic_leader(&mut cursor));
                    ensure_or_continue!(
                        payload_type.kind() == PayloadTypeKind::Image,
                        "Payload type kind: {:?} not suported",
                        payload_type.kind()
                    );
                    let leader = unwrap_or_continue!(ImageLeader::parse(&mut cursor));
                    if builder.is_some() {
                        warn!("A new leader packet has arrived while no trailer packet arrived");
                    }
                    builder = Some(PayloadBuilder::new(header, leader, &mut payload));
                }
                PacketType::Trailer => {
                    let payload_type =
                        unwrap_or_continue!(PayloadType::parse_generic_leader(&mut cursor));
                    let Some(builder) = builder.take() else {
                        warn!("Trailer packet received while no leader packet arrived");
                        continue;
                    };
                    ensure_or_continue!(
                        payload_type.kind() == PayloadTypeKind::Image,
                        "Payload type kind: {:?} not suported",
                        payload_type.kind()
                    );
                    let _trailer = unwrap_or_continue!(ImageTrailer::parse(&mut cursor));
                    let payload = handle_packet_mismatch!(builder.build(header));
                    unwrap_or_continue!(async_std::task::block_on(self.sender.send(Ok(payload))));
                }
                PacketType::GenericPayload => {
                    let Some(builder) = builder.as_mut() else {
                        warn!("Generic packet received while no leader packet arrived");
                        continue;
                    };
                    // TODO: migrate to Cursor::remaining_slice when it's stable
                    let remaining_slice = &self.buffer[cursor.position() as usize..length];
                    handle_packet_mismatch!(builder.push(header, remaining_slice));
                }
                PacketType::H264Payload => error!("H264 Payload not implemented"),
                PacketType::MultiZonePayload => error!("Multi Zone Payload not implemented"),
            };
        }

        self.signal_completion();
    }
}

struct PayloadBuilder<'a> {
    payload: &'a mut Vec<u8>,
    leader: ImageLeader,
    pub block_id: u64,
    last_packet_id: u32,
}

impl<'a> PayloadBuilder<'a> {
    fn new(header: PacketHeader, leader: ImageLeader, payload: &'a mut Vec<u8>) -> Self {
        payload.clear();
        Self {
            payload,
            leader,
            block_id: header.block_id,
            last_packet_id: 0,
        }
    }

    fn verify_header(&self, header: &PacketHeader) -> Result<(), PacketMismatch> {
        if header.block_id > self.block_id || header.packet_id > self.last_packet_id + 1 {
            // New frame or new packet, we can't wait, we quit
            return Err(PacketMismatch::TooNew);
        }
        if header.block_id < self.block_id {
            // We ignore packets from earlier presumed lost frames
            return Err(PacketMismatch::TooOld);
        }
        Ok(())
    }

    fn push(&mut self, header: PacketHeader, data: &[u8]) -> Result<(), PacketMismatch> {
        self.verify_header(&header)?;
        assert_eq!(header.packet_id, self.last_packet_id + 1);
        self.last_packet_id += 1;
        self.payload.extend_from_slice(data);
        Ok(())
    }

    fn build(self, header: PacketHeader) -> Result<Payload, PacketMismatch> {
        self.verify_header(&header)?;
        Ok(Payload {
            id: self.block_id,
            payload_type: crate::payload::PayloadType::Image,
            image_info: Some(crate::payload::ImageInfo {
                width: self.leader.width() as usize,
                height: self.leader.height() as usize,
                x_offset: self.leader.x_offset() as usize,
                y_offset: self.leader.y_offset() as usize,
                pixel_format: self.leader.pixel_format(),
                image_size: self.payload.len(),
            }),
            payload: self.payload.clone(),
            valid_payload_size: self.payload.len(),
            timestamp: self.leader.timestamp(),
        })
    }
}

enum PacketMismatch {
    TooNew,
    TooOld,
}

#[cfg(test)]
mod tests {
    use crate::payload::channel;

    use super::*;

    /// Generous next to the 100ms receive timeout the loop wakes on, so only a loop that never
    /// looks at its cancellation can exceed it.
    const STOP_TIMEOUT: Duration = Duration::from_secs(5);

    /// A camera that delivers nothing, which is what a packet size too large for the path looks
    /// like from the host: every receive runs to its timeout and no frame ever arrives.
    struct SilentSocket;

    impl StreamUdpSocket for SilentSocket {
        fn recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            thread::sleep(RECV_TIMEOUT);
            Err(io::Error::from(io::ErrorKind::WouldBlock))
        }

        fn set_read_timeout(&self, _timeout: Option<Duration>) -> io::Result<()> {
            Ok(())
        }

        fn port(&self) -> u16 {
            0
        }
    }

    /// A socket whose every receive fails outright rather than timing out.
    struct BrokenSocket;

    impl StreamUdpSocket for BrokenSocket {
        fn recv(&self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::from(io::ErrorKind::ConnectionReset))
        }

        fn set_read_timeout(&self, _timeout: Option<Duration>) -> io::Result<()> {
            Ok(())
        }

        fn port(&self) -> u16 {
            0
        }
    }

    fn assert_stops_within_timeout<S: StreamUdpSocket>(sock: S) {
        let mut handle = StreamHandle::new(sock).unwrap();
        let (sender, _receiver) = channel(1, 1);
        handle.spawn_streaming_loop(sender).unwrap();

        let (stopped_tx, stopped_rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let result = handle.stop_streaming_loop();
            stopped_tx.send(result).ok();
        });

        let stopped = stopped_rx.recv_timeout(STOP_TIMEOUT);

        assert!(
            matches!(stopped, Ok(Ok(()))),
            "stopping the streaming loop did not finish within {:?}: {:?}",
            STOP_TIMEOUT,
            stopped
        );
    }

    #[test]
    fn test_stop_streaming_loop_returns_when_no_frame_ever_arrives() {
        assert_stops_within_timeout(SilentSocket);
    }

    #[test]
    fn test_stop_streaming_loop_returns_when_every_receive_fails() {
        assert_stops_within_timeout(BrokenSocket);
    }
}
