/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/. */

use super::{
    elem_type::{ImmOrPNode, IntegerRepresentation, PValue, ValueKind},
    interface::{IInteger, INode, ISelector, IncrementMode},
    ivalue::IValue,
    node_base::{NodeAttributeBase, NodeBase, NodeElementBase},
    store::{CacheStore, IntegerId, NodeId, NodeStore, ValueStore},
    Device, GenApiError, GenApiResult, ValueCtxt,
};

/// The increment GenApi gives a node that declares none, and the one a node keeps when the
/// node it delegates to has no notion of an increment.
const DEFAULT_INCREMENT: i64 = 1;

#[derive(Debug, Clone)]
pub struct IntegerNode {
    pub(crate) attr_base: NodeAttributeBase,
    pub(crate) elem_base: NodeElementBase,

    pub(crate) streamable: bool,
    pub(crate) value_kind: ValueKind<IntegerId>,
    pub(crate) min: Option<ImmOrPNode<IntegerId>>,
    pub(crate) max: Option<ImmOrPNode<IntegerId>>,
    pub(crate) inc: Option<ImmOrPNode<i64>>,
    pub(crate) unit: Option<String>,
    pub(crate) representation: IntegerRepresentation,
    pub(crate) p_selected: Vec<NodeId>,
}

impl IntegerNode {
    #[must_use]
    pub fn value_kind(&self) -> &ValueKind<IntegerId> {
        &self.value_kind
    }

    #[must_use]
    pub fn min_elem(&self) -> Option<ImmOrPNode<IntegerId>> {
        self.min
    }

    #[must_use]
    pub fn max_elem(&self) -> Option<ImmOrPNode<IntegerId>> {
        self.max
    }

    #[must_use]
    pub fn inc_elem(&self) -> Option<ImmOrPNode<i64>> {
        self.inc
    }

    fn p_value(&self) -> Option<NodeId> {
        self.value_kind.p_value().map(PValue::p_value)
    }

    /// The limit GenApi propagates to a node that declares none of its own: the one of the node
    /// it delegates its value to, and the range its representation implies when it delegates to
    /// nothing. Basler leaves `GevSCPSPacketSize` bare and states its real range on the private
    /// node behind `pValue`, so a node read in isolation reports no limit the camera set.
    fn inherited_min<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<i64> {
        match self.p_value().and_then(|nid| nid.as_iinteger_kind(store)) {
            Some(node) => node.min(device, store, cx),
            None => Ok(self.representation.deduce_min()),
        }
    }

    fn inherited_max<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<i64> {
        match self.p_value().and_then(|nid| nid.as_iinteger_kind(store)) {
            Some(node) => node.max(device, store, cx),
            None => Ok(self.representation.deduce_max()),
        }
    }

    fn inherited_inc<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<Option<i64>> {
        match self.p_value().and_then(|nid| nid.as_iinteger_kind(store)) {
            Some(node) => node.inc(device, store, cx),
            None => Ok(None),
        }
    }

    #[must_use]
    pub fn unit_elem(&self) -> Option<&str> {
        self.unit.as_deref()
    }

    #[must_use]
    pub fn representation_elem(&self) -> IntegerRepresentation {
        self.representation
    }

    #[must_use]
    pub fn p_selected(&self) -> &[NodeId] {
        &self.p_selected
    }
}

impl INode for IntegerNode {
    fn node_base(&self) -> NodeBase<'_> {
        NodeBase::new(&self.attr_base, &self.elem_base)
    }

    fn streamable(&self) -> bool {
        self.streamable
    }
}

impl IInteger for IntegerNode {
    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn value<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<i64> {
        self.value_kind().value(device, store, cx)
    }

    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn set_value<T: ValueStore, U: CacheStore>(
        &self,
        value: i64,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<()> {
        cx.invalidate_cache_by(self.node_base().id());
        self.value_kind().set_value(value, device, store, cx)
    }

    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn min<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<i64> {
        match self.min {
            Some(min) => min.value(device, store, cx),
            None => self.inherited_min(device, store, cx),
        }
    }

    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn max<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<i64> {
        match self.max {
            Some(max) => max.value(device, store, cx),
            None => self.inherited_max(device, store, cx),
        }
    }

    fn inc_mode(&self, _: &impl NodeStore) -> Option<IncrementMode> {
        Some(IncrementMode::FixedIncrement)
    }

    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn inc<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<Option<i64>> {
        let inc = match self.inc {
            Some(inc) => Some(inc.value(device, store, cx)?),
            None => self.inherited_inc(device, store, cx)?,
        };

        Ok(Some(inc.unwrap_or(DEFAULT_INCREMENT)))
    }

    fn valid_value_set(&self, _: &impl NodeStore) -> &[i64] {
        &[]
    }

    fn representation(&self, _: &impl NodeStore) -> IntegerRepresentation {
        self.representation_elem()
    }

    fn unit(&self, _: &impl NodeStore) -> Option<&str> {
        self.unit_elem()
    }

    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn set_min<T: ValueStore, U: CacheStore>(
        &self,
        value: i64,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<()> {
        match self.min {
            Some(min) => min.set_value(value, device, store, cx),
            None => Err(GenApiError::not_writable()),
        }
    }

    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn set_max<T: ValueStore, U: CacheStore>(
        &self,
        value: i64,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<()> {
        match self.max {
            Some(max) => max.set_value(value, device, store, cx),
            None => Err(GenApiError::not_writable()),
        }
    }

    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn is_readable<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<bool> {
        Ok(self.elem_base.is_readable(device, store, cx)?
            && IValue::<i64>::is_readable(&self.value_kind, device, store, cx)?)
    }

    #[tracing::instrument(skip(self, device, store, cx),
                          level = "trace",
                          fields(node = store.name_by_id(self.node_base().id()).unwrap()))]
    fn is_writable<T: ValueStore, U: CacheStore>(
        &self,
        device: &mut impl Device,
        store: &impl NodeStore,
        cx: &mut ValueCtxt<T, U>,
    ) -> GenApiResult<bool> {
        Ok(self.elem_base.is_writable(device, store, cx)?
            && IValue::<i64>::is_writable(&self.value_kind, device, store, cx)?)
    }
}

impl ISelector for IntegerNode {
    fn selecting_nodes(&self, _: &impl NodeStore) -> GenApiResult<&[NodeId]> {
        Ok(self.p_selected())
    }
}
