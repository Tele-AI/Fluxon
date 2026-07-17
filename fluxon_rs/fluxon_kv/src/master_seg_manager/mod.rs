pub mod msg_pack;
pub mod one_seg_allocator;
use self::msg_pack::RequestSegmentRegistrationReq;
use self::msg_pack::SegmentDeviceDescription;
use self::one_seg_allocator::{Allocation, OneSegAllocator};
use crate::cluster_manager::NodeID;
use crate::p2p::p2p_module::P2pModuleAccessTrait;
use crate::rpcresp_kvresult_convert::msg_and_error::OK;
use crate::{
    p2p::{
        msg_pack::{MsgPack, RPCCaller},
        p2p_module::P2pModule,
    },
    rpcresp_kvresult_convert::msg_and_error::{KvError, KvResult},
};
use async_trait::async_trait;
use dashmap::DashMap;
use fluxon_framework::{LogicalModule, define_module};
use msg_pack::SegmentDeviceID;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

// --- Handler Functions ---
/// https://qcnoe3hd7k5c.feishu.cn/wiki/KkeXwBbP4iCRN8kWSDccP5GBnrd#share-AuMbdrSaXoadUbxRmUncooKnnQd
fn register_node_segments(
    view: &MasterSegManagerView,
    node_id: NodeID,
    node_start_time: i64,
    seg_map: std::collections::HashMap<
        SegmentDeviceID,
        (SegmentDeviceDescription, msg_pack::SegmentDeviceMemInfo),
    >,
) -> KvResult<()> {
    tracing::info!("Registering segments for node: {}", node_id);

    fn allocator_matches(
        existing: &OneSegAllocator,
        expected_desc: SegmentDeviceDescription,
        expected_addr: u64,
        expected_len: u64,
    ) -> bool {
        existing.seg_device_desc == expected_desc
            && existing.base_addr == expected_addr
            && existing.total_size_bytes() == expected_len
    }

    let alloc_map = &view
        .master_seg_manager()
        .inner()
        .node_allocators_and_tomb_tag;

    match alloc_map.entry(node_id.clone()) {
        dashmap::mapref::entry::Entry::Vacant(v) => {
            let mut total_size: u64 = 0;
            let mut device_id_2_allocator: HashMap<SegmentDeviceID, Arc<OneSegAllocator>> =
                HashMap::new();

            for (device_id, (seg_device_desc, seg_mem_info)) in seg_map {
                let allocator = OneSegAllocator::new(
                    device_id.clone(),
                    seg_device_desc,
                    seg_mem_info.addr,
                    seg_mem_info.len,
                )
                .map_err(|e| {
                    tracing::error!("Failed to create OneSegAllocator: {}", e);
                    e
                })?;

                total_size = total_size.saturating_add(seg_mem_info.len);
                device_id_2_allocator.insert(device_id, Arc::new(allocator));
            }

            v.insert(NodeSegmentsManager::new(
                node_start_time,
                total_size,
                device_id_2_allocator,
            ));
        }
        dashmap::mapref::entry::Entry::Occupied(mut occ) => {
            let node_segments_manager = occ.get_mut();

            // Tomb means the previous instance has left/restarted; replace the full segment set.
            if node_segments_manager.tomb_tag.is_tomb() {
                // An RPC response already in flight at MemberLeft must not resurrect the
                // departed generation. Only a genuinely newer epoch may replace a tomb.
                if node_segments_manager.node_start_time == node_start_time {
                    return Err(KvError::Api(
                        crate::rpcresp_kvresult_convert::msg_and_error::ApiError::RegisterSegmentFailed {
                            detail: format!(
                                "stale segment registration for tombed node generation: node={} node_start_time={}",
                                node_id, node_start_time
                            ),
                        },
                    ));
                }
                let mut total_size: u64 = 0;
                let mut device_id_2_allocator: HashMap<SegmentDeviceID, Arc<OneSegAllocator>> =
                    HashMap::new();

                for (device_id, (seg_device_desc, seg_mem_info)) in seg_map {
                    let allocator = OneSegAllocator::new(
                        device_id.clone(),
                        seg_device_desc,
                        seg_mem_info.addr,
                        seg_mem_info.len,
                    )
                    .map_err(|e| {
                        tracing::error!("Failed to create OneSegAllocator: {}", e);
                        e
                    })?;

                    total_size = total_size.saturating_add(seg_mem_info.len);
                    device_id_2_allocator.insert(device_id, Arc::new(allocator));
                }

                *node_segments_manager =
                    NodeSegmentsManager::new(node_start_time, total_size, device_id_2_allocator);
                tracing::info!("RegisterSegment replaced tombed node: {}", node_id);
                return Ok(());
            }

            if node_segments_manager.node_start_time != node_start_time {
                return Err(KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::RegisterSegmentFailed {
                        detail: format!(
                            "new segment generation attempted to replace a live generation: node={} live_node_start_time={} requested_node_start_time={}",
                            node_id, node_segments_manager.node_start_time, node_start_time
                        ),
                    },
                ));
            }

            // Non-tomb, same generation: allow re-entrant registration (idempotent) to
            // tolerate transient retries.
            for (device_id, (seg_device_desc, seg_mem_info)) in seg_map {
                if let Some(existing) = node_segments_manager.device_id_2_allocator.get(&device_id)
                {
                    if allocator_matches(
                        existing,
                        seg_device_desc,
                        seg_mem_info.addr,
                        seg_mem_info.len,
                    ) {
                        continue;
                    }
                    return Err(KvError::Unreachable(
                        crate::rpcresp_kvresult_convert::msg_and_error::UnreachableError::DuplicateSegId {
                            device_id: device_id.clone(),
                            node_id: node_id.to_string(),
                        },
                    ));
                }

                let allocator = OneSegAllocator::new(
                    device_id.clone(),
                    seg_device_desc,
                    seg_mem_info.addr,
                    seg_mem_info.len,
                )
                .map_err(|e| {
                    tracing::error!("Failed to create OneSegAllocator: {}", e);
                    e
                })?;

                node_segments_manager
                    .device_id_2_allocator
                    .insert(device_id, Arc::new(allocator));
                node_segments_manager.total_size = node_segments_manager
                    .total_size
                    .saturating_add(seg_mem_info.len);
            }
        }
    }

    tracing::info!("RegisterSegment success for node: {}", node_id);
    Ok(())
}

// --- MasterSegManager Module ---

define_module!(
    MasterSegManager,
    (master_seg_manager, MasterSegManager),
    (p2p, P2pModule)
);

pub struct MasterSegManager(MasterSegManagerInner);

#[derive(Clone, Debug)]
pub struct NodeTombTag(Arc<AtomicBool>);

impl Default for NodeTombTag {
    fn default() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }
}

impl NodeTombTag {
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    pub fn is_tomb(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub fn set_tomb(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// True only when both tags belong to the same node registration generation.
    pub fn same_generation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

pub struct NodeSegmentsManager {
    node_start_time: i64,
    total_size: u64,
    device_id_2_allocator: HashMap<SegmentDeviceID, Arc<OneSegAllocator>>,
    tomb_tag: NodeTombTag,
}

impl NodeSegmentsManager {
    pub fn new(
        node_start_time: i64,
        total_size: u64,
        device_id_2_allocator: HashMap<SegmentDeviceID, Arc<OneSegAllocator>>,
    ) -> Self {
        Self {
            node_start_time,
            total_size,
            device_id_2_allocator,
            tomb_tag: NodeTombTag::new(),
        }
    }
}

pub struct MasterSegManagerInner {
    view: std::sync::OnceLock<MasterSegManagerView>,
    /// { node_id -> { seg_name -> allocator } }
    /// nodes memory distribution will not change in current design
    node_allocators_and_tomb_tag: DashMap<NodeID, NodeSegmentsManager>,

    /// RPC caller for requesting segment registration from clients
    rpc_caller_request_segment_registration: RPCCaller<RequestSegmentRegistrationReq>,
}

impl MasterSegManagerInner {
    fn view(&self) -> &MasterSegManagerView {
        self.view.get().unwrap()
    }
}

/// MasterSegManager module creation parameters.
///
/// MasterSegManager is a master-only module. It is constructed only in the `master` init DAG
/// variant (see framework_init_steps.yaml).
#[derive(Clone, Debug)]
pub struct MasterSegManagerNewArg;

#[async_trait]
impl LogicalModule for MasterSegManager {
    type View = MasterSegManagerView;
    type NewArg = MasterSegManagerNewArg;
    type Error = KvError;

    fn name(&self) -> &str {
        "MasterSegManager"
    }

    fn attach_view(&self, view: Self::View) {
        MasterSegManager::attach_view(self, view);
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        tracing::info!("Shutting down MasterSegManager");
        Ok(())
    }
}

impl MasterSegManager {
    pub fn attach_view(&self, view: MasterSegManagerView) {
        // The framework attaches a module's PostView exactly once at the init barrier.
        // A second attach indicates a programming error.
        self.0
            .view
            .set(view)
            .unwrap_or_else(|_| panic!("MasterSegManager view attached twice"));
    }

    pub async fn construct(arg: MasterSegManagerNewArg) -> Result<Self, KvError> {
        let _ = arg;
        let inner = MasterSegManagerInner {
            view: std::sync::OnceLock::new(),
            node_allocators_and_tomb_tag: DashMap::new(),
            rpc_caller_request_segment_registration: RPCCaller::new(),
        };
        Ok(Self(inner))
    }

    pub async fn init2_for_init_dag(&self) -> Result<(), KvError> {
        tracing::info!("MasterSegManager init2_for_init_dag");
        self.register_rpc_handlers();

        self.0
            .rpc_caller_request_segment_registration
            .regist(self.0.view().p2p_module());
        Ok(())
    }

    fn inner(&self) -> &MasterSegManagerInner {
        &self.0
    }

    // pub fn allocate_from_seg(
    //     &self,
    //     node_id: &NodeID,
    //     seg_name: &str,
    //     size: u64,
    // ) -> Result<Allocation, KvError> {
    //     let node_allocators = self
    //         .inner()
    //         .allocators
    //         .get(node_id)
    //         .ok_or_else(|| KvError::Internal(format!("Node not found: {}", node_id)))?;

    //     let allocator = node_allocators
    //         .get(seg_name)
    //         .ok_or_else(|| KvError::Internal(format!("Segment not found: {}", seg_name)))?;

    //     allocator.clone().allocate(size)
    // }

    pub fn mark_node_tomb(&self, node_id: &NodeID) -> Option<NodeTombTag> {
        self.mark_node_tomb_generation(node_id, None)
    }

    /// Mark one exact membership generation as departed and return its shared tomb tag.
    /// A delayed leave for an older epoch must not tomb a newly registered generation that
    /// happens to reuse the same node id.
    pub fn mark_node_tomb_generation(
        &self,
        node_id: &NodeID,
        expected_node_start_time: Option<i64>,
    ) -> Option<NodeTombTag> {
        if let Some(allocators_and_tomb_tag) =
            self.inner().node_allocators_and_tomb_tag.get(node_id)
        {
            if expected_node_start_time
                .is_some_and(|expected| allocators_and_tomb_tag.node_start_time != expected)
            {
                return None;
            }
            allocators_and_tomb_tag.tomb_tag.set_tomb();
            Some(allocators_and_tomb_tag.tomb_tag.clone())
        } else {
            None
        }
    }

    pub fn get_node_tomb_tag(&self, node_id: &NodeID) -> Option<NodeTombTag> {
        if let Some(allocators_and_tomb_tag) =
            self.inner().node_allocators_and_tomb_tag.get(node_id)
        {
            Some(allocators_and_tomb_tag.tomb_tag.clone())
        } else {
            None
        }
    }

    /// Resolve the registration generation that owns an already-created allocation.
    ///
    /// A plain `get_node_tomb_tag(node_id)` is insufficient: the node may have left and
    /// re-registered between allocation and completion.  In that case the current tag belongs
    /// to a different allocator set and must never be attached to the old allocation.
    pub fn get_allocation_tomb_tag(
        &self,
        node_id: &NodeID,
        allocation: &Allocation,
    ) -> Option<NodeTombTag> {
        let node = self.inner().node_allocators_and_tomb_tag.get(node_id)?;
        if node.tomb_tag.is_tomb()
            || !node
                .device_id_2_allocator
                .values()
                .any(|allocator| allocation.belongs_to_allocator(allocator))
        {
            return None;
        }
        Some(node.tomb_tag.clone())
    }

    pub fn get_node_allocators(&self, node_id: &NodeID) -> Vec<Arc<OneSegAllocator>> {
        let mut ret = Vec::new();
        if let Some(node_allocators) = self.inner().node_allocators_and_tomb_tag.get(node_id) {
            if node_allocators.tomb_tag.is_tomb() {
                tracing::info!("Node {:?} is tagged as tomb, no allocators", node_id);
                return Vec::new();
            }
            for (_device_id, allocator) in node_allocators.device_id_2_allocator.iter() {
                ret.push(allocator.clone());
            }
        }
        ret
    }

    pub fn get_all_segments_allocator(&self) -> Vec<(NodeID, Arc<OneSegAllocator>)> {
        let mut ret = Vec::new();
        let mut tombed_nodes = Vec::new();
        for entry in self.inner().node_allocators_and_tomb_tag.iter() {
            if entry.value().tomb_tag.is_tomb() {
                tombed_nodes.push(entry.key().clone());
                continue;
            }
            for (_devid, allocator) in entry.value().device_id_2_allocator.iter() {
                ret.push((entry.key().clone(), allocator.clone()));
            }
        }
        // clean up tombed nodes
        for node_id in tombed_nodes {
            self.inner()
                .node_allocators_and_tomb_tag
                .remove_if(&node_id, |_, v| v.tomb_tag.is_tomb());
        }
        ret
    }

    fn register_rpc_handlers(&self) {
        // QuerySegBase RPC removed: no handlers to register here currently.
        let _ = self;
    }

    /// Request segment registration from a client node
    pub async fn request_segment_registration(
        &self,
        node_id: NodeID,
        expected_node_start_time: i64,
    ) -> Result<(), KvError> {
        let inner = self.inner();

        let req = MsgPack {
            serialize_part: RequestSegmentRegistrationReq {
                expected_node_start_time,
            },
            raw_bytes: Vec::new(),
        };

        tracing::info!("Requesting segment registration from node: {}", node_id);

        let resp = inner
            .rpc_caller_request_segment_registration
            .call(
                inner.view().p2p_module(),
                node_id.clone(),
                req,
                Some(Duration::from_secs(30)), // 30 second timeout
                1, // Master controls retry/backoff to validate member liveness/epoch before each attempt
            )
            .await
            .map_err(|e| {
                tracing::warn!(
                    "Failed to request segment registration from node {}: {:?}",
                    node_id,
                    e
                );
                e
            })?;

        if resp.serialize_part.error_code != OK {
            let error = crate::rpcresp_kvresult_convert::msg_and_error::KvError::from_json(
                resp.serialize_part.error_code,
                &resp.serialize_part.error_json,
            );
            tracing::error!(
                "RequestSegmentRegistrationResp error from node {}: {:?}",
                node_id,
                error
            );
            return Err(error);
        }

        if resp.serialize_part.seg_map.is_empty() {
            tracing::info!("Node {} responded with no segments to register.", node_id);
            return Ok(());
        }

        tracing::info!(
            "Received segment registration from node {}, segments: {:?}",
            node_id,
            resp.serialize_part.seg_map.keys()
        );

        // Now, register these segments in the master.
        match register_node_segments(
            inner.view(),
            node_id.clone(),
            expected_node_start_time,
            resp.serialize_part.seg_map,
        ) {
            Ok(()) => {
                tracing::info!("Successfully registered segments for node {}", node_id);
            }
            Err(e) => {
                tracing::error!("Failed to register segments for node {}: {:?}", node_id, e);
                return Err(e);
            }
        }

        Ok(())
    }

    pub fn get_node_space_size(&self, node_id: &str) -> u64 {
        self.inner()
            .node_allocators_and_tomb_tag
            .get(node_id)
            .filter(|node_segments_manager| !node_segments_manager.tomb_tag.is_tomb())
            .map(|node_segments_manager| node_segments_manager.total_size)
            .unwrap_or(0)
    }
}
