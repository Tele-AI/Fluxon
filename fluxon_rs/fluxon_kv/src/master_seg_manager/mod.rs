pub mod msg_pack;
pub mod one_seg_allocator;
use self::msg_pack::{
    ConfirmNodeSegmentsRegisteredReq, OwnerPlacementClass, RegisterNodeSegmentsReq,
    RegisterNodeSegmentsResp, RequestSegmentRegistrationReq,
};
use self::msg_pack::{OwnerCapacityReport, OwnerCapacityReportReq, OwnerCapacityReportResp};
use self::msg_pack::{SegmentAllocationAuthority, SegmentDeviceDescription, SegmentDeviceMemInfo};
use self::one_seg_allocator::{
    Allocation, NodePoolCapacityBudget, NodePoolCapacitySnapshot, OneSegAllocator,
};
use crate::cluster_manager::NodeID;
use crate::p2p::control_plane_rpc::{call_control_plane_rpc, send_control_plane_rpc_response};
use crate::p2p::p2p_module::P2pModuleAccessTrait;
use crate::rpcresp_kvresult_convert::FromError;
use crate::rpcresp_kvresult_convert::msg_and_error::OK;
use crate::{
    p2p::{
        msg_pack::{MsgPack, RPCCaller, RPCHandler},
        p2p_module::P2pModule,
    },
    rpcresp_kvresult_convert::msg_and_error::{KvError, KvResult},
};
use async_trait::async_trait;
use dashmap::DashMap;
use fluxon_framework::{LogicalModule, define_module};
use msg_pack::SegmentDeviceID;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

fn build_node_segments_manager(
    node_start_time: i64,
    allocation_authority: SegmentAllocationAuthority,
    owner_placement_class: OwnerPlacementClass,
    owner_local_target_bytes: Option<u64>,
    seg_map: std::collections::HashMap<
        SegmentDeviceID,
        (SegmentDeviceDescription, msg_pack::SegmentDeviceMemInfo),
    >,
) -> KvResult<NodeSegmentsManager> {
    if allocation_authority == SegmentAllocationAuthority::Owner {
        let exactly_one_cpu_segment = seg_map.len() == 1
            && seg_map
                .values()
                .next()
                .is_some_and(|(description, segment)| {
                    *description == SegmentDeviceDescription::Cpu
                        && segment.len != 0
                        && segment.len % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES == 0
                });
        if !exactly_one_cpu_segment {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: "owner allocation authority requires exactly one non-empty 4 KiB-aligned CPU segment"
                        .to_string(),
                },
            ));
        }
    }
    let total_size = seg_map.values().try_fold(0u64, |total, (_, info)| {
        total.checked_add(info.len).ok_or_else(|| {
            KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: "registered node segment capacity overflows u64".to_string(),
                },
            )
        })
    })?;
    let capacity_budget = Arc::new(NodePoolCapacityBudget::new(total_size)?);
    match (
        allocation_authority,
        owner_placement_class,
        owner_local_target_bytes,
    ) {
        (SegmentAllocationAuthority::Owner, OwnerPlacementClass::Inference, Some(target))
            if target != 0 && target <= total_size => {}
        (SegmentAllocationAuthority::Owner, OwnerPlacementClass::RemoteCpu, Some(0)) => {}
        (SegmentAllocationAuthority::Owner, placement_class, target) => {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "owner-authoritative segment has invalid placement class/local target: class={:?} target={:?} physical={}",
                        placement_class, target, total_size
                    ),
                },
            ));
        }
        (SegmentAllocationAuthority::Master, OwnerPlacementClass::Invalid, None) => {}
        (SegmentAllocationAuthority::Master, placement_class, target) => {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "master-authoritative segment must not report owner placement state: class={:?} target={:?}",
                        placement_class, target
                    ),
                },
            ));
        }
    }
    let mut device_id_2_allocator: HashMap<SegmentDeviceID, Arc<OneSegAllocator>> = HashMap::new();
    if allocation_authority == SegmentAllocationAuthority::Master {
        for (device_id, (seg_device_desc, seg_mem_info)) in &seg_map {
            let allocator = OneSegAllocator::new_with_capacity_budget(
                device_id.clone(),
                seg_device_desc.clone(),
                seg_mem_info.addr,
                seg_mem_info.len,
                capacity_budget.clone(),
            )
            .map_err(|e| {
                tracing::error!("Failed to create OneSegAllocator: {}", e);
                e
            })?;
            device_id_2_allocator.insert(device_id.clone(), Arc::new(allocator));
        }
    }
    Ok(NodeSegmentsManager::new(
        node_start_time,
        total_size,
        allocation_authority,
        owner_placement_class,
        owner_local_target_bytes,
        seg_map,
        device_id_2_allocator,
        capacity_budget,
    ))
}

fn validate_live_registration_identity(
    node_id: &NodeID,
    existing: &NodeSegmentsManager,
    requested_node_start_time: i64,
    requested_authority: SegmentAllocationAuthority,
    requested_placement_class: OwnerPlacementClass,
    requested_local_target_bytes: Option<u64>,
) -> KvResult<()> {
    if existing.node_start_time != requested_node_start_time {
        return Err(KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::RegisterSegmentFailed {
                detail: format!(
                    "new segment generation attempted to replace a live generation: node={} live_node_start_time={} requested_node_start_time={}",
                    node_id, existing.node_start_time, requested_node_start_time
                ),
            },
        ));
    }
    if existing.allocation_authority != requested_authority {
        return Err(KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::RegisterSegmentFailed {
                detail: format!(
                    "segment allocation authority changed within one live generation: node={} live={:?} requested={:?}",
                    node_id, existing.allocation_authority, requested_authority
                ),
            },
        ));
    }
    if existing.owner_placement_class != requested_placement_class {
        return Err(KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::RegisterSegmentFailed {
                detail: format!(
                    "owner placement class changed within one live generation: node={} live={:?} requested={:?}",
                    node_id, existing.owner_placement_class, requested_placement_class
                ),
            },
        ));
    }
    if existing.owner_local_target_bytes != requested_local_target_bytes {
        return Err(KvError::Api(
            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::RegisterSegmentFailed {
                detail: format!(
                    "owner local target changed within one live generation: node={} live={:?} requested={:?}",
                    node_id, existing.owner_local_target_bytes, requested_local_target_bytes
                ),
            },
        ));
    }
    Ok(())
}

// --- Handler Functions ---
/// https://qcnoe3hd7k5c.feishu.cn/wiki/KkeXwBbP4iCRN8kWSDccP5GBnrd#share-AuMbdrSaXoadUbxRmUncooKnnQd
fn register_node_segments(
    view: &MasterSegManagerView,
    node_id: NodeID,
    node_start_time: i64,
    allocation_authority: SegmentAllocationAuthority,
    owner_placement_class: OwnerPlacementClass,
    owner_local_target_bytes: Option<u64>,
    seg_map: std::collections::HashMap<
        SegmentDeviceID,
        (SegmentDeviceDescription, msg_pack::SegmentDeviceMemInfo),
    >,
) -> KvResult<()> {
    tracing::info!("Registering segments for node: {}", node_id);

    fn segment_matches(
        existing: &(SegmentDeviceDescription, SegmentDeviceMemInfo),
        expected_desc: SegmentDeviceDescription,
        expected_addr: u64,
        expected_len: u64,
    ) -> bool {
        existing.0 == expected_desc
            && existing.1.addr == expected_addr
            && existing.1.len == expected_len
    }

    let alloc_map = &view
        .master_seg_manager()
        .inner()
        .node_allocators_and_tomb_tag;

    match alloc_map.entry(node_id.clone()) {
        dashmap::mapref::entry::Entry::Vacant(v) => {
            v.insert(build_node_segments_manager(
                node_start_time,
                allocation_authority,
                owner_placement_class,
                owner_local_target_bytes,
                seg_map,
            )?);
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
                *node_segments_manager = build_node_segments_manager(
                    node_start_time,
                    allocation_authority,
                    owner_placement_class,
                    owner_local_target_bytes,
                    seg_map,
                )?;
                tracing::info!("RegisterSegment replaced tombed node: {}", node_id);
                return Ok(());
            }

            validate_live_registration_identity(
                &node_id,
                node_segments_manager,
                node_start_time,
                allocation_authority,
                owner_placement_class,
                owner_local_target_bytes,
            )?;

            // Non-tomb, same generation: allow re-entrant registration (idempotent) to
            // tolerate transient retries.
            for (device_id, (seg_device_desc, seg_mem_info)) in seg_map {
                if let Some(existing) = node_segments_manager.registered_segments.get(&device_id) {
                    if segment_matches(
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

                if allocation_authority == SegmentAllocationAuthority::Owner {
                    return Err(KvError::Api(
                        crate::rpcresp_kvresult_convert::msg_and_error::ApiError::RegisterSegmentFailed {
                            detail: format!(
                                "owner-authoritative generation cannot append a second segment: node={} device_id={}",
                                node_id, device_id
                            ),
                        },
                    ));
                }

                let new_total_size = node_segments_manager
                    .total_size
                    .checked_add(seg_mem_info.len)
                    .ok_or_else(|| {
                        KvError::Api(
                            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                                detail: format!(
                                    "registered node segment capacity overflows u64: node={}",
                                    node_id
                                ),
                            },
                        )
                    })?;
                let allocator = if allocation_authority == SegmentAllocationAuthority::Master {
                    Some(
                        OneSegAllocator::new_with_capacity_budget(
                            device_id.clone(),
                            seg_device_desc.clone(),
                            seg_mem_info.addr,
                            seg_mem_info.len,
                            node_segments_manager.capacity_budget.clone(),
                        )
                        .map(Arc::new)
                        .map_err(|e| {
                            tracing::error!("Failed to create OneSegAllocator: {}", e);
                            e
                        })?,
                    )
                } else {
                    None
                };

                node_segments_manager
                    .capacity_budget
                    .extend_physical_capacity(seg_mem_info.len)?;
                if let Some(allocator) = allocator {
                    node_segments_manager
                        .device_id_2_allocator
                        .insert(device_id.clone(), allocator);
                }
                node_segments_manager
                    .registered_segments
                    .insert(device_id, (seg_device_desc, seg_mem_info));
                node_segments_manager.total_size = new_total_size;
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
    allocation_authority: SegmentAllocationAuthority,
    owner_placement_class: OwnerPlacementClass,
    owner_local_target_bytes: Option<u64>,
    registered_segments: HashMap<SegmentDeviceID, (SegmentDeviceDescription, SegmentDeviceMemInfo)>,
    device_id_2_allocator: HashMap<SegmentDeviceID, Arc<OneSegAllocator>>,
    capacity_budget: Arc<NodePoolCapacityBudget>,
    owner_capacity_report: Option<StoredOwnerCapacityReport>,
    tomb_tag: NodeTombTag,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SegmentCapacitySnapshot {
    pub node_id: NodeID,
    pub device_id: SegmentDeviceID,
    pub available_bytes: u64,
    pub total_bytes: u64,
}

struct StoredOwnerCapacityReport {
    report: OwnerCapacityReport,
    received_at: Instant,
}

impl NodeSegmentsManager {
    fn new(
        node_start_time: i64,
        total_size: u64,
        allocation_authority: SegmentAllocationAuthority,
        owner_placement_class: OwnerPlacementClass,
        owner_local_target_bytes: Option<u64>,
        registered_segments: HashMap<
            SegmentDeviceID,
            (SegmentDeviceDescription, SegmentDeviceMemInfo),
        >,
        device_id_2_allocator: HashMap<SegmentDeviceID, Arc<OneSegAllocator>>,
        capacity_budget: Arc<NodePoolCapacityBudget>,
    ) -> Self {
        Self {
            node_start_time,
            total_size,
            allocation_authority,
            owner_placement_class,
            owner_local_target_bytes,
            registered_segments,
            device_id_2_allocator,
            capacity_budget,
            owner_capacity_report: None,
            tomb_tag: NodeTombTag::new(),
        }
    }
}

pub struct MasterSegManagerInner {
    view: std::sync::OnceLock<MasterSegManagerView>,
    /// { node_id -> { seg_name -> allocator } }
    /// nodes memory distribution will not change in current design
    node_allocators_and_tomb_tag: DashMap<NodeID, NodeSegmentsManager>,

    /// Allocation size classes observed in valid owner reports or requested
    /// by placement. Owners learn this set through the existing report
    /// response and publish exact allocator-derived capacity on the next tick.
    owner_capacity_size_classes: parking_lot::RwLock<BTreeSet<u64>>,

    /// RPC caller for requesting segment registration from clients
    rpc_caller_request_segment_registration: RPCCaller<RequestSegmentRegistrationReq>,
    /// RPC caller that closes the master-pull branch of the distributed init DAG
    /// after the exact owner generation has been installed locally.
    rpc_caller_confirm_node_segments_registered: RPCCaller<ConfirmNodeSegmentsRegisteredReq>,
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
            owner_capacity_size_classes: parking_lot::RwLock::new(BTreeSet::new()),
            rpc_caller_request_segment_registration: RPCCaller::new(),
            rpc_caller_confirm_node_segments_registered: RPCCaller::new(),
        };
        Ok(Self(inner))
    }

    pub async fn init2_for_init_dag(&self) -> Result<(), KvError> {
        tracing::info!("MasterSegManager init2_for_init_dag");
        self.register_segment_registration_handler_for_init_dag()
            .await?;
        self.register_rpc_handlers();

        self.0
            .rpc_caller_request_segment_registration
            .regist(self.0.view().p2p_module());
        self.0
            .rpc_caller_confirm_node_segments_registered
            .regist(self.0.view().p2p_module());
        Ok(())
    }

    async fn register_segment_registration_handler_for_init_dag(&self) -> KvResult<()> {
        let view = self.0.view().clone();
        RPCHandler::<RegisterNodeSegmentsReq>::new()
            .regist_for_init_dag(self.0.view().p2p_module(), move |resp, msg| {
                let view = view.clone();
                let caller = resp.node_id();
                let caller_node_start_time = resp.node_start_time();
                let spawn_view = view.clone();
                let worker_view = spawn_view.clone();
                spawn_view.spawn("rpc_register_node_segments", async move {
                    let request = msg.serialize_part;
                    tracing::info!(
                        owner = %caller,
                        owner_node_start_time = caller_node_start_time,
                        "owner segment registration DAG request reached master"
                    );
                    let response = if request.owner_node_start_time != caller_node_start_time {
                        RegisterNodeSegmentsResp::from_error(&KvError::Api(
                            crate::rpcresp_kvresult_convert::msg_and_error::ApiError::OwnerStartTimeMismatch {
                                expected: caller_node_start_time,
                                got: request.owner_node_start_time,
                            },
                        ))
                    } else {
                        match register_node_segments(
                            &worker_view,
                            caller.clone(),
                            caller_node_start_time,
                            request.registration.allocation_authority,
                            request.registration.owner_placement_class,
                            request.registration.owner_local_target_bytes,
                            request.registration.seg_map,
                        ) {
                            Ok(()) => RegisterNodeSegmentsResp {
                                error_code: OK,
                                error_json: String::new(),
                                accepted_owner_node_start_time: caller_node_start_time,
                            },
                            Err(error) => RegisterNodeSegmentsResp::from_error(&error),
                        }
                    };
                    if let Err(error) = send_control_plane_rpc_response(
                        &resp,
                        MsgPack {
                            serialize_part: response,
                            raw_bytes: Vec::new(),
                        },
                    )
                    .await
                    {
                        tracing::warn!(
                            %error,
                            owner = %caller,
                            owner_node_start_time = caller_node_start_time,
                            "failed to send owner segment registration DAG ACK"
                        );
                    }
                });
                Ok(())
            })
            .await?;
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
                tombed_nodes.push((entry.key().clone(), entry.value().tomb_tag.clone()));
                continue;
            }
            for (_devid, allocator) in entry.value().device_id_2_allocator.iter() {
                ret.push((entry.key().clone(), allocator.clone()));
            }
        }
        self.prune_tombed_nodes(tombed_nodes);
        ret
    }

    fn prune_tombed_nodes(&self, tombed_nodes: Vec<(NodeID, NodeTombTag)>) {
        // Remove observed tombs after iterator guards drop, fencing re-registration.
        for (node_id, tomb_tag) in tombed_nodes {
            self.inner()
                .node_allocators_and_tomb_tag
                .remove_if(&node_id, |_, node| {
                    node.tomb_tag.same_generation(&tomb_tag) && node.tomb_tag.is_tomb()
                });
        }
    }

    /// Snapshot physical capacity for all live segments in O(registered segments).
    ///
    /// Owner values come from the latest accepted allocator report, including
    /// prepared, pending-visible, and retained allocations. Owners without an
    /// initial report are omitted until their free capacity is known.
    /// These observation samples are not linearizable admission capacity.
    pub(crate) fn snapshot_segment_capacity(&self) -> Vec<SegmentCapacitySnapshot> {
        let mut snapshots = Vec::new();
        let mut tombed_nodes = Vec::new();
        for entry in self.inner().node_allocators_and_tomb_tag.iter() {
            let node = entry.value();
            if node.tomb_tag.is_tomb() {
                tombed_nodes.push((entry.key().clone(), node.tomb_tag.clone()));
                continue;
            }
            match node.allocation_authority {
                SegmentAllocationAuthority::Master => {
                    for (device_id, allocator) in &node.device_id_2_allocator {
                        let total_bytes = allocator.total_size_bytes();
                        snapshots.push(SegmentCapacitySnapshot {
                            node_id: entry.key().clone(),
                            device_id: device_id.clone(),
                            available_bytes: total_bytes.saturating_sub(allocator.used_size_bytes()),
                            total_bytes,
                        });
                    }
                }
                SegmentAllocationAuthority::Owner => {
                    let Some(stored) = node.owner_capacity_report.as_ref() else {
                        continue;
                    };
                    // Registration and report validation enforce one CPU segment
                    // and the same physical capacity within this node generation.
                    assert_eq!(node.registered_segments.len(), 1);
                    let device_id = node.registered_segments.keys().next().unwrap();
                    snapshots.push(SegmentCapacitySnapshot {
                        node_id: entry.key().clone(),
                        device_id: device_id.clone(),
                        available_bytes: stored.report.raw_free_bytes,
                        total_bytes: stored.report.physical_capacity_bytes,
                    });
                }
            }
        }
        self.prune_tombed_nodes(tombed_nodes);
        snapshots
    }

    fn register_rpc_handlers(&self) {
        let view = self.0.view().clone();
        RPCHandler::<OwnerCapacityReportReq>::new().regist(
            self.0.view().p2p_module(),
            move |resp, msg| {
                let caller = resp.node_id().clone();
                let spawn_view = view.clone();
                let worker_view = spawn_view.clone();
                spawn_view.spawn("rpc_owner_capacity_report", async move {
                    let result = worker_view
                        .master_seg_manager()
                        .update_owner_capacity_report(&caller, msg.serialize_part.report);
                    let response = match result {
                        Ok(accepted_report_epoch) => OwnerCapacityReportResp {
                            accepted_report_epoch,
                            requested_size_classes: worker_view
                                .master_seg_manager()
                                .owner_capacity_size_classes(),
                            error_code: OK,
                            error_json: String::new(),
                        },
                        Err(error) => OwnerCapacityReportResp {
                            accepted_report_epoch: 0,
                            requested_size_classes: Vec::new(),
                            error_code: error.code(),
                            error_json: error.to_json(),
                        },
                    };
                    if let Err(error) = send_control_plane_rpc_response(
                        &resp,
                        MsgPack {
                            serialize_part: response,
                            raw_bytes: Vec::new(),
                        },
                    )
                    .await
                    {
                        tracing::warn!(%error, caller = %caller, "failed to send owner capacity report response");
                    }
                });
                Ok(())
            },
        );
    }

    /// Request segment registration from a client node
    pub async fn request_segment_registration(
        &self,
        node_id: NodeID,
        expected_node_start_time: i64,
    ) -> Result<(), KvError> {
        let inner = self.inner();

        // The member directory can become visible before the generation-safe P2P
        // peer DAG reaches its send-ready terminal.  This is a local state check,
        // not an extra wire RTT, and prevents a known-not-ready peer from consuming
        // the 30-second control-RPC timeout.
        inner
            .view()
            .p2p_module()
            .ensure_peer_send_ready(&node_id)
            .await?;

        let req = MsgPack {
            serialize_part: RequestSegmentRegistrationReq {
                expected_node_start_time,
            },
            raw_bytes: Vec::new(),
        };

        tracing::info!("Requesting segment registration from node: {}", node_id);

        let resp = call_control_plane_rpc(
            &inner.rpc_caller_request_segment_registration,
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

        let Some(registration) = resp.serialize_part.registration else {
            tracing::info!("Node {} responded with no segments to register.", node_id);
            return Ok(());
        };
        if registration.seg_map.is_empty() {
            tracing::info!("Node {} responded with an empty segment map.", node_id);
            return Ok(());
        }

        tracing::info!(
            "Received segment registration from node {}, segments: {:?}",
            node_id,
            registration.seg_map.keys()
        );

        // Now, register these segments in the master.
        match register_node_segments(
            inner.view(),
            node_id.clone(),
            expected_node_start_time,
            registration.allocation_authority,
            registration.owner_placement_class,
            registration.owner_local_target_bytes,
            registration.seg_map,
        ) {
            Ok(()) => {
                tracing::info!("Successfully registered segments for node {}", node_id);
            }
            Err(e) => {
                tracing::error!("Failed to register segments for node {}: {:?}", node_id, e);
                return Err(e);
            }
        }

        // Close the recovery-pull branch with an explicit master -> owner
        // terminal.  Preparing or sending RequestSegmentRegistrationResp on the
        // owner cannot prove that the master received and installed it; this
        // callback is emitted only after the install above has completed.
        let master_node_start_time = inner
            .view()
            .p2p_module()
            .tier_snapshot()
            .self_peer_gen
            .node_start_time;
        let confirm = call_control_plane_rpc(
            &inner.rpc_caller_confirm_node_segments_registered,
            inner.view().p2p_module(),
            node_id.clone(),
            MsgPack {
                serialize_part: ConfirmNodeSegmentsRegisteredReq {
                    owner_node_start_time: expected_node_start_time,
                    master_node_start_time,
                },
                raw_bytes: Vec::new(),
            },
            Some(Duration::from_secs(10)),
            1,
        )
        .await?;
        crate::rpcresp_kvresult_convert::try_from_code(
            confirm.serialize_part.error_code,
            confirm.serialize_part.error_json,
        )?;
        if confirm.serialize_part.accepted_owner_node_start_time != expected_node_start_time {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::OwnerStartTimeMismatch {
                    expected: expected_node_start_time,
                    got: confirm.serialize_part.accepted_owner_node_start_time,
                },
            ));
        }
        if confirm.serialize_part.accepted_master_node_start_time != master_node_start_time {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "owner acknowledged a different master generation: expected={} got={}",
                        master_node_start_time,
                        confirm.serialize_part.accepted_master_node_start_time
                    ),
                },
            ));
        }
        tracing::info!(
            owner = %node_id,
            owner_node_start_time = expected_node_start_time,
            master_node_start_time,
            "master-pull segment registration DAG terminal acknowledged by owner"
        );

        Ok(())
    }

    pub fn get_node_physical_space_size(&self, node_id: &str) -> u64 {
        self.inner()
            .node_allocators_and_tomb_tag
            .get(node_id)
            .filter(|node_segments_manager| !node_segments_manager.tomb_tag.is_tomb())
            .map(|node_segments_manager| node_segments_manager.total_size)
            .unwrap_or(0)
    }

    pub fn get_node_allocation_authority(
        &self,
        node_id: &NodeID,
    ) -> Option<SegmentAllocationAuthority> {
        self.inner()
            .node_allocators_and_tomb_tag
            .get(node_id)
            .filter(|node| !node.tomb_tag.is_tomb())
            .map(|node| node.allocation_authority)
    }

    pub fn get_owner_placement_class(&self, node_id: &str) -> Option<OwnerPlacementClass> {
        self.inner()
            .node_allocators_and_tomb_tag
            .get(node_id)
            .filter(|node| !node.tomb_tag.is_tomb())
            .map(|node| node.owner_placement_class)
    }

    pub fn update_owner_capacity_report(
        &self,
        node_id: &NodeID,
        report: OwnerCapacityReport,
    ) -> KvResult<u64> {
        let invalid = |detail: String| {
            KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail,
                },
            )
        };
        let mut node = self
            .inner()
            .node_allocators_and_tomb_tag
            .get_mut(node_id)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NodeNotFound {
                        desc: node_id.to_string(),
                    },
                )
            })?;
        if node.tomb_tag.is_tomb() || node.allocation_authority != SegmentAllocationAuthority::Owner
        {
            return Err(invalid(format!(
                "capacity report requires a live owner-authoritative node: node={}",
                node_id
            )));
        }
        if report.owner_node_start_time != node.node_start_time {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::OwnerStartTimeMismatch {
                    expected: node.node_start_time,
                    got: report.owner_node_start_time,
                },
            ));
        }
        if !report.placement_class.is_valid()
            || report.placement_class != node.owner_placement_class
        {
            return Err(invalid(format!(
                "capacity report placement class mismatch: node={} registered={:?} reported={:?}",
                node_id, node.owner_placement_class, report.placement_class
            )));
        }
        let placement_shape_valid = match report.placement_class {
            OwnerPlacementClass::Inference => {
                report.local_target_bytes != 0
                    && report.local_target_bytes <= report.physical_capacity_bytes
            }
            OwnerPlacementClass::RemoteCpu => {
                report.local_target_bytes == 0 && report.controller_epoch == 1
            }
            OwnerPlacementClass::Invalid => false,
        };
        let allocated_plus_free = report.allocated_bytes.checked_add(report.raw_free_bytes);
        if report.report_epoch == 0
            || report.controller_epoch == 0
            || report.physical_capacity_bytes != node.total_size
            || report.global_target_bytes
                != report
                    .physical_capacity_bytes
                    .saturating_sub(report.local_target_bytes)
            || allocated_plus_free != Some(report.physical_capacity_bytes)
            || report.largest_free_bytes > report.raw_free_bytes
            || report.global_accounted_bytes > report.allocated_bytes
            || !placement_shape_valid
            || (report.controller_epoch == 1
                && node.owner_local_target_bytes != Some(report.local_target_bytes))
            || (report.settled
                && (report.global_accounted_bytes > report.global_target_bytes
                    || report.local_weighted_bytes > report.local_target_bytes))
        {
            return Err(invalid(format!(
                "invalid owner capacity accounting: node={} report={:?}",
                node_id, report
            )));
        }
        let mut previous_size = 0u64;
        for size_class in &report.size_classes {
            if size_class.allocation_size_bytes == 0
                || size_class.allocation_size_bytes
                    % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES
                    != 0
                || size_class.allocation_size_bytes <= previous_size
                || size_class.allocatable_bytes > report.raw_free_bytes
                || size_class.allocatable_bytes % size_class.allocation_size_bytes != 0
            {
                return Err(invalid(format!(
                    "invalid owner capacity size class: node={} previous={} class={:?}",
                    node_id, previous_size, size_class
                )));
            }
            previous_size = size_class.allocation_size_bytes;
        }

        if let Some(existing) = node.owner_capacity_report.as_ref() {
            if report.report_epoch < existing.report.report_epoch {
                return Err(invalid(format!(
                    "stale owner capacity report: node={} current={} reported={}",
                    node_id, existing.report.report_epoch, report.report_epoch
                )));
            }
            if report.report_epoch == existing.report.report_epoch && report != existing.report {
                return Err(invalid(format!(
                    "owner capacity report epoch replay changed payload: node={} epoch={}",
                    node_id, report.report_epoch
                )));
            }
            if report.controller_epoch < existing.report.controller_epoch
                || report.controller_epoch > existing.report.controller_epoch.saturating_add(1)
                || (report.controller_epoch == existing.report.controller_epoch
                    && report.local_target_bytes != existing.report.local_target_bytes)
                || (report.placement_class == OwnerPlacementClass::RemoteCpu
                    && report.controller_epoch != existing.report.controller_epoch)
            {
                return Err(invalid(format!(
                    "owner capacity controller epoch/target transition is invalid: node={} current_epoch={} reported_epoch={} current_local_target={} reported_local_target={}",
                    node_id,
                    existing.report.controller_epoch,
                    report.controller_epoch,
                    existing.report.local_target_bytes,
                    report.local_target_bytes
                )));
            }
            if report.report_epoch == existing.report.report_epoch {
                // An identical retry is idempotent, but it does not prove that
                // the allocator snapshot is fresh. Keep the original receipt
                // time so a stuck replay cannot indefinitely evade staleness.
                return Ok(report.report_epoch);
            }
        }
        let accepted_report_epoch = report.report_epoch;
        let reported_size_classes = report
            .size_classes
            .iter()
            .map(|size_class| size_class.allocation_size_bytes)
            .collect::<Vec<_>>();
        node.owner_capacity_report = Some(StoredOwnerCapacityReport {
            report,
            received_at: Instant::now(),
        });
        drop(node);
        for allocation_size in reported_size_classes {
            self.register_owner_capacity_size_class(allocation_size);
        }
        Ok(accepted_report_epoch)
    }

    pub fn register_owner_capacity_size_class(&self, allocation_size: u64) -> bool {
        if allocation_size == 0
            || allocation_size % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
        {
            return false;
        }
        self.inner()
            .owner_capacity_size_classes
            .write()
            .insert(allocation_size)
    }

    pub fn owner_capacity_size_classes(&self) -> Vec<u64> {
        self.inner()
            .owner_capacity_size_classes
            .read()
            .iter()
            .copied()
            .collect()
    }

    pub fn get_owner_capacity_report(
        &self,
        node_id: &str,
    ) -> Option<(OwnerCapacityReport, Duration)> {
        let node = self.inner().node_allocators_and_tomb_tag.get(node_id)?;
        if node.tomb_tag.is_tomb() || node.allocation_authority != SegmentAllocationAuthority::Owner
        {
            return None;
        }
        let stored = node.owner_capacity_report.as_ref()?;
        Some((stored.report.clone(), stored.received_at.elapsed()))
    }

    pub fn get_owner_local_target_bytes(&self, node_id: &str) -> Option<u64> {
        self.inner()
            .node_allocators_and_tomb_tag
            .get(node_id)
            .filter(|node| !node.tomb_tag.is_tomb())
            .and_then(|node| node.owner_local_target_bytes)
    }

    pub fn get_node_registered_segments(
        &self,
        node_id: &NodeID,
    ) -> Option<Vec<(SegmentDeviceDescription, SegmentDeviceMemInfo)>> {
        self.inner()
            .node_allocators_and_tomb_tag
            .get(node_id)
            .filter(|node| !node.tomb_tag.is_tomb())
            .map(|node| node.registered_segments.values().cloned().collect())
    }

    pub fn validate_owner_slot_geometry(
        &self,
        node_id: &NodeID,
        allocation_id: u64,
        segment_offset: u64,
        capacity_bytes: u64,
        base_addr: u64,
        addr: u64,
    ) -> Option<NodeTombTag> {
        if allocation_id == 0
            || capacity_bytes == 0
            || segment_offset % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
            || capacity_bytes % crate::OWNER_SEGMENT_ALLOCATION_GRANULARITY_BYTES != 0
        {
            return None;
        }
        let node = self.inner().node_allocators_and_tomb_tag.get(node_id)?;
        if node.tomb_tag.is_tomb() || node.allocation_authority != SegmentAllocationAuthority::Owner
        {
            return None;
        }
        let (_, segment) = node
            .registered_segments
            .values()
            .find(|(description, segment)| {
                *description == SegmentDeviceDescription::Cpu && segment.addr == base_addr
            })?;
        let allocation_end = segment_offset.checked_add(capacity_bytes)?;
        if allocation_end > segment.len {
            return None;
        }
        let expected_addr = segment.addr.checked_add(segment_offset)?;
        (addr == expected_addr).then(|| node.tomb_tag.clone())
    }

    pub fn get_node_active_space_size(&self, node_id: &str) -> u64 {
        self.get_node_pool_capacity(node_id)
            .map(|(_, snapshot)| snapshot.active_capacity_bytes)
            .unwrap_or(0)
    }

    /// Return the exact live node generation and its shared active/parked capacity state.
    pub fn get_node_pool_capacity(&self, node_id: &str) -> Option<(i64, NodePoolCapacitySnapshot)> {
        let node = self.inner().node_allocators_and_tomb_tag.get(node_id)?;
        if node.tomb_tag.is_tomb() {
            return None;
        }
        Some((node.node_start_time, node.capacity_budget.snapshot()))
    }

    /// Update one live node generation with optimistic epoch fencing.
    pub fn set_node_active_capacity(
        &self,
        node_id: &NodeID,
        expected_node_start_time: i64,
        expected_capacity_epoch: u64,
        active_capacity_bytes: u64,
    ) -> KvResult<NodePoolCapacitySnapshot> {
        let node = self
            .inner()
            .node_allocators_and_tomb_tag
            .get(node_id)
            .ok_or_else(|| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NodeNotFound {
                        desc: node_id.to_string(),
                    },
                )
            })?;
        if node.tomb_tag.is_tomb() {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::NodeNotFound {
                    desc: format!("{} (departed generation)", node_id),
                },
            ));
        }
        if node.node_start_time != expected_node_start_time {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::OwnerStartTimeMismatch {
                    expected: expected_node_start_time,
                    got: node.node_start_time,
                },
            ));
        }
        node.capacity_budget
            .set_active_capacity(expected_capacity_epoch, active_capacity_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::master_seg_manager::msg_pack::{OwnerSizeClassCapacity, SegmentDeviceMemInfo};

    fn one_cpu_segment(
        bytes: u64,
    ) -> HashMap<SegmentDeviceID, (SegmentDeviceDescription, SegmentDeviceMemInfo)> {
        HashMap::from([(
            "cpu:0".to_string(),
            (
                SegmentDeviceDescription::Cpu,
                SegmentDeviceMemInfo {
                    addr: 0x1000,
                    len: bytes,
                },
            ),
        )])
    }

    fn manager_with_owner(
        node_id: &str,
        node_start_time: i64,
        placement_class: OwnerPlacementClass,
        local_target_bytes: u64,
        physical_capacity_bytes: u64,
    ) -> (MasterSegManager, NodeID) {
        let manager = MasterSegManager(MasterSegManagerInner {
            view: std::sync::OnceLock::new(),
            node_allocators_and_tomb_tag: DashMap::new(),
            owner_capacity_size_classes: parking_lot::RwLock::new(BTreeSet::new()),
            rpc_caller_request_segment_registration: RPCCaller::new(),
            rpc_caller_confirm_node_segments_registered: RPCCaller::new(),
        });
        let node_id: NodeID = node_id.to_string().into();
        manager.inner().node_allocators_and_tomb_tag.insert(
            node_id.clone(),
            build_node_segments_manager(
                node_start_time,
                SegmentAllocationAuthority::Owner,
                placement_class,
                Some(local_target_bytes),
                one_cpu_segment(physical_capacity_bytes),
            )
            .unwrap(),
        );
        (manager, node_id)
    }

    fn valid_capacity_report(
        node_start_time: i64,
        placement_class: OwnerPlacementClass,
        report_epoch: u64,
        controller_epoch: u64,
        physical_capacity_bytes: u64,
        local_target_bytes: u64,
    ) -> OwnerCapacityReport {
        OwnerCapacityReport {
            owner_node_start_time: node_start_time,
            placement_class,
            controller_epoch,
            report_epoch,
            physical_capacity_bytes,
            local_target_bytes,
            global_target_bytes: physical_capacity_bytes - local_target_bytes,
            allocated_bytes: 0,
            raw_free_bytes: physical_capacity_bytes,
            largest_free_bytes: physical_capacity_bytes,
            global_accounted_bytes: 0,
            local_weighted_bytes: 0,
            settled: true,
            size_classes: vec![OwnerSizeClassCapacity {
                allocation_size_bytes: 4 * 1024,
                allocatable_bytes: physical_capacity_bytes,
            }],
        }
    }

    #[test]
    fn segment_capacity_snapshot_combines_authorities_and_counts_reserved_owner_bytes() {
        let capacity = 32 * 1024;
        let (manager, owner_id) = manager_with_owner(
            "owner-node",
            17,
            OwnerPlacementClass::Inference,
            16 * 1024,
            capacity,
        );
        assert!(manager.snapshot_segment_capacity().is_empty());
        let master_id: NodeID = "master-allocated-node".to_string().into();
        manager.inner().node_allocators_and_tomb_tag.insert(
            master_id.clone(),
            build_node_segments_manager(
                19,
                SegmentAllocationAuthority::Master,
                OwnerPlacementClass::Invalid,
                None,
                one_cpu_segment(capacity),
            )
            .unwrap(),
        );
        let allocator = manager.get_node_allocators(&master_id).pop().unwrap();
        let allocation = allocator.allocate(1).unwrap();

        let mut report = valid_capacity_report(
            17,
            OwnerPlacementClass::Inference,
            1,
            1,
            capacity,
            16 * 1024,
        );
        // Physical reservations reduce free space before any cache entry is visible.
        report.allocated_bytes = 8 * 1024;
        report.raw_free_bytes = capacity - report.allocated_bytes;
        report.largest_free_bytes = report.raw_free_bytes;
        report.size_classes[0].allocatable_bytes = report.raw_free_bytes;
        assert_eq!(report.local_weighted_bytes, 0);
        assert_eq!(report.global_accounted_bytes, 0);
        manager.update_owner_capacity_report(&owner_id, report).unwrap();

        let mut snapshots = manager.snapshot_segment_capacity();
        snapshots.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        assert_eq!(
            snapshots,
            vec![
                SegmentCapacitySnapshot {
                    node_id: master_id.clone(),
                    device_id: "cpu:0".to_string(),
                    available_bytes: capacity - 4 * 1024,
                    total_bytes: capacity,
                },
                SegmentCapacitySnapshot {
                    node_id: owner_id.clone(),
                    device_id: "cpu:0".to_string(),
                    available_bytes: capacity - 8 * 1024,
                    total_bytes: capacity,
                },
            ]
        );
        assert!(manager.get_node_allocators(&owner_id).is_empty());

        drop(allocation);
        manager
            .update_owner_capacity_report(
                &owner_id,
                valid_capacity_report(
                    17,
                    OwnerPlacementClass::Inference,
                    2,
                    1,
                    capacity,
                    16 * 1024,
                ),
            )
            .unwrap();
        let snapshots = manager.snapshot_segment_capacity();
        assert_eq!(snapshots.len(), 2);
        assert!(snapshots.iter().all(|segment| {
            segment.available_bytes == capacity && segment.total_bytes == capacity
        }));
    }

    #[test]
    fn segment_capacity_snapshot_excludes_tombs_and_waits_for_each_generations_report() {
        let (manager, node_id) = manager_with_owner(
            "owner-node",
            17,
            OwnerPlacementClass::RemoteCpu,
            0,
            32 * 1024,
        );
        let old_report = valid_capacity_report(
            17,
            OwnerPlacementClass::RemoteCpu,
            1,
            1,
            32 * 1024,
            0,
        );
        manager
            .update_owner_capacity_report(&node_id, old_report.clone())
            .unwrap();
        assert_eq!(manager.snapshot_segment_capacity().len(), 1);
        manager.mark_node_tomb_generation(&node_id, Some(17)).unwrap();
        assert!(manager.snapshot_segment_capacity().is_empty());
        assert!(!manager.inner().node_allocators_and_tomb_tag.contains_key(&node_id));

        manager.inner().node_allocators_and_tomb_tag.insert(
            node_id.clone(),
            build_node_segments_manager(
                18,
                SegmentAllocationAuthority::Owner,
                OwnerPlacementClass::RemoteCpu,
                Some(0),
                one_cpu_segment(16 * 1024),
            )
            .unwrap(),
        );
        assert!(manager.snapshot_segment_capacity().is_empty());
        assert!(manager.update_owner_capacity_report(&node_id, old_report).is_err());
        assert!(manager.snapshot_segment_capacity().is_empty());
        manager
            .update_owner_capacity_report(
                &node_id,
                valid_capacity_report(18, OwnerPlacementClass::RemoteCpu, 1, 1, 16 * 1024, 0),
            )
            .unwrap();
        assert_eq!(
            manager.snapshot_segment_capacity(),
            vec![SegmentCapacitySnapshot {
                node_id,
                device_id: "cpu:0".to_string(),
                available_bytes: 16 * 1024,
                total_bytes: 16 * 1024,
            }]
        );
    }

    #[test]
    fn one_node_generation_shares_one_budget_across_registered_segments() {
        let manager = build_node_segments_manager(
            17,
            SegmentAllocationAuthority::Master,
            OwnerPlacementClass::Invalid,
            None,
            HashMap::from([
                (
                    "cpu0".to_string(),
                    (
                        SegmentDeviceDescription::Cpu,
                        SegmentDeviceMemInfo {
                            addr: 0,
                            len: 8 * 1024,
                        },
                    ),
                ),
                (
                    "cpu1".to_string(),
                    (
                        SegmentDeviceDescription::Cpu,
                        SegmentDeviceMemInfo {
                            addr: 8 * 1024,
                            len: 8 * 1024,
                        },
                    ),
                ),
            ]),
        )
        .unwrap();
        let initial = manager.capacity_budget.snapshot();
        assert_eq!(initial.physical_capacity_bytes, 16 * 1024);
        assert_eq!(initial.active_capacity_bytes, 16 * 1024);

        let allocators = manager
            .device_id_2_allocator
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let _first = allocators[0].allocate(8 * 1024).unwrap();
        let _second = allocators[1].allocate(8 * 1024).unwrap();
        assert_eq!(
            manager.capacity_budget.snapshot().used_capacity_bytes,
            16 * 1024
        );
        assert!(allocators[0].allocate(1).is_err());
    }

    #[test]
    fn owner_authority_registers_one_complete_segment_without_master_allocator() {
        let manager = build_node_segments_manager(
            17,
            SegmentAllocationAuthority::Owner,
            OwnerPlacementClass::Inference,
            Some(12 * 1024),
            one_cpu_segment(16 * 1024),
        )
        .unwrap();

        assert_eq!(
            manager.allocation_authority,
            SegmentAllocationAuthority::Owner
        );
        assert_eq!(manager.owner_local_target_bytes, Some(12 * 1024));
        assert_eq!(manager.registered_segments.len(), 1);
        assert!(manager.device_id_2_allocator.is_empty());
        let capacity = manager.capacity_budget.snapshot();
        assert_eq!(capacity.physical_capacity_bytes, 16 * 1024);
        assert_eq!(capacity.active_capacity_bytes, 16 * 1024);
        assert_eq!(capacity.used_capacity_bytes, 0);
    }

    #[test]
    fn inference_owner_accepts_an_exact_full_local_target() {
        let physical_capacity_bytes = 16 * 1024;
        let manager = build_node_segments_manager(
            17,
            SegmentAllocationAuthority::Owner,
            OwnerPlacementClass::Inference,
            Some(physical_capacity_bytes),
            one_cpu_segment(physical_capacity_bytes),
        )
        .unwrap();

        assert_eq!(
            manager.owner_local_target_bytes,
            Some(physical_capacity_bytes)
        );
        assert!(manager.device_id_2_allocator.is_empty());
    }

    #[test]
    fn inference_capacity_report_accepts_an_exact_full_local_target() {
        let physical_capacity_bytes = 32 * 1024;
        let (manager, node_id) = manager_with_owner(
            "inference-full-local",
            17,
            OwnerPlacementClass::Inference,
            physical_capacity_bytes,
            physical_capacity_bytes,
        );
        let report = valid_capacity_report(
            17,
            OwnerPlacementClass::Inference,
            1,
            1,
            physical_capacity_bytes,
            physical_capacity_bytes,
        );

        assert_eq!(
            manager
                .update_owner_capacity_report(&node_id, report)
                .unwrap(),
            1
        );
    }

    #[test]
    fn remote_cpu_owner_registers_zero_local_target_without_master_allocator() {
        let manager = build_node_segments_manager(
            23,
            SegmentAllocationAuthority::Owner,
            OwnerPlacementClass::RemoteCpu,
            Some(0),
            one_cpu_segment(32 * 1024),
        )
        .unwrap();

        assert_eq!(
            manager.owner_placement_class,
            OwnerPlacementClass::RemoteCpu
        );
        assert_eq!(manager.owner_local_target_bytes, Some(0));
        assert!(manager.device_id_2_allocator.is_empty());
        assert!(
            build_node_segments_manager(
                23,
                SegmentAllocationAuthority::Owner,
                OwnerPlacementClass::RemoteCpu,
                Some(4 * 1024),
                one_cpu_segment(32 * 1024),
            )
            .is_err()
        );
    }

    #[test]
    fn remote_cpu_capacity_report_is_generation_and_controller_fenced() {
        let (manager, node_id) = manager_with_owner(
            "remote-cpu",
            23,
            OwnerPlacementClass::RemoteCpu,
            0,
            32 * 1024,
        );
        let report = valid_capacity_report(23, OwnerPlacementClass::RemoteCpu, 1, 1, 32 * 1024, 0);
        assert_eq!(
            manager
                .update_owner_capacity_report(&node_id, report.clone())
                .unwrap(),
            1
        );
        let first_received_at = manager
            .inner()
            .node_allocators_and_tomb_tag
            .get(&node_id)
            .unwrap()
            .owner_capacity_report
            .as_ref()
            .unwrap()
            .received_at;

        assert_eq!(
            manager
                .update_owner_capacity_report(&node_id, report.clone())
                .unwrap(),
            1
        );
        let replay_received_at = manager
            .inner()
            .node_allocators_and_tomb_tag
            .get(&node_id)
            .unwrap()
            .owner_capacity_report
            .as_ref()
            .unwrap()
            .received_at;
        assert_eq!(first_received_at, replay_received_at);

        let mut changed_controller = report.clone();
        changed_controller.report_epoch = 2;
        changed_controller.controller_epoch = 2;
        assert!(
            manager
                .update_owner_capacity_report(&node_id, changed_controller)
                .is_err()
        );

        let mut changed_local_target = report;
        changed_local_target.report_epoch = 2;
        changed_local_target.local_target_bytes = 4 * 1024;
        changed_local_target.global_target_bytes = 28 * 1024;
        assert!(
            manager
                .update_owner_capacity_report(&node_id, changed_local_target)
                .is_err()
        );
    }

    #[test]
    fn inference_capacity_target_changes_only_with_next_controller_epoch() {
        let (manager, node_id) = manager_with_owner(
            "inference",
            17,
            OwnerPlacementClass::Inference,
            8 * 1024,
            32 * 1024,
        );
        let first = valid_capacity_report(
            17,
            OwnerPlacementClass::Inference,
            1,
            1,
            32 * 1024,
            8 * 1024,
        );
        assert_eq!(
            manager
                .update_owner_capacity_report(&node_id, first.clone())
                .unwrap(),
            1
        );

        let mut same_controller_changed_target = first.clone();
        same_controller_changed_target.report_epoch = 2;
        same_controller_changed_target.local_target_bytes = 12 * 1024;
        same_controller_changed_target.global_target_bytes = 20 * 1024;
        assert!(
            manager
                .update_owner_capacity_report(&node_id, same_controller_changed_target)
                .is_err()
        );

        let mut next_controller = first;
        next_controller.report_epoch = 2;
        next_controller.controller_epoch = 2;
        next_controller.local_target_bytes = 12 * 1024;
        next_controller.global_target_bytes = 20 * 1024;
        assert_eq!(
            manager
                .update_owner_capacity_report(&node_id, next_controller)
                .unwrap(),
            2
        );
    }

    #[test]
    fn capacity_size_class_interest_is_sorted_and_report_driven() {
        let (manager, node_id) = manager_with_owner(
            "remote-cpu",
            23,
            OwnerPlacementClass::RemoteCpu,
            0,
            32 * 1024,
        );
        assert!(manager.register_owner_capacity_size_class(8 * 1024));
        assert!(!manager.register_owner_capacity_size_class(8 * 1024));
        assert!(!manager.register_owner_capacity_size_class(123));

        let report = valid_capacity_report(23, OwnerPlacementClass::RemoteCpu, 1, 1, 32 * 1024, 0);
        assert_eq!(
            manager
                .update_owner_capacity_report(&node_id, report)
                .unwrap(),
            1
        );
        assert_eq!(
            manager.owner_capacity_size_classes(),
            vec![4 * 1024, 8 * 1024]
        );
    }

    #[test]
    fn owner_registration_target_and_generation_are_replay_safe() {
        let manager = build_node_segments_manager(
            17,
            SegmentAllocationAuthority::Owner,
            OwnerPlacementClass::Inference,
            Some(12 * 1024),
            one_cpu_segment(16 * 1024),
        )
        .unwrap();
        let node: NodeID = "owner0".to_string().into();

        assert!(
            validate_live_registration_identity(
                &node,
                &manager,
                17,
                SegmentAllocationAuthority::Owner,
                OwnerPlacementClass::Inference,
                Some(12 * 1024),
            )
            .is_ok()
        );
        assert!(
            validate_live_registration_identity(
                &node,
                &manager,
                18,
                SegmentAllocationAuthority::Owner,
                OwnerPlacementClass::Inference,
                Some(12 * 1024),
            )
            .is_err()
        );
        assert!(
            validate_live_registration_identity(
                &node,
                &manager,
                17,
                SegmentAllocationAuthority::Master,
                OwnerPlacementClass::Invalid,
                None,
            )
            .is_err()
        );
        assert!(
            validate_live_registration_identity(
                &node,
                &manager,
                17,
                SegmentAllocationAuthority::Owner,
                OwnerPlacementClass::Inference,
                Some(8 * 1024),
            )
            .is_err()
        );

        assert!(
            build_node_segments_manager(
                17,
                SegmentAllocationAuthority::Owner,
                OwnerPlacementClass::Inference,
                None,
                one_cpu_segment(16 * 1024),
            )
            .is_err()
        );
        assert!(
            build_node_segments_manager(
                17,
                SegmentAllocationAuthority::Master,
                OwnerPlacementClass::Invalid,
                Some(8 * 1024),
                one_cpu_segment(16 * 1024),
            )
            .is_err()
        );
    }
}
