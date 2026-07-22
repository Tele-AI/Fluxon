// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::ClientSegPoolAccessTrait;
use crate::client_seg_pool::{ClientCpuMemReadGuard, ClientSegPool};
use crate::cluster_manager::{ClusterManager, NodeID, NodeIDString, NodeRole};
use crate::p2p::p2p_module::P2pModule;
use crate::rpcresp_kvresult_convert::msg_and_error::{KvError, KvResult};
use crate::{P2pModuleAccessTrait, cluster_manager::ClusterManagerAccessTrait};
use async_trait::async_trait;
use fluxon_commu::ClosedRuntimeHandle;
use fluxon_commu::p2p::PeerGen;
use fluxon_commu::transfer_engine::{
    AttachedTransferEngine, CLOSED_RUNTIME_DIRECT_FAST_PATH_NOT_READY_MARKER,
    ClosedRuntimeLocalMemoryKind,
};
use fluxon_commu::{
    ClientTransferEngineClusterRuntime, ClientTransferEngineCore, ClientTransferEngineRuntime,
    CpuAllocatedMem, TransferBreakdown,
};
use fluxon_framework::{LogicalModule, define_module};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub use fluxon_commu::{ClientTransferEngineNewArg, ClientTransferEngineRuntimeConfig};

// P2P-based raw memory transfer RPC; used only when engine type is explicitly P2p.
mod p2p_transfer_rpc;

define_module!(
    ClientTransferEngine,
    (p2p, P2pModule),
    (cluster_manager, ClusterManager),
    (client_seg_pool, ClientSegPool)
);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GpuMemoryRegistration {
    pub registration_id: u64,
    pub addr: u64,
    pub len: u64,
    pub device_id: u32,
}

impl GpuMemoryRegistration {
    fn contains(&self, addr: u64, len: u64) -> bool {
        if len == 0 {
            return addr >= self.addr
                && self
                    .addr
                    .checked_add(self.len)
                    .is_some_and(|end| addr <= end);
        }
        let Some(end) = addr.checked_add(len) else {
            return false;
        };
        let Some(registration_end) = self.addr.checked_add(self.len) else {
            return false;
        };
        addr >= self.addr && end <= registration_end
    }
}

#[derive(Clone, Debug)]
pub struct GpuMemoryGuard {
    registration: Arc<GpuMemoryRegistration>,
}

impl GpuMemoryGuard {
    pub fn registration(&self) -> &GpuMemoryRegistration {
        self.registration.as_ref()
    }

    fn contains(&self, addr: u64, len: u64) -> bool {
        self.registration.contains(addr, len)
    }
}

pub enum ClientTransferMemoryGuard {
    Cpu(ClientCpuMemReadGuard),
    Gpu(GpuMemoryGuard),
}

#[derive(Debug)]
enum GpuMemoryRegistryState {
    Empty,
    Registering(GpuMemoryRegistration),
    Active(Arc<GpuMemoryRegistration>),
    Unregistering(GpuMemoryRegistration),
}

#[derive(Debug)]
struct GpuMemoryRegistry {
    next_registration_id: AtomicU64,
    state: Mutex<GpuMemoryRegistryState>,
}

impl GpuMemoryRegistry {
    fn new() -> Self {
        Self {
            next_registration_id: AtomicU64::new(1),
            state: Mutex::new(GpuMemoryRegistryState::Empty),
        }
    }

    fn begin_register(
        &self,
        addr: u64,
        len: u64,
        device_id: u32,
    ) -> Result<GpuMemoryRegistration, String> {
        if addr == 0 {
            return Err("GPU registration address must be non-zero".to_string());
        }
        if len == 0 {
            return Err("GPU registration length must be non-zero".to_string());
        }
        if addr.checked_add(len).is_none() {
            return Err(format!(
                "GPU registration range overflows: addr={:#x} len={}",
                addr, len
            ));
        }
        let registration_id = self.next_registration_id.fetch_add(1, Ordering::Relaxed);
        let registration = GpuMemoryRegistration {
            registration_id,
            addr,
            len,
            device_id,
        };
        let mut state = self.state.lock().expect("gpu memory registry poisoned");
        match &*state {
            GpuMemoryRegistryState::Empty => {
                *state = GpuMemoryRegistryState::Registering(registration.clone());
                Ok(registration)
            }
            current => Err(format!(
                "one GPU registration is already active or changing: {current:?}"
            )),
        }
    }

    fn finish_register(&self, registration_id: u64, success: bool) {
        let mut state = self.state.lock().expect("gpu memory registry poisoned");
        let previous = std::mem::replace(&mut *state, GpuMemoryRegistryState::Empty);
        match previous {
            GpuMemoryRegistryState::Registering(registration)
                if registration.registration_id == registration_id =>
            {
                if success {
                    *state = GpuMemoryRegistryState::Active(Arc::new(registration));
                }
            }
            other => panic!(
                "GPU registration completion does not match registry state: id={} state={other:?}",
                registration_id
            ),
        }
    }

    fn begin_unregister(&self, registration_id: u64) -> Result<GpuMemoryRegistration, String> {
        let mut state = self.state.lock().expect("gpu memory registry poisoned");
        let previous = std::mem::replace(&mut *state, GpuMemoryRegistryState::Empty);
        match previous {
            GpuMemoryRegistryState::Active(registration) => {
                if registration.registration_id != registration_id {
                    let actual = registration.registration_id;
                    *state = GpuMemoryRegistryState::Active(registration);
                    return Err(format!(
                        "GPU registration id mismatch: expected={} got={}",
                        actual, registration_id
                    ));
                }
                let guard_count = Arc::strong_count(&registration).saturating_sub(1);
                if guard_count != 0 {
                    *state = GpuMemoryRegistryState::Active(registration);
                    return Err(format!(
                        "GPU registration is busy: registration_id={} active_guards={}",
                        registration_id, guard_count
                    ));
                }
                let descriptor = registration.as_ref().clone();
                *state = GpuMemoryRegistryState::Unregistering(descriptor.clone());
                Ok(descriptor)
            }
            other => {
                *state = other;
                Err(format!(
                    "GPU registration is not active: registration_id={}",
                    registration_id
                ))
            }
        }
    }

    fn finish_unregister(&self, registration_id: u64, success: bool) {
        let mut state = self.state.lock().expect("gpu memory registry poisoned");
        let previous = std::mem::replace(&mut *state, GpuMemoryRegistryState::Empty);
        match previous {
            GpuMemoryRegistryState::Unregistering(registration)
                if registration.registration_id == registration_id =>
            {
                if !success {
                    *state = GpuMemoryRegistryState::Active(Arc::new(registration));
                }
            }
            other => panic!(
                "GPU unregistration completion does not match registry state: id={} state={other:?}",
                registration_id
            ),
        }
    }

    fn guard_for_range(&self, addr: u64, len: u64) -> Option<GpuMemoryGuard> {
        let state = self.state.lock().expect("gpu memory registry poisoned");
        match &*state {
            GpuMemoryRegistryState::Active(registration) if registration.contains(addr, len) => {
                Some(GpuMemoryGuard {
                    registration: registration.clone(),
                })
            }
            _ => None,
        }
    }

    fn validate_destination(
        &self,
        registration_id: u64,
        addr: u64,
        capacity: u64,
    ) -> Result<GpuMemoryGuard, String> {
        let guard = self.guard_for_range(addr, capacity).ok_or_else(|| {
            format!(
                "GPU destination is outside the active registration: registration_id={} addr={:#x} capacity={}",
                registration_id, addr, capacity
            )
        })?;
        if guard.registration().registration_id != registration_id {
            return Err(format!(
                "GPU destination registration id mismatch: expected={} got={}",
                guard.registration().registration_id,
                registration_id
            ));
        }
        Ok(guard)
    }
}

#[derive(Clone)]
struct ClientTransferRuntimeAdapter {
    view: ClientTransferEngineView,
    gpu_memory_registry: Arc<GpuMemoryRegistry>,
}

impl ClientTransferRuntimeAdapter {
    fn local_segment_transfer_enabled(&self) -> bool {
        let self_info = self.view.cluster_manager().get_self_info();
        matches!(self_info.node_role(), NodeRole::Client | NodeRole::External)
            || self_info
                .metadata
                .get("side_transfer_worker")
                .is_some_and(|v| v == "true")
    }
}

#[async_trait]
impl ClientTransferEngineClusterRuntime for ClientTransferRuntimeAdapter {
    fn cluster_name(&self) -> &str {
        self.view.cluster_manager().cluster_name()
    }

    fn self_member_id(&self) -> &str {
        self.view.cluster_manager().self_member_id()
    }

    fn get_self_info(&self) -> crate::cluster_manager::ClusterMember {
        self.view.cluster_manager().get_self_info()
    }

    fn get_member_info_cached(
        &self,
        member_id: &str,
    ) -> Option<crate::cluster_manager::ClusterMember> {
        self.view
            .cluster_manager()
            .get_member_info_cached(member_id)
    }

    fn listen(&self) -> limit_thirdparty::tokio::sync::abroadcast::Receiver<crate::ClusterEvent> {
        self.view.cluster_manager().listen()
    }

    fn set_self_rdma_transfer_engine_runtime(
        &self,
        runtime: fluxon_commu::MemberRdmaTransferEngineRuntime,
    ) {
        self.view
            .cluster_manager()
            .set_self_rdma_transfer_engine_runtime(runtime);
    }

    async fn wait_accessible_self_ip_for_current_start_time(&self) -> Result<String, String> {
        self.view
            .cluster_manager()
            .wait_accessible_self_ip_for_current_start_time()
            .await
            .map_err(|err| err.to_string())
    }

    async fn fetch_transfer_ready_for_member(
        &self,
        member_id: &str,
    ) -> Result<Option<fluxon_commu::TransferReadyInfo>, String> {
        self.view
            .cluster_manager()
            .fetch_transfer_ready_for_member(member_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn publish_self_transfer_ready(
        &self,
        backend_epoch: u64,
    ) -> Result<fluxon_commu::TransferReadyInfo, String> {
        self.view
            .cluster_manager()
            .publish_self_transfer_ready(backend_epoch)
            .await
            .map_err(|err| err.to_string())
    }

    async fn set_self_transfer_backend_epoch(&self, backend_epoch: u64) -> Result<(), String> {
        self.view
            .cluster_manager()
            .set_self_transfer_backend_epoch(backend_epoch)
            .await
            .map_err(|err| err.to_string())
    }

    async fn clear_self_transfer_backend_epoch(&self) -> Result<(), String> {
        self.view
            .cluster_manager()
            .clear_self_transfer_backend_epoch()
            .await
            .map_err(|err| err.to_string())
    }

    fn try_report_transfer_link_te(
        &self,
        from: NodeIDString,
        to: NodeIDString,
        record: fluxon_commu::TransferLinkRecord,
    ) -> Result<(), String> {
        self.view
            .cluster_manager()
            .try_report_transfer_link_te(from, to, record)
            .map_err(|err| err.to_string())
    }
}

#[async_trait]
impl ClientTransferEngineRuntime for ClientTransferRuntimeAdapter {
    type LocalSegmentGuard = ClientTransferMemoryGuard;

    fn supports_local_segment_transfer(&self) -> bool {
        self.local_segment_transfer_enabled()
    }

    fn cluster_runtime(&self) -> &dyn ClientTransferEngineClusterRuntime {
        self
    }

    fn spawn<F, N>(&self, name: N, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
        N: Into<String>,
    {
        let _ = self.view.spawn(name, fut);
    }

    fn register_shutdown_waiter(&self) -> fluxon_framework_compiled::shutdown::ShutdownWaiter {
        self.view.register_shutdown_waiter()
    }

    async fn ensure_local_segment_guard(
        &self,
        local_addr: u64,
        seg_guard: Option<ClientTransferMemoryGuard>,
    ) -> Result<ClientTransferMemoryGuard, String> {
        if !self.local_segment_transfer_enabled() {
            return Err("local segment transfer is not supported on this node role".to_string());
        }
        if let Some(guard) = seg_guard {
            return match guard {
                ClientTransferMemoryGuard::Cpu(cpu_guard) => {
                    p2p_transfer_rpc::ensure_local_segment_guard(
                        &self.view,
                        local_addr,
                        Some(cpu_guard),
                    )
                    .await
                    .map(ClientTransferMemoryGuard::Cpu)
                }
                ClientTransferMemoryGuard::Gpu(gpu_guard) => {
                    if gpu_guard.contains(local_addr, 1) {
                        Ok(ClientTransferMemoryGuard::Gpu(gpu_guard))
                    } else {
                        Err(format!(
                            "GPU guard does not cover local address: addr={:#x}",
                            local_addr
                        ))
                    }
                }
            };
        }
        if let Some(gpu_guard) = self.gpu_memory_registry.guard_for_range(local_addr, 1) {
            return Ok(ClientTransferMemoryGuard::Gpu(gpu_guard));
        }
        p2p_transfer_rpc::ensure_local_segment_guard(&self.view, local_addr, None)
            .await
            .map(ClientTransferMemoryGuard::Cpu)
    }

    fn register_p2p_transfer_rpc(&self) {
        if !self.local_segment_transfer_enabled() {
            return;
        }
        p2p_transfer_rpc::register_transfer_rpc(&self.view);
    }

    async fn attach_transfer_engine(
        &self,
        transfer_engine: AttachedTransferEngine,
    ) -> Result<(), String> {
        self.view
            .p2p_module()
            .attach_transfer_engine(transfer_engine)
            .await
            .map_err(|err| err.to_string())
    }

    fn notify_transfer_rpc_backend_ready(&self) {
        self.view
            .p2p_module()
            .emit_transfer_rpc_backend_ready_for_runtime();
    }

    fn notify_transfer_rpc_backend_lost(&self, detail: String) {
        self.view
            .p2p_module()
            .emit_transfer_rpc_backend_lost_for_runtime(detail);
    }

    fn notify_transfer_rpc_peer_ready(&self, peer_gen: PeerGen, peer_transfer_backend_epoch: u64) {
        self.view
            .p2p_module()
            .emit_transfer_rpc_peer_ready_for_runtime(peer_gen, peer_transfer_backend_epoch);
    }

    async fn p2p_read_to_local(
        &self,
        peer: NodeIDString,
        remote_src: u64,
        local_target: u64,
        len: u64,
        seg_guard: ClientTransferMemoryGuard,
    ) -> Result<(), String> {
        if !self.local_segment_transfer_enabled() {
            return Err("p2p raw-memory read is not supported on this node role".to_string());
        }
        match seg_guard {
            ClientTransferMemoryGuard::Cpu(cpu_guard) => {
                p2p_transfer_rpc::p2p_read_to_local(
                    &self.view,
                    peer,
                    remote_src,
                    local_target,
                    len,
                    cpu_guard,
                )
                .await
            }
            ClientTransferMemoryGuard::Gpu(_) => Err(
                "GPU destination requires the RDMA fast path; P2P fallback is disabled".to_string(),
            ),
        }
    }

    async fn p2p_write_from_local(
        &self,
        peer: NodeIDString,
        local_src: u64,
        remote_target: u64,
        len: u64,
        copy_from: Option<Pin<&[u8]>>,
        seg_guard: ClientTransferMemoryGuard,
    ) -> Result<(), String> {
        if !self.local_segment_transfer_enabled() {
            return Err("p2p raw-memory write is not supported on this node role".to_string());
        }
        match seg_guard {
            ClientTransferMemoryGuard::Cpu(cpu_guard) => {
                p2p_transfer_rpc::p2p_write_from_local(
                    &self.view,
                    peer,
                    local_src,
                    remote_target,
                    len,
                    copy_from,
                    cpu_guard,
                )
                .await
            }
            ClientTransferMemoryGuard::Gpu(_) => {
                Err("GPU source requires the RDMA fast path; P2P fallback is disabled".to_string())
            }
        }
    }

    fn try_record_local_ipc_bytes_for_owner_topology(
        &self,
        logical_peer: &NodeID,
        direction: &'static str,
        bytes: u64,
    ) -> bool {
        self.view
            .p2p_module()
            .try_record_local_ipc_bytes_for_owner_topology(logical_peer, direction, bytes)
    }

    fn record_peer_network_bytes(
        &self,
        logical_peer: &NodeID,
        direction: &'static str,
        bytes: u64,
    ) {
        let _ = (logical_peer, direction, bytes);
    }

    async fn closed_sdk_runtime_handles(
        &self,
    ) -> Result<(ClosedRuntimeHandle, ClosedRuntimeHandle), String> {
        let cluster_manager = self.view.cluster_manager().closed_runtime_handle();
        let p2p_module = self
            .view
            .p2p_module()
            .ensure_closed_runtime_handle()
            .await
            .map_err(|err| err.to_string())?;
        Ok((cluster_manager, p2p_module))
    }
}

pub struct ClientTransferEngine {
    view: OnceLock<ClientTransferEngineView>,
    core: ClientTransferEngineCore,
    gpu_memory_registry: Arc<GpuMemoryRegistry>,
}

impl ClientTransferEngine {
    fn runtime(&self) -> ClientTransferRuntimeAdapter {
        ClientTransferRuntimeAdapter {
            view: self.view.get().unwrap().clone(),
            gpu_memory_registry: self.gpu_memory_registry.clone(),
        }
    }

    pub fn attach_view(&self, view: ClientTransferEngineView) {
        let shutdown_poller = view.register_shutdown_poller();
        self.view
            .set(view)
            .unwrap_or_else(|_| panic!("ClientTransferEngine view attached twice"));
        self.core.attach_shutdown_poller(shutdown_poller);
    }

    pub async fn construct(arg: ClientTransferEngineNewArg) -> Result<Self, KvError> {
        tracing::info!("Constructing ClientTransferEngine (PreView)");
        let core = ClientTransferEngineCore::construct(arg)
            .await
            .map_err(KvError::from)?;
        Ok(Self {
            view: OnceLock::new(),
            core,
            gpu_memory_registry: Arc::new(GpuMemoryRegistry::new()),
        })
    }

    pub async fn init2_for_init_dag(&self) -> Result<(), KvError> {
        self.core
            .init2_for_init_dag(self.runtime())
            .await
            .map_err(KvError::from)
    }

    pub async fn close(&self) {
        self.core.close().await;
    }

    pub async fn current_runtime_config(&self) -> ClientTransferEngineRuntimeConfig {
        self.core.current_runtime_config().await
    }

    pub async fn update_runtime_config(&self, config: ClientTransferEngineRuntimeConfig) {
        self.core.update_runtime_config(config).await;
    }

    pub async fn register_local_segment(&self, cpu_mem: &CpuAllocatedMem) -> KvResult<()> {
        self.core
            .register_local_segment(self.runtime(), cpu_mem)
            .await
            .map_err(KvError::from)
    }

    pub async fn unregister_local_segment(&self, cpu_mem: &CpuAllocatedMem) -> KvResult<()> {
        self.core
            .unregister_local_segment(cpu_mem)
            .await
            .map_err(KvError::from)
    }

    pub async fn register_gpu_memory(
        &self,
        addr: u64,
        len: u64,
        device_id: u32,
    ) -> KvResult<GpuMemoryRegistration> {
        let registration = self
            .gpu_memory_registry
            .begin_register(addr, len, device_id)
            .map_err(|detail| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                        detail,
                    },
                )
            })?;
        let result = self
            .core
            .register_local_memory(
                self.runtime(),
                addr,
                len,
                ClosedRuntimeLocalMemoryKind::Gpu { device_id },
            )
            .await
            .map_err(KvError::from);
        self.gpu_memory_registry
            .finish_register(registration.registration_id, result.is_ok());
        result.map(|()| registration)
    }

    pub async fn unregister_gpu_memory(&self, registration_id: u64) -> KvResult<()> {
        let registration = self
            .gpu_memory_registry
            .begin_unregister(registration_id)
            .map_err(|detail| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                        detail,
                    },
                )
            })?;
        let result = self
            .core
            .unregister_local_memory(
                registration.addr,
                registration.len,
                ClosedRuntimeLocalMemoryKind::Gpu {
                    device_id: registration.device_id,
                },
            )
            .await
            .map_err(KvError::from);
        self.gpu_memory_registry
            .finish_unregister(registration_id, result.is_ok());
        result
    }

    pub fn validate_gpu_destination(
        &self,
        registration_id: u64,
        addr: u64,
        capacity: u64,
    ) -> KvResult<GpuMemoryGuard> {
        self.gpu_memory_registry
            .validate_destination(registration_id, addr, capacity)
            .map_err(|detail| {
                KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                        detail,
                    },
                )
            })
    }

    pub async fn write_data(
        &self,
        data: Pin<&[u8]>,
        src_addr: u64,
        target_addr: u64,
        peer_id: Option<NodeIDString>,
        do_copy: bool,
        seg_guard: Option<ClientCpuMemReadGuard>,
    ) -> KvResult<TransferBreakdown> {
        self.core
            .write_data(
                self.runtime(),
                data,
                src_addr,
                target_addr,
                peer_id,
                do_copy,
                seg_guard.map(ClientTransferMemoryGuard::Cpu),
            )
            .await
            .map_err(KvError::from)
    }

    pub async fn transfer_data_no_copy(
        &self,
        peer_node: Option<NodeIDString>,
        peer_src_or_target: bool,
        src_addr: u64,
        target_addr: u64,
        len: u64,
        seg_guard: Option<ClientCpuMemReadGuard>,
    ) -> KvResult<TransferBreakdown> {
        self.core
            .transfer_data_no_copy(
                self.runtime(),
                peer_node,
                peer_src_or_target,
                src_addr,
                target_addr,
                len,
                seg_guard.map(ClientTransferMemoryGuard::Cpu),
                false,
            )
            .await
            .map_err(KvError::from)
    }

    /// Pull remote bytes into an explicitly validated caller-owned GPU range.
    ///
    /// Keeping this separate from the CPU entry point prevents a CUDA virtual
    /// address from silently falling through CPU/P2P guard discovery.  The
    /// guard is moved into the transfer engine and retains the exact
    /// registration generation until the backend releases the local segment
    /// lease.
    pub async fn transfer_data_no_copy_to_gpu(
        &self,
        peer_node: NodeIDString,
        src_addr: u64,
        target_addr: u64,
        len: u64,
        gpu_guard: GpuMemoryGuard,
    ) -> KvResult<TransferBreakdown> {
        if len == 0 {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: "GPU transfer length must be non-zero".to_string(),
                },
            ));
        }
        if !gpu_guard.contains(target_addr, len) {
            return Err(KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::InvalidArgument {
                    detail: format!(
                        "GPU transfer exceeds destination guard: target={:#x} len={} registration_id={}",
                        target_addr,
                        len,
                        gpu_guard.registration().registration_id
                    ),
                },
            ));
        }
        const DIRECT_READY_TIMEOUT: Duration = Duration::from_secs(5);
        const DIRECT_RETRY_DELAY: Duration = Duration::from_millis(10);

        let started = Instant::now();
        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);
            let result = self
                .core
                .transfer_data_no_copy(
                    self.runtime(),
                    Some(peer_node.clone()),
                    true,
                    src_addr,
                    target_addr,
                    len,
                    Some(ClientTransferMemoryGuard::Gpu(gpu_guard.clone())),
                    true,
                )
                .await;
            match result {
                Ok(breakdown) => return Ok(breakdown),
                Err(err)
                    if err
                        .to_string()
                        .contains(CLOSED_RUNTIME_DIRECT_FAST_PATH_NOT_READY_MARKER)
                        && started.elapsed() < DIRECT_READY_TIMEOUT =>
                {
                    tracing::debug!(
                        peer = %peer_node,
                        attempt,
                        elapsed_ms = started.elapsed().as_millis(),
                        "GPU direct transfer is waiting for the RDMA peer fast path"
                    );
                    limit_thirdparty::tokio::time::sleep(DIRECT_RETRY_DELAY).await;
                }
                Err(err) => return Err(KvError::from(err)),
            }
        }
    }
}

#[async_trait]
impl LogicalModule for ClientTransferEngine {
    type View = ClientTransferEngineView;
    type NewArg = ClientTransferEngineNewArg;
    type Error = KvError;

    fn name(&self) -> &str {
        "ClientTransferEngineModule"
    }

    fn attach_view(&self, view: Self::View) {
        ClientTransferEngine::attach_view(self, view);
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.close().await;
        Ok(())
    }
}

#[cfg(test)]
mod gpu_memory_registry_tests {
    use super::GpuMemoryRegistry;

    fn active_registry() -> (GpuMemoryRegistry, u64) {
        let registry = GpuMemoryRegistry::new();
        let registration = registry
            .begin_register(0x1000, 0x1000, 2)
            .expect("registration must start");
        let registration_id = registration.registration_id;
        registry.finish_register(registration_id, true);
        (registry, registration_id)
    }

    #[test]
    fn validates_exact_registration_generation_and_range() {
        let (registry, registration_id) = active_registry();

        let guard = registry
            .validate_destination(registration_id, 0x1400, 0x400)
            .expect("subrange must validate");
        assert_eq!(guard.registration().registration_id, registration_id);
        assert_eq!(guard.registration().device_id, 2);

        assert!(
            registry
                .validate_destination(registration_id + 1, 0x1400, 0x400)
                .unwrap_err()
                .contains("registration id mismatch")
        );
        assert!(
            registry
                .validate_destination(registration_id, 0x1f00, 0x200)
                .unwrap_err()
                .contains("outside the active registration")
        );
    }

    #[test]
    fn rejects_invalid_registration_geometry() {
        let registry = GpuMemoryRegistry::new();
        assert!(registry.begin_register(0, 1, 0).is_err());
        assert!(registry.begin_register(1, 0, 0).is_err());
        assert!(registry.begin_register(u64::MAX, 2, 0).is_err());
    }

    #[test]
    fn unregister_waits_for_destination_guards() {
        let (registry, registration_id) = active_registry();
        let guard = registry
            .validate_destination(registration_id, 0x1000, 0x1000)
            .expect("full range must validate");

        assert!(
            registry
                .begin_unregister(registration_id)
                .unwrap_err()
                .contains("GPU registration is busy")
        );
        drop(guard);

        let descriptor = registry
            .begin_unregister(registration_id)
            .expect("unregister must start after guards drop");
        assert_eq!(descriptor.registration_id, registration_id);
        registry.finish_unregister(registration_id, true);
        assert!(
            registry
                .validate_destination(registration_id, 0x1000, 1)
                .is_err()
        );
    }

    #[test]
    fn failed_backend_transitions_restore_retryable_state() {
        let registry = GpuMemoryRegistry::new();
        let first = registry
            .begin_register(0x1000, 0x1000, 0)
            .expect("first registration must start");
        registry.finish_register(first.registration_id, false);

        let second = registry
            .begin_register(0x3000, 0x1000, 1)
            .expect("failed registration must return to empty");
        registry.finish_register(second.registration_id, true);
        let descriptor = registry
            .begin_unregister(second.registration_id)
            .expect("unregister must start");
        registry.finish_unregister(descriptor.registration_id, false);

        assert!(
            registry
                .validate_destination(second.registration_id, 0x3000, 1)
                .is_ok()
        );
    }
}
