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
use fluxon_commu::transfer_engine::AttachedTransferEngine;
use fluxon_commu::{
    ClientTransferEngineClusterRuntime, ClientTransferEngineCore, ClientTransferEngineRuntime,
    CpuAllocatedMem, TransferBreakdown,
};
use fluxon_framework::{LogicalModule, define_module};
use std::future::Future;
use std::pin::Pin;
use std::sync::OnceLock;

pub use fluxon_commu::{ClientTransferEngineNewArg, ClientTransferEngineRuntimeConfig};

// P2P-based raw memory transfer RPC; used only when engine type is explicitly P2p.
mod p2p_transfer_rpc;

define_module!(
    ClientTransferEngine,
    (p2p, P2pModule),
    (cluster_manager, ClusterManager),
    (client_seg_pool, ClientSegPool)
);

#[derive(Clone)]
struct ClientTransferRuntimeAdapter {
    view: ClientTransferEngineView,
}

impl ClientTransferRuntimeAdapter {
    fn local_segment_transfer_enabled(&self) -> bool {
        let self_info = self.view.cluster_manager().get_self_info();
        matches!(self_info.node_role(), NodeRole::Client)
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
    type LocalSegmentGuard = ClientCpuMemReadGuard;

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
        seg_guard: Option<ClientCpuMemReadGuard>,
    ) -> Result<ClientCpuMemReadGuard, String> {
        if !self.local_segment_transfer_enabled() {
            return Err("local segment transfer is not supported on this node role".to_string());
        }
        p2p_transfer_rpc::ensure_local_segment_guard(&self.view, local_addr, seg_guard).await
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
        seg_guard: ClientCpuMemReadGuard,
    ) -> Result<(), String> {
        if !self.local_segment_transfer_enabled() {
            return Err("p2p raw-memory read is not supported on this node role".to_string());
        }
        p2p_transfer_rpc::p2p_read_to_local(
            &self.view,
            peer,
            remote_src,
            local_target,
            len,
            seg_guard,
        )
        .await
    }

    async fn p2p_write_from_local(
        &self,
        peer: NodeIDString,
        local_src: u64,
        remote_target: u64,
        len: u64,
        copy_from: Option<Pin<&[u8]>>,
        seg_guard: ClientCpuMemReadGuard,
    ) -> Result<(), String> {
        if !self.local_segment_transfer_enabled() {
            return Err("p2p raw-memory write is not supported on this node role".to_string());
        }
        p2p_transfer_rpc::p2p_write_from_local(
            &self.view,
            peer,
            local_src,
            remote_target,
            len,
            copy_from,
            seg_guard,
        )
        .await
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
}

impl ClientTransferEngine {
    fn runtime(&self) -> ClientTransferRuntimeAdapter {
        ClientTransferRuntimeAdapter {
            view: self.view.get().unwrap().clone(),
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
                seg_guard,
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
                seg_guard,
                false,
            )
            .await
            .map_err(KvError::from)
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
