// Memholder module consolidating memholder types and lifetime management.
//
// This module hosts both:
// - types previously under `client_kv_api::memholder`
// - lifetime/manager utilities previously in `memholder_lifetime`

mod ensure_memholder_mgmt_delete;
mod lifetime;
pub use ensure_memholder_mgmt_delete::*;
pub(crate) use lifetime::{DeleteShutdownCtx, MemholderManagerTrait};
use lifetime::{ExternalDeleteAckCtx, MemholderDropAck, OwnerDeleteAckCtx};
pub use lifetime::{
    MasterOwnerMemMgr, NodeHolderKey, OwnerDeleteAckItem, OwnerDeleteAckMemMgr, OwnerExternalMemMgr,
};
pub mod kvclient_encode;
// Include memholder tests in either unit-test builds or when feature `test_bins` is enabled.
#[cfg(any(test, feature = "test_bins"))]
pub mod memholder_test;

use std::sync::Arc;

use crate::client_kv_api::ClientKvApiView;
use crate::{cluster_manager::NodeID, external_client_api::ExternalClientApiView};

use bitcode::{Decode, Encode};

/// Memory metadata for owner/client user holders
#[derive(Clone)]
pub struct MemoryInfo {
    pub offset: u64,
    /// Computed absolute address (base + offset) at creation
    pub addr: u64,
    pub len: u32,
    pub holder_id: u64,
    pub key: String,
    pub master_node_id: NodeID,
    pub view: ClientKvApiView,
}

impl std::fmt::Debug for MemoryInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryInfo")
            .field("offset", &self.offset)
            .field("addr", &format_args!("{:#x}", self.addr))
            .field("len", &self.len)
            .field("holder_id", &self.holder_id)
            .field("key", &self.key)
            .field("master_node_id", &self.master_node_id)
            .finish()
    }
}

pub struct AllMemholderRefCount {
    pub view: ClientKvApiView,
}
impl AllMemholderRefCount {
    /// Create a new refcount tracker wrapper
    pub fn new(view: ClientKvApiView) -> Self {
        Self { view }
    }
}
impl Drop for AllMemholderRefCount {
    fn drop(&mut self) {
        tracing::debug!(
            "✅ AllMemholderRefCount dropped. Now client_id: {} can_be_dropped.",
            self.view.client_kv_api().client_id()
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UserMemHolderExposeKind {
    SegPtr,
    OwnedCopy,
}

pub struct UserMemHolder {
    pub memory_info: Arc<MemoryInfo>,
    pub refcount: Arc<AllMemholderRefCount>,
    expose_kind: UserMemHolderExposeKind,
}
impl UserMemHolder {
    pub fn holder_id(&self) -> u64 {
        self.memory_info.holder_id
    }
    pub fn get_offset(&self) -> u64 {
        self.memory_info.offset
    }
    pub fn get_length(&self) -> u32 {
        self.memory_info.len
    }
    pub fn set_tag(&self, _tag: String) {
        // no-op tag hook
    }
    pub fn bytes(&self) -> &[u8] {
        self.memory_info.bytes()
    }
    pub fn expose_kind(&self) -> UserMemHolderExposeKind {
        self.expose_kind
    }
    /// Create a new UserMemHolder with memory info and a refcount tracker
    pub fn new(
        memory_info: Arc<MemoryInfo>,
        refcount: Arc<AllMemholderRefCount>,
        expose_kind: UserMemHolderExposeKind,
    ) -> Self {
        tracing::debug!(
            "Creating UserMemHolder for key '{}', _holder_id_ {}, expose_kind={:?}.",
            memory_info.key,
            memory_info.holder_id,
            expose_kind
        );
        Self {
            memory_info,
            refcount,
            expose_kind,
        }
    }
    pub fn memory_info(&self) -> Arc<MemoryInfo> {
        self.memory_info.clone()
    }
}

impl Drop for UserMemHolder {
    fn drop(&mut self) {
        tracing::debug!(
            "destroying UserMemHolder for key '{}', _holder_id_ {} is being dropped, expose_kind={:?}",
            self.memory_info.key,
            self.holder_id(),
            self.expose_kind
        );
    }
}

impl MemoryInfo {
    pub async fn new(
        offset: u64,
        len: u32,
        holder_id: u64,
        key: String,
        master_node_id: NodeID,
        view: ClientKvApiView,
    ) -> Self {
        // In owner/client context, base address must exist once initialized; unwrap by design.
        let base_addr = {
            let base_guard = view
                .client_seg_pool()
                .cpu_mem_read_guard()
                .await
                .expect("segment cpu mem must be available when creating MemoryInfo");
            base_guard.allocated_addr_ro
        };
        let addr = base_addr + offset;
        Self {
            offset,
            addr,
            len,
            holder_id,
            key,
            master_node_id,
            view,
        }
    }
    pub fn bytes(&self) -> &[u8] {
        tracing::debug!(
            "MemHolder accessing memory: addr={:#x}, len={}",
            self.addr,
            self.len
        );
        unsafe { std::slice::from_raw_parts(self.addr as *const u8, self.len as usize) }
    }
}
/// Represents a memory holder that keeps a reference to transferred data
impl Drop for MemoryInfo {
    fn drop(&mut self) {
        let ctx = OwnerDeleteAckCtx {
            view: self.view.clone(),
            key: self.key.clone(),
            holder_id: self.holder_id,
        };
        ctx.run_drop_ack();
    }
}

/// External memory holder that holds actual data
#[derive(Clone)]
pub struct ExternalMemHolder {
    pub offset: u64,
    /// Computed absolute address (base + offset) at creation
    pub addr: u64,
    pub len: u32,
    pub holder_id: u64,
    pub key: String,
    pub external_client_id: String,
    pub view: ExternalClientApiView,
    pub owner_start_time: i64,
}
/// Info structure for external memory holder (for message passing)
#[derive(Debug, Clone, Encode, Decode, Default)]
pub struct ExternalMemHolderInfo {
    pub offset: u64,
    pub len: u32,
    pub holder_id: u64,
    pub source_kind: crate::master_kv_router::msg_pack::GetSourceKind,
}
impl ExternalMemHolder {
    /// Get the memory offset
    pub fn get_offset(&self) -> u64 {
        self.offset
    }

    /// Get the memory length
    pub fn get_length(&self) -> u64 {
        self.len as u64
    }

    /// Create a new ExternalMemHolder
    pub fn new(
        offset: u64,
        addr: u64,
        len: u32,
        holder_id: u64,
        key: String,
        external_client_id: String,
        view: ExternalClientApiView,
        owner_start_time: i64,
    ) -> Self {
        Self {
            offset,
            addr,
            len,
            holder_id,
            key,
            external_client_id,
            view,
            owner_start_time,
        }
    }

    /// Get a view of the held data from computed address
    pub fn bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.addr as *const u8, self.len as usize) }
    }
}

impl Drop for ExternalMemHolder {
    fn drop(&mut self) {
        tracing::debug!(
            "ExternalMemHolder dropping: key={}, holder_id={}, external_client_id={}",
            self.key,
            self.holder_id,
            self.external_client_id
        );

        let ctx = ExternalDeleteAckCtx {
            view: self.view.clone(),
            key: self.key.clone(),
            external_client_id: self.external_client_id.clone(),
            holder_id: self.holder_id,
            started_time: self.owner_start_time,
        };
        ctx.run_drop_ack();
    }
}
