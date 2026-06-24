use crate::ClientKvApiAccessTrait;
use crate::client_kv_api::ClientKvApi;
use crate::client_transfer_engine::{ClientTransferEngine, ClientTransferEngineAccessTrait};
use crate::cluster_manager::ClusterManagerAccessTrait;
use crate::config::ContributeToClusterPoolSize;
use crate::master_seg_manager::msg_pack::SegmentDeviceMemInfo;
use crate::p2p::p2p_module::P2pModule;
use crate::p2p::p2p_module::P2pModuleAccessTrait;
use crate::rpcresp_kvresult_convert::FromError;
use crate::rpcresp_kvresult_convert::msg_and_error::OK;
use crate::{
    cluster_manager::{ClusterManager, ClusterMember},
    master_seg_manager::msg_pack::{
        RequestSegmentRegistrationReq, RequestSegmentRegistrationResp, SegmentDeviceDescription,
    },
    p2p::msg_pack::{MsgPack, MsgPackSerializePart, RPCCaller, RPCHandler, RPCReq},
    rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult},
};
use async_trait::async_trait;
use bitcode::{Decode, Encode};
use fluxon_commu::{CpuAllocatedMem, ShareGroupOwnerRef};
use fluxon_framework::{LogicalModule, define_module};
use fluxon_util::new_map;
use limit_thirdparty::tokio;
use limit_thirdparty::tokio::sync::ARwLockReadGuardOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Deref;
use std::path::Path;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::ARwLock;

define_module!(
    ClientSegPool,
    (client_seg_pool, ClientSegPool),
    (cluster_manager, ClusterManager),
    (p2p, P2pModule),
    (client_transfer_engine, ClientTransferEngine),
    (client_kv_api, ClientKvApi)
);

/// ClientSegPool module creation parameters
#[derive(Clone, Debug)]
pub struct ClientSegPoolNewArg {
    pub contribute_size: ContributeToClusterPoolSize,
    pub share_mem_path: String,
    pub large_file_paths: crate::config::LargeFilePaths,
    pub cluster_name: String,
    pub etcd_addresses: Vec<String>,
    pub attach_existing_meta: Option<SharedJsonMeta>,
    pub side_transfer_worker: bool,
    pub require_transfer_rpc_fast_path_ready_timeout: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedJsonMeta {
    pub owner_id: String,
    pub node_start_time: i64,
    pub segment_len: u64,
    pub segment_label: Option<String>,
    pub sub_cluster: Option<String>,
    pub cluster_name: String,
    pub etcd_addresses: Vec<String>,
    pub share_mem_path: String,
    pub large_file_paths: crate::config::LargeFilePaths,
    pub protocol_version: String,
    pub write_ts: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SideTransferPeerFileMeta {
    pub side_id: String,
    pub owner_id: String,
    pub owner_start_time: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane_idx: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_base_addr: Option<u64>,
    pub sub_cluster: Option<String>,
    pub write_ts: Option<i64>,
}

impl SideTransferPeerFileMeta {
    pub fn worker_idx(&self) -> Option<u16> {
        self.lane_idx
            .or_else(|| parse_side_transfer_worker_lane_idx(&self.side_id))
    }
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ResolveSideTransferLaneReq {
    pub lane_idx: u16,
}

impl MsgPackSerializePart for ResolveSideTransferLaneReq {
    fn msg_id(&self) -> u32 {
        crate::rpcresp_kvresult_convert::msg_and_error::MsgId::ResolveSideTransferLaneReq as u32
    }
}

impl RPCReq for ResolveSideTransferLaneReq {
    type Resp = ResolveSideTransferLaneResp;
}

#[derive(Default, Debug, Clone, Encode, Decode)]
pub struct ResolveSideTransferLaneResp {
    pub error_code: u32,
    pub error_json: String,
    pub side_id: Option<String>,
    pub target_base_addr: Option<u64>,
}

impl MsgPackSerializePart for ResolveSideTransferLaneResp {
    fn msg_id(&self) -> u32 {
        crate::rpcresp_kvresult_convert::msg_and_error::MsgId::ResolveSideTransferLaneResp as u32
    }
}

pub const SIDE_TRANSFER_PEERS_DIRNAME: &str = "side_transfer_peers";

pub(crate) fn parse_side_transfer_worker_lane_idx(instance_key: &str) -> Option<u16> {
    let (_, suffix) = instance_key.rsplit_once("__side_")?;
    suffix.parse::<u16>().ok()
}

pub struct ClientSegPool(ClientSegPoolInner);

use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug)]
pub struct ClientMappedMem {
    pub registered_mem: CpuAllocatedMem,
    pub allocated_addr_ro: u64,
    layout_validated: AtomicBool,
}

impl ClientMappedMem {
    #[inline]
    pub fn contains_rw(&self, addr: u64, len: u64) -> bool {
        range_contains(self.allocated_addr, self.allocated_size, addr, len)
    }

    #[inline]
    pub fn contains_rw_or_ro(&self, addr: u64, len: u64) -> bool {
        range_contains(self.allocated_addr, self.allocated_size, addr, len)
            || range_contains(self.allocated_addr_ro, self.allocated_size, addr, len)
    }

    pub fn validate_layout(&self, ctx: &str) -> Result<(), String> {
        if self.layout_validated.load(Ordering::Acquire) {
            return Ok(());
        }
        let file_len = self
            .registered_mem
            ._file
            .metadata()
            .map_err(|e| format!("{ctx}: failed to stat mmap.file: {e}"))?
            .len();
        if file_len != self.allocated_size {
            return Err(format!(
                "{ctx}: cpu segment size mismatch: file_len={file_len}, allocated_size={}",
                self.allocated_size
            ));
        }
        self.layout_validated.store(true, Ordering::Release);
        Ok(())
    }
}

impl Deref for ClientMappedMem {
    type Target = CpuAllocatedMem;

    fn deref(&self) -> &Self::Target {
        &self.registered_mem
    }
}

pub struct ClientCpuMemReadGuard {
    guard: ARwLockReadGuardOwned<Option<ClientMappedMem>>,
}

impl ClientCpuMemReadGuard {
    pub fn new(guard: ARwLockReadGuardOwned<Option<ClientMappedMem>>) -> Self {
        Self { guard }
    }
}

impl Deref for ClientCpuMemReadGuard {
    type Target = ClientMappedMem;

    fn deref(&self) -> &Self::Target {
        self.guard
            .as_ref()
            .expect("ClientCpuMemReadGuard requires cpu_allocated_mem to be Some")
    }
}

pub struct ClientSegPoolInner {
    cpu_allocated_mem: std::sync::Arc<ARwLock<Option<ClientMappedMem>>>,
    view: std::sync::OnceLock<ClientSegPoolView>,
    /// Directory path for the local shared bundle (mmap.file, shared.json, peer metadata).
    share_mem_path: String,
    /// Ordered large-file roots; concrete subdirectories are derived by fixed relative layout.
    large_file_paths: crate::config::LargeFilePaths,
    side_transfer_worker: bool,
    attach_owner_ref: Option<ShareGroupOwnerRef>,

    // Redundant fields written to shared.json for external bootstrap and strict validation.
    cluster_name: String,
    etcd_addresses: Vec<String>,
    require_transfer_rpc_fast_path_ready_timeout: Option<Duration>,

    /// Whether we've already notified external by writing memory.file after readiness
    ready_notified: AtomicBool,
}

#[inline]
fn range_contains(base: u64, size: u64, addr: u64, len: u64) -> bool {
    let Some(end) = addr.checked_add(len) else {
        return false;
    };
    let Some(seg_end) = base.checked_add(size) else {
        return false;
    };
    addr >= base && end <= seg_end
}

impl ClientSegPoolInner {
    fn view(&self) -> &ClientSegPoolView {
        self.view.get().unwrap()
    }
}

impl ClientSegPool {
    pub fn side_transfer_peers_dir(share_mem_path: &str) -> std::path::PathBuf {
        std::path::Path::new(share_mem_path).join(SIDE_TRANSFER_PEERS_DIRNAME)
    }

    pub fn side_transfer_peer_file_path(
        share_mem_path: &str,
        side_id: &str,
    ) -> std::path::PathBuf {
        Self::side_transfer_peers_dir(share_mem_path).join(format!("{side_id}.json"))
    }

    pub fn attach_view(&self, view: ClientSegPoolView) {
        // The framework attaches a module's PostView exactly once at the init barrier.
        // A second attach indicates a programming error.
        self.0
            .view
            .set(view)
            .unwrap_or_else(|_| panic!("ClientSegPool view attached twice"));
    }

    pub async fn construct(arg: ClientSegPoolNewArg) -> Result<Self, KvError> {
        tracing::info!(
            "Constructing ClientSegPool in Client mode with share_mem_path: {}",
            arg.share_mem_path
        );

        let contribute_size = arg.contribute_size;
        let share_mem_path = arg.share_mem_path;
        let large_file_paths = arg.large_file_paths;
        let cluster_name = arg.cluster_name;
        let etcd_addresses = arg.etcd_addresses;
        let attach_existing_meta = arg.attach_existing_meta;
        let side_transfer_worker = arg.side_transfer_worker;
        let require_transfer_rpc_fast_path_ready_timeout =
            arg.require_transfer_rpc_fast_path_ready_timeout;
        let attach_owner_ref = attach_existing_meta
            .as_ref()
            .map(|meta| ShareGroupOwnerRef {
                owner_id: meta.owner_id.clone(),
                owner_start_time: meta.node_start_time,
            });

        if let Some(existing_meta) = attach_existing_meta {
            tracing::info!(
                "Attaching existing shared memory for side-transfer worker: path={}, len={}",
                share_mem_path,
                existing_meta.segment_len
            );

            use std::fs::OpenOptions;
            use std::os::unix::io::AsRawFd;
            use std::path::Path;
            use std::ptr;

            let map_len = existing_meta.segment_len as usize;
            let mmap_file_path = Path::new(&share_mem_path).join("mmap.file");
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&mmap_file_path)
                .map_err(|e| {
                    KvError::SharedMem(
                        crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                            path: mmap_file_path.to_string_lossy().to_string(),
                            len: map_len as u64,
                            detail: format!("Failed to open existing mmap.file: {}", e),
                        },
                    )
                })?;

            let fd = file.as_raw_fd();
            let (ptr, ptr_ro) = unsafe {
                let ptr = libc::mmap(
                    ptr::null_mut(),
                    map_len,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_SHARED,
                    fd,
                    0,
                );
                if ptr == libc::MAP_FAILED {
                    return Err(KvError::SharedMem(
                        crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                            path: mmap_file_path.to_string_lossy().to_string(),
                            len: map_len as u64,
                            detail: "mmap failed".to_string(),
                        },
                    ));
                }
                let ptr_ro = libc::mmap(
                    ptr::null_mut(),
                    map_len,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    fd,
                    0,
                );
                if ptr_ro == libc::MAP_FAILED {
                    libc::munmap(ptr, map_len);
                    return Err(KvError::SharedMem(
                        crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                            path: mmap_file_path.to_string_lossy().to_string(),
                            len: map_len as u64,
                            detail: "mmap read-only failed".to_string(),
                        },
                    ));
                }
                (ptr as u64, ptr_ro as u64)
            };

            let inner = ClientSegPoolInner {
                cpu_allocated_mem: std::sync::Arc::new(ARwLock::new(Some(ClientMappedMem {
                    registered_mem: CpuAllocatedMem {
                        _file: file,
                        allocated_addr: ptr,
                        allocated_size: map_len as u64,
                    },
                    allocated_addr_ro: ptr_ro,
                    layout_validated: AtomicBool::new(false),
                }))),
                view: std::sync::OnceLock::new(),
                share_mem_path: share_mem_path.clone(),
                large_file_paths: large_file_paths.clone(),
                side_transfer_worker,
                attach_owner_ref,
                cluster_name: cluster_name.clone(),
                etcd_addresses: etcd_addresses.clone(),
                require_transfer_rpc_fast_path_ready_timeout,
                ready_notified: AtomicBool::new(false),
            };
            return Ok(Self(inner));
        }

        if contribute_size.dram == 0 {
            let inner = ClientSegPoolInner {
                cpu_allocated_mem: std::sync::Arc::new(ARwLock::new(None)),
                view: std::sync::OnceLock::new(),
                share_mem_path: share_mem_path.clone(),
                large_file_paths: large_file_paths.clone(),
                side_transfer_worker,
                attach_owner_ref,
                cluster_name: cluster_name.clone(),
                etcd_addresses: etcd_addresses.clone(),
                require_transfer_rpc_fast_path_ready_timeout,
                ready_notified: AtomicBool::new(false),
            };
            return Ok(Self(inner));
        }

        // Allocate initial memory (same logic as legacy init).
        tracing::info!("==============================");
        tracing::debug!("allocating dram memory: {}", contribute_size.dram);

        use std::fs::OpenOptions;
        use std::os::unix::fs::OpenOptionsExt;
        use std::os::unix::io::AsRawFd;
        use std::path::Path;
        use std::ptr;

        let map_len = contribute_size.dram as usize;

        if share_mem_path.is_empty() {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                    path: String::new(),
                    len: map_len as u64,
                    detail: "share_mem_path is empty; explicit configuration required"
                        .to_string(),
                },
            ));
        }

        let base_path = &share_mem_path;
        tracing::info!(
            "Using share_mem_path: {} for memory-mapped file",
            base_path
        );
        std::fs::create_dir_all(base_path).map_err(|e| {
            KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                    path: base_path.to_string(),
                    len: map_len as u64,
                    detail: format!("Failed to create directory: {}", e),
                },
            )
        })?;

        let mmap_file_path = Path::new(base_path).join("mmap.file");
        if mmap_file_path.exists() {
            std::fs::remove_file(&mmap_file_path).map_err(|e| {
                KvError::SharedMem(
                    crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                        path: mmap_file_path.to_string_lossy().to_string(),
                        len: map_len as u64,
                        detail: format!("Failed to remove existing mmap.file: {}", e),
                    },
                )
            })?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o666)
            .open(&mmap_file_path)
            .map_err(|e| {
                KvError::SharedMem(
                    crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                        path: mmap_file_path.to_string_lossy().to_string(),
                        len: map_len as u64,
                        detail: format!("Failed to create mmap.file: {}", e),
                    },
                )
            })?;

        let fd = file.as_raw_fd();
        unsafe {
            let ret = libc::ftruncate(fd, map_len as i64);
            if ret != 0 {
                return Err(KvError::SharedMem(
                    crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                        path: mmap_file_path.to_string_lossy().to_string(),
                        len: map_len as u64,
                        detail: format!("ftruncate failed: {}", ret),
                    },
                ));
            }
        }

        let (ptr, ptr_ro) = unsafe {
            let ptr = libc::mmap(
                ptr::null_mut(),
                map_len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            );
            if ptr == libc::MAP_FAILED {
                return Err(KvError::SharedMem(
                    crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                        path: mmap_file_path.to_string_lossy().to_string(),
                        len: map_len as u64,
                        detail: "mmap failed".to_string(),
                    },
                ));
            }
            let ptr_ro = libc::mmap(
                ptr::null_mut(),
                map_len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            );
            if ptr_ro == libc::MAP_FAILED {
                libc::munmap(ptr, map_len);
                return Err(KvError::SharedMem(
                    crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                        path: mmap_file_path.to_string_lossy().to_string(),
                        len: map_len as u64,
                        detail: "mmap read-only failed".to_string(),
                    },
                ));
            }
            (ptr as u64, ptr_ro as u64)
        };

        if ptr == 0 || ptr_ro == 0 {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MappingFailed {
                    path: mmap_file_path.to_string_lossy().to_string(),
                    len: map_len as u64,
                    detail: "Get empty mem".to_string(),
                },
            ));
        }

        let inner = ClientSegPoolInner {
            cpu_allocated_mem: std::sync::Arc::new(ARwLock::new(Some(ClientMappedMem {
                registered_mem: CpuAllocatedMem {
                    _file: file,
                    allocated_addr: ptr,
                    allocated_size: map_len as u64,
                },
                allocated_addr_ro: ptr_ro,
                layout_validated: AtomicBool::new(false),
            }))),
            view: std::sync::OnceLock::new(),
            share_mem_path: base_path.to_string(),
            large_file_paths,
            side_transfer_worker,
            attach_owner_ref,
            cluster_name,
            etcd_addresses,
            require_transfer_rpc_fast_path_ready_timeout,
            ready_notified: AtomicBool::new(false),
        };
        Ok(Self(inner))
    }

    fn inner(&self) -> &ClientSegPoolInner {
        &self.0
    }

    pub fn share_mem_path(&self) -> &str {
        &self.inner().share_mem_path
    }

    pub fn large_file_paths(&self) -> &crate::config::LargeFilePaths {
        &self.inner().large_file_paths
    }

    fn transfer_rpc_fast_path_eligible_members(&self) -> Vec<ClusterMember> {
        let inner = self.inner();
        let self_info = inner.view().cluster_manager().get_self_info();
        inner
            .view()
            .cluster_manager()
            .get_client_members()
            .into_iter()
            .filter(|member| member.id != self_info.id)
            .filter(|member| {
                member
                    .metadata
                    .get("client")
                    .is_some_and(|value| value == "true")
            })
            .filter(|member| {
                member
                    .metadata
                    .get("p2p_relay")
                    .is_some_and(|value| value == "true")
            })
            .filter(|member| {
                !member
                    .metadata
                    .get("external_client")
                    .is_some_and(|value| value == "true")
            })
            .filter(|member| {
                !member
                    .metadata
                    .get("side_transfer_worker")
                    .is_some_and(|value| value == "true")
            })
            .collect()
    }

    fn format_transfer_rpc_fast_path_pending_details(
        &self,
        eligible_members: &[ClusterMember],
    ) -> String {
        let inner = self.inner();
        let snapshot = inner.view().p2p_module().tier_snapshot();
        let mut pending = Vec::new();
        for member in eligible_members {
            let peer_id = member.id.clone().into();
            let Some(peer_gen) = snapshot.peer_gen(&peer_id) else {
                pending.push(format!(
                    "{}(member_start_time={}, tier_snapshot=missing)",
                    member.id, member.node_start_time
                ));
                continue;
            };
            let peer_view = snapshot.peers.get(&peer_gen.peer_id);
            pending.push(format!(
                "{}(member_start_time={}, snapshot_start_time={}, transfer_rpc_ready={}, direct_ready={}, intra_ready={}, transfer_backend_epoch={:?}, transfer_rpc_ready_backend_epoch={:?})",
                member.id,
                member.node_start_time,
                peer_gen.node_start_time,
                snapshot.is_transfer_rpc_ready(&peer_gen),
                snapshot.is_send_ready_direct(&peer_gen),
                snapshot.is_send_ready_intra_effective(&peer_gen),
                snapshot.transfer_backend_epoch(&peer_gen),
                peer_view.and_then(|value| value.transfer_rpc_ready_backend_epoch),
            ));
        }
        pending.join(", ")
    }

    pub async fn wait_required_transfer_rpc_fast_path_ready(&self) -> KvResult<()> {
        let inner = self.inner();
        let Some(timeout) = inner.require_transfer_rpc_fast_path_ready_timeout else {
            return Ok(());
        };
        if inner.side_transfer_worker {
            return Ok(());
        }

        let started_at = Instant::now();
        let poll_interval = Duration::from_millis(200);
        let self_info = inner.view().cluster_manager().get_self_info();

        loop {
            let eligible_members = self.transfer_rpc_fast_path_eligible_members();
            if !eligible_members.is_empty() {
                let snapshot = inner.view().p2p_module().tier_snapshot();
                let all_ready = eligible_members.iter().all(|member| {
                    let peer_id = member.id.clone().into();
                    snapshot
                        .peer_gen(&peer_id)
                        .is_some_and(|peer_gen| snapshot.is_transfer_rpc_ready(&peer_gen))
                });
                if all_ready {
                    tracing::info!(
                        owner_id = %self_info.id,
                        owner_start_time = self_info.node_start_time,
                        peer_count = eligible_members.len(),
                        elapsed_ms = started_at.elapsed().as_millis(),
                        "required transfer-rpc fast path ready for all eligible owner peers"
                    );
                    return Ok(());
                }
            }

            let elapsed = started_at.elapsed();
            if elapsed >= timeout {
                let detail = if eligible_members.is_empty() {
                    format!(
                        "no eligible owner peers observed within {:?}; self={} start_time={}",
                        timeout, self_info.id, self_info.node_start_time
                    )
                } else {
                    format!(
                        "pending peers after {:?}: {}",
                        timeout,
                        self.format_transfer_rpc_fast_path_pending_details(&eligible_members)
                    )
                };
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!(
                        "required transfer-rpc fast path readiness gate timed out: {}",
                        detail
                    ),
                }));
            }

            limit_thirdparty::tokio::time::sleep(std::cmp::min(
                poll_interval,
                timeout.saturating_sub(elapsed),
            ))
            .await;
        }
    }

    pub async fn mapped_range(&self) -> Option<(u64, u64, u64)> {
        let cpu_mem_guard = self.0.cpu_allocated_mem.read().await;
        let cpu_mem = cpu_mem_guard.as_ref()?;
        Some((
            cpu_mem.allocated_addr,
            cpu_mem.allocated_addr_ro,
            cpu_mem.allocated_size,
        ))
    }
    pub async fn calculate_offset_from_addr(&self, addr: u64) -> KvResult<u64> {
        let base_guard = self.cpu_mem_read_guard().await?;
        let base_addr = base_guard.allocated_addr;
        if addr < base_addr {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::InvalidAddress {
                    address: addr,
                    detail: Some(format!("below base address {:#x}", base_addr)),
                },
            ));
        }
        Ok(addr - base_addr)
    }
    pub async fn calculate_addr_from_offset(&self, offset: u64) -> KvResult<u64> {
        let base_guard = self.cpu_mem_read_guard().await?;
        Ok(base_guard.allocated_addr + offset)
    }

    pub async fn cpu_mem_read_guard(&self) -> KvResult<ClientCpuMemReadGuard> {
        let guard = self.0.cpu_allocated_mem.clone().read_owned().await;
        if guard.as_ref().is_none() {
            return Err(KvError::Api(ApiError::SegmentNotMounted {
                detail: "cpu_allocated_mem is None; segment is not mounted".to_string(),
            }));
        }
        Ok(ClientCpuMemReadGuard::new(guard))
    }

    pub async fn get_guard_of_address(&self, addr: u64) -> KvResult<ClientCpuMemReadGuard> {
        let guard = self.cpu_mem_read_guard().await?;
        let rw_base = guard.allocated_addr;
        let ro_base = guard.allocated_addr_ro;
        let seg_len = guard.allocated_size;
        let Some(rw_end) = rw_base.checked_add(seg_len) else {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::InvalidAddress {
                    address: rw_base,
                    detail: Some(format!(
                        "segment range overflow: rw_base={:#x}, seg_len={}",
                        rw_base, seg_len
                    )),
                },
            ));
        };
        let Some(ro_end) = ro_base.checked_add(seg_len) else {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::InvalidAddress {
                    address: ro_base,
                    detail: Some(format!(
                        "segment range overflow: ro_base={:#x}, seg_len={}",
                        ro_base, seg_len
                    )),
                },
            ));
        };

        let in_rw = addr >= rw_base && addr < rw_end;
        let in_ro = addr >= ro_base && addr < ro_end;
        if !in_rw && !in_ro {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::InvalidAddress {
                    address: addr,
                    detail: Some(format!(
                        "not in segment range: rw=[{:#x},{:#x}), ro=[{:#x},{:#x})",
                        rw_base, rw_end, ro_base, ro_end
                    )),
                },
            ));
        }

        Ok(guard)
    }

    pub async fn copy_into_segment(&self, target_addr: u64, payload: &[u8]) -> Result<(), String> {
        let started_at = Instant::now();
        let guard = self.0.cpu_allocated_mem.read().await;
        let Some(seg) = guard.as_ref() else {
            return Err("cpu_allocated_mem is None; segment is not mounted".to_string());
        };
        seg.validate_layout("copy_into_segment")?;

        let len = payload.len() as u64;
        if !seg.contains_rw(target_addr, len) {
            let rw_end = seg
                .allocated_addr
                .checked_add(seg.allocated_size)
                .unwrap_or(u64::MAX);
            return Err(format!(
                "target_addr range not in local RW segment: target_addr={:#x}, len={}, rw=[{:#x},{:#x})",
                target_addr, len, seg.allocated_addr, rw_end
            ));
        }

        unsafe {
            std::ptr::copy_nonoverlapping(payload.as_ptr(), target_addr as *mut u8, payload.len());
        }
        tracing::info!(
            "copy_into_segment timing: target_addr={:#x} payload_len={} elapsed_us={}",
            target_addr,
            payload.len(),
            started_at.elapsed().as_micros().min(i64::MAX as u128) as i64
        );
        Ok(())
    }

    pub async fn read_from_segment(
        &self,
        src_addr: u64,
        len: usize,
    ) -> Result<bytes::Bytes, String> {
        let guard = self.0.cpu_allocated_mem.read().await;
        let Some(seg) = guard.as_ref() else {
            return Err("cpu_allocated_mem is None; segment is not mounted".to_string());
        };
        seg.validate_layout("read_from_segment")?;

        if !seg.contains_rw_or_ro(src_addr, len as u64) {
            let rw_end = seg
                .allocated_addr
                .checked_add(seg.allocated_size)
                .unwrap_or(u64::MAX);
            let ro_end = seg
                .allocated_addr_ro
                .checked_add(seg.allocated_size)
                .unwrap_or(u64::MAX);
            return Err(format!(
                "src_addr range not in local segment: src_addr={:#x}, len={}, rw=[{:#x},{:#x}), ro=[{:#x},{:#x})",
                src_addr, len, seg.allocated_addr, rw_end, seg.allocated_addr_ro, ro_end
            ));
        }

        let bytes = unsafe { std::slice::from_raw_parts(src_addr as *const u8, len).to_vec() };
        Ok(bytes::Bytes::from(bytes))
    }

    pub async fn register(&self) -> KvResult<()> {
        let inner = self.inner();
        let mut cpu_mem_slot = inner.cpu_allocated_mem.write().await;
        let Some(cpu_mem) = cpu_mem_slot.take() else {
            return Ok(());
        };
        let register_result = inner
            .view()
            .client_transfer_engine()
            .register_local_segment(&cpu_mem.registered_mem)
            .await;

        *cpu_mem_slot = Some(cpu_mem);
        register_result
    }

    pub async fn unregister(&self) -> KvResult<()> {
        let inner = self.inner();
        let mut cpu_mem_slot = inner.cpu_allocated_mem.write().await;
        let Some(cpu_mem) = cpu_mem_slot.take() else {
            return Ok(());
        };

        tracing::debug!("Unregistering segment global");
        inner
            .view()
            .client_transfer_engine()
            .unregister_local_segment(&cpu_mem.registered_mem)
            .await?;

        // cpu_allocated_mem stays None after unregister by design.
        Ok(())
    }

    pub async fn register_segment_partials(&self) -> KvResult<()> {
        self.register().await
    }

    pub async fn publish_side_transfer_peer(&self) -> KvResult<()> {
        let inner = self.inner();
        if !inner.side_transfer_worker {
            return Ok(());
        }

        let owner_ref = inner.attach_owner_ref.clone().ok_or_else(|| {
            KvError::Api(ApiError::Unknown {
                detail: "side-transfer worker missing owner binding".to_string(),
            })
        })?;
        let self_info = inner.view().cluster_manager().get_self_info();
        let lane_idx = parse_side_transfer_worker_lane_idx(&self_info.id).ok_or_else(|| {
            KvError::Api(ApiError::Unknown {
                detail: format!(
                    "side-transfer worker instance key missing '__side_<idx>' suffix: {}",
                    self_info.id
                ),
            })
        })?;
        let cpu_mem_guard = inner.cpu_allocated_mem.read().await;
        let cpu_mem = cpu_mem_guard.as_ref().ok_or_else(|| {
            KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::NotConfigured {
                    node_id: Some(self_info.id.clone()),
                    detail: Some("side-transfer worker segment not attached".to_string()),
                },
            )
        })?;
        let peers_dir = Self::side_transfer_peers_dir(&inner.share_mem_path);
        std::fs::create_dir_all(&peers_dir).map_err(|e| {
            KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: peers_dir.to_string_lossy().to_string(),
                    detail: format!("Failed to create side-transfer peer dir: {}", e),
                },
            )
        })?;

        let peer_path = Self::side_transfer_peer_file_path(&inner.share_mem_path, &self_info.id);
        let tmp_path = peer_path.with_file_name(format!(
            "{}.tmp.{}.{}",
            self_info.id,
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| chrono::Utc::now().timestamp_micros() * 1_000),
        ));
        let payload = SideTransferPeerFileMeta {
            side_id: self_info.id.clone(),
            owner_id: owner_ref.owner_id,
            owner_start_time: owner_ref.owner_start_time,
            lane_idx: Some(lane_idx),
            target_base_addr: Some(cpu_mem.allocated_addr),
            sub_cluster: self_info.sub_cluster,
            write_ts: Some(chrono::Utc::now().timestamp_micros()),
        };
        std::fs::write(&tmp_path, serde_json::to_vec(&payload).unwrap()).map_err(|e| {
            KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: tmp_path.to_string_lossy().to_string(),
                    detail: format!("Failed to write side-transfer peer metadata: {}", e),
                },
            )
        })?;
        std::fs::rename(&tmp_path, &peer_path).map_err(|e| {
            KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: peer_path.to_string_lossy().to_string(),
                    detail: format!("Failed to publish side-transfer peer metadata: {}", e),
                },
            )
        })?;
        Ok(())
    }

    async fn remove_side_transfer_peer(&self) -> KvResult<()> {
        let inner = self.inner();
        if !inner.side_transfer_worker {
            return Ok(());
        }
        let self_id = inner.view().cluster_manager().get_self_info().id;
        let peer_path = Self::side_transfer_peer_file_path(&inner.share_mem_path, &self_id);
        match std::fs::remove_file(&peer_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: peer_path.to_string_lossy().to_string(),
                    detail: format!("Failed to remove side-transfer peer metadata: {}", e),
                },
            )),
        }
    }
}

#[async_trait]
impl LogicalModule for ClientSegPool {
    type View = ClientSegPoolView;
    type NewArg = ClientSegPoolNewArg;
    type Error = KvError;

    fn name(&self) -> &str {
        "ClientSegPool"
    }

    fn attach_view(&self, view: Self::View) {
        ClientSegPool::attach_view(self, view);
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        let _ = self.remove_side_transfer_peer().await;
        loop {
            if self.inner().view().client_kv_api().can_be_dropped() {
                tracing::info!("ClientSegPool can be dropped");
                break;
            }
            tracing::info!(
                "ClientSegPool waiting ClientKvApi can not be dropped , will try again after 3s (some user memholder may still be in use)"
            );
            tokio::time::sleep(Duration::from_secs(3)).await;
        }

        if self.0.cpu_allocated_mem.read().await.is_some() {
            self.unregister().await?;
        };
        Ok(())
    }
}

impl ClientSegPool {
    pub async fn init2_for_init_dag(&self) -> KvResult<()> {
        let inner = &self.0;

        // English note:
        // - Invariant: register inbound RPC handlers before any awaited etcd operations that
        //   publish or mutate member metadata.
        // - Otherwise, P2P RX may start dispatching msg_id=2001 (segment registration) while the
        //   handler is not in the dispatch_map yet, and the request can be dropped after retries.
        // - This must be done before `set_self_shared_*` because those methods await etcd writes.
        if !inner.side_transfer_worker {
            let view = inner.view().clone();
            RPCHandler::<RequestSegmentRegistrationReq>::new().regist(
                inner.view().p2p_module(),
                move |resp, req| {
                    let view = view.clone();
                    let view_task = view.clone();
                    let req = req.serialize_part;
                    let _ = view.spawn("rpc_request_segment_registration", async move {
                        let response = handle_segment_registration_request(view_task, req).await;
                        if let Err(e) = resp.send_resp(response).await {
                            tracing::error!(
                                "Failed to send RequestSegmentRegistrationResp: {:?}",
                                e
                            );
                        }
                    });
                    Ok(())
                },
            );

            RPCCaller::<ResolveSideTransferLaneReq>::new().regist(inner.view().p2p_module());
            let view = inner.view().clone();
            RPCHandler::<ResolveSideTransferLaneReq>::new().regist(
                inner.view().p2p_module(),
                move |resp, req| {
                    let view = view.clone();
                    let view_task = view.clone();
                    let req = req.serialize_part;
                    let _ = view.spawn("rpc_resolve_side_transfer_lane", async move {
                        let response =
                            handle_resolve_side_transfer_lane_request(view_task, req).await;
                        if let Err(e) = resp.send_resp(response).await {
                            tracing::error!("Failed to send ResolveSideTransferLaneResp: {:?}", e);
                        }
                    });
                    Ok(())
                },
            );
        }

        let owner_ref = inner
            .attach_owner_ref
            .clone()
            .unwrap_or_else(|| ShareGroupOwnerRef {
                owner_id: inner.view().cluster_manager().get_self_info().id,
                owner_start_time: inner
                    .view()
                    .cluster_manager()
                    .get_self_info()
                    .node_start_time,
            });
        inner
            .view()
            .cluster_manager()
            .set_self_share_group_binding(owner_ref)
            .await?;

        // Do not touch ClientTransferEngine here; it is finalized in its init2.
        // Segment registration will be performed in init3 after all init2 complete.
        Ok(())
    }

    pub async fn init3_for_init_dag(&self) -> KvResult<()> {
        let inner = &self.0;
        if inner.cpu_allocated_mem.read().await.is_some() {
            tracing::info!("Client initialized with segments; registering local transfer segment");
            self.register_segment_partials().await?;
            if inner.side_transfer_worker {
                self.publish_side_transfer_peer().await?;
            }
        } else {
            tracing::info!("No CPU memory allocated, no segments to register");
        }
        Ok(())
    }

    /// After the owner has completed master segment registration, notify external by
    /// writing or updating `shared.json` to signal readiness. This is idempotent.
    pub async fn notify_external_ready(&self) -> KvResult<()> {
        let inner = &self.0;
        if inner.side_transfer_worker {
            return Ok(());
        }

        // The ready_notified flag means "shared.json has been successfully written".
        // Do not set it before the file is durably in place; otherwise a transient IO error
        // would permanently prevent future notifications.
        if inner.ready_notified.load(Ordering::SeqCst) {
            return Ok(());
        }

        let cpu_mem_guard = inner.cpu_allocated_mem.read().await;
        let Some(cpu_mem) = cpu_mem_guard.as_ref() else {
            // No segment allocated; do not mark ready.
            return Ok(());
        };
        let segment_len = cpu_mem.allocated_size;
        drop(cpu_mem_guard);

        self.wait_required_transfer_rpc_fast_path_ready().await?;

        use std::path::Path;
        let shared_json_path = Path::new(&inner.share_mem_path).join("shared.json");
        if let Some(parent) = shared_json_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                KvError::SharedMem(
                    crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                        path: parent.to_string_lossy().to_string(),
                        detail: format!("Failed to create share_mem_path: {}", e),
                    },
                )
            })?;
        }

        let share_mem_canonical = std::fs::canonicalize(&inner.share_mem_path)
            .map_err(|e| {
                KvError::SharedMem(
                    crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                        path: inner.share_mem_path.clone(),
                        detail: format!("Failed to canonicalize share_mem_path: {}", e),
                    },
                )
            })?
            .to_string_lossy()
            .into_owned();

        // Prepare metadata JSON
        let self_info = inner.view().cluster_manager().get_self_info();
        let protocol_version =
            fluxon_util::git_version_build_record::get_current_git_commitid().unwrap();
        let payload = SharedJsonMeta {
            owner_id: self_info.id,
            node_start_time: self_info.node_start_time,
            segment_len,
            segment_label: Some("cpu:0".to_string()),
            sub_cluster: self_info.sub_cluster,

            cluster_name: inner.cluster_name.clone(),
            etcd_addresses: inner.etcd_addresses.clone(),
            share_mem_path: share_mem_canonical,
            large_file_paths: inner.large_file_paths.clone(),

            protocol_version,

            write_ts: Some(chrono::Utc::now().timestamp_micros()),
        };

        // Write atomically via a per-attempt temp file in the same directory.
        // Segment-registration retries can schedule multiple delayed notifications concurrently;
        // a fixed shared.json.tmp path makes successful writers race with later renames that then
        // spuriously fail with ENOENT after another task already moved the temp file away.
        let tmp_path = shared_json_path.with_file_name(format!(
            "shared.json.tmp.{}.{}",
            std::process::id(),
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| chrono::Utc::now().timestamp_micros() * 1_000),
        ));
        std::fs::write(&tmp_path, serde_json::to_vec(&payload).unwrap()).map_err(|e| {
            KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: tmp_path.to_string_lossy().to_string(),
                    detail: format!("Failed to write shared.json.tmp: {}", e),
                },
            )
        })?;
        std::fs::rename(&tmp_path, &shared_json_path).map_err(|e| {
            KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: shared_json_path.to_string_lossy().to_string(),
                    detail: format!("Failed to rename shared.json.tmp to shared.json: {}", e),
                },
            )
        })?;

        inner.ready_notified.store(true, Ordering::SeqCst);

        tracing::info!(
            "Owner ready: written shared.json at {:?} (len={})",
            shared_json_path,
            segment_len
        );
        Ok(())
    }

    pub async fn notify_external_ready_strict_for_init_resource(&self) -> KvResult<()> {
        // The init-step DAG uses a resource hook to publish the external shared-memory bundle.
        // This strict variant is used by that hook and fails fast if the bundle is not
        // configured or if the expected files are missing.
        self.notify_external_ready().await?;

        let inner = &self.0;

        let cpu_mem_guard = inner.cpu_allocated_mem.read().await;
        if cpu_mem_guard.as_ref().is_none() {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::NotConfigured {
                    node_id: None,
                    detail: Some(
                        "ClientSegPool has no allocated segment; cannot publish shared.json"
                            .to_string(),
                    ),
                },
            ));
        }

        let shared_json_path = std::path::Path::new(&inner.share_mem_path).join("shared.json");
        let mmap_file_path = std::path::Path::new(&inner.share_mem_path).join("mmap.file");

        if !mmap_file_path.exists() {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: mmap_file_path.to_string_lossy().to_string(),
                    detail: "mmap.file is missing after notify_external_ready".to_string(),
                },
            ));
        }
        if !shared_json_path.exists() {
            return Err(KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: shared_json_path.to_string_lossy().to_string(),
                    detail: "shared.json is missing after notify_external_ready".to_string(),
                },
            ));
        }
        Ok(())
    }
}

/// Handle segment registration request from master
async fn handle_segment_registration_request(
    view: ClientSegPoolView,
    req: RequestSegmentRegistrationReq,
) -> MsgPack<RequestSegmentRegistrationResp> {
    tracing::info!(
        "Received segment registration request from master, preparing response with segment info."
    );

    let self_info = view.cluster_manager().get_self_info();
    let expected = req.expected_node_start_time;
    let got = self_info.node_start_time;
    if expected != got {
        let err = KvError::Api(ApiError::OwnerStartTimeMismatch { expected, got });
        return MsgPack {
            serialize_part: RequestSegmentRegistrationResp::from_error(&err),
            raw_bytes: Vec::new(),
        };
    }

    let client_seg_pool = view.client_seg_pool();
    let inner = &client_seg_pool.inner();
    let cpu_mem_guard = inner.cpu_allocated_mem.read().await;
    if let Some(cpu_mem) = cpu_mem_guard.as_ref() {
        let seg_map = new_map!(HashMap {
            "cpu:0".to_string() => (
                SegmentDeviceDescription::Cpu,
                SegmentDeviceMemInfo {
                    addr: cpu_mem.allocated_addr,
                    len: cpu_mem.allocated_size,
                },
            )
        });

        tracing::info!("Sending segment map to master: {:?}", seg_map);
        let resp = MsgPack {
            serialize_part: RequestSegmentRegistrationResp {
                error_code: OK,
                error_json: String::new(),
                seg_map,
            },
            raw_bytes: Vec::new(),
        };
        // After preparing a valid registration response, publish shared.json asynchronously.
        // notify_external_ready() performs its own transfer-rpc readiness wait when required.
        let view_task = view.clone();
        let _ = view.spawn("notify_external_ready_after_registration", async move {
            if let Err(e) = view_task.client_seg_pool().notify_external_ready().await {
                tracing::warn!("Failed to notify external ready (memory.file): {}", e);
            }
        });
        resp
    } else {
        tracing::info!("No CPU memory allocated, reporting not configured to master.");
        let err = crate::rpcresp_kvresult_convert::msg_and_error::KvError::SharedMem(
            crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::NotConfigured {
                node_id: None,
                detail: Some("No segments available for registration".to_string()),
            },
        );
        let resp = MsgPack {
            serialize_part: RequestSegmentRegistrationResp::from_error(&err),
            raw_bytes: Vec::new(),
        };
        // Even if no segment, notify to unblock external? Generally not; skip notify.
        resp
    }
}

fn read_side_transfer_peer_file(path: &Path) -> KvResult<SideTransferPeerFileMeta> {
    let payload = std::fs::read_to_string(path).map_err(|e| {
        KvError::SharedMem(
            crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                path: path.to_string_lossy().to_string(),
                detail: format!("Failed to read side-transfer peer metadata: {}", e),
            },
        )
    })?;
    serde_json::from_str(&payload).map_err(|e| {
        KvError::SharedMem(
            crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                path: path.to_string_lossy().to_string(),
                detail: format!("Failed to parse side-transfer peer metadata: {}", e),
            },
        )
    })
}

async fn handle_resolve_side_transfer_lane_request(
    view: ClientSegPoolView,
    req: ResolveSideTransferLaneReq,
) -> MsgPack<ResolveSideTransferLaneResp> {
    let self_info = view.cluster_manager().get_self_info();
    let peers_dir =
        ClientSegPool::side_transfer_peers_dir(&view.client_seg_pool().inner().share_mem_path);
    tracing::info!(
        "handle_resolve_side_transfer_lane_request: owner={} lane_idx={} peers_dir={}",
        self_info.id,
        req.lane_idx,
        peers_dir.to_string_lossy()
    );
    let entries = match std::fs::read_dir(&peers_dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            tracing::info!(
                "handle_resolve_side_transfer_lane_request: peers dir not found: owner={} lane_idx={} peers_dir={}",
                self_info.id,
                req.lane_idx,
                peers_dir.to_string_lossy()
            );
            return MsgPack {
                serialize_part: ResolveSideTransferLaneResp {
                    error_code: OK,
                    error_json: String::new(),
                    side_id: None,
                    target_base_addr: None,
                },
                raw_bytes: Vec::new(),
            };
        }
        Err(err) => {
            let err = KvError::SharedMem(
                crate::rpcresp_kvresult_convert::msg_and_error::SharedMemError::MetaDataLoadError {
                    path: peers_dir.to_string_lossy().to_string(),
                    detail: format!("Failed to list side-transfer peer dir: {}", err),
                },
            );
            return MsgPack {
                serialize_part: ResolveSideTransferLaneResp {
                    error_code: err.code(),
                    error_json: err.to_json(),
                    side_id: None,
                    target_base_addr: None,
                },
                raw_bytes: Vec::new(),
            };
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let Ok(meta) = read_side_transfer_peer_file(&path) else {
            tracing::warn!(
                "handle_resolve_side_transfer_lane_request: failed to parse peer file: owner={} lane_idx={} path={}",
                self_info.id,
                req.lane_idx,
                path.to_string_lossy()
            );
            continue;
        };
        tracing::info!(
            "handle_resolve_side_transfer_lane_request: inspecting peer file: owner={} lane_idx={} path={} meta={:?}",
            self_info.id,
            req.lane_idx,
            path.to_string_lossy(),
            meta
        );
        // Owner startup removes stale peer files before spawning side workers, so the peer-file
        // directory is already scoped to the current owner lifecycle. Matching on owner_id is
        // sufficient here and avoids false negatives when the owner's published node_start_time
        // lags the owner_ref embedded into the side peer file by a few seconds.
        if meta.owner_id != self_info.id {
            tracing::info!(
                "handle_resolve_side_transfer_lane_request: skip owner mismatch: owner={} lane_idx={} meta_owner={} side_id={}",
                self_info.id,
                req.lane_idx,
                meta.owner_id,
                meta.side_id
            );
            continue;
        }
        if meta.worker_idx() != Some(req.lane_idx) {
            tracing::info!(
                "handle_resolve_side_transfer_lane_request: skip lane mismatch: owner={} lane_idx={} meta_lane={:?} side_id={}",
                self_info.id,
                req.lane_idx,
                meta.worker_idx(),
                meta.side_id
            );
            continue;
        }
        let Some(target_base_addr) = meta.target_base_addr else {
            tracing::info!(
                "handle_resolve_side_transfer_lane_request: skip missing target_base_addr: owner={} lane_idx={} side_id={}",
                self_info.id,
                req.lane_idx,
                meta.side_id
            );
            continue;
        };
        // Peer files are written by the owner-controlled side worker only after shared-memory
        // attach and init3 are complete. Cluster membership propagation can still lag briefly, so
        // remote lane resolution should treat the peer file as authoritative and use membership as
        // a soft sanity check instead of a hard gate.
        if let Some(member) = view.cluster_manager().get_member_info_cached(&meta.side_id) {
            if member
                .metadata
                .get("side_transfer_worker")
                .is_some_and(|v| v == "true")
                == false
            {
                tracing::info!(
                    "handle_resolve_side_transfer_lane_request: skip cached member without side_transfer_worker marker: owner={} lane_idx={} side_id={} metadata={:?}",
                    self_info.id,
                    req.lane_idx,
                    meta.side_id,
                    member.metadata
                );
                continue;
            }
        }
        tracing::info!(
            "handle_resolve_side_transfer_lane_request: resolved lane: owner={} lane_idx={} side_id={} target_base_addr={:#x}",
            self_info.id,
            req.lane_idx,
            meta.side_id,
            target_base_addr
        );
        return MsgPack {
            serialize_part: ResolveSideTransferLaneResp {
                error_code: OK,
                error_json: String::new(),
                side_id: Some(meta.side_id),
                target_base_addr: Some(target_base_addr),
            },
            raw_bytes: Vec::new(),
        };
    }

    MsgPack {
        serialize_part: ResolveSideTransferLaneResp {
            error_code: OK,
            error_json: String::new(),
            side_id: None,
            target_base_addr: None,
        },
        raw_bytes: Vec::new(),
    }
}
