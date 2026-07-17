use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use etcd_client::{Client as EtcdClient, DeleteOptions};
use fluxon_commu::{EtcdPrefixScanAction, cluster_member_base_prefix, scan_etcd_prefix_paginated};
use fluxon_framework_compiled::shutdown::ViewShutdownExt;
use fluxon_fs_core::config::{
    FLUXON_FS_COMPONENT_METADATA_KEY, FLUXON_FS_CONFIG_ACCESS_MODEL_JSON_KEY,
    FLUXON_FS_CONTROL_SCHEMA_VERSION, FLUXON_FS_EXPORT_OVERLAY_JSON_KEY,
    FLUXON_FS_MOUNT_EXPORTS_JSON_KEY, FS_MASTER_AGENT_EXPORTS_PUSH_RPC_PATH,
    FS_MASTER_CONFIG_RPC_PATH, FS_MASTER_EXPORT_REGISTRY_RPC_PATH,
    FS_MASTER_MOUNT_REGISTRY_RPC_PATH, FluxonFsComponent, FluxonFsExport,
    FluxonFsExportRoutingMode, FluxonFsGlobalConfig, FluxonFsMasterConfig,
    FluxonFsMasterPanelConfig, FluxonFsRequestIdentity, FluxonFsS3KvMissPolicy,
    FsAgentExportSnapshotItemWire, admin_browse_export_for_agent_instance_key_v1,
    extract_cache_config_yaml_from_yaml_text, parse_cache_config_yaml,
    parse_master_config_from_yaml_text, parse_master_panel_config_from_yaml_text,
};
use fluxon_fs_core::path::relpath_from_abs_dirpath;
use fluxon_kv::config::ClientConfigYaml;
use fluxon_kv::rpcresp_kvresult_convert::msg_and_error::{
    ApiError as CoreApiError, KvError as CoreKvError, KvResult,
};
use fluxon_kv::user_api::FluxonUserApi;
use fluxon_kv::user_api::flat_dict::{FlatDict, FlatValue};
use fluxon_kv::{
    ClusterEvent, ClusterMember, ConfigArg, MembershipEventReceiver,
    run_client_with_startup_member_metadata,
};
use fluxon_util::etcd::{EtcdEndpointSet, ManagedEtcdClient};
use fluxon_util::run_async_from_sync::spawn_blocking_allow_sync_async_bridge;
use fluxon_util::{FluxonCliProxyDescriptorV2, FluxonCliProxyTransportV2};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use tokio::runtime::Handle;
use tokio::runtime::Runtime;

use crate::agent::{FluxonFsAgent, FsAgentError};

mod transfer_master;
use transfer_master::{
    TransferScanRuntimeState, register_transfer_result_and_heartbeat_rpc,
    start_transfer_reconcile_actor, start_transfer_scan_scheduler_actor,
    start_transfer_worker_scheduler_actor,
};
pub use transfer_master::{
    run_transfer_check_blocking, run_transfer_check_blocking_from_yaml_text,
};

const ETCD_PREFIX_FLUXON_CLI_PROXY_FS_S3: &str = "/fluxon_cli_proxy/v2/fs_s3";
const ETCD_PREFIX_FS_MOUNT_REGISTRY: &str = "/fluxon_fs_mount_registry";
const ETCD_PREFIX_FS_EXPORT_REGISTRY: &str = "/fluxon_fs_export_registry";

const EXPORT_REGISTRY_SYNC_RETRY_LOG_TICKS: u64 = 25;
const EXPORT_REGISTRY_SYNC_MAX_WAIT_SECS: u64 = 120;

fn export_registry_sync_budget_exhausted(waited: Duration) -> bool {
    waited >= Duration::from_secs(EXPORT_REGISTRY_SYNC_MAX_WAIT_SECS)
}

#[derive(Clone)]
struct AppState;

#[derive(Debug, Clone)]
struct ExportRegistry {
    // export_name -> (agent_instance_key -> record)
    exports: BTreeMap<String, BTreeMap<String, ExportRegistryRecord>>,
}

#[derive(Debug, Clone)]
struct ExportRegistryRecord {
    remote_root_dir_abs: String,
    export: FluxonFsExport,
    updated_unix_ms: i64,
}

#[derive(Clone)]
struct FsS3BackendAgent {
    agent: Arc<FluxonFsAgent>,
    kv_miss_policy: FluxonFsS3KvMissPolicy,
}

pub(crate) const TRANSFER_SCHEDULER_IDLE_SLEEP_MS: u64 = 1_000;
pub(crate) const TRANSFER_WORKER_LEASE_MS: i64 = 60_000;
pub(crate) const TRANSFER_HEARTBEAT_EXTENSION_MS: i64 = 60_000;
// Transfer control-plane RPCs scan directories, launch workers, and reconcile
// long-lived worker attempts. They are intentionally not bound to the generic
// 10s user-RPC default that serves small interactive control calls.
pub(crate) const TRANSFER_CONTROL_RPC_TIMEOUT_MS: u64 = 600_000;

fn s3_list_dir_result_from_agent<F>(
    export_name: Arc<str>,
    relpath: Arc<str>,
    result: Result<String, FsAgentError>,
    map_err: F,
) -> Result<Vec<fluxon_fs_s3_gateway::RemoteDirEntry>, fluxon_fs_s3_gateway::S3Error>
where
    F: FnOnce(Arc<str>, Arc<str>, FsAgentError) -> fluxon_fs_s3_gateway::S3Error,
{
    let entries_json = match result {
        Ok(v) => v,
        Err(FsAgentError::Os { errno, .. }) if errno == libc::ENOENT => return Ok(Vec::new()),
        Err(e) => return Err(map_err(export_name, relpath, e)),
    };
    serde_json::from_str(&entries_json).map_err(|e| fluxon_fs_s3_gateway::S3Error::Internal {
        detail: format!("parse entries_json failed: {}", e),
    })
}

fn s3_mkdir_result_from_agent<F>(
    export_name: Arc<str>,
    relpath: Arc<str>,
    result: Result<(), FsAgentError>,
    map_err: F,
) -> Result<(), fluxon_fs_s3_gateway::S3Error>
where
    F: FnOnce(Arc<str>, Arc<str>, FsAgentError) -> fluxon_fs_s3_gateway::S3Error,
{
    match result {
        Ok(()) => Ok(()),
        Err(FsAgentError::Os { errno, .. }) if errno == libc::EEXIST => Ok(()),
        Err(e) => Err(map_err(export_name, relpath, e)),
    }
}

fn map_fs_agent_error_to_s3_error(
    export_name: &str,
    relpath: &str,
    e: FsAgentError,
) -> fluxon_fs_s3_gateway::S3Error {
    match e {
        FsAgentError::InvalidArgument { detail } => {
            if detail.starts_with("unknown export_name:") || detail.starts_with("unknown export:") {
                return fluxon_fs_s3_gateway::S3Error::NoSuchBucket {
                    bucket: export_name.to_string(),
                };
            }
            fluxon_fs_s3_gateway::S3Error::InvalidRequest { detail }
        }
        FsAgentError::AccessDenied { .. } => fluxon_fs_s3_gateway::S3Error::AccessDenied {
            detail: format!(
                "remote fs permission denied: export={} relpath={} (scope_access does not allow this operation)",
                export_name, relpath
            ),
        },
        FsAgentError::Os { errno, .. } => fluxon_fs_s3_gateway::S3Error::Internal {
            detail: format!(
                "remote fs os error: errno={} export={} relpath={}",
                errno, export_name, relpath
            ),
        },
        FsAgentError::Kv(e) => fluxon_fs_s3_gateway::S3Error::Internal {
            detail: format!(
                "remote fs kv error: export={} relpath={} err={}",
                export_name, relpath, e
            ),
        },
        FsAgentError::Shutdown { detail } => fluxon_fs_s3_gateway::S3Error::Internal { detail },
        FsAgentError::Io { path, detail } => fluxon_fs_s3_gateway::S3Error::Internal {
            detail: format!(
                "remote fs io error: path={} detail={} export={} relpath={}",
                path, detail, export_name, relpath
            ),
        },
    }
}

impl FsS3BackendAgent {
    fn new(agent: Arc<FluxonFsAgent>, kv_miss_policy: FluxonFsS3KvMissPolicy) -> Self {
        Self {
            agent,
            kv_miss_policy,
        }
    }

    fn s3_path_for_err(export_name: &str, relpath: &str) -> String {
        // English note: keep it human-readable for logs; it is not a real URI.
        format!("s3://{}/{}", export_name, relpath)
    }

    fn map_agent_err(
        &self,
        export_name: Arc<str>,
        relpath: Arc<str>,
        e: FsAgentError,
    ) -> fluxon_fs_s3_gateway::S3Error {
        map_fs_agent_error_to_s3_error(export_name.as_ref(), relpath.as_ref(), e)
    }
}

impl fluxon_fs_s3_gateway::FsS3Backend for FsS3BackendAgent {
    fn ensure_export_config(
        &self,
        export_name: &str,
        export: &FluxonFsExport,
    ) -> Result<(), String> {
        self.agent
            .upsert_export_cfg(export_name.to_string(), export.clone())
            .map_err(|e| {
                format!(
                    "upsert export config into master backend agent failed: export={} err={}",
                    export_name, e
                )
            })
    }

    fn stat(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<fluxon_fs_s3_gateway::RemoteStat, fluxon_fs_s3_gateway::S3Error>>
    {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_stat_by_handle_s3_gateway_with_identity(
                    export2.as_ref(),
                    rel2.as_ref(),
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            let st = match j {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return Err(this.map_agent_err(export_name, relpath, e)),
                Err(e) => {
                    return Err(fluxon_fs_s3_gateway::S3Error::Internal {
                        detail: format!("spawn_blocking join failed: {}", e),
                    });
                }
            };
            Ok(fluxon_fs_s3_gateway::RemoteStat {
                exists: st.exists,
                is_file: st.is_file,
                is_dir: st.is_dir,
                size: st.size,
                mtime_ns: st.mtime_ns,
            })
        })
    }

    fn stat_on_exporter(
        &self,
        request_identity: FluxonFsRequestIdentity,
        exporter_id: Arc<str>,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<fluxon_fs_s3_gateway::RemoteStat, fluxon_fs_s3_gateway::S3Error>>
    {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let exporter2 = exporter_id.clone();
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_stat_via_exporter_s3_gateway_with_identity(
                    exporter2.as_ref(),
                    export2.as_ref(),
                    rel2.as_ref(),
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            let st = match j {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return Err(this.map_agent_err(export_name, relpath, e)),
                Err(e) => {
                    return Err(fluxon_fs_s3_gateway::S3Error::Internal {
                        detail: format!("spawn_blocking join failed: {}", e),
                    });
                }
            };
            Ok(fluxon_fs_s3_gateway::RemoteStat {
                exists: st.exists,
                is_file: st.is_file,
                is_dir: st.is_dir,
                size: st.size,
                mtime_ns: st.mtime_ns,
            })
        })
    }

    fn list_dir(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<
        'static,
        Result<Vec<fluxon_fs_s3_gateway::RemoteDirEntry>, fluxon_fs_s3_gateway::S3Error>,
    > {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_list_dir_json_by_handle_s3_gateway_with_identity(
                    export2.as_ref(),
                    rel2.as_ref(),
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            let entries_json = match j {
                Ok(v) => v,
                Err(e) => {
                    return Err(fluxon_fs_s3_gateway::S3Error::Internal {
                        detail: format!("spawn_blocking join failed: {}", e),
                    });
                }
            };
            s3_list_dir_result_from_agent(
                export_name,
                relpath,
                entries_json,
                |export_name, relpath, e| this.map_agent_err(export_name, relpath, e),
            )
        })
    }

    fn read_chunk_cached(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
        offset: i64,
        length: i64,
        file_size: i64,
        mtime_ns: i64,
    ) -> BoxFuture<'static, Result<Vec<u8>, fluxon_fs_s3_gateway::S3Error>> {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let kv_miss_policy = this.kv_miss_policy;
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_read_chunk_by_handle_s3_gateway_with_identity(
                    export2.as_ref(),
                    rel2.as_ref(),
                    offset,
                    length,
                    file_size,
                    mtime_ns,
                    true,
                    kv_miss_policy,
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            match j {
                Ok(Ok(v)) => Ok(v),
                Ok(Err(e)) => Err(this.map_agent_err(export_name, relpath, e)),
                Err(e) => Err(fluxon_fs_s3_gateway::S3Error::Internal {
                    detail: format!("spawn_blocking join failed: {}", e),
                }),
            }
        })
    }

    fn write_chunk(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
        offset: i64,
        data: Vec<u8>,
    ) -> BoxFuture<'static, Result<(), fluxon_fs_s3_gateway::S3Error>> {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            if data.len() > fluxon_fs_core::s3_gateway::FS_S3_OBJECT_PIECE_BYTES {
                return Err(fluxon_fs_s3_gateway::S3Error::InvalidRequest {
                    detail: "write chunk too large".to_string(),
                });
            }
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_write_chunk_by_handle_s3_gateway_with_identity(
                    export2.as_ref(),
                    rel2.as_ref(),
                    offset,
                    data,
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            match j {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(this.map_agent_err(export_name, relpath, e)),
                Err(e) => Err(fluxon_fs_s3_gateway::S3Error::Internal {
                    detail: format!("spawn_blocking join failed: {}", e),
                }),
            }
        })
    }

    fn truncate(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
        size: i64,
    ) -> BoxFuture<'static, Result<(), fluxon_fs_s3_gateway::S3Error>> {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_truncate_by_handle_s3_gateway_with_identity(
                    export2.as_ref(),
                    rel2.as_ref(),
                    size,
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            match j {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(this.map_agent_err(export_name, relpath, e)),
                Err(e) => Err(fluxon_fs_s3_gateway::S3Error::Internal {
                    detail: format!("spawn_blocking join failed: {}", e),
                }),
            }
        })
    }

    fn mkdir(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
        mode: i64,
    ) -> BoxFuture<'static, Result<(), fluxon_fs_s3_gateway::S3Error>> {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_mkdir_by_handle_s3_gateway_with_identity(
                    export2.as_ref(),
                    rel2.as_ref(),
                    mode,
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            match j {
                Ok(v) => s3_mkdir_result_from_agent(
                    export_name,
                    relpath,
                    v,
                    |export_name, relpath, e| this.map_agent_err(export_name, relpath, e),
                ),
                Err(e) => Err(fluxon_fs_s3_gateway::S3Error::Internal {
                    detail: format!("spawn_blocking join failed: {}", e),
                }),
            }
        })
    }

    fn rename(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        src_relpath: Arc<str>,
        dst_relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), fluxon_fs_s3_gateway::S3Error>> {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = format!(
                "{} -> {}",
                Self::s3_path_for_err(export_name.as_ref(), src_relpath.as_ref()),
                Self::s3_path_for_err(export_name.as_ref(), dst_relpath.as_ref())
            );
            let export2 = export_name.clone();
            let src2 = src_relpath.clone();
            let dst2 = dst_relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_rename_by_handle_with_identity(
                    export2.as_ref(),
                    src2.as_ref(),
                    dst2.as_ref(),
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            match j {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(this.map_agent_err(export_name, src_relpath, e)),
                Err(e) => Err(fluxon_fs_s3_gateway::S3Error::Internal {
                    detail: format!("spawn_blocking join failed: {}", e),
                }),
            }
        })
    }

    fn unlink(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), fluxon_fs_s3_gateway::S3Error>> {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_unlink_by_handle_s3_gateway_with_identity(
                    export2.as_ref(),
                    rel2.as_ref(),
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            match j {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(this.map_agent_err(export_name, relpath, e)),
                Err(e) => Err(fluxon_fs_s3_gateway::S3Error::Internal {
                    detail: format!("spawn_blocking join failed: {}", e),
                }),
            }
        })
    }

    fn rmdir(
        &self,
        request_identity: FluxonFsRequestIdentity,
        export_name: Arc<str>,
        relpath: Arc<str>,
    ) -> BoxFuture<'static, Result<(), fluxon_fs_s3_gateway::S3Error>> {
        let this = self.clone();
        let request_identity = Some(request_identity);
        Box::pin(async move {
            let path_for_err = Self::s3_path_for_err(export_name.as_ref(), relpath.as_ref());
            let export2 = export_name.clone();
            let rel2 = relpath.clone();
            let agent = this.agent.clone();
            let j = spawn_blocking_allow_sync_async_bridge(move || {
                agent.remote_rmdir_by_handle_s3_gateway_with_identity(
                    export2.as_ref(),
                    rel2.as_ref(),
                    &path_for_err,
                    request_identity.as_ref(),
                )
            })
            .await;
            match j {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(this.map_agent_err(export_name, relpath, e)),
                Err(e) => Err(fluxon_fs_s3_gateway::S3Error::Internal {
                    detail: format!("spawn_blocking join failed: {}", e),
                }),
            }
        })
    }
}

#[derive(Clone)]
struct FsMasterAdminBackendLive {
    cluster_name: String,
    etcd: Arc<tokio::sync::Mutex<EtcdClient>>,
    s3_agent: Arc<FluxonFsAgent>,
}

impl fluxon_fs_s3_gateway::FsMasterAdminBackend for FsMasterAdminBackendLive {
    fn list_fs_master_members(
        &self,
    ) -> BoxFuture<'static, Result<Vec<fluxon_fs_s3_gateway::FsMasterMemberRecord>, String>> {
        let cluster_name = self.cluster_name.clone();
        let etcd = self.etcd.clone();
        Box::pin(async move {
            let members = fetch_fs_members_snapshot(etcd, &cluster_name)
                .await
                .map_err(|e| format!("fetch fs members failed: {}", e))?;
            Ok(members
                .into_iter()
                .map(fs_master_member_record_from_local)
                .collect())
        })
    }

    fn list_fs_master_online_member_ids(
        &self,
    ) -> BoxFuture<'static, Result<BTreeSet<String>, String>> {
        let cluster_name = self.cluster_name.clone();
        let etcd = self.etcd.clone();
        Box::pin(async move {
            fetch_member_ids_snapshot(etcd, &cluster_name)
                .await
                .map_err(|e| format!("fetch fs online member ids failed: {}", e))
        })
    }

    fn list_fs_master_agent_dir(
        &self,
        agent_instance_key: String,
        dir_abs: String,
    ) -> BoxFuture<'static, Result<Vec<fluxon_fs_s3_gateway::FsMasterAdminBrowseDirEntry>, String>>
    {
        let s3_agent = self.s3_agent.clone();
        Box::pin(async move {
            if agent_instance_key.trim().is_empty() {
                return Err("agent_instance_key must be non-empty".to_string());
            }
            let relpath = relpath_from_abs_dirpath(dir_abs.as_str())
                .map_err(|e| format!("invalid admin browse dir_abs: {}", e))?;
            let (export_name, export) =
                admin_browse_export_for_agent_instance_key_v1(agent_instance_key.as_str());
            s3_agent
                .upsert_export_cfg(export_name.clone(), export)
                .map_err(|e| format!("upsert admin browse export failed: {}", e))?;
            let path_for_err = format!(
                "admin browse agent={} dir_abs={}",
                agent_instance_key, dir_abs
            );
            let export_name2 = export_name.clone();
            let relpath2 = relpath.clone();
            let entries_json = spawn_blocking_allow_sync_async_bridge(move || {
                s3_agent.remote_list_dir_json_by_handle(
                    export_name2.as_str(),
                    relpath2.as_str(),
                    &path_for_err,
                )
            })
            .await
            .map_err(|e| format!("admin browse join failed: {}", e))?
            .map_err(|e| format!("admin browse list_dir failed: {}", e))?;
            let mut entries: Vec<fluxon_fs_s3_gateway::RemoteDirEntry> =
                serde_json::from_str(&entries_json)
                    .map_err(|e| format!("admin browse parse entries_json failed: {}", e))?;
            entries.sort_by(|a, b| (!a.is_dir, a.name.clone()).cmp(&(!b.is_dir, b.name.clone())));
            Ok(entries
                .into_iter()
                .map(|entry| fluxon_fs_s3_gateway::FsMasterAdminBrowseDirEntry {
                    name: entry.name,
                    is_file: entry.is_file,
                    is_dir: entry.is_dir,
                })
                .collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_list_dir_missing_prefix_maps_to_empty_entries() {
        let result = s3_list_dir_result_from_agent(
            Arc::from("demo"),
            Arc::from("missing-prefix"),
            Err(FsAgentError::os(
                libc::ENOENT,
                "/tmp/demo/missing-prefix",
                "not found",
            )),
            |_export_name, _relpath, _e| panic!("ENOENT should not be mapped as an S3 error"),
        )
        .unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn s3_mkdir_existing_prefix_maps_to_ok() {
        s3_mkdir_result_from_agent(
            Arc::from("demo"),
            Arc::from("existing-prefix"),
            Err(FsAgentError::os(
                libc::EEXIST,
                "/tmp/demo/existing-prefix",
                "already exists",
            )),
            |_export_name, _relpath, _e| panic!("EEXIST should not be mapped as an S3 error"),
        )
        .unwrap();
    }

    #[test]
    fn remote_scope_access_denied_maps_to_s3_access_denied() {
        let err = map_fs_agent_error_to_s3_error(
            "demo",
            ".",
            FsAgentError::AccessDenied {
                path: "s3://demo/.".to_string(),
                detail: "fs list_dir denied: username=alice export_name=demo relpath=.".to_string(),
            },
        );
        match err {
            fluxon_fs_s3_gateway::S3Error::AccessDenied { detail } => {
                assert!(detail.contains("remote fs permission denied"));
                assert!(detail.contains("scope_access does not allow this operation"));
            }
            other => panic!("expected AccessDenied, got {:?}", other),
        }
    }

    #[test]
    fn host_os_eacces_stays_internal() {
        let err = map_fs_agent_error_to_s3_error(
            "demo",
            ".",
            FsAgentError::os(libc::EACCES, "/srv/demo", "Permission denied (os)"),
        );
        match err {
            fluxon_fs_s3_gateway::S3Error::Internal { detail } => {
                assert!(detail.contains("remote fs os error: errno=13"));
            }
            other => panic!("expected Internal, got {:?}", other),
        }
    }

    #[test]
    fn export_registry_sync_budget_exhausts_at_limit() {
        assert!(!export_registry_sync_budget_exhausted(Duration::from_secs(
            EXPORT_REGISTRY_SYNC_MAX_WAIT_SECS - 1,
        )));
        assert!(export_registry_sync_budget_exhausted(Duration::from_secs(
            EXPORT_REGISTRY_SYNC_MAX_WAIT_SECS,
        )));
    }

    #[test]
    fn fs_agent_membership_requires_structured_component_metadata() {
        let member = ClusterMember {
            id: "agent-a".to_string(),
            addresses: Vec::new(),
            port: None,
            node_start_time: 0,
            metadata: std::collections::HashMap::from([
                ("external_client".to_string(), "true".to_string()),
                (
                    FLUXON_FS_COMPONENT_METADATA_KEY.to_string(),
                    FluxonFsComponent::Agent.as_metadata_value().to_string(),
                ),
                (
                    "cmd".to_string(),
                    "python -m fluxon_py.runtime.start_fs_agent".to_string(),
                ),
            ]),
            sub_cluster: None,
            network: None,
        };
        assert!(is_fs_agent_cluster_member(&member));

        let no_structured_marker = ClusterMember {
            id: "fluxon_fs_agent_like_name".to_string(),
            addresses: Vec::new(),
            port: None,
            node_start_time: 0,
            metadata: std::collections::HashMap::from([
                ("external_client".to_string(), "true".to_string()),
                (
                    "cmd".to_string(),
                    "python -m fluxon_py.runtime.start_fs_agent".to_string(),
                ),
            ]),
            sub_cluster: None,
            network: None,
        };
        assert!(!is_fs_agent_cluster_member(&no_structured_marker));
    }
}

fn redirect_response(location: &str) -> Response {
    axum::response::Redirect::to(location).into_response()
}

pub fn run_master_blocking(config_path: &str, workdir: &str) -> anyhow::Result<()> {
    if config_path.trim().is_empty() {
        anyhow::bail!("config_path must be non-empty");
    }
    if workdir.trim().is_empty() {
        anyhow::bail!("workdir must be non-empty");
    }
    let config_path = PathBuf::from(config_path);
    let workdir = PathBuf::from(workdir);
    if !config_path.exists() {
        anyhow::bail!("config not found: {}", config_path.display());
    }
    if !workdir.exists() {
        anyhow::bail!("workdir not found: {}", workdir.display());
    }
    std::env::set_current_dir(&workdir)
        .with_context(|| format!("set workdir failed: {}", workdir.display()))?;

    let raw = std::fs::read_to_string(&config_path)
        .with_context(|| format!("read config: {}", config_path.display()))?;
    run_master_blocking_from_yaml_text(&raw)
}

pub fn run_master_blocking_from_yaml_text(raw: &str) -> anyhow::Result<()> {
    if raw.trim().is_empty() {
        anyhow::bail!("config yaml must be non-empty");
    }

    let master_cfg =
        parse_master_config_from_yaml_text(raw).map_err(|e| anyhow::anyhow!("{}", e))?;
    let panel_cfg =
        parse_master_panel_config_from_yaml_text(raw).map_err(|e| anyhow::anyhow!("{}", e))?;
    let cache_yaml =
        extract_cache_config_yaml_from_yaml_text(raw).map_err(|e| anyhow::anyhow!("{}", e))?;
    let fs_cache = parse_cache_config_yaml(&cache_yaml).map_err(|e| anyhow::anyhow!("{}", e))?;
    fluxon_fs_s3_gateway::validate_exports_bucket_names(&fs_cache)?;
    let pull_interval_ms = master_cfg
        .pull_interval_ms
        .with_context(|| "fluxon_fs.master.pull_interval_ms is required for fluxon_fs master")?;

    let kv_yaml = extract_kvclient_config_yaml_from_fluxon_config(raw)?;
    let kv_cfg = kv_yaml.verify().map_err(|e| anyhow::anyhow!("{}", e))?;
    if kv_cfg.instance_key.to_string() != master_cfg.instance_key {
        anyhow::bail!(
            "kvclient.instance_key must match fluxon_fs.master.instance_key (got kvclient.instance_key={:?} fluxon_fs.master.instance_key={:?})",
            kv_cfg.instance_key,
            master_cfg.instance_key
        );
    }

    // Ensure external client mode; FS master is a config/registry publisher, not a contributing data node.
    let dram = kv_cfg.contribute_to_cluster_pool_size.dram;
    let vram_is_zero = kv_cfg
        .contribute_to_cluster_pool_size
        .vram
        .values()
        .all(|v| *v == 0);
    if !(dram == 0 && vram_is_zero) {
        anyhow::bail!(
            "kvclient must be zero-contribution (external client) mode for fluxon_fs master"
        );
    }

    let rt = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .with_context(|| "build tokio runtime")?,
    );
    let rt2 = rt.clone();
    let res = rt.as_ref().block_on(async move {
        async_main(
            rt2,
            kv_cfg,
            master_cfg,
            panel_cfg,
            cache_yaml,
            fs_cache,
            pull_interval_ms,
            false,
        )
        .await
    });

    // Causal chain:
    // - When initialization fails early (e.g. port conflict), the async future returns quickly.
    // - Dropping a Tokio runtime may block indefinitely while waiting for blocking tasks to stop.
    // - For service-style entrypoints, failing fast is preferable to hanging on runtime drop.
    if let Ok(rt0) = Arc::try_unwrap(rt) {
        rt0.shutdown_background();
    }

    res
}

async fn async_main(
    rt: Arc<Runtime>,
    kv_cfg: fluxon_kv::config::ClientConfig,
    master_cfg: FluxonFsMasterConfig,
    panel_cfg: FluxonFsMasterPanelConfig,
    cache_yaml: String,
    fs_cache: FluxonFsGlobalConfig,
    pull_interval_ms: u64,
    transfer_check_only: bool,
) -> anyhow::Result<()> {
    let startup_member_metadata = HashMap::from([(
        FLUXON_FS_COMPONENT_METADATA_KEY.to_string(),
        FluxonFsComponent::Controller
            .as_metadata_value()
            .to_string(),
    )]);
    let (kv_framework, _client_cfg2) =
        run_client_with_startup_member_metadata(ConfigArg::Config(kv_cfg), startup_member_metadata)
            .await
            .with_context(|| "start kvclient (external)")?;

    let fs_framework =
        crate::new_fs_framework(format!("fluxon_fs.master:{}", master_cfg.instance_key));

    let cluster_name = kv_framework
        .cluster_manager_view()
        .cluster_manager()
        .cluster_name()
        .to_string();
    let endpoints = kv_framework
        .cluster_manager_view()
        .cluster_manager()
        .etcd_endpoints();
    let etcd_backend = ManagedEtcdClient::acquire(EtcdEndpointSet::new(endpoints.clone())?);

    let rt_handle = rt.handle().clone();
    let api = Arc::new(
        FluxonUserApi::new(kv_framework.clone(), rt_handle.clone())
            .map_err(|e| anyhow::anyhow!("{}", e))?,
    );

    let export_registry = Arc::new(Mutex::new(ExportRegistry {
        exports: BTreeMap::new(),
    }));

    let s3_kv_miss_policy = panel_cfg.s3_gateway.kv_miss_policy;
    let s3_cfg = panel_cfg.s3_gateway.clone();

    let s3_agent_api = FluxonUserApi::new(kv_framework.clone(), rt_handle.clone())
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    let s3_agent = Arc::new(FluxonFsAgent::new(
        fs_framework.clone(),
        kv_framework.clone(),
        s3_agent_api,
        rt_handle.clone(),
    ));
    s3_agent
        .set_cache_config_yaml(&cache_yaml)
        .map_err(|e| anyhow::anyhow!("{}", e))?;
    s3_agent.set_master_config(master_cfg.clone());
    let s3_backend = Arc::new(FsS3BackendAgent::new(s3_agent, s3_kv_miss_policy));

    let etcd2 = Arc::new(tokio::sync::Mutex::new(
        etcd_backend
            .client()
            .await
            .with_context(|| "connect etcd (panel runtime)")?,
    ));

    let s3_agent_for_admin = s3_backend.agent.clone();
    // Only enable transfer history queries when transfer state is configured.
    let transfer_history_query = if panel_cfg.transfer_state_store.is_some() {
        Some(fluxon_fs_s3_gateway::TransferHistoryQueryConfig {
            prometheus_base_url: panel_cfg.prometheus_base_url.clone(),
        })
    } else {
        None
    };
    let transfer_history_metrics_handle = if transfer_history_query.is_some()
        && !kv_framework
            .metric_reporter_view()
            .metric_reporter()
            .observability_disabled()
    {
        Some(
            kv_framework
                .metric_reporter_view()
                .metric_reporter()
                .metrics_handle(),
        )
    } else {
        None
    };
    let s3_state = Arc::new(
        fluxon_fs_s3_gateway::GatewayState::new(
            cluster_name.clone(),
            "/fs_s3".to_string(),
            fluxon_fs_s3_gateway::GatewayAccessConfig {
                access_db_path: panel_cfg.access_db_path.clone(),
                bootstrap_access_model: panel_cfg.bootstrap_access_model.clone(),
                transfer_state_store: panel_cfg.transfer_state_store.clone(),
            },
            Arc::new(fs_cache.clone()),
            s3_cfg,
            s3_backend.clone(),
            Arc::new(FsMasterAdminBackendLive {
                cluster_name: cluster_name.clone(),
                etcd: etcd2.clone(),
                s3_agent: s3_agent_for_admin,
            }),
            transfer_history_metrics_handle,
            transfer_history_query,
        )
        .map_err(|e| anyhow::anyhow!("{}", e))?,
    );
    let transfer_scan_runtime_state = Arc::new(Mutex::new(TransferScanRuntimeState::default()));
    if s3_state.transfer_feature_enabled() {
        start_transfer_scan_scheduler_actor(
            rt_handle.clone(),
            api.clone(),
            s3_state.clone(),
            transfer_scan_runtime_state.clone(),
        );
        start_transfer_reconcile_actor(rt_handle.clone(), s3_state.clone());
        start_transfer_worker_scheduler_actor(rt_handle.clone(), api.clone(), s3_state.clone());
    }

    register_fs_master_rpc(
        api.clone(),
        &cache_yaml,
        export_registry.clone(),
        s3_state.clone(),
        pull_interval_ms,
        transfer_scan_runtime_state.clone(),
    )?;
    register_mount_registry_rpc(
        rt_handle.clone(),
        &cluster_name,
        etcd_backend.clone(),
        api.clone(),
        s3_state.clone(),
    )?;
    register_export_registry_rpc(api.clone(), export_registry.clone())?;
    register_agent_exports_push_rpc(
        rt_handle.clone(),
        api.clone(),
        export_registry.clone(),
        cluster_name.clone(),
        etcd_backend.clone(),
        s3_state.clone(),
    )?;

    let mut etcd = etcd_backend
        .client()
        .await
        .with_context(|| "connect etcd")?;

    cleanup_legacy_export_registry_keys(&mut etcd, &cluster_name).await?;
    start_export_registry_manager(
        rt_handle.clone(),
        api.clone(),
        kv_framework.clone(),
        export_registry.clone(),
        cluster_name.clone(),
        etcd_backend,
        s3_state.clone(),
        FLUXON_FS_CONTROL_SCHEMA_VERSION,
        pull_interval_ms,
    );

    if transfer_check_only {
        let mut shutdown_waiter = fs_framework.register_shutdown_waiter();
        shutdown_waiter.wait().await;
        return Ok(());
    }

    publish_panel_descriptors(&mut etcd, &cluster_name, &panel_cfg).await?;

    let s3_root_state = s3_state.clone();
    let s3_root_handler = move |req: Request<Body>| {
        let s3_root_state = s3_root_state.clone();
        async move { fluxon_fs_s3_gateway::handle_external_request(s3_root_state, req).await }
    };

    let st = Arc::new(AppState);

    let app = Router::new()
        .route("/", get(panel_view_html))
        .route("/view", get(panel_view_html))
        .route("/cli", get(panel_view_cli))
        .route("/fs_s3", any(s3_root_handler.clone()))
        .route("/fs_s3/", any(s3_root_handler.clone()))
        .route("/fs_s3/*path", any(s3_root_handler))
        .with_state(st);

    let addr: SocketAddr = panel_cfg
        .listen_addr
        .parse()
        .with_context(|| format!("invalid listen_addr: {}", panel_cfg.listen_addr))?;
    tracing::info!(
        "fluxon_fs master listening: addr={} cluster={}",
        addr,
        cluster_name
    );
    let poller = fs_framework.register_shutdown_poller();
    let mut shutdown_waiter = fs_framework.register_shutdown_waiter();
    let shutdown_fut = async move {
        shutdown_waiter.wait().await;
    };

    let server = axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_fut);
    tokio::pin!(server);

    tokio::select! {
        res = &mut server => {
            // If the HTTP server ends unexpectedly while the framework is still running,
            // force a full framework shutdown to stop all background tasks consistently.
            if poller.is_running() {
                let _ = fs_framework.shutdown().await;
            }
            res.with_context(|| "serve http")?;
        }
        _ = fs_framework.wait_shutdown_signal() => {
            fs_framework
                .shutdown()
                .await
                .map_err(|e| anyhow::anyhow!("framework shutdown failed: {}", e))?;
            server.await.with_context(|| "serve http")?;
        }
    }

    kv_framework
        .shutdown()
        .await
        .map_err(|e| anyhow::anyhow!("kv framework shutdown failed: {}", e))?;
    Ok(())
}

fn extract_kvclient_config_yaml_from_fluxon_config(raw: &str) -> anyhow::Result<ClientConfigYaml> {
    let v: serde_yaml::Value = serde_yaml::from_str(raw).with_context(|| "parse config yaml")?;
    let top = v.as_mapping().context("config file must be a mapping")?;
    let kv = top
        .get(&serde_yaml::Value::String("kvclient".to_string()))
        .context("config must include kvclient mapping")?;
    serde_yaml::from_value(kv.clone()).with_context(|| "parse kvclient yaml")
}

async fn publish_panel_descriptors(
    etcd: &mut EtcdClient,
    cluster_name: &str,
    panel_cfg: &FluxonFsMasterPanelConfig,
) -> anyhow::Result<()> {
    let base_url = panel_cfg.public_base_url.trim().to_string();
    if base_url.is_empty() || !base_url.contains("://") {
        anyhow::bail!(
            "invalid fs panel public_base_url (must include scheme): {}",
            base_url
        );
    }
    let base_s3 = format!("{}/fs_s3", base_url.trim_end_matches('/'));
    let key_s3 = format!(
        "{}/{}/descriptor",
        ETCD_PREFIX_FLUXON_CLI_PROXY_FS_S3, cluster_name
    );
    let desc_s3 = FluxonCliProxyDescriptorV2 {
        transport: FluxonCliProxyTransportV2::Http { base_url: base_s3 },
        allow_prefixes: vec!["/".to_string()],
        html_inject: false,
    };
    etcd.put(
        key_s3.clone(),
        serde_json::to_string(&desc_s3).unwrap(),
        None,
    )
    .await
    .with_context(|| format!("etcd put descriptor: {}", key_s3))?;

    tracing::info!(
        "Published fs_s3 panel proxy descriptor: fs_s3_key={} base_url={}",
        key_s3,
        base_url
    );
    Ok(())
}

fn register_fs_master_rpc(
    api: Arc<FluxonUserApi>,
    cache_yaml: &str,
    export_registry: Arc<Mutex<ExportRegistry>>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    pull_interval_ms: u64,
    transfer_scan_runtime_state: Arc<Mutex<TransferScanRuntimeState>>,
) -> KvResult<()> {
    let yaml_text = cache_yaml.to_string();
    let control_state = s3_state.clone();
    let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync + 'static> =
        Arc::new(move |from_node_id, payload| {
            let got = payload.get("schema_version");
            let got_i64 = match got {
                Some(FlatValue::Int64(v)) => *v,
                _ => {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "schema_version must be int64".to_string(),
                    }));
                }
            };
            if got_i64 != FLUXON_FS_CONTROL_SCHEMA_VERSION {
                return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: format!(
                        "schema_version mismatch: expected={} got={}",
                        FLUXON_FS_CONTROL_SCHEMA_VERSION, got_i64
                    ),
                }));
            }
            let access_model_json = control_state.access_model_json_text().map_err(|detail| {
                CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: format!("build access_model from gateway state failed: {}", detail),
                })
            })?;
            let overlay_json = control_state
                .load_fs_export_overlay_for_agent(from_node_id.as_str())
                .and_then(|overlay| {
                    serde_json::to_string(&overlay).map_err(|e| {
                        format!(
                            "serialize export overlay for agent={} failed: {}",
                            from_node_id, e
                        )
                    })
                })
                .map_err(|detail| CoreKvError::Api(CoreApiError::InvalidArgument { detail }))?;
            let mount_exports_json = {
                let reg = export_registry.lock();
                serde_json::to_string(&build_mount_exports_from_registry_locked(&reg)).map_err(
                    |e| {
                        CoreKvError::Api(CoreApiError::InvalidArgument {
                            detail: format!("serialize mount export view failed: {}", e),
                        })
                    },
                )?
            };
            let mut out: FlatDict = FlatDict::new();
            out.insert(
                "schema_version".to_string(),
                FlatValue::Int64(FLUXON_FS_CONTROL_SCHEMA_VERSION),
            );
            out.insert(
                "config_yaml".to_string(),
                FlatValue::String(yaml_text.clone()),
            );
            out.insert(
                "pull_interval_ms".to_string(),
                FlatValue::Int64(pull_interval_ms as i64),
            );
            out.insert(
                FLUXON_FS_CONFIG_ACCESS_MODEL_JSON_KEY.to_string(),
                FlatValue::String(access_model_json),
            );
            out.insert(
                FLUXON_FS_EXPORT_OVERLAY_JSON_KEY.to_string(),
                FlatValue::String(overlay_json),
            );
            out.insert(
                FLUXON_FS_MOUNT_EXPORTS_JSON_KEY.to_string(),
                FlatValue::String(mount_exports_json),
            );
            Ok(out)
        });
    api.rpc_server()
        .register(FS_MASTER_CONFIG_RPC_PATH, handler)?;
    register_transfer_result_and_heartbeat_rpc(
        api.clone(),
        s3_state.clone(),
        transfer_scan_runtime_state,
    )?;
    Ok(())
}

fn exports_match_for_registry(lhs: &FluxonFsExport, rhs: &FluxonFsExport) -> bool {
    lhs.routing_mode == rhs.routing_mode
        && lhs.nodes == rhs.nodes
        && lhs.cache_kv_key_prefix == rhs.cache_kv_key_prefix
        && lhs.cache_bytes_field_key == rhs.cache_bytes_field_key
        && lhs.cache_max_bytes == rhs.cache_max_bytes
        && lhs.rpc_paths == rhs.rpc_paths
}

fn validate_snapshot_export_item(export_name: &str, export: &FluxonFsExport) -> Result<(), String> {
    if export_name.trim().is_empty() || export_name.contains('/') {
        return Err(format!(
            "invalid export_name in snapshot: export={}",
            export_name
        ));
    }
    if export.remote_root_dir_abs.trim().is_empty()
        || !Path::new(&export.remote_root_dir_abs).is_absolute()
    {
        return Err(format!(
            "invalid remote_root_dir_abs in snapshot: export={} remote_root_dir_abs={}",
            export_name, export.remote_root_dir_abs
        ));
    }
    match export.routing_mode {
        FluxonFsExportRoutingMode::StaticNodes => {
            if export.nodes.is_empty() {
                return Err(format!(
                    "invalid snapshot export nodes: export={} routing_mode=static_nodes requires non-empty nodes",
                    export_name
                ));
            }
            for node in &export.nodes {
                if node.trim().is_empty() {
                    return Err(format!(
                        "invalid snapshot export nodes: export={} contains empty node id",
                        export_name
                    ));
                }
            }
        }
        FluxonFsExportRoutingMode::AgentRegistry => {
            if !export.nodes.is_empty() {
                return Err(format!(
                    "invalid snapshot export nodes: export={} routing_mode=agent_registry requires empty nodes",
                    export_name
                ));
            }
        }
    }
    for (field, value) in [
        ("stat", export.rpc_paths.stat.as_str()),
        ("list_dir", export.rpc_paths.list_dir.as_str()),
        ("read_chunk", export.rpc_paths.read_chunk.as_str()),
        ("write_chunk", export.rpc_paths.write_chunk.as_str()),
        ("truncate", export.rpc_paths.truncate.as_str()),
        ("mkdir", export.rpc_paths.mkdir.as_str()),
        ("rmdir", export.rpc_paths.rmdir.as_str()),
        ("unlink", export.rpc_paths.unlink.as_str()),
        ("rename", export.rpc_paths.rename.as_str()),
        ("chmod", export.rpc_paths.chmod.as_str()),
        ("utime", export.rpc_paths.utime.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!(
                "invalid snapshot export rpc_paths.{}: export={}",
                field, export_name
            ));
        }
    }
    Ok(())
}

fn db_records_from_snapshot_items(
    agent_instance_key: &str,
    items: &[FsAgentExportSnapshotItemWire],
    updated_unix_ms: i64,
) -> Vec<fluxon_fs_s3_gateway::FsExportRegistryRecord> {
    items
        .iter()
        .map(|item| fluxon_fs_s3_gateway::FsExportRegistryRecord {
            export_name: item.export_name.clone(),
            agent_instance_key: agent_instance_key.to_string(),
            remote_root_dir_abs: item.export.remote_root_dir_abs.clone(),
            export: item.export.clone(),
            updated_unix_ms,
        })
        .collect()
}

fn apply_agent_snapshot_items_locked(
    reg: &mut ExportRegistry,
    agent_instance_key: &str,
    items: &[FsAgentExportSnapshotItemWire],
    updated_unix_ms: i64,
) -> Result<(), String> {
    for item in items {
        let export_name = item.export_name.as_str();
        let export = &item.export;
        if let Some(existing_by_agent) = reg.exports.get(export_name) {
            for (other_agent_instance_key, existing) in existing_by_agent {
                if other_agent_instance_key == agent_instance_key {
                    continue;
                }
                if !exports_match_for_registry(&existing.export, export) {
                    return Err(format!(
                        "conflicting export definition across agents: export={} agent_instance_key={} other_agent_instance_key={}",
                        export_name, agent_instance_key, other_agent_instance_key
                    ));
                }
            }
        }
    }

    remove_agent_from_export_registry_locked(reg, agent_instance_key);
    for item in items {
        reg.exports
            .entry(item.export_name.clone())
            .or_default()
            .insert(
                agent_instance_key.to_string(),
                ExportRegistryRecord {
                    remote_root_dir_abs: item.export.remote_root_dir_abs.clone(),
                    export: item.export.clone(),
                    updated_unix_ms,
                },
            );
    }
    compact_export_registry_locked(reg);
    Ok(())
}

fn build_mount_exports_from_registry_locked(
    reg: &ExportRegistry,
) -> BTreeMap<String, FluxonFsExport> {
    let mut out: BTreeMap<String, FluxonFsExport> = BTreeMap::new();
    for (export_name, by_agent) in &reg.exports {
        let Some((_agent_instance_key, record)) = by_agent.iter().next() else {
            continue;
        };
        // English note:
        // - remote_root_dir_abs is provider-local and can differ across agents for the same export.
        // - Client mount paths do not use remote_root_dir_abs; they only need routing/cache/RPC fields.
        // - We keep one representative export object so mount-side lookup stays on the same
        //   FluxonFsExport contract as static config and runtime publish.
        out.insert(export_name.clone(), record.export.clone());
    }
    out
}

fn register_mount_registry_rpc(
    rt: Handle,
    cluster_name: &str,
    etcd_backend: ManagedEtcdClient,
    api: Arc<FluxonUserApi>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
) -> KvResult<()> {
    let cluster_name = cluster_name.to_string();
    let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync + 'static> =
        Arc::new(move |from_node_id, payload| {
            let got = payload.get("schema_version");
            let got_i64 = match got {
                Some(FlatValue::Int64(v)) => *v,
                _ => {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "schema_version must be int64".to_string(),
                    }));
                }
            };
            if got_i64 != FLUXON_FS_CONTROL_SCHEMA_VERSION {
                return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: format!(
                        "schema_version mismatch: expected={} got={}",
                        FLUXON_FS_CONTROL_SCHEMA_VERSION, got_i64
                    ),
                }));
            }
            let local_mount_dir_abs = match payload.get("local_mount_dir_abs") {
                Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
                _ => {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "local_mount_dir_abs must be non-empty string".to_string(),
                    }));
                }
            };
            let remote_root_dir_abs = match payload.get("remote_root_dir_abs") {
                Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
                _ => {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "remote_root_dir_abs must be non-empty string".to_string(),
                    }));
                }
            };
            if !Path::new(&remote_root_dir_abs).is_absolute() {
                return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: "remote_root_dir_abs must be an absolute path".to_string(),
                }));
            }
            let record = serde_json::json!({
                "schema_version": FLUXON_FS_CONTROL_SCHEMA_VERSION,
                "external_instance_key": from_node_id,
                "local_mount_dir_abs": local_mount_dir_abs,
                "remote_root_dir_abs": remote_root_dir_abs,
                "updated_unix_ms": unix_ms_now(),
            });
            let value = serde_json::to_string(&record).unwrap();
            let key = mount_registry_etcd_key(
                &cluster_name,
                record
                    .get("external_instance_key")
                    .unwrap()
                    .as_str()
                    .unwrap(),
                record.get("local_mount_dir_abs").unwrap().as_str().unwrap(),
            );
            let db_record = fluxon_fs_s3_gateway::FsMountRegistryRecord {
                external_instance_key: record
                    .get("external_instance_key")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string(),
                local_mount_dir_abs: record
                    .get("local_mount_dir_abs")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string(),
                remote_root_dir_abs: record
                    .get("remote_root_dir_abs")
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string(),
                updated_unix_ms: record.get("updated_unix_ms").unwrap().as_i64().unwrap(),
            };

            // English note:
            // - This RPC handler runs inside P2P's Tokio runtime worker threads.
            // - `run_async_from_sync` is forbidden there (it blocks the scheduler).
            // - Mount registry persistence is a control-plane side effect; it must not block or fail
            //   the filesystem data-plane mount itself.
            //
            // Therefore we spawn the etcd write asynchronously and return ok immediately.
            let etcd_backend = etcd_backend.clone();
            let key2 = key.clone();
            let value2 = value.clone();
            let s3_state2 = s3_state.clone();
            let db_record2 = db_record.clone();
            rt.spawn_blocking(move || {
                if let Err(e) = s3_state2.persist_fs_mount_registry_record(&db_record2) {
                    tracing::warn!(
                        "mount registry state db upsert failed; external_instance_key={} local_mount_dir_abs={} err={}",
                        db_record2.external_instance_key,
                        db_record2.local_mount_dir_abs,
                        e
                    );
                }
            });
            rt.spawn(async move {
                let mut etcd = match etcd_backend.client().await {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(
                            "mount registry etcd connect failed; key={} err={}",
                            key2,
                            e
                        );
                        return;
                    }
                };
                if let Err(e) = etcd.put(key2.clone(), value2, None).await {
                    tracing::warn!("mount registry etcd put failed; key={} err={}", key2, e);
                }
            });

            let mut out: FlatDict = FlatDict::new();
            out.insert("ok".to_string(), FlatValue::Bool(true));
            Ok(out)
        });

    api.rpc_server()
        .register(FS_MASTER_MOUNT_REGISTRY_RPC_PATH, handler)?;
    Ok(())
}

fn register_export_registry_rpc(
    api: Arc<FluxonUserApi>,
    export_registry: Arc<Mutex<ExportRegistry>>,
) -> KvResult<()> {
    let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync + 'static> =
        Arc::new(move |_from_node_id, payload| {
            let got = payload.get("schema_version");
            let got_i64 = match got {
                Some(FlatValue::Int64(v)) => *v,
                _ => {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "schema_version must be int64".to_string(),
                    }));
                }
            };
            if got_i64 != FLUXON_FS_CONTROL_SCHEMA_VERSION {
                return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: format!(
                        "schema_version mismatch: expected={} got={}",
                        FLUXON_FS_CONTROL_SCHEMA_VERSION, got_i64
                    ),
                }));
            }

            let op = match payload.get("op") {
                Some(FlatValue::String(s)) if !s.trim().is_empty() => s.trim().to_ascii_lowercase(),
                _ => {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "op must be non-empty string".to_string(),
                    }));
                }
            };

            if op == "snapshot" {
                let export_name = match payload.get("export_name") {
                    Some(FlatValue::String(s)) if !s.trim().is_empty() => s.trim().to_string(),
                    _ => {
                        return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                            detail: "export_name must be non-empty string".to_string(),
                        }));
                    }
                };
                if export_name.contains('/') {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "export_name must not contain '/'".to_string(),
                    }));
                }

                let nodes: Vec<String> = {
                    let reg = export_registry.lock();
                    let m = match reg.exports.get(&export_name) {
                        Some(v) => v,
                        None => {
                            return Ok(FlatDict::from([
                                ("ok".to_string(), FlatValue::Bool(true)),
                                (
                                    "nodes_json".to_string(),
                                    FlatValue::String("[]".to_string()),
                                ),
                            ]));
                        }
                    };
                    let mut out: Vec<String> = m.keys().cloned().collect();
                    out.sort();
                    out
                };

                let mut out: FlatDict = FlatDict::new();
                out.insert("ok".to_string(), FlatValue::Bool(true));
                out.insert(
                    "nodes_json".to_string(),
                    FlatValue::String(serde_json::to_string(&nodes).unwrap()),
                );
                Ok(out)
            } else {
                Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: format!("unknown op: {}", op),
                }))
            }
        });

    api.rpc_server()
        .register(FS_MASTER_EXPORT_REGISTRY_RPC_PATH, handler)?;
    Ok(())
}

fn register_agent_exports_push_rpc(
    rt: Handle,
    api: Arc<FluxonUserApi>,
    export_registry: Arc<Mutex<ExportRegistry>>,
    cluster_name: String,
    etcd_backend: ManagedEtcdClient,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
) -> KvResult<()> {
    let path = FS_MASTER_AGENT_EXPORTS_PUSH_RPC_PATH.to_string();
    let handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync + 'static> =
        Arc::new(move |from_node_id, payload| {
            let got = payload.get("schema_version");
            let got_i64 = match got {
                Some(FlatValue::Int64(v)) => *v,
                _ => {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "schema_version must be int64".to_string(),
                    }));
                }
            };
            if got_i64 != FLUXON_FS_CONTROL_SCHEMA_VERSION {
                return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                    detail: format!(
                        "schema_version mismatch: expected={} got={}",
                        FLUXON_FS_CONTROL_SCHEMA_VERSION, got_i64
                    ),
                }));
            }

            let exports_json = match payload.get("exports_json") {
                Some(FlatValue::String(s)) if !s.trim().is_empty() => s.clone(),
                _ => {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: "exports_json must be non-empty string".to_string(),
                    }));
                }
            };
            let mut items: Vec<FsAgentExportSnapshotItemWire> = serde_json::from_str(&exports_json)
                .map_err(|e| {
                    CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: format!("parse exports_json failed: {}", e),
                    })
                })?;
            for item in items.iter() {
                validate_snapshot_export_item(&item.export_name, &item.export).map_err(
                    |detail| {
                        CoreKvError::Api(CoreApiError::InvalidArgument {
                            detail: format!(
                                "invalid push snapshot: agent_instance_key={} {}",
                                from_node_id, detail
                            ),
                        })
                    },
                )?;
            }
            items.sort_by(|a, b| a.export_name.cmp(&b.export_name));
            for pair in items.windows(2) {
                if pair[0].export_name == pair[1].export_name {
                    return Err(CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: format!(
                            "duplicate export_name in push snapshot: agent_instance_key={} export={}",
                            from_node_id, pair[0].export_name
                        ),
                    }));
                }
            }
            let updated_unix_ms = unix_ms_now();
            let db_records =
                db_records_from_snapshot_items(from_node_id.as_str(), &items, updated_unix_ms);

            {
                let mut reg = export_registry.lock();
                apply_agent_snapshot_items_locked(
                    &mut reg,
                    from_node_id.as_str(),
                    &items,
                    updated_unix_ms,
                )
                .map_err(|detail| {
                    CoreKvError::Api(CoreApiError::InvalidArgument {
                        detail: format!("apply push snapshot failed: {}", detail),
                    })
                })?;
            }
            // English note:
            // - State DB is now the panel authority for FS export runtime state.
            // - Acknowledging the push before DB replacement can reorder snapshots and make the
            //   panel read older state than the master already accepted.
            // - Therefore the authoritative DB replace must complete before returning `ok=true`.
            s3_state
                .replace_fs_export_registry_for_agent(from_node_id.as_str(), &db_records)
                .map_err(|detail| {
                    CoreKvError::Api(CoreApiError::Unknown {
                        detail: format!(
                            "export registry state db replace failed: agent_instance_key={} err={}",
                            from_node_id, detail
                        ),
                    })
                })?;
            spawn_persist_export_registry_snapshot_to_etcd(
                rt.clone(),
                export_registry.clone(),
                FLUXON_FS_CONTROL_SCHEMA_VERSION,
                cluster_name.clone(),
                etcd_backend.clone(),
            );

            let mut out: FlatDict = FlatDict::new();
            out.insert("ok".to_string(), FlatValue::Bool(true));
            Ok(out)
        });

    api.rpc_server().register(&path, handler)?;
    Ok(())
}

fn mount_registry_etcd_key(
    cluster_name: &str,
    external_instance_key: &str,
    local_mount_dir_abs: &str,
) -> String {
    let mount_id = sha256_hex(local_mount_dir_abs.as_bytes());
    format!(
        "{}/{}/mounts/{}/{}",
        ETCD_PREFIX_FS_MOUNT_REGISTRY, cluster_name, external_instance_key, mount_id
    )
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn unix_ms_now() -> i64 {
    let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    (d.as_millis() as i128).min(i64::MAX as i128) as i64
}

fn export_registry_legacy_prefix_etcd_key(cluster_name: &str) -> String {
    format!(
        "{}/{}/exports/",
        ETCD_PREFIX_FS_EXPORT_REGISTRY, cluster_name
    )
}

fn export_registry_snapshot_etcd_key(cluster_name: &str) -> String {
    format!(
        "{}/{}/snapshot",
        ETCD_PREFIX_FS_EXPORT_REGISTRY, cluster_name
    )
}

async fn cleanup_legacy_export_registry_keys(
    etcd: &mut EtcdClient,
    cluster_name: &str,
) -> anyhow::Result<()> {
    let prefix = export_registry_legacy_prefix_etcd_key(cluster_name);
    etcd.delete(prefix.clone(), Some(DeleteOptions::new().with_prefix()))
        .await
        .with_context(|| {
            format!(
                "etcd delete legacy export registry prefix failed: {}",
                prefix
            )
        })?;
    tracing::info!(
        "fluxon_fs legacy export registry cleaned: cluster={} prefix={}",
        cluster_name,
        prefix
    );
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FsExportRegistrySnapshotWire {
    schema_version: i64,
    updated_unix_ms: i64,
    records: Vec<FsExportRegistryRecordWire>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct FsExportRegistryRecordWire {
    export_name: String,
    agent_instance_key: String,
    remote_root_dir_abs: String,
    updated_unix_ms: i64,
}

fn start_export_registry_manager(
    rt: Handle,
    api: Arc<FluxonUserApi>,
    framework: Arc<fluxon_kv::Framework>,
    export_registry: Arc<Mutex<ExportRegistry>>,
    cluster_name: String,
    etcd_backend: ManagedEtcdClient,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    schema_version: i64,
    pull_interval_ms: u64,
) {
    let poller = framework.register_shutdown_poller();
    let live_agents: Arc<Mutex<BTreeSet<String>>> = Arc::new(Mutex::new(BTreeSet::new()));
    let inflight_sync: Arc<Mutex<BTreeSet<String>>> = Arc::new(Mutex::new(BTreeSet::new()));

    let mut initial_agents: Vec<String> = Vec::new();
    for m in api.membership_snapshot() {
        if !is_fs_agent_cluster_member(&m) {
            continue;
        }
        let id = m.id.to_string();
        live_agents.lock().insert(id.clone());
        initial_agents.push(id);
    }
    initial_agents.sort();

    for agent_id in initial_agents {
        spawn_sync_agent_exports_until_ready(
            rt.clone(),
            api.clone(),
            framework.clone(),
            export_registry.clone(),
            cluster_name.clone(),
            etcd_backend.clone(),
            s3_state.clone(),
            schema_version,
            pull_interval_ms,
            live_agents.clone(),
            inflight_sync.clone(),
            agent_id,
        );
    }

    let mut rx: MembershipEventReceiver = api.membership_listen();
    tokio::spawn(async move {
        while poller.is_running() {
            let ev = match rx.recv().await {
                Ok(v) => v,
                Err(_) => return,
            };
            match ev {
                ClusterEvent::MemberJoined(m) | ClusterEvent::MemberUpdated(m) => {
                    if !is_fs_agent_cluster_member(&m) {
                        continue;
                    }
                    let agent_id = m.id.to_string();
                    {
                        live_agents.lock().insert(agent_id.clone());
                    }
                    spawn_sync_agent_exports_until_ready(
                        rt.clone(),
                        api.clone(),
                        framework.clone(),
                        export_registry.clone(),
                        cluster_name.clone(),
                        etcd_backend.clone(),
                        s3_state.clone(),
                        schema_version,
                        pull_interval_ms,
                        live_agents.clone(),
                        inflight_sync.clone(),
                        agent_id,
                    );
                }
                ClusterEvent::MemberLeft(member_id) => {
                    let was_agent = {
                        let mut live = live_agents.lock();
                        live.remove(&member_id)
                    };
                    if was_agent {
                        remove_agent_exports_and_persist_snapshot(
                            rt.clone(),
                            export_registry.clone(),
                            s3_state.clone(),
                            &member_id,
                            schema_version,
                            &cluster_name,
                            &etcd_backend,
                        );
                    }
                }
            }
        }
    });
}

fn is_fs_agent_cluster_member(m: &ClusterMember) -> bool {
    matches!(
        fs_component_from_cluster_member(m),
        Some(FluxonFsComponent::Agent)
    )
}

fn remove_agent_exports_and_persist_snapshot(
    rt: Handle,
    export_registry: Arc<Mutex<ExportRegistry>>,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    agent_instance_key: &str,
    schema_version: i64,
    cluster_name: &str,
    etcd_backend: &ManagedEtcdClient,
) {
    {
        let mut reg = export_registry.lock();
        remove_agent_from_export_registry_locked(&mut reg, agent_instance_key);
    }
    spawn_persist_export_registry_snapshot_to_etcd(
        rt,
        export_registry,
        schema_version,
        cluster_name.to_string(),
        etcd_backend.clone(),
    );
    if let Err(e) = s3_state.delete_fs_export_registry_for_agent(agent_instance_key) {
        tracing::warn!(
            "export registry state db delete failed; agent_instance_key={} err={}",
            agent_instance_key,
            e
        );
    }
}

fn spawn_sync_agent_exports_until_ready(
    rt: Handle,
    api: Arc<FluxonUserApi>,
    framework: Arc<fluxon_kv::Framework>,
    export_registry: Arc<Mutex<ExportRegistry>>,
    cluster_name: String,
    etcd_backend: ManagedEtcdClient,
    s3_state: Arc<fluxon_fs_s3_gateway::GatewayState>,
    schema_version: i64,
    pull_interval_ms: u64,
    live_agents: Arc<Mutex<BTreeSet<String>>>,
    inflight_sync: Arc<Mutex<BTreeSet<String>>>,
    agent_id: String,
) {
    {
        let mut inflight = inflight_sync.lock();
        if inflight.contains(&agent_id) {
            return;
        }
        inflight.insert(agent_id.clone());
    }

    tokio::spawn(async move {
        let poller = framework.register_shutdown_poller();
        let retry_interval = Duration::from_millis(pull_interval_ms);
        let mut waited_ticks = 0u64;
        let start = std::time::Instant::now();

        loop {
            if !poller.is_running() {
                break;
            }
            let is_live = { live_agents.lock().contains(&agent_id) };
            if !is_live {
                break;
            }

            let err_for_this_iter: String = match pull_agent_exports_snapshot_once(
                api.clone(),
                &agent_id,
                schema_version,
            )
            .await
            {
                Ok(items) => {
                    let updated_unix_ms = unix_ms_now();
                    let db_records =
                        db_records_from_snapshot_items(agent_id.as_str(), &items, updated_unix_ms);
                    let apply_err = {
                        let mut reg = export_registry.lock();
                        apply_agent_snapshot_items_locked(
                            &mut reg,
                            &agent_id,
                            &items,
                            updated_unix_ms,
                        )
                        .err()
                    };
                    if let Some(e) = apply_err {
                        let err_for_this_iter = format!(
                            "apply pulled export snapshot failed: agent_instance_key={} err={}",
                            agent_id, e
                        );
                        tokio::time::sleep(retry_interval).await;
                        waited_ticks += 1;
                        if waited_ticks % EXPORT_REGISTRY_SYNC_RETRY_LOG_TICKS == 0 {
                            let waited_s = (waited_ticks * pull_interval_ms) / 1000;
                            tracing::warn!(
                                "fluxon_fs master waiting for agent export snapshot: agent={} waited_s={} last_err={}",
                                agent_id,
                                waited_s,
                                err_for_this_iter
                            );
                        }
                        continue;
                    }
                    let s3_state2 = s3_state.clone();
                    let agent_id2 = agent_id.clone();
                    let db_records2 = db_records.clone();
                    let db_replace_res = tokio::task::spawn_blocking(move || {
                        s3_state2
                            .replace_fs_export_registry_for_agent(agent_id2.as_str(), &db_records2)
                    })
                    .await;
                    match db_replace_res {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            let err_for_this_iter = format!(
                                "export registry state db replace failed after pull: agent_instance_key={} err={}",
                                agent_id, e
                            );
                            tokio::time::sleep(retry_interval).await;
                            waited_ticks += 1;
                            if waited_ticks % EXPORT_REGISTRY_SYNC_RETRY_LOG_TICKS == 0 {
                                let waited_s = (waited_ticks * pull_interval_ms) / 1000;
                                tracing::warn!(
                                    "fluxon_fs master waiting for agent export snapshot: agent={} waited_s={} last_err={}",
                                    agent_id,
                                    waited_s,
                                    err_for_this_iter
                                );
                            }
                            continue;
                        }
                        Err(e) => {
                            let err_for_this_iter = format!(
                                "export registry state db replace join failed after pull: agent_instance_key={} err={}",
                                agent_id, e
                            );
                            tokio::time::sleep(retry_interval).await;
                            waited_ticks += 1;
                            if waited_ticks % EXPORT_REGISTRY_SYNC_RETRY_LOG_TICKS == 0 {
                                let waited_s = (waited_ticks * pull_interval_ms) / 1000;
                                tracing::warn!(
                                    "fluxon_fs master waiting for agent export snapshot: agent={} waited_s={} last_err={}",
                                    agent_id,
                                    waited_s,
                                    err_for_this_iter
                                );
                            }
                            continue;
                        }
                    }
                    spawn_persist_export_registry_snapshot_to_etcd(
                        rt.clone(),
                        export_registry.clone(),
                        schema_version,
                        cluster_name.clone(),
                        etcd_backend.clone(),
                    );
                    break;
                }
                Err(e) => e.to_string(),
            };

            let waited = start.elapsed();
            if export_registry_sync_budget_exhausted(waited) {
                tracing::warn!(
                    "fluxon_fs master giving up agent export snapshot sync after {}s: agent={} last_err={}",
                    waited.as_secs(),
                    agent_id,
                    err_for_this_iter
                );
                break;
            }

            tokio::time::sleep(retry_interval).await;
            waited_ticks += 1;
            if waited_ticks % EXPORT_REGISTRY_SYNC_RETRY_LOG_TICKS == 0 {
                let waited_s = (waited_ticks * pull_interval_ms) / 1000;
                tracing::warn!(
                    "fluxon_fs master waiting for agent export snapshot: agent={} waited_s={} last_err={}",
                    agent_id,
                    waited_s,
                    err_for_this_iter
                );
            }
        }

        inflight_sync.lock().remove(&agent_id);
    });
}

async fn pull_agent_exports_snapshot_once(
    api: Arc<FluxonUserApi>,
    agent_instance_key: &str,
    schema_version: i64,
) -> anyhow::Result<Vec<FsAgentExportSnapshotItemWire>> {
    let agent_instance_key2 = agent_instance_key.to_string();
    let j = spawn_blocking_allow_sync_async_bridge(move || {
        let payload: FlatDict = FlatDict::from([(
            "schema_version".to_string(),
            FlatValue::Int64(schema_version),
        )]);
        api.rpc_client().call(
            &agent_instance_key2,
            fluxon_fs_core::config::FS_AGENT_EXPORTS_SNAPSHOT_RPC_PATH,
            payload,
            None,
        )
    })
    .await;

    let resp = match j {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            return Err(anyhow::anyhow!(
                "agent exports snapshot rpc failed: agent={} err={}",
                agent_instance_key,
                e
            ));
        }
        Err(e) => {
            return Err(anyhow::anyhow!(
                "agent exports snapshot rpc join failed: agent={} err={}",
                agent_instance_key,
                e
            ));
        }
    };

    match resp.get("ok") {
        Some(FlatValue::Bool(true)) => {}
        _ => anyhow::bail!(
            "agent exports snapshot rpc returned ok=false: agent={}",
            agent_instance_key
        ),
    }
    let exports_json = match resp.get("exports_json") {
        Some(FlatValue::String(s)) => s.clone(),
        _ => anyhow::bail!(
            "agent exports snapshot rpc missing exports_json: agent={}",
            agent_instance_key
        ),
    };

    let mut items: Vec<FsAgentExportSnapshotItemWire> =
        serde_json::from_str(&exports_json).with_context(|| "parse exports_json")?;
    for item in items.iter() {
        validate_snapshot_export_item(&item.export_name, &item.export).map_err(|detail| {
            anyhow::anyhow!(
                "invalid agent snapshot: agent_instance_key={} {}",
                agent_instance_key,
                detail
            )
        })?;
    }
    items.sort_by(|a, b| a.export_name.cmp(&b.export_name));
    for pair in items.windows(2) {
        if pair[0].export_name == pair[1].export_name {
            anyhow::bail!(
                "duplicate export_name in agent snapshot: agent_instance_key={} export={}",
                agent_instance_key,
                pair[0].export_name
            );
        }
    }
    Ok(items)
}

fn remove_agent_from_export_registry_locked(reg: &mut ExportRegistry, agent_instance_key: &str) {
    for m in reg.exports.values_mut() {
        m.remove(agent_instance_key);
    }
    compact_export_registry_locked(reg);
}

fn compact_export_registry_locked(reg: &mut ExportRegistry) {
    reg.exports.retain(|_, by_agent| !by_agent.is_empty());
}

fn spawn_persist_export_registry_snapshot_to_etcd(
    rt: Handle,
    export_registry: Arc<Mutex<ExportRegistry>>,
    schema_version: i64,
    cluster_name: String,
    etcd_backend: ManagedEtcdClient,
) {
    let snapshot = {
        let reg = export_registry.lock();
        let mut records: Vec<FsExportRegistryRecordWire> = Vec::new();
        for (export_name, by_agent) in reg.exports.iter() {
            for (agent_instance_key, rec) in by_agent.iter() {
                records.push(FsExportRegistryRecordWire {
                    export_name: export_name.clone(),
                    agent_instance_key: agent_instance_key.clone(),
                    remote_root_dir_abs: rec.remote_root_dir_abs.clone(),
                    updated_unix_ms: rec.updated_unix_ms,
                });
            }
        }
        records.sort_by(|a, b| {
            (a.export_name.clone(), a.agent_instance_key.clone())
                .cmp(&(b.export_name.clone(), b.agent_instance_key.clone()))
        });
        FsExportRegistrySnapshotWire {
            schema_version,
            updated_unix_ms: unix_ms_now(),
            records,
        }
    };

    let key = export_registry_snapshot_etcd_key(&cluster_name);
    rt.spawn(async move {
        let mut etcd = match etcd_backend.client().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    "export registry snapshot etcd connect failed; key={} err={}",
                    key,
                    e
                );
                return;
            }
        };
        let value = serde_json::to_string(&snapshot).unwrap();
        if let Err(e) = etcd.put(key.clone(), value, None).await {
            tracing::warn!(
                "export registry snapshot etcd put failed; key={} err={}",
                key,
                e
            );
        }
    });
}

async fn panel_view_html() -> Response {
    redirect_response("/fs_s3/ui/")
}

async fn panel_view_cli() -> Response {
    redirect_response("/fs_s3/ui/admin/fs_master/")
}

#[derive(Clone)]
struct FsMember {
    kind: FluxonFsComponent,
    raw: serde_json::Value,
}

fn fs_component_from_metadata_text(value: Option<&str>) -> Option<FluxonFsComponent> {
    value.and_then(FluxonFsComponent::from_metadata_value)
}

fn fs_component_from_cluster_member(m: &ClusterMember) -> Option<FluxonFsComponent> {
    fs_component_from_metadata_text(
        m.metadata
            .get(FLUXON_FS_COMPONENT_METADATA_KEY)
            .map(|s| s.as_str()),
    )
}

fn fs_component_from_member_json(v: &serde_json::Value) -> Option<FluxonFsComponent> {
    let value = v
        .get("metadata")
        .and_then(|m| m.as_object())
        .and_then(|m| m.get(FLUXON_FS_COMPONENT_METADATA_KEY))
        .and_then(|x| x.as_str());
    fs_component_from_metadata_text(value)
}

fn fs_master_member_record_from_local(
    member: FsMember,
) -> fluxon_fs_s3_gateway::FsMasterMemberRecord {
    let metadata = member
        .raw
        .get("metadata")
        .and_then(|value| value.as_object())
        .cloned()
        .unwrap_or_default();
    let addresses = member
        .raw
        .get("addresses")
        .and_then(|value| value.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.as_str().map(|value| value.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let kind = match member.kind {
        FluxonFsComponent::Agent => fluxon_fs_s3_gateway::FsMasterMemberKind::Agent,
        FluxonFsComponent::Controller => fluxon_fs_s3_gateway::FsMasterMemberKind::Controller,
    };
    fluxon_fs_s3_gateway::FsMasterMemberRecord {
        kind,
        member_id: member_id(&member.raw),
        owner_id: metadata
            .get("shared_storage_node_id")
            .and_then(|value| value.as_str())
            .unwrap_or("N/A")
            .to_string(),
        hostname: metadata
            .get("hostname")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string(),
        addresses,
        port: member.raw.get("port").and_then(|value| value.as_i64()),
        pid: metadata
            .get("pid")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string(),
        cmd: metadata
            .get("cmd")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_string(),
    }
}

async fn fetch_fs_members_snapshot(
    etcd: Arc<tokio::sync::Mutex<EtcdClient>>,
    cluster_name: &str,
) -> anyhow::Result<Vec<FsMember>> {
    let prefix = format!("{}/", cluster_member_base_prefix(cluster_name));
    let mut etcd = etcd.lock().await;
    let mut out: Vec<FsMember> = Vec::new();
    scan_etcd_prefix_paginated(&mut etcd, &prefix, |key, value| {
        let key = String::from_utf8_lossy(key);
        if !key.starts_with(&prefix) {
            return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                EtcdPrefixScanAction::Continue,
            );
        }
        let rest = &key[prefix.len()..];
        if rest.is_empty() || rest.contains('/') {
            return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                EtcdPrefixScanAction::Continue,
            );
        }
        let raw = String::from_utf8_lossy(value).to_string();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap_or(serde_json::json!({}));
        if !v.is_object() {
            return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                EtcdPrefixScanAction::Continue,
            );
        }
        let Some(kind) = fs_component_from_member_json(&v) else {
            return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                EtcdPrefixScanAction::Continue,
            );
        };
        out.push(FsMember { kind, raw: v });
        Ok::<EtcdPrefixScanAction, std::convert::Infallible>(EtcdPrefixScanAction::Continue)
    })
    .await
    .with_context(|| "etcd get members prefix")?;
    out.sort_by(|a, b| (a.kind, member_id(&a.raw)).cmp(&(b.kind, member_id(&b.raw))));
    Ok(out)
}

async fn fetch_member_ids_snapshot(
    etcd: Arc<tokio::sync::Mutex<EtcdClient>>,
    cluster_name: &str,
) -> anyhow::Result<BTreeSet<String>> {
    let prefix = format!("{}/", cluster_member_base_prefix(cluster_name));
    let mut etcd = etcd.lock().await;
    let mut out: BTreeSet<String> = BTreeSet::new();
    scan_etcd_prefix_paginated(&mut etcd, &prefix, |key, _value| {
        let key = String::from_utf8_lossy(key);
        if !key.starts_with(&prefix) {
            return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                EtcdPrefixScanAction::Continue,
            );
        }
        let rest = &key[prefix.len()..];
        if rest.is_empty() || rest.contains('/') {
            return Ok::<EtcdPrefixScanAction, std::convert::Infallible>(
                EtcdPrefixScanAction::Continue,
            );
        }
        out.insert(rest.to_string());
        Ok::<EtcdPrefixScanAction, std::convert::Infallible>(EtcdPrefixScanAction::Continue)
    })
    .await
    .with_context(|| "etcd get member ids prefix")?;
    Ok(out)
}

fn member_id(v: &serde_json::Value) -> String {
    v.get("id")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}
