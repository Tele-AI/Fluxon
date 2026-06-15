use std::fmt::{Display, Formatter};
use std::time::Duration;

pub use fluxon_commu_closed_sdk_consumer::rdma_probe::{
    capture_rdma_runtime_snapshot, probe_rdma_snapshot,
};
pub use fluxon_commu_closed_sdk_consumer::{
    ClosedSdkConsumerError, ClosedSdkRuntimeAnchor, ClosedSdkVersionInfo,
    FLUXON_COMMU_CLOSED_ABI_VERSION, FLUXON_COMMU_CLOSED_SDK_SCHEMA_VERSION, abi_version,
    assert_abi_compatible, boundary_mode, cluster_manager_call, construct_cluster_manager_handle,
    construct_p2p_module_handle, construct_transfer_engine_handle,
    current_cluster_manager_self_rdma_resolved_config, drop_runtime_handle,
    p2p_attach_transfer_engine, p2p_call_raw_observed, p2p_register_dispatch,
    p2p_register_rpc_response_msg_id, p2p_register_user_rpc_bytes_handler,
    p2p_register_user_rpc_bytes_handler_async, p2p_send_response_raw, query_version,
    recv_cluster_event_stream, recv_cluster_rdma_resolved_config_stream,
    register_transfer_engine_open_runtime_callback, required_open_surface_version, runtime_anchor,
    runtime_anchor_checksum, runtime_invoke, sdk_schema_version, sdk_version,
    subscribe_cluster_manager_events, transfer_engine_current_runtime_config,
    transfer_engine_drain_inbound_fast_path_messages, transfer_engine_ensure_started_if_needed,
    transfer_engine_init2_for_init_dag, transfer_engine_register_local_segment,
    transfer_engine_sync_desired_peers, transfer_engine_transfer_data_no_copy,
    transfer_engine_try_send_wire_direct, transfer_engine_unregister_local_segment,
    transfer_engine_update_enabled_rdma_devices, transfer_engine_update_runtime_config,
    watch_cluster_manager_self_rdma_resolved_config,
};
use fluxon_commu_contract::ClosedRuntimeError;
pub use fluxon_commu_contract::{ClosedRuntimeHandle, RdmaProbeSnapshot, RdmaRuntimeSnapshot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentProviderContractSnapshot {
    pub version_info: ClosedSdkVersionInfo,
    pub sdk_runtime_anchor: ClosedSdkRuntimeAnchor,
    pub provider_runtime_anchor: ClosedSdkRuntimeAnchor,
}

#[derive(Debug)]
pub enum CurrentProviderContractError {
    Consumer(ClosedSdkConsumerError),
    BoundaryModeMismatch {
        expected_by_open_provider: &'static str,
        actual_sdk_boundary_mode: String,
    },
    OpenSurfaceVersionMismatch {
        expected_by_sdk: String,
        actual_open_surface: String,
    },
    RuntimeAnchorMismatch {
        field: &'static str,
        expected_by_sdk: usize,
        actual_provider: usize,
    },
}

impl Display for CurrentProviderContractError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Consumer(err) => write!(f, "{err}"),
            Self::BoundaryModeMismatch {
                expected_by_open_provider,
                actual_sdk_boundary_mode,
            } => write!(
                f,
                "closed SDK boundary mode mismatch: open provider expects {}, sdk advertises {}",
                expected_by_open_provider, actual_sdk_boundary_mode
            ),
            Self::OpenSurfaceVersionMismatch {
                expected_by_sdk,
                actual_open_surface,
            } => write!(
                f,
                "closed SDK requires open surface version {}, but current fluxon_commu version is {}",
                expected_by_sdk, actual_open_surface
            ),
            Self::RuntimeAnchorMismatch {
                field,
                expected_by_sdk,
                actual_provider,
            } => write!(
                f,
                "closed SDK runtime anchor mismatch for {}: sdk={}, provider={}",
                field, expected_by_sdk, actual_provider
            ),
        }
    }
}

impl std::error::Error for CurrentProviderContractError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Consumer(err) => Some(err),
            _ => None,
        }
    }
}

impl From<ClosedSdkConsumerError> for CurrentProviderContractError {
    fn from(value: ClosedSdkConsumerError) -> Self {
        Self::Consumer(value)
    }
}

pub fn current_provider_runtime_anchor() -> ClosedSdkRuntimeAnchor {
    crate::provider::current_provider_runtime_anchor()
}

pub fn assert_current_provider_contract()
-> Result<CurrentProviderContractSnapshot, CurrentProviderContractError> {
    assert_abi_compatible()?;

    let version_info = query_version()?;
    let expected_boundary_mode = crate::provider::CURRENT_PROVIDER_BOUNDARY_MODE;
    if version_info.boundary_mode != expected_boundary_mode {
        return Err(CurrentProviderContractError::BoundaryModeMismatch {
            expected_by_open_provider: expected_boundary_mode,
            actual_sdk_boundary_mode: version_info.boundary_mode.clone(),
        });
    }
    let actual_open_surface = env!("CARGO_PKG_VERSION").to_string();
    if version_info.required_open_surface_version != actual_open_surface {
        return Err(CurrentProviderContractError::OpenSurfaceVersionMismatch {
            expected_by_sdk: version_info.required_open_surface_version.clone(),
            actual_open_surface,
        });
    }

    let sdk_runtime_anchor = runtime_anchor();
    let provider_runtime_anchor = current_provider_runtime_anchor();
    assert_runtime_anchor_field(
        "ClusterManager",
        sdk_runtime_anchor.cluster_manager_size,
        provider_runtime_anchor.cluster_manager_size,
    )?;
    assert_runtime_anchor_field(
        "P2pModule",
        sdk_runtime_anchor.p2p_module_size,
        provider_runtime_anchor.p2p_module_size,
    )?;
    assert_runtime_anchor_field(
        "ClientTransferEngineCore",
        sdk_runtime_anchor.transfer_engine_core_size,
        provider_runtime_anchor.transfer_engine_core_size,
    )?;

    Ok(CurrentProviderContractSnapshot {
        version_info,
        sdk_runtime_anchor,
        provider_runtime_anchor,
    })
}

pub fn assert_source_provider_contract()
-> Result<CurrentProviderContractSnapshot, CurrentProviderContractError> {
    assert_current_provider_contract()
}

pub type SourceProviderContractSnapshot = CurrentProviderContractSnapshot;

pub type SourceProviderContractError = CurrentProviderContractError;

fn assert_runtime_anchor_field(
    field: &'static str,
    expected_by_sdk: usize,
    actual_provider: usize,
) -> Result<(), CurrentProviderContractError> {
    if expected_by_sdk == actual_provider {
        return Ok(());
    }
    Err(CurrentProviderContractError::RuntimeAnchorMismatch {
        field,
        expected_by_sdk,
        actual_provider,
    })
}

pub(crate) fn is_live_dependent_drop_error(error: &ClosedSdkConsumerError) -> bool {
    matches!(
        error,
        ClosedSdkConsumerError::RuntimeError {
            error: ClosedRuntimeError::Internal { detail }
        } if detail.contains("still has live")
    )
}

pub(crate) fn spawn_deferred_drop_runtime_handle(
    handle: ClosedRuntimeHandle,
    reason: &'static str,
) {
    tokio::spawn(async move {
        const MAX_ATTEMPTS: usize = 200;
        const RETRY_INTERVAL: Duration = Duration::from_millis(100);

        for attempt in 1..=MAX_ATTEMPTS {
            match drop_runtime_handle(handle).await {
                Ok(()) => {
                    tracing::info!(
                        reason,
                        handle_kind = ?handle.kind,
                        handle_raw = handle.raw,
                        attempt,
                        "deferred closed runtime handle drop succeeded"
                    );
                    return;
                }
                Err(error) if is_live_dependent_drop_error(&error) && attempt < MAX_ATTEMPTS => {
                    tokio::time::sleep(RETRY_INTERVAL).await;
                }
                Err(ClosedSdkConsumerError::RuntimeError {
                    error: ClosedRuntimeError::InvalidHandle { .. },
                }) => {
                    tracing::debug!(
                        reason,
                        handle_kind = ?handle.kind,
                        handle_raw = handle.raw,
                        "deferred closed runtime handle already dropped"
                    );
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        reason,
                        handle_kind = ?handle.kind,
                        handle_raw = handle.raw,
                        attempt,
                        err = %error,
                        "deferred closed runtime handle drop failed"
                    );
                    return;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::assert_current_provider_contract;

    #[test]
    fn current_provider_matches_closed_sdk() {
        let snapshot = assert_current_provider_contract()
            .expect("source-backed provider must match closed SDK");
        assert_eq!(
            snapshot.sdk_runtime_anchor.cluster_manager_size,
            snapshot.provider_runtime_anchor.cluster_manager_size
        );
        assert_eq!(
            snapshot.sdk_runtime_anchor.p2p_module_size,
            snapshot.provider_runtime_anchor.p2p_module_size
        );
        assert_eq!(
            snapshot.sdk_runtime_anchor.transfer_engine_core_size,
            snapshot.provider_runtime_anchor.transfer_engine_core_size
        );
    }
}
