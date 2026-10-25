use std::fmt::{Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use tokio::sync::Mutex;

pub use fluxon_commu_closed_sdk_consumer::rdma_probe::{
    capture_rdma_runtime_snapshot, probe_rdma_snapshot,
};
pub use fluxon_commu_closed_sdk_consumer::{
    ClosedSdkConsumerError, ClosedSdkRuntimeAnchor, ClosedSdkVersionInfo,
    FLUXON_COMMU_CLOSED_ABI_VERSION, FLUXON_COMMU_CLOSED_SDK_SCHEMA_VERSION, abi_version,
    assert_abi_compatible, boundary_mode, cluster_manager_call, construct_cluster_manager_handle,
    construct_p2p_module_handle, construct_transfer_engine_handle,
    current_cluster_manager_self_rdma_resolved_config, drop_runtime_handle,
    initialize_process_logging, p2p_attach_transfer_engine, p2p_call_raw_observed,
    p2p_register_dispatch, p2p_register_rpc_response_msg_id, p2p_register_user_rpc_bytes_handler,
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
pub use fluxon_commu_contract::ClosedRuntimeProcessLoggingConfig;
use fluxon_commu_contract::FLUXON_COMMU_OPEN_SURFACE_VERSION;
pub use fluxon_commu_contract::{ClosedRuntimeHandle, RdmaProbeSnapshot, RdmaRuntimeSnapshot};

type RuntimeHandleReleaseFuture =
    Pin<Box<dyn Future<Output = Result<(), ClosedSdkConsumerError>> + Send>>;

enum RuntimeHandleReleaseState {
    Idle,
    Pending(RuntimeHandleReleaseFuture),
    Complete,
}

/// Retain the final-release operation across canceled close waiters.
/// A successful close is a completion barrier; admission belongs to the module.
pub(crate) struct RuntimeHandleRelease {
    state: Mutex<RuntimeHandleReleaseState>,
}

impl RuntimeHandleRelease {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(RuntimeHandleReleaseState::Idle),
        }
    }

    pub(crate) async fn run<F>(
        &self,
        release: impl FnOnce() -> F,
    ) -> Result<(), ClosedSdkConsumerError>
    where
        F: Future<Output = Result<(), ClosedSdkConsumerError>> + Send + 'static,
    {
        let mut state = self.state.lock().await;
        match &*state {
            RuntimeHandleReleaseState::Complete => return Ok(()),
            RuntimeHandleReleaseState::Idle => {
                *state = RuntimeHandleReleaseState::Pending(Box::pin(release()));
            }
            RuntimeHandleReleaseState::Pending(_) => {}
        }
        // The owner keeps the FFI completion receiver if this waiter is canceled.
        let result = match &mut *state {
            RuntimeHandleReleaseState::Pending(operation) => operation.as_mut().await,
            _ => unreachable!("handle release must be pending before polling"),
        };
        *state = if result.is_ok() {
            RuntimeHandleReleaseState::Complete
        } else {
            RuntimeHandleReleaseState::Idle
        };
        result
    }
}

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
                "closed SDK requires open surface version {}, but the current open contract version is {}",
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
    let actual_open_surface = FLUXON_COMMU_OPEN_SURFACE_VERSION.to_string();
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{ClosedSdkConsumerError, RuntimeHandleRelease};
    use super::assert_current_provider_contract;
    use fluxon_commu_contract::FLUXON_COMMU_OPEN_SURFACE_VERSION;

    #[tokio::test]
    async fn canceled_release_waiter_resumes_one_operation_until_completion() {
        let release = Arc::new(RuntimeHandleRelease::new());
        let submitted = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let first_release = release.clone();
        let first_submitted = submitted.clone();
        let first = tokio::spawn(async move {
            first_release
                .run(move || async move {
                    first_submitted.fetch_add(1, Ordering::SeqCst);
                    started_tx.send(()).unwrap();
                    finish_rx.await.unwrap();
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let second = release
            .run(|| async { panic!("a canceled release must resume its original operation") });
        tokio::pin!(second);
        std::future::poll_fn(|cx| match second.as_mut().poll(cx) {
            std::task::Poll::Pending => std::task::Poll::Ready(()),
            std::task::Poll::Ready(_) => panic!("close returned before release completion"),
        })
        .await;
        finish_tx.send(()).unwrap();
        second.await.unwrap();
        release
            .run(|| async { panic!("completed handle release must not run twice") })
            .await
            .unwrap();
        assert_eq!(submitted.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_release_can_be_retried_without_marking_completion() {
        let release = RuntimeHandleRelease::new();
        let error = release
            .run(|| async { Err(ClosedSdkConsumerError::RuntimeSubmitRejected { status: -1 }) })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ClosedSdkConsumerError::RuntimeSubmitRejected { status: -1 }
        ));
        release.run(|| async { Ok(()) }).await.unwrap();
        release
            .run(|| async { panic!("successful retry must finish the release") })
            .await
            .unwrap();
    }

    #[test]
    fn current_provider_matches_closed_sdk() {
        let snapshot = assert_current_provider_contract()
            .expect("source-backed provider must match closed SDK");
        assert_eq!(
            snapshot.version_info.required_open_surface_version,
            FLUXON_COMMU_OPEN_SURFACE_VERSION
        );
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
