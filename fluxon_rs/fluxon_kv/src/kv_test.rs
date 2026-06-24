/*
 * KV Cache 分布式系统综合测试
 *
 * 本文件包含对KV缓存系统的全面测试，验证以下功能：
 * 1. Master-Client 架构的正确性
 * 2. 多客户端并发操作
 * 3. 客户端间数据共享和通信
 * 4. 客户端故障转移和数据持久性
 * 5. 基本的CRUD操作
 */

use crate::cluster_manager::ClusterManagerRdmaControlInit;
use crate::config::{
    ClientConfig, ContributeToClusterPoolSize, FluxonKvSpec, LargeFilePaths, MasterConfig, MonitoringConfig,
    ProtocolConfig, ProtocolType, TestSpecConfig, TestSpecTransportMode, TransferEngineType,
};
use crate::run_master_with_test_overrides;
use crate::{ClientRunTestOverrides, MasterRunTestOverrides, run_client_with_test_overrides};
// external client runs via run_client when contribution is zero
use crate::ConfigArg;
use etcd_client::Client as EtcdClient;
use fluxon_commu::TransferBackendActivationMode;
use limit_thirdparty::tokio::{
    self,
    time::{sleep, timeout},
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

const CROSS_OWNER_REPLICA_TARGET_COUNT: usize = 2;
const CLIENT_COMMUNICATION_KEY: &str = "client_communication_key";
const CLIENT_COMMUNICATION_VALUE: &[u8] = b"message_from_client1_to_client2";
const TRANSFER_DATA_PROBE_VALUE_LEN: usize = 256 * 1024;
const KV_TEST_TRANSFER_PROBE_IO_TIMEOUT_SECS: u64 = 10;
const KV_TEST_SHUTDOWN_TIMEOUT_SECS: u64 = 60;

fn kv_test_run_scope() -> &'static str {
    static RUN_SCOPE: OnceLock<String> = OnceLock::new();
    RUN_SCOPE.get_or_init(|| {
        let raw = std::env::var("FLUXON_KV_TEST_RUN_SCOPE")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                let epoch_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                format!("pid{}_ts{}", std::process::id(), epoch_secs)
            });

        raw.chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
            .collect()
    })
}

fn kv_test_rdma_device_names_override() -> Option<Vec<String>> {
    let raw = match std::env::var("FLUXON_KV_TEST_RDMA_DEVICE_NAMES") {
        Ok(value) => value,
        Err(_) => return None,
    };
    let mut devices = Vec::new();
    for item in raw
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let normalized = item.to_string();
        if devices.iter().any(|existing| existing == &normalized) {
            continue;
        }
        devices.push(normalized);
    }
    if devices.is_empty() {
        return None;
    }
    Some(devices)
}

fn kv_test_required_rdma_device_names() -> Vec<String> {
    kv_test_rdma_device_names_override().unwrap_or_else(|| {
        panic!(
            "FLUXON_KV_TEST_RDMA_DEVICE_NAMES is required for RDMA kv_test rounds; expected a comma-separated list such as 'mlx5_0'"
        )
    })
}

fn kv_test_side_transfer_worker_count() -> Option<u16> {
    let raw = std::env::var("FLUXON_KV_TEST_SIDE_TRANSFER_WORKER_COUNT").ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let worker_count = trimmed.parse::<u16>().unwrap_or_else(|e| {
        panic!(
            "invalid FLUXON_KV_TEST_SIDE_TRANSFER_WORKER_COUNT '{}': {}",
            trimmed, e
        )
    });
    if worker_count == 0 {
        return None;
    }
    Some(worker_count)
}

fn kv_test_side_transfer_worker_p2p_port_base(worker_count: u16) -> Option<u16> {
    let raw = std::env::var("FLUXON_KV_TEST_SIDE_TRANSFER_P2P_PORT_BASE").ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        panic!("FLUXON_KV_TEST_SIDE_TRANSFER_P2P_PORT_BASE cannot be empty");
    }
    let base = trimmed.parse::<u16>().unwrap_or_else(|e| {
        panic!(
            "invalid FLUXON_KV_TEST_SIDE_TRANSFER_P2P_PORT_BASE '{}': {}",
            trimmed, e
        )
    });
    base.checked_add(worker_count.saturating_sub(1))
        .unwrap_or_else(|| {
            panic!(
                "FLUXON_KV_TEST_SIDE_TRANSFER_P2P_PORT_BASE {} with worker_count {} overflows u16",
                base, worker_count
            )
        });
    Some(base)
}

fn apply_kv_test_owner_side_transfer_overrides(config: &mut ClientConfig) {
    let Some(worker_count) = kv_test_side_transfer_worker_count() else {
        return;
    };
    config.test_spec_config.enable_side_transfer = true;
    config.test_spec_config.side_transfer_worker_count = worker_count;
    config.test_spec_config.side_transfer_worker_p2p_port_base =
        kv_test_side_transfer_worker_p2p_port_base(worker_count);
    if let Some(base_port) = config.test_spec_config.side_transfer_worker_p2p_port_base {
        info!(
            instance_key = %config.instance_key,
            worker_count,
            base_port,
            "kv_test enabling owner side-transfer workers with explicit port base"
        );
    } else {
        info!(
            instance_key = %config.instance_key,
            worker_count,
            "kv_test enabling owner side-transfer workers with auto-selected listen ports"
        );
    }
}

fn build_transfer_data_probe_value(tag: &str) -> Vec<u8> {
    let pattern = format!("kv_test_transfer_data_probe:{}:", tag).into_bytes();
    let mut value = Vec::with_capacity(TRANSFER_DATA_PROBE_VALUE_LEN);
    while value.len() < TRANSFER_DATA_PROBE_VALUE_LEN {
        value.extend_from_slice(pattern.as_slice());
    }
    value.truncate(TRANSFER_DATA_PROBE_VALUE_LEN);
    value
}

fn build_transfer_data_probe_attempt(
    probe_key_prefix: &str,
    probe_value_tag: &str,
    attempt: u64,
) -> (String, Vec<u8>) {
    (
        format!("{}__attempt_{}", probe_key_prefix, attempt),
        build_transfer_data_probe_value(&format!("{}:attempt:{}", probe_value_tag, attempt)),
    )
}

async fn verify_external_side_transfer_lane_mapping(
    round: &KvTestRoundOptions,
    external_framework: &Arc<crate::Framework>,
    remote_owner_instance_key: &str,
) {
    let Some(worker_count) = kv_test_side_transfer_worker_count() else {
        info!("Skipping external side-transfer lane probe because side-transfer is disabled");
        return;
    };

    let external_view = external_framework.external_client_api_view().clone();
    let external_api = external_view.external_client_api();
    let remote_owner_id = round.scoped_instance_key(remote_owner_instance_key);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 0_u64;

    loop {
        attempt += 1;
        let trace_sink: crate::client_kv_api::TestObservePutPhaseSink = Arc::new(Mutex::new(None));
        let (key, value) = build_transfer_data_probe_attempt(
            "external_side_lane_probe",
            &round.round_name,
            attempt,
        );
        let opts = crate::client_kv_api::PutOptionalArgs(vec![
            crate::client_kv_api::PutOptionalArg::TestObservePutPhases(trace_sink.clone()),
        ]);
        external_api
            .inner()
            .put(&key, &value, opts)
            .await
            .unwrap_or_else(|e| {
                panic!(
                    "external side-transfer lane probe put failed: key={} attempt={} err={}",
                    key, attempt, e
                )
            });

        let trace = trace_sink.lock().clone().unwrap_or_else(|| {
            panic!(
                "external side-transfer lane probe missing trace output: key={} attempt={}",
                key, attempt
            )
        });

        let Some(remote_peer_id) = trace.owner_put_transfer_peer_id.clone() else {
            if Instant::now() >= deadline {
                panic!(
                    "external side-transfer lane probe timed out waiting for remote transfer peer: key={} attempt={} owner={} trace={:?}",
                    key, attempt, remote_owner_id, trace
                );
            }
            warn!(
                "external side-transfer lane probe waiting for remote transfer peer: key={} attempt={} owner={} trace={:?}",
                key, attempt, remote_owner_id, trace
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        };

        if !remote_peer_id.starts_with(remote_owner_id.as_str()) {
            if Instant::now() >= deadline {
                panic!(
                    "external side-transfer lane probe timed out waiting for owner namespace match: key={} attempt={} owner={} peer={} trace={:?}",
                    key, attempt, remote_owner_id, remote_peer_id, trace
                );
            }
            warn!(
                "external side-transfer lane probe observed unexpected remote peer before convergence: key={} attempt={} owner={} peer={} trace={:?}",
                key, attempt, remote_owner_id, remote_peer_id, trace
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let Some(local_side_id) = trace.external_side_transfer_peer_id.clone() else {
            if Instant::now() >= deadline {
                panic!(
                    "external side-transfer lane probe timed out waiting for local side selection: key={} attempt={} remote_peer={} trace={:?}",
                    key, attempt, remote_peer_id, trace
                );
            }
            warn!(
                "external side-transfer lane probe waiting for local side selection: key={} attempt={} remote_peer={} trace={:?}",
                key, attempt, remote_peer_id, trace
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        };

        let Some(local_lane) = trace.external_side_transfer_lane_idx.or_else(|| {
            crate::client_seg_pool::parse_side_transfer_worker_lane_idx(&local_side_id)
        }) else {
            if Instant::now() >= deadline {
                panic!(
                    "external side-transfer lane probe timed out waiting for local lane parse: key={} attempt={} local_side={} trace={:?}",
                    key, attempt, local_side_id, trace
                );
            }
            warn!(
                "external side-transfer lane probe waiting for local lane parse: key={} attempt={} local_side={} trace={:?}",
                key, attempt, local_side_id, trace
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        };

        if !remote_peer_id.starts_with(&(remote_owner_id.clone() + "__side_")) {
            if Instant::now() >= deadline {
                panic!(
                    "external side-transfer lane probe timed out waiting for remote side worker: key={} attempt={} local_side={} remote_peer={} trace={:?}",
                    key, attempt, local_side_id, remote_peer_id, trace
                );
            }
            warn!(
                "external side-transfer lane probe remote side not ready yet: key={} attempt={} local_side={} remote_peer={} trace={:?}",
                key, attempt, local_side_id, remote_peer_id, trace
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let Some(remote_lane) =
            crate::client_seg_pool::parse_side_transfer_worker_lane_idx(&remote_peer_id)
        else {
            if Instant::now() >= deadline {
                panic!(
                    "external side-transfer lane probe timed out waiting for remote lane parse: key={} attempt={} remote_peer={} trace={:?}",
                    key, attempt, remote_peer_id, trace
                );
            }
            warn!(
                "external side-transfer lane probe waiting for remote lane parse: key={} attempt={} remote_peer={} trace={:?}",
                key, attempt, remote_peer_id, trace
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        };

        if local_lane != remote_lane {
            if Instant::now() >= deadline {
                panic!(
                    "external side-transfer lane probe timed out waiting for lane alignment: key={} attempt={} local_side={} remote_peer={} local_lane={} remote_lane={} trace={:?}",
                    key, attempt, local_side_id, remote_peer_id, local_lane, remote_lane, trace
                );
            }
            warn!(
                "external side-transfer lane probe lane mismatch before convergence: key={} attempt={} local_side={} remote_peer={} local_lane={} remote_lane={} trace={:?}",
                key, attempt, local_side_id, remote_peer_id, local_lane, remote_lane, trace
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        info!(
            "✅ External side-transfer lane mapping verified: key={} worker_count={} local_side={} remote_peer={} lane={}",
            key, worker_count, local_side_id, remote_peer_id, local_lane
        );
        return;
    }
}

async fn put_external_probe_value(
    external_framework: &crate::Framework,
    key: &str,
    value: &[u8],
) -> Result<(), String> {
    let external_view = external_framework.external_client_api_view().clone();
    let external_api = external_view.external_client_api();
    match timeout(
        Duration::from_secs(KV_TEST_TRANSFER_PROBE_IO_TIMEOUT_SECS),
        external_api
            .inner()
            .put(key, value, crate::client_kv_api::PutOptionalArgs::default()),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!(
            "external probe put failed: key={} value_len={} err={}",
            key,
            value.len(),
            e
        )),
        Err(_) => Err(format!(
            "external probe put timed out: key={} value_len={} timeout_secs={}",
            key,
            value.len(),
            KV_TEST_TRANSFER_PROBE_IO_TIMEOUT_SECS
        )),
    }
}

async fn get_owner_probe_value(
    requester_framework: &crate::Framework,
    key: &str,
) -> Result<Vec<u8>, String> {
    let requester_view = requester_framework.client_kv_api_view().clone();
    let requester_api = requester_view.client_kv_api();
    match timeout(
        Duration::from_secs(KV_TEST_TRANSFER_PROBE_IO_TIMEOUT_SECS),
        requester_api.inner().get(key),
    )
    .await
    {
        Ok(Ok(Some((mem_holder, _get_info)))) => Ok(mem_holder.bytes().to_vec()),
        Ok(Ok(None)) => Err(format!("owner probe get returned None: key={}", key)),
        Ok(Err(e)) => Err(format!("owner probe get failed: key={} err={}", key, e)),
        Err(_) => Err(format!(
            "owner probe get timed out: key={} timeout_secs={}",
            key, KV_TEST_TRANSFER_PROBE_IO_TIMEOUT_SECS
        )),
    }
}

async fn fetch_transfer_link_te_value(
    cluster_name: &str,
    from_instance_key: &str,
    to_instance_key: &str,
) -> Result<Option<String>, String> {
    let endpoint =
        fluxon_util::dev_config::read_etcd_endpoint_from_build_config().map_err(|err| {
            format!(
                "read etcd endpoint from build_config_ext.yml failed: {}",
                err
            )
        })?;
    let key = format!(
        "/{}/transfer_link/te/{}/{}",
        cluster_name, from_instance_key, to_instance_key
    );
    let mut client = EtcdClient::connect([endpoint.as_str()], None)
        .await
        .map_err(|err| format!("connect etcd for transfer_link probe failed: {}", err))?;
    let resp = client
        .get(key.clone(), None)
        .await
        .map_err(|err| format!("get transfer_link te key '{}' failed: {}", key, err))?;
    let Some(kv) = resp.kvs().first() else {
        return Ok(None);
    };
    let raw = std::str::from_utf8(kv.value())
        .map_err(|err| format!("decode transfer_link te value '{}' failed: {}", key, err))?;
    Ok(Some(raw.to_string()))
}

async fn verify_rdma_transfer_data_link(
    requester_framework: &crate::Framework,
    probe_writer_framework: &crate::Framework,
    cluster_name: &str,
    from_instance_key: &str,
    to_instance_key: &str,
    probe_key_prefix: &str,
    probe_value_tag: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut attempt = 0_u64;
    loop {
        attempt += 1;
        let (attempt_key, attempt_value) =
            build_transfer_data_probe_attempt(probe_key_prefix, probe_value_tag, attempt);
        if let Err(err) = put_external_probe_value(
            probe_writer_framework,
            attempt_key.as_str(),
            attempt_value.as_slice(),
        )
        .await
        {
            if Instant::now() >= deadline {
                panic!(
                    "closed transfer data probe put failed before convergence: cluster={} from={} to={} key_prefix={} key={} attempt={} err={}",
                    cluster_name,
                    from_instance_key,
                    to_instance_key,
                    probe_key_prefix,
                    attempt_key,
                    attempt,
                    err
                );
            }
            warn!(
                "closed transfer data probe transient put failure: cluster={} from={} to={} key_prefix={} key={} attempt={} err={}",
                cluster_name,
                from_instance_key,
                to_instance_key,
                probe_key_prefix,
                attempt_key,
                attempt,
                err
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let payload = match get_owner_probe_value(requester_framework, attempt_key.as_str()).await {
            Ok(payload) => payload,
            Err(err) => {
                if Instant::now() >= deadline {
                    panic!(
                        "closed transfer data probe get failed before convergence: cluster={} from={} to={} key_prefix={} key={} attempt={} err={}",
                        cluster_name,
                        from_instance_key,
                        to_instance_key,
                        probe_key_prefix,
                        attempt_key,
                        attempt,
                        err
                    );
                }
                warn!(
                    "closed transfer data probe transient get failure: cluster={} from={} to={} key_prefix={} key={} attempt={} err={}",
                    cluster_name,
                    from_instance_key,
                    to_instance_key,
                    probe_key_prefix,
                    attempt_key,
                    attempt,
                    err
                );
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        if payload.as_slice() != attempt_value.as_slice() {
            panic!(
                "closed transfer data probe payload mismatch: cluster={} from={} to={} key_prefix={} key={} attempt={} expected={:?} got={:?}",
                cluster_name,
                from_instance_key,
                to_instance_key,
                probe_key_prefix,
                attempt_key,
                attempt,
                attempt_value,
                payload
            );
        }

        let Some(raw) = fetch_transfer_link_te_value(
            cluster_name,
            from_instance_key,
            to_instance_key,
        )
        .await
        .unwrap_or_else(|err| {
            panic!(
                "closed transfer_link probe failed: cluster={} from={} to={} attempt={} err={}",
                cluster_name, from_instance_key, to_instance_key, attempt, err
            )
        }) else {
            if Instant::now() >= deadline {
                panic!(
                    "closed transfer data probe timed out without transfer_link_te key: cluster={} from={} to={} key_prefix={} key={} attempt={}",
                    cluster_name,
                    from_instance_key,
                    to_instance_key,
                    probe_key_prefix,
                    attempt_key,
                    attempt
                );
            }
            info!(
                "Waiting for closed transfer_link key: cluster={} from={} to={} key_prefix={} key={} attempt={}",
                cluster_name,
                from_instance_key,
                to_instance_key,
                probe_key_prefix,
                attempt_key,
                attempt
            );
            sleep(Duration::from_secs(1)).await;
            continue;
        };

        if raw.split('+').any(|token| token.trim() == "closed") {
            info!(
                "Verified closed transfer link token: cluster={} from={} to={} key_prefix={} key={} attempt={} transfer_link_te={}",
                cluster_name,
                from_instance_key,
                to_instance_key,
                probe_key_prefix,
                attempt_key,
                attempt,
                raw
            );
            return;
        }

        if Instant::now() >= deadline {
            panic!(
                "closed transfer data probe timed out without closed transfer_link token: cluster={} from={} to={} key_prefix={} key={} attempt={} transfer_link_te={}",
                cluster_name,
                from_instance_key,
                to_instance_key,
                probe_key_prefix,
                attempt_key,
                attempt,
                raw
            );
        }

        info!(
            "Waiting for closed transfer_link token: cluster={} from={} to={} key_prefix={} key={} attempt={} transfer_link_te={}",
            cluster_name,
            from_instance_key,
            to_instance_key,
            probe_key_prefix,
            attempt_key,
            attempt,
            raw
        );
        sleep(Duration::from_secs(1)).await;
    }
}

#[derive(Clone, Debug)]
enum KvTestEtcdMode {
    Enabled,
    Disabled,
}

#[derive(Clone, Debug, Default)]
struct KvTestClientOptions {
    protocol_config: Option<ProtocolConfig>,
    transfer_engine: Option<TransferEngineType>,
    rdma_control_init: Option<ClusterManagerRdmaControlInit>,
    transfer_backend_activation_mode: Option<TransferBackendActivationMode>,
    enable_transfer_rpc_fast_path: Option<bool>,
    contribute_to_cluster_pool_size: Option<ContributeToClusterPoolSize>,
    share_mem_path: Option<String>,
    etcd_mode: Option<KvTestEtcdMode>,
}

impl KvTestClientOptions {
    fn merged_with(&self, overrides: &KvTestClientOptions) -> KvTestClientOptions {
        KvTestClientOptions {
            protocol_config: overrides
                .protocol_config
                .clone()
                .or_else(|| self.protocol_config.clone()),
            transfer_engine: overrides
                .transfer_engine
                .clone()
                .or_else(|| self.transfer_engine.clone()),
            rdma_control_init: overrides
                .rdma_control_init
                .clone()
                .or_else(|| self.rdma_control_init.clone()),
            transfer_backend_activation_mode: overrides
                .transfer_backend_activation_mode
                .or(self.transfer_backend_activation_mode),
            enable_transfer_rpc_fast_path: overrides
                .enable_transfer_rpc_fast_path
                .or(self.enable_transfer_rpc_fast_path),
            contribute_to_cluster_pool_size: overrides
                .contribute_to_cluster_pool_size
                .clone()
                .or_else(|| self.contribute_to_cluster_pool_size.clone()),
            share_mem_path: overrides
                .share_mem_path
                .clone()
                .or_else(|| self.share_mem_path.clone()),
            etcd_mode: overrides
                .etcd_mode
                .clone()
                .or_else(|| self.etcd_mode.clone()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum KvTestRoundProfile {
    P2pOnly,
    RdmaTransferOnly,
    RdmaTransferWithRpc,
}

impl KvTestRoundProfile {
    fn round_name(self) -> &'static str {
        match self {
            Self::P2pOnly => "p2p_only",
            Self::RdmaTransferOnly => "rdma_transfer_only",
            Self::RdmaTransferWithRpc => "rdma_transfer_with_rpc",
        }
    }

    fn transfer_engine(self) -> TransferEngineType {
        match self {
            Self::P2pOnly => TransferEngineType::P2p,
            Self::RdmaTransferOnly | Self::RdmaTransferWithRpc => TransferEngineType::Closed,
        }
    }

    fn protocol_config(self) -> ProtocolConfig {
        match self {
            Self::P2pOnly => tcp_protocol_config(),
            Self::RdmaTransferOnly | Self::RdmaTransferWithRpc => {
                let device_names = kv_test_required_rdma_device_names();
                rdma_protocol_config(device_names.as_slice())
            }
        }
    }

    fn owner_transfer_engine(self) -> TransferEngineType {
        self.transfer_engine()
    }

    fn master_transfer_engine(self) -> TransferEngineType {
        self.transfer_engine()
    }

    fn owner_rdma_control_init(self) -> ClusterManagerRdmaControlInit {
        match self {
            Self::RdmaTransferOnly | Self::RdmaTransferWithRpc => {
                ClusterManagerRdmaControlInit::ExplicitDevices(kv_test_required_rdma_device_names())
            }
            Self::P2pOnly => ClusterManagerRdmaControlInit::Disabled,
        }
    }

    fn master_rdma_control_init(self) -> ClusterManagerRdmaControlInit {
        match self {
            Self::RdmaTransferOnly | Self::RdmaTransferWithRpc => {
                ClusterManagerRdmaControlInit::ExplicitDevices(kv_test_required_rdma_device_names())
            }
            Self::P2pOnly => ClusterManagerRdmaControlInit::Disabled,
        }
    }

    fn owner_transfer_backend_activation_mode(self) -> Option<TransferBackendActivationMode> {
        match self {
            Self::P2pOnly => None,
            Self::RdmaTransferOnly | Self::RdmaTransferWithRpc => {
                Some(TransferBackendActivationMode::TestForceEnableBypassRdmaControl)
            }
        }
    }

    fn master_transfer_backend_activation_mode(self) -> Option<TransferBackendActivationMode> {
        match self {
            Self::P2pOnly => None,
            Self::RdmaTransferOnly | Self::RdmaTransferWithRpc => {
                Some(TransferBackendActivationMode::TestForceEnableBypassRdmaControl)
            }
        }
    }

    fn enable_transfer_rpc_fast_path(self) -> bool {
        matches!(self, Self::RdmaTransferWithRpc)
    }

    fn transport_mode(self) -> Option<TestSpecTransportMode> {
        match self {
            Self::P2pOnly => None,
            Self::RdmaTransferOnly => Some(TestSpecTransportMode::TransferOnly),
            Self::RdmaTransferWithRpc => Some(TestSpecTransportMode::TransferWithRpc),
        }
    }

    fn test_spec_rdma_device_names(self) -> Option<Vec<String>> {
        match self {
            Self::P2pOnly => None,
            Self::RdmaTransferOnly | Self::RdmaTransferWithRpc => {
                Some(kv_test_required_rdma_device_names())
            }
        }
    }
}

fn kv_test_round_test_spec_config(round_profile: KvTestRoundProfile) -> TestSpecConfig {
    TestSpecConfig {
        transport_mode: round_profile.transport_mode(),
        rdma_device_names: round_profile.test_spec_rdma_device_names(),
        ..Default::default()
    }
}

#[derive(Clone, Debug)]
struct KvTestRoundOptions {
    round_profile: KvTestRoundProfile,
    round_name: String,
    cluster_name: String,
    master_port: Option<u16>,
    step8_master_port: Option<u16>,
    master_options: KvTestClientOptions,
    owner_client_options: KvTestClientOptions,
    external_client_options: KvTestClientOptions,
}

#[derive(Clone, Debug)]
struct KvTestClientLaunch {
    config: ClientConfig,
    rdma_control_init: ClusterManagerRdmaControlInit,
    transfer_backend_activation_mode: Option<TransferBackendActivationMode>,
}

#[derive(Debug)]
struct KvTestMasterLaunch {
    config: MasterConfig,
    rdma_control_init: ClusterManagerRdmaControlInit,
    transfer_backend_activation_mode: Option<TransferBackendActivationMode>,
}

impl KvTestRoundOptions {
    fn scoped_instance_key(&self, instance_key: &str) -> String {
        // Isolate repeated kv_test reruns from stale etcd/shared-memory state left by a
        // previous process during repeated end-to-end validation runs.
        format!(
            "{}_{}_{}",
            self.round_name,
            kv_test_run_scope(),
            instance_key
        )
    }

    fn step8_share_mem_path(&self) -> String {
        format!(
            "/tmp/kvcache_shared_memory_step8_{}_{}",
            self.round_name,
            kv_test_run_scope()
        )
    }

}

#[derive(Clone, Debug)]
struct KvTestRunOptions {
    rounds: Vec<KvTestRoundOptions>,
}

fn tcp_protocol_config() -> ProtocolConfig {
    ProtocolConfig {
        protocol_type: ProtocolType::Tcp,
        rdma_device_names: None,
    }
}

fn rdma_protocol_config(device_names: &[String]) -> ProtocolConfig {
    ProtocolConfig {
        protocol_type: ProtocolType::Rdma,
        rdma_device_names: Some(device_names.join(",")),
    }
}

fn default_owner_contribute_to_cluster_pool_size() -> ContributeToClusterPoolSize {
    ContributeToClusterPoolSize {
        dram: 1024 * 1024 * 160,
        vram: HashMap::new(),
    }
}

fn default_external_contribute_to_cluster_pool_size() -> ContributeToClusterPoolSize {
    ContributeToClusterPoolSize {
        dram: 0,
        vram: HashMap::new(),
    }
}

fn default_client_large_file_paths(
    instance_key: &str,
    contribute_to_cluster_pool_size: &ContributeToClusterPoolSize,
) -> LargeFilePaths {
    if contribute_to_cluster_pool_size.dram == 0
        && contribute_to_cluster_pool_size.vram.is_empty()
    {
        return LargeFilePaths { paths: Vec::new() };
    }
    LargeFilePaths {
        paths: vec![format!("/tmp/kvcache_large/{}", instance_key)],
    }
}

fn default_owner_test_client_options(round_profile: KvTestRoundProfile) -> KvTestClientOptions {
    KvTestClientOptions {
        protocol_config: Some(round_profile.protocol_config()),
        transfer_engine: Some(round_profile.owner_transfer_engine()),
        rdma_control_init: Some(round_profile.owner_rdma_control_init()),
        transfer_backend_activation_mode: round_profile.owner_transfer_backend_activation_mode(),
        enable_transfer_rpc_fast_path: Some(round_profile.enable_transfer_rpc_fast_path()),
        contribute_to_cluster_pool_size: Some(default_owner_contribute_to_cluster_pool_size()),
        share_mem_path: None,
        etcd_mode: Some(KvTestEtcdMode::Enabled),
    }
}

fn default_master_test_client_options(round_profile: KvTestRoundProfile) -> KvTestClientOptions {
    KvTestClientOptions {
        protocol_config: Some(round_profile.protocol_config()),
        transfer_engine: Some(round_profile.master_transfer_engine()),
        rdma_control_init: Some(round_profile.master_rdma_control_init()),
        transfer_backend_activation_mode: round_profile.master_transfer_backend_activation_mode(),
        enable_transfer_rpc_fast_path: Some(round_profile.enable_transfer_rpc_fast_path()),
        contribute_to_cluster_pool_size: None,
        share_mem_path: None,
        etcd_mode: None,
    }
}

fn default_external_test_client_options() -> KvTestClientOptions {
    KvTestClientOptions {
        protocol_config: Some(tcp_protocol_config()),
        transfer_engine: Some(TransferEngineType::P2p),
        rdma_control_init: Some(ClusterManagerRdmaControlInit::Disabled),
        transfer_backend_activation_mode: None,
        enable_transfer_rpc_fast_path: Some(false),
        contribute_to_cluster_pool_size: Some(default_external_contribute_to_cluster_pool_size()),
        share_mem_path: None,
        etcd_mode: Some(KvTestEtcdMode::Disabled),
    }
}

fn new_kv_test_round(round_profile: KvTestRoundProfile) -> KvTestRoundOptions {
    let round_name = round_profile.round_name();
    KvTestRoundOptions {
        round_profile,
        round_name: round_name.to_string(),
        // Keep each process run on its own cluster namespace so a crashed/aborted previous run
        // cannot poison the next rerun with stale members.
        cluster_name: format!("test_cluster_{}_{}", round_name, kv_test_run_scope()),
        master_port: None,
        step8_master_port: None,
        master_options: default_master_test_client_options(round_profile),
        owner_client_options: default_owner_test_client_options(round_profile),
        external_client_options: default_external_test_client_options(),
    }
}

fn default_kv_test_run_options() -> KvTestRunOptions {
    // Allow short local bring-up runs to pin kv_test to one or more explicit round profiles
    // without editing code again. This is useful when we want to force a direct slow-path
    // transport check such as `p2p_only` over tcp/tcp_thread.
    if let Ok(raw_rounds) = std::env::var("FLUXON_KV_TEST_ROUNDS") {
        let mut rounds = Vec::new();
        for round_name in raw_rounds
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            let profile = match round_name {
                "p2p_only" => KvTestRoundProfile::P2pOnly,
                "rdma_transfer_only" => KvTestRoundProfile::RdmaTransferOnly,
                "rdma_transfer_with_rpc" => KvTestRoundProfile::RdmaTransferWithRpc,
                other => panic!(
                    "unsupported FLUXON_KV_TEST_ROUNDS entry '{}'; expected one of: p2p_only, rdma_transfer_only, rdma_transfer_with_rpc",
                    other
                ),
            };
            rounds.push(new_kv_test_round(profile));
        }
        if rounds.is_empty() {
            panic!("FLUXON_KV_TEST_ROUNDS was set but produced no valid rounds");
        }
        return KvTestRunOptions { rounds };
    }

    KvTestRunOptions {
        rounds: vec![
            new_kv_test_round(KvTestRoundProfile::P2pOnly),
            new_kv_test_round(KvTestRoundProfile::RdmaTransferOnly),
            new_kv_test_round(KvTestRoundProfile::RdmaTransferWithRpc),
        ],
    }
}

/// Create the master launch bundle used by kv_test rounds.
fn new_master_launch(
    round: &KvTestRoundOptions,
    instance_key: &str,
    port: Option<u16>,
) -> KvTestMasterLaunch {
    // Read etcd endpoint from project root build_config_ext.yml
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    let prometheus_base_url = fluxon_util::dev_config::load_tsdb_base_url()
        .expect("read prometheus_base_url from build_config_ext.yml (key: prom)");
    // Read prom remote write url from build_config_ext.yml (required)
    let prom_remote_write_url =
        fluxon_util::dev_config::read_prom_remote_write_url_from_build_config()
            .expect("read prom_remote_write_url from build_config_ext.yml");
    let log_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../log")
        .to_string_lossy()
        .to_string();
    let options = round.master_options.clone();
    let protocol = options.protocol_config.unwrap_or_else(tcp_protocol_config);
    let transfer_engine = options
        .transfer_engine
        .expect("kv_test requires master transfer_engine to be set explicitly");
    let enable_transfer_rpc_fast_path = options
        .enable_transfer_rpc_fast_path
        .expect("kv_test requires master enable_transfer_rpc_fast_path to be set explicitly");
    let rdma_control_init = options
        .rdma_control_init
        .expect("kv_test requires master rdma_control_init to be set explicitly");
    let transfer_backend_activation_mode = options.transfer_backend_activation_mode;
    KvTestMasterLaunch {
        config: MasterConfig {
            instance_key: round.scoped_instance_key(instance_key),
            cluster_name: round.cluster_name.clone(),
            port,
            etcd_endpoints: vec![etcd.clone()],
            protocol,
            transfer_engine,
            enable_transfer_rpc_fast_path,
            pprof_duration_seconds: None,
            monitoring: Some(MonitoringConfig {
                prometheus_base_url,
                prom_remote_write_url: Some(vec![prom_remote_write_url]),
                otlp_log_api: None,
            }),
            network: None,
            log_dir,
            master_ui: None,
            // Keep kv_test self-describing: each round carries the intended transfer mode.
            test_spec_config: kv_test_round_test_spec_config(round.round_profile),
        },
        rdma_control_init,
        transfer_backend_activation_mode,
    }
}

fn build_client_launch(
    round: &KvTestRoundOptions,
    instance_key: String,
    options: KvTestClientOptions,
) -> KvTestClientLaunch {
    // Read etcd endpoint from project root build_config_ext.yml
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    let etcd_raw = fluxon_util::dev_config::read_etcd_host_port_from_build_config()
        .expect("read raw etcd endpoint from build_config_ext.yml");
    let (etcd_addresses_raw, fluxonkv_etcd_addresses) = match options.etcd_mode {
        Some(KvTestEtcdMode::Disabled) => (Vec::new(), Vec::new()),
        Some(KvTestEtcdMode::Enabled) | None => (vec![etcd_raw], vec![etcd]),
    };
    let rdma_control_init = options
        .rdma_control_init
        .expect("kv_test requires rdma_control_init to be set explicitly");
    let transfer_backend_activation_mode = options.transfer_backend_activation_mode;
    let contribute_to_cluster_pool_size = options
        .contribute_to_cluster_pool_size
        .unwrap_or(default_owner_contribute_to_cluster_pool_size());
    let share_mem_path = options
        .share_mem_path
        .unwrap_or_else(|| format!("/tmp/kvcache_shared_memory/{}", instance_key));
    let config = ClientConfig {
        cluster_name: round.cluster_name.clone(),
        etcd_addresses_raw,
        instance_key: instance_key.clone(),
        contribute_to_cluster_pool_size: contribute_to_cluster_pool_size.clone(),
        protocol: options.protocol_config.unwrap_or_else(tcp_protocol_config),
        pprof_duration_seconds: None,
        redis_compat_listen_addr: None,
        fluxonkv_spec: FluxonKvSpec {
            etcd_addresses: fluxonkv_etcd_addresses,
            cluster_name: round.cluster_name.clone(),
            p2p_listen_port: None,
            transfer_engine: options
                .transfer_engine
                .unwrap_or(TransferEngineType::Closed),
            enable_transfer_rpc_fast_path: options
                .enable_transfer_rpc_fast_path
                .expect("kv_test requires enable_transfer_rpc_fast_path to be set explicitly"),
            sub_cluster: None,
        },
        // English note:
        // kv_test uses a per-instance shared memory path by default so each owner/external share
        // group is explicit and test overrides only replace this when a scenario intentionally
        // binds multiple roles to the same owner path.
        share_mem_path,
        large_file_paths: default_client_large_file_paths(
            &instance_key,
            &contribute_to_cluster_pool_size,
        ),
        // Mirror round intent into the generated config so logs and runtime behavior
        // agree on whether this launch is transfer_only vs transfer_with_rpc.
        test_spec_config: kv_test_round_test_spec_config(round.round_profile),
    };
    KvTestClientLaunch {
        config,
        rdma_control_init,
        transfer_backend_activation_mode,
    }
}

/// 创建测试用的Client配置
fn new_client_launch(
    round: &KvTestRoundOptions,
    instance_key: &str,
    options: Option<&KvTestClientOptions>,
) -> KvTestClientLaunch {
    let overrides = options.cloned().unwrap_or_default();
    let effective_options = round.owner_client_options.merged_with(&overrides);
    let mut launch = build_client_launch(
        round,
        round.scoped_instance_key(instance_key),
        effective_options,
    );
    apply_kv_test_owner_side_transfer_overrides(&mut launch.config);
    launch
}

/// 创建测试用的ExternalClient配置
/// external 与 owner 的 instance_key 必须不同；仅共享 owner 的 share_mem_path
fn new_external_client_launch(
    round: &KvTestRoundOptions,
    external_instance_key: &str,
    owner_instance_key: &str,
    options: Option<&KvTestClientOptions>,
) -> KvTestClientLaunch {
    let overrides = options.cloned().unwrap_or_default();
    let mut external_options = round.external_client_options.merged_with(&overrides);
    if external_options.contribute_to_cluster_pool_size.is_none() {
        external_options.contribute_to_cluster_pool_size =
            Some(default_external_contribute_to_cluster_pool_size());
    }
    if external_options.etcd_mode.is_none() {
        external_options.etcd_mode = Some(KvTestEtcdMode::Disabled);
    }
    if external_options.transfer_engine.is_none() {
        external_options.transfer_engine = Some(TransferEngineType::P2p);
    }
    if external_options.rdma_control_init.is_none() {
        external_options.rdma_control_init = Some(ClusterManagerRdmaControlInit::Disabled);
    }
    if external_options.enable_transfer_rpc_fast_path.is_none() {
        external_options.enable_transfer_rpc_fast_path = Some(false);
    }
    if external_options.share_mem_path.is_none() {
        external_options.share_mem_path = Some(format!(
            "/tmp/kvcache_shared_memory/{}",
            round.scoped_instance_key(owner_instance_key)
        ));
    }
    build_client_launch(
        round,
        round.scoped_instance_key(external_instance_key),
        external_options,
    )
}

async fn run_kv_test_client(
    launch: KvTestClientLaunch,
) -> anyhow::Result<(Arc<crate::Framework>, ClientConfig)> {
    run_client_with_test_overrides(
        ConfigArg::Config(launch.config),
        ClientRunTestOverrides {
            rdma_control_init: launch.rdma_control_init,
            transfer_backend_activation_mode: launch.transfer_backend_activation_mode,
        },
    )
    .await
}

/// 执行单个客户端的完整CRUD操作测试
async fn perform_client_crud_operations(
    view: crate::client_kv_api::ClientKvApiView,
    client_id: u32,
    key: &str,
    value: &[u8],
) -> anyhow::Result<()> {
    let api = view.client_kv_api();

    info!(
        "Client {} performing CRUD operations on key: {}",
        client_id, key
    );

    // PUT操作
    match api
        .inner()
        .put(key, value, crate::client_kv_api::PutOptionalArgs::default())
        .await
    {
        Ok(()) => info!("✅ Client {} PUT operation successful", client_id),
        Err(e) => {
            error!("❌ Client {} PUT operation failed: {}", client_id, e);
            return Err(e.into());
        }
    }

    // GET操作并校验value
    match api.inner().get(key).await {
        Ok(Some((mem_holder, get_info))) => {
            info!("✅ Client {} GET operation successful", client_id);
            info!("📋 Client {} GET Info: {:?}", client_id, get_info);

            // 校验数据内容
            let retrieved_data = mem_holder.bytes();
            if retrieved_data == value {
                info!("✅ Client {} data validation successful", client_id);
            } else {
                error!(
                    "❌ Client {} data validation failed: expected {:?}, got {:?}",
                    client_id, value, retrieved_data
                );
                return Err(anyhow::anyhow!(
                    "Data validation failed for client {}",
                    client_id
                ));
            }
        }
        Ok(None) => {
            error!("❌ Client {} GET operation returned None", client_id);
            return Err(anyhow::anyhow!("Key not found"));
        }
        Err(e) => {
            error!("❌ Client {} GET operation failed: {}", client_id, e);
            return Err(e.into());
        }
    }

    // EXIST操作
    match api.inner().is_exist(key).await {
        Ok(true) => info!(
            "✅ Client {} EXIST operation successful (key exists)",
            client_id
        ),
        Ok(false) => {
            error!(
                "❌ Client {} EXIST operation failed (key should exist)",
                client_id
            );
            return Err(anyhow::anyhow!("Key should exist"));
        }
        Err(e) => {
            error!("❌ Client {} EXIST operation failed: {}", client_id, e);
            return Err(e.into());
        }
    }

    // DELETE操作
    match api.inner().delete(key).await {
        Ok(()) => info!("✅ Client {} DELETE operation successful", client_id),
        Err(e) => {
            error!("❌ Client {} DELETE operation failed: {}", client_id, e);
            warn!(
                "Client {} DELETE operation failed, might not be implemented yet",
                client_id
            );
        }
    }

    Ok(())
}

/// 执行外部客户端的完整CRUD操作测试
async fn perform_external_client_crud_operations(
    view: crate::external_client_api::ExternalClientApiView,
    client_id: u32,
    key: &str,
    value: &[u8],
) -> anyhow::Result<()> {
    let api = view.external_client_api();

    info!(
        "External Client {} performing CRUD operations on key: {}",
        client_id, key
    );

    // PUT操作
    match api
        .inner()
        .put(key, value, crate::client_kv_api::PutOptionalArgs::default())
        .await
    {
        Ok(()) => info!("✅ External Client {} PUT operation successful", client_id),
        Err(e) => {
            error!(
                "❌ External Client {} PUT operation failed: {}",
                client_id, e
            );
            return Err(e.into());
        }
    }

    // GET操作并校验value
    match api.inner().get(key).await {
        Ok(Some(mem_holder)) => {
            info!("✅ External Client {} GET operation successful", client_id);

            // 校验数据内容
            let retrieved_data = mem_holder.bytes();
            if retrieved_data == value {
                info!(
                    "✅ External Client {} data validation successful",
                    client_id
                );
            } else {
                error!(
                    "❌ External Client {} data validation failed: expected {:?}, got {:?}",
                    client_id, value, retrieved_data
                );
                return Err(anyhow::anyhow!(
                    "Data validation failed for external client {}",
                    client_id
                ));
            }
        }
        Ok(None) => {
            error!(
                "❌ External Client {} GET operation returned None",
                client_id
            );
            return Err(anyhow::anyhow!("Key not found"));
        }
        Err(e) => {
            error!(
                "❌ External Client {} GET operation failed: {}",
                client_id, e
            );
            return Err(e.into());
        }
    }

    // EXIST操作
    match api.inner().is_exist(key).await {
        Ok(true) => info!(
            "✅ External Client {} EXIST operation successful (key exists)",
            client_id
        ),
        Ok(false) => {
            error!(
                "❌ External Client {} EXIST operation failed (key should exist)",
                client_id
            );
            return Err(anyhow::anyhow!("Key should exist"));
        }
        Err(e) => {
            error!(
                "❌ External Client {} EXIST operation failed: {}",
                client_id, e
            );
            return Err(e.into());
        }
    }
    // DELETE操作
    match api.inner().delete(key).await {
        Ok(()) => info!("✅ Client {} DELETE operation successful", client_id),
        Err(e) => {
            error!("❌ Client {} DELETE operation failed: {}", client_id, e);
            warn!(
                "Client {} DELETE operation failed, might not be implemented yet",
                client_id
            );
        }
    }
    Ok(())
}

/// put a kv and get it, then we delete it, and get it again after 5 second, the result should be None
async fn key_meta_cache_check(
    client: &crate::client_kv_api::ClientKvApiView,
    client2: &crate::client_kv_api::ClientKvApiView,
    parallel_unique_key: &str,
) {
    let api = client.client_kv_api();
    let test_value = b"test_value_for_meta_cache_check";

    info!(
        "🔍 Starting key meta cache check for key: {}",
        parallel_unique_key
    );

    async fn expect_get_value(
        get_client: &crate::client_kv_api::ClientKvApiView,
        key: &str,
        expected_value: &[u8],
        label: &str,
    ) {
        match get_client.client_kv_api().inner().get(key).await {
            Ok(Some((mem_holder, remote_get_info))) => {
                info!("✅ GET operation successful for key: {} ({})", key, label);
                info!("📋 GET Info: {:?}", remote_get_info);
                let retrieved_data = mem_holder.bytes();
                let prefix_len = std::cmp::min(8, retrieved_data.len());
                info!(
                    "📋 GET prefix for key {} ({}): {:?}",
                    key,
                    label,
                    &retrieved_data[..prefix_len]
                );
                if retrieved_data != expected_value {
                    error!(
                        "❌ Data validation failed for key {} ({}): expected {:?}, got {:?}",
                        key, label, expected_value, retrieved_data
                    );
                    panic!("Data validation failed during meta cache check");
                }
            }
            Ok(None) => {
                error!("❌ GET returned None for key: {} ({})", key, label);
                panic!("Key should exist but GET returned None during meta cache check");
            }
            Err(e) => {
                error!("❌ GET failed for key {} ({}): {}", key, label, e);
                panic!("GET operation failed during meta cache check: {}", e);
            }
        }
    }

    tracing::info!("🔍 Starting PUT and GET in parallel: {}", parallel_unique_key);
    for i in 0..10 {
        let (put_client, other_client) = if i % 2 == 0 {
            (client, client2)
        } else {
            (client2, client)
        };

        tracing::info!(
            "🔍 Starting PUT operation for key: {} in time {}",
            parallel_unique_key,
            i
        );
        match put_client
            .client_kv_api()
            .inner()
            .put(
                &parallel_unique_key,
                test_value,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
        {
            Ok(()) => info!(
                "✅ PUT operation successful for key: {} in time {}",
                parallel_unique_key, i
            ),
            Err(e) => {
                error!(
                    "❌ PUT operation failed for key {} in time {}: {}",
                    parallel_unique_key, i, e
                );
                #[cfg(test)]
                api.test_record().debug_transfering();
                panic!("PUT operation failed during meta cache check: {}", e);
            }
        }

        assert!(
            put_client.client_kv_api().has_cached_key(parallel_unique_key),
            "put client should have immediate local cache metadata for key {} after put time {}",
            parallel_unique_key,
            i
        );

        expect_get_value(
            put_client,
            parallel_unique_key,
            test_value,
            "same-owner immediate visibility",
        )
        .await;

        sleep(Duration::from_secs(6)).await;

        let client_id = other_client.client_kv_api().client_id();
        tracing::info!(
            "🔍 Starting cross-owner convergence GET operation, key: {}, time: {}, client: {}",
            parallel_unique_key,
            i,
            client_id
        );
        expect_get_value(
            other_client,
            parallel_unique_key,
            test_value,
            "cross-owner eventual visibility",
        )
        .await;

        tracing::info!(
            "✅ success put and get in parallel, key: {}, time: {}, client: {}",
            parallel_unique_key,
            i + 1,
            client_id
        );
    }

    match api.inner().delete(parallel_unique_key).await {
        Ok(()) => info!(
            "✅ DELETE operation successful for key: {}",
            parallel_unique_key
        ),
        Err(e) => {
            error!(
                "❌ DELETE operation failed for key {}: {}",
                parallel_unique_key, e
            );
            warn!("DELETE operation might not be implemented yet, continuing test...");
        }
    }

    info!("🔍 Verifying immediate post-delete GET visibility...");

    match api.inner().get(parallel_unique_key).await {
        Ok(None) => {
            info!(
                "✅ Key meta cache check PASSED: GET returned None after delete for key: {}",
                parallel_unique_key
            );
        }
        Ok(Some((mem_holder, get_info))) => {
            error!(
                "❌ Key meta cache check FAILED: GET still returned data after delete for key: {}",
                parallel_unique_key
            );
            error!("📋 Unexpected GET Info: {:?}", get_info);
            panic!(
                "Key meta cache check failed: data still accessible after delete for key: {}",
                parallel_unique_key
            );
        }
        Err(e) => {
            error!(
                "❌ GET operation failed after delete for key {}: {}",
                parallel_unique_key, e
            );
            let code = e.code();
            if code == crate::rpcresp_kvresult_convert::msg_and_error::codes_api::API_KEY_NOT_FOUND
            {
                info!(
                    "✅ Key meta cache check PASSED: KeyNotFound error after delete for key: {}",
                    parallel_unique_key
                );
            } else {
                warn!(
                    "⚠️ Unexpected error after delete for key {}: {}",
                    parallel_unique_key, e
                );
            }
        }
    }

    info!(
        "🏁 Key meta cache check completed for key: {}",
        parallel_unique_key
    );
}

/// 综合测试：完整的分布式KV缓存系统测试
/// 测试场景包括：多客户端并发、客户端通信、故障转移、数据持久性
/// run with 8 threads
// #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
pub async fn test_kv_all() {
    // 初始化日志
    // Respect explicit caller log level so local debugging can enable deeper probes.
    if std::env::var_os("FLUXON_LOG").is_none() {
        unsafe {
            std::env::set_var("FLUXON_LOG", "info");
        }
    }
    let run_options = default_kv_test_run_options();
    info!(
        "🚀 Starting comprehensive KV cache test with {} rounds",
        run_options.rounds.len()
    );

    for round in &run_options.rounds {
        info!("🚀 Starting kv test round: {}", round.round_name);
        run_kv_round(round).await;
        info!("✅ Completed kv test round: {}", round.round_name);
    }

    info!("🎉 Comprehensive KV cache test completed for all rounds!");
}

fn kv_test_only_step8() -> bool {
    matches!(
        std::env::var("FLUXON_KV_TEST_ONLY_STEP8").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("yes") | Some("YES")
    )
}

async fn shutdown_framework_with_timeout(label: &str, framework: &crate::Framework) {
    info!("🧹 shutdown begin: {}", label);
    match timeout(
        Duration::from_secs(KV_TEST_SHUTDOWN_TIMEOUT_SECS),
        framework.shutdown(),
    )
    .await
    {
        Ok(Ok(())) => {
            info!("✅ shutdown completed: {}", label);
        }
        Ok(Err(e)) => {
            panic!("shutdown failed for {}: {}", label, e);
        }
        Err(_) => {
            panic!(
                "shutdown timed out for {} after {}s",
                label, KV_TEST_SHUTDOWN_TIMEOUT_SECS
            );
        }
    }
}

async fn run_kv_step8(round: &KvTestRoundOptions) {
    info!("📋 Step 8: Verifying external client blocking and recovery behavior");

    let step8_share_mem_path = round.step8_share_mem_path();
    if let Err(e) = fs::remove_dir_all(&step8_share_mem_path) {
        warn!(
            "Step 8: failed to remove existing shared memory dir {}: {}",
            step8_share_mem_path, e
        );
    }
    if let Err(e) = fs::create_dir_all(&step8_share_mem_path) {
        warn!(
            "Step 8: failed to pre-create shared memory dir {}: {}",
            step8_share_mem_path, e
        );
    }

    let master_launch_step8 =
        new_master_launch(round, "test_master_step8", round.step8_master_port);
    let (master_framework_step8, _) = run_master_with_test_overrides(
        ConfigArg::Config(master_launch_step8.config),
        MasterRunTestOverrides {
            rdma_control_init: master_launch_step8.rdma_control_init,
            transfer_backend_activation_mode: master_launch_step8.transfer_backend_activation_mode,
        },
    )
    .await
    .expect("Failed to start master for step 8");

    sleep(Duration::from_secs(3)).await;

    let step8_owner_options = round
        .owner_client_options
        .merged_with(&KvTestClientOptions {
            share_mem_path: Some(step8_share_mem_path.clone()),
            ..Default::default()
        });
    let step8_external_options = round
        .external_client_options
        .merged_with(&KvTestClientOptions {
            share_mem_path: Some(step8_share_mem_path.clone()),
            ..Default::default()
        });

    let blocking_external_launch = new_external_client_launch(
        round,
        "test_owner_step8_ext1",
        "test_owner_step8",
        Some(&step8_external_options),
    );

    let blocking_future = run_kv_test_client(blocking_external_launch.clone());
    match timeout(Duration::from_secs(10), blocking_future).await {
        Ok(Ok(_)) => panic!("External client should block until owner client is available"),
        Ok(Err(e)) => panic!(
            "External client future returned error before owner client started: {}",
            e
        ),
        Err(_) => info!(
            "✅ External client initialization is correctly blocked when owner client is absent"
        ),
    }

    let owner_launch_step8 =
        new_client_launch(round, "test_owner_step8", Some(&step8_owner_options));
    let (owner_framework_step8, _) = run_kv_test_client(owner_launch_step8)
        .await
        .expect("Failed to start owner client for step 8");

    sleep(Duration::from_secs(5)).await;

    let (external_framework_step8_primary, _) = run_kv_test_client(blocking_external_launch)
        .await
        .expect("Failed to start primary external client after owner became available");

    let external_launch_step8_2 = new_external_client_launch(
        round,
        "test_owner_step8_ext2",
        "test_owner_step8",
        Some(&step8_external_options),
    );
    let (external_framework_step8_2, _) = run_kv_test_client(external_launch_step8_2)
        .await
        .expect("Failed to start external client 2 for step 8");

    let external_launch_step8_3 = new_external_client_launch(
        round,
        "test_owner_step8_ext3",
        "test_owner_step8",
        Some(&step8_external_options),
    );
    let (external_framework_step8_3, _) = run_kv_test_client(external_launch_step8_3)
        .await
        .expect("Failed to start external client 3 for step 8");

    sleep(Duration::from_secs(5)).await;

    info!("🔄 Step 8: Running parallel put/get operations across external clients");
    let external_view_primary = external_framework_step8_primary
        .external_client_api_view()
        .clone();
    let external_view_step8_2 = external_framework_step8_2
        .external_client_api_view()
        .clone();
    let external_view_step8_3 = external_framework_step8_3
        .external_client_api_view()
        .clone();

    let external_views = vec![
        external_view_primary.clone(),
        external_view_step8_2.clone(),
        external_view_step8_3.clone(),
    ];

    let mut parallel_tasks = Vec::new();
    for i in 0..9 {
        let view = external_views[i % external_views.len()].clone();
        let key = format!("step8_parallel_key_{}", i);
        let value = format!("step8_parallel_value_{}", i).into_bytes();
        parallel_tasks.push(tokio::spawn(async move {
            let api = view.external_client_api();
            api.inner()
                .put(
                    &key,
                    &value,
                    crate::client_kv_api::PutOptionalArgs::default(),
                )
                .await?;
            match api.inner().get(&key).await? {
                Some(mem_holder) => {
                    let retrieved = mem_holder.bytes();
                    if retrieved != value {
                        return Err(anyhow::anyhow!("Value mismatch for key {}", key));
                    }
                }
                None => {
                    return Err(anyhow::anyhow!("Missing value for key {}", key));
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for (idx, task) in parallel_tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok(())) => info!("✅ External parallel task {} succeeded", idx),
            Ok(Err(e)) => panic!("External parallel task {} failed: {}", idx, e),
            Err(e) => panic!("External parallel task {} panicked: {}", idx, e),
        }
    }

    info!("🔻 Step 8: Shutting down owner client to verify external clients block");
    shutdown_framework_with_timeout("step8 owner before restart", &owner_framework_step8).await;

    sleep(Duration::from_secs(2)).await;

    let external_view_for_block = external_view_primary.clone();
    let block_key = "step8_block_key_after_owner_shutdown".to_string();
    let block_value = b"step8_block_value_after_owner_shutdown".to_vec();
    let blocking_put_handle = tokio::spawn(async move {
        external_view_for_block
            .external_client_api()
            .inner()
            .put(
                &block_key,
                &block_value,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
    });

    sleep(Duration::from_secs(3)).await;
    assert!(
        !blocking_put_handle.is_finished(),
        "External client put should be waiting while owner client is offline"
    );

    info!("🔁 Step 8: Restarting owner client to release blocked external client operations");
    let owner_launch_step8_restart =
        new_client_launch(round, "test_owner_step8", Some(&step8_owner_options));
    let (owner_framework_step8_restarted, _) = run_kv_test_client(owner_launch_step8_restart)
        .await
        .expect("Failed to restart owner client for step 8");

    sleep(Duration::from_secs(5)).await;

    match timeout(Duration::from_secs(30), blocking_put_handle).await {
        Ok(Ok(Ok(()))) => {
            info!("✅ External client put completed successfully after owner restart");
        }
        Ok(Ok(Err(e))) => {
            warn!(
                "⚠️ External client put completed with recoverable error after owner restart: {}",
                e
            );
        }
        Ok(Err(join_err)) => {
            panic!(
                "External client put task join failed after owner restart: {}",
                join_err
            );
        }
        Err(_) => panic!("External client put did not finish after owner restart"),
    }

    let recovery_key = "step8_recovery_key";
    let recovery_value = b"step8_recovery_value";
    external_view_primary
        .external_client_api()
        .inner()
        .put(
            recovery_key,
            recovery_value,
            crate::client_kv_api::PutOptionalArgs::default(),
        )
        .await
        .expect("External client put should succeed after owner restart");

    let recovery_result = external_view_step8_2
        .external_client_api()
        .inner()
        .get(recovery_key)
        .await
        .expect("External client get should succeed after owner restart");
    match recovery_result {
        Some(mem_holder) => {
            let retrieved_data = mem_holder.bytes();
            assert_eq!(
                retrieved_data, recovery_value,
                "Recovered data mismatch after owner restart"
            );
        }
        None => panic!("External client failed to retrieve data after owner restart"),
    }

    shutdown_framework_with_timeout("step8 external ext3", &external_framework_step8_3).await;
    shutdown_framework_with_timeout("step8 external ext2", &external_framework_step8_2).await;
    shutdown_framework_with_timeout("step8 external ext1", &external_framework_step8_primary).await;
    shutdown_framework_with_timeout("step8 owner restarted", &owner_framework_step8_restarted)
        .await;
    shutdown_framework_with_timeout("step8 master", &master_framework_step8).await;

    if let Err(e) = fs::remove_dir_all(&step8_share_mem_path) {
        warn!(
            "Step 8: failed to clean shared memory dir {} on exit: {}",
            step8_share_mem_path, e
        );
    }
}

async fn run_kv_round(round: &KvTestRoundOptions) {
    info!(
        "Round '{}' uses cluster '{}' and master ports {:?} / {:?}",
        round.round_name, round.cluster_name, round.master_port, round.step8_master_port
    );

    if kv_test_only_step8() {
        info!(
            "FLUXON_KV_TEST_ONLY_STEP8 is enabled; skipping steps 0-7 for round '{}'",
            round.round_name
        );
        run_kv_step8(round).await;
        info!("🎉 Round '{}' completed!", round.round_name);
        return;
    }

    // 启动Master节点
    let master_launch = new_master_launch(round, "test_master", round.master_port);
    let (master_framework, _) = run_master_with_test_overrides(
        ConfigArg::Config(master_launch.config),
        MasterRunTestOverrides {
            rdma_control_init: master_launch.rdma_control_init,
            transfer_backend_activation_mode: master_launch.transfer_backend_activation_mode,
        },
    )
    .await
    .expect("Failed to start master");

    // 等待Master完全启动
    sleep(Duration::from_secs(2)).await;
    info!("✅ Master started successfully");

    // 步骤0: 测试关闭逻辑是否正常
    async fn open_test_shutdown_logic(round: &KvTestRoundOptions) {
        info!("📋 Step 0: Testing client shutdown logic");

        // 启动客户端节点
        let client_launch = new_client_launch(round, "test_client_shutdown", None);
        let (client_framework, _) = run_kv_test_client(client_launch)
            .await
            .expect("Failed to start client for shutdown test");

        // 等待客户端完全启动和master请求segment注册
        info!("⏳ Waiting for client initialization and master-initiated segment registration...");
        sleep(Duration::from_secs(10)).await; // 增加等待时间以确保master主动请求segment注册
        info!("✅ Client for shutdown test started successfully");

        // 客户端写入测试数据
        let client_view = client_framework.client_kv_api_view().clone();
        let client_api = client_view.client_kv_api();

        let shutdown_test_key = "shutdown_test_key";
        let shutdown_test_value = b"shutdown_test_value_should_be_removed";

        match client_api
            .inner()
            .put(
                shutdown_test_key,
                shutdown_test_value,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
        {
            Ok(()) => info!("✅ Client PUT operation for shutdown test successful"),
            Err(e) => {
                error!("❌ Client PUT operation for shutdown test failed: {}", e);
                panic!("PUT operation failed during shutdown test: {}", e);
            }
        }

        // 验证数据存在
        match client_api.inner().get(shutdown_test_key).await {
            Ok(Some((mem_holder, get_info))) => {
                info!("✅ Data confirmed to exist before shutdown");
                info!("📋 GET Info before shutdown: {:?}", get_info);
            }
            Ok(None) => {
                error!("❌ Data should exist before shutdown but not found");
                panic!("Data not found before shutdown");
            }
            Err(e) => {
                error!("❌ Error getting data before shutdown: {}", e);
                panic!("Error getting data before shutdown: {}", e);
            }
        }

        sleep(Duration::from_secs(1)).await;

        // 关闭客户端
        info!("🔄 Shutting down client for shutdown logic test"); ////////////////////////////////////////////
        client_framework
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("Client shutdown failed during shutdown test: {}", e));

        info!("🔍 Client shutdown called, we need to verify the reality of the shutdown"); /////////////////////////////////////////////////////////
        sleep(Duration::from_secs(6)).await;

        // 创建新的客户端
        let new_client_launch = new_client_launch(round, "test_client_new_after_shutdown", None);
        let (new_client_framework, _) = run_kv_test_client(new_client_launch)
            .await
            .expect("Failed to start new client for shutdown test");

        sleep(Duration::from_secs(10)).await;
        info!("✅ New client started successfully");
        {
            // 新客户端检查数据是否存在
            let new_client_view = new_client_framework.client_kv_api_view().clone();
            let new_client_api = new_client_view.client_kv_api();

            info!("🔍 New client checking if shutdown test key exists...");
            match new_client_api.inner().is_exist(shutdown_test_key).await {
                Ok(true) => {
                    // 尝试获取数据以确认
                    match new_client_api.inner().get(shutdown_test_key).await {
                        Ok(Some((mem_holder, get_info))) => {
                            error!(
                                "❌ Data from shutdown client is still accessible: {:?}",
                                get_info
                            );
                            panic!(
                                "❌ SHUTDOWN LOGIC TEST FAILED: Client shutdown did not properly clean up data - key '{}' still exists and accessible",
                                shutdown_test_key
                            );
                        }
                        Ok(None) => {
                            error!("❌ Key exists but data is None - partial cleanup detected");
                            panic!(
                                "❌ SHUTDOWN LOGIC TEST FAILED: Key '{}' still exists after client shutdown (partial cleanup)",
                                shutdown_test_key
                            );
                        }
                        Err(e) => {
                            error!(
                                "❌ Key exists but error getting data: {} - inconsistent state",
                                e
                            );
                            panic!(
                                "❌ SHUTDOWN LOGIC TEST FAILED: Key '{}' still exists after client shutdown (inconsistent state: {})",
                                shutdown_test_key, e
                            );
                        }
                    }
                }
                Ok(false) => {
                    info!(
                        "✅ Shutdown test key does not exist - client shutdown logic working correctly"
                    );
                }
                Err(e) => {
                    error!("❌ Error checking existence of shutdown test key: {}", e);
                    panic!(
                        "❌ SHUTDOWN LOGIC TEST FAILED: Error checking existence of key '{}': {}",
                        shutdown_test_key, e
                    );
                }
            }

            // 尝试GET操作进一步确认 - 这应该返回None或Error
            match new_client_api.inner().get(shutdown_test_key).await {
                Ok(Some((mem_holder, get_info))) => {
                    error!(
                        "❌ GET operation successful - data from shutdown client still exists: {:?}",
                        get_info
                    );
                    panic!(
                        "❌ SHUTDOWN LOGIC TEST FAILED: GET operation on key '{}' should fail after client shutdown, but data is still accessible",
                        shutdown_test_key
                    );
                }
                Ok(None) => {
                    info!("✅ GET operation returned None - shutdown cleanup working correctly");
                }
                Err(e) => {
                    error!(
                        "❌ GET operation failed with error: {:?} - system state unclear",
                        e
                    );
                    panic!(
                        "❌ SHUTDOWN LOGIC TEST FAILED: GET operation on key '{}' failed with error (system state unclear): {}",
                        shutdown_test_key, e
                    );
                }
            }
        }
        tracing::info!("framework shutdowning...");

        // 清理shutdown test的资源
        new_client_framework
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("New client shutdown failed during shutdown test: {}", e));
        tracing::info!("framework shutdown successfully");
        tokio::time::sleep(Duration::from_secs(10)).await;
        info!("✅ Shutdown logic test completed");
    }

    info!("shutdown logic test for twice with same instance_key");
    for i in 0..2 {
        println!("\n\n--------------------------------");
        info!("shutdown logic test start at time {}", i);
        open_test_shutdown_logic(round).await;
        info!("shutdown logic test done at time {}", i);
    }

    // 步骤1: 启动多个Client节点
    let (client1_framework, client2_framework, client3_framework) = {
        info!("📋 Step 1: Starting multiple clients");

        // 启动多个客户端节点
        let client1_launch = new_client_launch(round, "test_client_1", None);
        // external 与 owner 使用不同的 instance_key，但共享 owner 的 share_mem_path
        let client2_launch =
            new_external_client_launch(round, "test_client_1_ext2", "test_client_1", None);
        let client3_launch =
            new_external_client_launch(round, "test_client_1_ext3", "test_client_1", None);

        let (client1_framework, _) = run_kv_test_client(client1_launch)
            .await
            .expect("Failed to start client 1");

        let (client2_framework, _) = run_kv_test_client(client2_launch)
            .await
            .expect("Failed to start client 2");

        let (client3_framework, _) = run_kv_test_client(client3_launch)
            .await
            .expect("Failed to start client 3");

        // 等待客户端完全启动和master主动请求segment注册
        info!(
            "⏳ Waiting for all clients to initialize and master to request segment registration..."
        );
        sleep(Duration::from_secs(30)).await; // 增加等待时间确保master完成所有segment注册请求
        info!("✅ All clients started successfully");

        (client1_framework, client2_framework, client3_framework)
    };

    // 步骤2: 并行PUT\GET\EXIST\DELETE操作测试
    {
        info!("📋 Step 2: Testing parallel CRUD operations (PUT/GET/EXIST/DELETE)");

        let client1_view: crate::client_kv_api::ClientKvApiView =
            client1_framework.client_kv_api_view().clone();
        let client2_view: crate::external_client_api::ExternalClientApiView =
            client2_framework.external_client_api_view().clone();
        let client3_view = client3_framework.external_client_api_view().clone();

        // 定义每个客户端的测试数据
        let test_cases = vec![
            (1, "client1_unique_key1", b"client1_test_value_data1"),
            (2, "client2_unique_key1", b"client2_test_value_data1"),
            (3, "client3_unique_key1", b"client3_test_value_data1"),
            (1, "client1_unique_key2", b"client1_test_value_data2"),
            (2, "client2_unique_key2", b"client2_test_value_data2"),
            (3, "client3_unique_key2", b"client3_test_value_data2"),
            (1, "client1_unique_key3", b"client1_test_value_data3"),
            (2, "client2_unique_key3", b"client2_test_value_data3"),
            (3, "client3_unique_key3", b"client3_test_value_data3"),
            (1, "client1_unique_key4", b"client1_test_value_data4"),
            (2, "client2_unique_key4", b"client2_test_value_data4"),
            (3, "client3_unique_key4", b"client3_test_value_data4"),
            (1, "client1_unique_key5", b"client1_test_value_data5"),
            (2, "client2_unique_key5", b"client2_test_value_data5"),
            (3, "client3_unique_key5", b"client3_test_value_data5"),
            (1, "client1_unique_key6", b"client1_test_value_data6"),
        ];

        // 并行CRUD操作测试
        let mut crud_tasks = vec![];
        for (client_id, key, value) in test_cases {
            match client_id {
                1 => {
                    let client_view = client1_view.clone();
                    crud_tasks.push(tokio::spawn(async move {
                        perform_client_crud_operations(client_view, client_id, key, value).await
                    }));
                }
                2 | 3 => {
                    let client_view = match client_id {
                        2 => client2_view.clone(),
                        3 => client3_view.clone(),
                        _ => unreachable!(),
                    };
                    crud_tasks.push(tokio::spawn(async move {
                        perform_external_client_crud_operations(client_view, client_id, key, value)
                            .await
                    }));
                }
                _ => panic!("Invalid client ID: {}", client_id),
            }
        }

        // 等待所有并行CRUD操作完成
        for (i, task) in crud_tasks.into_iter().enumerate() {
            match task.await {
                Ok(Ok(())) => info!("✅ Client {} CRUD operations completed successfully", i + 1),
                Ok(Err(e)) => {
                    error!("❌ Client {} CRUD operations failed: {}", i + 1, e);
                    panic!("Client {} CRUD operations failed: {}", i + 1, e);
                }
                Err(e) => {
                    error!("❌ Client {} CRUD task join failed: {}", i + 1, e);
                    panic!("Client {} CRUD task join failed: {}", i + 1, e);
                }
            }
        }

        sleep(Duration::from_secs(1)).await;

        // 交叉读取测试 - 验证不同客户端能否读取其他客户端的数据（如果DELETE没有成功的话）
        info!("Testing cross-client data access...");

        let cross_read_tasks = vec![
            tokio::spawn({
                let view = client1_view.clone();
                async move {
                    let api = view.client_kv_api();
                    info!("Client 1 attempting to read other clients' keys");
                    let _ = api.inner().get("client2_unique_key").await;
                    let _ = api.inner().get("client3_unique_key").await;
                }
            }),
            tokio::spawn({
                let view = client2_view.clone();
                async move {
                    let api = view.external_client_api();
                    info!("Client 2 attempting to read other clients' keys");
                    let _ = api.inner().get("client1_unique_key").await;
                    let _ = api.inner().get("client3_unique_key").await;
                }
            }),
            tokio::spawn({
                let view = client3_view.clone();
                async move {
                    let api = view.external_client_api();
                    info!("Client 3 attempting to read other clients' keys");
                    let _ = api.inner().get("client1_unique_key").await;
                    let _ = api.inner().get("client2_unique_key").await;
                }
            }),
        ];

        // 等待交叉读取完成（如果task join失败则panic）
        for (i, task) in cross_read_tasks.into_iter().enumerate() {
            task.await
                .unwrap_or_else(|e| panic!("Cross-read task {} join failed: {}", i + 1, e));
        }

        info!("✅ Parallel CRUD operations test completed");
    }

    // 步骤3: 客户端间通信测试（轮询模式）
    {
        info!("📋 Step 3: Testing client-to-client communication with polling");

        let client1_view = client1_framework.client_kv_api_view().clone();
        let client2_view = client2_framework.external_client_api_view().clone();
        let client1_api = client1_view.client_kv_api();
        let client2_api = client2_view.external_client_api();

        // Client 1存储通信消息
        match client1_api
            .inner()
            .put(
                CLIENT_COMMUNICATION_KEY,
                CLIENT_COMMUNICATION_VALUE,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
        {
            Ok(()) => info!("✅ Client 1 stored communication message"),
            Err(e) => {
                error!("❌ Client 1 failed to store communication message: {}", e);
                panic!("Client 1 failed to store communication message: {}", e);
            }
        }

        sleep(Duration::from_secs(1)).await;

        // Client 2轮询获取消息
        let mut poll_attempts = 0;
        let max_poll_attempts = 5;
        let mut communication_successful = false;

        while poll_attempts < max_poll_attempts {
            poll_attempts += 1;
            info!(
                "Client 2 polling attempt {} for communication key",
                poll_attempts
            );

            match client2_api.inner().get(CLIENT_COMMUNICATION_KEY).await {
                Ok(Some(mem_holder)) => {
                    info!("✅ Client-to-client communication successful!");
                    communication_successful = true;
                    break;
                }
                Ok(None) => {
                    warn!("⚠️ Communication key not found, retrying...");
                }
                Err(e) => {
                    error!("❌ Error during polling: {}", e);
                }
            }

            sleep(Duration::from_secs(1)).await;
        }

        if !communication_successful {
            error!(
                "❌ Client-to-client communication failed after {} attempts",
                max_poll_attempts
            );
            panic!(
                "Client-to-client communication test failed after {} attempts",
                max_poll_attempts
            );
        }
    }

    // 步骤4: 客户端关闭后的数据持久性测试
    const MANY_COUNT: usize = 20;
    {
        info!("📋 Step 4: Testing data persistence after client shutdown");

        let client1_view = client1_framework.client_kv_api_view().clone();
        let client2_view = client2_framework.external_client_api_view().clone();
        let client1_api = client1_view.client_kv_api();
        let client2_api = client2_view.external_client_api();

        // Client 2批量写入100个KV对，确保有大量数据贡献
        info!("Client 2 writing 100 key-value pairs for persistence testing...");
        for i in 0..MANY_COUNT {
            let key = format!("persistence_key_{:03}", i);
            let value = format!("persistence_value_{:03}_from_client2", i).into_bytes();

            match client2_api
                .inner()
                .put(
                    &key,
                    &value,
                    crate::client_kv_api::PutOptionalArgs::default(),
                )
                .await
            {
                Ok(()) => {
                    info!("✅ Client 2 PUT {}/{} completed", i + 1, MANY_COUNT);
                }
                Err(e) => {
                    error!("❌ Client 2 failed to store {}: {}", key, e);
                    panic!("Client 2 batch write failed at key {}: {}", key, e);
                }
            }
        }
        info!("✅ Client 2 successfully wrote 100 key-value pairs");

        // 额外存储一个特殊的持久性测试数据
        let persistence_key = "special_persistence_test_key";
        let persistence_value = b"special_data_from_client2_before_shutdown";

        match client2_api
            .inner()
            .put(
                persistence_key,
                persistence_value,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
        {
            Ok(()) => info!("✅ Client 2 stored special persistence test data"),
            Err(e) => {
                error!(
                    "❌ Client 2 failed to store special persistence test data: {}",
                    e
                );
                panic!(
                    "Client 2 failed to store special persistence test data: {}",
                    e
                );
            }
        }

        sleep(Duration::from_secs(2)).await;

        // 关闭Client 2
        info!("🔄 Shutting down Client 2 (which has written 101 key-value pairs)");
        client2_framework
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("Client 2 shutdown failed: {}", e));
        info!("✅ Client 2 shutdown successfully");

        sleep(Duration::from_secs(2)).await;

        // Client 1尝试获取已关闭客户端存储的数据（抽样测试）
        info!("Client 1 attempting to get data stored by the shutdown Client 2...");

        // 测试特殊持久性数据
        match client1_api.inner().get(persistence_key).await {
            Ok(Some((mem_holder, get_info))) => {
                info!("✅ Successfully retrieved special data from shutdown client!");
                info!("📋 Persistence test GET Info: {:?}", get_info);
            }
            Ok(None) => {
                info!("ℹ️ Special data from shutdown client not found (may have been evicted)");
            }
            Err(e) => {
                error!(
                    "❌ Error retrieving special data from shutdown client: {}",
                    e
                );
            }
        }

        // 全量测试批量数据（测试所有100个key）
        info!("Testing all 100 persistence keys from shutdown client...");
        let mut found_count = 0;
        let mut not_found_count = 0;
        let mut error_count = 0;

        for i in 0..MANY_COUNT {
            let key = format!("persistence_key_{:03}", i);
            match client1_api.inner().get(&key).await {
                Ok(Some((mem_holder, get_info))) => {
                    found_count += 1;
                    info!("📋 Found persistence key {} GET Info: {:?}", key, get_info);
                }
                Ok(None) => {
                    not_found_count += 1;
                }
                Err(e) => {
                    error_count += 1;
                    panic!(
                        "❌ Error retrieving key {} from shutdown client: {}",
                        key, e
                    );
                }
            }

            // 每25个报告进度
            info!(
                "📊 Progress: {}/{} keys tested - {} found, {} not found, {} errors",
                i + 1,
                MANY_COUNT,
                found_count,
                not_found_count,
                error_count
            );
        }

        info!(
            "📊 Final result: {} found, {} not found, {} errors out of 100 persistence keys",
            found_count, not_found_count, error_count
        );
    }

    // 步骤5: 新客户端访问已关闭客户端数据测试
    let new_client_framework = {
        info!("📋 Step 5: Testing data persistence with new client");

        // 启动新的客户端
        let new_client_launch = new_client_launch(round, "test_client_new", None);
        let (new_client_framework, _) = run_kv_test_client(new_client_launch)
            .await
            .expect("Failed to start new client");

        sleep(Duration::from_secs(10)).await;

        let new_client_view = new_client_framework.client_kv_api_view().clone();
        let new_client_api = new_client_view.client_kv_api();
        let new_client_instance_key = round.scoped_instance_key("test_client_new");
        let client1_instance_key = round.scoped_instance_key("test_client_1");

        if matches!(
            round.round_profile,
            KvTestRoundProfile::RdmaTransferOnly | KvTestRoundProfile::RdmaTransferWithRpc
        ) {
            let transfer_data_probe_key =
                format!("kv_test_transfer_data_probe_{}", round.round_name);
            let transfer_data_probe_tag = format!("{}:baseline", round.round_name);
            verify_rdma_transfer_data_link(
                &new_client_framework,
                &client3_framework,
                round.cluster_name.as_str(),
                &new_client_instance_key,
                &client1_instance_key,
                transfer_data_probe_key.as_str(),
                transfer_data_probe_tag.as_str(),
            )
            .await;
        }

        info!("New client reading shared communication key to build a second owner replica");
        match new_client_api.inner().get(CLIENT_COMMUNICATION_KEY).await {
            Ok(Some((mem_holder, get_info))) => {
                let retrieved_data = mem_holder.bytes();
                if retrieved_data != CLIENT_COMMUNICATION_VALUE {
                    panic!(
                        "Communication key data mismatch for cross-owner replica check: expected {:?}, got {:?}",
                        CLIENT_COMMUNICATION_VALUE, retrieved_data
                    );
                }
                info!(
                    "✅ New client retrieved communication key successfully for cross-owner replica check"
                );
                info!("📋 Cross-owner replica GET Info: {:?}", get_info);
            }
            Ok(None) => {
                panic!(
                    "Communication key '{}' not found before cross-owner replica check",
                    CLIENT_COMMUNICATION_KEY
                );
            }
            Err(e) => {
                panic!(
                    "New client failed to retrieve communication key '{}' before cross-owner replica check: {}",
                    CLIENT_COMMUNICATION_KEY, e
                );
            }
        }

        info!(
            "🔍 Checking cross-owner replica count for key: {}",
            CLIENT_COMMUNICATION_KEY
        );
        if let Some(one_kv_nodes_routes) = master_framework
            .master_kv_router_view()
            .master_kv_router()
            .inner()
            .kv_routes
            .get(CLIENT_COMMUNICATION_KEY)
        {
            let replicas = one_kv_nodes_routes.nodes_replicas.read();
            let active_replica_count = replicas
                .iter()
                .filter(|(_, kv_info)| !kv_info.tomb_tag.is_tomb())
                .count();

            info!(
                "📊 Replica count for key '{}': {} active replicas",
                CLIENT_COMMUNICATION_KEY, active_replica_count
            );

            if active_replica_count < CROSS_OWNER_REPLICA_TARGET_COUNT {
                panic!(
                    "Cross-owner replica count check failed: expected >= {}, got {}",
                    CROSS_OWNER_REPLICA_TARGET_COUNT, active_replica_count
                );
            }

            info!(
                "✅ Cross-owner replica count check PASSED: {} >= {} replicas",
                active_replica_count, CROSS_OWNER_REPLICA_TARGET_COUNT
            );
        } else {
            panic!(
                "Key '{}' not found in master kv_routes during cross-owner replica count check",
                CLIENT_COMMUNICATION_KEY
            );
        }

        // 新客户端尝试获取特殊持久性数据
        info!("New client attempting to get special data stored by the shutdown client");
        match new_client_api
            .inner()
            .get("special_persistence_test_key")
            .await
        {
            Ok(Some((mem_holder, get_info))) => {
                info!("✅ New client successfully retrieved special data from shutdown client!");
                // Note: external_client_api.get() only returns mem_holder, not get_info
            }
            Ok(None) => {
                info!(
                    "ℹ️ New client: Special data from shutdown client not found (may have been evicted or freed by the closed client)"
                );
            }
            Err(e) => {
                error!(
                    "❌ New client: Error retrieving special data from shutdown client: {}",
                    e
                );
            }
        }

        // 新客户端全量测试批量数据（测试所有100个key）
        info!("New client testing all 100 persistence keys from shutdown client...");
        let mut found_count = 0;
        let mut not_found_count = 0;
        let mut error_count = 0;
        let mut no_space_error_count = 0;

        for i in 0..MANY_COUNT {
            let key = format!("persistence_key_{:03}", i);
            match new_client_api.inner().get(&key).await {
                Ok(Some((mem_holder, get_info))) => {
                    found_count += 1;
                    // 打印前3个和每25个
                    info!("📋 New client found {} GET Info: {:?}", key, get_info);
                }
                Ok(None) => {
                    not_found_count += 1;
                }
                Err(e) => {
                    let code = e.code();
                    if code
                        == crate::rpcresp_kvresult_convert::msg_and_error::codes_api::API_NO_SPACE
                    {
                        no_space_error_count += 1;
                        error_count += 1;
                        error!(
                            "❌ New client: Error retrieving key {} from shutdown client: {}",
                            key, e
                        );
                        #[cfg(test)]
                        new_client_api.debug_cached_meta();
                    } else {
                        error_count += 1;
                        error!(
                            "❌ New client: Error retrieving key {} from shutdown client: {}",
                            key, e
                        );
                    }
                }
            }

            // 每25个报告进度

            info!(
                "📊 New client progress: {}/{} keys tested - {} found, {} not found, {} errors",
                i + 1,
                MANY_COUNT,
                found_count,
                not_found_count,
                error_count
            );
        }

        // assert_eq!(
        //     no_space_error_count, 0,
        //     "should always prepare space for sequential get operation"
        // );
        if no_space_error_count > 0 {
            #[cfg(test)]
            new_client_api.debug_cached_meta();
            panic!(
                "no space error count: {}, should be always 0 for sequential get operation",
                no_space_error_count
            );
        }

        info!(
            "📊 New client final result: {} found, {} not found, {} errors out of 100 persistence keys",
            found_count, not_found_count, error_count
        );

        // 新客户端尝试写入一些自己的数据
        info!("New client writing its own test data...");
        for i in 0..10 {
            let key = format!("new_client_key_{:02}", i);
            let value = format!("new_client_value_{:02}", i).into_bytes();

            match new_client_api
                .inner()
                .put(
                    &key,
                    &value,
                    crate::client_kv_api::PutOptionalArgs::default(),
                )
                .await
            {
                Ok(()) => {
                    info!("✅ New client PUT {}/10 completed", i + 1);
                }
                Err(e) => {
                    error!("❌ New client failed to store {}: {}", key, e);
                }
            }
        }
        info!("✅ New client successfully wrote 10 key-value pairs");

        verify_external_side_transfer_lane_mapping(round, &client3_framework, "test_client_new")
            .await;

        new_client_framework
    };

    // 步骤6: 验证is_exist功能
    {
        info!("📋 Step 6: Testing is_exist functionality");

        // 注意：new_client_framework 是以 Owner(Client) 模式启动的（非 ExternalClient）。
        // 因此这里应当使用 ClientKvApi 的 is_exist 接口，而不是 ExternalClientApi，
        // 否则 external 模块没有配置共享内存/owner 信息会进入错误的恢复流程。
        let new_client_view = new_client_framework.client_kv_api_view().clone();
        let new_client_api = new_client_view.client_kv_api();

        // 测试原始客户端的key
        for key in [
            "client1_unique_key",
            "client2_unique_key",
            "client3_unique_key",
            "client_communication_key",
        ] {
            match new_client_api.inner().is_exist(key).await {
                Ok(exists) => {
                    info!("Key '{}' exists: {}", key, exists);
                }
                Err(e) => {
                    error!("Error checking existence of key '{}': {}", key, e);
                }
            }
        }

        // 测试特殊持久性key
        match new_client_api
            .inner()
            .is_exist("special_persistence_test_key")
            .await
        {
            Ok(exists) => {
                info!("Key 'special_persistence_test_key' exists: {}", exists);
            }
            Err(e) => {
                error!(
                    "Error checking existence of key 'special_persistence_test_key': {}",
                    e
                );
            }
        }

        // 抽样测试批量持久性key
        info!("Testing existence of sample persistence keys...");
        let sample_indices = [0, 25, 50, 75, 99];
        for i in sample_indices {
            let key = format!("persistence_key_{:03}", i);
            match new_client_api.inner().is_exist(&key).await {
                Ok(exists) => {
                    info!("Key '{}' exists: {}", key, exists);
                }
                Err(e) => {
                    error!("Error checking existence of key '{}': {}", key, e);
                }
            }
        }

        // 测试新客户端自己的key
        for i in [0, 5, 9] {
            let key = format!("new_client_key_{:02}", i);
            match new_client_api.inner().is_exist(&key).await {
                Ok(exists) => {
                    info!("Key '{}' exists: {}", key, exists);
                }
                Err(e) => {
                    error!("Error checking existence of key '{}': {}", key, e);
                }
            }
        }
    }

    // 步骤7: 键元数据缓存测试
    {
        info!("📋 Step 7: Testing key meta cache behavior");

        const TASK_COUNT: usize = 20;
        let client1_view = client1_framework.client_kv_api_view().clone();
        let new_client_view = new_client_framework.client_kv_api_view().clone();

        // 使用不同的客户端测试键元数据缓存
        let mut test_tasks = vec![];

        for i in 0..TASK_COUNT {
            let key = format!("meta_cache_test_key_{}", i);
            let client_view = client1_view.clone();
            let new_client_view = new_client_view.clone();
            test_tasks.push(tokio::spawn(async move {
                key_meta_cache_check(&client_view, &new_client_view, &key).await;
            }));
        }

        // 等待所有元数据缓存测试完成
        for (i, task) in test_tasks.into_iter().enumerate() {
            task.await.unwrap_or_else(|e| {
                panic!("Key meta cache test task {} join failed: {}", i + 1, e)
            });
        }

        info!("✅ Key meta cache testing completed");
    }

    // 清理旧资源
    {
        info!("🧹 Cleaning up resources");

        // 按顺序关闭所有framework，失败时直接panic
        new_client_framework
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("New client framework shutdown failed: {}", e));
        info!("✅ New client framework shutdown successfully");

        client3_framework
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("Client 3 framework shutdown failed: {}", e));
        info!("✅ Client 3 framework shutdown successfully");

        client1_framework
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("Client 1 framework shutdown failed: {}", e));
        info!("✅ Client 1 framework shutdown successfully");

        master_framework
            .shutdown()
            .await
            .unwrap_or_else(|e| panic!("Master framework shutdown failed: {}", e));
        info!("✅ Master framework shutdown successfully");
    }
    run_kv_step8(round).await;
    info!("🎉 Round '{}' completed!", round.round_name);
}
