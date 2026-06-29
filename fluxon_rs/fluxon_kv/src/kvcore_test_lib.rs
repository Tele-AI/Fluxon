use etcd_client::{Client, DeleteOptions};
use limit_thirdparty::tokio::time::sleep;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use crate::client_kv_api::ClientKvApiView;
use crate::cluster_manager::app_logic_ext::ClusterManagerAppLogicExt;
use crate::config::{
    ClientConfig, ContributeToClusterPoolSize, FluxonKvSpec, MasterConfig, MonitoringConfig,
    ProtocolConfig, ProtocolType, TestSpecConfig, TransferEngineType,
};
use crate::{ConfigArg, Framework, run_client, run_master};

pub const LEASE_TEST_CLUSTER: &str = "lease_test_cluster";
static INTEGRATION_TEST_LOCK: OnceLock<limit_thirdparty::tokio::sync::AMutex<()>> = OnceLock::new();

// ---------- Lease-related test helpers (reused across tests) ----------

pub async fn integration_test_lock() -> limit_thirdparty::tokio::sync::AMutexGuard<'static, ()> {
    INTEGRATION_TEST_LOCK
        .get_or_init(|| limit_thirdparty::tokio::sync::AMutex::new(()))
        .lock()
        .await
}

async fn clean_etcd_members(cluster_name: &str) {
    fluxon_util::test_util::start_test_etcd().expect("start etcd for lease manager tests");
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    let mut client = Client::connect(vec![etcd], None)
        .await
        .expect("etcd connect for lease manager tests");
    let prefix = format!("/cluster/{}/members", cluster_name);
    let _ = client
        .delete(prefix, Some(DeleteOptions::new().with_prefix()))
        .await
        .expect("clean lease manager test cluster members");
}

fn test_cluster_name(master_key: &str) -> String {
    format!("{}_{}", LEASE_TEST_CLUSTER, master_key)
}

/// Use shared test workdir base from fluxon_util (merged into test_util)
use fluxon_util::test_util::test_workdir_base;

pub fn new_master_config(instance_key: &str, port: Option<u16>) -> MasterConfig {
    new_master_config_with_cluster(instance_key, port, LEASE_TEST_CLUSTER)
}

fn new_master_config_with_cluster(
    instance_key: &str,
    port: Option<u16>,
    cluster_name: &str,
) -> MasterConfig {
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    let prometheus_base_url = fluxon_util::dev_config::load_tsdb_base_url()
        .expect("read prometheus_base_url from build_config_ext.yml (key: prom)");
    let prom_remote_write_url =
        fluxon_util::dev_config::read_prom_remote_write_url_from_build_config()
            .expect("read prom_remote_write_url from build_config_ext.yml");
    // Put master logs into {test_dir}/master_log to separate from other test artifacts
    let base = test_workdir_base();
    // Ensure subdirs exist:
    //   - `<base>/sharemem`  (for client shared memory)
    //   - `<base>/master_log` (for master logs)
    let sharemem_dir = std::path::Path::new(base).join("sharemem");
    fs::create_dir_all(&sharemem_dir).expect("create test sharemem dir");
    let master_log_dir_path = std::path::Path::new(base).join("master_log");
    fs::create_dir_all(&master_log_dir_path).expect("create test master log dir");
    let log_dir = master_log_dir_path.to_string_lossy().to_string();
    let conf = MasterConfig {
        instance_key: instance_key.to_string(),
        cluster_name: cluster_name.to_string(),
        port,
        etcd_endpoints: vec![etcd.clone()],
        protocol: ProtocolConfig {
            protocol_type: ProtocolType::Tcp,
            rdma_device_names: None,
        },
        transfer_engine: TransferEngineType::P2p,
        enable_transfer_rpc_fast_path: false,
        monitoring: Some(MonitoringConfig {
            prometheus_base_url,
            prom_remote_write_url: Some(vec![prom_remote_write_url]),
            otlp_log_api: None,
        }),
        network: None,
        log_dir,
        pprof_duration_seconds: None,
        master_ui: None,
        test_spec_config: TestSpecConfig::default(),
    };
    println!("fluxonkv core created master config for test: {:?}", conf);
    conf
}

pub fn new_client_config(instance_key: &str) -> ClientConfig {
    // Default test memory contribution (160MB) for general tests
    new_client_config_with_dram(instance_key, 1024 * 1024 * 160)
}

/// Build a client config with a custom DRAM contribution size (bytes).
/// Only used by tests that need to tailor capacity (e.g., lease test5 canvas).
pub fn new_client_config_with_dram(instance_key: &str, dram_bytes: u64) -> ClientConfig {
    new_client_config_with_cluster_and_dram(instance_key, LEASE_TEST_CLUSTER, dram_bytes)
}

fn new_client_config_with_cluster_and_dram(
    instance_key: &str,
    cluster_name: &str,
    dram_bytes: u64,
) -> ClientConfig {
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    let etcd_raw = fluxon_util::dev_config::read_etcd_host_port_from_build_config()
        .expect("read raw etcd endpoint from build_config_ext.yml");
    // Shared memory path lives under the same test workdir base used by master logs
    let base = test_workdir_base();
    let share_mem_path = format!("{}/sharemem/{}", base, instance_key);
    let conf = ClientConfig {
        cluster_name: cluster_name.to_string(),
        etcd_addresses_raw: vec![etcd_raw],
        instance_key: instance_key.to_string(),
        contribute_to_cluster_pool_size: ContributeToClusterPoolSize {
            dram: dram_bytes,
            vram: HashMap::new(),
        },
        protocol: ProtocolConfig {
            protocol_type: ProtocolType::Tcp,
            rdma_device_names: None,
        },
        pprof_duration_seconds: None,
        redis_compat_listen_addr: None,
        fluxonkv_spec: FluxonKvSpec {
            etcd_addresses: vec![etcd],
            cluster_name: cluster_name.to_string(),
            p2p_listen_port: None,
            transfer_engine: TransferEngineType::Closed,
            enable_transfer_rpc_fast_path: true,
            sub_cluster: None,
        },
        share_mem_path,
        large_file_paths: crate::config::LargeFilePaths {
            paths: vec![format!("{}/large/{}", base, instance_key)],
        },
        test_spec_config: TestSpecConfig::default(),
    };
    println!("fluxonkv core created client config for test: {:?}", conf);
    conf
}

pub async fn start_master_and_client(
    master_key: &str,
    client_key: &str,
) -> (Arc<Framework>, Arc<Framework>) {
    let cluster_name = test_cluster_name(master_key);
    clean_etcd_members(&cluster_name).await;

    let (master_fw, _) = run_master(ConfigArg::Config(new_master_config_with_cluster(
        master_key,
        None,
        &cluster_name,
    )))
    // Start the lease cleanup task for the master
    //     {
    //             if let Err(e) =
    //             master_fw.master_lease_manager().start_cleanup_task().await {
    //                         tracing::warn!("Failed to start lease cleanup task: {:?}",
    //                         e);
    //                                     // Continue anyway
    //                                             }
    //                                                 }
    //
    .await
    .expect("start master");
    let (client_fw, _) = run_client(ConfigArg::Config(new_client_config_with_cluster_and_dram(
        client_key,
        &cluster_name,
        1024 * 1024 * 160,
    )))
    .await
    .expect("start client");

    // Give a short grace for modules to finish init and connect
    sleep(Duration::from_secs(3)).await;
    (master_fw, client_fw)
}

/// Start master and client with a custom client DRAM contribution size (bytes).
/// Use only for tests that need to override capacity.
pub async fn start_master_and_client_with_client_dram(
    master_key: &str,
    client_key: &str,
    client_dram_bytes: u64,
) -> (Arc<Framework>, Arc<Framework>) {
    let cluster_name = test_cluster_name(master_key);
    clean_etcd_members(&cluster_name).await;

    let (master_fw, _) = run_master(ConfigArg::Config(new_master_config_with_cluster(
        master_key,
        None,
        &cluster_name,
    )))
    .await
    .expect("start master");
    let (client_fw, _) = run_client(ConfigArg::Config(new_client_config_with_cluster_and_dram(
        client_key,
        &cluster_name,
        client_dram_bytes,
    )))
    .await
    .expect("start client");

    // Give a short grace for modules to finish init and connect
    sleep(Duration::from_secs(3)).await;
    (master_fw, client_fw)
}

pub async fn stop_master_and_client(master_fw: Arc<Framework>, client_fw: Arc<Framework>) {
    let _ = client_fw.shutdown().await;
    let _ = master_fw.shutdown().await;
}

pub async fn wait_master_ready(client_view: &ClientKvApiView) {
    // Ensure cluster has a known master; this waits internally
    let _ = client_view
        .cluster_manager()
        .find_or_wait_master_node()
        .await
        .unwrap();
}
