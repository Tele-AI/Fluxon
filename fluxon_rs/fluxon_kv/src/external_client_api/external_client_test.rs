use std::collections::HashMap;

use crate::cluster_manager::NodeID;
use crate::config::{
    ClientConfig, ContributeToClusterPoolSize, FluxonKvSpec, MasterConfig, MonitoringConfig,
    ProtocolConfig, ProtocolType, TestSpecConfig, TransferEngineType,
};
use crate::master_kv_router::MasterKvRouterView;
use crate::{ConfigArg, run_client, run_master};
use limit_thirdparty::tokio::{self};
use std::time::{Duration, Instant};
use tracing::info;

fn new_master_config(instance_key: &str, port: u16, cluster: &str, etcd: &str) -> MasterConfig {
    let prometheus_base_url = fluxon_util::dev_config::load_tsdb_base_url()
        .expect("read prometheus_base_url from build_config_ext.yml (key: prom)");
    let prom_remote_write_url =
        fluxon_util::dev_config::read_prom_remote_write_url_from_build_config()
            .expect("read prom_remote_write_url from build_config_ext.yml");
    let log_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../log")
        .to_string_lossy()
        .to_string();
    MasterConfig {
        instance_key: instance_key.to_string(),
        cluster_name: cluster.to_string(),
        port: Some(port),
        etcd_endpoints: vec![etcd.to_string()],
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
    }
}

fn new_client_config(
    instance_key: &str,
    cluster: &str,
    shm_path: &str,
    etcd: &str,
) -> ClientConfig {
    let etcd_raw = if let Some(rest) = etcd.strip_prefix("http://") {
        rest.to_string()
    } else if let Some(rest) = etcd.strip_prefix("https://") {
        rest.to_string()
    } else {
        etcd.to_string()
    };
    ClientConfig {
        cluster_name: cluster.to_string(),
        etcd_addresses_raw: vec![etcd_raw],
        instance_key: instance_key.to_string(),
        contribute_to_cluster_pool_size: ContributeToClusterPoolSize {
            dram: 1024 * 1024 * 64,
            vram: HashMap::new(),
        },
        protocol: ProtocolConfig {
            protocol_type: ProtocolType::Tcp,
            rdma_device_names: None,
        },
        pprof_duration_seconds: None,
        redis_compat_listen_addr: None,
        fluxonkv_spec: FluxonKvSpec {
            etcd_addresses: vec![etcd.to_string()],
            cluster_name: cluster.to_string(),
            p2p_listen_port: None,
            transfer_engine: TransferEngineType::Closed,
            enable_transfer_rpc_fast_path: true,
            sub_cluster: None,
        },
        shared_memory_path: shm_path.to_string(),
        shared_file_path: format!("{}_files", shm_path),
        test_spec_config: TestSpecConfig::default(),
    }
}

fn new_zero_contribution_client_config(
    owner_instance_key: &str,
    cluster: &str,
    shm_path: &str,
) -> ClientConfig {
    // External instance_key MUST be different from owner.
    // External bootstrap shares both owner bundle roots: shared_memory_path for mmap.file and
    // shared_file_path for shared.json / peer metadata.
    let unique_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let ext_instance_key = format!("{}__ext_{}", owner_instance_key, unique_suffix);

    ClientConfig {
        cluster_name: cluster.to_string(),
        etcd_addresses_raw: Vec::new(),
        instance_key: ext_instance_key,
        contribute_to_cluster_pool_size: ContributeToClusterPoolSize {
            dram: 0,
            vram: HashMap::new(),
        },
        protocol: ProtocolConfig {
            protocol_type: ProtocolType::Rdma,
            rdma_device_names: None,
        },
        pprof_duration_seconds: None,
        redis_compat_listen_addr: None,
        fluxonkv_spec: FluxonKvSpec {
            etcd_addresses: Vec::new(),
            cluster_name: cluster.to_string(),
            p2p_listen_port: None,
            transfer_engine: TransferEngineType::P2p,
            enable_transfer_rpc_fast_path: false,
            sub_cluster: None,
        },
        shared_memory_path: shm_path.to_string(),
        shared_file_path: format!("{}_files", shm_path),
        test_spec_config: TestSpecConfig::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_external_client_basic_crud() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    fluxon_util::test_util::start_test_etcd().expect("start etcd for external client tests");
    // 测试仅设置日志级别，生产日志由 run_xxx 落盘
    unsafe {
        std::env::set_var("FLUXON_LOG", "info");
    }
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    let cluster = "test_cluster_ext";
    let shm_path = "/tmp/kvcache_shared_memory_exttest";
    std::fs::create_dir_all(shm_path).unwrap();

    // Start master
    let master_cfg = new_master_config("ext_test_master", 50120, cluster, &etcd);
    let (master_fw, _) = run_master(ConfigArg::Config(master_cfg))
        .await
        .expect("start master");

    // Start owner client (provides shared memory)
    let owner_cfg = new_client_config("ext_test_owner", cluster, shm_path, &etcd);
    let (owner_fw, _) = run_client(ConfigArg::Config(owner_cfg))
        .await
        .expect("start owner client");
    let owner_node_id = owner_fw
        .cluster_manager_view()
        .cluster_manager()
        .get_self_info()
        .id;
    // Wait until master has registered allocators for owner node
    wait_node_allocators(
        master_fw.master_kv_router_view(),
        &owner_node_id,
        Duration::from_secs(30),
    )
    .await;

    // Start external client bound to owner's shared memory
    let ext_cfg = new_zero_contribution_client_config(&owner_node_id, cluster, shm_path);
    let (ext_fw, _) = run_client(ConfigArg::Config(ext_cfg))
        .await
        .expect("start external client");

    // First RPCs will wait for P2P ready; no extra sleep
    {
        // Hold views in variables to extend lifetimes for borrowed APIs
        let ext_view = ext_fw.external_client_api_view();
        let api = ext_view.external_client_api();
        let owner_view = owner_fw.client_kv_api_view();
        let owner_api = owner_view.client_kv_api();
        let key = "ext_client_test_key";
        let value = b"hello_external";

        // ensure not exists (owner validation)
        match owner_api.inner().is_exist(key).await {
            Ok(false) => {}
            Ok(true) => {
                // Clean up residue to ensure a clean test
                owner_api
                    .inner()
                    .delete(key)
                    .await
                    .expect("owner cleanup delete");
                assert!(
                    !owner_api
                        .inner()
                        .is_exist(key)
                        .await
                        .expect("owner exist after cleanup")
                );
            }
            Err(e) => panic!("owner is_exist pre-check failed: {}", e),
        }

        // put (external) and owner validates
        api.inner()
            .put(key, value, crate::client_kv_api::PutOptionalArgs::new())
            .await
            .expect("external put");
        // owner validate exists
        assert!(
            owner_api
                .inner()
                .is_exist(key)
                .await
                .expect("owner exist true after put")
        );
        // owner get and verify
        match owner_api
            .inner()
            .get(key)
            .await
            .expect("owner get after put")
        {
            Some((holder, _)) => assert_eq!(holder.bytes(), value),
            None => panic!("owner get returned None after external put"),
        }

        // get and verify (external)
        let got = api
            .inner()
            .get(key)
            .await
            .expect("external get")
            .expect("none");
        assert_eq!(got.bytes(), value);
        // owner re-validate data still accessible and correct
        match owner_api
            .inner()
            .get(key)
            .await
            .expect("owner get after external get")
        {
            Some((holder, _)) => assert_eq!(holder.bytes(), value),
            None => panic!("owner get returned None after external get"),
        }

        // is_exist true (external) and owner validation
        assert!(api.inner().is_exist(key).await.expect("exist true"));
        assert!(
            owner_api
                .inner()
                .is_exist(key)
                .await
                .expect("owner exist true after external get")
        );

        // delete (external)
        api.inner().delete(key).await.expect("external delete");
        // owner validation: not exist and get returns None
        assert!(
            !owner_api
                .inner()
                .is_exist(key)
                .await
                .expect("owner exist false after delete")
        );
        match owner_api
            .inner()
            .get(key)
            .await
            .expect("owner get after delete")
        {
            Some(_) => panic!("owner get should be None after delete"),
            None => {}
        }

        // is_exist false
        match api.inner().is_exist(key).await {
            Ok(false) => {}
            other => panic!("expected is_exist false after delete, got {:?}", other),
        }
    }
    // Shutdown in reverse order
    ext_fw.shutdown().await.expect("shutdown ext");
    owner_fw.shutdown().await.expect("shutdown owner");
    master_fw.shutdown().await.expect("shutdown master");

    info!("✅ external client basic crud test finished");
}

// #[tokio::test]
pub async fn test_external_client_lifetime() {
    unsafe {
        std::env::set_var("FLUXON_LOG", "info");
    }
    info!("[ELT-SETUP] init logger done");
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    info!("[ELT-SETUP] etcd endpoint loaded");
    let cluster = "test_cluster_ext_lifetime";
    let shm_path = "/tmp/kvcache_shared_memory_ext_lifetime";
    std::fs::create_dir_all(shm_path).unwrap();
    info!("[ELT-SETUP] cluster='{}', shm_path='{}'", cluster, shm_path);

    // Start master
    let master_cfg = new_master_config("ext_lt_master", 50130, cluster, &etcd);
    let (master_fw, _) = run_master(ConfigArg::Config(master_cfg))
        .await
        .expect("start master");
    info!("[ELT-SETUP] master started");

    // Start owner
    let owner_cfg = new_client_config("ext_lt_owner", cluster, shm_path, &etcd);
    let (owner_fw, _) = run_client(ConfigArg::Config(owner_cfg))
        .await
        .expect("start owner client");
    info!("[ELT-SETUP] owner started");

    let owner_view = owner_fw.client_kv_api_view();
    let owner_api = owner_view.client_kv_api();
    let owner_node_id = owner_fw
        .cluster_manager_view()
        .cluster_manager()
        .get_self_info()
        .id
        .clone();
    info!("[ELT-SETUP] owner node_id={}", owner_node_id);

    // Start external bound to owner
    let ext_cfg = new_zero_contribution_client_config(&owner_node_id, cluster, shm_path);
    let (ext_fw, _) = run_client(ConfigArg::Config(ext_cfg))
        .await
        .expect("start external client");
    info!("[ELT-SETUP] external started (bound to owner)");
    // Ensure owner allocators registered (no fixed sleep)
    wait_node_allocators(
        master_fw.master_kv_router_view(),
        &owner_node_id,
        Duration::from_secs(30),
    )
    .await;
    info!("[ELT-SETUP] master has allocators for owner node");

    {
        let ext_view = ext_fw.external_client_api_view();
        let api = ext_view.external_client_api();
        let key = "lt_key";
        let val = b"lt_value";
        info!("[ELT-OES-A] using key='{}'", key);

        // Ensure clean state via owner
        info!("[ELT-OES-A] ensure clean state via owner");
        if owner_api
            .inner()
            .is_exist(key)
            .await
            .expect("owner pre-check exist")
        {
            owner_api
                .inner()
                .delete(key)
                .await
                .expect("owner cleanup delete");
            assert!(
                !owner_api
                    .inner()
                    .is_exist(key)
                    .await
                    .expect("owner post-cleanup exist")
            );
            info!("[ELT-OES-A] cleaned residual key on owner");
        }

        // External PUT, owner validates
        info!("[ELT-OES-A] external PUT");
        api.inner()
            .put(key, val, crate::client_kv_api::PutOptionalArgs::new())
            .await
            .expect("external put");
        assert!(
            owner_api
                .inner()
                .is_exist(key)
                .await
                .expect("owner exist after put")
        );
        info!("[ELT-OES-A] owner validates existence after PUT");

        // External GET acquires holding; owner get_holding_len increases by 1
        let base_holding = owner_api.inner().get_holding_len();
        info!(
            "[ELT-OES-A] base holding={} before external GET",
            base_holding
        );
        {
            info!("[ELT-OES-A] external GET to acquire holding");
            let holder = api
                .inner()
                .get(key)
                .await
                .expect("external get")
                .expect("none");
            // Wait until owner bookkeeping observes the holding increase (no fixed sleep)
            let start = Instant::now();
            loop {
                if owner_api.inner().get_holding_len() >= base_holding + 1 {
                    break;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(5),
                    "timeout waiting holding increase"
                );
                limit_thirdparty::tokio::time::sleep(Duration::from_millis(20)).await;
            }
            let after_holding = owner_api.inner().get_holding_len();
            assert_eq!(
                after_holding,
                base_holding + 1,
                "owner holding should increase by 1"
            );
            info!("[ELT-OES-A] holding increased to {}", after_holding);

            // Drop holder -> should send delete_ack and owner holding returns to base
            // async spawn, so we sleep to make sure ack is processed
            tokio::time::sleep(Duration::from_millis(500)).await;
            info!("[ELT-OES-A] dropped holder and waited for delete_ack");
        }

        // drop(holder);
        let mut waited = 0u64;
        info!("[ELT-OES-A] wait holding back to base");
        loop {
            let cur = owner_api.inner().get_holding_len();
            if cur == base_holding {
                break;
            }
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(100)).await;
            waited += 1;
            assert!(
                waited < 200,
                "timeout waiting for holding cleanup after drop"
            );
        }
        info!("[ELT-OES-A] holding returned to base={}", base_holding);

        // Acquire again and then shutdown external: owner should cleanup on MemberLeft
        info!("[ELT-OES-A] external GET again to hold, then shutdown external");
        let _holder2 = api
            .inner()
            .get(key)
            .await
            .expect("external get 2")
            .expect("none");
        limit_thirdparty::tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(owner_api.inner().get_holding_len(), base_holding + 1);
        // Shutdown external
        info!("[ELT-OES-A] shutdown external");
        ext_fw.shutdown().await.expect("shutdown ext");
        drop(ext_fw);
        let mut waited2 = 0u64;
        info!("[ELT-OES-A] wait owner cleanup holdings after external shutdown");
        loop {
            if owner_api.inner().get_holding_len() == base_holding {
                break;
            }
            limit_thirdparty::tokio::time::sleep(Duration::from_millis(100)).await;
            waited2 += 1;
            assert!(
                waited2 < 300,
                "timeout waiting owner holding cleanup after external shutdown"
            );
        }
        info!("[ELT-OES-A] owner cleaned holdings back to base after external shutdown");

        // Owner restart recovery test with adjustable loop count
        let loop_n: usize = std::env::var("ELT_LOOP_N")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2);
        info!("[ELT-LOOP] configured iterations={}", loop_n);

        // Keep current owner handle as Option for the first iteration shutdown
        let mut owner_fw_opt = Some(owner_fw);

        for i in 0..loop_n {
            info!("[ELT-LOOP] iteration {}/{} start", i + 1, loop_n);

            // Ensure owner is down before starting external-only
            if let Some(owner_fw_cur) = owner_fw_opt.take() {
                info!("[ELT-OWNER-OFF] shutdown owner for restart test");
                owner_fw_cur.shutdown().await.expect("shutdown owner");
                drop(owner_fw_cur);
            } else {
                info!("[ELT-OWNER-OFF] owner already down");
            }

            // Start external client only (owner is down)
            info!("[ELT-EXT-ONLY] start a new external client for recovery path");
            let ext_cfg2 = new_zero_contribution_client_config(&owner_node_id, cluster, shm_path);
            let (ext_fw2, _) = run_client(ConfigArg::Config(ext_cfg2))
                .await
                .expect("start external 2");
            let ext_view2 = ext_fw2.external_client_api_view();
            let api2 = ext_view2.external_client_api();

            // ELT-BLOCK1: while owner is down, external GET should timeout
            info!("[ELT-BLOCK1] try external GET with timeout while owner down (expect timeout)");
            let timed = limit_thirdparty::tokio::time::timeout(Duration::from_secs(2), async {
                let _ = api2.inner().get("lt_key2").await; // should block until owner available
            })
            .await;
            match timed {
                Err(_elapsed) => info!("[ELT-BLOCK1] GET timed out as expected"),
                Ok(_) => {
                    panic!("[ELT-BLOCK1] expected external GET to timeout before owner restart")
                }
            }

            // Restart owner now so recovery can proceed
            info!("[ELT-OWNER-ON] restart owner now");
            let (owner_fw_new, _) = run_client(ConfigArg::Config(new_client_config(
                "ext_lt_owner",
                cluster,
                shm_path,
                &etcd,
            )))
            .await
            .expect("restart owner");

            // short wait before checking, member join and segment re-registration needs some time
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Wait until owner re-registers allocators on master
            wait_node_allocators(
                master_fw.master_kv_router_view(),
                &owner_node_id,
                Duration::from_secs(30),
            )
            .await;
            info!("[ELT-OWNER-ON] owner re-registered allocators");

            // After owner restarts but before data is re-put, a GET should respond within timeout
            // and return None (no timeout expected) per design [ELT-RESP]
            let key2 = "lt_key2";
            info!("[ELT-RESP] external GET after owner restart (expect None within timeout)");
            let resp = limit_thirdparty::tokio::time::timeout(Duration::from_secs(2), async {
                api2.inner().get(key2).await
            })
            .await
            .expect("external GET should not timeout after owner restart");
            match resp {
                Ok(None) => info!("[ELT-RESP] GET returned None as expected (pre-reput)"),
                Ok(Some(_)) => panic!("[ELT-RESP] unexpected Some before owner re-put the value"),
                Err(e) => panic!("[ELT-RESP] GET returned error after owner restart: {}", e),
            }

            // Now operations should succeed again: owner puts and external reads
            let owner_view2 = owner_fw_new.client_kv_api_view();
            let owner_api2 = owner_view2.client_kv_api();
            let val = b"lt_value";
            info!("[ELT-OES-B] owner PUT after restart");
            owner_api2
                .inner()
                .put(key2, val, crate::client_kv_api::PutOptionalArgs::new())
                .await
                .expect("owner put after restart");
            info!("[ELT-RESP] external GET after owner re-put (expect Some)");
            let got2 = api2
                .inner()
                .get(key2)
                .await
                .expect("external get after restart")
                .expect("none");
            assert_eq!(got2.bytes(), val);

            info!(
                "[ELT-END] cleanup and shutdown external + owner of iteration {}/{}",
                i + 1,
                loop_n
            );
            ext_fw2.shutdown().await.expect("shutdown ext2");
            owner_fw_new.shutdown().await.expect("shutdown owner2");
        }
    }
    info!("[ELT-END] shutdown master");
    master_fw.shutdown().await.expect("shutdown master");
}

// Helper: wait until master has non-tomb allocators for the given node
async fn wait_node_allocators(master_view: MasterKvRouterView, node_id: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let node_cow: NodeID = node_id.to_string().into();
        let allocs = master_view
            .master_seg_manager()
            .get_node_allocators(&node_cow);
        if !allocs.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timeout waiting node allocators for {}",
            node_id
        );
        limit_thirdparty::tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
