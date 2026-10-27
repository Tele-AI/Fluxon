//! Memholder tests following canvas design at:
//! fluxon_doc/test_design/fluxon_kv_test/kv_backend_test/memholder_test.canvas
//!
//! This module provides async test routines (not `#[test]` directly) that can be
//! invoked from test binaries when the `test_bins` feature is enabled.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Weak;
use std::time::{Duration, Instant};

use crate::memholder::lifetime::MemholderManagerTrait;
use limit_thirdparty::tokio::time::sleep;

use crate::config::{
    ClientConfig, ContributeToClusterPoolSize, FluxonKvSpec, MasterConfig, MonitoringConfig,
    ProtocolConfig, ProtocolType, ReplicaTaskPlacementConfig, TestSpecConfig, TransferEngineType,
};
use crate::master_kv_router::MasterKvRouterView;
use crate::master_seg_manager::msg_pack::{OwnerPlacementClass, SegmentAllocationAuthority};
use crate::memholder::NodeHolderKey;

// Helpers: config builders mirroring kv_test.rs pattern
fn read_etcd() -> String {
    fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml")
}

fn new_master_config(
    instance_key: &str,
    port: Option<u16>,
    cluster: &str,
    etcd: &str,
) -> MasterConfig {
    let prometheus_base_url = fluxon_util::dev_config::load_tsdb_base_url()
        .expect("read prometheus_base_url from build_config_ext.yml (key: prom)");
    let prom_remote_write_url =
        fluxon_util::dev_config::read_prom_remote_write_url_from_build_config()
            .expect("read prom_remote_write_url from build_config_ext.yml");
    MasterConfig {
        instance_key: instance_key.to_string(),
        cluster_name: cluster.to_string(),
        port,
        etcd_endpoints: vec![etcd.to_string()],
        protocol: ProtocolConfig {
            protocol_type: ProtocolType::Tcp,
            rdma_device_names: None,
            tcp_thread_reactor: Default::default(),
        },
        transfer_engine: TransferEngineType::P2p,
        enable_transfer_rpc_fast_path: false,
        monitoring: Some(MonitoringConfig {
            prometheus_base_url,
            prom_remote_write_url: Some(vec![prom_remote_write_url]),
            otlp_log_api: None,
        }),
        network: None,
        log_dir: format!(
            "{}/memholder_master_logs",
            fluxon_util::test_util::test_workdir_base()
        ),
        pprof_duration_seconds: None,
        master_ui: None,
        replica_task_placement: ReplicaTaskPlacementConfig::default(),
        replica_cache_capacity_ratio: crate::config::DEFAULT_REPLICA_CACHE_CAPACITY_RATIO,
        replica_writeback_tier1_capacity_ratio: None,
        test_spec_config: TestSpecConfig::default(),
    }
}

fn new_client_config_with_size(
    instance_key: &str,
    dram_bytes: u64,
    cluster: &str,
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
            dram: dram_bytes,
            vram: HashMap::new(),
        },
        protocol: ProtocolConfig {
            protocol_type: ProtocolType::Tcp,
            rdma_device_names: None,
            tcp_thread_reactor: Default::default(),
        },
        pprof_duration_seconds: None,
        replica_writeback_hot_capacity_ratio: Some(0.5),
        owner_placement_class: Some(OwnerPlacementClass::Inference),
        redis_compat_listen_addr: None,
        fluxonkv_spec: FluxonKvSpec {
            etcd_addresses: vec![etcd.to_string()],
            cluster_name: cluster.to_string(),
            p2p_listen_port: None,
            transfer_engine: TransferEngineType::Closed,
            enable_transfer_rpc_fast_path: true,
            sub_cluster: None,
        },
        share_mem_path: format!(
            "{}/sharemem/{}",
            fluxon_util::test_util::test_workdir_base(),
            instance_key
        ),
        large_file_paths: crate::config::LargeFilePaths {
            paths: vec![format!(
                "{}/large/{}",
                fluxon_util::test_util::test_workdir_base(),
                instance_key
            )],
        },
        ssd_storage: None,
        test_spec_config: TestSpecConfig::default(),
    }
}

fn new_zero_contribution_client_config(
    external_instance_key: &str,
    owner_instance_key: &str,
    cluster: &str,
) -> ClientConfig {
    ClientConfig {
        cluster_name: cluster.to_string(),
        etcd_addresses_raw: Vec::new(),
        instance_key: external_instance_key.to_string(),
        contribute_to_cluster_pool_size: ContributeToClusterPoolSize {
            dram: 0,
            vram: HashMap::new(),
        },
        protocol: ProtocolConfig {
            protocol_type: ProtocolType::Rdma,
            rdma_device_names: None,
            tcp_thread_reactor: Default::default(),
        },
        pprof_duration_seconds: None,
        replica_writeback_hot_capacity_ratio: None,
        owner_placement_class: None,
        redis_compat_listen_addr: None,
        fluxonkv_spec: FluxonKvSpec {
            etcd_addresses: Vec::new(),
            cluster_name: cluster.to_string(),
            p2p_listen_port: None,
            transfer_engine: TransferEngineType::P2p,
            enable_transfer_rpc_fast_path: false,
            sub_cluster: None,
        },
        share_mem_path: format!(
            "{}/sharemem/{}",
            fluxon_util::test_util::test_workdir_base(),
            owner_instance_key
        ),
        large_file_paths: crate::config::LargeFilePaths { paths: Vec::new() },
        ssd_storage: None,
        test_spec_config: TestSpecConfig::default(),
    }
}

fn unique_cluster_name(prefix: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}_{}", prefix, ts)
}

// Helper: wait until the owner-authoritative segment contract is visible on master.
async fn wait_owner_allocation_authority(
    master_view: MasterKvRouterView,
    node_id: &str,
    timeout: Duration,
) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("timeout waiting owner allocation authority for {}", node_id);
        }
        let node_cow: crate::cluster_manager::NodeID = node_id.to_string().into();
        let authority = master_view
            .master_seg_manager()
            .get_node_allocation_authority(&node_cow);
        if authority == Some(SegmentAllocationAuthority::Owner) {
            let allocators = master_view
                .master_seg_manager()
                .get_node_allocators(&node_cow);
            assert!(
                allocators.is_empty(),
                "owner-authoritative node must not expose master allocators: node={} count={}",
                node_id,
                allocators.len()
            );
            return;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

// Helper: wait until an owner MemoryInfo generation no longer has a live slot reference.
async fn wait_weak_drop<T>(label: &str, weak: &Weak<T>, timeout: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("timeout waiting weak allocation drop: {label}");
        }
        if weak.upgrade().is_none() {
            return;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

/// Group: test block defer (memholder holds shutdown for ~10s)
#[cfg(test)]
pub mod test_memholder {
    use super::{
        new_client_config_with_size, new_master_config, new_zero_contribution_client_config,
        read_etcd, unique_cluster_name, wait_owner_allocation_authority, wait_weak_drop,
    };
    use crate::memholder::{MemholderManagerTrait, NodeHolderKey};
    use crate::{ConfigArg, run_client, run_master};
    use std::sync::Arc;
    use std::time::{Duration, Instant};
    use tokio::time::sleep;
    use tracing::{info, warn};
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    pub async fn test_memholder_lifetime_block_defer() {
        let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
        fluxon_util::test_util::start_test_etcd().expect("start etcd for memholder tests");
        // 控制日志级别；run_client 将落盘
        unsafe {
            std::env::set_var("FLUXON_LOG", "debug");
        }
        // 1) Start master + owner + external (use isolated cluster name)
        let etcd = read_etcd();
        let cluster = unique_cluster_name("test_cluster_memholder_block");
        info!(
            "[INIT] starting master+owner: cluster={} etcd={}",
            cluster, etcd
        );
        let (master, _) = run_master(ConfigArg::Config(new_master_config(
            "mh_master",
            None,
            &cluster,
            &etcd,
        )))
        .await
        .expect("start master");
        sleep(Duration::from_secs(2)).await;

        let owner_name = "mh_owner";
        let (owner, _) = run_client(ConfigArg::Config(new_client_config_with_size(
            owner_name,
            8 * 1024 * 1024,
            &cluster,
            &etcd,
        )))
        .await
        .expect("start owner");

        // Wait for owner authority and geometry without creating a master allocator.
        let owner_id = owner
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        wait_owner_allocation_authority(
            master.master_kv_router_view().clone(),
            &owner_id,
            Duration::from_secs(30),
        )
        .await;
        info!("[INIT] owner registered with master: owner_id={}", owner_id);

        let (external, _) = run_client(ConfigArg::Config(new_zero_contribution_client_config(
            "mh_external",
            owner_name,
            &cluster,
        )))
        .await
        .expect("start external");
        let external_id = external
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        info!(
            "[INIT] external started: external_id={} (shared owner_id={})",
            external_id, owner_id
        );

        // 2) Seed through the owner-authoritative external Put path, then
        // acquire one owner holder and one external holder for the same slot.
        let owner_view = owner.client_kv_api_view();
        let owner_api = owner_view.client_kv_api();
        let external_view = external.external_client_api_view();
        let external_api = external_view.external_client_api();

        let key = "mh_block_defer_key";
        let val = vec![1u8; 256 * 1024];

        external_api
            .inner()
            .put(key, &val, crate::client_kv_api::PutOptionalArgs::default())
            .await
            .expect("external seed put");
        let (owner_holder, _info) = owner_api
            .inner()
            .get(key)
            .await
            .expect("owner get")
            .expect("owner get none");
        let external_holder = external_api
            .inner()
            .get(key)
            .await
            .expect("external get")
            .expect("external get none");
        let owner_holder_id = owner_holder.holder_id();
        let external_holder_id = external_holder.holder_id;
        info!(
            "[GET] acquired memholders for key='{}': owner_holder_id={} external_holder_id={}",
            key, owner_holder_id, external_holder_id
        );

        // Spawn tasks that hold memholders for 10s
        let owner_holder_task = {
            let h = owner_holder.clone();
            tokio::spawn(async move {
                let _keep = h; // keep alive
                sleep(Duration::from_secs(10)).await;
            })
        };
        let _external_holder_task = {
            let h = external_holder.clone();
            tokio::spawn(async move {
                let _keep = h; // keep alive
                sleep(Duration::from_secs(10)).await;
            })
        };
        info!(
            "[HOLD] spawned tasks to hold memholders ~10s: owner_holder_id={} external_holder_id={}",
            owner_holder_id, external_holder_id
        );

        // Drop local clones to ensure only the tasks hold them
        drop(owner_holder);
        drop(external_holder);

        // 3) trigger shutdown and measure duration; owner shutdown should be >= 10s
        info!("[SHUTDOWN] initiating owner shutdown; expect >=10s due to held memholders");
        let t0 = Instant::now();
        owner
            .shutdown()
            .await
            .expect("owner shutdown should complete after holders drop");
        let elapsed = t0.elapsed();
        info!(
            "[SHUTDOWN] owner shutdown elapsed_secs={:.3}",
            elapsed.as_secs_f64()
        );
        assert!(
            elapsed.as_secs() >= 10,
            "owner shutdown expected to be >=10s, got {:?}",
            elapsed
        );

        // ensure task finished
        owner_holder_task.await.expect("owner holder task join");

        // external shutdown (does not block on holder by design)
        external.shutdown().await.expect("external shutdown");

        // master shutdown
        master.shutdown().await.expect("master shutdown");

        info!("[END] test_memholder_lifetime_block_defer OK");
    }

    /// Group: test memholder pin against eviction and post-shutdown behavior
    #[cfg(test)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    pub async fn test_memholder_pin() {
        let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
        fluxon_util::test_util::start_test_etcd().expect("start etcd for memholder tests");
        unsafe {
            std::env::set_var("FLUXON_LOG", "debug");
        }

        // 1) Start cluster with small owner segment (to trigger eviction easily)
        let etcd = read_etcd();
        let cluster = unique_cluster_name("test_cluster_memholder_pin");
        let (master, _) = run_master(ConfigArg::Config(new_master_config(
            "pin_master",
            None,
            &cluster,
            &etcd,
        )))
        .await
        .expect("start master");
        sleep(Duration::from_secs(2)).await;

        let owner_name = "pin_owner";
        let (owner, _) = run_client(ConfigArg::Config(new_client_config_with_size(
            owner_name,
            4 * 1024 * 1024,
            &cluster,
            &etcd,
        )))
        .await
        .expect("start owner");
        wait_owner_allocation_authority(
            master.master_kv_router_view().clone(),
            &owner
                .cluster_manager_view()
                .cluster_manager()
                .get_self_info()
                .id,
            Duration::from_secs(30),
        )
        .await;
        let (external, _) = run_client(ConfigArg::Config(new_zero_contribution_client_config(
            "pin_external",
            owner_name,
            &cluster,
        )))
        .await
        .expect("start external");

        let owner_api_view = owner.client_kv_api_view();

        let external_api_view = external.external_client_api_view();
        let external_api = external_api_view.external_client_api();

        let owner_id = owner
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        let external_id = external
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        info!(
            "[INIT_SMALL] cluster={} etcd={} owner_id={} external_id={} owner_dram_bytes={}B",
            cluster,
            etcd,
            owner_id,
            external_id,
            4 * 1024 * 1024
        );

        // 2) Seed every generation through the owner-authoritative external
        // Put path. The owner/external labels below describe the holder origin,
        // not a second allocation path.
        let kv_owner_hold = "kv_owner_hold";
        let kv_owner_release = "kv_owner_release";
        let kv_ext_hold = "kv_ext_hold";
        let kv_ext_release = "kv_ext_release";
        // 初始四个 put 也降为约总容量的 1/100（~40KiB for 4MiB）
        let v1 = vec![2u8; 40 * 1024];
        let v2 = vec![3u8; 40 * 1024];
        let v3 = vec![4u8; 40 * 1024];
        let v4 = vec![5u8; 40 * 1024];

        external_api
            .inner()
            .put(
                kv_owner_hold,
                &v1,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
            .expect("put owner_hold");
        external_api
            .inner()
            .put(
                kv_owner_release,
                &v2,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
            .expect("put owner_release");
        external_api
            .inner()
            .put(
                kv_ext_hold,
                &v3,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
            .expect("put ext_hold");
        external_api
            .inner()
            .put(
                kv_ext_release,
                &v4,
                crate::client_kv_api::PutOptionalArgs::default(),
            )
            .await
            .expect("put ext_release");

        // Owner get twice per key
        let (oh1, _) = owner_api_view
            .client_kv_api()
            .inner()
            .get(kv_owner_hold)
            .await
            .unwrap()
            .unwrap();
        let (oh2, _) = owner_api_view
            .client_kv_api()
            .inner()
            .get(kv_owner_hold)
            .await
            .unwrap()
            .unwrap();
        let owner_hold_holder_id = oh1.holder_id();
        let owner_hold_weak = Arc::downgrade(&oh1.memory_info);
        drop(oh2); // drop one; keep one

        let (or1, _) = owner_api_view
            .client_kv_api()
            .inner()
            .get(kv_owner_release)
            .await
            .unwrap()
            .unwrap();
        let (or2, _) = owner_api_view
            .client_kv_api()
            .inner()
            .get(kv_owner_release)
            .await
            .unwrap()
            .unwrap();
        let owner_release_holder_id = or1.holder_id();
        let owner_release_weak = Arc::downgrade(&or1.memory_info);
        drop(or1);
        drop(or2); // drop all for release key

        // External get twice per key
        let eh1 = external_api
            .inner()
            .get(kv_ext_hold)
            .await
            .unwrap()
            .unwrap();
        let eh2 = external_api
            .inner()
            .get(kv_ext_hold)
            .await
            .unwrap()
            .unwrap();
        let ext_hold_holder_id = eh1.holder_id;
        let ext_hold_memory_info = owner_api_view
            .client_kv_api()
            .inner()
            .external_get_holding
            .inner_map()
            .get(&NodeHolderKey::new(external_id.clone(), ext_hold_holder_id))
            .expect("capture ext_hold owner holding")
            .memory_info
            .clone();
        let ext_hold_weak = Arc::downgrade(&ext_hold_memory_info);
        drop(ext_hold_memory_info);
        drop(eh2);

        let er1 = external_api
            .inner()
            .get(kv_ext_release)
            .await
            .unwrap()
            .unwrap();
        let er2 = external_api
            .inner()
            .get(kv_ext_release)
            .await
            .unwrap()
            .unwrap();
        let ext_release_holder_ids = [er1.holder_id, er2.holder_id];
        let ext_release_holder_id = ext_release_holder_ids[0];
        let ext_release_memory_info = owner_api_view
            .client_kv_api()
            .inner()
            .external_get_holding
            .inner_map()
            .get(&NodeHolderKey::new(
                external_id.clone(),
                ext_release_holder_id,
            ))
            .expect("capture ext_release owner holding")
            .memory_info
            .clone();
        let ext_release_weak = Arc::downgrade(&ext_release_memory_info);
        drop(ext_release_memory_info);
        drop(er1);
        drop(er2); // drop all for release key

        let external_release_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let all_released = ext_release_holder_ids.iter().all(|holder_id| {
                !owner_api_view
                    .client_kv_api()
                    .inner()
                    .external_get_holding
                    .inner_map()
                    .contains_key(&NodeHolderKey::new(external_id.clone(), *holder_id))
            });
            if all_released {
                break;
            }
            assert!(
                Instant::now() < external_release_deadline,
                "timeout waiting external release holders to unpin owner slots"
            );
            sleep(Duration::from_millis(20)).await;
        }

        info!(
            "[PUT_GET] holders prepared: owner_hold_holder_id={} owner_release_holder_id={} ext_hold_holder_id={} ext_release_holder_id={}",
            owner_hold_holder_id,
            owner_release_holder_id,
            ext_hold_holder_id,
            ext_release_holder_id
        );

        // 3) Drive owner-local Moka and the sole OwnerSegmentAllocator past both
        // the local budget and physical capacity through that same external Put
        // path. Pinned generations must not become victims; released
        // generations should be selected and freed.
        let chunk_sz = 1024;
        let mut put_ok: u32 = 0;
        let mut put_nospace: u32 = 0;
        let mut put_err: u32 = 0;
        info!(
            "[LOOP1-EVICT] start owner-local eviction fill: chunk_sz={}B count=1500",
            chunk_sz
        );
        for i in 0..1500u32 {
            let k = format!("fill_key_{:03}", i);
            let v = vec![0xAu8; chunk_sz];
            match external_api
                .inner()
                .put(&k, &v, crate::client_kv_api::PutOptionalArgs::default())
                .await
            {
                Ok(_) => {
                    put_ok += 1;
                }
                Err(e) => {
                    let es = e.to_string();
                    if es.contains("NoSpace") {
                        put_nospace += 1;
                        warn!("fill put {} got NoSpace, ignore", k);
                    } else {
                        put_err += 1;
                        warn!("fill put {} error: {}", k, es);
                    }
                }
            }
            if i % 100 == 0 {
                info!(
                    "[LOOP1-EVICT] progress i={} ok={} nospace={} err={}",
                    i, put_ok, put_nospace, put_err
                );
            }
        }
        info!(
            "[LOOP1-EVICT] fill summary: ok={} nospace={} err={} (chunk_sz={}B)",
            put_ok, put_nospace, put_err, chunk_sz
        );
        assert!(
            put_ok >= 1024,
            "owner pressure did not admit enough fill keys: ok={} nospace={} err={}",
            put_ok,
            put_nospace,
            put_err
        );
        assert_eq!(put_err, 0, "owner pressure returned unexpected errors");

        // 4) asserts
        info!(
            "[LOOP1-ASSERT_WEAK] expect owner release weak dropped for key='{}'",
            kv_owner_release
        );
        wait_weak_drop(
            "owner_release",
            &owner_release_weak,
            Duration::from_secs(20),
        )
        .await;
        info!(
            "[LOOP1-ASSERT_WEAK] owner_release weak dropped (holder_id={})",
            owner_release_holder_id
        );

        info!(
            "[LOOP1-ASSERT_WEAK] expect external release weak dropped for key='{}'",
            kv_ext_release
        );
        wait_weak_drop("ext_release", &ext_release_weak, Duration::from_secs(20)).await;
        info!(
            "[LOOP1-ASSERT_WEAK] ext_release weak dropped (holder_id={})",
            ext_release_holder_id
        );

        // Hold keys: live owner/external holders keep the exact owner slot
        // generation pinned outside Moka victim selection.
        info!(
            "[LOOP1-ASSERT_WEAK] expect owner hold weak still pinned for key='{}'",
            kv_owner_hold
        );
        assert!(
            owner_hold_weak.upgrade().is_some(),
            "owner_hold weak should upgrade"
        );
        info!(
            "[LOOP1-ASSERT_WEAK] owner_hold weak still upgrades (holder_id={})",
            owner_hold_holder_id
        );

        info!(
            "[LOOP1-ASSERT_WEAK] expect external hold weak still pinned for key='{}'",
            kv_ext_hold
        );
        assert!(
            ext_hold_weak.upgrade().is_some(),
            "ext_hold weak should upgrade"
        );
        info!(
            "[LOOP1-ASSERT_WEAK] ext_hold weak still upgrades (holder_id={})",
            ext_hold_holder_id
        );

        // 5) loop x 2: repeat quick validation (without full setup)
        info!("[LOOP1-CHECK] quick recheck of pinned allocations (iterations=1)");
        for _ in 0..1 {
            // already did once; one more quick check
            assert!(owner_hold_weak.upgrade().is_some());
            assert!(ext_hold_weak.upgrade().is_some());
        }

        // 6) Drop the holders, then restart the owner. The old generation's
        // MemoryInfo references must disappear before the new generation is ready.
        info!("[LOOP2-OWNER_RESTART] dropping live holders before owner shutdown");
        drop(oh1);
        let ext_hold_key = NodeHolderKey::new(external_id.clone(), ext_hold_holder_id);
        drop(eh1);
        let external_hold_deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if !owner_api_view
                .client_kv_api()
                .inner()
                .external_get_holding
                .inner_map()
                .contains_key(&ext_hold_key)
            {
                break;
            }
            assert!(
                Instant::now() < external_hold_deadline,
                "timeout waiting external hold to unpin before owner shutdown"
            );
            sleep(Duration::from_millis(20)).await;
        }
        info!("[LOOP2-OWNER_RESTART] shutting down owner to release pinned allocations");
        owner.shutdown().await.expect("owner shutdown");
        // shutdown() closes modules and joins their tasks, but the Framework
        // still owns those modules until the old generation itself is dropped.
        drop(owner);
        drop(owner_api_view);
        // allow master to observe owner down
        sleep(Duration::from_secs(2)).await;
        // The old owner generation must own no live physical slot references.
        wait_weak_drop("owner_hold", &owner_hold_weak, Duration::from_secs(10)).await;
        wait_weak_drop("ext_hold", &ext_hold_weak, Duration::from_secs(10)).await;
        info!("[LOOP2-ASSERT_WEAK] pinned allocations dropped after owner shutdown");

        // Recreate owner
        info!("[LOOP2-OWNER_RESTART] restarting owner");
        let (owner_restarted, _) = run_client(ConfigArg::Config(new_client_config_with_size(
            owner_name,
            4 * 1024 * 1024,
            &cluster,
            &etcd,
        )))
        .await
        .expect("restart owner");
        let owner_restarted_id = owner_restarted
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        wait_owner_allocation_authority(
            master.master_kv_router_view().clone(),
            &owner_restarted_id,
            Duration::from_secs(30),
        )
        .await;
        info!(
            "[LOOP2-OWNER_RESTART] restarted owner registered with master: owner_id={}",
            owner_restarted_id
        );

        // shutdown remaining
        external.shutdown().await.expect("external shutdown");
        owner_restarted
            .shutdown()
            .await
            .expect("restarted owner shutdown");
        master.shutdown().await.expect("master shutdown");

        info!("[END] test_memholder_pin OK");
    }
}
