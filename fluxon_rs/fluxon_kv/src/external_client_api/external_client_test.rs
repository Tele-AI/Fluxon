use std::collections::HashMap;

use crate::cluster_manager::NodeID;
use crate::config::{
    ClientConfig, ContributeToClusterPoolSize, FluxonKvSpec, LargeFilePaths, MasterConfig,
    MonitoringConfig, ProtocolConfig, ProtocolType, ReplicaTaskPlacementConfig, TestSpecConfig,
    TransferEngineType,
};
use crate::master_kv_router::MasterKvRouterView;
use crate::master_seg_manager::msg_pack::{OwnerPlacementClass, SegmentAllocationAuthority};
use crate::{ConfigArg, run_client, run_master};
use limit_thirdparty::tokio::{self};
use std::time::{Duration, Instant};
use tracing::info;

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
    let log_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../log")
        .to_string_lossy()
        .to_string();
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
        log_dir,
        pprof_duration_seconds: None,
        master_ui: None,
        replica_task_placement: ReplicaTaskPlacementConfig::default(),
        replica_cache_capacity_ratio: crate::config::DEFAULT_REPLICA_CACHE_CAPACITY_RATIO,
        replica_writeback_tier1_capacity_ratio: None,
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
        share_mem_path: shm_path.to_string(),
        large_file_paths: LargeFilePaths {
            paths: vec![format!("{}_large", shm_path)],
        },
        ssd_storage: None,
        test_spec_config: TestSpecConfig::default(),
    }
}

fn new_zero_contribution_client_config(
    owner_instance_key: &str,
    cluster: &str,
    shm_path: &str,
) -> ClientConfig {
    // External instance_key MUST be different from owner.
    // External bootstrap shares the owner bundle root for mmap.file, shared.json, and peer metadata.
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
        share_mem_path: shm_path.to_string(),
        large_file_paths: LargeFilePaths { paths: Vec::new() },
        ssd_storage: None,
        test_spec_config: TestSpecConfig::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_external_client_basic_crud() {
    external_client_basic_crud(Some(OwnerPlacementClass::Inference), Some(0.5)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_external_client_basic_crud_default_owner() {
    external_client_basic_crud(None, None).await;
}

async fn external_client_basic_crud(
    placement: Option<OwnerPlacementClass>,
    hot_ratio: Option<f64>,
) {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    tokio::time::timeout(Duration::from_secs(120), async {
        fluxon_util::test_util::start_test_etcd().expect("start etcd for external client tests");
        // 测试仅设置日志级别，生产日志由 run_xxx 落盘
        unsafe {
            std::env::set_var("FLUXON_LOG", "info");
        }
        let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
            .expect("read etcd endpoint from build_config_ext.yml");
        let suffix = uuid::Uuid::new_v4();
        let cluster = format!("test_cluster_ext_{suffix}");
        let master_id = format!("ext_test_master_{suffix}");
        let owner_id = format!("ext_test_owner_{suffix}");
        let shm_path = format!(
            "{}/sharemem/kvcache_shared_memory_exttest_{suffix}",
            fluxon_util::test_util::test_workdir_base()
        );
        std::fs::create_dir_all(&shm_path).unwrap();

        // Start master
        let master_cfg = new_master_config(&master_id, None, &cluster, &etcd);
        let (master_fw, _) = run_master(ConfigArg::Config(master_cfg))
            .await
            .expect("start master");

        // Start owner client (provides shared memory)
        let mut owner_cfg = new_client_config(&owner_id, &cluster, &shm_path, &etcd);
        owner_cfg.owner_placement_class = placement;
        owner_cfg.replica_writeback_hot_capacity_ratio = hot_ratio;
        let (owner_fw, _) = run_client(ConfigArg::Config(owner_cfg))
            .await
            .expect("start owner client");
        let owner_node_id = owner_fw
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        // Owner-authoritative registration publishes geometry and authority while
        // deliberately leaving the master allocator map empty.
        wait_owner_allocation_authority(
            master_fw.master_kv_router_view(),
            &owner_node_id,
            Duration::from_secs(30),
        )
        .await;

        // Start external client bound to owner's shared memory
        let ext_cfg = new_zero_contribution_client_config(&owner_node_id, &cluster, &shm_path);
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
            wait_master_route(
                master_fw.master_kv_router_view(),
                key,
                Duration::from_secs(5),
            )
            .await;
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
            drop(got);
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

            // Native and external clients share the same owner allocation path.
            owner_api
                .inner()
                .put(key, value, crate::client_kv_api::PutOptionalArgs::new())
                .await
                .expect("native owner put");
            wait_master_route(
                master_fw.master_kv_router_view(),
                key,
                Duration::from_secs(5),
            )
            .await;
            let native_value = api
                .inner()
                .get(key)
                .await
                .expect("external get native put")
                .unwrap();
            assert_eq!(native_value.bytes(), value);
            drop(native_value);

            let rejected = owner_api
                .inner()
                .put(
                    key,
                    b"must_not_replace",
                    crate::client_kv_api::PutOptionalArgs(vec![
                        crate::client_kv_api::PutOptionalArg::RejectIfExists,
                    ]),
                )
                .await;
            assert!(matches!(
                rejected,
                Err(crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                    crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyAlreadyExists {
                        key: rejected_key,
                    }
                )) if rejected_key == key
            ));
            let unchanged = api
                .inner()
                .get(key)
                .await
                .expect("external get after rejected native put")
                .expect("rejected put must preserve the original value");
            assert_eq!(unchanged.bytes(), value);
            drop(unchanged);
            api.inner().delete(key).await.expect("delete native put");

            let invalid_lease = tokio::time::timeout(
                Duration::from_secs(15),
                owner_api.inner().put(
                    key,
                    value,
                    crate::client_kv_api::PutOptionalArgs(vec![
                        crate::client_kv_api::PutOptionalArg::LeaseId(u64::MAX),
                    ]),
                ),
            )
            .await
            .expect("a rejected lease must not retry publication forever")
            .expect_err("an unknown lease must reject native publication");
            assert!(matches!(
                invalid_lease,
                crate::rpcresp_kvresult_convert::msg_and_error::KvError::LeaseMgr(
                    crate::rpcresp_kvresult_convert::msg_and_error::LeaseMgrError::LeaseNotFound { .. }
                )
            ), "{invalid_lease}");
            assert!(
                owner_api.inner().get(key).await.expect("get after rejected lease").is_none(),
                "a rejected publication must release its local visibility"
            );
            owner_api.inner().put(
                key,
                value,
                crate::client_kv_api::PutOptionalArgs::new(),
            ).await.expect("put after rejected lease must release the key reservation");
            api.inner().delete(key).await.expect("delete retry after rejected lease");

            let overwrite_values = (0..8_u8).map(|byte| vec![byte; 64]).collect::<Vec<_>>();
            let overwrites = tokio::time::timeout(
                Duration::from_secs(30),
                futures::future::join_all(overwrite_values.iter().map(|value| {
                    owner_api.inner().put(key, value, crate::client_kv_api::PutOptionalArgs::new())
                })),
            ).await.expect("concurrent owner overwrites must finish");
            for result in overwrites {
                result.expect("default put permits replacement after the preceding writer finishes");
            }
            let last_value = api.inner().get(key).await.expect("get concurrent overwrite").unwrap();
            assert!(overwrite_values.iter().any(|value| value.as_slice() == last_value.bytes()));
            drop(last_value);
            api.inner().delete(key).await.expect("delete concurrent overwrite");

            let batch_specs = [
                [("native_order_a", 8_u64), ("native_order_b", 16_u64)],
                [("native_order_b", 8_u64), ("native_order_a", 16_u64)],
            ];
            tokio::time::timeout(Duration::from_secs(30), futures::future::join_all(
                batch_specs.into_iter().map(|specs| {
                    let inner = owner_api.inner();
                    async move {
                        let requests = specs.into_iter().map(|(key, len)| {
                            crate::master_kv_router::msg_pack::BatchPutStartItemReq {
                                key: key.to_string(),
                                len,
                                reject_if_inflight_same_key: false,
                                reject_if_exist_same_key: false,
                                make_replica_task: false,
                                preferred_sub_cluster: None,
                            }
                        }).collect();
                        let items = inner.owner_batch_put_start_reserved(requests, None)
                            .await.expect("reserve overlapping mixed-length batch");
                        for item in &items {
                            unsafe { std::ptr::write_bytes(item.src_addr as *mut u8, 0x5a, item.value_len as usize); }
                        }
                        for result in inner.owner_batch_put_commit_reserved(items, 1)
                            .await.expect("commit overlapping mixed-length batch") {
                            result.expect("each overlapping batch item must commit");
                        }
                    }
                }),
            )).await.expect("mixed-length batches must acquire keys in a consistent order");
            for batch_key in ["native_order_a", "native_order_b"] {
                let holder = api.inner().get(batch_key).await.expect("get mixed-length batch").unwrap();
                assert!(matches!(holder.bytes().len(), 8 | 16));
                assert!(holder.bytes().iter().all(|byte| *byte == 0x5a));
                drop(holder);
                api.inner().delete(batch_key).await.expect("delete mixed-length batch");
            }

            let reserved_payloads = [
                ("native_reserved_first", b"reserved_payload".as_slice()),
                ("native_reserved_second", b"second_payload__".as_slice()),
            ];
            let lease_id = owner_api.allocate_lease(120).await.expect("allocate native batch lease");
            let reserve_requests = reserved_payloads
                .iter()
                .map(
                    |(key, value)| crate::master_kv_router::msg_pack::BatchPutStartItemReq {
                        key: (*key).to_string(),
                        len: value.len() as u64,
                        reject_if_inflight_same_key: true,
                        reject_if_exist_same_key: true,
                        make_replica_task: false,
                        preferred_sub_cluster: None,
                    },
                )
                .collect::<Vec<_>>();
            let aborted = tokio::time::timeout(
                Duration::from_secs(15),
                owner_api
                    .inner()
                    .owner_batch_put_start_reserved(reserve_requests.clone(), Some(lease_id)),
            )
            .await
            .expect("native reservation must not wait for a master allocator")
            .expect("reserve native owner batch for abort");
            assert_eq!(aborted.len(), reserved_payloads.len());
            tokio::time::timeout(
                Duration::from_secs(15),
                owner_api.inner().owner_batch_put_abort_reserved(aborted),
            )
            .await
            .expect("native reserved batch abort timed out")
            .expect("abort native owner batch");
            for (key, _) in &reserved_payloads {
                assert!(
                    !owner_api
                        .inner()
                        .is_exist(key)
                        .await
                        .expect("aborted reservation must stay unpublished")
                );
            }

            // Reusing the same keys verifies that abort releases their Put fences.
            let reserved = tokio::time::timeout(
                Duration::from_secs(15),
                owner_api
                    .inner()
                    .owner_batch_put_start_reserved(reserve_requests, Some(lease_id)),
            )
            .await
            .expect("aborted native reservations must release the key fences")
            .expect("reserve native owner batch again");
            assert_eq!(reserved.len(), reserved_payloads.len());
            for (item, (key, value)) in reserved.iter().zip(&reserved_payloads) {
                assert_eq!(item.key, *key);
                assert_eq!(item.value_len, value.len() as u64);
                assert!(item.peer_node_id.is_none());
                assert_ne!(item.src_addr, 0);
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        value.as_ptr(),
                        item.src_addr as *mut u8,
                        value.len(),
                    );
                }
            }
            let committed = tokio::time::timeout(
                Duration::from_secs(15),
                owner_api
                    .inner()
                    .owner_batch_put_commit_reserved(reserved, 1),
            )
            .await
            .expect("native reserved batch commit timed out")
            .expect("commit native owner batch");
            assert_eq!(committed.len(), reserved_payloads.len());
            for result in committed {
                result.expect("each native reserved item must commit");
            }
            for (key, value) in &reserved_payloads {
                assert_eq!(
                    master_fw.master_kv_router_view().master_kv_router().inner()
                        .kv_routes.get(*key).expect("native commit must publish before returning").lease_id,
                    Some(lease_id),
                );
                let holder = api
                    .inner()
                    .get(key)
                    .await
                    .expect("external get native reserved value")
                    .expect("committed native reserved value must be visible");
                assert_eq!(holder.bytes(), *value);
                drop(holder);
                api.inner()
                    .delete(key)
                    .await
                    .expect("delete native reserved value");
            }
        }
        // The diagnostic short-circuit mode deliberately skips payload memcpy and
        // transfer. Validate only allocation, commit, route publication, logical
        // length, and reclaim; input-byte equality is not part of this mode.
        ext_fw.shutdown().await.expect("shutdown ext");
        let mut short_circuit_cfg =
            new_zero_contribution_client_config(&owner_node_id, &cluster, &shm_path);
        short_circuit_cfg
            .test_spec_config
            .short_circuit_put_payload_path = true;
        let (short_circuit_fw, _) = run_client(ConfigArg::Config(short_circuit_cfg))
            .await
            .expect("start short-circuit external client");
        {
            let short_circuit_view = short_circuit_fw.external_client_api_view();
            let short_circuit_api = short_circuit_view.external_client_api();
            let owner_view = owner_fw.client_kv_api_view();
            let owner_api = owner_view.client_kv_api();
            let key = "ext_client_short_circuit_key";
            let logical_payload = b"payload_is_intentionally_not_copied";

            short_circuit_api
                .inner()
                .put(
                    key,
                    logical_payload,
                    crate::client_kv_api::PutOptionalArgs::new(),
                )
                .await
                .expect("short-circuit external put");
            wait_master_route(
                master_fw.master_kv_router_view(),
                key,
                Duration::from_secs(5),
            )
            .await;
            assert!(
                owner_api
                    .inner()
                    .is_exist(key)
                    .await
                    .expect("owner sees short-circuit route")
            );
            let (holder, _) = owner_api
                .inner()
                .get(key)
                .await
                .expect("owner get after short-circuit put")
                .expect("short-circuit owner slot");
            assert_eq!(holder.bytes().len(), logical_payload.len());
            drop(holder);

            short_circuit_api
                .inner()
                .delete(key)
                .await
                .expect("delete short-circuit key");
            assert!(
                !owner_api
                    .inner()
                    .is_exist(key)
                    .await
                    .expect("owner no longer sees short-circuit key")
            );
        }
        short_circuit_fw
            .shutdown()
            .await
            .expect("shutdown short-circuit ext");

        // Shutdown remaining services in reverse order.
        owner_fw.shutdown().await.expect("shutdown owner");
        master_fw.shutdown().await.expect("shutdown master");

        info!("✅ external client basic crud test finished");
    })
    .await
    .expect("owner CRUD must finish without allocation or cleanup timeouts");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_native_put_admission_cancellation_releases_before_shutdown() {
    use crate::client_kv_api::external_api::HandlerForExternalClient;
    use crate::client_kv_api::msg_pack::{
        ExternalBatchPutStartItemReq, ExternalBatchPutStartReq, ExternalPutRevokeReq,
    };
    use crate::master_kv_router::msg_pack::BatchPutStartItemReq;
    use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError};

    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    tokio::time::timeout(Duration::from_secs(120), async {
        fluxon_util::test_util::start_test_etcd().expect("start etcd for admission cleanup");
        let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
            .expect("read test etcd endpoint");
        let suffix = uuid::Uuid::new_v4();
        let cluster = format!("native_admission_cleanup_{suffix}");
        let shm_path = format!(
            "{}/sharemem/native_admission_cleanup_{suffix}",
            fluxon_util::test_util::test_workdir_base()
        );
        std::fs::create_dir_all(&shm_path).expect("create cleanup test shared memory directory");
        let (master, _) = run_master(ConfigArg::Config(new_master_config(
            "cleanup_master",
            None,
            &cluster,
            &etcd,
        )))
        .await
        .expect("start cleanup master");
        let (owner, _) = run_client(ConfigArg::Config(new_client_config(
            "cleanup_owner",
            &cluster,
            &shm_path,
            &etcd,
        )))
        .await
        .expect("start cleanup owner");
        let owner_view = owner.client_kv_api_view();
        let owner_api = owner_view.client_kv_api();
        let inner = owner_api.inner();
        let owner_info = owner
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info();
        let master_view = master.master_kv_router_view();
        wait_owner_allocation_authority(
            master_view.clone(),
            &owner_info.id,
            Duration::from_secs(30),
        )
        .await;

        // A disconnected external writer can leave a live slot reservation.
        let mut external = owner_api
            .external_batch_put_start(ExternalBatchPutStartReq {
                items: vec![ExternalBatchPutStartItemReq {
                    key: "z_blocked".to_string(),
                    len: 4096,
                    reject_if_inflight_same_key: false,
                    reject_if_exist_same_key: false,
                    make_replica_task: false,
                    preferred_sub_cluster: None,
                    radix: None,
                }],
                atomic_group_lens: None,
                started_time: owner_info.node_start_time,
            })
            .await
            .expect("reserve external predecessor");
        assert_eq!(external.error_code, 0);
        let predecessor = external.items.pop().expect("external predecessor item");
        assert_eq!(predecessor.error_code, 0);
        let predecessor_id = predecessor.put_id.expect("external predecessor identity");

        let requests = vec![
            BatchPutStartItemReq {
                key: "a_partial".to_string(),
                len: 8192,
                reject_if_inflight_same_key: false,
                reject_if_exist_same_key: false,
                make_replica_task: false,
                preferred_sub_cluster: None,
            },
            BatchPutStartItemReq {
                key: "z_blocked".to_string(),
                len: 4096,
                reject_if_inflight_same_key: false,
                reject_if_exist_same_key: false,
                make_replica_task: false,
                preferred_sub_cluster: None,
            },
        ];
        let master_inner = master_view.master_kv_router().inner();
        for stopping in [false, true] {
            // Explicitly terminate this independent native reservation after
            // checking shutdown cleanup; its guard keeps dependencies live.
            let completion_barrier = if stopping {
                inner
                    .owner_batch_put_start_reserved(
                        vec![BatchPutStartItemReq {
                            key: "shutdown_barrier".to_string(),
                            len: 4096,
                            reject_if_inflight_same_key: false,
                            reject_if_exist_same_key: false,
                            make_replica_task: false,
                            preferred_sub_cluster: None,
                        }],
                        None,
                    )
                    .await
                    .expect("reserve explicit shutdown barrier")
            } else {
                Vec::new()
            };
            let baseline_slots = 1 + u64::from(stopping);
            let baseline_reservations = usize::from(stopping);
            let task_view = owner_view.clone();
            let task_requests = requests.clone();
            let caller = tokio::spawn(async move {
                task_view
                    .client_kv_api()
                    .inner()
                    .owner_batch_put_start_reserved(task_requests, None)
                    .await
            });
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if master_inner.prepared_put_key_reservations.len() == baseline_reservations + 2
                        && inner.owner_local_reserve_control_snapshot().allocated_bytes
                            == baseline_slots * 4096 + 8192
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("native batch must own its first group before waiting on the predecessor");

            let shutdown = if stopping {
                let shutdown_owner = owner.clone();
                let shutdown = tokio::spawn(async move { shutdown_owner.shutdown().await });
                let result = tokio::time::timeout(Duration::from_secs(10), caller)
                    .await
                    .expect("shutdown must wake blocked native admission")
                    .expect("native caller task");
                assert!(matches!(
                    result,
                    Err(KvError::Api(ApiError::SystemShutdown { .. }))
                ));
                assert!(
                    !shutdown.is_finished(),
                    "shutdown must wait for the explicit reservation cleanup"
                );
                Some(shutdown)
            } else {
                caller.abort();
                assert!(
                    caller
                        .await
                        .expect_err("caller must be cancelled")
                        .is_cancelled()
                );
                None
            };
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if master_inner.prepared_put_key_reservations.len() == baseline_reservations
                        && inner.owner_local_reserve_control_snapshot().allocated_bytes
                            == baseline_slots * 4096
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("cancelled admission must release local slots and master reservations");
            assert!(master_inner.key_activity.is_quiescent("a_partial"));
            assert!(master_inner.key_activity.is_quiescent("z_blocked"));

            if let Some(shutdown) = shutdown {
                let revoked = owner_api
                    .external_put_revoke(ExternalPutRevokeReq {
                        key: "z_blocked".to_string(),
                        put_id: Some(predecessor_id),
                        started_time: owner_info.node_start_time,
                    })
                    .await
                    .expect("release external predecessor during shutdown");
                assert_eq!(revoked.error_code, 0);
                inner
                    .owner_batch_put_abort_reserved(completion_barrier)
                    .await
                    .expect("cleanup remains admitted after shutdown intent");
                tokio::time::timeout(Duration::from_secs(30), shutdown)
                    .await
                    .expect("owner shutdown must finish after native cleanup")
                    .expect("owner shutdown task")
                    .expect("owner shutdown result");
                assert!(master_inner.prepared_put_key_reservations.is_empty());
                assert!(master_inner.key_activity.is_quiescent("shutdown_barrier"));
            }
        }
        master.shutdown().await.expect("shutdown cleanup master");
    })
    .await
    .expect("native cancellation and shutdown cleanup must be bounded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_default_owner_reclaims_capacity_across_native_and_external_puts() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    tokio::time::timeout(Duration::from_secs(180), async {
        fluxon_util::test_util::start_test_etcd().expect("start etcd for owner pressure test");
        let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
            .expect("read etcd endpoint from build_config_ext.yml");
        let suffix = uuid::Uuid::new_v4();
        let cluster = format!("test_cluster_default_owner_pressure_{suffix}");
        let shm_path = format!(
            "{}/sharemem/default_owner_pressure_{suffix}",
            fluxon_util::test_util::test_workdir_base()
        );
        std::fs::create_dir_all(&shm_path).expect("create owner pressure shared-memory directory");
        let (master_fw, _) = run_master(ConfigArg::Config(new_master_config(
            "owner_pressure_master",
            None,
            &cluster,
            &etcd,
        )))
        .await
        .expect("start pressure-test master");
        let mut owner_config =
            new_client_config("owner_pressure_owner", &cluster, &shm_path, &etcd);
        owner_config.owner_placement_class = None;
        owner_config.replica_writeback_hot_capacity_ratio = None;
        assert_eq!(
            owner_config.contribute_to_cluster_pool_size.dram,
            64 * 1024 * 1024
        );
        let (owner_fw, _) = run_client(ConfigArg::Config(owner_config))
            .await
            .expect("start default pressure-test owner");
        let owner_node_id = owner_fw
            .cluster_manager_view()
            .cluster_manager()
            .get_self_info()
            .id;
        wait_owner_allocation_authority(
            master_fw.master_kv_router_view(),
            &owner_node_id,
            Duration::from_secs(30),
        )
        .await;
        let (ext_fw, _) = run_client(ConfigArg::Config(new_zero_contribution_client_config(
            &owner_node_id,
            &cluster,
            &shm_path,
        )))
        .await
        .expect("start pressure-test external client");

        {
            let owner_view = owner_fw.client_kv_api_view();
            let owner_api = owner_view.client_kv_api();
            let ext_view = ext_fw.external_client_api_view();
            let external_api = ext_view.external_client_api();
            // Each round writes 80 MiB into a 64 MiB owner. Keep old keys so
            // progress requires capacity reclaim instead of explicit deletes.
            for round in 0..2 {
                for index in 0..80 {
                    let key = format!("owner_pressure_{round}_{index}");
                    let payload = vec![(round * 80 + index) as u8; 1024 * 1024];
                    tokio::time::timeout(Duration::from_secs(15), async {
                        let opts = crate::client_kv_api::PutOptionalArgs::new();
                        if round == 0 {
                            owner_api
                                .inner()
                                .put(&key, &payload, opts)
                                .await
                                .expect("native put must reclaim capacity");
                        } else {
                            external_api
                                .inner()
                                .put(&key, &payload, opts)
                                .await
                                .expect("external put must reclaim capacity");
                        }
                        let holder = external_api
                            .inner()
                            .get(&key)
                            .await
                            .expect("read newest value under capacity pressure")
                            .expect("newest value must remain readable");
                        assert_eq!(holder.bytes(), payload.as_slice());
                        drop(holder);
                    })
                    .await
                    .unwrap_or_else(|_| {
                        panic!("owner pressure round {round}, item {index} stalled")
                    });
                }
            }
        }

        ext_fw
            .shutdown()
            .await
            .expect("stop pressure-test external client");
        owner_fw.shutdown().await.expect("stop pressure-test owner");
        master_fw
            .shutdown()
            .await
            .expect("stop pressure-test master");
    })
    .await
    .expect("default owner must reclaim capacity across both write paths");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_external_remote_get_uses_bound_owner_slot_without_master_allocator() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    fluxon_util::test_util::start_test_etcd().expect("start etcd for remote external Get test");
    unsafe {
        std::env::set_var("FLUXON_LOG", "info");
    }
    let etcd = fluxon_util::dev_config::read_etcd_endpoint_from_build_config()
        .expect("read etcd endpoint from build_config_ext.yml");
    let cluster = "test_cluster_ext_remote_owner_target";
    let root = fluxon_util::test_util::test_workdir_base();
    let source_shm = format!("{root}/sharemem/ext_remote_get_source");
    let target_shm = format!("{root}/sharemem/ext_remote_get_target");
    std::fs::create_dir_all(&source_shm).expect("create source owner shared-memory root");
    std::fs::create_dir_all(&target_shm).expect("create target owner shared-memory root");

    let (master_fw, _) = run_master(ConfigArg::Config(new_master_config(
        "ext_remote_get_master",
        None,
        cluster,
        &etcd,
    )))
    .await
    .expect("start master");
    let (source_owner_fw, _) = run_client(ConfigArg::Config(new_client_config(
        "ext_remote_get_source_owner",
        cluster,
        &source_shm,
        &etcd,
    )))
    .await
    .expect("start source owner");
    let (target_owner_fw, _) = run_client(ConfigArg::Config(new_client_config(
        "ext_remote_get_target_owner",
        cluster,
        &target_shm,
        &etcd,
    )))
    .await
    .expect("start target owner");
    let source_owner_id = source_owner_fw
        .cluster_manager_view()
        .cluster_manager()
        .get_self_info()
        .id;
    let target_owner_id = target_owner_fw
        .cluster_manager_view()
        .cluster_manager()
        .get_self_info()
        .id;
    wait_owner_allocation_authority(
        master_fw.master_kv_router_view(),
        &source_owner_id,
        Duration::from_secs(30),
    )
    .await;
    wait_owner_allocation_authority(
        master_fw.master_kv_router_view(),
        &target_owner_id,
        Duration::from_secs(30),
    )
    .await;

    let (source_external_fw, _) = run_client(ConfigArg::Config(
        new_zero_contribution_client_config(&source_owner_id, cluster, &source_shm),
    ))
    .await
    .expect("start source-bound external");
    let (target_external_fw, _) = run_client(ConfigArg::Config(
        new_zero_contribution_client_config(&target_owner_id, cluster, &target_shm),
    ))
    .await
    .expect("start target-bound external");
    let target_external_id = target_external_fw
        .cluster_manager_view()
        .cluster_manager()
        .get_self_info()
        .id;
    let master_view = master_fw.master_kv_router_view();
    for node_id in [&source_owner_id, &target_owner_id, &target_external_id] {
        let node: NodeID = node_id.to_string().into();
        assert!(
            master_view
                .master_seg_manager()
                .get_node_allocators(&node)
                .is_empty(),
            "owner-authoritative/external requester must not expose a master allocator: {node_id}"
        );
    }

    let key = "external-remote-owner-target-without-master-allocator";
    let value = vec![0xa5_u8; 1024 * 1024 + 123];
    let source_external_view = source_external_fw.external_client_api_view();
    let source_external = source_external_view.external_client_api();
    source_external
        .inner()
        .put(
            key,
            &value,
            crate::client_kv_api::PutOptionalArgs(vec![
                crate::client_kv_api::PutOptionalArg::SkipMakeReplicaTask,
            ]),
        )
        .await
        .expect("put only on source owner");
    wait_master_route(master_view.clone(), key, Duration::from_secs(5)).await;
    {
        let route = master_view
            .master_kv_router()
            .inner()
            .kv_routes
            .get(key)
            .expect("source Put must publish one route");
        let replicas = route.node_replicas.read();
        assert!(replicas.contains_key(source_owner_id.as_str()));
        assert!(
            !replicas.contains_key(target_owner_id.as_str()),
            "SkipMakeReplicaTask must leave the target owner genuinely remote before Get"
        );
    }
    let target_owner_view = target_owner_fw.client_kv_api_view();
    let rejected = tokio::time::timeout(
        Duration::from_secs(15),
        target_owner_view.client_kv_api().inner().put(
            key,
            b"must not replace a remote value",
            crate::client_kv_api::PutOptionalArgs(vec![
                crate::client_kv_api::PutOptionalArg::RejectIfExists,
            ]),
        ),
    )
    .await
    .expect("global native rejection must complete");
    assert!(matches!(
        rejected,
        Err(
            crate::rpcresp_kvresult_convert::msg_and_error::KvError::Api(
                crate::rpcresp_kvresult_convert::msg_and_error::ApiError::KeyAlreadyExists { .. }
            )
        )
    ));
    assert!(
        target_owner_view
            .client_kv_api()
            .inner()
            .local_visible_mem_holder(key)
            .is_none(),
        "target owner must not have a local slot before the remote Get"
    );

    let target_external_view = target_external_fw.external_client_api_view();
    let got = target_external_view
        .external_client_api()
        .inner()
        .get(key)
        .await
        .expect("dram=0 external remote Get must use its bound owner target")
        .expect("remote key must exist");
    assert_eq!(got.bytes(), value.as_slice());
    let publish_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if target_owner_view
            .client_kv_api()
            .inner()
            .local_visible_mem_holder(key)
            .is_some()
        {
            break;
        }
        assert!(
            Instant::now() < publish_deadline,
            "remote Get must eventually publish the exact target-owner slot"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    drop(got);

    source_external
        .inner()
        .delete(key)
        .await
        .expect("delete remote Get regression key");
    target_external_fw
        .shutdown()
        .await
        .expect("shutdown target-bound external");
    source_external_fw
        .shutdown()
        .await
        .expect("shutdown source-bound external");
    target_owner_fw
        .shutdown()
        .await
        .expect("shutdown target owner");
    source_owner_fw
        .shutdown()
        .await
        .expect("shutdown source owner");
    master_fw.shutdown().await.expect("shutdown master");
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
    let shm_path = format!(
        "{}/sharemem/kvcache_shared_memory_ext_lifetime",
        fluxon_util::test_util::test_workdir_base()
    );
    std::fs::create_dir_all(&shm_path).unwrap();
    info!("[ELT-SETUP] cluster='{}', shm_path='{}'", cluster, shm_path);

    // Start master
    let master_cfg = new_master_config("ext_lt_master", None, cluster, &etcd);
    let (master_fw, _) = run_master(ConfigArg::Config(master_cfg))
        .await
        .expect("start master");
    info!("[ELT-SETUP] master started");

    // Start owner
    let owner_cfg = new_client_config("ext_lt_owner", cluster, &shm_path, &etcd);
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
    let ext_cfg = new_zero_contribution_client_config(&owner_node_id, cluster, &shm_path);
    let (ext_fw, _) = run_client(ConfigArg::Config(ext_cfg))
        .await
        .expect("start external client");
    info!("[ELT-SETUP] external started (bound to owner)");
    // Ensure owner authority is registered without creating a master allocator.
    wait_owner_allocation_authority(
        master_fw.master_kv_router_view(),
        &owner_node_id,
        Duration::from_secs(30),
    )
    .await;
    info!("[ELT-SETUP] master recorded owner allocation authority");

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
        wait_master_route(
            master_fw.master_kv_router_view(),
            key,
            Duration::from_secs(5),
        )
        .await;
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
            let ext_cfg2 = new_zero_contribution_client_config(&owner_node_id, cluster, &shm_path);
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
                &shm_path,
                &etcd,
            )))
            .await
            .expect("restart owner");

            // short wait before checking, member join and segment re-registration needs some time
            tokio::time::sleep(Duration::from_secs(3)).await;

            // Wait until the restarted owner re-registers its authority and geometry.
            wait_owner_allocation_authority(
                master_fw.master_kv_router_view(),
                &owner_node_id,
                Duration::from_secs(30),
            )
            .await;
            info!("[ELT-OWNER-ON] owner re-registered allocation authority");

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
            wait_master_route(
                master_fw.master_kv_router_view(),
                key2,
                Duration::from_secs(5),
            )
            .await;
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

async fn wait_master_route(master_view: MasterKvRouterView, key: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if master_view
            .master_kv_router()
            .inner()
            .kv_routes
            .contains_key(key)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timeout waiting authoritative master route for key={}",
            key
        );
        limit_thirdparty::tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// Helper: wait until the owner-authoritative segment contract is visible on master.
async fn wait_owner_allocation_authority(
    master_view: MasterKvRouterView,
    node_id: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        let node_cow: NodeID = node_id.to_string().into();
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
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timeout waiting owner allocation authority for {}: observed={:?}",
            node_id,
            authority
        );
        limit_thirdparty::tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
