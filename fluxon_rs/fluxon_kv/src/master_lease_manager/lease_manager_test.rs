use limit_thirdparty::tokio;
use limit_thirdparty::tokio::time::sleep;
use std::time::Duration;

use crate::kvcore_test_lib::{start_master_and_client, stop_master_and_client, wait_master_ready};

// ---------- Tests aligned with lease_test.canvas ----------

async fn grant_short_lease_for_test(master_fw: &crate::Framework, ttl_seconds: u64) -> u64 {
    master_fw
        .master_lease_manager_view()
        .master_lease_manager()
        .grant_lease_for_test(ttl_seconds)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test1_lease_expire_removes_keys() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    // 控制日志级别：生产日志由 run_master/run_client 落盘，测试仅需设置环境变量
    unsafe {
        std::env::set_var("FLUXON_LOG", "debug");
    }
    let (master_fw, client_fw) = start_master_and_client("lease_master_t1", "lease_client_t1").await;
    let client_view = client_fw.client_kv_api_view();
    wait_master_ready(&client_view).await;

    // Allocate a lease with a short TTL so we can observe expiry quickly.
    let lease_id = grant_short_lease_for_test(master_fw.as_ref(), 3).await;

    let keys: Vec<String> = (0..10).map(|i| format!("t1_key_{}", i)).collect();
    let value = vec![7u8; 128];
    for k in &keys {
        client_view
            .client_kv_api()
            .put(k, &value, Some(lease_id))
            .await
            .unwrap();
    }
    tracing::info!(
        "[test1-create-lease-put-10-keys] created lease and put keys; lease_id={:?}, keys={}",
        lease_id,
        keys.len()
    );

    // Within TTL, keys should exist
    tracing::info!("[test1-verify-present-during-ttl] verifying keys exist within TTL window");
    for _ in 0..3 {
        for k in &keys {
            assert!(
                client_view
                    .client_kv_api()
                    .inner()
                    .is_exist(k)
                    .await
                    .unwrap()
            );
        }
        sleep(Duration::from_millis(500)).await;
    }

    // After TTL+grace, keys should be removed by lease expiry (expected behavior)
    sleep(Duration::from_secs(10)).await;
    for k in &keys {
        let exist = client_view
            .client_kv_api()
            .inner()
            .is_exist(k)
            .await
            .unwrap();
        assert!(!exist, "key should be removed after lease expire: {}", k);
    }
    tracing::info!("[test1-verify-missing-after-ttl] verified keys removed after TTL expiration");

    stop_master_and_client(master_fw, client_fw).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test2_rebind_to_new_lease_preserves_until_new_expire() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    unsafe {
        std::env::set_var("FLUXON_LOG", "debug");
    }
    let (master_fw, client_fw) = start_master_and_client("lease_master_t2", "lease_client_t2").await;
    let client_view = client_fw.client_kv_api_view();
    wait_master_ready(&client_view).await;

    let l1 = grant_short_lease_for_test(master_fw.as_ref(), 2).await;
    let keys: Vec<String> = (0..10).map(|i| format!("t2_key_{}", i)).collect();
    let value_a = vec![3u8; 64];
    for k in &keys {
        client_view
            .client_kv_api()
            .put(k, &value_a, Some(l1))
            .await
            .unwrap();
    }
    tracing::info!(
        "[test2-create-lease-put-10-keys] created first lease and put keys; lease_id={:?}, keys={}",
        l1,
        keys.len()
    );

    // Rebind to a newer lease with longer TTL
    let l2 = grant_short_lease_for_test(master_fw.as_ref(), 4).await;
    let value_b = vec![9u8; 64];
    for k in &keys {
        client_view
            .client_kv_api()
            .put(k, &value_b, Some(l2))
            .await
            .unwrap();
    }
    tracing::info!(
        "[test2-rebind-to-new-lease-put-keys] re-bound keys to new lease; new_lease_id={:?}",
        l2
    );

    // After l1 expiry, keys should still exist due to l2
    sleep(Duration::from_secs(3)).await;
    for k in &keys {
        assert!(
            client_view
                .client_kv_api()
                .inner()
                .is_exist(k)
                .await
                .unwrap()
        );
    }
    tracing::info!(
        "[test2-verify-still-present-after-first-expire] keys still exist after first lease expired, held by second lease"
    );

    // After l2 expiry + grace, keys should be gone
    sleep(Duration::from_secs(3)).await;
    for k in &keys {
        let exist = client_view
            .client_kv_api()
            .inner()
            .is_exist(k)
            .await
            .unwrap();
        assert!(
            !exist,
            "key should be removed after second lease expire: {}",
            k
        );
    }
    tracing::info!(
        "[test2-verify-gone-after-second-expire] verified keys removed after second lease expiration"
    );

    stop_master_and_client(master_fw, client_fw).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test3_keepalive() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    unsafe {
        std::env::set_var("FLUXON_LOG", "debug");
    }
    let (master_fw, client_fw) = start_master_and_client("lease_master_t3", "lease_client_t3").await;
    let client_view = client_fw.client_kv_api_view();
    wait_master_ready(&client_view).await;

    let lease_id = grant_short_lease_for_test(master_fw.as_ref(), 2).await;

    let keys: Vec<String> = (0..10).map(|i| format!("t3_key_{}", i)).collect();
    let value = vec![1u8; 32];
    for k in &keys {
        client_view
            .client_kv_api()
            .put(k, &value, Some(lease_id))
            .await
            .unwrap();
    }
    tracing::info!(
        "[test3-create-lease-put-10-keys] created lease and put keys; lease_id={:?}, keys={}",
        lease_id,
        keys.len()
    );

    // Keepalive loop; keys should exist throughout
    tracing::info!(
        "[test3-keepalive-loop-verify-present] entering keepalive loop (5 iterations, ~0.5s sleep each)"
    );
    for i in 0..5 {
        client_view
            .client_kv_api()
            .keepalive_lease(lease_id)
            .await
            .unwrap();
        for k in &keys {
            assert!(
                client_view
                    .client_kv_api()
                    .inner()
                    .is_exist(k)
                    .await
                    .unwrap()
            );
        }
        if i % 2 == 0 {
            tracing::info!(
                "[test3-keepalive-loop-verify-present] keepalive iteration {}",
                i
            );
        }
        sleep(Duration::from_millis(500)).await;
    }

    // Stop keepalive, wait beyond TTL
    sleep(Duration::from_secs(3)).await;
    for k in &keys {
        assert!(
            !client_view
                .client_kv_api()
                .inner()
                .is_exist(k)
                .await
                .unwrap()
        );
    }
    tracing::info!(
        "[test3-stop-keepalive-wait-verify-missing] verified keys removed after stopping keepalive and waiting past TTL"
    );

    stop_master_and_client(master_fw, client_fw).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test4_delete_under_lease_then_get_fails() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    unsafe {
        std::env::set_var("FLUXON_LOG", "debug");
    }
    let (master_fw, client_fw) = start_master_and_client("lease_master_t4", "lease_client_t4").await;
    let client_view = client_fw.client_kv_api_view();
    wait_master_ready(&client_view).await;

    let lease_id = grant_short_lease_for_test(master_fw.as_ref(), 5).await;

    let keys: Vec<String> = (0..10).map(|i| format!("t4_key_{}", i)).collect();
    let value = vec![2u8; 64];
    for k in &keys {
        client_view
            .client_kv_api()
            .put(k, &value, Some(lease_id))
            .await
            .unwrap();
    }
    tracing::info!(
        "[test4-create-lease-put-10-keys] created lease and put keys; lease_id={:?}, keys={}",
        lease_id,
        keys.len()
    );

    // Now delete each key explicitly; subsequent get should fail
    for k in &keys {
        client_view.client_kv_api().inner().delete(k).await.unwrap();
    }
    sleep(Duration::from_secs(2)).await;
    for k in &keys {
        assert!(
            !client_view
                .client_kv_api()
                .inner()
                .is_exist(k)
                .await
                .unwrap()
        );
    }
    tracing::info!(
        "[test4-delete-keys-sleep-2s-verify-missing] deleted keys and verified not exist after delay"
    );

    stop_master_and_client(master_fw, client_fw).await;
}

// test5 requires a realistic eviction scenario and large payloads; keep as ignored by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test5_eviction_when_lease_consumes_space() {
    let _test_guard = crate::kvcore_test_lib::integration_test_lock().await;
    unsafe {
        std::env::set_var("FLUXON_LOG", "debug");
    }
    // For this canvas case, override client capacity to 100MB
    let (master_fw, client_fw) = crate::kvcore_test_lib::start_master_and_client_with_client_dram(
        "lease_master_t5",
        "lease_client_t5",
        1024 * 1024 * 100,
    )
    .await;
    let client_view = client_fw.client_kv_api_view();
    wait_master_ready(&client_view).await;
    sleep(Duration::from_secs(2)).await;
    // Put a few normal keys (~5MB each)
    let normal_value = vec![0xABu8; 5 * 1024 * 1024];
    let initial_normal_keys: Vec<String> = (0..3).map(|i| format!("t5_normal_{}", i)).collect();
    for k in &initial_normal_keys {
        client_view
            .client_kv_api()
            .put(k, &normal_value, None)
            .await
            .unwrap();
    }
    tracing::info!("[test5-put-normal-keys-5mb-each] put several normal keys (~5MB each), count=3");

    // Create lease and put a lot of kvs (~95MB aggregate)
    let lease_id = client_view
        .client_kv_api()
        .allocate_lease(120)
        .await
        .unwrap();
    // 给足 TTL，避免在大批量写入 + sleep 期间提前过期
    client_view
        .client_kv_api()
        .keepalive_lease(lease_id)
        .await
        .unwrap();
    let leased_value = vec![0xCDu8; 5 * 1024 * 1024];
    // Track only the keys that are actually stored successfully.
    let mut leased_success_keys: Vec<String> = Vec::new();
    for i in 0..19 {
        // 19*5MB ≈ 95MB
        let k = format!("t5_lease_{}", i);
        match client_view
            .client_kv_api()
            .put(&k, &leased_value, Some(lease_id))
            .await
        {
            Ok(()) => leased_success_keys.push(k),
            Err(e) => {
                // 容量不满足等情况允许失败；只校验成功放入的 key
                tracing::warn!("leased put error: {}", e);
            }
        }
        // 避免突发放入导致观察误差；给后台状态一些时间收敛
        sleep(Duration::from_secs(1)).await;
    }
    tracing::info!(
        "[test5-create-lease-put-95mb] created lease and put many leased keys (~95MB); lease_id={:?}, success_keys={}",
        lease_id,
        leased_success_keys.len()
    );

    // Canvas assertion: assert 放成功的所有 lease key 都存在（TTL 尚未到期）
    tracing::info!(
        "[test5-assert-all-leased-keys-exist] verifying all successfully stored leased keys still exist before TTL expiry"
    );
    assert!(
        leased_success_keys.len() >= 6,
        "expected at least 6 leased keys to proceed, got {}",
        leased_success_keys.len()
    );
    for k in &leased_success_keys {
        let exist = client_view
            .client_kv_api()
            .inner()
            .is_exist(k)
            .await
            .unwrap();
        assert!(exist, "leased key should still exist prior to TTL: {}", k);
    }

    // 新设计：继续随机放 lease key 和 非 lease key，应全部触发 NoSpace 报错
    // 因为 lease key 已经占满了可用空间
    // 再次续租，确保后续校验阶段仍在 TTL 内
    client_view
        .client_kv_api()
        .keepalive_lease(lease_id)
        .await
        .unwrap();
    tracing::info!(
        "[test5-verify-no-space-after-lease-full] subsequent puts should fail or be evicted shortly (both lease and non-lease)"
    );
    for i in 0..6u32 {
        // 少量尝试即可
        let nk = format!("t5_more_normal_{}", i);
        // 非 lease：允许先返回 Ok（moka 容量调整是异步的），随后校验一定时间后仍不可见
        let res = client_view
            .client_kv_api()
            .inner()
            .put(
                &nk,
                &leased_value,
                crate::client_kv_api::PutOptionalArgs::new(),
            )
            .await;
        match res {
            Ok(()) => {
                sleep(Duration::from_secs(2)).await;
                let still_exist = client_view
                    .client_kv_api()
                    .inner()
                    .is_exist(&nk)
                    .await
                    .unwrap();
                assert!(
                    !still_exist,
                    "expected eventual eviction for non-lease put when lease is full: {}",
                    nk
                );
            }
            Err(e) => {
                let es = e.to_string();
                assert!(
                    es.contains("NoSpace"),
                    "expected NoSpace error, got: {}",
                    es
                );
            }
        }
        // lease：应直接 NoSpace（物理空间不足）
        let lk = format!("t5_more_lease_{}", i);
        match client_view
            .client_kv_api()
            .put(&lk, &leased_value, Some(lease_id))
            .await
        {
            Ok(()) => panic!("expected NoSpace for lease put: {}", lk),
            Err(e) => {
                let es = e.to_string();
                assert!(
                    es.contains("NoSpace"),
                    "expected NoSpace error, got: {}",
                    es
                );
            }
        }
    }

    // Step-A: delete 20MB (4 leased keys), then ensure 3 non-lease puts still fail
    let delete_20mb: Vec<String> = leased_success_keys.iter().take(4).cloned().collect();
    assert_eq!(delete_20mb.len(), 4, "need 4 leased keys to delete 20MB");
    for k in &delete_20mb {
        client_view.client_kv_api().inner().delete(k).await.unwrap();
    }
    tracing::info!("[test5-delete-4-lease-keys-20mb] deleted 4 leased keys (20MB)");
    sleep(Duration::from_secs(3)).await;
    // 为保证与“删20MB仍NoSpace”的语义一致，这里先用相同 lease 回填直到出现 NoSpace
    tracing::info!("[test5-stepA-refill-lease] start refilling lease until NoSpace");
    let mut refill_ok = 0u32;
    let mut saw_nospace = false;
    for i in 0..20u32 {
        let rk = format!("t5_refill_lease_{}", i);
        match client_view
            .client_kv_api()
            .put(&rk, &leased_value, Some(lease_id))
            .await
        {
            Ok(()) => {
                refill_ok += 1;
                sleep(Duration::from_secs(1)).await;
            }
            Err(e) => {
                let es = e.to_string();
                if es.contains("NoSpace") {
                    saw_nospace = true;
                    tracing::info!(
                        "[test5-stepA-refill-lease] reached NoSpace after {} refill puts",
                        refill_ok
                    );
                    break;
                } else {
                    panic!("unexpected error when refilling lease: {}", es);
                }
            }
        }
    }
    assert!(
        saw_nospace,
        "expected NoSpace when refilling lease after deleting only 20MB, but did not hit NoSpace"
    );
    // 随后尝试非 lease 写入，期望全部 NoSpace
    for i in 0..3u32 {
        let k = format!("t5_post_del20_normal_{}", i);
        match client_view
            .client_kv_api()
            .inner()
            .put(
                &k,
                &leased_value,
                crate::client_kv_api::PutOptionalArgs::new(),
            )
            .await
        {
            Ok(()) => panic!(
                "expected NoSpace after deleting only 20MB, but non-lease put succeeded: {}",
                k
            ),
            Err(e) => assert!(
                e.to_string().contains("NoSpace"),
                "expected NoSpace, got: {}",
                e
            ),
        }
        sleep(Duration::from_secs(1)).await;
    }

    // Step-B: delete 15MB (3 leased keys), then ensure 3 non-lease puts succeed
    // Rationale: leave >= 5MB stable headroom for non-lease put considering
    // source+target buffering and allocator alignment.
    let delete_15mb: Vec<String> = leased_success_keys
        .iter()
        .skip(4)
        .take(3)
        .cloned()
        .collect();
    assert_eq!(
        delete_15mb.len(),
        3,
        "need 3 leased keys to delete additional 15MB"
    );
    for k in &delete_15mb {
        client_view.client_kv_api().inner().delete(k).await.unwrap();
    }
    tracing::info!("[test5-delete-3-lease-keys-15mb] deleted 3 more leased keys (15MB)");
    sleep(Duration::from_secs(3)).await;
    for i in 0..3u32 {
        // total deleted so far: 20MB + 15MB = 35MB
        let k = format!("t5_post_del35_normal_{}", i);
        // 仍然保持真实的非 lease 写入
        client_view
            .client_kv_api()
            .inner()
            .put(
                &k,
                &leased_value,
                crate::client_kv_api::PutOptionalArgs::new(),
            )
            .await
            .unwrap();
        sleep(Duration::from_secs(1)).await;
    }

    stop_master_and_client(master_fw, client_fw).await;
}
