use anyhow::Context;
use etcd_client as etcd;

use fluxon_util::etcd::{get_cluster_lease_id, DistributeIdAllocator};
use fluxon_util::lease_manager::{
    record_register_by as lm_record_register_by, LeaseBackendUid, LeaseManager, LeaseRegisterKind,
};

use crate::error::MpscError;
use crate::keys;
use crate::manager::{get_chan_meta_with_version, ChanGlobalMeta, ChanManager};

pub struct ChanCreateConfig {
    pub capacity: i64,
    pub ttl_seconds: i64,
    pub weight: Option<i64>,
    /// Optional override for the channel-level global lease id.
    ///
    /// When present, `create_mpsc_channel` will reuse this lease id
    /// for `/channels/meta/{chan_id}` 而不是通过
    /// `get_cluster_lease_id` 新建一个 cluster lease。生命周期
    /// 由外层（例如 MPMC）控制；本组件仅负责 keepalive，且
    /// 不会在 drop 时主动 revoke。
    pub override_global_lease_id: Option<i64>,
    /// Optional override for the per-channel member lease id.
    ///
    /// 当 MPSC 被 MPMC 作为子模块使用时，可以将 MPMC 的全局
    /// lease 复用为 member lease，使得整个子链路在同一个
    /// lease 生命周期内存在。此时本组件仅负责 keepalive，
    /// 且不会在 drop 时 revoke 该 lease。
    pub override_member_lease_id: Option<i64>,
    /// Optional kvclient payload lease id used for backend payload keys.
    /// 当存在时，create_mpsc_channel 会直接将该 id 写入 channel
    /// meta 的 `payload_lease_id` 字段，而不会在内部自行分配。
    pub override_payload_lease_id: Option<i64>,
}

/// Create a new MPSC channel id and write its metadata into etcd.
///
/// Semantics mirror the Python ChanManager.create_chan design:
/// - Allocate a unique chan_id using a global distributed ID allocator
///   with prefix "channels".
/// - Attach a meta lease to `/channels/meta/{chan_id}` whose TTL comes
///   from `ttl_seconds`.
/// - Initialize `/channels/{chan_id}/next_producer_id` to 0 under the
///   same lease.
///
/// This function is intentionally kept in a dedicated module to
/// highlight its role as the channel creation entry point.
///
/// Different from the initial version that only returned `chan_id`,
/// this now returns a fully-populated `ChanManager` so that the
/// caller can observe the core ids associated with the channel:
/// `chan_id`, `global_lease_id`, `global_long_lease_id` and the
/// `payload_lease_id` recorded in channel meta.
pub async fn create_mpsc_channel(
    lease_manager: &LeaseManager,
    etcd_endpoints: Vec<String>,
    kv_backend_uid: LeaseBackendUid,
    cfg: ChanCreateConfig,
    rt_handle: tokio::runtime::Handle,
) -> Result<ChanManager, MpscError> {
    // Build etcd client directly from provided endpoints; LeaseManager 仅负责
    // lease keepalive，不再对外暴露具体 client 实现。
    let mut client = etcd::Client::connect(etcd_endpoints.clone(), None)
        .await
        .map_err(|e| MpscError::Internal(format!("connect etcd failed: {}", e)))?;

    let etcd_backend_uid = LeaseBackendUid::etcd_from(etcd_endpoints.clone());

    // 1) Allocate a new channel id using the global distributed
    // allocator under prefix "channels".
    //
    // This top-level counter must remain monotonic across idle windows, so its
    // etcd key must not be tied to a short-lived lease. Old Python behavior
    // intentionally left this key unleased for the same reason.
    let allocator = DistributeIdAllocator::new_without_lease(client.clone(), "channels");
    let chan_id = allocator
        .allocate_id()
        .await
        .map_err(|e| MpscError::Internal(format!("allocate chan_id failed: {}", e)))?;

    tracing::debug!("allocated new mpsc chan_id={} ", chan_id);

    // 2) Acquire or reuse global lease for this channel's metadata;
    // this lease will be used to keep `/channels/meta/{chan_id}` and
    // `/channels/{chan_id}/next_producer_id` alive.
    let global_lease_id = match cfg.override_global_lease_id {
        Some(id) => id,
        None => get_cluster_lease_id(
            &mut client,
            &format!("channels/{}", chan_id),
            cfg.ttl_seconds,
        )
        .await
        .map_err(|e| MpscError::Internal(format!("get_cluster_lease_id(meta) failed: {}", e)))?,
    };

    // 3) Acquire per-channel long-lived global lease id for id
    // allocator; this follows the design in `mpsc.md` where every
    // global chan owns a long-lived lease to guard id allocation
    // against short-term stale reads.
    let global_long_lease_id = get_cluster_lease_id(
        &mut client,
        &format!("id_allocator/channels/{}", chan_id),
        30 * 60,
    )
    .await
    .map_err(|e| {
        MpscError::Internal(format!("get_cluster_lease_id(id_allocator) failed: {}", e))
    })?;

    // 3.1) If the caller overrides the per-channel member lease, validate that
    // lease before publishing any new channel metadata. Otherwise a dead
    // override lease would only be discovered after `/channels/meta/{chan_id}`
    // was already written, leaving a stale half-created channel behind.
    let prevalidated_override_member_lease = match cfg.override_member_lease_id {
        Some(override_member_lease_id) => Some(
            lease_manager
                .register_lease_for_keepalive(
                    etcd_backend_uid.clone(),
                    cfg.ttl_seconds,
                    override_member_lease_id as u64,
                    LeaseRegisterKind::Etcd {
                        revoke_on_drop: false,
                    },
                    rt_handle.clone(),
                )
                .await
                .map_err(|e| {
                    MpscError::Internal(format!(
                        "prevalidate override member lease failed for chan_id={} lease_id={}: {}",
                        chan_id, override_member_lease_id, e
                    ))
                })?,
        ),
        None => None,
    };

    // 3.2) Determine kvclient payload lease id for backend payload
    // keys. 覆写场景下沿用上层传入的 lease id；否则通过
    // kv_backend_uid + 统一 LeaseManager 抽象，在更高层注册的
    // allocator 闭包中分配新的 kvclient lease，并同时注册
    // kvclient keepalive。
    let payload_lease_id_i64 = match cfg.override_payload_lease_id {
        Some(id) => id,
        None => {
            let id_u64 = lease_manager
                .allocate_kvclient_lease(kv_backend_uid.clone(), cfg.ttl_seconds)
                .await
                .map_err(|e| {
                    MpscError::Internal(format!(
                        "allocate kvclient payload lease failed for chan_id={}: {}",
                        chan_id, e
                    ))
                })?;
            id_u64 as i64
        }
    };
    let payload_lease_handle = lease_manager
        .register_lease_for_keepalive(
            kv_backend_uid.clone(),
            cfg.ttl_seconds,
            payload_lease_id_i64 as u64,
            LeaseRegisterKind::KvClient {
                register_by: format!("mpsc_payload:chan_id={}", chan_id),
            },
            rt_handle.clone(),
        )
        .await
        .map_err(|e| {
            MpscError::Internal(format!(
                "register kvclient payload lease failed for chan_id={}: {}",
                chan_id, e
            ))
        })?;

    // 4) Write meta atomically. Persist both channel-level lease ids
    // into meta so that other components can reconstruct
    // `ChanManager` from etcd alone.
    let meta = ChanGlobalMeta {
        capacity: cfg.capacity,
        ttl_seconds: cfg.ttl_seconds,
        global_lease_id,
        global_long_lease_id,
        payload_lease_id: Some(payload_lease_id_i64),
    };
    let meta_bytes = serde_json::to_vec(&meta)
        .map_err(|e| MpscError::Internal(format!("serialize ChanMeta failed: {}", e)))?;

    let meta_key = keys::etcd_meta_key(chan_id);

    let compare = etcd::Compare::create_revision(meta_key.clone(), etcd::CompareOp::Equal, 0);
    let put_meta = etcd::TxnOp::put(
        meta_key.clone(),
        meta_bytes,
        Some(etcd::PutOptions::new().with_lease(global_lease_id)),
    );
    let txn = etcd::Txn::new()
        .when(vec![compare])
        .and_then(vec![put_meta]);
    let txn_res = client
        .txn(txn)
        .await
        .with_context(|| format!("failed to write meta for chan_id={}", chan_id))
        .map_err(|e| MpscError::Internal(e.to_string()))?;

    if !txn_res.succeeded() {
        return Err(MpscError::Internal(format!(
            "meta key already exists for chan_id={}",
            chan_id
        )));
    }

    // 构造带有三种 etcd lease handle + 一个 kvclient
    // payload lease 的 ChanManager：
    // - member_lease: per-channel 成员 lease，由当前 manager 实例持有
    // - global_lease: channel 元数据的 global lease（TTL = 用户 ttl）
    // - global_long_lease: id allocator 的长 TTL global lease
    // - payload_lease: kvclient payload lease（始终存在）
    let global_lease_handle = lease_manager
        .register_lease_for_keepalive(
            etcd_backend_uid.clone(),
            cfg.ttl_seconds,
            global_lease_id as u64,
            LeaseRegisterKind::Etcd {
                revoke_on_drop: false,
            },
            rt_handle.clone(),
        )
        .await
        .map_err(|e| {
            MpscError::Internal(format!(
                "register_lease(global) failed for chan_id={}: {}",
                chan_id, e
            ))
        })?;
    // Enrich register_by for easier attribution in diagnostics
    lm_record_register_by(
        global_lease_id as u64,
        format!("mpsc_global:chan_id={}", chan_id),
    );
    let global_long_lease_handle = lease_manager
        .register_lease_for_keepalive(
            etcd_backend_uid.clone(),
            30 * 60,
            global_long_lease_id as u64,
            LeaseRegisterKind::Etcd {
                revoke_on_drop: false,
            },
            rt_handle.clone(),
        )
        .await
        .map_err(|e| {
            MpscError::Internal(format!(
                "register_lease(global_long) failed for chan_id={}: {}",
                chan_id, e
            ))
        })?;
    lm_record_register_by(
        global_long_lease_id as u64,
        format!("mpsc_global_long:chan_id={}", chan_id),
    );

    let (member_lease_id_u64, member_lease_handle) = match prevalidated_override_member_lease {
        Some(handle) => (handle.id(), handle),
        None => {
            let member_resp = client
                .lease_grant(cfg.ttl_seconds, None)
                .await
                .map_err(|e| {
                    MpscError::Internal(format!(
                        "lease_grant(member_lease) failed for chan_id={} : {}",
                        chan_id, e
                    ))
                })?;
            let member_lease_id_u64 = member_resp.id() as u64;
            let member_lease_handle = lease_manager
                .register_lease_for_keepalive(
                    etcd_backend_uid.clone(),
                    cfg.ttl_seconds,
                    member_lease_id_u64,
                    LeaseRegisterKind::Etcd {
                        revoke_on_drop: true,
                    },
                    rt_handle.clone(),
                )
                .await
                .map_err(|e| {
                    MpscError::Internal(format!(
                        "register_lease(member) failed for chan_id={}: {}",
                        chan_id, e
                    ))
                })?;
            (member_lease_id_u64, member_lease_handle)
        }
    };
    lm_record_register_by(
        member_lease_id_u64,
        format!("mpsc_member:chan_id={}", chan_id),
    );

    let etcd_client = client;

    Ok(ChanManager {
        lease_manager: lease_manager.clone(),
        etcd_backend_uid,
        kv_backend_uid,
        chan_id,
        member_lease: member_lease_handle,
        global_lease: global_lease_handle,
        global_long_lease: global_long_lease_handle,
        payload_lease: payload_lease_handle,
        capacity: cfg.capacity,
        etcd_client,
    })
}

impl ChanManager {
    /// Create a manager for an existing channel id by loading its
    /// global metadata from etcd and reconstructing the two channel-
    /// level leases (global lease and global-long lease for
    /// id-allocator).
    pub async fn new_with_chan_id(
        lease_manager: LeaseManager,
        etcd_endpoints: Vec<String>,
        kv_backend_uid: LeaseBackendUid,
        chan_id: i64,
        rt_handle: tokio::runtime::Handle,
    ) -> anyhow::Result<Self> {
        fn is_hard_lease_failure(err: &anyhow::Error) -> bool {
            let msg = err.to_string().to_ascii_lowercase();
            msg.contains("lease not found")
                || msg.contains("requested lease not found")
                || msg.contains("expired")
        }

        // 1) 加载全局元数据，获取 TTL 以及（如有）记录下来的
        // global_lease_id / global_long_lease_id，同时记录下等
        // 待后续事务校验使用的 etcd 版本号。
        let etcd_backend_uid = LeaseBackendUid::etcd_from(etcd_endpoints.clone());
        let client = etcd::Client::connect(etcd_endpoints, None)
            .await
            .map_err(|e| anyhow::anyhow!("connect etcd failed: {}", e))?;
        let mut meta_client = client.clone();
        let meta_with_ver = get_chan_meta_with_version(&mut meta_client, chan_id)
            .await
            .map_err(|e| anyhow::anyhow!("get_chan_meta failed for chan_id={}: {}", chan_id, e))?;
        let meta = meta_with_ver.meta;
        let meta_version = meta_with_ver.version;
        let meta_key = keys::etcd_meta_key(chan_id);

        // 2) 恢复 global lease handle：直接使用元数据中记录的
        // global_lease_id。
        let global_lease = match lease_manager
            .register_lease_for_keepalive(
                etcd_backend_uid.clone(),
                meta.ttl_seconds,
                meta.global_lease_id as u64,
                LeaseRegisterKind::Etcd {
                    revoke_on_drop: false,
                },
                rt_handle.clone(),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                if is_hard_lease_failure(&e) {
                    let mut cleanup_client = client.clone();
                    cleanup_client
                        .delete(meta_key.clone(), None)
                        .await
                        .map_err(|de| anyhow::anyhow!(
                            "hard lease failure detected; failed to delete meta_key={} for chan_id={}: delete_err={}, cause={}",
                            meta_key, chan_id, de, e
                        ))?;
                }
                return Err(anyhow::anyhow!(
                    "register_lease(global) failed for chan_id={}: {}",
                    chan_id,
                    e
                ));
            }
        };
        lm_record_register_by(
            meta.global_lease_id as u64,
            format!("mpsc_global:chan_id={}", chan_id),
        );

        // 3) 恢复 id allocator 的长 TTL lease handle，同样直接
        // 使用元数据中的 global_long_lease_id。
        let global_long_lease = match lease_manager
            .register_lease_for_keepalive(
                etcd_backend_uid.clone(),
                30 * 60,
                meta.global_long_lease_id as u64,
                LeaseRegisterKind::Etcd {
                    revoke_on_drop: false,
                },
                rt_handle.clone(),
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                if is_hard_lease_failure(&e) {
                    let mut cleanup_client = client.clone();
                    cleanup_client
                        .delete(meta_key.clone(), None)
                        .await
                        .map_err(|de| anyhow::anyhow!(
                            "hard lease failure detected; failed to delete meta_key={} for chan_id={}: delete_err={}, cause={}",
                            meta_key, chan_id, de, e
                        ))?;
                }
                return Err(anyhow::anyhow!(
                    "register_lease(global_long) failed for chan_id={}: {}",
                    chan_id,
                    e
                ));
            }
        };
        lm_record_register_by(
            meta.global_long_lease_id as u64,
            format!("mpsc_global_long:chan_id={}", chan_id),
        );

        // 4) 创建一个 per-channel member lease，用于该 manager
        // 实例持有的生命周期控制。
        let mut client2 = client.clone();
        let member_resp = client2
            .lease_grant(meta.ttl_seconds, None)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "lease_grant(member_lease) failed for chan_id={}: {}",
                    chan_id,
                    e
                )
            })?;

        // 4.1) 在创建 member lease 之后，通过一次事务对
        // `/channels/meta/{chan_id}` 的版本进行校验，确保从
        // get_chan_meta 读取到 meta 到这里期间，该 meta 没有被
        // 删除或更新。如果校验失败，则直接返回错误，由上层
        // 决定是否重试。
        let compare =
            etcd::Compare::version(meta_key.clone(), etcd::CompareOp::Equal, meta_version);
        let get_op = etcd::TxnOp::get(meta_key.clone(), None);
        let txn = etcd::Txn::new().when(vec![compare]).and_then(vec![get_op]);
        let txn_res = client2.txn(txn).await.map_err(|e| {
            anyhow::anyhow!("meta version check failed for chan_id={}: {}", chan_id, e)
        })?;
        if !txn_res.succeeded() {
            anyhow::bail!(
                "channel meta changed or deleted during ChanManager bootstrap, chan_id={}",
                chan_id
            );
        }

        let member_lease = lease_manager
            .register_lease_for_keepalive(
                etcd_backend_uid.clone(),
                meta.ttl_seconds,
                member_resp.id() as u64,
                LeaseRegisterKind::Etcd {
                    revoke_on_drop: true,
                },
                rt_handle.clone(),
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "register_lease(member) failed for chan_id={}: {}",
                    chan_id,
                    e
                )
            })?;
        lm_record_register_by(
            member_resp.id() as u64,
            format!("mpsc_member:chan_id={}", chan_id),
        );

        // 5) 根据 meta 中记录的 payload_lease_id 恢复 kvclient
        // payload lease keepalive。当前实现要求该字段存在且为
        // 正整数；否则视为 meta 配置错误，直接返回错误，由上层
        // 决定是否进行修复或迁移。
        let payload_lease_id = match meta.payload_lease_id {
            Some(id) if id > 0 => id,
            Some(id) => {
                anyhow::bail!(
                    "invalid payload_lease_id={} in meta for chan_id={}",
                    id,
                    chan_id
                );
            }
            None => {
                anyhow::bail!(
                    "payload_lease_id missing in meta for chan_id={}, cannot construct ChanManager with payload lease",
                    chan_id
                );
            }
        };
        let payload_lease = lease_manager
            .register_lease_for_keepalive(
                kv_backend_uid.clone(),
                meta.ttl_seconds,
                payload_lease_id as u64,
                LeaseRegisterKind::KvClient {
                    register_by: format!("mpsc_payload:chan_id={}", chan_id),
                },
                rt_handle,
            )
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "register_lease(payload kvclient) failed for chan_id={}: {}",
                    chan_id,
                    e
                )
            })?;

        // First-keepalive probe moved into LeaseManager::register_lease_for_keepalive
        // for KvClient leases. If registration returned Ok above, an initial
        // keepalive has already been executed successfully and any error would
        // have been surfaced from there. Avoid duplicating logic here.

        Ok(ChanManager {
            lease_manager,
            etcd_backend_uid,
            kv_backend_uid,
            chan_id,
            member_lease,
            global_lease,
            global_long_lease,
            payload_lease,
            capacity: meta.capacity,
            etcd_client: client,
        })
    }
}
