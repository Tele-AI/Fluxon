use crate::keys;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;

use anyhow::Context;
use etcd_client as etcd;
use fluxon_util::etcd::DistributeIdAllocator;
use fluxon_util::lease_manager::{GeneralLease, LeaseBackendUid, LeaseManager};

/// Initial produce offset when no messages have been produced.
pub const PRODUCE_OFFSET_BEGIN: i64 = -1;
/// Initial consume offset.
pub const CONSUME_OFFSET_BEGIN: i64 = 0;
/// Minimum supported TTL for MQ metadata/member leases.
pub const MIN_TTL_SECONDS: i64 = 90;

/// Channel type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChanType {
    Mpsc,
    Mpmc,
}

/// Channel role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChanRole {
    Producer,
    Consumer,
}

/// Channel-level global metadata persisted in etcd under
/// `/channels/meta/{chan_id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChanGlobalMeta {
    pub capacity: i64,
    pub ttl_seconds: i64,
    /// Channel-level global lease id for metadata (TTL =
    /// user-configured ttl_seconds).
    ///
    /// Historical meta written by earlier versions may store this
    /// under `meta_lease_id` or `cluster_lease_id`; keep aliases for
    /// backward compatibility.
    #[serde(default, alias = "meta_lease_id", alias = "cluster_lease_id")]
    pub global_lease_id: i64,
    /// Channel-level long TTL global lease id used for id allocation
    /// (typically 30 minutes).
    ///
    /// Historical meta may store this under `cluster_long_lease_id`.
    #[serde(default, alias = "cluster_long_lease_id")]
    pub global_long_lease_id: i64,
    /// Optional kvclient payload lease id used for backend payload keys.
    ///
    /// 语义约定（当前版本）：
    /// - 对于由 Rust `create_mpsc_channel` 或新版 Python MPMC 工厂
    ///   创建的 channel，必须保存为 `Some(id)` 且 `id > 0`；
    ///   `ChanManager::new_with_chan_id` 依赖该字段重建 payload
    ///   lease keepalive。
    /// - `None` 仅用于兼容早期未记录该字段的历史 meta；此类
    ///   channel 在新实现中会被视为配置错误，构造 ChanManager
    ///   时直接返回错误，由上层决定是否迁移或丢弃。
    ///
    /// Older meta may not carry this field; keep it optional with
    /// #[serde(default)] so deserialization stays backward compatible.
    #[serde(default)]
    pub payload_lease_id: Option<i64>,
}

/// Per-member (producer/consumer) metadata. This is kept separate
/// from `ChanGlobalMeta` to avoid overloading semantics. The concrete
/// persisted shape is still evolving; for now it mainly documents the
/// conceptual split between global channel config and member-level
/// config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChanMemberMeta {
    /// Unique member id within the channel (e.g. producer/consumer idx).
    pub member_id: String,
    /// Member role for this channel.
    pub role: ChanRole,
    /// Optional FluxonKV external client id (cluster member id) that owns this MQ member.
    ///
    /// This is used by monitoring/CLI tooling to group MQ producers/consumers by the
    /// KV external client identity, then by the bound owner.
    ///
    /// Keep it optional for backward compatibility with historical membership values.
    #[serde(default)]
    pub external_client_id: Option<String>,
    /// Optional kvclient sub-cluster tag for this member.
    ///
    /// This is only meaningful for `ChanRole::Consumer`: the binding consumer
    /// writes its kvclient sub-cluster into its etcd membership value so that
    /// producers can watch it and derive KV placement hints without involving
    /// additional control-plane logic.
    pub kvclient_sub_cluster: Option<String>,
}

/// Error type for mpsc channel operations.
#[derive(Debug, Error)]
pub enum MpscError {
    #[error("etcd error: {0}")]
    Etcd(#[from] etcd::Error),

    #[error("channel meta not found: chan_id={0}")]
    ChanMetaNotFound(i64),

    #[error("invalid channel meta for chan_id={chan_id}: {source}")]
    InvalidChanMeta {
        chan_id: i64,
        #[source]
        source: serde_json::Error,
    },

    #[error(
        "ttl_seconds too small for chan_id={chan_id}: {ttl_seconds} (minimum {MIN_TTL_SECONDS})"
    )]
    InvalidTtl { chan_id: i64, ttl_seconds: i64 },

    #[error("invalid UTF-8 in etcd value for chan_id={chan_id}")]
    InvalidUtf8 { chan_id: i64 },
}

/// Helper result for channel meta that also carries the etcd version
/// of the meta key. 版本用于在后续步骤中做事务校验，确保在
/// 使用 meta 构造 ChanManager 期间，该 meta 没有被删除或修改。
pub struct ChanMetaWithVersion {
    pub meta: ChanGlobalMeta,
    pub version: i64,
}

/// Get channel meta and its etcd version for the given channel id.
///
/// 这是一个无对象的辅助函数，仅依赖 etcd client 和 chan_id，
/// 不再挂在 ChanManager 上，避免为了读取 meta 专门构造一个
/// 临时的 ChanManager 实例。
pub async fn get_chan_meta_with_version(
    client: &mut etcd::Client,
    chan_id: i64,
) -> Result<ChanMetaWithVersion, MpscError> {
    let key = keys::etcd_meta_key(chan_id);
    let resp = client.get(key, None).await?;
    let kvs = resp.kvs();
    let kv = match kvs.first() {
        Some(kv) => kv,
        None => return Err(MpscError::ChanMetaNotFound(chan_id)),
    };
    let value = kv.value();
    let meta: ChanGlobalMeta = serde_json::from_slice(value)
        .map_err(|source| MpscError::InvalidChanMeta { chan_id, source })?;
    if meta.ttl_seconds < MIN_TTL_SECONDS {
        return Err(MpscError::InvalidTtl {
            chan_id,
            ttl_seconds: meta.ttl_seconds,
        });
    }
    let version = kv.version();
    Ok(ChanMetaWithVersion { meta, version })
}

/// Get channel meta for the given channel id, without exposing etcd
/// version. 对于只需要存在性和内容而不需要版本校验的调用方，
/// 使用该函数即可。
pub async fn get_chan_meta(
    client: &mut etcd::Client,
    chan_id: i64,
) -> Result<ChanGlobalMeta, MpscError> {
    let ChanMetaWithVersion { meta, .. } = get_chan_meta_with_version(client, chan_id).await?;
    Ok(meta)
}

/// Channel manager that operates on etcd metadata and cooperates with
/// the shared endpoints-scoped `LeaseManager` for lease registration.
///
/// Backed by `LeaseManager` and its backend uid, this struct
/// aggregates the channel id and the three etcd lease handles
/// associated with a bound member plus the kvclient payload
/// lease handle:
///
/// - member lease (producer/consumer)
/// - global lease for `/channels/meta/{chan_id}` and
///   `/channels/{chan_id}/next_producer_id`
/// - per-channel global long lease for distributed id allocation
/// - kvclient payload lease for backend payload keys (always present)
///
/// 这样可以把 lease 的生命周期集中放在 "manager" 一处；外层的
/// producer/consumer 只需要持有 `ChanManager` 即可，不再直接
/// 保存各自的 `LeaseHandle`。
pub struct ChanManager {
    pub(crate) lease_manager: LeaseManager,
    /// Backend uid for etcd metadata/leases.
    pub(crate) etcd_backend_uid: LeaseBackendUid,
    /// Backend uid for kvclient payload leases.
    pub(crate) kv_backend_uid: LeaseBackendUid,
    /// Channel id owned/managed by this manager.
    pub chan_id: i64,
    /// Per-channel member lease owned by this manager instance.
    pub member_lease: GeneralLease,
    /// Global lease handle (TTL = chan ttl_seconds).
    pub global_lease: GeneralLease,
    /// Long-lived global lease handle for id allocation.
    pub global_long_lease: GeneralLease,
    /// kvclient payload lease handle owned by this manager.
    ///
    /// 语义约定：ChanManager 在构造时必须已经为该 channel
    /// 决定好 payload lease id，并通过 LeaseManager 注册
    /// 对应的 kvclient keepalive；此处始终持有一个有效句柄。
    pub payload_lease: GeneralLease,
    pub(crate) etcd_client: etcd::Client,
}

impl ChanManager {
    /// Acquire an etcd client for this channel's backend.
    ///
    /// ChanManager 内部持有一个已连接的 etcd client，并在此处
    /// 返回其 clone；调用方无需也不应通过 LeaseManager 直接拿
    /// 底层 client。该方法为同步方法，无需 `async`。
    pub fn etcd_client(&self) -> etcd::Client {
        self.etcd_client.clone()
    }

    /// Accessor for the current member lease id.
    ///
    /// ChanManager 始终持有一个与该 channel 绑定的 member
    /// lease；上层的 producer/consumer 在进行 membership 绑定
    /// 时应复用该 lease，而不是额外创建新的 lease。
    pub fn member_lease_id(&self) -> i64 {
        self.member_lease.id() as i64
    }
}
