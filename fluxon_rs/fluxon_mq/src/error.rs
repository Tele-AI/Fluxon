use thiserror::Error;

/// Unified error type for MPSC operations.
///
/// 所有错误在这里扁平罗列，并通过 `code()` 提供
/// 唯一且稳定的错误码，便于 Python 侧解码与定位。
///
/// 注意：这些错误码与 payload 回调返回的 0/1/2 含义
/// 完全独立，不要混用。
#[derive(Debug, Error)]
pub enum MpscError {
    #[error("no new message available")]
    NoMessage,

    #[error("consumer is closed")]
    Closed,

    #[error("etcd error: {0}")]
    Etcd(#[from] etcd_client::Error),

    #[error("spawn blocking task failed: {0}")]
    JoinError(#[from] tokio::task::JoinError),

    #[error(
        "message buffer full: channel_id={channel_id} capacity={capacity} used_slots={used_slots}"
    )]
    MessageBufferFull {
        channel_id: i64,
        capacity: i64,
        used_slots: i64,
    },

    #[error("put payload returned non-retryable error (code=2)")]
    PutPayloadNonRetryable,

    #[error("put payload returned unknown code {code}")]
    PutPayloadUnknownCode { code: i32 },

    #[error("get payload returned non-retryable: {message}")]
    GetPayloadNonRetryable { message: String },

    #[error("get payload returned unknown code {code}")]
    GetPayloadUnknownCode { code: i32 },

    #[error("delete payload returned non-retryable: {message}")]
    DeletePayloadNonRetryable { message: String },

    #[error("delete payload returned unknown code {code}")]
    DeletePayloadUnknownCode { code: i32 },

    #[error("failed to update consume offset in etcd for producer {producer_id}: {source}")]
    ConsumeOffsetUpdate {
        producer_id: String,
        #[source]
        source: anyhow::Error,
    },

    #[error("internal error: {0}")]
    Internal(String),
}

impl MpscError {
    /// 获取该错误的唯一错误码（稳定用于跨语言解码）。
    ///
    /// 约定（可根据需要扩展，但不复用已有值）：
    /// - 1000 段：可重试类错误
    /// - 2000+ 段：ETCD / 系统错误
    /// - 3000 段：put payload 相关
    /// - 4000 段：get payload 相关
    /// - 6000 段：offset 相关
    /// - 9000 段：内部错误
    pub fn code(&self) -> i32 {
        match self {
            // 可重试类
            MpscError::NoMessage => 1000,
            MpscError::Closed => 1001,

            // etcd / 系统
            MpscError::Etcd(_) => 2000,
            MpscError::JoinError(_) => 2001,
            MpscError::MessageBufferFull { .. } => 2002,

            // put payload
            MpscError::PutPayloadNonRetryable => 3000,
            MpscError::PutPayloadUnknownCode { .. } => 3001,

            // get payload
            MpscError::GetPayloadNonRetryable { .. } => 4000,
            MpscError::GetPayloadUnknownCode { .. } => 4001,

            // delete payload
            MpscError::DeletePayloadNonRetryable { .. } => 7000,
            MpscError::DeletePayloadUnknownCode { .. } => 7001,

            // offset 相关
            MpscError::ConsumeOffsetUpdate { .. } => 6000,

            // 内部错误（兜底）
            MpscError::Internal(_) => 9000,
        }
    }
}
