use super::msg_and_error::OK;
use super::msg_and_error::{
    ApiError, ConfigError, ErrorCode, KvError, KvResult, SharedMemError, UnreachableError,
};
use crate::client_kv_api::msg_pack::{
    ExternalBatchGetCancelResp, ExternalBatchGetItemResp, ExternalBatchGetResp,
    ExternalBatchGetStartResp, ExternalBatchGetTransferResp, ExternalBatchIsExistResp,
    ExternalBatchPutCommitItemResp, ExternalBatchPutCommitResp, ExternalBatchPutStartItemResp,
    ExternalBatchPutStartResp, ExternalBatchPutTransferEndItemResp,
    ExternalBatchPutTransferEndResp, ExternalDeleteAckResp, ExternalDeleteResp, ExternalGetResp,
    ExternalIsExistResp, ExternalObservabilitySnapshotResp, ExternalPutCommitResp,
    ExternalPutRevokeResp, ExternalPutStartResp, ExternalPutTransferEndResp,
};
use crate::master_kv_router::msg_pack::{
    BatchDeleteAckResp, BatchDeleteClientKvMetaCacheResp, BatchIsExistResp,
    BatchPreparePutKeysResp, BatchReleasePutKeyReservationsResp, DeleteAckResp, DeleteResp,
    GetDoneResp, GetMasterOnlyMetricPartResp, GetMetaResp, GetRevokeResp, GetStartResp,
    MemHolderKeepAliveResp, MemHolderReleaseResp, PutAppendDoneResp, PutAppendRevokeResp,
    PutAppendStartResp, PutDoneResp, PutRevokeResp, PutStartResp, ReleaseLocalGrantResp,
    ReserveLocalGrantResp,
};
use crate::master_seg_manager::msg_pack::RequestSegmentRegistrationResp;
use crate::memholder::ExternalMemHolderInfo;

/// Shared helpers to convert RPC response structs into KvResult types for KV APIs
/// so both owner and external client paths can reuse consistent semantics.

/// Convert ExternalGetResp into KvResult<Option<ExternalMemHolderInfo>>
/// Semantics:
/// - success && Some(info) => Ok(Some(info))
/// - success && None       => Ok(None)
/// - !success and error_msg indicates key-not-found => Ok(None)
/// - !success otherwise => Err(KvError::Internal(error_msg))
pub trait ToResult {
    type Ok;
    fn to_result(self) -> KvResult<Self::Ok>;
}

/// Construct error responses consistently from KvError
pub trait FromError {
    fn from_error(e: &KvError) -> Self;
}

impl ToResult for ExternalGetResp {
    type Ok = Option<ExternalMemHolderInfo>;
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            return Ok(self.external_memholder_info);
        }
        if self.error_code == super::msg_and_error::codes_api::API_KEY_NOT_FOUND {
            return Ok(None);
        }
        Err(KvError::from_json(self.error_code, &self.error_json))
    }
}

/// Convert ExternalIsExistResp into KvResult<bool>
impl ToResult for ExternalIsExistResp {
    type Ok = bool;
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            return Ok(self.exists);
        }
        if self.error_code == super::msg_and_error::codes_api::API_KEY_NOT_FOUND {
            return Ok(false);
        }
        Err(KvError::from_json(self.error_code, &self.error_json))
    }
}

impl ToResult for ExternalBatchIsExistResp {
    type Ok = Vec<bool>;
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            return Ok(self.exists_list);
        }
        Err(KvError::from_json(self.error_code, &self.error_json))
    }
}

/// Convert ExternalDeleteResp into KvResult<()>
impl ToResult for ExternalDeleteResp {
    type Ok = ();
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            return Ok(());
        }
        if self.error_code == super::msg_and_error::codes_api::API_KEY_NOT_FOUND {
            return Ok(());
        }
        Err(KvError::from_json(self.error_code, &self.error_json))
    }
}

/// Convert ExternalPutStartResp success flag; return original resp on success
impl ToResult for ExternalPutStartResp {
    type Ok = ExternalPutStartResp;
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            Ok(self)
        } else {
            Err(KvError::from_json(self.error_code, &self.error_json))
        }
    }
}

/// Convert ExternalPutTransferEndResp to KvResult
impl ToResult for ExternalPutTransferEndResp {
    type Ok = ();
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            Ok(())
        } else {
            Err(KvError::from_json(self.error_code, &self.error_json))
        }
    }
}

impl ToResult for ExternalPutCommitResp {
    type Ok = ();
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            Ok(())
        } else {
            Err(KvError::from_json(self.error_code, &self.error_json))
        }
    }
}

impl ToResult for ExternalPutRevokeResp {
    type Ok = ();
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            Ok(())
        } else {
            Err(KvError::from_json(self.error_code, &self.error_json))
        }
    }
}

impl ToResult for ExternalDeleteAckResp {
    type Ok = ();
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            return Ok(());
        }
        if self.error_code == super::msg_and_error::codes_api::API_KEY_NOT_FOUND {
            return Ok(());
        }
        Err(KvError::from_json(self.error_code, &self.error_json))
    }
}

impl ToResult for BatchIsExistResp {
    type Ok = Vec<bool>;
    fn to_result(self) -> KvResult<Self::Ok> {
        if self.error_code == OK {
            return Ok(self.exists_list);
        }
        Err(KvError::from_json(self.error_code, &self.error_json))
    }
}

/// Centralized: convert code+desc to KvError or Ok if code==Ok
pub fn try_from_code(code: ErrorCode, json: String) -> KvResult<()> {
    if code == OK {
        Ok(())
    } else {
        Err(KvError::from_json(code, &json))
    }
}

impl FromError for RequestSegmentRegistrationResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}

// ---- FromError for External Client Resps ----
impl FromError for ExternalGetResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalPutStartResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalPutTransferEndResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalPutCommitResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalPutRevokeResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalDeleteResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalIsExistResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchIsExistResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchGetResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchGetItemResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchGetStartResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchGetTransferResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchGetCancelResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
        }
    }
}
impl FromError for ExternalBatchPutStartResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchPutStartItemResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchPutTransferEndResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchPutTransferEndItemResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchPutCommitResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalBatchPutCommitItemResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalObservabilitySnapshotResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ExternalDeleteAckResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}

// ---- FromError for Master KV Router Resps ----
impl FromError for GetStartResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for GetRevokeResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for GetDoneResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for PutStartResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for PutRevokeResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for PutDoneResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ReserveLocalGrantResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for ReleaseLocalGrantResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for BatchPreparePutKeysResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for BatchReleasePutKeyReservationsResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for PutAppendStartResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for PutAppendRevokeResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for PutAppendDoneResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for MemHolderKeepAliveResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for MemHolderReleaseResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for DeleteResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for DeleteAckResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for BatchDeleteAckResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for GetMetaResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for BatchIsExistResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
impl FromError for BatchDeleteClientKvMetaCacheResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}

impl FromError for GetMasterOnlyMetricPartResp {
    fn from_error(e: &KvError) -> Self {
        let code = e.code();
        Self {
            error_code: code,
            error_json: e.to_json(),
            ..Default::default()
        }
    }
}
