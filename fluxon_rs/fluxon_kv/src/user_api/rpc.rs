use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::runtime::Handle;

use crate::Framework;
use crate::cluster_manager::NodeID;
use crate::rpcresp_kvresult_convert::msg_and_error::{ApiError, KvError, KvResult};
use crate::user_api::codec_flat_dict::{decode_flat_dict_bytes, encode_flat_dict_bytes};
use crate::user_api::flat_dict::FlatDict;
use crate::user_rpc;
use fluxon_util::run_async_from_sync::{SyncAsyncBridge, borrow_stable_owner};

// User-RPC is designed for "small" control-plane calls.
// We intentionally hardcode the default timeout to the minimum to keep the user-facing
// contract simple: callers may override per-call, but must never go below the minimum.
pub const USER_RPC_DEFAULT_TIMEOUT_MS: u64 = user_rpc::USER_RPC_MIN_TIMEOUT_MS;

pub trait UserRpcClient: Send + Sync {
    fn call(
        &self,
        node_id: &str,
        path: &str,
        payload: FlatDict,
        timeout_ms: Option<u64>,
    ) -> KvResult<FlatDict>;
}

pub trait UserRpcServer: Send + Sync {
    fn register(
        &self,
        path: &str,
        handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync + 'static>,
    ) -> KvResult<()>;

    fn register_async(
        &self,
        path: &str,
        handler: Arc<dyn Fn(String, FlatDict) -> UserRpcFlatDictFuture + Send + Sync + 'static>,
    ) -> KvResult<()>;
}

pub type UserRpcFlatDictFuture = Pin<Box<dyn Future<Output = KvResult<FlatDict>> + Send + 'static>>;

pub(crate) fn validate_timeout_ms(timeout_ms: u64) -> KvResult<()> {
    user_rpc::validate_timeout_ms(timeout_ms)
}

pub(crate) struct FluxonUserRpcImpl {
    pub(crate) framework: Arc<Framework>,
    pub(crate) runtime: Handle,
}

impl UserRpcClient for FluxonUserRpcImpl {
    fn call(
        &self,
        node_id: &str,
        path: &str,
        payload: FlatDict,
        timeout_ms: Option<u64>,
    ) -> KvResult<FlatDict> {
        let timeout_ms = timeout_ms.unwrap_or(USER_RPC_DEFAULT_TIMEOUT_MS);
        validate_timeout_ms(timeout_ms)?;

        let bytes = encode_flat_dict_bytes(&payload)?;
        let framework = borrow_stable_owner(&self.framework);
        let node: NodeID = node_id.to_string().into();

        let res = self.runtime.run_async_from_sync(async {
            user_rpc::user_rpc_call(framework, node, path.to_string(), bytes, timeout_ms).await
        });

        let out_bytes = match res {
            Ok(v) => v?,
            Err(e) => {
                return Err(KvError::Api(ApiError::Unknown {
                    detail: format!("runtime bridge failed: {}", e),
                }));
            }
        };
        decode_flat_dict_bytes(&out_bytes)
    }
}

impl UserRpcServer for FluxonUserRpcImpl {
    fn register(
        &self,
        path: &str,
        handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync + 'static>,
    ) -> KvResult<()> {
        let p = path.to_string();
        let h: Arc<dyn crate::p2p::p2p_module::UserRpcHandler> =
            Arc::new(FluxonUserRpcHandler { handler });
        crate::p2p::p2p_module::user_rpc_register_handler(
            self.framework.p2p_view().p2p_module(),
            p,
            h,
        );
        Ok(())
    }

    fn register_async(
        &self,
        path: &str,
        handler: Arc<dyn Fn(String, FlatDict) -> UserRpcFlatDictFuture + Send + Sync + 'static>,
    ) -> KvResult<()> {
        let p = path.to_string();
        let h: Arc<dyn crate::p2p::p2p_module::UserRpcAsyncHandler> =
            Arc::new(FluxonUserRpcAsyncHandler { handler });
        crate::p2p::p2p_module::user_rpc_register_handler_async(
            self.framework.p2p_view().p2p_module(),
            p,
            h,
        );
        Ok(())
    }
}

struct FluxonUserRpcHandler {
    handler: Arc<dyn Fn(String, FlatDict) -> KvResult<FlatDict> + Send + Sync + 'static>,
}

impl crate::p2p::p2p_module::UserRpcHandler for FluxonUserRpcHandler {
    fn handle(&self, from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError> {
        let req = decode_flat_dict_bytes(payload)?;
        let out = (self.handler)(from_node.to_string(), req)?;
        let out_bytes = encode_flat_dict_bytes(&out)?;
        Ok(out_bytes)
    }
}

struct FluxonUserRpcAsyncHandler {
    handler: Arc<dyn Fn(String, FlatDict) -> UserRpcFlatDictFuture + Send + Sync + 'static>,
}

impl crate::p2p::p2p_module::UserRpcAsyncHandler for FluxonUserRpcAsyncHandler {
    fn handle(&self, from_node: NodeID, payload: Vec<u8>) -> crate::p2p::p2p_module::UserRpcFuture {
        let handler = self.handler.clone();
        let from_node = from_node.to_string();
        Box::pin(async move {
            let req = decode_flat_dict_bytes(&payload)?;
            let out = handler(from_node, req).await?;
            let out_bytes = encode_flat_dict_bytes(&out)?;
            Ok(out_bytes)
        })
    }
}
