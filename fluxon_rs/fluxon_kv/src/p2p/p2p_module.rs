use crate::cluster_manager::NodeID;
use crate::rpcresp_kvresult_convert::msg_and_error::KvError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub use fluxon_commu::p2p::rpc::{MIN_EXPLICIT_RPC_TIMEOUT_SECS, RPCResponsor, Responser};
pub use fluxon_commu::p2p::rpc::{
    UserRpcBytesOutput as UserRpcHandlerOutput, UserRpcHandlerLocalObserve,
};
pub use fluxon_commu::p2p::{
    MsgId, P2pModule, P2pModuleAccessTrait, P2pModuleNewArg, P2pModuleView, P2pModuleViewTrait,
    P2pTcpThreadReactorWaitMode, P2pTcpThreadTransportTuning, RpcTransportPolicy, TaskId,
    UserRpcReq, UserRpcResp,
};

pub trait UserRpcHandler: Send + Sync + 'static {
    fn handle(&self, from_node: NodeID, payload: &[u8]) -> Result<Vec<u8>, KvError>;

    fn handle_observed(
        &self,
        from_node: NodeID,
        payload: &[u8],
    ) -> Result<UserRpcHandlerOutput, KvError> {
        self.handle(from_node, payload)
            .map(UserRpcHandlerOutput::from_payload)
    }
}

pub type UserRpcFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, KvError>> + Send + 'static>>;

pub trait UserRpcAsyncHandler: Send + Sync + 'static {
    fn handle(&self, from_node: NodeID, payload: Vec<u8>) -> UserRpcFuture;
}

struct KvUserRpcBytesHandlerAdapter {
    inner: Arc<dyn UserRpcHandler>,
}

struct KvUserRpcBytesAsyncHandlerAdapter {
    inner: Arc<dyn UserRpcAsyncHandler>,
}

impl fluxon_commu::p2p::rpc::UserRpcBytesHandler for KvUserRpcBytesHandlerAdapter {
    fn handle(
        &self,
        from_node: NodeID,
        payload: &[u8],
    ) -> Result<fluxon_commu::p2p::rpc::UserRpcBytesOutput, fluxon_commu::p2p::rpc::UserRpcBytesError>
    {
        self.inner
            .handle_observed(from_node, payload)
            .map_err(|err| fluxon_commu::p2p::rpc::UserRpcBytesError {
                error_code: err.code(),
                error_json: err.to_json(),
            })
    }
}

impl fluxon_commu::p2p::rpc::UserRpcBytesAsyncHandler for KvUserRpcBytesAsyncHandlerAdapter {
    fn handle(
        &self,
        from_node: NodeID,
        payload: Vec<u8>,
    ) -> fluxon_commu::p2p::rpc::UserRpcBytesFuture {
        let fut = self.inner.handle(from_node, payload);
        Box::pin(async move {
            fut.await
                .map(fluxon_commu::p2p::rpc::UserRpcBytesOutput::from_payload)
                .map_err(|err| fluxon_commu::p2p::rpc::UserRpcBytesError {
                    error_code: err.code(),
                    error_json: err.to_json(),
                })
        })
    }
}

pub fn user_rpc_register_handler(p2p: &P2pModule, path: String, handler: Arc<dyn UserRpcHandler>) {
    fluxon_commu::p2p::rpc::register_user_rpc_bytes_handler(
        p2p,
        path,
        Arc::new(KvUserRpcBytesHandlerAdapter { inner: handler }),
    );
}

pub fn user_rpc_register_handler_async(
    p2p: &P2pModule,
    path: String,
    handler: Arc<dyn UserRpcAsyncHandler>,
) {
    fluxon_commu::p2p::rpc::register_user_rpc_bytes_handler_async(
        p2p,
        path,
        Arc::new(KvUserRpcBytesAsyncHandlerAdapter { inner: handler }),
    );
}
