pub(crate) mod control_plane_rpc;
pub mod msg_pack;
pub mod p2p_module;

pub type P2PError = fluxon_commu::p2p::P2pError;
pub type P2PResult<T> = Result<T, P2PError>;

pub use fluxon_commu::p2p::network_transport_kind;
