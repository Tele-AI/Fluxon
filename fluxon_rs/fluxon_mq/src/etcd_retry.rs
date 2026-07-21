use etcd_client::Error;
use tonic::Code;

pub(crate) fn is_transient_etcd_error(error: &Error) -> bool {
    match error {
        Error::IoError(_) | Error::TransportError(_) => true,
        Error::GRpcStatus(status) => matches!(
            status.code(),
            Code::Aborted | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Unavailable
        ),
        Error::InvalidArgs(_)
        | Error::InvalidUri(_)
        | Error::WatchError(_)
        | Error::Utf8Error(_)
        | Error::LeaseKeepAliveError(_)
        | Error::ElectError(_)
        | Error::InvalidHeaderValue(_)
        | Error::EndpointError(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::is_transient_etcd_error;
    use etcd_client::Error;
    use tonic::Status;

    #[test]
    fn classifies_only_transient_grpc_statuses_for_retry() {
        assert!(is_transient_etcd_error(&Error::GRpcStatus(
            Status::unavailable("etcdserver: request timed out")
        )));
        assert!(is_transient_etcd_error(&Error::GRpcStatus(
            Status::deadline_exceeded("deadline")
        )));
        assert!(!is_transient_etcd_error(&Error::GRpcStatus(
            Status::invalid_argument("bad key")
        )));
        assert!(!is_transient_etcd_error(&Error::InvalidArgs(
            "bad options".to_string()
        )));
    }
}
