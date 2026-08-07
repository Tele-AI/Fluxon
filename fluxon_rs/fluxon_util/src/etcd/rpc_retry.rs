use std::future::Future;
use std::time::Duration;

use etcd_client::Error;
use tonic::Code;

/// Number of retries after the first etcd RPC attempt.
pub const DEFAULT_ETCD_RPC_MAX_RETRIES: u32 = 2;

const INITIAL_RETRY_DELAY: Duration = Duration::from_millis(50);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(1);

/// Returns whether an etcd error is safe to retry as a transient RPC failure.
pub fn is_transient_etcd_error(error: &Error) -> bool {
    match error {
        Error::IoError(error) => matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::HostUnreachable
                | std::io::ErrorKind::NetworkUnreachable
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock
                | std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::UnexpectedEof
        ),
        Error::TransportError(_) => true,
        Error::GRpcStatus(status) => matches!(
            status.code(),
            Code::DeadlineExceeded | Code::ResourceExhausted | Code::Unavailable
        ),
        _ => false,
    }
}

/// Runs one unary etcd RPC and retries transient failures.
///
/// `max_retries` counts attempts after the initial call. Callers remain responsible for
/// choosing operations whose semantics permit replay.
pub async fn retry_etcd_rpc<T, F, Fut>(
    max_retries: u32,
    operation: &str,
    mut rpc: F,
) -> Result<T, Error>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Error>>,
{
    let mut retries = 0;

    loop {
        match rpc().await {
            Ok(value) => return Ok(value),
            Err(error) if is_transient_etcd_error(&error) && retries < max_retries => {
                retries += 1;
                let delay = retry_delay(retries);
                tracing::warn!(
                    operation,
                    retry = retries,
                    max_retries,
                    delay_ms = delay.as_millis(),
                    error = %error,
                    "transient etcd RPC failure; retrying"
                );
                tokio::time::sleep(delay).await;
            }
            Err(error) => {
                if is_transient_etcd_error(&error) {
                    tracing::warn!(
                        operation,
                        attempts = retries.saturating_add(1),
                        max_retries,
                        error = %error,
                        "transient etcd RPC failure; retry budget exhausted"
                    );
                } else {
                    tracing::debug!(
                        operation,
                        attempts = retries.saturating_add(1),
                        error = %error,
                        "etcd RPC failure is not retryable"
                    );
                }
                return Err(error);
            }
        }
    }
}

fn retry_delay(retry: u32) -> Duration {
    let exponent = retry.saturating_sub(1).min(5);
    (INITIAL_RETRY_DELAY * (1_u32 << exponent)).min(MAX_RETRY_DELAY)
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use etcd_client::Error;
    use tonic::Status;

    use super::{is_transient_etcd_error, retry_etcd_rpc};

    fn unavailable() -> Error {
        Error::GRpcStatus(Status::unavailable("temporary outage"))
    }

    #[test]
    fn classifies_only_explicit_transient_statuses() {
        assert!(is_transient_etcd_error(&unavailable()));
        assert!(is_transient_etcd_error(&Error::GRpcStatus(
            Status::deadline_exceeded("deadline")
        )));
        assert!(is_transient_etcd_error(&Error::GRpcStatus(
            Status::resource_exhausted("overloaded")
        )));
        assert!(!is_transient_etcd_error(&Error::GRpcStatus(
            Status::aborted("transaction conflict")
        )));
        assert!(!is_transient_etcd_error(&Error::GRpcStatus(
            Status::cancelled("caller cancelled")
        )));
        assert!(!is_transient_etcd_error(&Error::GRpcStatus(
            Status::invalid_argument("invalid key")
        )));
        assert!(is_transient_etcd_error(&Error::IoError(io::Error::new(
            io::ErrorKind::ConnectionReset,
            "connection reset",
        ))));
        assert!(is_transient_etcd_error(&Error::IoError(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            "connection refused",
        ))));
        assert!(is_transient_etcd_error(&Error::IoError(io::Error::new(
            io::ErrorKind::NotConnected,
            "not connected",
        ))));
        assert!(is_transient_etcd_error(&Error::IoError(io::Error::new(
            io::ErrorKind::HostUnreachable,
            "host unreachable",
        ))));
        assert!(is_transient_etcd_error(&Error::IoError(io::Error::new(
            io::ErrorKind::NetworkUnreachable,
            "network unreachable",
        ))));
        assert!(!is_transient_etcd_error(&Error::IoError(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "bad certificate permissions",
        ))));
    }

    #[tokio::test]
    async fn zero_retries_makes_one_attempt() {
        let attempts = AtomicUsize::new(0);

        let result = retry_etcd_rpc(0, "get", || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(unavailable())
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn two_retries_make_three_attempts() {
        let attempts = AtomicUsize::new(0);

        let result = retry_etcd_rpc(2, "put", || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(unavailable())
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn non_transient_failure_is_not_retried() {
        let attempts = AtomicUsize::new(0);

        let result = retry_etcd_rpc(2, "delete", || async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(Error::InvalidArgs("invalid range".to_string()))
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn success_stops_before_retry_budget_is_exhausted() {
        let attempts = AtomicUsize::new(0);

        let result = retry_etcd_rpc(2, "get", || async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                Err(unavailable())
            } else {
                Ok("value")
            }
        })
        .await;

        assert_eq!(result.unwrap(), "value");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
