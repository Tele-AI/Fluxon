use crate::notify_state::AsyncStopSignal;
use etcd_client as etcd;
use std::future::Future;
use std::time::Duration;
use tracing::warn;

pub const ETCD_PREFIX_WATCH_RESTART_SLEEP: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum EtcdPrefixWatchLoopControl {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum OwnedEtcdWatchEventKind {
    Put,
    Delete,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct OwnedEtcdWatchEvent {
    pub kind: OwnedEtcdWatchEventKind,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub async fn run_prefix_watch_loop<S, ResyncFn, ResyncFut, BatchFn, BatchFut>(
    mut client: etcd::Client,
    prefix: String,
    opts: etcd::WatchOptions,
    restart_sleep: Duration,
    watch_label: String,
    stop: S,
    mut on_resync: ResyncFn,
    mut on_batch: BatchFn,
) where
    S: AsyncStopSignal,
    ResyncFn: FnMut() -> ResyncFut + Send,
    ResyncFut: Future<Output = EtcdPrefixWatchLoopControl> + Send,
    BatchFn: FnMut(Vec<OwnedEtcdWatchEvent>) -> BatchFut + Send,
    BatchFut: Future<Output = EtcdPrefixWatchLoopControl> + Send,
{
    loop {
        if stop.is_stopped() {
            return;
        }

        let watch_res = tokio::select! {
            biased;
            res = client.watch(prefix.clone(), Some(opts.clone())) => res,
            _ = stop.wait_stopped() => return,
        };
        let (_watcher, mut stream) = match watch_res {
            Ok(pair) => pair,
            Err(err) => {
                warn!(
                    "{} failed to start prefix watch for prefix {}: {:?}",
                    watch_label, prefix, err
                );
                if wait_restart_or_stop(&stop, restart_sleep).await {
                    return;
                }
                continue;
            }
        };

        if matches!(on_resync().await, EtcdPrefixWatchLoopControl::Stop) {
            return;
        }

        loop {
            if stop.is_stopped() {
                return;
            }

            let msg = tokio::select! {
                biased;
                res = stream.message() => res,
                _ = stop.wait_stopped() => return,
            };
            match msg {
                Ok(Some(resp)) => {
                    let events = owned_watch_events_from_response(resp);
                    if events.is_empty() {
                        continue;
                    }
                    if matches!(on_batch(events).await, EtcdPrefixWatchLoopControl::Stop) {
                        return;
                    }
                }
                Ok(None) => {
                    warn!(
                        "{} prefix watch stream closed for prefix {}; restarting watch",
                        watch_label, prefix
                    );
                    if wait_restart_or_stop(&stop, restart_sleep).await {
                        return;
                    }
                    break;
                }
                Err(err) => {
                    warn!(
                        "{} prefix watch stream error for prefix {}: {:?}; restarting watch",
                        watch_label, prefix, err
                    );
                    if wait_restart_or_stop(&stop, restart_sleep).await {
                        return;
                    }
                    break;
                }
            }
        }
    }
}

async fn wait_restart_or_stop<S: AsyncStopSignal>(stop: &S, restart_sleep: Duration) -> bool {
    tokio::select! {
        biased;
        _ = stop.wait_stopped() => true,
        _ = tokio::time::sleep(restart_sleep) => false,
    }
}

fn owned_watch_events_from_response(resp: etcd::WatchResponse) -> Vec<OwnedEtcdWatchEvent> {
    let mut owned_events = Vec::with_capacity(resp.events().len());
    for event in resp.events() {
        let kind = match event.event_type() {
            etcd::EventType::Put => OwnedEtcdWatchEventKind::Put,
            etcd::EventType::Delete => OwnedEtcdWatchEventKind::Delete,
        };
        let Some(kv) = event.kv() else {
            continue;
        };
        owned_events.push(OwnedEtcdWatchEvent {
            kind,
            key: kv.key().to_vec(),
            value: kv.value().to_vec(),
        });
    }
    owned_events
}
