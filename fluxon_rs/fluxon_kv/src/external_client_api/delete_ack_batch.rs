use super::ExternalClientApiView;
use ::tokio::sync::mpsc;
use limit_thirdparty::tokio;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub(crate) const EXTERNAL_DELETE_ACK_BATCH_MAX_ITEMS: usize = 1024;
const EXTERNAL_DELETE_ACK_BATCH_MERGE_WINDOW: Duration = Duration::from_millis(1);
const EXTERNAL_DELETE_ACK_BATCH_RPC_TIMEOUT: Duration = Duration::from_secs(5);
const EXTERNAL_DELETE_ACK_BATCH_LOG_EVERY: u64 = 512;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExternalDeleteAckItem {
    pub external_client_id: String,
    pub holder_id: u64,
    pub owner_start_time: i64,
}

#[derive(Debug, Eq, PartialEq)]
struct ExternalDeleteAckBatch {
    external_client_id: String,
    owner_start_time: i64,
    holder_ids: Vec<u64>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ExternalDeleteAckBatchSnapshot {
    pub enqueued_items: u64,
    pub enqueue_failures: u64,
    pub rpc_batches: u64,
    pub rpc_items: u64,
    pub released_items: u64,
    pub missing_items: u64,
    pub generation_mismatch_items: u64,
    pub rpc_failures: u64,
    pub max_batch_items: u64,
}

#[derive(Default)]
struct ExternalDeleteAckBatchCounters {
    enqueued_items: AtomicU64,
    enqueue_failures: AtomicU64,
    rpc_batches: AtomicU64,
    rpc_items: AtomicU64,
    released_items: AtomicU64,
    missing_items: AtomicU64,
    generation_mismatch_items: AtomicU64,
    rpc_failures: AtomicU64,
    max_batch_items: AtomicU64,
}

impl ExternalDeleteAckBatchCounters {
    fn snapshot(&self) -> ExternalDeleteAckBatchSnapshot {
        ExternalDeleteAckBatchSnapshot {
            enqueued_items: self.enqueued_items.load(Ordering::Relaxed),
            enqueue_failures: self.enqueue_failures.load(Ordering::Relaxed),
            rpc_batches: self.rpc_batches.load(Ordering::Relaxed),
            rpc_items: self.rpc_items.load(Ordering::Relaxed),
            released_items: self.released_items.load(Ordering::Relaxed),
            missing_items: self.missing_items.load(Ordering::Relaxed),
            generation_mismatch_items: self.generation_mismatch_items.load(Ordering::Relaxed),
            rpc_failures: self.rpc_failures.load(Ordering::Relaxed),
            max_batch_items: self.max_batch_items.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct ExternalDeleteAckBatchHandle {
    tx: mpsc::UnboundedSender<ExternalDeleteAckItem>,
    rx: Mutex<Option<mpsc::UnboundedReceiver<ExternalDeleteAckItem>>>,
    counters: ExternalDeleteAckBatchCounters,
}

impl ExternalDeleteAckBatchHandle {
    pub(crate) fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
            counters: ExternalDeleteAckBatchCounters::default(),
        }
    }

    pub(crate) fn enqueue(&self, item: ExternalDeleteAckItem) -> Result<(), String> {
        match self.tx.send(item) {
            Ok(()) => {
                self.counters.enqueued_items.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(err) => {
                self.counters
                    .enqueue_failures
                    .fetch_add(1, Ordering::Relaxed);
                Err(format!(
                    "external holder ACK batch worker is unavailable for holder_id={}",
                    err.0.holder_id
                ))
            }
        }
    }

    pub(crate) fn take_rx(&self) -> Option<mpsc::UnboundedReceiver<ExternalDeleteAckItem>> {
        self.rx.lock().take()
    }

    pub(crate) fn snapshot(&self) -> ExternalDeleteAckBatchSnapshot {
        self.counters.snapshot()
    }

    fn record_batch(&self, item_count: usize) -> u64 {
        let item_count = u64::try_from(item_count).unwrap_or(u64::MAX);
        self.counters
            .max_batch_items
            .fetch_max(item_count, Ordering::Relaxed);
        self.counters
            .rpc_items
            .fetch_add(item_count, Ordering::Relaxed);
        self.counters.rpc_batches.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn record_result(&self, result: super::ExternalDeleteAckBatchSendResult) {
        match result {
            super::ExternalDeleteAckBatchSendResult::Applied { released, missing } => {
                self.counters
                    .released_items
                    .fetch_add(u64::from(released), Ordering::Relaxed);
                self.counters
                    .missing_items
                    .fetch_add(u64::from(missing), Ordering::Relaxed);
            }
            super::ExternalDeleteAckBatchSendResult::OwnerGenerationChanged { items } => {
                self.counters
                    .generation_mismatch_items
                    .fetch_add(items, Ordering::Relaxed);
            }
        }
    }

    fn record_rpc_failure(&self) {
        self.counters.rpc_failures.fetch_add(1, Ordering::Relaxed);
    }
}

fn build_external_delete_ack_batches(
    items: Vec<ExternalDeleteAckItem>,
) -> Vec<ExternalDeleteAckBatch> {
    let mut groups: BTreeMap<(String, i64), BTreeSet<u64>> = BTreeMap::new();
    for item in items {
        groups
            .entry((item.external_client_id, item.owner_start_time))
            .or_default()
            .insert(item.holder_id);
    }
    let mut batches = Vec::new();
    for ((external_client_id, owner_start_time), holder_ids) in groups {
        let holder_ids: Vec<_> = holder_ids.into_iter().collect();
        for chunk in holder_ids.chunks(EXTERNAL_DELETE_ACK_BATCH_MAX_ITEMS) {
            batches.push(ExternalDeleteAckBatch {
                external_client_id: external_client_id.clone(),
                owner_start_time,
                holder_ids: chunk.to_vec(),
            });
        }
    }
    batches
}

fn log_external_delete_ack_batch_snapshot(view: &ExternalClientApiView, reason: &'static str) {
    let snapshot = view
        .external_client_api()
        .inner()
        .external_delete_ack_batch_snapshot();
    tracing::info!(
        reason,
        enqueued_items = snapshot.enqueued_items,
        enqueue_failures = snapshot.enqueue_failures,
        rpc_batches = snapshot.rpc_batches,
        rpc_items = snapshot.rpc_items,
        released_items = snapshot.released_items,
        missing_items = snapshot.missing_items,
        generation_mismatch_items = snapshot.generation_mismatch_items,
        rpc_failures = snapshot.rpc_failures,
        max_batch_items = snapshot.max_batch_items,
        "external holder ACK batch snapshot"
    );
}

pub(crate) fn spawn_external_delete_ack_batch(
    view: ExternalClientApiView,
    mut rx: mpsc::UnboundedReceiver<ExternalDeleteAckItem>,
) {
    let spawn_view = view.clone();
    let worker_view = view.clone();
    spawn_view.spawn("external_delete_ack_batch", async move {
        tracing::info!(
            max_items = EXTERNAL_DELETE_ACK_BATCH_MAX_ITEMS,
            merge_window_us = EXTERNAL_DELETE_ACK_BATCH_MERGE_WINDOW.as_micros(),
            "external holder ACK batch worker started"
        );
        let mut shutdown_waiter = worker_view.register_shutdown_waiter();
        loop {
            let first = tokio::select! {
                biased;
                _ = shutdown_waiter.wait() => {
                    log_external_delete_ack_batch_snapshot(&worker_view, "shutdown");
                    return;
                }
                item = rx.recv() => {
                    let Some(item) = item else {
                        log_external_delete_ack_batch_snapshot(&worker_view, "channel_closed");
                        return;
                    };
                    item
                }
            };

            let mut pending = Vec::with_capacity(EXTERNAL_DELETE_ACK_BATCH_MAX_ITEMS);
            pending.push(first);
            let merge_window = tokio::time::sleep(EXTERNAL_DELETE_ACK_BATCH_MERGE_WINDOW);
            tokio::pin!(merge_window);
            let mut shutting_down = false;
            while pending.len() < EXTERNAL_DELETE_ACK_BATCH_MAX_ITEMS {
                tokio::select! {
                    biased;
                    _ = shutdown_waiter.wait() => {
                        shutting_down = true;
                        break;
                    }
                    item = rx.recv() => {
                        match item {
                            Some(item) => pending.push(item),
                            None => break,
                        }
                    }
                    _ = &mut merge_window => break,
                }
            }
            if shutting_down {
                tracing::info!(
                    dropped_pending_items = pending.len(),
                    "external holder ACK batch worker skipped pending items during shutdown"
                );
                log_external_delete_ack_batch_snapshot(&worker_view, "shutdown_with_pending");
                return;
            }

            for batch in build_external_delete_ack_batches(pending) {
                let item_count = batch.holder_ids.len();
                let batch_number = worker_view
                    .external_client_api()
                    .inner()
                    .external_delete_ack_batch
                    .record_batch(item_count);
                let send_result = tokio::time::timeout(
                    EXTERNAL_DELETE_ACK_BATCH_RPC_TIMEOUT,
                    worker_view
                        .external_client_api()
                        .inner()
                        .send_external_delete_ack_batch(
                            &batch.external_client_id,
                            batch.owner_start_time,
                            batch.holder_ids,
                        ),
                )
                .await;
                match send_result {
                    Ok(Ok(result)) => worker_view
                        .external_client_api()
                        .inner()
                        .external_delete_ack_batch
                        .record_result(result),
                    Ok(Err(err)) => {
                        worker_view
                            .external_client_api()
                            .inner()
                            .external_delete_ack_batch
                            .record_rpc_failure();
                        tracing::warn!(
                            external_client_id = %batch.external_client_id,
                            owner_start_time = batch.owner_start_time,
                            items = item_count,
                            error = %err,
                            "external holder ACK batch RPC failed"
                        );
                    }
                    Err(_) => {
                        worker_view
                            .external_client_api()
                            .inner()
                            .external_delete_ack_batch
                            .record_rpc_failure();
                        tracing::warn!(
                            external_client_id = %batch.external_client_id,
                            owner_start_time = batch.owner_start_time,
                            items = item_count,
                            timeout_secs = EXTERNAL_DELETE_ACK_BATCH_RPC_TIMEOUT.as_secs(),
                            "external holder ACK batch RPC timed out"
                        );
                    }
                }
                if batch_number % EXTERNAL_DELETE_ACK_BATCH_LOG_EVERY == 0 {
                    log_external_delete_ack_batch_snapshot(&worker_view, "periodic");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{
        EXTERNAL_DELETE_ACK_BATCH_MAX_ITEMS, ExternalDeleteAckItem,
        build_external_delete_ack_batches,
    };

    fn item(client: &str, generation: i64, holder_id: u64) -> ExternalDeleteAckItem {
        ExternalDeleteAckItem {
            external_client_id: client.to_string(),
            holder_id,
            owner_start_time: generation,
        }
    }

    #[test]
    fn grouping_is_generation_safe_and_deduplicates_holder_ids() {
        let batches = build_external_delete_ack_batches(vec![
            item("client-a", 11, 3),
            item("client-a", 11, 3),
            item("client-a", 11, 2),
            item("client-a", 12, 4),
            item("client-b", 11, 5),
        ]);
        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0].external_client_id, "client-a");
        assert_eq!(batches[0].owner_start_time, 11);
        assert_eq!(batches[0].holder_ids, vec![2, 3]);
        assert_eq!(batches[1].owner_start_time, 12);
        assert_eq!(batches[1].holder_ids, vec![4]);
        assert_eq!(batches[2].external_client_id, "client-b");
        assert_eq!(batches[2].holder_ids, vec![5]);
    }

    #[test]
    fn batches_never_exceed_wire_batch_limit() {
        let item_count = EXTERNAL_DELETE_ACK_BATCH_MAX_ITEMS * 2 + 1;
        let items = (0..item_count)
            .map(|holder_id| item("client-a", 11, holder_id as u64))
            .collect();
        let batches = build_external_delete_ack_batches(items);
        assert_eq!(batches.len(), 3);
        assert!(
            batches
                .iter()
                .all(|batch| batch.holder_ids.len() <= EXTERNAL_DELETE_ACK_BATCH_MAX_ITEMS)
        );
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.holder_ids.len())
                .sum::<usize>(),
            item_count
        );
    }
}
