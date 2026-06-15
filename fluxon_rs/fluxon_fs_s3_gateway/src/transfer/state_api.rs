use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use fluxon_fs_core::config::{
    FluxonFsTransferJobState, FluxonFsTransferScanResultWire,
    FluxonFsTransferWorkerHeartbeatResultWire, FluxonFsTransferWorkerResultAckWire,
    FluxonFsTransferWorkerResultWire, FluxonFsTransferWorkerStopReasonWire,
};

use crate::GatewayState;

use super::db::normalize_transfer_root_relpath;
use super::types::{
    FsTransferBatchCollectInfoRecord, FsTransferBatchFileIssueRecord, FsTransferCreateJobArg,
    FsTransferJobRecord, FsTransferJobSnapshot, FsTransferJobSummarySnapshot,
    FsTransferReadyBatchClass, FsTransferReadyBatchDispatch, FsTransferRecentFailureSnapshot,
    FsTransferSchedulerJobSnapshot,
};

// Large scan events can monopolize the durable store actor long enough to
// delay worker launch acks and heartbeats. Keeping each store apply bounded
// lets worker control-plane traffic interleave with ongoing scan ingestion.
const TRANSFER_SCAN_STORE_BATCH_CHUNK_LIMIT: usize = 32;

fn transfer_scan_result_total_batch_count(result: &FluxonFsTransferScanResultWire) -> usize {
    result.direct_files_only_batches.len() + result.full_dir_batches.len()
}

fn build_scan_store_chunk(
    result: &FluxonFsTransferScanResultWire,
    direct_files_only_batches: Vec<fluxon_fs_core::config::FluxonFsTransferScanBatchWire>,
    full_dir_batches: Vec<fluxon_fs_core::config::FluxonFsTransferScanBatchWire>,
    child_scan_units: Vec<fluxon_fs_core::config::FluxonFsTransferScanChildUnitWire>,
) -> FluxonFsTransferScanResultWire {
    FluxonFsTransferScanResultWire {
        job_id: result.job_id.clone(),
        scan_epoch: result.scan_epoch,
        scan_unit_id: result.scan_unit_id.clone(),
        scan_task_id: result.scan_task_id.clone(),
        root_relpath: result.root_relpath.clone(),
        generation: result.generation,
        frontier: fluxon_fs_core::config::FluxonFsTransferScanFrontier {
            direct_files: Vec::new(),
            direct_dirs: Vec::new(),
            empty_dirs: Vec::new(),
        },
        direct_files_only_batches,
        child_scan_units,
        full_dir_batches,
        finished: result.finished,
    }
}

fn split_scan_store_apply_chunks(
    result: &FluxonFsTransferScanResultWire,
    keep_last_chunk_for_final_apply: bool,
) -> (
    Vec<FluxonFsTransferScanResultWire>,
    Option<FluxonFsTransferScanResultWire>,
) {
    if transfer_scan_result_total_batch_count(result) == 0 {
        if keep_last_chunk_for_final_apply {
            return (Vec::new(), Some(result.clone()));
        }
        return (vec![result.clone()], None);
    }

    let mut chunks = Vec::new();
    for direct_chunk in result
        .direct_files_only_batches
        .chunks(TRANSFER_SCAN_STORE_BATCH_CHUNK_LIMIT)
    {
        chunks.push(build_scan_store_chunk(
            result,
            direct_chunk.to_vec(),
            Vec::new(),
            Vec::new(),
        ));
    }
    for full_chunk in result
        .full_dir_batches
        .chunks(TRANSFER_SCAN_STORE_BATCH_CHUNK_LIMIT)
    {
        chunks.push(build_scan_store_chunk(
            result,
            Vec::new(),
            full_chunk.to_vec(),
            Vec::new(),
        ));
    }

    if !keep_last_chunk_for_final_apply {
        return (chunks, None);
    }

    let final_chunk = chunks.pop().map(|mut chunk| {
        chunk.child_scan_units = result.child_scan_units.clone();
        chunk
    });
    (chunks, final_chunk)
}

// GatewayState is the transfer control-plane facade. It validates request-level
// arguments, forwards durable mutations into TransferStateStore, and wakes the
// local scheduler actor. It is not the durable authority by itself.
impl GatewayState {
    fn transfer_state_store_enabled(
        &self,
    ) -> Result<&Arc<dyn super::types::TransferStateStore>, String> {
        self.transfer_state_store.as_ref().ok_or_else(|| {
            "transfer feature is disabled because transfer_state_store is not configured"
                .to_string()
        })
    }

    pub fn create_transfer_job(
        &self,
        arg: FsTransferCreateJobArg,
    ) -> Result<FsTransferJobRecord, String> {
        let transfer_state_store = self.transfer_state_store_enabled()?;
        let now = Utc::now().timestamp_millis();
        let job_id = uuid::Uuid::new_v4().to_string();
        if arg.src_export.trim().is_empty() {
            return Err("src_export must be non-empty".to_string());
        }
        if arg.dst_export.trim().is_empty() {
            return Err("dst_export must be non-empty".to_string());
        }
        if arg.desired_scan_concurrency <= 0 {
            return Err("desired_scan_concurrency must be > 0".to_string());
        }
        let job = FsTransferJobRecord {
            job_id: job_id.clone(),
            src_export: arg.src_export,
            src_root_relpath: normalize_transfer_root_relpath(arg.src_root_relpath.as_str())?,
            dst_export: arg.dst_export,
            dst_root_relpath: normalize_transfer_root_relpath(arg.dst_root_relpath.as_str())?,
            desired_scan_concurrency: arg.desired_scan_concurrency,
            desired_worker_count: arg.desired_worker_count,
            batch_ready_bytes: arg.batch_ready_bytes,
            job_spec_blob: arg.job_spec_blob,
            scan_epoch: 1,
            scan_finished: false,
            scan_discovered_batch_count: 0,
            scan_discovered_file_count: 0,
            scan_discovered_bytes: 0,
            ready_batch_count: 0,
            running_batch_count: 0,
            done_batch_count: 0,
            finished_batch_count: 0,
            expired_batch_count: 0,
            failed_file_count: 0,
            state: fluxon_fs_core::config::FluxonFsTransferJobState::Running,
            last_error: String::new(),
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
        };
        transfer_state_store.insert_transfer_job(&job)?;
        self.transfer_history_record_job_meta(
            job.job_id.as_str(),
            job.src_export.as_str(),
            job.dst_export.as_str(),
        );
        self.transfer_scan_scheduler.notify();
        self.transfer_worker_scheduler.notify();
        Ok(job)
    }

    pub fn update_transfer_job_desired_concurrency(
        &self,
        job_id: &str,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
    ) -> Result<(), String> {
        if desired_scan_concurrency <= 0 {
            return Err("desired_scan_concurrency must be > 0".to_string());
        }
        if desired_worker_count < 0 {
            return Err("desired_worker_count must be >= 0".to_string());
        }
        self.transfer_state_store_enabled()?
            .update_transfer_job_desired_concurrency(
                job_id,
                desired_scan_concurrency,
                desired_worker_count,
            )?;
        self.transfer_scan_scheduler.notify();
        self.transfer_worker_scheduler.notify();
        Ok(())
    }

    pub fn cancel_transfer_job(&self, job_id: &str) -> Result<(), String> {
        let now_unix_ms = Utc::now().timestamp_millis();
        self.transfer_state_store_enabled()?
            .cancel_transfer_job(job_id, now_unix_ms)?;
        self.note_transfer_job_cancelled(job_id, now_unix_ms);
        self.transfer_scan_scheduler.notify();
        self.transfer_worker_scheduler.notify();
        Ok(())
    }

    pub fn update_transfer_job_desired_worker_count(
        &self,
        job_id: &str,
        desired_worker_count: i64,
    ) -> Result<(), String> {
        self.transfer_state_store_enabled()?
            .update_transfer_job_desired_worker_count(job_id, desired_worker_count)?;
        self.transfer_worker_scheduler.notify();
        Ok(())
    }

    pub fn import_transfer_prescan_job(
        &self,
        job_id: &str,
        src_export: &str,
        src_root_relpath: &str,
        dst_export: &str,
        dst_root_relpath: &str,
        desired_scan_concurrency: i64,
        desired_worker_count: i64,
    ) -> Result<FsTransferJobRecord, String> {
        if desired_scan_concurrency <= 0 {
            return Err("desired_scan_concurrency must be > 0".to_string());
        }
        let job = self
            .transfer_state_store_enabled()?
            .import_transfer_prescan_job(
                job_id,
                src_export,
                src_root_relpath,
                dst_export,
                dst_root_relpath,
                desired_scan_concurrency,
                desired_worker_count,
            )?;
        self.transfer_history_record_job_meta(
            job.job_id.as_str(),
            job.src_export.as_str(),
            job.dst_export.as_str(),
        );
        self.transfer_scan_scheduler.notify();
        self.transfer_worker_scheduler.notify();
        Ok(job)
    }

    pub fn list_transfer_job_snapshots(&self) -> Result<Vec<FsTransferJobSnapshot>, String> {
        let mut snapshots = self
            .transfer_state_store_enabled()?
            .load_transfer_job_snapshots()?;
        for snapshot in &mut snapshots {
            self.transfer_history_record_job_meta(
                snapshot.job.job_id.as_str(),
                snapshot.job.src_export.as_str(),
                snapshot.job.dst_export.as_str(),
            );
            let current_running_batch_owner_by_batch_id = snapshot
                .running_batches
                .iter()
                .map(|batch| (batch.batch_id.clone(), batch.owner_worker_task_id.clone()))
                .collect::<BTreeMap<_, _>>();
            let mut live_detail = self
                .transfer_job_live_detail_snapshot(
                    snapshot.job.job_id.as_str(),
                    &current_running_batch_owner_by_batch_id,
                )
                .unwrap_or(super::types::FsTransferJobLiveDetailSnapshot {
                    scan: super::types::FsTransferScanLiveDetailSnapshot {
                        queued_scan_unit_count: 0,
                        inflight_scan_unit_count: 0,
                        completed_scan_unit_count: 0,
                        discovered_batch_count: 0,
                        discovered_file_count: 0,
                        discovered_bytes: 0,
                        scan_rate_files_per_sec: 0,
                        scan_rate_bytes_per_sec: 0,
                        last_scan_result_unix_ms: 0,
                    },
                    workers: super::types::FsTransferWorkerAggregateLiveDetailSnapshot {
                        launching_worker_count: 0,
                        running_worker_count: 0,
                        stopped_worker_count: 0,
                        finished_worker_count: 0,
                        writing_batch_count: 0,
                        aggregate_visible_file_count: 0,
                        aggregate_visible_bytes: 0,
                        aggregate_live_bandwidth_bytes_per_sec: 0,
                        aggregate_total_written_bytes: 0,
                    },
                    recent_failures: Vec::new(),
                    active_workers: Vec::new(),
                });
            live_detail.scan.discovered_batch_count =
                snapshot.job.scan_discovered_batch_count.max(0);
            live_detail.scan.discovered_file_count = snapshot.job.scan_discovered_file_count.max(0);
            live_detail.scan.discovered_bytes = snapshot.job.scan_discovered_bytes.max(0);
            snapshot.live_detail = Some(live_detail);
        }
        Ok(snapshots)
    }

    pub fn transfer_job_snapshot(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferJobSnapshot>, String> {
        let Some(mut snapshot) = self
            .transfer_state_store_enabled()?
            .load_transfer_job_snapshot(job_id)?
        else {
            return Ok(None);
        };
        self.transfer_history_record_job_meta(
            snapshot.job.job_id.as_str(),
            snapshot.job.src_export.as_str(),
            snapshot.job.dst_export.as_str(),
        );
        let current_running_batch_owner_by_batch_id = snapshot
            .running_batches
            .iter()
            .map(|batch| (batch.batch_id.clone(), batch.owner_worker_task_id.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut live_detail = self
            .transfer_job_live_detail_snapshot(
                snapshot.job.job_id.as_str(),
                &current_running_batch_owner_by_batch_id,
            )
            .unwrap_or(super::types::FsTransferJobLiveDetailSnapshot {
                scan: super::types::FsTransferScanLiveDetailSnapshot {
                    queued_scan_unit_count: 0,
                    inflight_scan_unit_count: 0,
                    completed_scan_unit_count: 0,
                    discovered_batch_count: 0,
                    discovered_file_count: 0,
                    discovered_bytes: 0,
                    scan_rate_files_per_sec: 0,
                    scan_rate_bytes_per_sec: 0,
                    last_scan_result_unix_ms: 0,
                },
                workers: super::types::FsTransferWorkerAggregateLiveDetailSnapshot {
                    launching_worker_count: 0,
                    running_worker_count: 0,
                    stopped_worker_count: 0,
                    finished_worker_count: 0,
                    writing_batch_count: 0,
                    aggregate_visible_file_count: 0,
                    aggregate_visible_bytes: 0,
                    aggregate_live_bandwidth_bytes_per_sec: 0,
                    aggregate_total_written_bytes: 0,
                },
                recent_failures: Vec::new(),
                active_workers: Vec::new(),
            });
        live_detail.scan.discovered_batch_count = snapshot.job.scan_discovered_batch_count.max(0);
        live_detail.scan.discovered_file_count = snapshot.job.scan_discovered_file_count.max(0);
        live_detail.scan.discovered_bytes = snapshot.job.scan_discovered_bytes.max(0);
        snapshot.live_detail = Some(live_detail);
        Ok(Some(snapshot))
    }

    pub fn transfer_scheduler_job_snapshot(
        &self,
        job_id: &str,
    ) -> Result<Option<FsTransferSchedulerJobSnapshot>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_scheduler_job_snapshot(job_id)
    }

    pub fn transfer_batch_record(
        &self,
        job_id: &str,
        batch_id: &str,
    ) -> Result<Option<super::types::FsTransferBatchRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_batch_record(job_id, batch_id)
    }

    pub fn transfer_job_record(&self, job_id: &str) -> Result<Option<FsTransferJobRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_job_record(job_id)
    }

    pub fn list_transfer_job_summaries(&self) -> Result<Vec<FsTransferJobSummarySnapshot>, String> {
        let mut summaries = self
            .transfer_state_store_enabled()?
            .load_transfer_job_summary_snapshots()?;
        for summary in &mut summaries {
            let current_running_batch_owner_by_batch_id = summary
                .running_batch_owners
                .iter()
                .map(|owner| (owner.batch_id.clone(), owner.owner_worker_task_id.clone()))
                .collect::<BTreeMap<_, _>>();
            let mut live_detail = self
                .transfer_job_live_detail_snapshot(
                    summary.job.job_id.as_str(),
                    &current_running_batch_owner_by_batch_id,
                )
                .unwrap_or(super::types::FsTransferJobLiveDetailSnapshot {
                    scan: super::types::FsTransferScanLiveDetailSnapshot {
                        queued_scan_unit_count: 0,
                        inflight_scan_unit_count: 0,
                        completed_scan_unit_count: 0,
                        discovered_batch_count: 0,
                        discovered_file_count: 0,
                        discovered_bytes: 0,
                        scan_rate_files_per_sec: 0,
                        scan_rate_bytes_per_sec: 0,
                        last_scan_result_unix_ms: 0,
                    },
                    workers: super::types::FsTransferWorkerAggregateLiveDetailSnapshot {
                        launching_worker_count: 0,
                        running_worker_count: 0,
                        stopped_worker_count: 0,
                        finished_worker_count: 0,
                        writing_batch_count: 0,
                        aggregate_visible_file_count: 0,
                        aggregate_visible_bytes: 0,
                        aggregate_live_bandwidth_bytes_per_sec: 0,
                        aggregate_total_written_bytes: 0,
                    },
                    recent_failures: Vec::new(),
                    active_workers: Vec::new(),
                });
            live_detail.scan.discovered_batch_count =
                summary.job.scan_discovered_batch_count.max(0);
            live_detail.scan.discovered_file_count = summary.job.scan_discovered_file_count.max(0);
            live_detail.scan.discovered_bytes = summary.job.scan_discovered_bytes.max(0);
            summary.live_detail = Some(live_detail);
        }
        Ok(summaries)
    }

    pub fn transfer_job_recent_failure_detail(
        &self,
        job_id: &str,
        failure_index: i64,
    ) -> Result<Option<FsTransferRecentFailureSnapshot>, String> {
        let Some(snapshot) = self.transfer_job_snapshot(job_id)? else {
            return Ok(None);
        };
        Ok(snapshot.live_detail.and_then(|detail| {
            detail
                .recent_failures
                .into_iter()
                .find(|failure| failure.failure_index == failure_index)
        }))
    }

    pub fn transfer_job_file_issues(
        &self,
        job_id: &str,
    ) -> Result<Vec<FsTransferBatchFileIssueRecord>, String> {
        let mut items = self
            .list_transfer_batch_file_issues()?
            .into_iter()
            .filter(|row| row.job_id == job_id)
            .collect::<Vec<_>>();
        items.sort_by(|a, b| {
            a.batch_id
                .cmp(&b.batch_id)
                .then(a.relpath.cmp(&b.relpath))
                .then(a.created_at_unix_ms.cmp(&b.created_at_unix_ms))
        });
        Ok(items)
    }

    pub fn transfer_job_file_issue_detail(
        &self,
        job_id: &str,
        batch_id: &str,
        relpath: &str,
    ) -> Result<Option<FsTransferBatchFileIssueRecord>, String> {
        Ok(self
            .transfer_job_file_issues(job_id)?
            .into_iter()
            .find(|row| row.batch_id == batch_id && row.relpath == relpath))
    }

    pub fn transfer_worker_scheduler_notify(&self) {
        self.transfer_worker_scheduler.notify();
    }

    pub fn transfer_worker_scheduler_wait_handle(&self) -> Arc<tokio::sync::Notify> {
        self.transfer_worker_scheduler.wake.clone()
    }

    pub fn transfer_scan_scheduler_notify(&self) {
        self.transfer_scan_scheduler.notify();
    }

    pub fn transfer_scan_scheduler_wait_handle(&self) -> Arc<tokio::sync::Notify> {
        self.transfer_scan_scheduler.wake.clone()
    }

    pub fn list_running_transfer_jobs(&self) -> Result<Vec<FsTransferJobRecord>, String> {
        let mut jobs = self
            .transfer_state_store_enabled()?
            .load_transfer_job_records()?;
        jobs.retain(|job| job.state == FluxonFsTransferJobState::Running);
        Ok(jobs)
    }

    pub fn list_transfer_batches(
        &self,
    ) -> Result<Vec<super::types::FsTransferBatchRecord>, String> {
        self.transfer_state_store_enabled()?.load_transfer_batches()
    }

    pub fn list_transfer_batches_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<super::types::FsTransferBatchRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_batches_for_job(job_id)
    }

    pub fn list_transfer_direct_files_complete_records(
        &self,
    ) -> Result<Vec<super::types::FsTransferDirectFilesCompleteRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_direct_files_complete_records()
    }

    pub fn list_transfer_direct_files_complete_records_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<super::types::FsTransferDirectFilesCompleteRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_direct_files_complete_records_for_job(job_id)
    }

    pub fn list_transfer_worker_attempt_records(
        &self,
    ) -> Result<Vec<super::types::FsTransferWorkerAttemptRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_worker_attempt_records()
    }

    pub fn list_transfer_batch_collect_infos(
        &self,
    ) -> Result<Vec<FsTransferBatchCollectInfoRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_batch_collect_info_records()
    }

    pub fn list_transfer_batch_file_issues(
        &self,
    ) -> Result<Vec<FsTransferBatchFileIssueRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_transfer_batch_file_issue_records()
    }

    pub fn load_next_ready_transfer_batch_for_job(
        &self,
        job_id: &str,
        batch_class: FsTransferReadyBatchClass,
    ) -> Result<Option<FsTransferReadyBatchDispatch>, String> {
        self.transfer_state_store_enabled()?
            .load_next_ready_transfer_batch_for_job(job_id, batch_class)
    }

    pub fn list_transfer_ready_batches_for_job(
        &self,
        job_id: &str,
    ) -> Result<Vec<super::types::FsTransferBatchRecord>, String> {
        self.transfer_state_store_enabled()?
            .load_ready_transfer_batches_for_job(job_id)
    }

    pub fn apply_transfer_scan_append(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        let transfer_state_store = self.transfer_state_store_enabled()?;
        let (chunks, _) = split_scan_store_apply_chunks(result, false);
        for chunk in chunks {
            transfer_state_store.apply_transfer_scan_append(&chunk)?;
        }
        Ok(())
    }

    pub fn finish_transfer_scan_unit(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        // Finished payloads are durably applied when the master accepts the
        // Finished event. Finalize only clears the master-side pending barrier.
        self.transfer_state_store_enabled()?
            .finish_transfer_scan_unit(result)
    }

    pub fn apply_transfer_scan_result(
        &self,
        result: &FluxonFsTransferScanResultWire,
    ) -> Result<(), String> {
        let transfer_state_store = self.transfer_state_store_enabled()?;
        let (prefix_chunks, final_chunk) = split_scan_store_apply_chunks(result, true);
        for chunk in prefix_chunks {
            transfer_state_store.apply_transfer_scan_append(&chunk)?;
        }
        if let Some(chunk) = final_chunk {
            transfer_state_store.apply_transfer_scan_result(&chunk)?;
        }
        Ok(())
    }

    pub fn begin_transfer_scan_epoch(&self, job_id: &str) -> Result<i64, String> {
        let epoch = self
            .transfer_state_store_enabled()?
            .begin_transfer_scan_epoch(job_id)?;
        self.note_transfer_scan_epoch_started(job_id);
        self.transfer_scan_scheduler.notify();
        Ok(epoch)
    }

    pub fn finish_transfer_scan_epoch(&self, job_id: &str, scan_epoch: i64) -> Result<(), String> {
        self.transfer_state_store_enabled()?
            .finish_transfer_scan_epoch(job_id, scan_epoch)?;
        self.transfer_scan_scheduler.notify();
        self.transfer_worker_scheduler.notify();
        Ok(())
    }

    pub fn assign_transfer_batch_to_worker(
        &self,
        job_id: &str,
        batch_id: &str,
        src_exporter_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        dst_exporter_id: &str,
        lease_expire_unix_ms: i64,
    ) -> Result<(), String> {
        self.transfer_state_store_enabled()?
            .assign_transfer_batch_to_worker(
                job_id,
                batch_id,
                src_exporter_id,
                worker_id,
                worker_task_id,
                dst_exporter_id,
                lease_expire_unix_ms,
            )?;
        self.note_transfer_batch_assigned(
            job_id,
            batch_id,
            worker_id,
            worker_task_id,
            lease_expire_unix_ms,
        );
        self.transfer_worker_scheduler.notify();
        Ok(())
    }

    pub fn record_transfer_worker_launch_retry(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        self.transfer_state_store_enabled()?
            .record_transfer_worker_launch_retry(
                job_id,
                batch_id,
                worker_id,
                worker_task_id,
                err_text,
                now_unix_ms,
            )?;
        self.note_transfer_worker_launch_retry(job_id, worker_task_id, now_unix_ms, err_text);
        self.transfer_worker_scheduler.notify();
        Ok(())
    }

    pub fn mark_transfer_worker_launch_acknowledged(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        self.transfer_state_store_enabled()?
            .mark_transfer_worker_launch_acknowledged(
                job_id,
                batch_id,
                worker_id,
                worker_task_id,
                now_unix_ms,
            )?;
        self.note_transfer_worker_launch_acknowledged(job_id, worker_task_id);
        self.transfer_worker_scheduler.notify();
        Ok(())
    }

    pub fn mark_transfer_worker_attempt_stopped(
        &self,
        job_id: &str,
        batch_id: &str,
        worker_id: &str,
        worker_task_id: &str,
        stop_reason: Option<FluxonFsTransferWorkerStopReasonWire>,
        err_text: &str,
        now_unix_ms: i64,
    ) -> Result<(), String> {
        self.transfer_state_store_enabled()?
            .mark_transfer_worker_attempt_stopped(
                job_id,
                batch_id,
                worker_id,
                worker_task_id,
                stop_reason,
                err_text,
                now_unix_ms,
            )?;
        self.note_transfer_worker_stopped(
            job_id,
            worker_task_id,
            now_unix_ms,
            stop_reason,
            err_text,
        );
        self.transfer_worker_scheduler.notify();
        Ok(())
    }

    pub fn apply_transfer_worker_result(
        &self,
        result: &FluxonFsTransferWorkerResultWire,
    ) -> Result<FluxonFsTransferWorkerResultAckWire, String> {
        let visible_file_count = result.file_results.len() as i64;
        let visible_bytes = result
            .file_results
            .iter()
            .fold(0_i64, |acc, row| acc.saturating_add(row.visible_size));
        let final_telemetry = result
            .final_telemetry
            .as_ref()
            .map(super::types::FsTransferWorkerHeartbeatLiveTelemetry::from_wire);
        let decision = self
            .transfer_state_store_enabled()?
            .apply_transfer_worker_result(result)?;
        if decision.accepted {
            self.note_transfer_worker_result_applied(
                result.job_id.as_str(),
                result.worker_task_id.as_str(),
                visible_file_count,
                visible_bytes,
                final_telemetry.as_ref(),
            );
            self.transfer_worker_scheduler.notify();
        }
        Ok(decision)
    }

    pub fn apply_transfer_worker_heartbeat(
        &self,
        heartbeat: &fluxon_fs_core::config::FluxonFsTransferWorkerHeartbeatWire,
        heartbeat_received_unix_ms: i64,
        heartbeat_lease_duration_ms: i64,
    ) -> Result<FluxonFsTransferWorkerHeartbeatResultWire, String> {
        let lease_expire_unix_ms =
            heartbeat_received_unix_ms.saturating_add(heartbeat_lease_duration_ms.max(0));
        let telemetry = heartbeat
            .telemetry
            .as_ref()
            .map(super::types::FsTransferWorkerHeartbeatLiveTelemetry::from_wire);
        let decision = self
            .transfer_state_store_enabled()?
            .apply_transfer_worker_heartbeat(
                heartbeat,
                heartbeat_received_unix_ms,
                heartbeat_lease_duration_ms,
            )?;
        if decision.continue_running {
            self.note_transfer_worker_heartbeat(
                heartbeat.job_id.as_str(),
                heartbeat.worker_task_id.as_str(),
                heartbeat_received_unix_ms,
                lease_expire_unix_ms,
                telemetry.as_ref(),
            );
            self.transfer_worker_scheduler.notify();
        } else {
            self.note_transfer_worker_stopped(
                heartbeat.job_id.as_str(),
                heartbeat.worker_task_id.as_str(),
                heartbeat_received_unix_ms,
                decision.stop_reason,
                "worker heartbeat told worker to stop",
            );
        }
        Ok(decision)
    }

    pub fn reconcile_transfer_scheduler_state(&self, now_unix_ms: i64) -> Result<(), String> {
        let handle = self.transfer_reconcile_handle.as_ref().ok_or_else(|| {
            "transfer feature is disabled because transfer_state_store is not configured"
                .to_string()
        })?;
        handle.reconcile_transfer_scheduler_state_blocking(
            self.backend.clone(),
            self.fs_cache.clone(),
            now_unix_ms,
        )?;
        Ok(())
    }
}
