use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use parking_lot::Mutex;
use tracing_appender::non_blocking;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Registry;
use tracing_subscriber::filter::FilterExt;
use tracing_subscriber::fmt::time::FormatTime;
use tracing_subscriber::{filter, prelude::*}; // layering

// Build-time generated workspace crate list
mod generated_crates {
    include!(concat!(env!("OUT_DIR"), "/our_crates.rs"));
}

// These RDMA transfer crates carry the runtime evidence we need when diagnosing whether
// RPC fast-path traffic actually entered the closed transfer / verbs backend. Keep the scope explicit:
// only these dependency targets are promoted to DEBUG alongside workspace crates.
const RDMA_DEBUG_TARGETS: &[&str] = &["fabric_lib", "libfabric_sys", "libibverbs_sys"];
const LOG_RETENTION_DAYS: usize = 31;
const TEST_LOG_SHARD_WINDOW_SECONDS_ENV: &str = "FLUXON_TEST_LOG_SHARD_WINDOW_SECONDS";
const TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV: &str = "FLUXON_TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS";

// Simple UTC timer in RFC3339 seconds (no subsecond precision)
struct UtcSecondTimer;
impl FormatTime for UtcSecondTimer {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        let now = chrono::Utc::now();
        write!(w, "{}", now.format("%Y-%m-%dT%H:%M:%SZ"))
    }
}

// Keep guards alive for the whole process lifetime to flush non-blocking writers.
static GLOBAL_FILE_LOG_GUARD: OnceLock<WorkerGuard> = OnceLock::new();
static GLOBAL_CONSOLE_LOG_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

// Expose the current process log file path for sidecar collectors (e.g. OTLP tailer).
static GLOBAL_LOG_FILE_PATH: OnceLock<PathBuf> = OnceLock::new();

pub const DEFAULT_DAILY_LOG_RETENTION_DAYS: usize = LOG_RETENTION_DAYS;

#[derive(Clone, Copy, Debug)]
struct LogShardWindowConfig {
    window_seconds: i64,
    anchor_unix_seconds: i64,
}

fn read_test_log_shard_window_config() -> anyhow::Result<Option<LogShardWindowConfig>> {
    let Some(raw_window) = std::env::var_os(TEST_LOG_SHARD_WINDOW_SECONDS_ENV) else {
        return Ok(None);
    };
    let raw_window = raw_window
        .into_string()
        .map_err(|_| anyhow::anyhow!("{TEST_LOG_SHARD_WINDOW_SECONDS_ENV} must be valid utf-8"))?;
    let window_text = raw_window.trim();
    if window_text.is_empty() {
        return Ok(None);
    }
    let window_seconds: i64 = window_text.parse().map_err(|e| {
        anyhow::anyhow!(
            "{TEST_LOG_SHARD_WINDOW_SECONDS_ENV} must be a positive integer: {e}"
        )
    })?;
    if window_seconds <= 0 {
        anyhow::bail!("{TEST_LOG_SHARD_WINDOW_SECONDS_ENV} must be > 0");
    }

    let raw_anchor = std::env::var(TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV).map_err(|_| {
        anyhow::anyhow!(
            "{TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV} is required when {TEST_LOG_SHARD_WINDOW_SECONDS_ENV} is set"
        )
    })?;
    let anchor_unix_seconds: i64 = raw_anchor.trim().parse().map_err(|e| {
        anyhow::anyhow!(
            "{TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV} must be an integer unix timestamp: {e}"
        )
    })?;
    Ok(Some(LogShardWindowConfig {
        window_seconds,
        anchor_unix_seconds,
    }))
}

fn resolve_shard_date_from_datetime(now: chrono::DateTime<chrono::Utc>) -> anyhow::Result<chrono::NaiveDate> {
    let Some(config) = read_test_log_shard_window_config()? else {
        return Ok(now.date_naive());
    };
    let unix_seconds = now.timestamp();
    let delta_seconds = unix_seconds - config.anchor_unix_seconds;
    if delta_seconds < 0 {
        anyhow::bail!(
            "test log shard anchor must not be in the future: anchor={}, ts={}",
            config.anchor_unix_seconds,
            unix_seconds
        );
    }
    let bucket_index = delta_seconds / config.window_seconds;
    let base_date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .expect("valid hard-coded synthetic base date");
    Ok(base_date + chrono::Days::new(bucket_index as u64))
}

fn current_shard_date() -> anyhow::Result<chrono::NaiveDate> {
    resolve_shard_date_from_datetime(chrono::Utc::now())
}

fn cleanup_old_daily_sharded_logs(
    base_path: &Path,
    retention_days: usize,
) -> anyhow::Result<()> {
    let parent = match base_path.parent() {
        Some(parent) => parent,
        None => return Ok(()),
    };
    let file_name = match base_path.file_name().and_then(|v| v.to_str()) {
        Some(file_name) => file_name,
        None => return Ok(()),
    };
    let Some(stem) = file_name.strip_suffix(".log") else {
        return Ok(());
    };
    fs::create_dir_all(parent)?;
    let keep_since = current_shard_date()? - chrono::Days::new(retention_days.saturating_sub(1) as u64);
    let prefix = format!("{stem}.");
    for entry in std::fs::read_dir(parent)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let entry_name = entry.file_name();
        let Some(entry_name) = entry_name.to_str() else {
            continue;
        };
        if !entry_name.starts_with(prefix.as_str()) || !entry_name.ends_with(".log") {
            continue;
        }
        let date_text = &entry_name[prefix.len()..entry_name.len() - ".log".len()];
        let Ok(shard_date) = chrono::NaiveDate::parse_from_str(date_text, "%Y-%m-%d") else {
            continue;
        };
        if shard_date < keep_since {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(err.into()),
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct DailyShardedFileWriter {
    base_path: PathBuf,
    retention_days: usize,
    state: Mutex<DailyShardedFileWriterState>,
}

#[derive(Debug, Default)]
struct DailyShardedFileWriterState {
    current_path: Option<PathBuf>,
    current_file: Option<fs::File>,
}

impl DailyShardedFileWriter {
    fn new(base_path: PathBuf, retention_days: usize) -> Self {
        Self {
            base_path,
            retention_days,
            state: Mutex::new(DailyShardedFileWriterState::default()),
        }
    }

    fn current_path(&self) -> anyhow::Result<PathBuf> {
        current_daily_sharded_log_path(&self.base_path)
    }

    fn rotate_if_needed(
        &self,
        state: &mut DailyShardedFileWriterState,
    ) -> io::Result<()> {
        let next_path = self
            .current_path()
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        if state.current_path.as_ref() == Some(&next_path) && state.current_file.is_some() {
            return Ok(());
        }
        cleanup_old_daily_sharded_logs(&self.base_path, self.retention_days)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        if let Some(parent) = next_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&next_path)?;
        state.current_path = Some(next_path);
        state.current_file = Some(file);
        Ok(())
    }
}

impl io::Write for DailyShardedFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self.state.lock();
        self.rotate_if_needed(&mut state)?;
        state
            .current_file
            .as_mut()
            .expect("log writer file must exist after rotation")
            .write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self.state.lock();
        if let Some(file) = state.current_file.as_mut() {
            file.flush()
        } else {
            Ok(())
        }
    }
}

fn setup_global_log_guards(file_guard: WorkerGuard, console_guard: WorkerGuard) {
    let _ = GLOBAL_FILE_LOG_GUARD.set(file_guard);
    let _ = GLOBAL_CONSOLE_LOG_GUARD.set(console_guard);
}

fn workspace_targets_filter(
    workspace_level: filter::LevelFilter,
    non_workspace_default: filter::LevelFilter,
) -> filter::Targets {
    let mut targets = filter::Targets::new().with_default(non_workspace_default);
    for c in generated_crates::OUR_CRATES {
        targets = targets.with_target(*c, workspace_level);
    }
    for c in RDMA_DEBUG_TARGETS {
        targets = targets.with_target(*c, workspace_level);
    }
    targets
}

fn third_party_log_target_overrides(
    enable_iceoryx_logs: bool,
    default_level: filter::LevelFilter,
) -> filter::Targets {
    let mut targets = filter::Targets::new().with_default(default_level);
    if enable_iceoryx_logs {
        // Narrow scope: only lift suppression for iceoryx2 crates so benchmark logs stay readable.
        for target in [
            "iceoryx2",
            "iceoryx2-bb-concurrency",
            "iceoryx2-bb-container",
            "iceoryx2-bb-derive-macros",
            "iceoryx2-bb-elementary",
            "iceoryx2-bb-elementary-traits",
            "iceoryx2-bb-linux",
            "iceoryx2-bb-lock-free",
            "iceoryx2-bb-memory",
            "iceoryx2-bb-posix",
            "iceoryx2-bb-system-types",
            "iceoryx2-cal",
            "iceoryx2-log",
            "iceoryx2-log-types",
            "iceoryx2-loggers",
            "iceoryx2-pal-concurrency-sync",
            "iceoryx2-pal-configuration",
            "iceoryx2-pal-os-api",
            "iceoryx2-pal-posix",
        ] {
            targets = targets.with_target(target, filter::LevelFilter::DEBUG);
        }
    }
    targets
}

/// Init log for production.
/// - `log_path`: directory to write log files
/// - `instance_key`: used in daily file names to disambiguate instances
pub fn init_log(log_path: &Path, instance_key: &str) {
    init_log_impl(log_path, instance_key, NoopLayer);
}

/// Init log for production, with an extra `tracing_subscriber::Layer` installed.
///
/// This is used by infrastructure crates (e.g. observability) to attach additional sinks
/// such as OTLP log exporters, without introducing crate dependency cycles.
pub fn init_log_with_extra_layer<L>(log_path: &Path, instance_key: &str, extra_layer: L)
where
    L: tracing_subscriber::Layer<Registry> + Send + Sync + 'static,
{
    init_log_impl(log_path, instance_key, extra_layer);
}

#[derive(Clone, Copy, Debug, Default)]
struct NoopLayer;

impl<S> tracing_subscriber::Layer<S> for NoopLayer where S: tracing::Subscriber {}

fn current_daily_log_file_path(log_path: &Path, instance_key: &str) -> PathBuf {
    current_daily_sharded_log_path(&log_path.join(format!("fluxon-kv-{instance_key}.log")))
        .unwrap_or_else(|_| {
            let date = chrono::Utc::now().format("%Y-%m-%d");
            log_path.join(format!("fluxon-kv-{instance_key}.{date}.log"))
        })
}

pub fn daily_sharded_log_path(
    base_path: &Path,
    date: chrono::NaiveDate,
) -> anyhow::Result<PathBuf> {
    let file_name = base_path.file_name().and_then(|v| v.to_str()).ok_or_else(|| {
        anyhow::anyhow!(
            "log path must end with a valid utf-8 filename: {}",
            base_path.display()
        )
    })?;
    let stem = file_name
        .strip_suffix(".log")
        .ok_or_else(|| anyhow::anyhow!("log path must end with .log: {}", base_path.display()))?;
    Ok(base_path.with_file_name(format!(
        "{}.{}.log",
        stem,
        date.format("%Y-%m-%d")
    )))
}

pub fn current_daily_sharded_log_path(base_path: &Path) -> anyhow::Result<PathBuf> {
    daily_sharded_log_path(base_path, current_shard_date()?)
}

pub fn latest_existing_daily_sharded_log_path(base_path: &Path) -> Option<PathBuf> {
    let parent = base_path.parent()?;
    let file_name = base_path.file_name()?.to_str()?;
    let stem = file_name.strip_suffix(".log")?;
    let prefix = format!("{}.", stem);
    let mut latest: Option<(chrono::NaiveDate, PathBuf)> = None;
    let entries = std::fs::read_dir(parent).ok()?;
    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let entry_name = entry.file_name();
        let Some(entry_name) = entry_name.to_str() else {
            continue;
        };
        if !entry_name.starts_with(prefix.as_str()) || !entry_name.ends_with(".log") {
            continue;
        }
        if entry_name.len() <= prefix.len() + ".log".len() {
            continue;
        }
        let date_text = &entry_name[prefix.len()..entry_name.len() - ".log".len()];
        let Ok(date) = chrono::NaiveDate::parse_from_str(date_text, "%Y-%m-%d") else {
            continue;
        };
        let replace = match latest.as_ref() {
            Some((prev, _)) => date > *prev,
            None => true,
        };
        if replace {
            latest = Some((date, path));
        }
    }
    latest.map(|(_, path)| path)
}

pub fn resolve_readable_log_path(base_path: &Path) -> Option<PathBuf> {
    if let Ok(current) = current_daily_sharded_log_path(base_path) {
        if current.exists() {
            return Some(current);
        }
    }
    if let Some(latest) = latest_existing_daily_sharded_log_path(base_path) {
        return Some(latest);
    }
    if base_path.exists() {
        return Some(base_path.to_path_buf());
    }
    None
}

pub fn display_runtime_log_path(base_path_text: &str) -> String {
    let base_path = Path::new(base_path_text);
    resolve_readable_log_path(base_path)
        .unwrap_or_else(|| base_path.to_path_buf())
        .display()
        .to_string()
}

fn init_log_impl<L>(log_path: &Path, instance_key: &str, extra_layer: L)
where
    L: tracing_subscriber::Layer<Registry> + Send + Sync + 'static,
{
    // Sync FLUXON_LOG <-> RUST_LOG, prefer FLUXON_LOG when both provided.
    let fluxon = std::env::var("FLUXON_LOG").ok();
    let rust_log = std::env::var("RUST_LOG").ok();
    match (fluxon.as_deref(), rust_log.as_deref()) {
        (Some(f), Some(r)) => {
            if f != r {
                eprintln!(
                    "[fluxon] Both FLUXON_LOG='{f}' and RUST_LOG='{r}' set; using FLUXON_LOG"
                );
            }
            unsafe {
                std::env::set_var("RUST_LOG", f);
                std::env::set_var("FLUXON_LOG", f);
            }
        }
        (Some(f), None) => unsafe {
            std::env::set_var("RUST_LOG", f);
            std::env::set_var("FLUXON_LOG", f);
        },
        (None, Some(r)) => unsafe {
            std::env::set_var("FLUXON_LOG", r);
        },
        (None, None) => unsafe {
            // default startup level; callers can override via env before calling
            let default = "debug";
            std::env::set_var("RUST_LOG", default);
            std::env::set_var("FLUXON_LOG", default);
        },
    }

    // Enable backtrace globally
    unsafe {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    // Bridge `log` crate records to `tracing` so external deps honor RUST_LOG
    let _ = tracing_log::LogTracer::init();

    // If RUST_LOG contains a bare level (e.g. "debug"), add our crate targets with that
    // level. If the bare level is debug, downgrade the bare level to info to reduce noise
    // on console, while keeping per-target debug when explicitly requested.
    let current = std::env::var("RUST_LOG").unwrap_or("debug".to_string());
    {
        let raw = current.trim();
        if !raw.is_empty() {
            use std::collections::HashSet;
            let parts: Vec<String> = raw
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();

            let mut existing_targets: HashSet<String> = HashSet::new();
            for p in &parts {
                if let Some((t, _)) = p.split_once('=') {
                    existing_targets.insert(t.trim().to_string());
                }
            }

            let mut crate_level: Option<String> = None;
            let mut new_parts: Vec<String> = Vec::with_capacity(parts.len());
            let mut removed_bare_debug = false;

            for p in parts.into_iter() {
                if p.contains('=') {
                    new_parts.push(p);
                    continue;
                }
                let lvl = p.to_ascii_lowercase();
                if crate_level.is_none() {
                    crate_level = Some(lvl.clone());
                }
                if lvl == "debug" {
                    removed_bare_debug = true; // replace later with bare info
                } else {
                    new_parts.push(p);
                }
            }

            if removed_bare_debug {
                new_parts.push("info".to_string());
            }

            if let Some(level) = crate_level {
                // Use build-time generated workspace member list.
                let our_crates: &[&str] = generated_crates::OUR_CRATES;
                let mut changed = removed_bare_debug;
                for c in our_crates {
                    if !existing_targets.contains(&c.to_string()) {
                        new_parts.push(format!("{}={}", c, level));
                        changed = true;
                    }
                }
                if changed {
                    let merged = new_parts.join(",");
                    unsafe {
                        std::env::set_var("RUST_LOG", &merged);
                    }
                }
            }
        }
    }

    println!(
        "rust_log env: {}",
        std::env::var("RUST_LOG").unwrap_or("info".to_string())
    );

    // Prepare log directory
    match std::fs::create_dir_all(log_path) {
        Ok(_) => {}
        Err(e) => {
            eprintln!(
                "Failed to create log directory {:?}, err: {:?}",
                log_path, e
            );
            return;
        }
    }

    // File log keeps workspace crates at DEBUG; non-workspace crates default to WARN.
    // This avoids dumping verbose dependency debug logs (e.g. h2/tower) into file output.
    let file_path = current_daily_log_file_path(log_path, instance_key);
    // Keep a copy for the whole process lifetime; collectors can clone it.
    if let Some(prev) = GLOBAL_LOG_FILE_PATH.get() {
        if prev != &file_path {
            eprintln!(
                "[fluxon] init_log called multiple times with different log file paths; keeping the first one: prev={:?}, new={:?}",
                prev, file_path
            );
        }
    } else {
        let _ = GLOBAL_LOG_FILE_PATH.set(file_path.clone());
    }
    let file_appender = DailyShardedFileWriter::new(
        log_path.join(format!("fluxon-kv-{instance_key}.log")),
        LOG_RETENTION_DAYS,
    );
    let (file_writer, file_guard) = non_blocking(file_appender);
    let enable_iceoryx_logs = matches!(
        std::env::var("FLUXON_ENABLE_ICEORYX_LOGS")
            .ok()
            .as_deref()
            .map(|v| v.trim().to_ascii_lowercase()),
        Some(v) if !matches!(v.as_str(), "" | "0" | "false" | "no")
    );
    let file_filter =
        workspace_targets_filter(filter::LevelFilter::DEBUG, filter::LevelFilter::WARN);
    let file_layer = tracing_subscriber::fmt::layer()
        .with_timer(UtcSecondTimer)
        .with_writer(file_writer)
        .with_ansi(false)
        .with_filter(file_filter.clone().or(third_party_log_target_overrides(
            enable_iceoryx_logs,
            filter::LevelFilter::WARN,
        )));

    // Console logging follows user config (RUST_LOG/FLUXON_LOG); file logging ignores it.
    let (console_writer, console_guard) = non_blocking(io::stdout());
    let console_env_filter = EnvFilter::from_default_env();
    let console_layer = tracing_subscriber::fmt::layer()
        .with_writer(console_writer)
        .with_filter(console_env_filter);

    // Register layers.
    // `extra_layer` follows the same target filtering as file output (workspace=DEBUG, deps=WARN)
    // to avoid exporting noisy dependency debug logs by default.
    let extra_layer = extra_layer.with_filter(file_filter.clone().or(
        third_party_log_target_overrides(enable_iceoryx_logs, filter::LevelFilter::WARN),
    ));
    let _ = tracing_subscriber::registry()
        .with(extra_layer)
        .with(file_layer)
        .with(console_layer)
        .with(filter::LevelFilter::DEBUG)
        .try_init();

    // Hold guards globally
    setup_global_log_guards(file_guard, console_guard);

    // Success notice: tell users where logs are written.
    println!(
        "[fluxon] Logging initialized. base_dir={:?}, retention_days={}, current_file={:?}, instance_key='{}'",
        log_path, LOG_RETENTION_DAYS, file_path, instance_key
    );
}

pub fn current_log_file_path() -> Option<PathBuf> {
    GLOBAL_LOG_FILE_PATH.get().cloned()
}

// --- Test helpers ---

fn sanitize_case_name(name: &str) -> String {
    let mut s = name.replace('/', "_").replace('\\', "_").replace(' ', "_");
    if s.is_empty() {
        s = "unnamed_test".to_string();
    }
    s
}

/// Init logging for tests that do not run through run_master/run_client.
/// Writes to: <repo>/log/test_workdir_<unix_ts>/tests/<test_case_name>/
pub fn init_log_test(test_case_name: &str) {
    let case = sanitize_case_name(test_case_name);
    let dir = Path::new(crate::test_util::test_workdir_base())
        .join("tests")
        .join(&case);
    fs::create_dir_all(&dir).expect("create test log dir");
    // Use test_case_name as instance key so file names are recognizable.
    init_log(&dir, &case);
}
