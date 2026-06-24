use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fluxon_util::DEFAULT_DAILY_LOG_RETENTION_DAYS;
use tempfile::TempDir;

const TEST_LOG_SHARD_WINDOW_SECONDS_ENV: &str = "FLUXON_TEST_LOG_SHARD_WINDOW_SECONDS";
const TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV: &str = "FLUXON_TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS";

struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvVarGuard {
    fn set(key: &'static str, value: impl Into<String>) -> Self {
        let previous = std::env::var(key).ok();
        unsafe {
            std::env::set_var(key, value.into());
        }
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        match self.previous.as_deref() {
            Some(value) => unsafe {
                std::env::set_var(self.key, value);
            },
            None => unsafe {
                std::env::remove_var(self.key);
            },
        }
    }
}

fn count_service_shards(root: &Path, prefix: &str) -> usize {
    fs::read_dir(root)
        .expect("read log directory")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with(prefix) && name.ends_with(".log"))
        .count()
}

#[test]
fn kv_log_shards_roll_and_cleanup_with_test_window() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let log_path = temp_dir.path();
    let instance_key = "log_mgmt_window";
    let base_prefix = format!("fluxon-kv-{instance_key}");
    let stale_path = log_path.join(format!("{base_prefix}.2025-12-01.log"));
    fs::write(&stale_path, "stale\n").expect("write stale shard");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("unix epoch")
        .as_secs() as i64;
    let _window_guard = EnvVarGuard::set(TEST_LOG_SHARD_WINDOW_SECONDS_ENV, "10");
    let _anchor_guard = EnvVarGuard::set(TEST_LOG_SHARD_ANCHOR_UNIX_SECONDS_ENV, (now - 2).to_string());

    fluxon_util::init_log(log_path, instance_key);
    tracing::info!(target: "fluxon_util", "[kv-log-mgmt][phase=before] ts={}", now);
    std::thread::sleep(Duration::from_millis(300));
    std::thread::sleep(Duration::from_secs(11));
    let after_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("unix epoch")
        .as_secs();
    tracing::info!(target: "fluxon_util", "[kv-log-mgmt][phase=after] ts={after_ts}");
    std::thread::sleep(Duration::from_millis(500));

    let shard_1 = log_path.join(format!("{base_prefix}.2026-01-01.log"));
    let shard_2 = log_path.join(format!("{base_prefix}.2026-01-02.log"));
    assert!(shard_1.exists(), "missing shard: {}", shard_1.display());
    assert!(shard_2.exists(), "missing shard: {}", shard_2.display());
    assert!(
        !stale_path.exists(),
        "stale shard should be removed once retention cleanup runs"
    );
    assert_eq!(
        count_service_shards(log_path, base_prefix.as_str()),
        2,
        "expected exactly two retained shard files within the synthetic test window"
    );

    let shard_1_text = fs::read_to_string(&shard_1).expect("read first shard");
    let shard_2_text = fs::read_to_string(&shard_2).expect("read second shard");
    assert!(
        shard_1_text.contains("[kv-log-mgmt][phase=before]"),
        "first shard should contain the before marker"
    );
    assert!(
        !shard_1_text.contains("[kv-log-mgmt][phase=after]"),
        "first shard should not contain the after marker"
    );
    assert!(
        shard_2_text.contains("[kv-log-mgmt][phase=after]"),
        "second shard should contain the after marker"
    );
    assert!(
        !shard_2_text.contains("[kv-log-mgmt][phase=before]"),
        "second shard should not contain the before marker"
    );
    assert_eq!(DEFAULT_DAILY_LOG_RETENTION_DAYS, 31);
}

#[test]
fn resolve_readable_log_path_ignores_plain_base_log_when_daily_shards_exist() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let base_path = temp_dir.path().join("startup.log");
    fs::write(&base_path, "plain\n").expect("write base log");
    let shard_path = temp_dir.path().join("startup.2026-06-21.log");
    fs::write(&shard_path, "shard\n").expect("write shard log");

    let resolved = fluxon_util::resolve_readable_log_path(&base_path).expect("resolve readable log path");
    assert_eq!(resolved, shard_path);
}

#[test]
fn latest_existing_daily_sharded_log_path_skips_invalid_candidates() {
    let temp_dir = TempDir::new().expect("create temp dir");
    let base_path = temp_dir.path().join("demo.log");
    let invalid_shard_path = temp_dir.path().join("demo.not-a-date.log");
    let valid_shard_path = temp_dir.path().join("demo.2026-06-20.log");
    fs::write(&invalid_shard_path, "invalid\n").expect("write invalid shard");
    fs::write(&valid_shard_path, "valid\n").expect("write valid shard");

    let resolved =
        fluxon_util::latest_existing_daily_sharded_log_path(&base_path).expect("resolve latest shard");
    assert_eq!(resolved, valid_shard_path);
}
