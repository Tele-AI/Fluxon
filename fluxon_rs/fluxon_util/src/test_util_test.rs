use crate::test_util::{is_etcd_running, start_test_etcd};
use std::fs;
use std::net::TcpListener;
use std::process::Command;
use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;

const BUILD_CONFIG_EXT_PATH_ENV: &str = "FLUXON_BUILD_CONFIG_EXT_PATH";

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

fn build_config_env_lock() -> &'static Mutex<()> {
    static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    ENV_MUTEX.get_or_init(|| Mutex::new(()))
}

fn pick_free_etcd_port_pair() -> (u16, u16) {
    for _ in 0..32 {
        let client_socket = TcpListener::bind(("127.0.0.1", 0)).expect("bind etcd client port");
        let client_port = client_socket
            .local_addr()
            .expect("read etcd client port")
            .port();
        let peer_port = if client_port == u16::MAX {
            client_port - 1
        } else {
            client_port + 1
        };
        if TcpListener::bind(("127.0.0.1", peer_port)).is_ok() {
            drop(client_socket);
            return (client_port, peer_port);
        }
    }
    panic!("failed to reserve a free etcd port pair");
}

fn install_test_build_config_ext() -> (TempDir, EnvVarGuard) {
    let temp_dir = TempDir::new().expect("create temp build config dir");
    let (client_port, _peer_port) = pick_free_etcd_port_pair();
    let build_config_ext_path = temp_dir.path().join("build_config_ext.yml");
    fs::write(
        &build_config_ext_path,
        format!("etcd: 127.0.0.1:{client_port}\n"),
    )
    .expect("write temp build_config_ext");
    let guard = EnvVarGuard::set(BUILD_CONFIG_EXT_PATH_ENV, build_config_ext_path.display().to_string());
    (temp_dir, guard)
}

#[test]
#[serial_test::serial(build_config_ext)]
fn test_etcd_only_starts_once() {
    let _env_lock = build_config_env_lock().lock().expect("lock build config env");
    let _temp_build_config = if std::env::var_os(BUILD_CONFIG_EXT_PATH_ENV).is_none() {
        Some(install_test_build_config_ext())
    } else {
        None
    };
    start_test_etcd().expect("start local test etcd");
    assert!(is_etcd_running(), "etcd should be reachable after startup");

    let endpoint =
        crate::dev_config::read_etcd_endpoint_from_build_config().expect("read etcd endpoint");
    let etcdctl = crate::dev_config::repo_root()
        .expect("repo root")
        .join("fluxon_release")
        .join("ext_images")
        .join("etcd")
        .join("etcdctl");
    assert!(etcdctl.exists(), "missing etcdctl at {}", etcdctl.display());

    let test_key = format!(
        "/fluxon_util/test_util_test/{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap()
    );
    let test_value = "test_value_123";

    let put_result = Command::new(&etcdctl)
        .env("ETCDCTL_API", "3")
        .arg("--endpoints")
        .arg(&endpoint)
        .arg("put")
        .arg(&test_key)
        .arg(test_value)
        .output()
        .expect("write test data");
    assert!(
        put_result.status.success(),
        "etcd put failed: {}",
        String::from_utf8_lossy(&put_result.stderr)
    );

    let get_result = Command::new(&etcdctl)
        .env("ETCDCTL_API", "3")
        .arg("--endpoints")
        .arg(&endpoint)
        .arg("get")
        .arg(&test_key)
        .output()
        .expect("read test data");
    assert!(
        get_result.status.success(),
        "etcd get failed: {}",
        String::from_utf8_lossy(&get_result.stderr)
    );
    let result = String::from_utf8_lossy(&get_result.stdout);
    let lines: Vec<&str> = result.trim().split('\n').collect();
    assert!(
        lines.len() >= 2 && lines[0] == test_key && lines[1] == test_value,
        "etcd returned unexpected data: {result}"
    );

    start_test_etcd().expect("second start_test_etcd should be idempotent");
    start_test_etcd().expect("third start_test_etcd should be idempotent");
    assert!(is_etcd_running(), "etcd should remain reachable");

    let _ = Command::new(&etcdctl)
        .env("ETCDCTL_API", "3")
        .arg("--endpoints")
        .arg(&endpoint)
        .arg("del")
        .arg(&test_key)
        .output();

    println!("test etcd is reachable and start_test_etcd is idempotent");
}
