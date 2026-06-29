use crate::test_util::{is_etcd_running, start_test_etcd};
use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Mutex, OnceLock};

struct BuildConfigGuard {
    path: PathBuf,
    previous: Option<Vec<u8>>,
}

impl BuildConfigGuard {
    fn install(content: String) -> Self {
        let path = crate::dev_config::repo_root()
            .expect("repo root")
            .join("build_config_ext.yml");
        let previous = fs::read(&path).ok();
        fs::write(&path, content).expect("write test build_config_ext.yml");
        Self { path, previous }
    }
}

impl Drop for BuildConfigGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(content) => {
                fs::write(&self.path, content).expect("restore previous build_config_ext.yml")
            }
            None => {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}

fn build_config_file_lock() -> &'static Mutex<()> {
    static BUILD_CONFIG_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    BUILD_CONFIG_MUTEX.get_or_init(|| Mutex::new(()))
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

fn install_test_build_config_ext() -> BuildConfigGuard {
    let (client_port, _peer_port) = pick_free_etcd_port_pair();
    BuildConfigGuard::install(format!("etcd: 127.0.0.1:{client_port}\n"))
}

#[test]
#[serial_test::serial(build_config_ext)]
fn test_etcd_only_starts_once() {
    let _config_lock = build_config_file_lock()
        .lock()
        .expect("lock build config file");
    let _test_build_config = install_test_build_config_ext();
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
