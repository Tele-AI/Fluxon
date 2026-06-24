use crate::test_util::{is_etcd_running, start_test_etcd};
use std::process::Command;

#[test]
#[serial_test::serial(build_config_ext)]
fn test_etcd_only_starts_once() {
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
