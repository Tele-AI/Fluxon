use std::error::Error;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, Once, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

static ETCD_PROCESS: OnceLock<Mutex<Option<Child>>> = OnceLock::new();
static ETCD_CLEANUP_REGISTERED: Once = Once::new();

fn etcd_process() -> &'static Mutex<Option<Child>> {
    ETCD_PROCESS.get_or_init(|| Mutex::new(None))
}

extern "C" fn cleanup_test_etcd_at_exit() {
    let _ = stop_test_etcd();
}

fn register_cleanup() {
    ETCD_CLEANUP_REGISTERED.call_once(|| unsafe {
        libc::atexit(cleanup_test_etcd_at_exit);
    });
}

fn boxed_error(message: impl Into<String>) -> Box<dyn Error> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::Other,
        message.into(),
    ))
}

fn split_host_port(authority: &str) -> Result<(String, u16), Box<dyn Error>> {
    let authority = authority.trim();
    if authority.is_empty() {
        return Err(boxed_error("empty etcd endpoint authority"));
    }

    if let Some(rest) = authority.strip_prefix('[') {
        let Some((host, port_with_colon)) = rest.split_once(']') else {
            return Err(boxed_error(format!(
                "invalid bracketed etcd endpoint authority: {authority}"
            )));
        };
        let Some(port) = port_with_colon.strip_prefix(':') else {
            return Err(boxed_error(format!(
                "missing port in etcd endpoint authority: {authority}"
            )));
        };
        return Ok((host.to_string(), port.parse()?));
    }

    let Some((host, port)) = authority.rsplit_once(':') else {
        return Err(boxed_error(format!(
            "etcd endpoint must be host:port, got: {authority}"
        )));
    };
    if host.is_empty() || port.is_empty() {
        return Err(boxed_error(format!(
            "etcd endpoint must be host:port, got: {authority}"
        )));
    }
    Ok((host.to_string(), port.parse()?))
}

fn url_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn peer_port(client_port: u16) -> u16 {
    if client_port < u16::MAX {
        client_port + 1
    } else {
        client_port - 1
    }
}

fn ext_etcd_dir() -> Result<PathBuf, Box<dyn Error>> {
    Ok(crate::dev_config::repo_root()?
        .join("fluxon_release")
        .join("ext_images")
        .join("etcd"))
}

fn etcdctl_path() -> Result<PathBuf, Box<dyn Error>> {
    Ok(ext_etcd_dir()?.join("etcdctl"))
}

fn endpoint_health(endpoint: &str, timeout: Duration) -> bool {
    let Ok(etcdctl) = etcdctl_path() else {
        return false;
    };
    if !etcdctl.exists() {
        return false;
    }

    let mut child = match Command::new(etcdctl)
        .env("ETCDCTL_API", "3")
        .arg("--endpoints")
        .arg(endpoint)
        .arg("--dial-timeout")
        .arg(format!("{}ms", timeout.as_millis()))
        .arg("--command-timeout")
        .arg(format!("{}ms", timeout.as_millis()))
        .arg("endpoint")
        .arg("health")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => return false,
    };

    let deadline = Instant::now() + timeout + Duration::from_millis(200);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return status.success(),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(20)),
            Ok(None) => {
                let _ = child.kill();
                let _ = child.wait();
                return false;
            }
            Err(_) => return false,
        }
    }
}

pub fn is_etcd_running() -> bool {
    let Ok(endpoint) = crate::dev_config::read_etcd_endpoint_from_build_config() else {
        return false;
    };
    endpoint_health(&endpoint, Duration::from_secs(1))
}

pub fn stop_test_etcd() -> Result<(), Box<dyn Error>> {
    let mut guard = etcd_process()
        .lock()
        .map_err(|_| boxed_error("test etcd process mutex poisoned"))?;
    if let Some(mut child) = guard.take() {
        match child.try_wait()? {
            Some(_) => {}
            None => {
                child.kill()?;
                let _ = child.wait();
            }
        }
    }
    Ok(())
}

pub fn start_test_etcd() -> Result<(), Box<dyn Error>> {
    register_cleanup();

    let endpoint = crate::dev_config::read_etcd_endpoint_from_build_config()?;
    if endpoint_health(&endpoint, Duration::from_millis(500)) {
        return Ok(());
    }

    let mut guard = etcd_process()
        .lock()
        .map_err(|_| boxed_error("test etcd process mutex poisoned"))?;
    if endpoint_health(&endpoint, Duration::from_secs(2)) {
        return Ok(());
    }
    if let Some(child) = guard.as_mut() {
        if child.try_wait()?.is_none() {
            wait_for_etcd_ready(child, &endpoint)?;
            return Ok(());
        }
        let _ = guard.take();
    }

    let host_port = crate::dev_config::read_etcd_host_port_from_build_config()?;
    let (host, client_port) = split_host_port(&host_port)?;
    let host_for_url = url_host(&host);
    let peer_port = peer_port(client_port);
    let peer_url = format!("http://{host_for_url}:{peer_port}");
    let client_url = format!("http://{host_for_url}:{client_port}");

    let etcd_dir = ext_etcd_dir()?;
    let start_script = etcd_dir.join("start.sh");
    if !start_script.exists() {
        return Err(boxed_error(format!(
            "missing etcd start script: {} (run setup_and_pack/pack_release_ext.py first)",
            start_script.display()
        )));
    }

    let workdir = PathBuf::from(test_workdir_base()).join("etcd");
    fs::create_dir_all(&workdir)?;
    let config_path = workdir.join("config.sh");
    let stdout_path = workdir.join("stdout.log");
    let stderr_path = workdir.join("stderr.log");
    fs::write(
        &config_path,
        format!(
            r#"ETCD_ARGS=(
  --data-dir "${{WORKDIR}}/etcd-data"
  --name test-etcd
  --advertise-client-urls {client_url}
  --listen-client-urls {client_url}
  --listen-peer-urls {peer_url}
  --initial-advertise-peer-urls {peer_url}
  --initial-cluster {initial_cluster}
  --initial-cluster-token test-etcd-cluster
  --initial-cluster-state new
  --auto-compaction-retention=1
)
"#,
            client_url = shell_quote(&client_url),
            peer_url = shell_quote(&peer_url),
            initial_cluster = shell_quote(&format!("test-etcd={peer_url}")),
        ),
    )?;

    let stdout = File::create(&stdout_path)?;
    let stderr = File::create(&stderr_path)?;
    let mut child = Command::new(&start_script)
        .arg("--config")
        .arg(&config_path)
        .arg("--workdir")
        .arg(&workdir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(|e| {
            boxed_error(format!(
                "failed to start etcd via {}: {e}",
                start_script.display()
            ))
        })?;

    if let Err(e) = wait_for_etcd_ready(&mut child, &endpoint) {
        if endpoint_health(&endpoint, Duration::from_secs(2)) {
            let _ = child.wait();
            return Ok(());
        }
        let stdout_hint = read_log_tail(&stdout_path);
        let stderr_hint = read_log_tail(&stderr_path);
        return Err(boxed_error(format!(
            "{e}\netcd stdout tail:\n{stdout_hint}\netcd stderr tail:\n{stderr_hint}"
        )));
    }
    *guard = Some(child);
    Ok(())
}

fn wait_for_etcd_ready(child: &mut Child, endpoint: &str) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if endpoint_health(endpoint, Duration::from_millis(800)) {
            return Ok(());
        }
        if let Some(status) = child.try_wait()? {
            return Err(boxed_error(format!(
                "test etcd exited before becoming healthy: {status}"
            )));
        }
        if Instant::now() >= deadline {
            return Err(boxed_error(format!(
                "timed out waiting for test etcd endpoint to become healthy: {endpoint}"
            )));
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn read_log_tail(path: &Path) -> String {
    let Ok(content) = fs::read_to_string(path) else {
        return format!("(failed to read {})", path.display());
    };
    let lines: Vec<&str> = content.lines().rev().take(40).collect();
    lines.into_iter().rev().collect::<Vec<_>>().join("\n")
}

/// Return per-run test workdir base under repo log folder.
/// Format: <repo>/log/test_workdir_YYYY_MM_DD_HH_MM_SS
pub fn test_workdir_base() -> &'static str {
    static TEST_WORKDIR: OnceLock<String> = OnceLock::new();
    TEST_WORKDIR.get_or_init(|| {
        // Keep consistent with prior behavior of writing under ../../log from each crate
        let mut base = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        base.push("../../log");

        // Human-friendly timestamp to the second (UTC)
        let ts = chrono::Utc::now().format("%Y_%m_%d_%H_%M_%S");
        base.push(format!("test_workdir_{}", ts));

        fs::create_dir_all(&base).expect("create test base workdir");
        base.to_string_lossy().to_string()
    })
}
