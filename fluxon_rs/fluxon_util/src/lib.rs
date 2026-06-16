pub mod dag_viz_html;
pub mod dev_config;
pub mod fs_statvfs;
pub mod fs_watch;
pub mod git_version_build_record;
pub mod hash;
pub mod init_dag_compiler;
pub mod map_lock;
pub mod merge_recent_async_notifies;
pub mod prefix_scan;
pub mod prom_remote_write;
pub mod scoped_future_set;
pub mod semaphore_map;
pub mod stream;
pub mod build_info {
    pub const GIT_COMMIT_ID: &str = env!("GIT_COMMIT_HASH");
    pub const SOURCE_SHA256: &str = env!("FLUXON_RS_SOURCE_SHA256");

    pub fn format_long_version(pkg_version: &str) -> String {
        format!(
            "{}\ncommit: {}\nsource-sha256: {}",
            pkg_version, GIT_COMMIT_ID, SOURCE_SHA256
        )
    }
}
pub mod auto_clean_map;
pub mod etcd;
pub mod lease_manager;
pub mod run_async_from_sync;
pub mod test_util;
pub mod vallocator;
// Logging related code moved into its own module to keep crate root clean.
pub mod log;
// Rate limit utilities
pub mod limitrate;
// PyO3 helpers: run long-time Python call without holding GIL in caller thread.
pub mod pyo3;
// Re-export for stable public API: existing call sites can keep using `fluxon_util::init_log`.
pub use log::{current_log_file_path, init_log, init_log_test, init_log_with_extra_layer};
#[cfg(test)]
mod test_util_test;

// Registered panel proxy descriptor used by fluxon_cli (/r/<service>/<cluster>/...).
//
// Causal chain:
// - fluxon_cli must proxy multiple business panels without linking to their crates (no dependency cycles).
// - Therefore the proxy routing contract must be a small, stable, serde-friendly descriptor stored in etcd.
// - Transport is an enum to avoid stringly-typed divergence across services.
//
// Contract:
// - Publishers must write FluxonCliProxyDescriptorV2 JSON to the key returned by
//   `fluxon_cli_proxy_desc_etcd_key_v2(service, cluster)`.
// - Consumers (fluxon_cli) must NOT guess or fallback to other keys/schemas.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FluxonCliProxyTransportV2 {
    Http { base_url: String },
    P2pRpc { node_id: String },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FluxonCliProxyDescriptorV2 {
    pub transport: FluxonCliProxyTransportV2,
    pub allow_prefixes: Vec<String>,
    pub html_inject: bool,
}

pub fn fluxon_cli_proxy_desc_etcd_key_v2(service_name: &str, cluster_name: &str) -> String {
    format!(
        "/fluxon_cli_proxy/v2/{}/{}/descriptor",
        service_name, cluster_name
    )
}

pub fn fluxon_cli_proxy_desc_etcd_service_prefix_v2(service_name: &str) -> String {
    format!("/fluxon_cli_proxy/v2/{}/", service_name)
}

use anyhow::{Result, anyhow, bail};
use local_ip_address::list_afinet_netifas;
use std::path::PathBuf;
/// 综合宏：同时生成枚举定义和 From<i32> 实现
/// example:
/// define_error_code_enum_with_from! {
///     #[repr(i32)]
///     #[derive(Debug, Clone, Copy, PartialEq)]
///     pub enum AllocErrorCode {
///         Success = 0,
///         OutOfMemory = 1,
///         InvalidSize = 2,
///         InvalidPointer = 3,
///         DoubleFree = 4,
///         InvalidAddress = 5,
///         NewException = 6,
///         DeallocationException = 7,
///         DeallocationUnknownException = 8,
///         AllocationFailed = 9,
///         AllocationException = 10,
///         AllocationUnknownException = 11,
///         SizeNotAligned = 12,
///         InvalidCode = 10000000,
///     }
///     default: AllocErrorCode::InvalidCode
/// }
#[macro_export]
macro_rules! define_error_code_enum_with_from {
    (
        $(#[$attr:meta])*
        $vis:vis enum $name:ident {
            $($(#[$variant_attr:meta])* $variant:ident = $value:expr),* $(,)?
        }
        default: $default:expr
    ) => {
        // 生成枚举定义
        $(#[$attr])*
        $vis enum $name {
            $($(#[$variant_attr])* $variant = $value,)*
        }

        // 生成 From<i32> 实现
        impl From<i32> for $name {
            fn from(value: i32) -> Self {
                match value {
                    $(
                        $value => $name::$variant,
                    )*
                    _ => $default,
                }
            }
        }
    };
}

/// 获取主机IP地址
pub fn get_host_ip() -> Result<String> {
    let network_interfaces = list_afinet_netifas()
        .map_err(|e| anyhow!("fail to get local network info, err:{:?}", e))?;
    let mut addresses = network_interfaces
        .into_iter()
        .filter(|(_, ip)| !ip.is_loopback())
        .map(|(_, ip)| ip.to_string())
        .collect::<Vec<String>>();
    if addresses.len() > 0 {
        return Ok(addresses.remove(0));
    }

    bail!("No non-loopback IP address found")
}

#[macro_export]
macro_rules! new_map {
    // 匹配空映射
    ($map_type:ident { }) => {
        $map_type::new()
    };
    // 匹配一个或多个键值对
    ($map_type:ident { $($key:expr => $value:expr),+ $(,)? }) => {{
        let map = $map_type::from([
            $( ($key, $value), )+
        ]);
        map
    }};
}

pub fn build_target_dir_() -> PathBuf {
    let target_name = std::env::var("CARGO_TARGET_DIR").unwrap_or("target".to_string());
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let mut cur_dir = PathBuf::from(out_dir.clone());
    while cur_dir.file_name().unwrap() != &*target_name {
        cur_dir = cur_dir.parent().unwrap().to_owned();
    }
    cur_dir
}

#[cfg(test)]
mod tests {
    use crate::{current_log_file_path, init_log};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use tracing::{debug, error, info, warn};

    #[cfg(trybuild)]
    #[test]
    fn trybuild_scoped_sync_async_bridge() {
        let t = trybuild::TestCases::new();
        t.pass("tests/compile_tests/scoped_sync_async_bridge/pass/*.rs");
        t.compile_fail("tests/compile_tests/scoped_sync_async_bridge/fail/*.rs");
    }

    #[test]
    fn test_init_log_with_file_path() {
        // 创建临时目录用于日志文件
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let log_path = temp_dir.path();
        let instance_key = "test_instance";

        // 初始化日志系统
        init_log(log_path, instance_key);

        // 写入不同级别的日志
        debug!("This is a debug message");
        info!("This is an info message");
        warn!("This is a warning message");
        error!("This is an error message");

        // 等待日志写入
        std::thread::sleep(std::time::Duration::from_millis(100));

        // 验证日志文件是否创建
        let log_key = instance_key;
        let mut log_file_found = false;

        // 遍历日志目录，查找日志文件
        for entry in fs::read_dir(log_path).expect("Failed to read log directory") {
            let entry = entry.expect("Failed to read entry");
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            if file_name_str.contains(log_key) && file_name_str.contains(".log") {
                log_file_found = true;
                if current_log_file_path()
                    .as_ref()
                    .is_some_and(|path| path.starts_with(log_path))
                {
                    let content = fs::read_to_string(entry.path()).expect("Failed to read log");
                    assert!(
                        content.contains("debug message"),
                        "Log should contain debug"
                    );
                    assert!(content.contains("info message"), "Log should contain info");
                    assert!(
                        content.contains("warning message"),
                        "Log should contain warning"
                    );
                    assert!(
                        content.contains("error message"),
                        "Log should contain error"
                    );
                }
            }
        }

        assert!(log_file_found, "Log file should be created");
    }

    // 移除“不指定日志路径”的测试：生产入口强制要求提供 log_path。
    #[test]
    fn test_init_log_invalid_path() {
        // 测试无效路径的处理
        let invalid_path = PathBuf::from("/proc/invalid_path_that_cannot_be_created/logs");

        // 这应该会打印错误信息但不会 panic
        init_log(&invalid_path, "test_instance");

        // 验证不会崩溃
        info!("This should still work");
    }

    // 移除 init_log_test 相关测试：测试不再使用测试专用 logger。

    #[test]
    fn test_log_file_rotation() {
        // 测试日志文件按天滚动的功能
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let log_path = temp_dir.path();
        let instance_key = "rotation_test";

        // 初始化日志
        init_log(log_path, instance_key);

        // 写入一些日志
        for i in 0..10 {
            info!("Log message {}", i);
            warn!("Warning message {}", i);
        }

        // 等待日志写入
        std::thread::sleep(std::time::Duration::from_millis(100));

        // 验证文件存在
        let files: Vec<_> = fs::read_dir(log_path)
            .expect("Failed to read log directory")
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();

        assert!(
            files.iter().any(|f| f.contains("fluxon-kv-rotation_test")),
            "Log files should be created with correct instance key"
        );
    }

    #[test]
    fn test_multiple_init_log_calls() {
        // 测试多次调用 init_log 的行为
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let log_path = temp_dir.path();

        // 第一次初始化
        init_log(log_path, "instance1");
        info!("First init message");

        // 第二次初始化（应该被忽略，因为 try_init 会失败）
        init_log(log_path, "instance2");
        info!("Second init message");

        // 验证不会崩溃
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
