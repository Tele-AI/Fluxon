use std::fs;
use std::os::unix::fs::PermissionsExt;

use fluxon_commu_closed_sdk_consumer::{initialize_process_logging, runtime_invoke};
use fluxon_commu_contract::{
    ClosedRuntimeHandle, ClosedRuntimeHandleKind, ClosedRuntimeProcessLoggingConfig,
    ClosedRuntimeRequest,
};

#[test]
fn actual_closed_cdylib_requires_and_writes_process_log() {
    let runtime = tokio::runtime::Runtime::new().expect("create closed logging test runtime");
    let preinit_error = runtime
        .block_on(runtime_invoke(&ClosedRuntimeRequest::DropHandle {
            handle: ClosedRuntimeHandle {
                kind: ClosedRuntimeHandleKind::ClusterManager,
                raw: u64::MAX,
            },
        }))
        .expect_err("closed runtime request before logging init must fail");
    assert!(
        preinit_error
            .to_string()
            .contains("closed process logging must be initialized")
    );

    let expected_core = std::path::Path::new(
        &std::env::var("FLUXON_COMMU_CLOSED_SDK_ROOT")
            .expect("closed SDK root must be explicit for the integration test"),
    )
    .join("lib/libfluxon_commu_core.so")
    .canonicalize()
    .expect("canonicalize expected closed core");
    let maps = fs::read_to_string("/proc/self/maps").expect("read integration test process maps");
    assert!(
        maps.lines()
            .any(|line| { line.split_whitespace().last() == expected_core.to_str() })
    );
    println!("closed_core_loaded_from={}", expected_core.display());

    let test_root = std::env::current_exe()
        .expect("resolve current test executable")
        .parent()
        .expect("test executable must have a parent")
        .join(format!("closed_logging_process_{}", std::process::id()));
    fs::create_dir_all(&test_root).expect("create closed logging integration root");
    let canonical_root = test_root
        .canonicalize()
        .expect("canonicalize closed logging integration root");
    let log_path = canonical_root.join("events.jsonl");
    let config = ClosedRuntimeProcessLoggingConfig {
        absolute_path: log_path.to_string_lossy().into_owned(),
        filter: "info".to_string(),
    };
    let initialized_path = runtime
        .block_on(initialize_process_logging(config.clone()))
        .expect("initialize actual closed cdylib process logging");
    assert_eq!(initialized_path, config.absolute_path);
    println!("closed_process_log_path={}", log_path.display());
    assert_eq!(
        fs::metadata(&log_path)
            .expect("read closed process log metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let log_text = fs::read_to_string(&log_path).expect("read closed process log");
    let initialization_record = log_text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("decode JSONL record"))
        .find(|record| {
            record["fields"]["event"]
                .as_str()
                .is_some_and(|event| event == "closed_sdk_process_logging_initialized")
        })
        .expect("closed process log must contain its initialization record");
    assert_eq!(
        initialization_record["fields"]["path"].as_str(),
        Some(config.absolute_path.as_str())
    );

    let idempotent_path = runtime
        .block_on(initialize_process_logging(config.clone()))
        .expect("same closed logging config must be idempotent");
    assert_eq!(idempotent_path, config.absolute_path);

    let conflicting_root = canonical_root.join("conflict");
    fs::create_dir_all(&conflicting_root).expect("create conflicting logging root");
    let conflicting_path = conflicting_root.join("events.jsonl");
    let conflict = runtime
        .block_on(initialize_process_logging(
            ClosedRuntimeProcessLoggingConfig {
                absolute_path: conflicting_path.to_string_lossy().into_owned(),
                filter: "info".to_string(),
            },
        ))
        .expect_err("different closed logging config must fail");
    assert!(conflict.to_string().contains("already initialized"));
    assert!(!conflicting_path.exists());

    let final_log_text = fs::read_to_string(&log_path).expect("reread closed process log");
    let rejected_record = final_log_text
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("decode JSONL record"))
        .find(|record| {
            record["fields"]["event"]
                .as_str()
                .is_some_and(|event| event == "closed_sdk_process_logging_init_rejected")
        })
        .expect("closed process log must contain the synchronized warning record");
    assert!(
        rejected_record["fields"]["error"]
            .as_str()
            .is_some_and(|error| error.contains("already initialized"))
    );
}
