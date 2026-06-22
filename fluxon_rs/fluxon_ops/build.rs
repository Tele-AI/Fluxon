use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const SUPERVISOR_TERM_SECONDS: &str = "60";
const SUPERVISOR_KILL_SECONDS: &str = "10";
const SUPERVISOR_SUPERSEDE_SECONDS: &str = "30";

fn repo_root(manifest_dir: &Path) -> PathBuf {
    manifest_dir
        .parent()
        .and_then(|v| v.parent())
        .unwrap_or_else(|| panic!("fluxon_ops build.rs expects fluxon_rs/<crate> layout"))
        .to_path_buf()
}

fn render_selection_supervisor(repo_root: &Path) -> String {
    let python_script = r#"
import pathlib
import sys
import types

repo_root = pathlib.Path(sys.argv[1])
term_seconds = int(sys.argv[2])
kill_seconds = int(sys.argv[3])
supersede_seconds = int(sys.argv[4])
module_dir = repo_root / "deployment" / "utils"
sys.path.insert(0, str(module_dir))
import selection_supervisor_codegen as module
print(
    module.render_python_selection_supervisor_module(
        timeouts=types.SimpleNamespace(
            term_seconds=term_seconds,
            kill_seconds=kill_seconds,
            supersede_seconds=supersede_seconds,
        )
    ),
    end="",
)
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(python_script)
        .arg(repo_root)
        .arg(SUPERVISOR_TERM_SECONDS)
        .arg(SUPERVISOR_KILL_SECONDS)
        .arg(SUPERVISOR_SUPERSEDE_SECONDS)
        .output()
        .unwrap_or_else(|e| panic!("run python3 to render selection supervisor failed: {}", e));
    if !output.status.success() {
        panic!(
            "render selection supervisor failed: status={:?} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout).expect("selection supervisor output must be utf-8")
}

fn render_log_shard_helper(repo_root: &Path) -> String {
    let helper_path = repo_root.join("deployment").join("utils").join("log_shard.py");
    fs::read_to_string(&helper_path)
        .unwrap_or_else(|e| panic!("read log shard helper failed: {} ({})", helper_path.display(), e))
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let repo_root = repo_root(&manifest_dir);
    let source = render_selection_supervisor(&repo_root);
    let log_shard_source = render_log_shard_helper(&repo_root);

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR"));
    let out_path = out_dir.join("selection_supervisor.py");
    fs::write(&out_path, source).expect("write embedded selection supervisor source");
    let helper_out_path = out_dir.join("log_shard.py");
    fs::write(&helper_out_path, log_shard_source).expect("write embedded log shard helper");

    println!("cargo:rerun-if-changed=build.rs");
    println!(
        "cargo:rerun-if-changed={}",
        repo_root
            .join("deployment")
            .join("utils")
            .join("selection_supervisor_codegen.py")
            .display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        repo_root.join("deployment").join("utils").join("log_shard.py").display()
    );
}
