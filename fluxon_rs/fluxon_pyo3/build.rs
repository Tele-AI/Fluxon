use std::{
    env,
    path::{Path, PathBuf},
};

const DEFAULT_RUNTIME_SEARCH_SUBDIRS: &[&str] = &[
    "lib",
    "lib64",
    "lib/x86_64-linux-gnu",
    "lib/plugins",
    "lib64/plugins",
    "lib/x86_64-linux-gnu/plugins",
];
const CLOSED_SDK_RUNTIME_ROOT_DIR_NAMES: &[&str] = &["native_runtime", "vendor_runtime"];

fn main() {
    let target_dir = get_target_dir();
    let runtime_search_subdirs = load_runtime_search_subdirs();
    let runtime_root_dir_names = CLOSED_SDK_RUNTIME_ROOT_DIR_NAMES.to_vec();

    for path in native_runtime_search_dirs(
        &target_dir,
        &runtime_search_subdirs,
        &runtime_root_dir_names,
    ) {
        if path.is_dir() {
            println!("cargo:rustc-link-search=native={}", path.display());
        }
    }

    println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,$ORIGIN");
    println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,$ORIGIN/.");
    println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,$ORIGIN/..");
    for relative_path in runtime_rpath_suffixes(&runtime_search_subdirs, &runtime_root_dir_names) {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,$ORIGIN/../{relative_path}");
    }
    println!(
        "cargo:rustc-cdylib-link-arg=-Wl,-rpath,{}",
        target_dir.join("release").display()
    );
    println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,/usr/local/lib");
    println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,/usr/lib");
    println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,/usr/lib/x86_64-linux-gnu");

    if cfg!(target_os = "macos") {
        println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,@loader_path");
        println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,@loader_path/.");
        println!("cargo:rustc-cdylib-link-arg=-Wl,-rpath,@loader_path/..");
    }

    // The closed SDK export is the single owner of the native link closure.
    // Duplicating those link-lib directives here makes fluxon_pyo3 depend on a second,
    // divergent native library search contract and breaks manylinux linking when the
    // packed bundle layout differs from the prepared closed runtime outputs.

    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/");
    println!("cargo:rerun-if-changed=../target/debug/");
    println!("cargo:rerun-if-changed=../target/release/");
}

fn get_target_dir() -> PathBuf {
    if let Ok(target_dir) = env::var("CARGO_TARGET_DIR") {
        let path = PathBuf::from(target_dir);
        if path.is_absolute() {
            return path;
        }
        return env::current_dir().unwrap().join(path);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    for dir in out_dir.ancestors() {
        if dir
            .file_name()
            .map(|name| name == "target")
            .unwrap_or(false)
        {
            return dir.to_path_buf();
        }
    }

    panic!("failed to locate target directory from OUT_DIR");
}

fn native_runtime_search_dirs(
    target_dir: &Path,
    runtime_search_subdirs: &[String],
    runtime_root_dir_names: &[&str],
) -> Vec<PathBuf> {
    let mut dirs = vec![target_dir.join("release")];
    for root_name in runtime_root_dir_names {
        for subdir in runtime_search_subdirs {
            dirs.push(target_dir.join(root_name).join(subdir));
        }
    }
    dirs
}

fn runtime_rpath_suffixes(
    runtime_search_subdirs: &[String],
    runtime_root_dir_names: &[&str],
) -> Vec<String> {
    let mut suffixes = Vec::new();
    for root_name in runtime_root_dir_names {
        for subdir in runtime_search_subdirs {
            suffixes.push(format!("{root_name}/{subdir}"));
        }
    }
    suffixes
}

fn load_runtime_search_subdirs() -> Vec<String> {
    DEFAULT_RUNTIME_SEARCH_SUBDIRS
        .iter()
        .map(|entry| (*entry).to_string())
        .collect()
}
