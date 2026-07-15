use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
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

const PYTHON_TEST_EMBED_LINK_ARGS_SCRIPT: &str = r#"
import sysconfig

args = []
seen = set()

def add(arg):
    arg = str(arg or "").strip()
    if arg and arg not in seen:
        seen.add(arg)
        args.append(arg)

for key in ("LIBPL", "LIBDIR"):
    path = sysconfig.get_config_var(key)
    if path:
        add("-L" + path)

libname = ""
ldlibrary = sysconfig.get_config_var("LDLIBRARY") or sysconfig.get_config_var("LIBRARY") or ""
if ldlibrary.startswith("libpython"):
    stem = ldlibrary[3:]
    for suffix in (".so", ".a", ".dylib"):
        pos = stem.find(suffix)
        if pos >= 0:
            stem = stem[:pos]
            break
    libname = stem

if not libname:
    version = sysconfig.get_config_var("VERSION") or ""
    abiflags = sysconfig.get_config_var("ABIFLAGS") or ""
    if version:
        libname = "python" + version + abiflags

if libname:
    add("-l" + libname)

for key in ("LIBS", "SYSLIBS"):
    for arg in (sysconfig.get_config_var(key) or "").split():
        add(arg)

print("\n".join(args))
"#;

fn main() {
    emit_python_test_embed_link_args();
    build_fluxon_fs_video_ffmpeg_shim();

    let target_dir = get_target_dir();
    let runtime_search_subdirs = load_runtime_search_subdirs();
    let runtime_root_dir_names = runtime_root_dir_names();

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
    println!("cargo:rerun-if-changed=src/video_reader_ffi.c");
    println!("cargo:rerun-if-changed=../target/debug/");
    println!("cargo:rerun-if-changed=../target/release/");
}

fn build_fluxon_fs_video_ffmpeg_shim() {
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_FLUXON_FS_VIDEO_FFMPEG");
    if env::var_os("CARGO_FEATURE_FLUXON_FS_VIDEO_FFMPEG").is_none() {
        return;
    }

    let ffmpeg_libs = ["libavformat", "libavcodec", "libavutil", "libswscale"];
    let mut build = cc::Build::new();
    build
        .file("src/video_reader_ffi.c")
        .flag_if_supported("-Wno-deprecated-declarations");

    let mut pkg_config_ok = true;
    let mut link_paths = Vec::new();
    let mut link_libs = Vec::new();
    let mut framework_paths = Vec::new();
    let mut frameworks = Vec::new();
    for lib_name in ffmpeg_libs {
        let mut config = pkg_config::Config::new();
        config.cargo_metadata(false);
        match config.probe(lib_name) {
            Ok(lib) => {
                for include_path in lib.include_paths {
                    build.include(include_path);
                }
                link_paths.extend(lib.link_paths);
                link_libs.extend(lib.libs);
                framework_paths.extend(lib.framework_paths);
                frameworks.extend(lib.frameworks);
            }
            Err(err) => {
                pkg_config_ok = false;
                println!(
                    "cargo:warning=pkg-config could not find {} for FluxonFS VideoReader: {}. Falling back to default compiler/linker paths.",
                    lib_name, err
                );
                break;
            }
        }
    }

    build.compile("fluxon_fs_video_reader_ffi");

    if pkg_config_ok {
        emit_unique_link_searches("native", link_paths);
        emit_unique_link_searches("framework", framework_paths);
        emit_unique_link_libs(link_libs);
        emit_unique_frameworks(frameworks);
    } else {
        println!("cargo:rustc-link-lib=avformat");
        println!("cargo:rustc-link-lib=avcodec");
        println!("cargo:rustc-link-lib=avutil");
        println!("cargo:rustc-link-lib=swscale");
    }
}

fn emit_unique_link_searches(kind: &str, paths: Vec<PathBuf>) {
    let mut seen = Vec::new();
    for path in paths {
        if seen.iter().any(|existing| existing == &path) {
            continue;
        }
        println!("cargo:rustc-link-search={kind}={}", path.display());
        seen.push(path);
    }
}

fn emit_unique_link_libs(libs: Vec<String>) {
    let mut seen = Vec::new();
    for lib in libs {
        if seen.iter().any(|existing| existing == &lib) {
            continue;
        }
        println!("cargo:rustc-link-lib={lib}");
        seen.push(lib);
    }
}

fn emit_unique_frameworks(frameworks: Vec<String>) {
    let mut seen = Vec::new();
    for framework in frameworks {
        if seen.iter().any(|existing| existing == &framework) {
            continue;
        }
        println!("cargo:rustc-link-lib=framework={framework}");
        seen.push(framework);
    }
}

fn emit_python_test_embed_link_args() {
    println!("cargo:rerun-if-env-changed=PYTHON");

    let python = env::var("PYTHON")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "python3".to_string());

    let output = match Command::new(&python)
        .arg("-c")
        .arg(PYTHON_TEST_EMBED_LINK_ARGS_SCRIPT)
        .output()
    {
        Ok(output) => output,
        Err(err) => {
            write_python_test_link_source(
                None,
                &format!(
                    "failed to query Python embed link args with {}: {}",
                    python, err
                ),
            );
            println!(
                "cargo:warning=failed to query Python embed link args with {}: {}",
                python, err
            );
            return;
        }
    };

    if !output.status.success() {
        write_python_test_link_source(
            None,
            &format!(
                "failed to query Python embed link args with {}: status={} stderr={}",
                python,
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        );
        println!(
            "cargo:warning=failed to query Python embed link args with {}: status={} stderr={}",
            python,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
        return;
    }

    let mut python_link_lib = None;
    for arg in String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|arg| !arg.is_empty())
    {
        if let Some(path) = arg.strip_prefix("-L") {
            if !path.is_empty() {
                println!("cargo:rustc-link-search=native={path}");
            }
            continue;
        }

        if let Some(lib) = arg.strip_prefix("-l") {
            if lib.starts_with("python") {
                python_link_lib = Some(lib.to_string());
            }
        }
    }

    if let Some(lib) = python_link_lib {
        write_python_test_link_source(Some(&lib), "");
    } else {
        let message = format!(
            "Python embed link args from {} did not include a libpython entry",
            python
        );
        write_python_test_link_source(None, &message);
        println!(
            "cargo:warning=Python embed link args from {} did not include a libpython entry",
            python
        );
    }
}

fn write_python_test_link_source(python_link_lib: Option<&str>, message: &str) {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let path = out_dir.join("python_test_link.rs");
    let source = match python_link_lib {
        Some(lib) => format!("#[link(name = {lib:?})]\nunsafe extern \"C\" {{}}\n"),
        None => format!("compile_error!({:?});\n", message),
    };
    fs::write(&path, source).expect("write generated Python test link source");
    println!(
        "cargo:rustc-env=FLUXON_PYO3_TEST_PYTHON_LINK_RS={}",
        path.display()
    );
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

fn runtime_root_dir_names() -> Vec<&'static str> {
    CLOSED_SDK_RUNTIME_ROOT_DIR_NAMES.to_vec()
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
