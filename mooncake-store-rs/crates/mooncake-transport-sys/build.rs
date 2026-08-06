use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

fn main() {
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_EXTRA_CMAKE_PREFIX_PATH");
    println!("cargo:rerun-if-env-changed=JSONCPP_PREFIX");
    println!("cargo:rerun-if-env-changed=JSONCPP_INCLUDE_DIR");
    println!("cargo:rerun-if-env-changed=JSONCPP_LIBRARY_PATH");
    println!("cargo:rerun-if-env-changed=PYTHON_EXECUTABLE");
    println!("cargo:rerun-if-env-changed=MOONCAKE_SKIP_CLASSIC_TE");
    println!("cargo:rerun-if-env-changed=MOONCAKE_SKIP_NATIVE_BUILD");
    println!("cargo:rerun-if-changed=../../third_party/Mooncake");
    println!("cargo:rerun-if-changed=src/classic_shim.cc");
    println!("cargo:rerun-if-changed=src/tent_shim.cc");

    if skip_native_build() {
        println!("cargo:warning=skipping Mooncake native build because MOONCAKE_SKIP_NATIVE_BUILD is enabled");
        return;
    }

    let upstream_dir = env_path("MOONCAKE_UPSTREAM_DIR").unwrap_or_else(default_upstream_dir);
    let build_dir =
        env_path("MOONCAKE_UPSTREAM_BUILD_DIR").unwrap_or_else(|| upstream_dir.join("build-rust"));
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set by Cargo"));
    let yalantinglibs_prefix = ensure_yalantinglibs_prefix(&upstream_dir, &build_dir);
    let python = detect_python();
    let jsoncpp = detect_jsoncpp();

    ensure_upstream_native_artifacts(
        &upstream_dir,
        &build_dir,
        &yalantinglibs_prefix,
        &python,
        &jsoncpp,
    );
    build_native_shims(&upstream_dir, &build_dir, &out_dir);
}

fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key).map(PathBuf::from)
}

fn default_upstream_dir() -> PathBuf {
    PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir must exist"))
        .join("../../third_party/Mooncake")
}

fn skip_classic_te() -> bool {
    matches!(
        env::var("MOONCAKE_SKIP_CLASSIC_TE")
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn skip_native_build() -> bool {
    matches!(
        env::var("MOONCAKE_SKIP_NATIVE_BUILD")
            .ok()
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

struct PythonConfig {
    executable: PathBuf,
    include_dir: PathBuf,
    library_path: PathBuf,
    library_name: String,
}

struct JsonCppConfig {
    include_dir: PathBuf,
    library_path: PathBuf,
}

fn merged_cmake_prefix_path(yalantinglibs_prefix: &Path) -> String {
    let mut prefixes = vec![yalantinglibs_prefix.display().to_string()];
    if let Some(extra) = env::var_os("MOONCAKE_EXTRA_CMAKE_PREFIX_PATH") {
        let extra = extra.to_string_lossy();
        for prefix in extra
            .split(';')
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            prefixes.push(prefix.to_string());
        }
    }
    prefixes.join(";")
}

fn detect_python() -> PythonConfig {
    if let Some(candidate) = env_path("PYTHON_EXECUTABLE") {
        if let Some(config) = python_config_for_candidate(&candidate) {
            return config;
        }
        panic!(
            "PYTHON_EXECUTABLE points to {} but Python dev headers or libraries were not usable",
            candidate.display()
        );
    }

    for candidate in ["python3.11", "python3.10", "python3.8", "python3"] {
        let candidate = PathBuf::from(candidate);
        if let Some(config) = python_config_for_candidate(&candidate) {
            return config;
        }
    }

    panic!("Python 3.7+ with development headers is required to configure upstream Mooncake");
}

fn python_config_for_candidate(candidate: &Path) -> Option<PythonConfig> {
    let output = Command::new(candidate).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let version_text = if stdout.trim().is_empty() {
        stderr.trim()
    } else {
        stdout.trim()
    };
    let version = version_text.strip_prefix("Python ")?;
    let mut parts = version.split('.');
    let major = parts.next().and_then(|part| part.parse::<u32>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u32>().ok());
    if !matches!((major, minor), (Some(major), Some(minor)) if major > 3 || (major == 3 && minor >= 7))
    {
        return None;
    }

    let (include_dir, library_path, library_name) = query_python_dev(candidate)?;
    Some(PythonConfig {
        executable: resolve_python_executable(candidate)
            .unwrap_or_else(|| candidate.to_path_buf()),
        include_dir,
        library_path,
        library_name,
    })
}

/// Resolve an interpreter to its own absolute path.
///
/// Candidates are probed as bare command names (`python3.10`), and CMake's
/// `FindPython3` cannot derive the `Development.Module` component from one --
/// it needs a path it can resolve sysconfig against. Upstream's
/// `rpc_communicator` requires that component, so passing the bare name makes
/// the upstream configure step fail with "Could NOT find Python3".
fn resolve_python_executable(candidate: &Path) -> Option<PathBuf> {
    let output = Command::new(candidate)
        .args(["-c", "import sys; print(sys.executable)"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
    if path.is_absolute() && path.exists() {
        Some(path)
    } else {
        None
    }
}

fn query_python_dev(candidate: &Path) -> Option<(PathBuf, PathBuf, String)> {
    let script = r#"import sysconfig
include_dir = sysconfig.get_paths().get('include')
libdir = sysconfig.get_config_var('LIBDIR')
ldlibrary = sysconfig.get_config_var('LDLIBRARY')
if include_dir and libdir and ldlibrary:
    print(include_dir)
    print(libdir)
    print(ldlibrary)
"#;
    let output = Command::new(candidate)
        .arg("-c")
        .arg(script)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let mut lines = stdout.lines();
    let include_dir = PathBuf::from(lines.next()?.trim());
    let library_dir = PathBuf::from(lines.next()?.trim());
    let library_file = lines.next()?.trim();
    let library_path = library_dir.join(library_file);
    if !include_dir.exists() || !library_path.exists() {
        return None;
    }

    let mut library_name = library_file.strip_prefix("lib")?.to_string();
    for marker in [".so", ".a", ".dylib"] {
        if let Some(index) = library_name.find(marker) {
            library_name.truncate(index);
            break;
        }
    }

    Some((include_dir, library_path, library_name))
}

fn detect_jsoncpp() -> JsonCppConfig {
    if let Some(config) = detect_jsoncpp_from_env() {
        return config;
    }

    let include_candidates = [
        "/usr/include/jsoncpp",
        "/usr/local/include/jsoncpp",
        "/usr/include",
    ];
    let library_candidates = [
        "/usr/lib/x86_64-linux-gnu/libjsoncpp.so",
        "/usr/lib64/libjsoncpp.so",
        "/usr/local/lib/libjsoncpp.so",
        "/usr/local/lib64/libjsoncpp.so",
    ];

    let include_dir = include_candidates
        .iter()
        .map(PathBuf::from)
        .find(|path| path.join("json/value.h").exists())
        .unwrap_or_else(|| panic!("JsonCpp headers were not found in standard include paths"));
    let library_path = library_candidates
        .iter()
        .map(PathBuf::from)
        .find(|path| path.exists())
        .unwrap_or_else(|| panic!("libjsoncpp.so was not found in standard library paths"));

    JsonCppConfig {
        include_dir,
        library_path,
    }
}

fn detect_jsoncpp_from_env() -> Option<JsonCppConfig> {
    let include_dir = env_path("JSONCPP_INCLUDE_DIR");
    let library_path = env_path("JSONCPP_LIBRARY_PATH");
    match (include_dir, library_path) {
        (Some(include_dir), Some(library_path))
            if include_dir.join("json/value.h").exists() && library_path.exists() =>
        {
            return Some(JsonCppConfig {
                include_dir,
                library_path,
            });
        }
        _ => {}
    }

    let prefix = env_path("JSONCPP_PREFIX")?;
    let include_candidates = [prefix.join("include/jsoncpp"), prefix.join("include")];
    let library_candidates = [
        prefix.join("lib/libjsoncpp.so"),
        prefix.join("lib64/libjsoncpp.so"),
    ];
    let include_dir = include_candidates
        .into_iter()
        .find(|path| path.join("json/value.h").exists())?;
    let library_path = library_candidates.into_iter().find(|path| path.exists())?;

    Some(JsonCppConfig {
        include_dir,
        library_path,
    })
}

fn ensure_yalantinglibs_prefix(upstream_dir: &Path, build_dir: &Path) -> PathBuf {
    let install_dir = build_dir.join("yalantinglibs-install");
    let config_dir = install_dir.join("lib/cmake/yalantinglibs");
    let config_file = config_dir.join("yalantinglibsConfig.cmake");
    if config_file.exists() {
        return install_dir;
    }

    let source_dir = upstream_dir.join("extern/yalantinglibs");
    if !source_dir.exists() {
        panic!(
            "yalantinglibs source was not found at {}",
            source_dir.display()
        );
    }
    let ylt_build_dir = build_dir.join("yalantinglibs-build");

    run(
        Command::new("cmake")
            .arg("-S")
            .arg(&source_dir)
            .arg("-B")
            .arg(&ylt_build_dir)
            .arg(format!("-DCMAKE_INSTALL_PREFIX={}", install_dir.display()))
            .arg("-DBUILD_EXAMPLES=OFF")
            .arg("-DBUILD_BENCHMARK=OFF")
            .arg("-DBUILD_UNIT_TESTS=OFF"),
        "configure bundled yalantinglibs",
    );
    run(
        Command::new("cmake")
            .arg("--build")
            .arg(&ylt_build_dir)
            .arg("-j8"),
        "build bundled yalantinglibs",
    );
    run(
        Command::new("cmake").arg("--install").arg(&ylt_build_dir),
        "install bundled yalantinglibs",
    );

    install_dir
}

fn ensure_upstream_native_artifacts(
    upstream_dir: &Path,
    build_dir: &Path,
    yalantinglibs_prefix: &Path,
    python: &PythonConfig,
    jsoncpp: &JsonCppConfig,
) {
    if !upstream_dir.exists() {
        panic!(
            "Mooncake upstream source was not found at {}. Run `git submodule update --init --recursive` or set MOONCAKE_UPSTREAM_DIR.",
            upstream_dir.display()
        );
    }

    let tent_shared = build_dir.join("mooncake-transfer-engine/tent/src/libtent_shared.so");
    let skip_classic = skip_classic_te();
    let transfer_engine = build_dir.join("mooncake-transfer-engine/src/libtransfer_engine.a");
    let artifacts = if skip_classic {
        vec![tent_shared.as_path()]
    } else {
        vec![transfer_engine.as_path(), tent_shared.as_path()]
    };
    if native_artifacts_are_fresh(upstream_dir, build_dir, &artifacts) {
        return;
    }

    run(
        Command::new("cmake")
            .arg("-S")
            .arg(upstream_dir)
            .arg("-B")
            .arg(build_dir)
            .arg(format!(
                "-DCMAKE_PREFIX_PATH={}",
                merged_cmake_prefix_path(yalantinglibs_prefix)
            ))
            .arg(format!(
                "-Dyalantinglibs_DIR={}",
                yalantinglibs_prefix
                    .join("lib/cmake/yalantinglibs")
                    .display()
            ))
            .arg(format!(
                "-DPYTHON_EXECUTABLE={}",
                python.executable.display()
            ))
            .arg(format!(
                "-DPython_EXECUTABLE={}",
                python.executable.display()
            ))
            .arg(format!(
                "-DPython3_EXECUTABLE={}",
                python.executable.display()
            ))
            .arg(format!(
                "-DPython3_INCLUDE_DIR={}",
                python.include_dir.display()
            ))
            .arg(format!(
                "-DPython3_INCLUDE_DIRS={}",
                python.include_dir.display()
            ))
            .arg(format!(
                "-DPython3_LIBRARY={}",
                python.library_path.display()
            ))
            .arg(format!(
                "-DPython3_LIBRARIES={}",
                python.library_path.display()
            ))
            .arg(format!(
                "-DPython3_LIBRARY_DIRS={}",
                python
                    .library_path
                    .parent()
                    .expect("python library must have a parent directory")
                    .display()
            ))
            .arg(format!(
                "-DPython3_RUNTIME_LIBRARY_DIRS={}",
                python
                    .library_path
                    .parent()
                    .expect("python library must have a parent directory")
                    .display()
            ))
            .arg(format!("-DPython3_LIBNAME={}", python.library_name))
            .arg(format!(
                "-DJSONCPP_INCLUDE_DIR={}",
                jsoncpp.include_dir.display()
            ))
            .arg(format!(
                "-DJSONCPP_LIBRARY={}",
                jsoncpp.library_path.display()
            ))
            .arg("-DWITH_TE=ON")
            .arg("-DWITH_STORE=OFF")
            .arg("-DWITH_STORE_RUST=OFF")
            .arg("-DBUILD_EXAMPLES=OFF")
            .arg("-DBUILD_UNIT_TESTS=OFF")
            .arg("-DUSE_TENT=ON")
            .arg("-DUSE_REDIS=ON")
            .arg("-DUSE_HTTP=OFF")
            .arg("-DUSE_ETCD=OFF")
            .arg("-DCMAKE_POSITION_INDEPENDENT_CODE=ON"),
        "configure upstream Mooncake TE/TENT",
    );

    run(
        Command::new("cmake")
            .arg("--build")
            .arg(build_dir)
            .args(if skip_classic {
                vec!["--target", "tent_shared"]
            } else {
                vec!["--target", "transfer_engine", "tent_shared"]
            })
            .arg("-j8"),
        if skip_classic {
            "build upstream Mooncake TENT"
        } else {
            "build upstream Mooncake TE/TENT"
        },
    );
}

fn native_artifacts_are_fresh(upstream_dir: &Path, build_dir: &Path, artifacts: &[&Path]) -> bool {
    if artifacts.iter().any(|artifact| !artifact.exists()) {
        return false;
    }

    let Some(oldest_artifact_mtime) = artifacts
        .iter()
        .filter_map(|artifact| file_mtime(artifact))
        .min()
    else {
        return false;
    };

    let Some(latest_source_mtime) = latest_source_mtime(upstream_dir, build_dir) else {
        return false;
    };

    oldest_artifact_mtime >= latest_source_mtime
}

fn latest_source_mtime(upstream_dir: &Path, build_dir: &Path) -> Option<SystemTime> {
    latest_tree_mtime(upstream_dir, build_dir).or_else(|| file_mtime(upstream_dir))
}

fn latest_tree_mtime(path: &Path, build_dir: &Path) -> Option<SystemTime> {
    if ignore_source_path(path, build_dir) {
        return None;
    }

    let metadata = fs::metadata(path).ok()?;
    let mut latest = metadata.modified().ok();
    if !metadata.is_dir() {
        return latest;
    }

    let entries = fs::read_dir(path).ok()?;
    for entry in entries.flatten() {
        let child = entry.path();
        if ignore_source_path(&child, build_dir) {
            continue;
        }
        if let Some(candidate) = latest_tree_mtime(&child, build_dir) {
            latest = Some(match latest {
                Some(current) if current >= candidate => current,
                _ => candidate,
            });
        }
    }
    latest
}

fn ignore_source_path(path: &Path, build_dir: &Path) -> bool {
    if path.starts_with(build_dir) {
        return true;
    }
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(".git" | "build-rust" | "build-wheel-compat")
    )
}

fn file_mtime(path: &Path) -> Option<SystemTime> {
    fs::metadata(path).ok()?.modified().ok()
}

fn build_native_shims(upstream_dir: &Path, build_dir: &Path, out_dir: &Path) {
    let include = upstream_dir.join("mooncake-transfer-engine/include");
    let tent_include = upstream_dir.join("mooncake-transfer-engine/tent/include");
    let classic_dir = build_dir.join("mooncake-transfer-engine/src");
    let tent_dir = build_dir.join("mooncake-transfer-engine/tent/src");

    if !skip_classic_te() {
        build_native_shim(
            out_dir,
            "classic_shim.cc",
            "libmooncake_classic_shim.so",
            "compile classic transfer-engine shim",
            &[&include],
            &[(&classic_dir, "transfer_engine")],
        );
    }
    build_native_shim(
        out_dir,
        "tent_shim.cc",
        "libmooncake_tent_shim.so",
        "compile tent transfer-engine shim",
        &[&include, &tent_include],
        &[(&tent_dir, "tent_shared")],
    );
}

fn build_native_shim(
    out_dir: &Path,
    source_name: &str,
    library_name: &str,
    description: &str,
    includes: &[&Path],
    link_libs: &[(&Path, &str)],
) {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir must exist"));
    let src = manifest_dir.join("src").join(source_name);
    let library = out_dir.join(library_name);
    let mut compile = Command::new("c++");
    compile.arg("-std=c++20").arg("-fPIC").arg("-shared");
    for include in includes {
        compile.arg("-I").arg(include);
    }
    for (path, lib) in link_libs {
        compile.arg("-L").arg(path).arg(format!("-l{lib}"));
    }
    compile.arg(&src).arg("-o").arg(&library);

    if library.exists() {
        std::fs::remove_file(&library).unwrap_or_else(|error| {
            panic!(
                "failed to remove stale shim library {}: {error}",
                library.display()
            )
        });
    }

    run(&mut compile, description);
}

fn run(command: &mut Command, description: &str) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("failed to {description}: {error}"));
    if !status.success() {
        panic!("failed to {description}: exit status {status}");
    }
}
