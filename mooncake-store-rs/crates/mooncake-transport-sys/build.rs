use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_BUILD_DIR");
    println!("cargo:rerun-if-changed=../../third_party/Mooncake");
    println!("cargo:rerun-if-changed=src/classic_shim.cc");
    println!("cargo:rerun-if-changed=src/tent_shim.cc");

    let upstream_dir = env_path("MOONCAKE_UPSTREAM_DIR").unwrap_or_else(default_upstream_dir);
    let build_dir =
        env_path("MOONCAKE_UPSTREAM_BUILD_DIR").unwrap_or_else(|| upstream_dir.join("build-rust"));
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set by Cargo"));
    let yalantinglibs_prefix = ensure_yalantinglibs_prefix(&upstream_dir, &build_dir);
    let python = detect_python();
    let jsoncpp = detect_jsoncpp();

    ensure_upstream_native_artifacts(&upstream_dir, &build_dir);
    build_native_shims(&upstream_dir, &out_dir);

    let classic_dir = build_dir.join("mooncake-transfer-engine/src");
    let tent_dir = build_dir.join("mooncake-transfer-engine/tent/src");

    println!("cargo:rustc-link-search=native={}", classic_dir.display());
    println!("cargo:rustc-link-search=native={}", tent_dir.display());
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=mooncake_classic_shim");
    println!("cargo:rustc-link-lib=static=mooncake_tent_shim");
    println!("cargo:rustc-link-lib=dylib=transfer_engine");
    println!("cargo:rustc-link-lib=dylib=tent_shared");
    println!("cargo:rustc-link-lib=dylib=stdc++");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", classic_dir.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", tent_dir.display());
}

fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key).map(PathBuf::from)
}

fn default_upstream_dir() -> PathBuf {
    PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir must exist"))
        .join("../../third_party/Mooncake")
}

struct PythonConfig {
    executable: PathBuf,
    include_dir: PathBuf,
    library_dir: PathBuf,
    library_name: String,
}

struct JsonCppConfig {
    include_dir: PathBuf,
    library_path: PathBuf,
}

fn detect_python() -> PythonConfig {
    for candidate in ["python3.8", "python3"] {
        let output = Command::new(candidate).arg("--version").output();
        let Ok(output) = output else {
            continue;
        };
        if !output.status.success() {
            continue;
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        let version_text = if stdout.trim().is_empty() {
            stderr.trim()
        } else {
            stdout.trim()
        };
        let Some(version) = version_text.strip_prefix("Python ") else {
            continue;
        };
        let mut parts = version.split('.');
        let major = parts.next().and_then(|part| part.parse::<u32>().ok());
        let minor = parts.next().and_then(|part| part.parse::<u32>().ok());
        if !matches!((major, minor), (Some(major), Some(minor)) if major > 3 || (major == 3 && minor >= 7)) {
            continue;
        }

        let Some((include_dir, library_dir, library_name)) = query_python_dev(candidate) else {
            continue;
        };
        return PythonConfig {
            executable: PathBuf::from(candidate),
            include_dir,
            library_dir,
            library_name,
        };
    }
    panic!("Python 3.7+ with development headers is required to configure upstream Mooncake");
}

fn query_python_dev(candidate: &str) -> Option<(PathBuf, PathBuf, String)> {
    let script = r#"import sysconfig
include_dir = sysconfig.get_paths().get('include')
libdir = sysconfig.get_config_var('LIBDIR')
ldlibrary = sysconfig.get_config_var('LDLIBRARY')
if include_dir and libdir and ldlibrary:
    print(include_dir)
    print(libdir)
    print(ldlibrary)
"#;
    let output = Command::new(candidate).arg("-c").arg(script).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    let mut lines = stdout.lines();
    let include_dir = PathBuf::from(lines.next()?.trim());
    let library_dir = PathBuf::from(lines.next()?.trim());
    let library_file = lines.next()?.trim();
    let library_name = library_file
        .strip_prefix("lib")
        .and_then(|value| value.strip_suffix(".so"))
        .map(str::to_string)?;
    Some((include_dir, library_dir, library_name))
}

fn detect_jsoncpp() -> JsonCppConfig {
    let include_candidates = ["/usr/include/jsoncpp", "/usr/local/include/jsoncpp"];
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
        Command::new("cmake")
            .arg("--install")
            .arg(&ylt_build_dir),
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

    let transfer_engine = build_dir.join("mooncake-transfer-engine/src/libtransfer_engine.so");
    let tent_shared = build_dir.join("mooncake-transfer-engine/tent/src/libtent_shared.so");
    if transfer_engine.exists() && tent_shared.exists() {
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
                yalantinglibs_prefix.display()
            ))
            .arg(format!(
                "-Dyalantinglibs_DIR={}",
                yalantinglibs_prefix.join("lib/cmake/yalantinglibs").display()
            ))
            .arg(format!("-DPYTHON_EXECUTABLE={}", python.executable.display()))
            .arg(format!("-DPython_EXECUTABLE={}", python.executable.display()))
            .arg(format!("-DPython3_EXECUTABLE={}", python.executable.display()))
            .arg(format!("-DPython3_INCLUDE_DIR={}", python.include_dir.display()))
            .arg(format!("-DPython3_INCLUDE_DIRS={}", python.include_dir.display()))
            .arg(format!("-DPython3_LIBRARY={}", python.library_dir.join(format!("lib{}.so", python.library_name)).display()))
            .arg(format!("-DPython3_LIBRARIES={}", python.library_dir.join(format!("lib{}.so", python.library_name)).display()))
            .arg(format!("-DPython3_LIBRARY_DIRS={}", python.library_dir.display()))
            .arg(format!("-DPython3_RUNTIME_LIBRARY_DIRS={}", python.library_dir.display()))
            .arg(format!("-DPython3_LIBNAME={}", python.library_name))
            .arg(format!("-DJSONCPP_INCLUDE_DIR={}", jsoncpp.include_dir.display()))
            .arg(format!("-DJSONCPP_LIBRARY={}", jsoncpp.library_path.display()))
            .arg("-DWITH_TE=ON")
            .arg("-DWITH_STORE=OFF")
            .arg("-DWITH_STORE_RUST=OFF")
            .arg("-DBUILD_EXAMPLES=OFF")
            .arg("-DBUILD_UNIT_TESTS=OFF")
            .arg("-DUSE_TENT=ON")
            .arg("-DUSE_REDIS=ON")
            .arg("-DUSE_HTTP=OFF")
            .arg("-DUSE_ETCD=OFF")
            .arg("-DBUILD_SHARED_LIBS=ON"),
        "configure upstream Mooncake TE/TENT",
    );

    run(
        Command::new("cmake")
            .arg("--build")
            .arg(build_dir)
            .arg("--target")
            .arg("transfer_engine")
            .arg("tent_shared")
            .arg("-j8"),
        "build upstream Mooncake TE/TENT",
    );
}

fn build_native_shims(upstream_dir: &Path, out_dir: &Path) {
    let include = upstream_dir.join("mooncake-transfer-engine/include");
    let tent_include = upstream_dir.join("mooncake-transfer-engine/tent/include");

    build_native_shim(
        out_dir,
        "classic_shim.cc",
        "libmooncake_classic_shim.a",
        "compile classic transfer-engine shim",
        &[&include],
    );
    build_native_shim(
        out_dir,
        "tent_shim.cc",
        "libmooncake_tent_shim.a",
        "compile tent transfer-engine shim",
        &[&include, &tent_include],
    );
}

fn build_native_shim(
    out_dir: &Path,
    source_name: &str,
    archive_name: &str,
    description: &str,
    includes: &[&Path],
) {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir must exist"));
    let src = manifest_dir.join("src").join(source_name);
    let object = out_dir.join(format!("{source_name}.o"));
    let archive = out_dir.join(archive_name);
    let mut compile = Command::new("c++");
    compile.arg("-std=c++20").arg("-fPIC");
    for include in includes {
        compile.arg("-I").arg(include);
    }
    compile.arg("-c").arg(&src).arg("-o").arg(&object);
    run(&mut compile, description);

    if archive.exists() {
        std::fs::remove_file(&archive).unwrap_or_else(|error| {
            panic!(
                "failed to remove stale shim archive {}: {error}",
                archive.display()
            )
        });
    }

    run(
        Command::new("ar").arg("crus").arg(&archive).arg(&object),
        &format!("archive {}", archive_name),
    );
}

fn run(command: &mut Command, description: &str) {
    let status = command
        .status()
        .unwrap_or_else(|error| panic!("failed to {description}: {error}"));
    if !status.success() {
        panic!("failed to {description}: exit status {status}");
    }
}
