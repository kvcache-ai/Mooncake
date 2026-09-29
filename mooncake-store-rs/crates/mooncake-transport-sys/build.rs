use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MOONCAKE_ROOT_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_SKIP_CLASSIC_TE");
    println!("cargo:rerun-if-env-changed=MOONCAKE_SKIP_NATIVE_BUILD");
    println!("cargo:rerun-if-changed=src/classic_shim.cc");
    println!("cargo:rerun-if-changed=src/tent_shim.cc");

    if skip_native_build() {
        println!("cargo:warning=skipping Mooncake native shim build because MOONCAKE_SKIP_NATIVE_BUILD is enabled");
        return;
    }

    let upstream_dir = required_directory("MOONCAKE_ROOT_DIR");
    let build_dir = required_directory("MOONCAKE_BUILD_DIR");
    println!(
        "cargo:rerun-if-changed={}",
        upstream_dir.join("mooncake-common/FindYLT.cmake").display()
    );

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set by Cargo"));
    build_native_shims(&upstream_dir, &build_dir, &out_dir);
}

fn required_directory(key: &str) -> PathBuf {
    let value = env::var_os(key).unwrap_or_else(|| {
        panic!("{key} must be provided by CMake or set explicitly for Cargo builds")
    });
    assert!(
        !value.is_empty(),
        "{key} must be a non-empty path provided by CMake or explicitly for Cargo builds"
    );

    let path = PathBuf::from(value);
    assert!(
        path.is_absolute(),
        "{key} must be an absolute path: {}",
        path.display()
    );
    assert!(
        path.is_dir(),
        "{key} must name an existing directory: {}",
        path.display()
    );
    path
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

fn build_native_shims(upstream_dir: &Path, build_dir: &Path, out_dir: &Path) {
    let include = upstream_dir.join("mooncake-transfer-engine/include");
    let tent_include = upstream_dir.join("mooncake-transfer-engine/tent/include");
    let common_include = upstream_dir.join("mooncake-common/include");
    let ylt_include = build_dir.join("_deps/yalantinglibs-src/include");
    let ylt_thirdparty = ylt_include.join("ylt/thirdparty");
    let ylt_standalone = ylt_include.join("ylt/standalone");
    let classic_dir = build_dir.join("mooncake-transfer-engine/src");
    let tent_dir = build_dir.join("mooncake-transfer-engine/tent/src");

    if !skip_classic_te() {
        build_native_shim(
            out_dir,
            "classic_shim.cc",
            "libmooncake_classic_shim.so",
            "compile classic transfer-engine shim",
            &[
                &include,
                &common_include,
                &ylt_include,
                &ylt_thirdparty,
                &ylt_standalone,
            ],
            &[(&classic_dir, "transfer_engine")],
        );
    }
    build_native_shim(
        out_dir,
        "tent_shim.cc",
        "libmooncake_tent_shim.so",
        "compile tent transfer-engine shim",
        &[
            &include,
            &tent_include,
            &common_include,
            &ylt_include,
            &ylt_thirdparty,
            &ylt_standalone,
        ],
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
