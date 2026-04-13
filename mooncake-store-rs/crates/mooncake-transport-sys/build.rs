use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_BUILD_DIR");
    println!("cargo:rerun-if-changed=../../third_party/Mooncake");
    println!("cargo:rerun-if-changed=src/classic_shim.cc");

    let upstream_dir = env_path("MOONCAKE_UPSTREAM_DIR").unwrap_or_else(default_upstream_dir);
    let build_dir =
        env_path("MOONCAKE_UPSTREAM_BUILD_DIR").unwrap_or_else(|| upstream_dir.join("build-rust"));
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR must be set by Cargo"));

    ensure_upstream_native_artifacts(&upstream_dir, &build_dir);
    build_classic_shim(&upstream_dir, &out_dir);

    let classic_dir = build_dir.join("mooncake-transfer-engine/src");
    let tent_dir = build_dir.join("mooncake-transfer-engine/tent/src");

    println!("cargo:rustc-link-search=native={}", classic_dir.display());
    println!("cargo:rustc-link-search=native={}", tent_dir.display());
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=mooncake_classic_shim");
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

fn ensure_upstream_native_artifacts(upstream_dir: &Path, build_dir: &Path) {
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

fn build_classic_shim(upstream_dir: &Path, out_dir: &Path) {
    let src = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir must exist"))
        .join("src/classic_shim.cc");
    let object = out_dir.join("classic_shim.o");
    let archive = out_dir.join("libmooncake_classic_shim.a");
    let include = upstream_dir.join("mooncake-transfer-engine/include");

    run(
        Command::new("c++")
            .arg("-std=c++20")
            .arg("-fPIC")
            .arg("-I")
            .arg(&include)
            .arg("-c")
            .arg(&src)
            .arg("-o")
            .arg(&object),
        "compile classic transfer-engine shim",
    );

    if archive.exists() {
        std::fs::remove_file(&archive).unwrap_or_else(|error| {
            panic!(
                "failed to remove stale classic shim archive {}: {error}",
                archive.display()
            )
        });
    }

    run(
        Command::new("ar").arg("crus").arg(&archive).arg(&object),
        "archive classic transfer-engine shim",
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
