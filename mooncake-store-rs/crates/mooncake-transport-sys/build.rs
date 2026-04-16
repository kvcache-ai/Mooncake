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
