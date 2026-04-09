use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_BUILD_DIR");

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir must exist"));
    let upstream_dir =
        env_path("MOONCAKE_UPSTREAM_DIR").unwrap_or_else(|| manifest_dir.join("../../third_party/Mooncake"));
    let build_dir =
        env_path("MOONCAKE_UPSTREAM_BUILD_DIR").unwrap_or_else(|| upstream_dir.join("build-rust"));
    let classic_dir = build_dir.join("mooncake-transfer-engine/src");
    let tent_dir = build_dir.join("mooncake-transfer-engine/tent/src");

    println!(
        "cargo:rustc-link-arg-bin=mooncake-store-client=-Wl,-rpath,{}",
        classic_dir.display()
    );
    println!(
        "cargo:rustc-link-arg-bin=mooncake-store-client=-Wl,-rpath,{}",
        tent_dir.display()
    );
}

fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key).map(PathBuf::from)
}
