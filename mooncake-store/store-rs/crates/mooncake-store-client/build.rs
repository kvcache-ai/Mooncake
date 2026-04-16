use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/control_plane.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/control_plane.proto");
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_BUILD_DIR");

    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir must exist"));
    let upstream_dir = env_path("MOONCAKE_UPSTREAM_DIR")
        .unwrap_or_else(|| manifest_dir.join("../../third_party/Mooncake"));
    if !upstream_dir.exists() {
        panic!(
            "Mooncake upstream source was not found at {}. Run `git submodule update --init --recursive` or set MOONCAKE_UPSTREAM_DIR.",
            upstream_dir.display()
        );
    }
    let build_dir =
        env_path("MOONCAKE_UPSTREAM_BUILD_DIR").unwrap_or_else(|| upstream_dir.join("build-rust"));
    let classic_dir = build_dir.join("mooncake-transfer-engine/src");
    let tent_dir = build_dir.join("mooncake-transfer-engine/tent/src");

    #[cfg(not(target_os = "windows"))]
    {
        if classic_dir.exists() {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", classic_dir.display());
        } else {
            println!(
                "cargo:warning=classic_dir path does not exist: {}",
                classic_dir.display()
            );
        }
        if tent_dir.exists() {
            println!("cargo:rustc-link-arg=-Wl,-rpath,{}", tent_dir.display());
        } else {
            println!(
                "cargo:warning=tent_dir path does not exist: {}",
                tent_dir.display()
            );
        }
    }

    #[cfg(target_os = "windows")]
    {
        println!(
            "cargo:warning=On Windows, ensure the Mooncake libraries are discoverable via PATH or an equivalent DLL loading mechanism"
        );
    }

    Ok(())
}

fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key).map(PathBuf::from)
}
