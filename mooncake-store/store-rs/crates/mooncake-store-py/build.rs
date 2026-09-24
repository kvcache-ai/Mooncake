use std::env;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    shadow_rs::ShadowBuilder::builder().build()?;

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/dummy_store.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/dummy_store.proto");
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_UPSTREAM_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=PYTHON");
    println!("cargo:rerun-if-env-changed=PYO3_PYTHON");
    link_python_embed();
    Ok(())
}

fn link_python_embed() {
    let python = env::var("PYO3_PYTHON")
        .ok()
        .or_else(|| env::var("PYTHON").ok())
        .unwrap_or_else(|| "python3".to_string());
    let output = Command::new(&python)
        .arg("-c")
        .arg(
            "import sysconfig; \
             print(sysconfig.get_config_var('LIBDIR') or ''); \
             print(sysconfig.get_config_var('LDLIBRARY') or '')",
        )
        .output();
    let Ok(output) = output else {
        return;
    };
    if !output.status.success() {
        return;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let mut lines = text.lines();
    let Some(libdir) = lines.next().filter(|line| !line.is_empty()) else {
        return;
    };
    let Some(ldlibrary) = lines.next().filter(|line| !line.is_empty()) else {
        return;
    };
    let Some(name) = ldlibrary.strip_prefix("lib").map(|value| {
        value
            .strip_suffix(".so")
            .or_else(|| value.strip_suffix(".a"))
            .or_else(|| value.strip_suffix(".dylib"))
            .unwrap_or(value)
    }) else {
        return;
    };
    println!("cargo:rustc-link-search=native={libdir}");
    println!("cargo:rustc-link-lib={name}");
}
