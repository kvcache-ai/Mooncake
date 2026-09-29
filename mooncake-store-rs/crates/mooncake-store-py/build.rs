fn main() -> Result<(), Box<dyn std::error::Error>> {
    shadow_rs::ShadowBuilder::builder().build()?;

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/dummy_store.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/dummy_store.proto");
    println!("cargo:rerun-if-env-changed=MOONCAKE_ROOT_DIR");
    println!("cargo:rerun-if-env-changed=MOONCAKE_BUILD_DIR");
    println!("cargo:rerun-if-env-changed=PYO3_PYTHON");
    Ok(())
}
