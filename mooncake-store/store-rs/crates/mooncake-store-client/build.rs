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

    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_KVCS_CAPI");
    println!("cargo:rerun-if-env-changed=KVCS_SDK_ROOT");
    println!("cargo:rerun-if-env-changed=KVCS_SDK_USE_MOCK");
    if std::env::var_os("CARGO_FEATURE_KVCS_CAPI").is_some() {
        let root = std::env::var_os("KVCS_SDK_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("/opt/kvcs-sdk/latest"));
        let header = root.join("C/include/kvcs_capi.h");
        let use_mock = match std::env::var("KVCS_SDK_USE_MOCK") {
            Err(std::env::VarError::NotPresent) => false,
            Ok(value) if value == "0" => false,
            Ok(value) if value == "1" => true,
            Ok(value) => {
                return Err(format!("KVCS_SDK_USE_MOCK must be 0 or 1, got {value:?}").into())
            }
            Err(error) => return Err(format!("cannot read KVCS_SDK_USE_MOCK: {error}").into()),
        };
        let lib_dir = if use_mock {
            root.join("mock/lib")
        } else {
            root.join("lib")
        };
        if !header.is_file() {
            return Err(format!(
                "KVCS SDK C ABI header does not exist: {}; set KVCS_SDK_ROOT to an extracted SDK root",
                header.display()
            )
            .into());
        }
        let link_name = if use_mock { "kvcsmock" } else { "kvcs" };
        let shared_library = lib_dir.join(format!("lib{link_name}.so"));
        if !shared_library.is_file() {
            return Err(format!(
                "KVCS SDK shared library was not found under {}; expected lib{link_name}.so",
                lib_dir.display(),
            )
            .into());
        }
        println!("cargo:rerun-if-changed={}", header.display());
        println!("cargo:rerun-if-changed={}", shared_library.display());
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
        println!("cargo:rustc-link-lib=dylib={link_name}");
    }

    Ok(())
}
