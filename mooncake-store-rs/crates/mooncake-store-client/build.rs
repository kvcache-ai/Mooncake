const SUPPORTED_KVCS_SDK_VERSION: &str = "0.4.0";
const DEFAULT_KVCS_SDK_ROOT: &str = "/opt/kvcs-sdk/latest";

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
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_OS");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_ENV");
    println!("cargo:rerun-if-env-changed=CARGO_CFG_TARGET_ARCH");
    if std::env::var_os("CARGO_FEATURE_KVCS_CAPI").is_some() {
        configure_kvcs_capi()?;
    }

    Ok(())
}

fn configure_kvcs_capi() -> Result<(), Box<dyn std::error::Error>> {
    let target_os = cargo_target_value("CARGO_CFG_TARGET_OS")?;
    let target_env = cargo_target_value("CARGO_CFG_TARGET_ENV")?;
    if target_os != "linux" || target_env != "gnu" {
        return Err(format!(
            "KVCS SDK {SUPPORTED_KVCS_SDK_VERSION} requires a Linux GNU target; got {target_os}-{target_env}"
        )
        .into());
    }
    let target_arch = cargo_target_value("CARGO_CFG_TARGET_ARCH")?;
    let expected_sdk_arch = match target_arch.as_str() {
        "x86_64" => "x86_64",
        "aarch64" => "aarch64",
        _ => {
            return Err(format!(
                "KVCS SDK {SUPPORTED_KVCS_SDK_VERSION} supports only x86_64 and aarch64; got {target_arch}"
            )
            .into())
        }
    };

    let root = std::env::var_os("KVCS_SDK_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(DEFAULT_KVCS_SDK_ROOT));
    let package_metadata = root.join("COMMIT_ID");
    let package_metadata_text = std::fs::read_to_string(&package_metadata).map_err(|error| {
        format!(
            "cannot read KVCS SDK package metadata {}: {error}; KVCS_SDK_ROOT must name the extracted SDK root",
            package_metadata.display()
        )
    })?;
    let version = package_metadata_value(&package_metadata_text, "version").ok_or_else(|| {
        format!(
            "KVCS SDK package metadata {} has no version entry",
            package_metadata.display()
        )
    })?;
    if version != SUPPORTED_KVCS_SDK_VERSION {
        return Err(format!(
            "unsupported KVCS SDK version {version}; this adapter requires {SUPPORTED_KVCS_SDK_VERSION}"
        )
        .into());
    }
    let sdk_arch = package_metadata_value(&package_metadata_text, "arch").ok_or_else(|| {
        format!(
            "KVCS SDK package metadata {} has no arch entry",
            package_metadata.display()
        )
    })?;
    if sdk_arch != expected_sdk_arch {
        return Err(format!(
            "KVCS SDK architecture {sdk_arch} does not match Cargo target architecture {target_arch}"
        )
        .into());
    }

    let use_mock = match std::env::var("KVCS_SDK_USE_MOCK") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "0" => false,
        Ok(value) if value == "1" => true,
        Ok(value) => return Err(format!("KVCS_SDK_USE_MOCK must be 0 or 1, got {value:?}").into()),
        Err(error) => return Err(format!("cannot read KVCS_SDK_USE_MOCK: {error}").into()),
    };
    let header = root.join("C/include/kvcs_capi.h");
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
    println!("cargo:rerun-if-changed={}", package_metadata.display());
    println!("cargo:rerun-if-changed={}", header.display());
    println!("cargo:rerun-if-changed={}", shared_library.display());
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib={link_name}");
    Ok(())
}

fn cargo_target_value(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    std::env::var(name).map_err(|error| format!("Cargo did not provide {name}: {error}").into())
}

fn package_metadata_value<'a>(metadata: &'a str, key: &str) -> Option<&'a str> {
    metadata.lines().find_map(|line| {
        let (candidate, value) = line.split_once('=')?;
        (candidate == key).then_some(value)
    })
}
