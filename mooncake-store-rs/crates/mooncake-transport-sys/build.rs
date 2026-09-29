use std::env;
use std::path::PathBuf;

fn main() {
    for name in [
        "MOONCAKE_ROOT_DIR",
        "MOONCAKE_BUILD_DIR",
        "MOONCAKE_CLASSIC_SHIM_LIB_PATH",
        "MOONCAKE_TENT_SHIM_LIB_PATH",
        "MOONCAKE_SKIP_CLASSIC_TE",
        "MOONCAKE_SKIP_NATIVE_BUILD",
    ] {
        println!("cargo:rerun-if-env-changed={name}");
    }
    println!("cargo:rerun-if-changed=src/classic_shim.cc");
    println!("cargo:rerun-if-changed=src/tent_shim.cc");

    if skip_native_build() {
        println!("cargo:warning=skipping native path validation because MOONCAKE_SKIP_NATIVE_BUILD is enabled");
        return;
    }

    let root_dir = required_directory("MOONCAKE_ROOT_DIR");
    required_directory("MOONCAKE_BUILD_DIR");
    println!(
        "cargo:rerun-if-changed={}",
        root_dir.join("mooncake-common/FindYLT.cmake").display()
    );

    if !skip_classic_te() {
        let classic_shim = required_file("MOONCAKE_CLASSIC_SHIM_LIB_PATH");
        println!("cargo:rerun-if-changed={}", classic_shim.display());
    }

    let tent_shim = required_file("MOONCAKE_TENT_SHIM_LIB_PATH");
    println!("cargo:rerun-if-changed={}", tent_shim.display());
}

fn required_directory(key: &str) -> PathBuf {
    let value = env::var_os(key).unwrap_or_else(|| panic!("{key} must be set explicitly"));
    assert!(!value.is_empty(), "{key} must be a non-empty path");
    let path = PathBuf::from(value);
    assert!(
        path.is_absolute(),
        "{key} must be absolute: {}",
        path.display()
    );
    assert!(
        path.is_dir(),
        "{key} must be a directory: {}",
        path.display()
    );
    path
}

fn required_file(key: &str) -> PathBuf {
    let value = env::var_os(key).unwrap_or_else(|| panic!("{key} must be set explicitly"));
    assert!(!value.is_empty(), "{key} must be a non-empty path");
    let path = PathBuf::from(value);
    assert!(
        path.is_absolute(),
        "{key} must be absolute: {}",
        path.display()
    );
    assert!(path.is_file(), "{key} must name a file: {}", path.display());
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
