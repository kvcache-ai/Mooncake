use shadow_rs::shadow;

shadow!(build);

pub fn long_version() -> String {
    format!(
        "{}\nbranch: {}\ncommit: {}\nbuild_time: {}\nbuild_env: {}, {}",
        build::PKG_VERSION,
        build::BRANCH,
        build::SHORT_COMMIT,
        build::BUILD_TIME,
        build::RUST_VERSION,
        build::BUILD_RUST_CHANNEL,
    )
}

static LONG_VERSION: std::sync::LazyLock<String> = std::sync::LazyLock::new(long_version);

pub fn long_version_static() -> &'static str {
    &LONG_VERSION
}

pub fn log_build_info() {
    tracing::info!(
        version = build::PKG_VERSION,
        branch = build::BRANCH,
        commit = build::SHORT_COMMIT,
        build_time = build::BUILD_TIME,
        build_env = format_args!("{}, {}", build::RUST_VERSION, build::BUILD_RUST_CHANNEL),
        "build info",
    );
}
