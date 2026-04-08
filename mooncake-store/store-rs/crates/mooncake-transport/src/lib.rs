#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum TransportEngineKind {
    ClassicTe,
    Tent,
}

pub fn default_upstream_build_dir() -> &'static str {
    mooncake_transport_sys::DEFAULT_UPSTREAM_BUILD_DIR
}
