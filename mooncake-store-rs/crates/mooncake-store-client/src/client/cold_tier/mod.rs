mod device;

use std::sync::OnceLock;

pub(super) fn cold_paths_short_circuited() -> bool {
    static SHORT_CIRCUIT: OnceLock<bool> = OnceLock::new();
    *SHORT_CIRCUIT.get_or_init(|| std::env::var_os("MC_SHORT_CIRCUIT_COLD_PATHS").is_some())
}
