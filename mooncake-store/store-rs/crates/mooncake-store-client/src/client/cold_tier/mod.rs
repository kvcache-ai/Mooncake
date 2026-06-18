// Cold tier submodule.
//
// Organizes cold tier implementation into focused modules:
// - device: device management, lifecycle, admission control

mod device;

use std::sync::OnceLock;

/// Returns true if cold tier code paths should be skipped entirely.
///
/// Set `MC_SHORT_CIRCUIT_COLD_PATHS=1` to bypass all cold tier logic
/// (offload, restore, cleanup) without removing the code.  Useful for
/// debugging or isolating hot-path-only behavior during incidents.
pub(super) fn cold_paths_short_circuited() -> bool {
    static SHORT_CIRCUIT: OnceLock<bool> = OnceLock::new();
    *SHORT_CIRCUIT.get_or_init(|| std::env::var_os("MC_SHORT_CIRCUIT_COLD_PATHS").is_some())
}
