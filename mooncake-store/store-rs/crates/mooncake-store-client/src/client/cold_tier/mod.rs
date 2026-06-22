mod cleanup;
mod device;
mod helpers;
mod offload;
mod scheduler;
mod worker;

const DISABLE_COLD_TIER_OFFLOAD_ENV: &str = "MC_STORE_RS_DISABLE_COLD_TIER_OFFLOAD";
const LEGACY_SHORT_CIRCUIT_COLD_PATHS_ENV: &str = "MC_SHORT_CIRCUIT_COLD_PATHS";

pub(super) fn cold_tier_offload_disabled() -> bool {
    env_flag_enabled(DISABLE_COLD_TIER_OFFLOAD_ENV)
        || std::env::var_os(LEGACY_SHORT_CIRCUIT_COLD_PATHS_ENV).is_some()
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_offload_env<T>(
        disable_value: Option<&str>,
        legacy_value: Option<&str>,
        f: impl FnOnce() -> T,
    ) -> T {
        let _guard = crate::observability::test_process_lock().lock();
        let previous_disable = std::env::var(DISABLE_COLD_TIER_OFFLOAD_ENV).ok();
        let previous_legacy = std::env::var(LEGACY_SHORT_CIRCUIT_COLD_PATHS_ENV).ok();
        match disable_value {
            Some(value) => std::env::set_var(DISABLE_COLD_TIER_OFFLOAD_ENV, value),
            None => std::env::remove_var(DISABLE_COLD_TIER_OFFLOAD_ENV),
        }
        match legacy_value {
            Some(value) => std::env::set_var(LEGACY_SHORT_CIRCUIT_COLD_PATHS_ENV, value),
            None => std::env::remove_var(LEGACY_SHORT_CIRCUIT_COLD_PATHS_ENV),
        }
        let result = f();
        match previous_disable {
            Some(value) => std::env::set_var(DISABLE_COLD_TIER_OFFLOAD_ENV, value),
            None => std::env::remove_var(DISABLE_COLD_TIER_OFFLOAD_ENV),
        }
        match previous_legacy {
            Some(value) => std::env::set_var(LEGACY_SHORT_CIRCUIT_COLD_PATHS_ENV, value),
            None => std::env::remove_var(LEGACY_SHORT_CIRCUIT_COLD_PATHS_ENV),
        }
        result
    }

    #[test]
    fn offload_disable_env_parses_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            with_offload_env(Some(value), None, || assert!(cold_tier_offload_disabled()));
        }
    }

    #[test]
    fn offload_disable_env_ignores_falsey_values() {
        for value in ["0", "false", "FALSE", "no", "off", ""] {
            with_offload_env(Some(value), None, || assert!(!cold_tier_offload_disabled()));
        }
    }

    #[test]
    fn legacy_short_circuit_env_disables_offload() {
        with_offload_env(None, Some("1"), || assert!(cold_tier_offload_disabled()));
    }
}

#[cfg(test)]
#[allow(unused_imports)]
pub(super) use device::cold_tier_free_percentage;
pub(super) use helpers::{
    backend_remove_cold_payload, backend_remove_cold_payload_batch, backend_remove_pending_source,
    backend_store_cold_payload_batch, backend_store_pending_source, read_local_hot_replica_payload,
    same_cold_payload,
};
#[cfg(test)]
#[allow(unused_imports)]
pub(super) use offload::{checked_add_cold_tier_bytes, is_stale_pending_cold_backing_error};
#[allow(unused_imports)] // publish_pending_cold_backing_for_eviction used by PR6
pub(super) use offload::{
    enqueue_pending_offload, materialize_pending_offloads_bounded,
    materialize_prepared_pending_offload_batch, prepare_pending_offload_entries,
    prepare_pending_offload_entry, publish_initial_write_cold_backing,
    publish_pending_cold_backing_for_eviction,
};
pub(super) use scheduler::ColdTierHandle;
pub(super) use worker::{AsyncOffloadHandle, AsyncRestorePromotionHandle};
