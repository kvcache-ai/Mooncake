mod cleanup;
mod control;
mod device;
mod helpers;
mod offload;
mod restore;
mod scheduler;
mod staging_pool;
mod target_selection;
mod worker;

const ENABLE_COLD_TIER_ENV: &str = "MC_STORE_RS_ENABLE_COLD_TIER";

pub(super) fn cold_tier_enabled() -> bool {
    env_flag_enabled(ENABLE_COLD_TIER_ENV)
}

pub(super) fn cold_tier_disabled() -> bool {
    !cold_tier_enabled()
}

pub(super) fn cold_paths_short_circuited() -> bool {
    static SHORT_CIRCUIT: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SHORT_CIRCUIT.get_or_init(|| std::env::var_os("MC_SHORT_CIRCUIT_COLD_PATHS").is_some())
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

    fn with_cold_tier_env<T>(enable_value: Option<&str>, f: impl FnOnce() -> T) -> T {
        let _guard = crate::observability::test_process_lock().lock();
        let previous_enable = std::env::var(ENABLE_COLD_TIER_ENV).ok();
        match enable_value {
            Some(value) => std::env::set_var(ENABLE_COLD_TIER_ENV, value),
            None => std::env::remove_var(ENABLE_COLD_TIER_ENV),
        }
        let result = f();
        match previous_enable {
            Some(value) => std::env::set_var(ENABLE_COLD_TIER_ENV, value),
            None => std::env::remove_var(ENABLE_COLD_TIER_ENV),
        }
        result
    }

    #[test]
    fn cold_tier_enable_env_defaults_to_disabled() {
        with_cold_tier_env(None, || assert!(cold_tier_disabled()));
    }

    #[test]
    fn cold_tier_enable_env_parses_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "on"] {
            with_cold_tier_env(Some(value), || assert!(cold_tier_enabled()));
        }
    }

    #[test]
    fn cold_tier_enable_env_ignores_falsey_values() {
        for value in ["0", "false", "FALSE", "no", "off", ""] {
            with_cold_tier_env(Some(value), || assert!(cold_tier_disabled()));
        }
    }
}

#[cfg(test)]
#[allow(unused_imports)]
pub(super) use device::cold_tier_free_percentage;
pub(super) use helpers::{
    backend_remove_cold_payload, backend_remove_cold_payload_batch, backend_remove_pending_source,
    backend_store_cold_payload_batch, backend_store_pending_source, cold_backing_placeholder,
    materialized_cold_backing, read_local_hot_replica_payload, same_cold_payload,
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
#[cfg(test)]
#[allow(unused_imports)]
pub(super) use restore::restore_payload_from_cold_backing;
#[allow(unused_imports)]
pub(super) use restore::{
    batch_read_from_cold_staged, enqueue_restore_promotion, execute_local_restore_batch_reads,
    execute_owner_cold_restore_promote_phase, execute_owner_cold_restore_ssd_phase_staging,
    promote_owned_materialized_route_by_key, read_from_cold_one_shot,
    trigger_remote_owner_cold_restore, try_init_staging_pool, wait_for_restore_promotions,
    PromoteTimingBreakdown,
};
pub(super) use scheduler::ColdTierHandle;
pub(super) use staging_pool::{ColdRestoreStagingPool, StagingSlot};
pub(super) use worker::{AsyncOffloadHandle, AsyncRestorePromotionHandle};
