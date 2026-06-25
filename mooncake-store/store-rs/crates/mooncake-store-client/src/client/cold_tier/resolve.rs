use super::super::{
    ClientRuntimeId, ObjectRoute, ReplicaRoute, ResolvedObject, Result, StoreClient, StoreError,
};
use super::{cold_backing_placeholder, materialized_cold_backing, select_cold_backing_target};
use mooncake_store_core::ColdTierDeviceState;
use std::collections::{BTreeSet, VecDeque};

pub(in super::super) fn resolved_uses_cold_backing(entry: &ResolvedObject) -> bool {
    materialized_cold_backing(&entry.route).is_some()
        && (entry.route.replicas.is_empty() || entry.replica.segment_name.0 == "__cold_backing__")
}

impl StoreClient {
    pub(in super::super) fn resolve_cold_backing_read(
        &self,
        route: ObjectRoute,
        tenant: &str,
        logical_key: &str,
        readable_runtimes: &BTreeSet<ClientRuntimeId>,
    ) -> Result<ResolvedObject> {
        if super::cold_tier_disabled() {
            return Err(StoreError::NotFound(format!(
                "tenant={tenant} key={logical_key} has no readable replica owner"
            )));
        }
        let Some(mut cold_backing) = materialized_cold_backing(&route) else {
            return Err(StoreError::NotFound(format!(
                "tenant={tenant} key={logical_key} has no readable replica owner"
            )));
        };
        select_cold_backing_target(&mut cold_backing, |id| {
            self.storage_owner.cold_tier_devices.has_local_backend(id)
        });
        if !self
            .storage_owner
            .cold_tier_devices
            .has_local_backend(&cold_backing.cold_tier_id)
        {
            if self
                .storage_owner
                .cold_tier_devices
                .device_state(&cold_backing.cold_tier_id)
                == Some(ColdTierDeviceState::Unregistered)
            {
                return Err(StoreError::NotFound(format!(
                    "tenant={tenant} key={logical_key}: cold tier device {} is unregistered (owner shut down)",
                    cold_backing.cold_tier_id
                )));
            }
            if !readable_runtimes.contains(&cold_backing.owner) {
                let refreshed_for_cold = self.readable_runtime_set(true)?;
                if !refreshed_for_cold.contains(&cold_backing.owner) {
                    let has_live_incarnation = self
                        .metadata
                        .get_live_runtime_by_stable_id(&cold_backing.owner.stable_id)
                        .ok()
                        .flatten()
                        .is_some();
                    if !has_live_incarnation {
                        return Err(StoreError::NotFound(format!(
                            "tenant={tenant} key={logical_key}: cold backing owner {} is not readable",
                            cold_backing.owner
                        )));
                    }
                }
            }
        }
        Ok(ResolvedObject {
            tenant: tenant.to_string(),
            key: logical_key.to_string(),
            route,
            replica: cold_backing_placeholder(&cold_backing),
            fallback_replicas: VecDeque::<ReplicaRoute>::new(),
        })
    }
}
