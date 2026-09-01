use super::super::{
    ClientLease, ClientLifecycleState, ClientRuntimeId, ObjectRoute, ReplicaRoute, ResolvedObject,
    Result, StoreClient, StoreError,
};
use super::{cold_backing_placeholder, materialized_cold_backing, select_cold_backing_target};
use mooncake_store_core::ColdTierDeviceState;
use std::collections::{BTreeSet, VecDeque};

pub(in super::super) fn resolved_uses_cold_backing(entry: &ResolvedObject) -> bool {
    materialized_cold_backing(&entry.route).is_some()
        && (entry.route.replicas.is_empty() || entry.replica.segment_name.0 == "__cold_backing__")
}

impl StoreClient {
    pub(in super::super) fn select_cold_backing_owner_lease(
        owner: &ClientRuntimeId,
        leases: impl IntoIterator<Item = ClientLease>,
    ) -> Option<ClientLease> {
        let mut exact_readable = None;
        let mut newest_active_incarnation = None;
        for lease in leases {
            if lease.runtime == *owner
                && matches!(
                    lease.state,
                    ClientLifecycleState::Active | ClientLifecycleState::Draining
                )
            {
                exact_readable = Some(lease.clone());
            }
            if lease.runtime.stable_id == owner.stable_id
                && matches!(lease.state, ClientLifecycleState::Active)
            {
                let replace =
                    newest_active_incarnation
                        .as_ref()
                        .is_none_or(|current: &ClientLease| {
                            lease.runtime.epoch > current.runtime.epoch
                        });
                if replace {
                    newest_active_incarnation = Some(lease);
                }
            }
        }
        newest_active_incarnation.or(exact_readable)
    }

    pub(in super::super) fn lookup_cold_backing_owner_lease_once(
        &self,
        owner: &ClientRuntimeId,
        force_refresh: bool,
    ) -> Result<ClientLease> {
        Self::select_cold_backing_owner_lease(
            owner,
            self.available_compatible_live_clients(force_refresh)?,
        )
        .ok_or_else(|| {
            StoreError::NotFound(format!("cold backing owner {} is not available", owner))
        })
    }

    pub(in super::super) fn lookup_cold_backing_owner_lease(
        &self,
        owner: &ClientRuntimeId,
    ) -> Result<ClientLease> {
        match self.lookup_cold_backing_owner_lease_once(owner, false) {
            Ok(lease) => Ok(lease),
            Err(StoreError::NotFound(_)) => self.lookup_cold_backing_owner_lease_once(owner, true),
            Err(error) => Err(error),
        }
    }

    pub(in super::super) fn resolve_cold_backing_read(
        &self,
        mut route: ObjectRoute,
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
            self.storage_owner
                .cold_tier_devices
                .runtime_backend_available(id)
        });
        if self
            .storage_owner
            .cold_tier_devices
            .has_runtime_backend(&cold_backing.cold_tier_id)
        {
            if !self
                .storage_owner
                .cold_tier_devices
                .runtime_backend_available(&cold_backing.cold_tier_id)
            {
                return Err(StoreError::NotFound(format!(
                    "tenant={tenant} key={logical_key}: persistent target {} is unavailable",
                    cold_backing.cold_tier_id
                )));
            }
            cold_backing.replicas.retain(|replica| {
                self.storage_owner
                    .cold_tier_devices
                    .runtime_backend_available(&replica.cold_tier_id)
            });
            let mut selector = super::target_selection::ReplicaTargetSelector::new(
                self.storage_owner.cold_tier_devices.admission(),
            );
            let selected = selector.select_target(&cold_backing);
            super::target_selection::promote_cold_backing_target(&mut cold_backing, &selected);
            if route.nof_backing.is_some() {
                route.nof_backing = Some(super::nof::cold_as_nof(&cold_backing));
            } else {
                route.cold_backing = Some(cold_backing.clone());
            }
        }
        if !self
            .storage_owner
            .cold_tier_devices
            .has_runtime_backend(&cold_backing.cold_tier_id)
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
