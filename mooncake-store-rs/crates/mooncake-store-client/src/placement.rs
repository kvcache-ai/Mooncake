use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use mooncake_store_core::{
    ClientLease, ClientLifecycleState, ClientRuntimeId, MetadataBackend, Result, StoreError,
};

use crate::{ObjectRef, StoreClient};

const DEFAULT_SCOPE_LABEL: &str = "pool";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementChoice {
    pub tenant: String,
    pub key: String,
    pub owners: Vec<ClientRuntimeId>,
}

#[derive(Clone)]
pub struct PlacementPlanner {
    metadata: Arc<dyn MetadataBackend>,
    scope_label_key: String,
    required_labels: BTreeMap<String, String>,
}

impl PlacementPlanner {
    pub fn new(metadata: Arc<dyn MetadataBackend>) -> Self {
        Self {
            metadata,
            scope_label_key: DEFAULT_SCOPE_LABEL.to_string(),
            required_labels: BTreeMap::new(),
        }
    }

    pub fn scope_label(mut self, key: impl Into<String>) -> Self {
        self.scope_label_key = key.into();
        self
    }

    pub fn require_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.required_labels.insert(key.into(), value.into());
        self
    }

    pub fn plan(
        &self,
        observer: &StoreClient,
        objects: &[ObjectRef<'_>],
        replica_count: usize,
    ) -> Result<Vec<PlacementChoice>> {
        self.plan_from_lease(
            observer.lease(),
            observer.default_tenant(),
            objects,
            replica_count,
        )
    }

    pub fn ranked_candidates(
        &self,
        observer: &StoreClient,
        object: &ObjectRef<'_>,
    ) -> Result<Vec<ClientRuntimeId>> {
        self.ranked_candidates_from_lease(observer.lease(), observer.default_tenant(), object)
    }

    pub fn rank_many(
        &self,
        observer: &StoreClient,
        objects: &[ObjectRef<'_>],
    ) -> Result<Vec<PlacementChoice>> {
        self.rank_many_from_lease(observer.lease(), observer.default_tenant(), objects)
    }

    pub fn plan_from_lease(
        &self,
        observer: &ClientLease,
        default_tenant: &str,
        objects: &[ObjectRef<'_>],
        replica_count: usize,
    ) -> Result<Vec<PlacementChoice>> {
        if replica_count == 0 {
            return Err(StoreError::InvalidState(
                "replica_count must be greater than zero".to_string(),
            ));
        }
        let candidates = self.candidates(observer)?;
        if candidates.len() < replica_count {
            return Err(StoreError::InvalidState(format!(
                "not enough active placement candidates: have={} need={replica_count}",
                candidates.len()
            )));
        }

        let mut plans = Vec::with_capacity(objects.len());
        for object in objects {
            let tenant = object.tenant.unwrap_or(default_tenant).to_string();
            let scored = self.rank_candidates(&tenant, object.key, &candidates);
            plans.push(PlacementChoice {
                tenant,
                key: object.key.to_string(),
                owners: scored.into_iter().take(replica_count).collect(),
            });
        }
        Ok(plans)
    }

    pub fn ranked_candidates_from_lease(
        &self,
        observer: &ClientLease,
        default_tenant: &str,
        object: &ObjectRef<'_>,
    ) -> Result<Vec<ClientRuntimeId>> {
        let tenant = object.tenant.unwrap_or(default_tenant);
        let candidates = self.candidates(observer)?;
        Ok(self.rank_candidates(tenant, object.key, &candidates))
    }

    pub fn rank_many_from_lease(
        &self,
        observer: &ClientLease,
        default_tenant: &str,
        objects: &[ObjectRef<'_>],
    ) -> Result<Vec<PlacementChoice>> {
        let candidates = self.candidates(observer)?;
        let mut plans = Vec::with_capacity(objects.len());
        for object in objects {
            let tenant = object.tenant.unwrap_or(default_tenant).to_string();
            let owners = self.rank_candidates(&tenant, object.key, &candidates);
            plans.push(PlacementChoice {
                tenant,
                key: object.key.to_string(),
                owners,
            });
        }
        Ok(plans)
    }

    fn candidates(&self, observer: &ClientLease) -> Result<Vec<ClientLease>> {
        let pool = observer
            .endpoints
            .labels
            .get(&self.scope_label_key)
            .cloned();
        let mut candidates = self
            .metadata
            .list_live_clients()?
            .into_iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .filter(|lease| is_compatible(observer, lease))
            .filter(|lease| {
                pool.as_ref().is_none_or(|pool| {
                    lease
                        .endpoints
                        .labels
                        .get(&self.scope_label_key)
                        .is_some_and(|value| value == pool)
                })
            })
            .filter(|lease| {
                self.required_labels.iter().all(|(key, value)| {
                    lease
                        .endpoints
                        .labels
                        .get(key)
                        .is_some_and(|candidate| candidate == value)
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.runtime.cmp(&right.runtime));
        Ok(candidates)
    }

    fn rank_candidates(
        &self,
        tenant: &str,
        key: &str,
        candidates: &[ClientLease],
    ) -> Vec<ClientRuntimeId> {
        let mut scored = candidates
            .iter()
            .map(|candidate| {
                (
                    rendezvous_score(tenant, key, &candidate.runtime),
                    candidate.runtime.clone(),
                )
            })
            .collect::<Vec<_>>();
        scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        scored.into_iter().map(|(_, runtime)| runtime).collect()
    }
}

fn is_compatible(left: &ClientLease, right: &ClientLease) -> bool {
    left.compatibility.store_api_version == right.compatibility.store_api_version
        && left.compatibility.metadata_schema_version == right.compatibility.metadata_schema_version
        && left.compatibility.transport_api_version == right.compatibility.transport_api_version
}

fn rendezvous_score(tenant: &str, key: &str, runtime: &ClientRuntimeId) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    tenant.hash(&mut hasher);
    key.hash(&mut hasher);
    runtime.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        CompatibilityDescriptor, MetadataBackend, SegmentName,
    };

    use super::PlacementPlanner;
    use crate::ObjectRef;

    #[test]
    fn planner_filters_to_active_storage_nodes_in_same_pool() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        publish_client(
            metadata.as_ref(),
            "writer",
            1,
            ClientLifecycleState::Active,
            "pool-a",
            "false",
        );
        publish_client(
            metadata.as_ref(),
            "storage-a",
            1,
            ClientLifecycleState::Active,
            "pool-a",
            "true",
        );
        publish_client(
            metadata.as_ref(),
            "storage-b",
            1,
            ClientLifecycleState::Standby,
            "pool-a",
            "true",
        );
        publish_client(
            metadata.as_ref(),
            "storage-c",
            1,
            ClientLifecycleState::Active,
            "pool-b",
            "true",
        );

        let observer = metadata
            .list_live_clients()
            .expect("list should work")
            .into_iter()
            .find(|lease| lease.runtime.stable_id.0 == "writer")
            .expect("writer should exist");

        let planner = PlacementPlanner::new(metadata).require_label("storage", "true");
        let plans = planner
            .plan_from_lease(
                &observer,
                "tenant-a",
                &[ObjectRef::new("key-1").tenant("tenant-a")],
                1,
            )
            .expect("plan should work");

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].owners.len(), 1);
        assert_eq!(plans[0].owners[0].stable_id.0, "storage-a");
    }

    #[test]
    fn planner_spreads_keys_across_active_nodes() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        publish_client(
            metadata.as_ref(),
            "writer",
            1,
            ClientLifecycleState::Active,
            "pool-a",
            "false",
        );
        publish_client(
            metadata.as_ref(),
            "storage-a",
            1,
            ClientLifecycleState::Active,
            "pool-a",
            "true",
        );
        publish_client(
            metadata.as_ref(),
            "storage-b",
            1,
            ClientLifecycleState::Active,
            "pool-a",
            "true",
        );

        let observer = metadata
            .list_live_clients()
            .expect("list should work")
            .into_iter()
            .find(|lease| lease.runtime.stable_id.0 == "writer")
            .expect("writer should exist");
        let planner = PlacementPlanner::new(metadata).require_label("storage", "true");
        let refs = (0..64)
            .map(|index| {
                ObjectRef::new(Box::leak(format!("key-{index}").into_boxed_str()))
                    .tenant("tenant-a")
            })
            .collect::<Vec<_>>();
        let plans = planner
            .plan_from_lease(&observer, "tenant-a", &refs, 1)
            .expect("plan should work");

        let mut owners = std::collections::BTreeMap::<String, usize>::new();
        for plan in plans {
            *owners
                .entry(plan.owners[0].stable_id.0.clone())
                .or_default() += 1;
        }
        assert_eq!(owners.len(), 2);
    }

    #[test]
    fn planner_filters_incompatible_storage_nodes() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        publish_client(
            metadata.as_ref(),
            "writer",
            1,
            ClientLifecycleState::Active,
            "pool-a",
            "false",
        );
        publish_client_with_compat(
            metadata.as_ref(),
            "storage-a",
            1,
            ClientLifecycleState::Active,
            "pool-a",
            "true",
            CompatibilityDescriptor::default(),
        );
        let incompatible = CompatibilityDescriptor {
            transport_api_version: 2,
            ..CompatibilityDescriptor::default()
        };
        publish_client_with_compat(
            metadata.as_ref(),
            "storage-b",
            1,
            ClientLifecycleState::Active,
            "pool-a",
            "true",
            incompatible,
        );

        let observer = metadata
            .list_live_clients()
            .expect("list should work")
            .into_iter()
            .find(|lease| lease.runtime.stable_id.0 == "writer")
            .expect("writer should exist");
        let planner = PlacementPlanner::new(metadata).require_label("storage", "true");

        let error = planner
            .plan_from_lease(
                &observer,
                "tenant-a",
                &[ObjectRef::new("key-1").tenant("tenant-a")],
                2,
            )
            .expect_err("incompatible node should not count as a candidate");
        assert!(error
            .to_string()
            .contains("not enough active placement candidates"));
    }

    fn publish_client(
        metadata: &InMemoryMetadataBackend,
        stable_id: &str,
        epoch: u64,
        state: ClientLifecycleState,
        pool: &str,
        storage: &str,
    ) {
        publish_client_with_compat(
            metadata,
            stable_id,
            epoch,
            state,
            pool,
            storage,
            CompatibilityDescriptor::default(),
        );
    }

    fn publish_client_with_compat(
        metadata: &InMemoryMetadataBackend,
        stable_id: &str,
        epoch: u64,
        state: ClientLifecycleState,
        pool: &str,
        storage: &str,
        compatibility: CompatibilityDescriptor,
    ) {
        let runtime = ClientRuntimeId::new(stable_id, ClientEpoch(epoch));
        let mut endpoints = ClientEndpointSet {
            rpc_address: "127.0.0.1:0".to_string(),
            segment_name: Some(SegmentName::new(format!("{stable_id}-segment"))),
            labels: Default::default(),
        };
        endpoints
            .labels
            .insert("pool".to_string(), pool.to_string());
        endpoints
            .labels
            .insert("storage".to_string(), storage.to_string());
        metadata
            .upsert_client_lease(&ClientLease {
                runtime,
                state,
                compatibility,
                endpoints,
                expires_at_ms: 10_000,
            })
            .expect("lease should upsert");
    }
}
