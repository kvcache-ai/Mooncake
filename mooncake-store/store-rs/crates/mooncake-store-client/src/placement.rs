use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use mooncake_store_core::{ClientLease, ClientLifecycleState, ClientRuntimeId, MetadataBackend, Result, StoreError};

use crate::{ObjectRef, StoreClient};

const DEFAULT_SCOPE_LABEL: &str = "pool";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementChoice {
    pub tenant: String,
    pub key: String,
    pub owners: Vec<ClientRuntimeId>,
}

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
        self.plan_from_lease(observer.lease(), observer.default_tenant(), objects, replica_count)
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
            let mut scored = candidates
                .iter()
                .map(|candidate| {
                    (
                        rendezvous_score(&tenant, object.key, &candidate.runtime),
                        candidate.runtime.clone(),
                    )
                })
                .collect::<Vec<_>>();
            scored.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
            plans.push(PlacementChoice {
                tenant,
                key: object.key.to_string(),
                owners: scored
                    .into_iter()
                    .take(replica_count)
                    .map(|(_, runtime)| runtime)
                    .collect(),
            });
        }
        Ok(plans)
    }

    fn candidates(&self, observer: &ClientLease) -> Result<Vec<ClientLease>> {
        let pool = observer.endpoints.labels.get(&self.scope_label_key).cloned();
        let mut candidates = self
            .metadata
            .list_live_clients()?
            .into_iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active)
            .filter(|lease| is_compatible(observer, lease))
            .filter(|lease| {
                pool.as_ref().is_none_or(|pool| {
                    lease.endpoints
                        .labels
                        .get(&self.scope_label_key)
                        .is_some_and(|value| value == pool)
                })
            })
            .filter(|lease| {
                self.required_labels.iter().all(|(key, value)| {
                    lease.endpoints
                        .labels
                        .get(key)
                        .is_some_and(|candidate| candidate == value)
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| left.runtime.cmp(&right.runtime));
        Ok(candidates)
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
