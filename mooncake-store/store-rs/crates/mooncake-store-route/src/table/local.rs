use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::Hash;

use mooncake_store_core::{
    route_reuse_identity, CasResult, ClientRuntimeId, NamespaceScope, ObjectKey, ObjectRoute,
    ReuseIdentity, RouteCasRequest, RouteVersion,
};

use crate::metrics::record_cas_outcome;

#[derive(Default)]
pub(crate) struct LocalRouteTable {
    routes: HashMap<String, ObjectRoute>,
    scope_index: HashMap<NamespaceScope, HashSet<String>>,
    reuse_index: HashMap<ReuseIdentity, HashSet<String>>,
    version_floors: HashMap<String, RouteVersion>,
}

impl LocalRouteTable {
    pub(crate) fn get(&self, key: &ObjectKey) -> Option<ObjectRoute> {
        self.routes.get(&key.0).cloned()
    }

    pub(crate) fn get_many(&self, keys: &[ObjectKey]) -> Vec<Option<ObjectRoute>> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    pub(crate) fn contains_many(&self, keys: &[ObjectKey]) -> Vec<bool> {
        keys.iter()
            .map(|key| self.routes.contains_key(&key.0))
            .collect()
    }

    pub(crate) fn contains_readable_many(
        &self,
        keys: &[ObjectKey],
        readable: &BTreeSet<ClientRuntimeId>,
    ) -> Vec<bool> {
        keys.iter()
            .map(|key| match self.routes.get(&key.0) {
                None => false,
                Some(route) => route.replicas.iter().any(|r| readable.contains(&r.owner)),
            })
            .collect()
    }

    pub(crate) fn evict_unreadable_routes(
        &mut self,
        keys: &[ObjectKey],
        readable: &BTreeSet<ClientRuntimeId>,
    ) {
        for key in keys {
            let dominated = self
                .routes
                .get(&key.0)
                .is_some_and(|route| !route.replicas.iter().any(|r| readable.contains(&r.owner)));
            if dominated {
                self.apply_update(key, None);
            }
        }
    }

    pub(crate) fn version_floor(&self, key: &ObjectKey) -> Option<RouteVersion> {
        self.version_floors.get(&key.0).copied()
    }

    pub(crate) fn version_floors(&self, keys: &[ObjectKey]) -> Vec<Option<RouteVersion>> {
        keys.iter().map(|key| self.version_floor(key)).collect()
    }

    pub(crate) fn list_routes(&self) -> Vec<ObjectRoute> {
        self.routes.values().cloned().collect()
    }

    pub(crate) fn list_by_replica_owner(&self, owner: &ClientRuntimeId) -> Vec<ObjectRoute> {
        self.routes
            .values()
            .filter(|route| route.replicas.iter().any(|replica| replica.owner == *owner))
            .cloned()
            .collect()
    }

    pub(crate) fn list_in_scope(&self, scope: &NamespaceScope) -> Vec<ObjectRoute> {
        self.scope_index
            .get(scope)
            .into_iter()
            .flat_map(|keys| keys.iter())
            .filter_map(|key| self.routes.get(key).cloned())
            .collect()
    }

    pub(crate) fn list_reuse_candidates(&self, reuse: &ReuseIdentity) -> Vec<ObjectRoute> {
        self.reuse_index
            .get(reuse)
            .into_iter()
            .flat_map(|keys| keys.iter())
            .filter_map(|key| self.routes.get(key).cloned())
            .collect()
    }

    pub(crate) fn compare_and_swap(
        &mut self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> CasResult {
        let current = self.get(key);
        let matches = match (expected, current.as_ref()) {
            (None, None) => true,
            (Some(version), Some(route)) => route.version == version,
            _ => false,
        };
        let floor_blocked = self.version_floor_blocks_insert(key, expected, next);
        if !matches || floor_blocked {
            let version_floor = if floor_blocked && current.is_none() {
                self.version_floors.get(&key.0).copied()
            } else {
                None
            };
            let result = CasResult {
                applied: false,
                current,
                version_floor,
            };
            record_cas_outcome(&result, next, key);
            return result;
        }

        self.apply_update(key, next);
        let result = CasResult {
            applied: true,
            current: next.cloned(),
            version_floor: None,
        };
        record_cas_outcome(&result, next, key);
        result
    }

    pub(crate) fn compare_and_swap_many(&mut self, requests: &[RouteCasRequest]) -> Vec<CasResult> {
        requests
            .iter()
            .map(|request| {
                self.compare_and_swap(&request.key, request.expected, request.next.as_ref())
            })
            .collect()
    }

    pub(crate) fn replace(&mut self, key: &ObjectKey, next: Option<&ObjectRoute>) {
        if self.version_floor_blocks_insert(key, None, next) {
            return;
        }
        self.apply_update(key, next);
    }

    pub(crate) fn replace_many(&mut self, requests: &[RouteCasRequest]) {
        for request in requests {
            self.replace(&request.key, request.next.as_ref());
        }
    }

    fn version_floor_blocks_insert(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> bool {
        if expected.is_some() {
            return false;
        }
        let Some(route) = next else {
            return false;
        };
        self.version_floors
            .get(&key.0)
            .is_some_and(|floor| route.version <= *floor)
    }

    fn apply_update(&mut self, key: &ObjectKey, next: Option<&ObjectRoute>) {
        let old = self.routes.get(&key.0).cloned();
        if let Some(route) = old.as_ref() {
            self.remove_indexes(key, route);
        }
        match next {
            Some(route) => {
                self.routes.insert(key.0.clone(), route.clone());
                self.version_floors.remove(&key.0);
                self.insert_indexes(key, route);
            }
            None => {
                self.routes.remove(&key.0);
                if let Some(old) = old.as_ref() {
                    self.version_floors.insert(key.0.clone(), old.version);
                }
            }
        }
    }

    fn insert_indexes(&mut self, key: &ObjectKey, route: &ObjectRoute) {
        if let Some(scope) = route.namespace.clone() {
            self.scope_index
                .entry(scope)
                .or_default()
                .insert(key.0.clone());
        }
        if let Ok(reuse) = route_reuse_identity(route) {
            self.reuse_index
                .entry(reuse)
                .or_default()
                .insert(key.0.clone());
        }
    }

    fn remove_indexes(&mut self, key: &ObjectKey, route: &ObjectRoute) {
        if let Some(scope) = route.namespace.as_ref() {
            remove_index_key(&mut self.scope_index, scope, key.0.as_str());
        }
        if let Ok(reuse) = route_reuse_identity(route) {
            remove_index_key(&mut self.reuse_index, &reuse, key.0.as_str());
        }
    }
}

fn remove_index_key<K: Eq + Hash>(
    index: &mut HashMap<K, HashSet<String>>,
    identity: &K,
    key: &str,
) {
    let Some(keys) = index.get_mut(identity) else {
        return;
    };
    keys.remove(key);
    if keys.is_empty() {
        index.remove(identity);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_core::{
        ClientEpoch, CompatibilityDescriptor, ReplicaRoute, ReplicaTier, RouteState, SegmentName,
    };

    fn test_route(key: &ObjectKey, version: u64) -> ObjectRoute {
        ObjectRoute {
            key: key.clone(),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(version),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new("owner", ClientEpoch(1)),
                segment_name: SegmentName::new("seg".to_string()),
                segment_offset: 0,
                length: 100,
                checksum: Some(42),
                offset: Some(0),
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
        }
    }

    #[test]
    fn replace_ignores_stale_route_at_version_floor() {
        let key = ObjectKey::new("key-a".to_string());
        let old = test_route(&key, 3);
        let mut table = LocalRouteTable::default();

        table.replace(&key, Some(&old));
        table.replace(&key, None);
        table.replace(&key, Some(&old));

        assert!(
            table.get(&key).is_none(),
            "delayed mirror replace must not resurrect an evicted route"
        );
        assert_eq!(table.version_floor(&key), Some(RouteVersion(3)));
    }

    #[test]
    fn replace_accepts_newer_route_above_version_floor() {
        let key = ObjectKey::new("key-a".to_string());
        let old = test_route(&key, 3);
        let new = test_route(&key, 4);
        let mut table = LocalRouteTable::default();

        table.replace(&key, Some(&old));
        table.replace(&key, None);
        table.replace(&key, Some(&new));

        assert_eq!(
            table.get(&key).as_ref().map(|route| route.version),
            Some(RouteVersion(4))
        );
        assert_eq!(table.version_floor(&key), None);
    }
}
