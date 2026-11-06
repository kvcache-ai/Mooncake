use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::ops::Bound;

use mooncake_store_core::{
    route_reuse_identity, AllocationRouteIdentity, AllocationRouteReference,
    AllocationRouteReferenceState, CasResult, ClientRuntimeId, ColdBackingState, NamespaceScope,
    ObjectKey, ObjectRoute, ReplicaRoute, Result, ReuseIdentity, RouteCasRequest, RouteState,
    RouteVersion, StoreError,
};

use crate::metrics::record_authoritative_route_update;
use crate::{RouteOwnerPage, DEFAULT_ROUTE_OWNER_PAGE_SIZE};

fn route_has_materialized_cold_backing(route: &ObjectRoute) -> bool {
    route
        .cold_backing
        .as_ref()
        .is_some_and(|cb| cb.state == ColdBackingState::Materialized)
}

#[derive(Default)]
pub(crate) struct LocalRouteTable {
    routes: HashMap<String, ObjectRoute>,
    owner_index: HashMap<ClientRuntimeId, BTreeSet<String>>,
    allocation_start_index: HashMap<AllocationRouteStart, BTreeSet<String>>,
    owner_epochs: HashMap<ClientRuntimeId, u64>,
    scope_index: HashMap<NamespaceScope, HashSet<String>>,
    reuse_index: HashMap<ReuseIdentity, HashSet<String>>,
    version_floors: HashMap<String, RouteVersion>,
}

/// The reverse index intentionally omits length and generation. Any route at
/// the same physical start with a different identity must block reclamation.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct AllocationRouteStart {
    owner: ClientRuntimeId,
    segment_name: mooncake_store_core::SegmentName,
    offset_bytes: u64,
}

impl From<&AllocationRouteIdentity> for AllocationRouteStart {
    fn from(identity: &AllocationRouteIdentity) -> Self {
        Self {
            owner: identity.owner.clone(),
            segment_name: identity.segment_name.clone(),
            offset_bytes: identity.offset_bytes,
        }
    }
}

impl From<&ReplicaRoute> for AllocationRouteStart {
    fn from(replica: &ReplicaRoute) -> Self {
        Self {
            owner: replica.owner.clone(),
            segment_name: replica.segment_name.clone(),
            offset_bytes: replica.segment_offset,
        }
    }
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
                Some(route) => {
                    route.replicas.iter().any(|r| readable.contains(&r.owner))
                        || route_has_materialized_cold_backing(route)
                }
            })
            .collect()
    }

    pub(crate) fn evict_unreadable_routes(
        &mut self,
        keys: &[ObjectKey],
        readable: &BTreeSet<ClientRuntimeId>,
    ) {
        for key in keys {
            let dominated = self.routes.get(&key.0).is_some_and(|route| {
                !route.replicas.iter().any(|r| readable.contains(&r.owner))
                    && !route_has_materialized_cold_backing(route)
            });
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

    pub(crate) fn allocation_route_references(
        &self,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReferenceState>> {
        identities
            .iter()
            .map(|identity| {
                if identity.allocation_id == 0 {
                    return Ok(AllocationRouteReferenceState::Conflict);
                }
                let Some(keys) = self
                    .allocation_start_index
                    .get(&AllocationRouteStart::from(identity))
                else {
                    return Ok(AllocationRouteReferenceState::Absent);
                };
                let mut saw_exact = false;
                for key in keys {
                    let route = self.routes.get(key).ok_or_else(|| {
                        StoreError::InvalidState(format!(
                            "allocation reverse index references missing route {key}"
                        ))
                    })?;
                    let mut route_has_start = false;
                    for replica in route
                        .replicas
                        .iter()
                        .filter(|replica| identity.matches_replica_start(replica))
                    {
                        route_has_start = true;
                        if !identity.matches_replica(replica) {
                            return Ok(AllocationRouteReferenceState::Conflict);
                        }
                        saw_exact = true;
                    }
                    if !route_has_start {
                        return Err(StoreError::InvalidState(format!(
                            "allocation reverse index route {key} does not reference the indexed start"
                        )));
                    }
                }
                Ok(if saw_exact {
                    AllocationRouteReferenceState::Exact
                } else {
                    AllocationRouteReferenceState::Conflict
                })
            })
            .collect()
    }

    pub(crate) fn allocation_route_reference_keys(
        &self,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReference>> {
        identities
            .iter()
            .map(|identity| {
                if identity.allocation_id == 0 {
                    return Ok(AllocationRouteReference::conflict());
                }
                let Some(keys) = self
                    .allocation_start_index
                    .get(&AllocationRouteStart::from(identity))
                else {
                    return Ok(AllocationRouteReference::absent());
                };
                let mut exact_key = None;
                for key in keys {
                    let route = self.routes.get(key).ok_or_else(|| {
                        StoreError::InvalidState(format!(
                            "allocation reverse index references missing route {key}"
                        ))
                    })?;
                    if route.state != RouteState::Active {
                        continue;
                    }
                    let mut route_has_start = false;
                    for replica in route
                        .replicas
                        .iter()
                        .filter(|replica| identity.matches_replica_start(replica))
                    {
                        route_has_start = true;
                        if !identity.matches_replica(replica) {
                            return Ok(AllocationRouteReference::conflict());
                        }
                    }
                    if !route_has_start {
                        return Err(StoreError::InvalidState(format!(
                            "allocation reverse index route {key} does not reference the indexed start"
                        )));
                    }
                    if exact_key.replace(ObjectKey::new(key.clone())).is_some() {
                        return Ok(AllocationRouteReference::conflict());
                    }
                }
                Ok(match exact_key {
                    Some(key) => AllocationRouteReference::exact(key),
                    None => AllocationRouteReference::absent(),
                })
            })
            .collect()
    }

    pub(crate) fn list_by_replica_owner_page(&self, owner: &ClientRuntimeId, cursor: Option<&str>, limit: usize) -> RouteOwnerPage {
        self.list_by_replica_owner_page_at_epoch(owner, cursor, limit, None).expect("unconditional owner page must succeed")
    }

    pub(crate) fn list_by_replica_owner_page_at_epoch(
        &self,
        owner: &ClientRuntimeId,
        cursor: Option<&str>,
        limit: usize,
        expected_epoch: Option<u64>,
    ) -> Result<RouteOwnerPage> {
        let snapshot_epoch = self.owner_epochs.get(owner).copied().unwrap_or_default();
        if let Some(expected_epoch) = expected_epoch.filter(|epoch| *epoch != snapshot_epoch) {
            return Err(StoreError::StaleEpoch(format!(
                "owner-route snapshot changed for {owner}: expected epoch {expected_epoch}, current epoch {snapshot_epoch}"
            )));
        }
        let Some(keys) = self.owner_index.get(owner) else {
            return Ok(RouteOwnerPage {
                routes: Vec::new(),
                next_cursor: None,
                snapshot_epoch: Some(snapshot_epoch),
            });
        };
        let start = cursor.map_or(Bound::Unbounded, Bound::Excluded);
        let limit = if limit == 0 {
            DEFAULT_ROUTE_OWNER_PAGE_SIZE
        } else {
            limit.clamp(1, DEFAULT_ROUTE_OWNER_PAGE_SIZE)
        };
        let mut routes: Vec<ObjectRoute> = Vec::with_capacity(limit);
        for key in keys.range::<str, _>((start, Bound::Unbounded)) {
            let Some(route) = self.routes.get(key) else {
                continue;
            };
            if routes.len() == limit.max(1) {
                let next_cursor = routes.last().map(|route| route.key.0.clone());
                return Ok(RouteOwnerPage {
                    routes,
                    next_cursor,
                    snapshot_epoch: Some(snapshot_epoch),
                });
            }
            routes.push(route.clone());
        }
        Ok(RouteOwnerPage {
            routes,
            next_cursor: None,
            snapshot_epoch: Some(snapshot_epoch),
        })
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
            return result;
        }

        self.apply_update(key, next);
        let result = CasResult {
            applied: true,
            current: next.cloned(),
            version_floor: None,
        };
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
        if self.replace_is_stale(key, None, next) {
            return;
        }
        self.apply_update(key, next);
    }

    pub(crate) fn replace_many(&mut self, requests: &[RouteCasRequest]) {
        for request in requests {
            if self.replace_is_stale(&request.key, request.expected, request.next.as_ref()) {
                continue;
            }
            self.apply_update(&request.key, request.next.as_ref());
        }
    }

    // Secondary mirrors are asynchronous, so an older publish or delete can arrive
    // after a newer route is already visible on the authority.
    fn replace_is_stale(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> bool {
        match next {
            Some(route) => {
                self.routes
                    .get(&key.0)
                    .is_some_and(|current| route.version < current.version)
                    || self
                        .version_floors
                        .get(&key.0)
                        .is_some_and(|floor| route.version <= *floor)
            }
            None => expected.is_some_and(|version| {
                self.routes
                    .get(&key.0)
                    .is_some_and(|current| current.version > version)
            }),
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
        self.bump_owner_epochs(old.as_ref(), next);
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
        record_authoritative_route_update(next, key);
    }

    fn bump_owner_epochs(&mut self, old: Option<&ObjectRoute>, next: Option<&ObjectRoute>) {
        let owners = old
            .into_iter()
            .chain(next)
            .flat_map(|route| route.replicas.iter().map(|replica| replica.owner.clone()))
            .collect::<BTreeSet<_>>();
        for owner in owners {
            let epoch = self.owner_epochs.entry(owner).or_default();
            *epoch = epoch.wrapping_add(1);
        }
    }

    fn insert_indexes(&mut self, key: &ObjectKey, route: &ObjectRoute) {
        for owner in route
            .replicas
            .iter()
            .map(|replica| replica.owner.clone())
            .collect::<BTreeSet<_>>()
        {
            self.owner_index
                .entry(owner)
                .or_default()
                .insert(key.0.clone());
        }
        for start in route
            .replicas
            .iter()
            .map(AllocationRouteStart::from)
            .collect::<BTreeSet<_>>()
        {
            self.allocation_start_index
                .entry(start)
                .or_default()
                .insert(key.0.clone());
        }
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
        for owner in route
            .replicas
            .iter()
            .map(|replica| replica.owner.clone())
            .collect::<BTreeSet<_>>()
        {
            remove_ordered_index_key(&mut self.owner_index, &owner, key.0.as_str());
        }
        for start in route
            .replicas
            .iter()
            .map(AllocationRouteStart::from)
            .collect::<BTreeSet<_>>()
        {
            remove_ordered_index_key(&mut self.allocation_start_index, &start, key.0.as_str());
        }
        if let Some(scope) = route.namespace.as_ref() {
            remove_index_key(&mut self.scope_index, scope, key.0.as_str());
        }
        if let Ok(reuse) = route_reuse_identity(route) {
            remove_index_key(&mut self.reuse_index, &reuse, key.0.as_str());
        }
    }
}

fn remove_ordered_index_key<K: Eq + Hash>(
    index: &mut HashMap<K, BTreeSet<String>>,
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
                allocation_id: None,
                segment_offset: 0,
                length: 100,
                checksum: Some(42),
                offset: Some(0),
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,
            nof_backing: None,
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

    #[test]
    fn replace_ignores_delayed_older_route_when_newer_route_is_present() {
        let key = ObjectKey::new("key-a".to_string());
        let old = test_route(&key, 3);
        let new = test_route(&key, 4);
        let mut table = LocalRouteTable::default();

        table.replace(&key, Some(&old));
        table.replace(&key, Some(&new));
        table.replace(&key, Some(&old));

        assert_eq!(
            table.get(&key).as_ref().map(|route| route.version),
            Some(RouteVersion(4)),
            "a delayed mirror must not roll a live route back to an older version"
        );
    }

    #[test]
    fn replace_many_ignores_delayed_delete_when_newer_route_is_present() {
        let key = ObjectKey::new("key-a".to_string());
        let old = test_route(&key, 3);
        let new = test_route(&key, 4);
        let mut table = LocalRouteTable::default();

        table.replace(&key, Some(&old));
        table.replace(&key, Some(&new));
        table.replace_many(&[RouteCasRequest {
            key: key.clone(),
            expected: Some(old.version),
            next: None,
        }]);

        assert_eq!(
            table.get(&key).as_ref().map(|route| route.version),
            Some(RouteVersion(4)),
            "a delayed mirror delete must not remove a newer route"
        );
    }

    #[test]
    fn owner_route_pages_are_sorted_bounded_and_resume_after_cursor() {
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut table = LocalRouteTable::default();
        for key in ["route-c", "route-a", "route-b"] {
            let key = ObjectKey::new(key.to_string());
            table.replace(&key, Some(&test_route(&key, 1)));
        }

        let first = table
            .list_by_replica_owner_page_at_epoch(&owner, None, 2, None)
            .expect("first page should establish a snapshot epoch");
        assert_eq!(
            first
                .routes
                .iter()
                .map(|route| route.key.0.as_str())
                .collect::<Vec<_>>(),
            vec!["route-a", "route-b"]
        );
        assert_eq!(first.next_cursor.as_deref(), Some("route-b"));

        let second = table
            .list_by_replica_owner_page_at_epoch(&owner, first.next_cursor.as_deref(), 2, None)
            .expect("second page should establish a snapshot epoch");
        assert_eq!(
            second
                .routes
                .iter()
                .map(|route| route.key.0.as_str())
                .collect::<Vec<_>>(),
            vec!["route-c"]
        );
        assert_eq!(second.next_cursor, None);
    }

    #[test]
    fn owner_route_page_snapshot_rejects_owner_mutation_between_pages() {
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut table = LocalRouteTable::default();
        for key in ["route-a", "route-b"] {
            let key = ObjectKey::new(key.to_string());
            table.replace(&key, Some(&test_route(&key, 1)));
        }

        let first = table
            .list_by_replica_owner_page_at_epoch(&owner, None, 1, None)
            .expect("first page should establish a snapshot epoch");
        let epoch = first
            .snapshot_epoch
            .expect("snapshot page must return an epoch");
        assert_eq!(first.next_cursor.as_deref(), Some("route-a"));

        let replacement_key = ObjectKey::new("route-b".to_string());
        table.replace(&replacement_key, Some(&test_route(&replacement_key, 2)));

        assert!(matches!(
            table.list_by_replica_owner_page_at_epoch(
                &owner,
                first.next_cursor.as_deref(),
                1,
                Some(epoch),
            ),
            Err(StoreError::StaleEpoch(_))
        ));
    }

    #[test]
    fn owner_route_index_tracks_replica_owner_replacement() {
        let old_owner = ClientRuntimeId::new("owner-old", ClientEpoch(1));
        let new_owner = ClientRuntimeId::new("owner-new", ClientEpoch(2));
        let key = ObjectKey::new("route-owner-move".to_string());
        let mut table = LocalRouteTable::default();
        let mut old_route = test_route(&key, 1);
        old_route.replicas[0].owner = old_owner.clone();
        table.replace(&key, Some(&old_route));

        let mut new_route = old_route.clone();
        new_route.version = RouteVersion(2);
        new_route.replicas[0].owner = new_owner.clone();
        table.replace(&key, Some(&new_route));

        assert!(table
            .list_by_replica_owner_page(&old_owner, None, 0)
            .routes
            .is_empty());
        assert_eq!(
            table
                .list_by_replica_owner_page(&new_owner, None, 0)
                .routes
                .into_iter()
                .map(|route| route.key.0)
                .collect::<Vec<_>>(),
            vec!["route-owner-move"]
        );
    }

    #[test]
    fn owner_route_page_clamps_requested_limit() {
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut table = LocalRouteTable::default();
        for index in 0..=DEFAULT_ROUTE_OWNER_PAGE_SIZE {
            let key = ObjectKey::new(format!("route-{index:04}"));
            table.replace(&key, Some(&test_route(&key, 1)));
        }

        for limit in [0, usize::MAX] {
            let page = table.list_by_replica_owner_page(&owner, None, limit);
            assert_eq!(page.routes.len(), DEFAULT_ROUTE_OWNER_PAGE_SIZE);
            assert_eq!(
                page.next_cursor.as_deref(),
                page.routes.last().map(|route| route.key.0.as_str())
            );
        }
    }

    #[test]
    fn owner_route_index_removes_deleted_keys_and_deduplicates_replica_owners() {
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let key = ObjectKey::new("route-a");
        let mut route = test_route(&key, 1);
        route.replicas.push(route.replicas[0].clone());
        let mut table = LocalRouteTable::default();

        table.replace(&key, Some(&route));
        assert_eq!(
            table
                .list_by_replica_owner_page(&owner, None, 0)
                .routes
                .len(),
            1
        );

        table.replace(&key, None);
        assert!(table
            .list_by_replica_owner_page(&owner, None, 0)
            .routes
            .is_empty());
        assert!(!table.owner_index.contains_key(&owner));
    }

    #[test]
    fn allocation_reverse_index_fails_closed_on_same_start_generation_or_length_change() {
        let key = ObjectKey::new("route-allocation-reference".to_string());
        let mut route = test_route(&key, 1);
        route.replicas[0].allocation_id = Some(7);
        let identity = AllocationRouteIdentity {
            owner: route.replicas[0].owner.clone(),
            segment_name: route.replicas[0].segment_name.clone(),
            offset_bytes: route.replicas[0].segment_offset,
            length_bytes: route.replicas[0].length,
            allocation_id: 7,
        };
        let mut table = LocalRouteTable::default();
        table.replace(&key, Some(&route));

        assert_eq!(
            table
                .allocation_route_references(std::slice::from_ref(&identity))
                .expect("exact generation should be indexed"),
            vec![AllocationRouteReferenceState::Exact]
        );

        let mut replacement = route.clone();
        replacement.version = RouteVersion(2);
        replacement.replicas[0].allocation_id = Some(8);
        table.replace(&key, Some(&replacement));
        assert_eq!(
            table
                .allocation_route_references(std::slice::from_ref(&identity))
                .expect("new generation at the same start must block reclaim"),
            vec![AllocationRouteReferenceState::Conflict]
        );

        replacement.replicas[0].allocation_id = Some(7);
        replacement.replicas[0].length = 99;
        replacement.version = RouteVersion(3);
        table.replace(&key, Some(&replacement));
        assert_eq!(
            table
                .allocation_route_references(std::slice::from_ref(&identity))
                .expect("length change at the same start must block reclaim"),
            vec![AllocationRouteReferenceState::Conflict]
        );

        table.replace(&key, None);
        assert_eq!(
            table
                .allocation_route_references(&[identity])
                .expect("removed route should be absent"),
            vec![AllocationRouteReferenceState::Absent]
        );
    }

    #[test]
    fn allocation_reverse_index_returns_the_unique_exact_route_key() {
        let key = ObjectKey::new("route-allocation-reference-key".to_string());
        let mut route = test_route(&key, 1);
        route.replicas[0].allocation_id = Some(7);
        let identity = AllocationRouteIdentity {
            owner: route.replicas[0].owner.clone(),
            segment_name: route.replicas[0].segment_name.clone(),
            offset_bytes: route.replicas[0].segment_offset,
            length_bytes: route.replicas[0].length,
            allocation_id: 7,
        };
        let mut table = LocalRouteTable::default();
        table.replace(&key, Some(&route));

        let references = table
            .allocation_route_reference_keys(std::slice::from_ref(&identity))
            .expect("exact identity should have a unique route key");
        assert_eq!(references[0].state, AllocationRouteReferenceState::Exact);
        assert_eq!(references[0].route_key, Some(key));
    }
}
