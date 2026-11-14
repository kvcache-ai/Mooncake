use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::ops::Bound;

use mooncake_store_core::{
    route_reuse_identity, AllocationRouteIdentity, AllocationRouteReference,
    AllocationRouteReferenceState, CasResult, ClientRuntimeId, ColdBackingState, NamespaceScope,
    ObjectKey, ObjectRoute, ReplicaRoute, Result, ReuseIdentity, RouteCasRequest, RouteState,
    RouteVersion, StoreError,
};

use crate::metrics::record_cas_outcome;
use crate::metrics::{
    record_authoritative_route_update, record_delete_marker_event, record_delete_marker_state,
};
use crate::table::{DeleteMarkerSnapshot, DeleteMarkerStore};
use crate::RouteAuthorityRecord;
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
    delete_markers: DeleteMarkerStore,
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
    pub(crate) fn with_delete_marker_budget(budget_bytes: usize) -> Self {
        Self {
            delete_markers: DeleteMarkerStore::new(budget_bytes),
            ..Self::default()
        }
    }

    pub(crate) fn get(&self, key: &ObjectKey) -> Option<ObjectRoute> {
        self.routes.get(&key.0).cloned()
    }

    pub(crate) fn get_many(&self, keys: &[ObjectKey]) -> Vec<Option<ObjectRoute>> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    pub(crate) fn get_record(&self, key: &ObjectKey) -> RouteAuthorityRecord {
        RouteAuthorityRecord {
            route: self.get(key),
            delete_version: self.delete_marker(key),
        }
    }

    pub(crate) fn get_records(&self, keys: &[ObjectKey]) -> Vec<RouteAuthorityRecord> {
        keys.iter().map(|key| self.get_record(key)).collect()
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
                self.apply_update(key, None, None);
            }
        }
    }

    pub(crate) fn delete_marker(&self, key: &ObjectKey) -> Option<RouteVersion> {
        self.delete_markers.get(key)
    }

    pub(crate) fn delete_markers(&self, keys: &[ObjectKey]) -> Vec<Option<RouteVersion>> {
        self.delete_markers.get_many(keys)
    }

    pub(crate) fn delete_marker_snapshot(&self) -> DeleteMarkerSnapshot {
        self.delete_markers.snapshot()
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

    pub(crate) fn list_by_replica_owner_page(
        &self,
        owner: &ClientRuntimeId,
        cursor: Option<&str>,
        limit: usize,
    ) -> RouteOwnerPage {
        self.list_by_replica_owner_page_at_epoch(owner, cursor, limit, None)
            .expect("unconditional owner page must succeed")
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
        let marker = self.delete_marker(key);
        let matches = match (expected, current.as_ref(), marker) {
            (None, None, _) => true,
            (Some(version), Some(route), _) => route.version == version,
            (Some(version), None, Some(marker)) => marker == version,
            _ => false,
        };
        let marker_blocked = self.delete_marker_blocks_insert(key, next);
        if !matches || marker_blocked {
            if marker_blocked {
                record_delete_marker_event("blocked_stale_route", 1);
            }
            let delete_version = if current.is_none() { marker } else { None };
            let result = CasResult {
                applied: false,
                current,
                delete_version,
                version_floor: None,
            };
            record_cas_outcome(&result);
            return result;
        }

        let applied_delete_version = self.apply_update(key, next, expected);
        let result = CasResult {
            applied: true,
            current: next
                .filter(|route| route.state != mooncake_store_core::RouteState::Tombstone)
                .cloned(),
            delete_version: applied_delete_version,
            version_floor: None,
        };
        record_cas_outcome(&result);
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
            if self.delete_marker_blocks_insert(key, next) {
                record_delete_marker_event("blocked_stale_route", 1);
            }
            return;
        }
        self.apply_update(key, next, None);
    }

    pub(crate) fn replace_many(&mut self, requests: &[RouteCasRequest]) {
        for request in requests {
            if self.replace_is_stale(&request.key, request.expected, request.next.as_ref()) {
                if self.delete_marker_blocks_insert(&request.key, request.next.as_ref()) {
                    record_delete_marker_event("blocked_stale_route", 1);
                }
                continue;
            }
            self.apply_update(&request.key, request.next.as_ref(), request.expected);
        }
    }

    // Asynchronous mirrors can arrive after a newer publish or delete.
    fn replace_is_stale(
        &self,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> bool {
        match next {
            Some(route) => {
                if route.state == mooncake_store_core::RouteState::Tombstone {
                    return self
                        .routes
                        .get(&key.0)
                        .is_some_and(|current| current.version > route.version)
                        || self
                            .delete_marker(key)
                            .is_some_and(|marker| marker >= route.version);
                }
                self.routes
                    .get(&key.0)
                    .is_some_and(|current| route.version < current.version)
                    || self
                        .delete_marker(key)
                        .is_some_and(|marker| route.version <= marker)
            }
            None => expected.is_some_and(|version| {
                self.routes
                    .get(&key.0)
                    .is_some_and(|current| current.version > version)
            }),
        }
    }

    fn delete_marker_blocks_insert(&self, key: &ObjectKey, next: Option<&ObjectRoute>) -> bool {
        let Some(route) = next else {
            return false;
        };
        route.state != mooncake_store_core::RouteState::Tombstone
            && self
                .delete_marker(key)
                .is_some_and(|marker| route.version <= marker)
    }

    fn apply_update(
        &mut self,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
        delete_version: Option<RouteVersion>,
    ) -> Option<RouteVersion> {
        let old = self.routes.get(&key.0).cloned();
        let next_live =
            next.filter(|route| route.state != mooncake_store_core::RouteState::Tombstone);
        self.bump_owner_epochs(old.as_ref(), next_live);
        if let Some(route) = old.as_ref() {
            self.remove_indexes(key, route);
        }
        match next {
            Some(route) if route.state == mooncake_store_core::RouteState::Tombstone => {
                self.routes.remove(&key.0);
                self.record_delete_marker(key, route.version);
                record_authoritative_route_update(None, key);
                return Some(route.version);
            }
            Some(route) => {
                self.routes.insert(key.0.clone(), route.clone());
                if self.delete_markers.remove_if_older(key, route.version) {
                    record_delete_marker_event("superseded", 1);
                    self.record_delete_marker_state();
                }
                self.insert_indexes(key, route);
            }
            None => {
                self.routes.remove(&key.0);
                if let Some(version) = old
                    .as_ref()
                    .map(|route| route.version)
                    .into_iter()
                    .chain(delete_version)
                    .max()
                {
                    self.record_delete_marker(key, version);
                    record_authoritative_route_update(None, key);
                    return Some(version);
                }
            }
        }
        record_authoritative_route_update(next_live, key);
        None
    }

    fn record_delete_marker(&mut self, key: &ObjectKey, version: RouteVersion) {
        let evicted_count = self.delete_markers.record(key, version);
        if evicted_count != 0 {
            record_delete_marker_event("capacity_evicted", evicted_count as u64);
        }
        self.record_delete_marker_state();
    }

    fn record_delete_marker_state(&self) {
        let snapshot = self.delete_markers.snapshot();
        record_delete_marker_state(
            snapshot.entries as u64,
            snapshot.accounted_bytes as u64,
            snapshot.budget_bytes as u64,
        );
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
    use std::sync::{Arc, Mutex};

    use crate::metrics::record_cas_outcome;

    use crate::{set_route_metrics_sink, RouteMetricsSink};

    static METRICS_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[derive(Default)]
    struct RecordingRouteMetricsSink {
        cas_outcomes: Mutex<Vec<&'static str>>,
        routes: Mutex<HashMap<String, ObjectRoute>>,
        delete_marker_events: Mutex<Vec<(&'static str, u64)>>,
    }

    impl RouteMetricsSink for RecordingRouteMetricsSink {
        fn record_route_repair(&self, _operation: &'static str) {}

        fn record_route_cas(&self, outcome: &'static str) {
            self.cas_outcomes.lock().unwrap().push(outcome);
        }

        fn record_route(&self, route: &ObjectRoute) {
            self.routes
                .lock()
                .unwrap()
                .insert(route.key.0.clone(), route.clone());
        }

        fn remove_route(&self, key: &ObjectKey) {
            self.routes.lock().unwrap().remove(&key.0);
        }
        fn record_delete_marker_state(&self, _entries: u64, _bytes: u64, _budget_bytes: u64) {}

        fn record_delete_marker_event(&self, event: &'static str, count: u64) {
            self.delete_marker_events
                .lock()
                .unwrap()
                .push((event, count));
        }
    }

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
    fn logical_cas_metrics_do_not_change_authoritative_route_gauges() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        let sink = Arc::new(RecordingRouteMetricsSink::default());
        set_route_metrics_sink(sink.clone());
        let key = ObjectKey::new("observed-route".to_string());
        let route = test_route(&key, 1);
        let result = CasResult {
            applied: true,
            current: Some(route.clone()),
            delete_version: None,
            version_floor: None,
        };

        record_cas_outcome(&result);

        assert!(sink.cas_outcomes.lock().unwrap().contains(&"ok"));
        assert!(
            sink.routes.lock().unwrap().get(&key.0).is_none(),
            "a logical CAS caller must not add observed routes to authority gauges"
        );
    }

    #[test]
    fn local_route_table_mutations_own_route_gauge_updates() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        let sink = Arc::new(RecordingRouteMetricsSink::default());
        set_route_metrics_sink(sink.clone());
        let key = ObjectKey::new("authority-route".to_string());
        let route = test_route(&key, 1);
        let mut table = LocalRouteTable::default();

        table.replace(&key, Some(&route));
        assert_eq!(sink.routes.lock().unwrap().get(&key.0), Some(&route));

        table.replace(&key, None);
        assert!(sink.routes.lock().unwrap().get(&key.0).is_none());
    }

    #[test]
    fn replace_ignores_stale_route_at_delete_marker() {
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
        assert_eq!(table.delete_marker(&key), Some(RouteVersion(3)));
    }

    #[test]
    fn replace_many_records_routes_blocked_by_delete_markers() {
        let _guard = METRICS_TEST_LOCK.lock().unwrap();
        let sink = Arc::new(RecordingRouteMetricsSink::default());
        set_route_metrics_sink(sink.clone());
        let key = ObjectKey::new("batch-marker-metric".to_string());
        let route = test_route(&key, 3);
        let mut table = LocalRouteTable::default();

        table.replace(&key, Some(&route));
        table.replace(&key, None);
        sink.delete_marker_events.lock().unwrap().clear();
        table.replace_many(&[RouteCasRequest {
            key,
            expected: None,
            next: Some(route),
        }]);

        assert!(sink
            .delete_marker_events
            .lock()
            .unwrap()
            .contains(&("blocked_stale_route", 1)));
    }

    #[test]
    fn replace_accepts_newer_route_above_delete_marker() {
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
        assert_eq!(table.delete_marker(&key), None);
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
    fn versioned_delete_mirror_retains_actual_marker_over_stale_live_route() {
        let key = ObjectKey::new("versioned-delete".to_string());
        let old = test_route(&key, 1);
        let mut table = LocalRouteTable::default();
        table.replace(&key, Some(&old));
        table.replace_many(&[RouteCasRequest {
            key: key.clone(),
            expected: Some(RouteVersion(5)),
            next: None,
        }]);
        assert!(table.get(&key).is_none());
        assert_eq!(table.delete_marker(&key), Some(RouteVersion(5)));
        table.replace(&key, Some(&test_route(&key, 4)));
        assert!(table.get(&key).is_none());
        table.replace(&key, Some(&test_route(&key, 6)));
        assert_eq!(table.get(&key).unwrap().version, RouteVersion(6));
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
    fn replace_many_installs_delete_marker_when_route_is_missing() {
        let key = ObjectKey::new("missing-delete-marker".to_string());
        let mut table = LocalRouteTable::default();

        table.replace_many(&[RouteCasRequest {
            key: key.clone(),
            expected: Some(RouteVersion(9)),
            next: None,
        }]);

        assert_eq!(table.get(&key), None);
        assert_eq!(table.delete_marker(&key), Some(RouteVersion(9)));
        let stale = test_route(&key, 9);
        table.replace(&key, Some(&stale));
        assert_eq!(table.get(&key), None);

        let newer = test_route(&key, 10);
        table.replace(&key, Some(&newer));
        assert_eq!(table.get(&key), Some(newer));
        assert_eq!(table.delete_marker(&key), None);
    }

    #[test]
    fn tombstone_route_is_stored_as_delete_marker() {
        let key = ObjectKey::new("tombstone-marker".to_string());
        let mut table = LocalRouteTable::default();
        let mut tombstone = test_route(&key, 4);
        tombstone.state = RouteState::Tombstone;
        tombstone.replicas.clear();

        table.replace(&key, Some(&tombstone));

        assert_eq!(table.get(&key), None);
        assert_eq!(table.delete_marker(&key), Some(RouteVersion(4)));
    }

    #[test]
    fn unconditional_insert_above_delete_marker_is_allowed() {
        let key = ObjectKey::new("marker-initial-insert".to_string());
        let mut table = LocalRouteTable::default();
        table.replace_many(&[RouteCasRequest {
            key: key.clone(),
            expected: Some(RouteVersion(4)),
            next: None,
        }]);

        let stale = test_route(&key, 4);
        let stale_result = table.compare_and_swap(&key, None, Some(&stale));
        assert!(!stale_result.applied);
        assert_eq!(stale_result.delete_version, Some(RouteVersion(4)));

        let newer = test_route(&key, 5);
        let newer_result = table.compare_and_swap(&key, None, Some(&newer));
        assert!(newer_result.applied);
        assert_eq!(table.get(&key), Some(newer));
        assert_eq!(table.delete_marker(&key), None);
    }

    #[test]
    fn owner_route_pages_are_sorted_bounded_and_resume_after_cursor() {
        for (keys, expected_second) in [
            (vec!["route-c", "route-a", "route-b"], vec!["route-c"]),
            (
                vec!["route-c", "route-a", "route-b", "route-d"],
                vec!["route-c", "route-d"],
            ),
        ] {
            let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
            let mut table = LocalRouteTable::default();
            for key in keys {
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
                expected_second
            );
            assert_eq!(second.next_cursor, None);
        }
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
