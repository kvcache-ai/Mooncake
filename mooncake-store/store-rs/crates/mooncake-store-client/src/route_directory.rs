use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId, ClientStableId, MetadataBackend,
    ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, Result, RouteCasRequest, RouteDirectory,
    RouteState, RouteVersion, StoreError,
};
use parking_lot::Mutex;
use tracing::warn;

use crate::{control_plane::ControlPlaneClient, observability::OperationTracker};

const ROUTE_SCOPE_LABEL: &str = "route_scope";
const AUTHORITY_CANDIDATE_CACHE_TTL: Duration = Duration::from_millis(100);
const ROUTE_REPAIR_MISSING_AUTHORITY: &str = "route_repair_missing_authority";
const ROUTE_REPAIR_STALE_AUTHORITY: &str = "route_repair_stale_authority";
const ROUTE_REPAIR_DIVERGENT_AUTHORITY: &str = "route_repair_divergent_authority";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RouteControlMode {
    MetadataOnly,
    #[default]
    EmbeddedWrh,
}

pub(crate) fn build_route_directory(
    mode: RouteControlMode,
    metadata: Arc<dyn MetadataBackend>,
    lease: &ClientLease,
    control_plane: Arc<ControlPlaneClient>,
) -> Arc<dyn RouteDirectory> {
    match mode {
        RouteControlMode::MetadataOnly => Arc::new(MetadataRouteDirectory { metadata }),
        RouteControlMode::EmbeddedWrh => Arc::new(EmbeddedWrhRouteDirectory::new(
            metadata,
            lease,
            control_plane,
        )),
    }
}

struct MetadataRouteDirectory {
    metadata: Arc<dyn MetadataBackend>,
}

impl RouteDirectory for MetadataRouteDirectory {
    fn get_object_route(
        &self,
        _observer: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        self.metadata.get_object_route(key)
    }

    fn compare_and_swap_object_route(
        &self,
        _observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        self.metadata
            .compare_and_swap_object_route(key, expected, next)
    }
}

struct EmbeddedWrhRouteDirectory {
    metadata: Arc<dyn MetadataBackend>,
    namespace: String,
    local_stable_id: ClientStableId,
    control_plane: Arc<ControlPlaneClient>,
    authority_cache: Mutex<RouteAuthorityCache>,
}

#[derive(Clone)]
struct RouteAuthoritySelection {
    primary: Option<ClientLease>,
    secondary: Option<ClientLease>,
}

#[derive(Clone, Default)]
struct RouteReadRepairState {
    primary: RouteAuthorityObservation,
    secondary: RouteAuthorityObservation,
}

#[derive(Clone, Default)]
enum RouteAuthorityObservation {
    #[default]
    Unobserved,
    Missing,
    Present(ObjectRoute),
}

#[derive(Clone, Copy)]
enum RouteRepairTarget {
    Primary,
    Secondary,
}

#[derive(Default)]
struct RouteAuthorityCache {
    refreshed_at: Option<Instant>,
    leases: Vec<ClientLease>,
}

impl RouteAuthorityCache {
    fn snapshot(&self) -> Option<Vec<ClientLease>> {
        let refreshed_at = self.refreshed_at?;
        if refreshed_at.elapsed() > AUTHORITY_CANDIDATE_CACHE_TTL {
            return None;
        }
        Some(self.leases.clone())
    }
}

impl EmbeddedWrhRouteDirectory {
    fn new(
        metadata: Arc<dyn MetadataBackend>,
        lease: &ClientLease,
        control_plane: Arc<ControlPlaneClient>,
    ) -> Self {
        let namespace = metadata.route_namespace();
        route_mesh(&namespace)
            .lock()
            .register_local(&lease.runtime.stable_id);
        Self {
            metadata,
            namespace,
            local_stable_id: lease.runtime.stable_id.clone(),
            control_plane,
            authority_cache: Mutex::new(RouteAuthorityCache::default()),
        }
    }

    fn active_route_leases(&self, force_refresh: bool) -> Result<Vec<ClientLease>> {
        if !force_refresh {
            if let Some(snapshot) = self.authority_cache.lock().snapshot() {
                return Ok(snapshot);
            }
        }
        let leases = self
            .metadata
            .list_live_clients()?
            .into_iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active && route_capable(lease))
            .collect::<Vec<_>>();
        let mut cache = self.authority_cache.lock();
        cache.refreshed_at = Some(Instant::now());
        cache.leases = leases.clone();
        Ok(leases)
    }

    fn authority_candidates_once(
        &self,
        observer: &ClientLease,
        force_refresh: bool,
    ) -> Result<Vec<ClientLease>> {
        let route_scope = observer.endpoints.labels.get(ROUTE_SCOPE_LABEL).cloned();
        let mut candidates = BTreeMap::<String, ClientLease>::new();
        for lease in self.active_route_leases(force_refresh)? {
            if !compatibility_matches(observer, &lease) {
                continue;
            }
            if route_scope.as_ref().is_some_and(|scope| {
                lease
                    .endpoints
                    .labels
                    .get(ROUTE_SCOPE_LABEL)
                    .is_none_or(|candidate| candidate != scope)
            }) {
                continue;
            }
            let stable_id = lease.runtime.stable_id.0.clone();
            match candidates.get(&stable_id) {
                Some(current) if current.runtime.epoch >= lease.runtime.epoch => {}
                _ => {
                    candidates.insert(stable_id, lease);
                }
            }
        }
        Ok(candidates.into_values().collect())
    }

    fn authority_candidates(&self, observer: &ClientLease) -> Result<Vec<ClientLease>> {
        let candidates = self.authority_candidates_once(observer, false)?;
        if !candidates.is_empty() {
            return Ok(candidates);
        }
        self.authority_candidates_once(observer, true)
    }

    fn select_authorities_from_candidates(
        &self,
        candidates: &[ClientLease],
        key: &ObjectKey,
    ) -> RouteAuthoritySelection {
        let ranked = self.ranked_authorities_from_candidates(candidates, key);
        RouteAuthoritySelection {
            primary: ranked.first().cloned(),
            secondary: ranked.get(1).cloned(),
        }
    }

    fn ranked_authorities_from_candidates(
        &self,
        candidates: &[ClientLease],
        key: &ObjectKey,
    ) -> Vec<ClientLease> {
        let mut ranked = candidates
            .iter()
            .map(|lease| {
                (
                    weighted_rendezvous_score(
                        &self.namespace,
                        &key.0,
                        &lease.runtime.stable_id.0,
                        route_weight(lease),
                    ),
                    lease.clone(),
                )
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.runtime.stable_id.cmp(&right.1.runtime.stable_id))
        });
        ranked.into_iter().map(|(_, lease)| lease).collect()
    }

    fn read_local_batch(
        &self,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRoute>>> {
        authority_get_many(&self.namespace, authority, keys)
    }

    fn read_authority_batch(
        &self,
        authority: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<Option<ObjectRoute>>>> {
        if authority.runtime.stable_id == self.local_stable_id {
            return self
                .read_local_batch(&authority.runtime.stable_id, keys)
                .map(|routes| routes.into_iter().map(Ok).collect());
        }
        self.control_plane.batch_get_routes(
            authority,
            &self.namespace,
            &authority.runtime.stable_id,
            keys,
        )
    }

    fn cas_local_batch(
        &self,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<CasResult>> {
        authority_compare_and_swap_many(&self.namespace, authority, requests)
    }

    fn cas_authority_batch(
        &self,
        authority: &ClientLease,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<CasResult>>> {
        if authority.runtime.stable_id == self.local_stable_id {
            return self
                .cas_local_batch(&authority.runtime.stable_id, requests)
                .map(|results| results.into_iter().map(Ok).collect());
        }
        self.control_plane.batch_compare_and_swap_routes(
            authority,
            &self.namespace,
            &authority.runtime.stable_id,
            requests,
        )
    }

    fn replace_local_batch(
        &self,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<()> {
        authority_replace_many(&self.namespace, authority, requests)
    }

    fn mirror_secondary_batch(&self, secondary: &ClientLease, requests: &[RouteCasRequest]) {
        let result = if secondary.runtime.stable_id == self.local_stable_id {
            self.replace_local_batch(&secondary.runtime.stable_id, requests)
                .map(|_| requests.iter().map(|_| Ok(())).collect())
        } else {
            self.control_plane.batch_replace_routes(
                secondary,
                &self.namespace,
                &secondary.runtime.stable_id,
                requests,
            )
        };
        match result {
            Ok(results) => {
                for (request, result) in requests.iter().zip(results.into_iter()) {
                    if let Err(error) = result {
                        warn!(
                            authority = %secondary.runtime,
                            key = %request.key.0,
                            error = %error,
                            "secondary route mirror failed"
                        );
                    }
                }
            }
            Err(error) => {
                warn!(
                    authority = %secondary.runtime,
                    error = %error,
                    items = requests.len(),
                    "secondary route mirror batch failed"
                );
            }
        }
    }

    fn read_ranked_authorities(
        &self,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
        rank: usize,
        resolved: &mut [Option<ObjectRoute>],
        repairs: Option<(&mut [RouteReadRepairState], RouteRepairTarget)>,
        failure_message: &'static str,
    ) -> Result<bool> {
        let mut groups = BTreeMap::<String, (ClientLease, Vec<usize>)>::new();
        for (index, authorities) in ranked_authorities.iter().enumerate() {
            let Some(authority) = authorities.get(rank).cloned() else {
                continue;
            };
            groups
                .entry(authority.runtime.stable_id.0.clone())
                .or_insert_with(|| (authority, Vec::new()))
                .1
                .push(index);
        }
        if groups.is_empty() {
            return Ok(false);
        }

        let mut repairs = repairs;
        for (_, (authority, indices)) in groups {
            let batch_keys = indices
                .iter()
                .map(|index| keys[*index].clone())
                .collect::<Vec<_>>();
            match self.read_authority_batch(&authority, &batch_keys) {
                Ok(results) => {
                    for ((index, key), result) in indices
                        .into_iter()
                        .zip(batch_keys.into_iter())
                        .zip(results.into_iter())
                    {
                        match result {
                            Ok(Some(route)) => {
                                if let Some((states, target)) = repairs.as_mut() {
                                    Self::set_repair_observation(
                                        &mut states[index],
                                        *target,
                                        RouteAuthorityObservation::Present(route.clone()),
                                    );
                                }
                                Self::merge_fresher_route(
                                    &mut resolved[index],
                                    route,
                                    &authority.runtime.to_string(),
                                    &key,
                                );
                            }
                            Ok(None) => {
                                if let Some((states, target)) = repairs.as_mut() {
                                    Self::set_repair_observation(
                                        &mut states[index],
                                        *target,
                                        RouteAuthorityObservation::Missing,
                                    );
                                }
                            }
                            Err(error) => warn!(
                                runtime = %authority.runtime,
                                key = %key.0,
                                error = %error,
                                "{failure_message}"
                            ),
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        runtime = %authority.runtime,
                        error = %error,
                        items = batch_keys.len(),
                        "{failure_message}"
                    );
                }
            }
        }
        Ok(true)
    }

    fn merge_fresher_route(
        current: &mut Option<ObjectRoute>,
        candidate: ObjectRoute,
        authority: &str,
        key: &ObjectKey,
    ) {
        match current {
            None => *current = Some(candidate),
            Some(existing) if candidate.version > existing.version => *current = Some(candidate),
            Some(existing) if candidate.version == existing.version && *existing != candidate => {
                let choose_candidate =
                    canonical_route_key(&candidate) < canonical_route_key(existing);
                if choose_candidate {
                    *existing = candidate.clone();
                }
                warn!(
                    key = %key.0,
                    authority = %authority,
                    version = candidate.version.0,
                    canonical = if choose_candidate { "candidate" } else { "existing" },
                    "route authorities disagree on the same route version; choosing canonical route"
                );
            }
            Some(_) => {}
        }
    }

    fn merge_metadata_routes(
        &self,
        keys: &[ObjectKey],
        resolved: &mut [Option<ObjectRoute>],
    ) -> Result<()> {
        for (index, key) in keys.iter().enumerate() {
            if let Some(route) = self.metadata.get_object_route(key)? {
                Self::merge_fresher_route(&mut resolved[index], route, "metadata", key);
            }
        }
        Ok(())
    }

    fn set_repair_observation(
        state: &mut RouteReadRepairState,
        target: RouteRepairTarget,
        observation: RouteAuthorityObservation,
    ) {
        match target {
            RouteRepairTarget::Primary => state.primary = observation,
            RouteRepairTarget::Secondary => state.secondary = observation,
        }
    }

    fn repair_request(
        key: &ObjectKey,
        best: &ObjectRoute,
        observed: &RouteAuthorityObservation,
    ) -> Option<RouteCasRequest> {
        match observed {
            RouteAuthorityObservation::Missing => {
                record_route_repair_metric(ROUTE_REPAIR_MISSING_AUTHORITY);
                Some(RouteCasRequest {
                    key: key.clone(),
                    expected: None,
                    next: Some(best.clone()),
                })
            }
            RouteAuthorityObservation::Present(current) if current.version < best.version => {
                record_route_repair_metric(ROUTE_REPAIR_STALE_AUTHORITY);
                Some(RouteCasRequest {
                    key: key.clone(),
                    expected: Some(current.version),
                    next: Some(best.clone()),
                })
            }
            RouteAuthorityObservation::Present(current)
                if current.version == best.version && current != best =>
            {
                record_route_repair_metric(ROUTE_REPAIR_DIVERGENT_AUTHORITY);
                Some(RouteCasRequest {
                    key: key.clone(),
                    expected: Some(current.version),
                    next: Some(best.clone()),
                })
            }
            _ => None,
        }
    }

    fn backfill_missing_authorities(
        &self,
        keys: &[ObjectKey],
        selections: &[RouteAuthoritySelection],
        resolved: &[Option<ObjectRoute>],
        repairs: &[RouteReadRepairState],
    ) {
        let mut backfills = BTreeMap::<String, (ClientLease, Vec<RouteCasRequest>)>::new();
        for (index, route) in resolved.iter().enumerate() {
            let Some(route) = route.as_ref() else {
                continue;
            };
            if let Some(primary) = selections[index].primary.clone() {
                if let Some(request) =
                    Self::repair_request(&keys[index], route, &repairs[index].primary)
                {
                    backfills
                        .entry(primary.runtime.stable_id.0.clone())
                        .or_insert_with(|| (primary, Vec::new()))
                        .1
                        .push(request);
                }
            }
            if let Some(secondary) = selections[index].secondary.clone() {
                if let Some(request) =
                    Self::repair_request(&keys[index], route, &repairs[index].secondary)
                {
                    backfills
                        .entry(secondary.runtime.stable_id.0.clone())
                        .or_insert_with(|| (secondary, Vec::new()))
                        .1
                        .push(request);
                }
            }
        }

        for (_, (authority, requests)) in backfills {
            match self.cas_authority_batch(&authority, &requests) {
                Ok(results) => {
                    for (request, result) in requests.iter().zip(results.into_iter()) {
                        if let Err(error) = result {
                            warn!(
                                authority = %authority.runtime,
                                key = %request.key.0,
                                error = %error,
                                "route backfill failed"
                            );
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        authority = %authority.runtime,
                        error = %error,
                        items = requests.len(),
                        "route backfill batch failed"
                    );
                }
            }
        }
    }
}

impl Drop for EmbeddedWrhRouteDirectory {
    fn drop(&mut self) {
        route_mesh(&self.namespace)
            .lock()
            .unregister_local(&self.local_stable_id);
    }
}

impl RouteDirectory for EmbeddedWrhRouteDirectory {
    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        let mut routes = self.get_object_routes(observer, std::slice::from_ref(key))?;
        Ok(routes.pop().unwrap_or(None))
    }

    fn get_object_routes(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<Vec<Option<ObjectRoute>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let candidates = self.authority_candidates(observer)?;
        let ranked_authorities = keys
            .iter()
            .map(|key| self.ranked_authorities_from_candidates(&candidates, key))
            .collect::<Vec<_>>();
        let selections = ranked_authorities
            .iter()
            .map(|authorities| RouteAuthoritySelection {
                primary: authorities.first().cloned(),
                secondary: authorities.get(1).cloned(),
            })
            .collect::<Vec<_>>();
        let mut resolved = vec![None; keys.len()];
        let mut repairs = vec![RouteReadRepairState::default(); keys.len()];

        self.read_ranked_authorities(
            keys,
            &ranked_authorities,
            0,
            &mut resolved,
            Some((repairs.as_mut_slice(), RouteRepairTarget::Primary)),
            "authority route batch read failed; trying secondary or metadata",
        )?;
        self.read_ranked_authorities(
            keys,
            &ranked_authorities,
            1,
            &mut resolved,
            Some((repairs.as_mut_slice(), RouteRepairTarget::Secondary)),
            "secondary authority route batch read failed; trying other authorities or metadata",
        )?;

        let mut rank = 2usize;
        while self.read_ranked_authorities(
            keys,
            &ranked_authorities,
            rank,
            &mut resolved,
            None,
            "fallback authority route batch read failed; trying other authorities or metadata",
        )? {
            rank = rank.saturating_add(1);
        }

        self.merge_metadata_routes(keys, &mut resolved)?;

        self.backfill_missing_authorities(keys, &selections, &resolved, &repairs);
        Ok(resolved)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let request = RouteCasRequest {
            key: key.clone(),
            expected,
            next: next.cloned(),
        };
        let mut results =
            self.compare_and_swap_object_routes(observer, std::slice::from_ref(&request))?;
        results.pop().ok_or_else(|| {
            StoreError::InvalidState("missing route cas result from batch path".to_string())
        })?
    }

    fn compare_and_swap_object_routes(
        &self,
        observer: &ClientLease,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<CasResult>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let candidates = self.authority_candidates(observer)?;
        let selections = requests
            .iter()
            .map(|request| self.select_authorities_from_candidates(&candidates, &request.key))
            .collect::<Vec<_>>();
        let mut resolved = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<_>>();
        let mut primary_groups = BTreeMap::<String, (ClientLease, Vec<usize>)>::new();
        for (index, selection) in selections.iter().enumerate() {
            let Some(primary) = selection.primary.clone() else {
                continue;
            };
            primary_groups
                .entry(primary.runtime.stable_id.0.clone())
                .or_insert_with(|| (primary, Vec::new()))
                .1
                .push(index);
        }

        for (_, (authority, indices)) in primary_groups {
            let batch_requests = indices
                .iter()
                .map(|index| requests[*index].clone())
                .collect::<Vec<_>>();
            if authority.runtime.stable_id == self.local_stable_id {
                match self.cas_local_batch(&authority.runtime.stable_id, &batch_requests) {
                    Ok(results) => {
                        for (index, result) in indices.into_iter().zip(results.into_iter()) {
                            resolved[index] = Some(Ok(result));
                        }
                    }
                    Err(error) => {
                        warn!(
                            runtime = %authority.runtime,
                            error = %error,
                            items = batch_requests.len(),
                            "authority route batch cas failed; falling back to metadata"
                        );
                    }
                }
                continue;
            }

            match self.control_plane.batch_compare_and_swap_routes(
                &authority,
                &self.namespace,
                &authority.runtime.stable_id,
                &batch_requests,
            ) {
                Ok(results) => {
                    for ((index, request), result) in indices
                        .into_iter()
                        .zip(batch_requests.into_iter())
                        .zip(results.into_iter())
                    {
                        match result {
                            Ok(result) => resolved[index] = Some(Ok(result)),
                            Err(error) => warn!(
                                runtime = %authority.runtime,
                                key = %request.key.0,
                                error = %error,
                                "authority route cas failed; falling back to metadata"
                            ),
                        }
                    }
                }
                Err(error) => {
                    warn!(
                        runtime = %authority.runtime,
                        error = %error,
                        items = batch_requests.len(),
                        "authority route batch cas failed; falling back to metadata"
                    );
                }
            }
        }

        let mut mirrors = BTreeMap::<String, (ClientLease, Vec<RouteCasRequest>)>::new();
        let mut results = Vec::with_capacity(requests.len());
        for (index, request) in requests.iter().enumerate() {
            let result = match resolved[index].take() {
                Some(result) => result,
                None => self.metadata.compare_and_swap_object_route(
                    &request.key,
                    request.expected,
                    request.next.as_ref(),
                ),
            };
            if result.as_ref().ok().is_some_and(|cas| cas.applied) {
                if let (Some(primary), Some(secondary)) = (
                    selections[index].primary.as_ref(),
                    selections[index].secondary.as_ref(),
                ) {
                    if secondary.runtime.stable_id != primary.runtime.stable_id {
                        mirrors
                            .entry(secondary.runtime.stable_id.0.clone())
                            .or_insert_with(|| (secondary.clone(), Vec::new()))
                            .1
                            .push(RouteCasRequest {
                                key: request.key.clone(),
                                expected: None,
                                next: request.next.clone(),
                            });
                    }
                }
            }
            results.push(result);
        }

        for (_, (secondary, requests)) in mirrors {
            self.mirror_secondary_batch(&secondary, &requests);
        }
        Ok(results)
    }
}

pub(crate) fn authority_get(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Result<Option<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    Ok(guard
        .routes_by_authority
        .get(&authority.0)
        .and_then(|routes| routes.get(&key.0))
        .cloned())
}

pub(crate) fn authority_get_many(
    namespace: &str,
    authority: &ClientStableId,
    keys: &[ObjectKey],
) -> Result<Vec<Option<ObjectRoute>>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let routes = guard.routes_by_authority.get(&authority.0);
    Ok(keys
        .iter()
        .map(|key| routes.and_then(|routes| routes.get(&key.0)).cloned())
        .collect())
}

pub(crate) fn authority_list_routes_by_replica_owner(
    namespace: &str,
    authority: &ClientStableId,
    owner: &ClientRuntimeId,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    Ok(guard
        .routes_by_authority
        .get(&authority.0)
        .into_iter()
        .flat_map(|routes| routes.values())
        .filter(|route| route.replicas.iter().any(|replica| replica.owner == *owner))
        .cloned()
        .collect())
}

pub(crate) fn authority_compare_and_swap(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
    expected: Option<RouteVersion>,
    next: Option<&ObjectRoute>,
) -> Result<CasResult> {
    let mesh = route_mesh(namespace);
    let mut guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let current = guard
        .routes_by_authority
        .entry(authority.0.clone())
        .or_default()
        .get(&key.0)
        .cloned();
    let matches = match (expected, current.as_ref()) {
        (None, None) => true,
        (Some(version), Some(route)) => route.version == version,
        _ => false,
    };
    if !matches {
        return Ok(CasResult {
            applied: false,
            current,
        });
    }
    apply_route_update(&mut guard, authority, key, next);
    Ok(CasResult {
        applied: true,
        current: next.cloned(),
    })
}

pub(crate) fn authority_compare_and_swap_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<Vec<CasResult>> {
    let mesh = route_mesh(namespace);
    let mut guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let mut results = Vec::with_capacity(requests.len());
    for request in requests {
        let current = guard
            .routes_by_authority
            .entry(authority.0.clone())
            .or_default()
            .get(&request.key.0)
            .cloned();
        let matches = match (request.expected, current.as_ref()) {
            (None, None) => true,
            (Some(version), Some(route)) => route.version == version,
            _ => false,
        };
        if !matches {
            results.push(CasResult {
                applied: false,
                current,
            });
            continue;
        }
        apply_route_update(&mut guard, authority, &request.key, request.next.as_ref());
        results.push(CasResult {
            applied: true,
            current: request.next.clone(),
        });
    }
    Ok(results)
}

pub(crate) fn authority_replace(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
    next: Option<&ObjectRoute>,
) -> Result<()> {
    let mesh = route_mesh(namespace);
    let mut guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    apply_route_update(&mut guard, authority, key, next);
    Ok(())
}

pub(crate) fn authority_replace_many(
    namespace: &str,
    authority: &ClientStableId,
    requests: &[RouteCasRequest],
) -> Result<()> {
    let mesh = route_mesh(namespace);
    let mut guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    for request in requests {
        apply_route_update(&mut guard, authority, &request.key, request.next.as_ref());
    }
    Ok(())
}

fn apply_route_update(
    mesh: &mut ClusterRouteMesh,
    authority: &ClientStableId,
    key: &ObjectKey,
    next: Option<&ObjectRoute>,
) {
    let routes = mesh
        .routes_by_authority
        .entry(authority.0.clone())
        .or_default();
    match next {
        Some(route) => {
            routes.insert(key.0.clone(), route.clone());
        }
        None => {
            routes.remove(&key.0);
        }
    }
}

#[derive(Default)]
struct ClusterRouteMesh {
    attached_locals: BTreeMap<String, usize>,
    routes_by_authority: BTreeMap<String, BTreeMap<String, ObjectRoute>>,
}

impl ClusterRouteMesh {
    fn register_local(&mut self, stable_id: &ClientStableId) {
        *self.attached_locals.entry(stable_id.0.clone()).or_default() += 1;
        self.routes_by_authority
            .entry(stable_id.0.clone())
            .or_default();
    }

    fn unregister_local(&mut self, stable_id: &ClientStableId) {
        let key = stable_id.0.clone();
        match self.attached_locals.get_mut(&key) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                self.attached_locals.remove(&key);
                self.routes_by_authority.remove(&key);
            }
            None => {}
        }
    }

    fn is_local(&self, stable_id: &ClientStableId) -> bool {
        self.attached_locals
            .get(&stable_id.0)
            .is_some_and(|count| *count > 0)
    }
}

fn route_mesh(namespace: &str) -> Arc<Mutex<ClusterRouteMesh>> {
    static ROUTE_MESHES: OnceLock<Mutex<BTreeMap<String, Arc<Mutex<ClusterRouteMesh>>>>> =
        OnceLock::new();
    let meshes = ROUTE_MESHES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = meshes.lock();
    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(ClusterRouteMesh::default())))
        .clone()
}

fn compatibility_matches(left: &ClientLease, right: &ClientLease) -> bool {
    left.compatibility.store_api_version == right.compatibility.store_api_version
        && left.compatibility.metadata_schema_version == right.compatibility.metadata_schema_version
        && left.compatibility.transport_api_version == right.compatibility.transport_api_version
}

fn route_capable(lease: &ClientLease) -> bool {
    lease
        .endpoints
        .labels
        .get("route")
        .is_some_and(|value| value == "true")
}

fn route_weight(lease: &ClientLease) -> f64 {
    lease
        .endpoints
        .labels
        .get("route_weight")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|weight| *weight > 0.0)
        .unwrap_or(1.0)
}

fn route_state_rank(state: RouteState) -> u8 {
    match state {
        RouteState::Active => 0,
        RouteState::Deleting => 1,
        RouteState::Tombstone => 2,
    }
}

fn replica_tier_rank(tier: ReplicaTier) -> u8 {
    match tier {
        ReplicaTier::Dram => 0,
        ReplicaTier::Nvme => 1,
        ReplicaTier::File => 2,
        ReplicaTier::Unknown => 3,
    }
}

fn canonical_route_key(route: &ObjectRoute) -> String {
    let mut key = String::new();
    let _ = write!(
        key,
        "{}|{}|{}|{}|{}|{}|",
        route.key.0,
        route.version.0,
        route_state_rank(route.state),
        route.compatibility.store_api_version,
        route.compatibility.metadata_schema_version,
        route.compatibility.transport_api_version,
    );
    for capability in &route.compatibility.capabilities {
        key.push_str(capability);
        key.push(',');
    }
    key.push('|');
    for replica in &route.replicas {
        append_replica_key(&mut key, replica);
    }
    key
}

fn append_replica_key(key: &mut String, replica: &ReplicaRoute) {
    let _ = write!(
        key,
        "{}|{}|{}|{}|{}|{}|{}|{};",
        replica.owner,
        replica.segment_name.0,
        replica.offset,
        replica.segment_offset,
        replica.length,
        replica.checksum.unwrap_or_default(),
        replica_tier_rank(replica.tier),
        replica.priority,
    );
}

fn record_route_repair_metric(operation: &'static str) {
    let result: Result<()> = Ok(());
    OperationTracker::new(operation).finish(&result, 0);
}

fn ranked_before(
    candidate_score: f64,
    candidate: &ClientLease,
    current: Option<&(f64, ClientLease)>,
) -> bool {
    let Some((current_score, current_lease)) = current else {
        return true;
    };
    match candidate_score.total_cmp(current_score) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => candidate.runtime.stable_id < current_lease.runtime.stable_id,
        std::cmp::Ordering::Greater => false,
    }
}

fn weighted_rendezvous_score(namespace: &str, key: &str, stable_id: &str, weight: f64) -> f64 {
    let hash = stable_hash(&[namespace, key, stable_id]);
    let unit = ((hash as f64) + 1.0) / ((u64::MAX as f64) + 2.0);
    -unit.ln() / weight.max(f64::MIN_POSITIVE)
}

fn stable_hash(parts: &[&str]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= u64::from(b'|');
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        ClientStableId, CompatibilityDescriptor, MetadataBackend, ObjectKey, ObjectRoute,
        ReplicaRoute, ReplicaTier, RouteCasRequest, RouteDirectory, RouteState, RouteVersion,
        SegmentName,
    };

    use super::{
        authority_compare_and_swap, authority_compare_and_swap_many, authority_get,
        authority_get_many, authority_list_routes_by_replica_owner, authority_replace,
        authority_replace_many, build_route_directory, compatibility_matches, ranked_before,
        route_capable, route_mesh, route_weight, stable_hash, weighted_rendezvous_score,
        ClusterRouteMesh, ControlPlaneClient, EmbeddedWrhRouteDirectory, RouteControlMode,
    };

    fn lease(stable_id: &str, epoch: u64, route: bool, route_scope: Option<&str>) -> ClientLease {
        let mut labels = BTreeMap::new();
        if route {
            labels.insert("route".to_string(), "true".to_string());
        }
        if let Some(route_scope) = route_scope {
            labels.insert("route_scope".to_string(), route_scope.to_string());
        }
        labels.insert("route_weight".to_string(), format!("{}", epoch.max(1)));
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(epoch)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet {
                rpc_address: format!("127.0.0.1:{}", 7000 + epoch as u16),
                segment_name: Some(SegmentName::new(format!("{stable_id}-segment"))),
                labels,
            },
            expires_at_ms: 60_000,
        }
    }

    fn route_for(key: &str, owner: &ClientRuntimeId) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new(key),
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: owner.clone(),
                segment_name: SegmentName::new("seg-a"),
                offset: 64,
                segment_offset: 64,
                length: 32,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
        }
    }

    #[test]
    fn authority_list_routes_filters_by_replica_owner() {
        let namespace = "route-directory-test";
        let authority = ClientStableId::new("authority-a");
        let owner_a = ClientRuntimeId::new("owner-a", ClientEpoch(1));
        let owner_b = ClientRuntimeId::new("owner-b", ClientEpoch(1));
        route_mesh(namespace).lock().register_local(&authority);

        let route_a = ObjectRoute {
            key: ObjectKey::new("tenant-a::key-a"),
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: owner_a.clone(),
                segment_name: SegmentName::new("seg-a"),
                offset: 64,
                segment_offset: 64,
                length: 32,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
        };
        let route_b = ObjectRoute {
            key: ObjectKey::new("tenant-a::key-b"),
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: owner_b.clone(),
                segment_name: SegmentName::new("seg-b"),
                offset: 128,
                segment_offset: 128,
                length: 32,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
        };

        authority_replace(namespace, &authority, &route_a.key, Some(&route_a))
            .expect("route-a replace should succeed");
        authority_replace(namespace, &authority, &route_b.key, Some(&route_b))
            .expect("route-b replace should succeed");

        let found = authority_list_routes_by_replica_owner(namespace, &authority, &owner_a)
            .expect("owner filter should succeed");
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].key, route_a.key);

        route_mesh(namespace).lock().unregister_local(&authority);
    }

    #[test]
    fn metadata_only_directory_round_trips_routes_via_metadata() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let observer = lease("observer", 1, true, None);
        let control = Arc::new(ControlPlaneClient::new().expect("control client should build"));
        let directory = build_route_directory(
            RouteControlMode::MetadataOnly,
            metadata.clone(),
            &observer,
            control,
        );
        let key = ObjectKey::new("tenant-a::alpha");
        let route_v1 = route_for(&key.0, &observer.runtime);
        let route_v2 = ObjectRoute {
            version: RouteVersion(2),
            ..route_v1.clone()
        };

        let created = directory
            .compare_and_swap_object_route(&observer, &key, None, Some(&route_v1))
            .expect("metadata-only create should succeed");
        assert!(created.applied);
        assert_eq!(created.current, Some(route_v1.clone()));
        assert_eq!(
            directory
                .get_object_route(&observer, &key)
                .expect("metadata-only get should succeed"),
            Some(route_v1.clone())
        );

        let rejected = directory
            .compare_and_swap_object_route(&observer, &key, None, Some(&route_v2))
            .expect("metadata-only cas rejection should succeed");
        assert!(!rejected.applied);

        let updated = directory
            .compare_and_swap_object_route(&observer, &key, Some(RouteVersion(1)), Some(&route_v2))
            .expect("metadata-only update should succeed");
        assert!(updated.applied);
        assert_eq!(
            metadata
                .get_object_route(&key)
                .expect("metadata get should succeed"),
            Some(route_v2)
        );
    }

    #[test]
    fn authority_helpers_and_scoring_cover_local_mesh_behaviors() {
        let namespace = "route-directory-helpers";
        let authority = ClientStableId::new("authority-helpers");
        let owner = ClientRuntimeId::new("owner-a", ClientEpoch(1));
        let mut mesh = ClusterRouteMesh::default();
        assert!(!mesh.is_local(&authority));
        mesh.register_local(&authority);
        mesh.register_local(&authority);
        assert!(mesh.is_local(&authority));
        mesh.unregister_local(&authority);
        assert!(mesh.is_local(&authority));
        mesh.unregister_local(&authority);
        assert!(!mesh.is_local(&authority));

        route_mesh(namespace).lock().register_local(&authority);
        let route_a = route_for("tenant-a::alpha", &owner);
        let route_b = route_for("tenant-a::beta", &owner);
        let route_c = route_for("tenant-a::gamma", &owner);

        let created =
            authority_compare_and_swap(namespace, &authority, &route_a.key, None, Some(&route_a))
                .expect("single cas create should succeed");
        assert!(created.applied);
        assert_eq!(
            authority_get(namespace, &authority, &route_a.key).expect("single get should succeed"),
            Some(route_a.clone())
        );

        let batched = authority_compare_and_swap_many(
            namespace,
            &authority,
            &[
                RouteCasRequest {
                    key: route_a.key.clone(),
                    expected: Some(RouteVersion(9)),
                    next: Some(route_b.clone()),
                },
                RouteCasRequest {
                    key: route_b.key.clone(),
                    expected: None,
                    next: Some(route_b.clone()),
                },
            ],
        )
        .expect("batch cas should succeed");
        assert!(!batched[0].applied);
        assert!(batched[1].applied);

        authority_replace_many(
            namespace,
            &authority,
            &[
                RouteCasRequest {
                    key: route_c.key.clone(),
                    expected: None,
                    next: Some(route_c.clone()),
                },
                RouteCasRequest {
                    key: route_a.key.clone(),
                    expected: None,
                    next: None,
                },
            ],
        )
        .expect("batch replace should succeed");
        let listed = authority_get_many(
            namespace,
            &authority,
            &[
                route_a.key.clone(),
                route_b.key.clone(),
                route_c.key.clone(),
            ],
        )
        .expect("batch get should succeed");
        assert_eq!(listed[0], None);
        assert_eq!(listed[1], Some(route_b.clone()));
        assert_eq!(listed[2], Some(route_c.clone()));

        let capable = lease("capable", 2, true, Some("scope-a"));
        let incapable = lease("incapable", 2, false, Some("scope-a"));
        assert!(route_capable(&capable));
        assert!(!route_capable(&incapable));
        assert_eq!(route_weight(&capable), 2.0);

        let same_hash = stable_hash(&["ns", "key", "authority"]);
        assert_eq!(same_hash, stable_hash(&["ns", "key", "authority"]));
        let weighted = weighted_rendezvous_score("ns", "key", "authority", 2.0);
        assert!(weighted.is_finite());
        assert!(ranked_before(
            weighted,
            &capable,
            Some(&(weighted, lease("zzz", 1, true, Some("scope-a"))))
        ));

        let mut incompatible = capable.clone();
        incompatible.compatibility.store_api_version += 1;
        assert!(!compatibility_matches(&capable, &incompatible));

        route_mesh(namespace).lock().unregister_local(&authority);
        assert!(authority_get(namespace, &authority, &route_b.key).is_err());
    }

    #[test]
    fn embedded_directory_prefers_scoped_local_authorities_and_mirrors_secondaries() {
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let observer = lease("observer", 1, false, Some("scope-a"));
        let authority_a = lease("authority-a", 1, true, Some("scope-a"));
        let authority_b = lease("authority-b", 2, true, Some("scope-a"));
        let other_scope = lease("authority-c", 3, true, Some("scope-b"));
        let incompatible = {
            let mut lease = lease("authority-d", 4, true, Some("scope-a"));
            lease.compatibility.transport_api_version += 1;
            lease
        };
        for lease in [
            observer.clone(),
            authority_a.clone(),
            authority_b.clone(),
            other_scope,
            incompatible,
        ] {
            metadata
                .upsert_client_lease(&lease)
                .expect("lease upsert should succeed");
        }

        let control = Arc::new(ControlPlaneClient::new().expect("control client should build"));
        let directory = EmbeddedWrhRouteDirectory::new(metadata.clone(), &observer, control);
        let namespace = metadata.route_namespace();
        route_mesh(&namespace)
            .lock()
            .register_local(&authority_a.runtime.stable_id);
        route_mesh(&namespace)
            .lock()
            .register_local(&authority_b.runtime.stable_id);

        let candidates = directory
            .authority_candidates(&observer)
            .expect("authority selection should succeed");
        assert_eq!(candidates.len(), 2);
        assert!(candidates.iter().all(|lease| {
            lease
                .endpoints
                .labels
                .get("route_scope")
                .map(String::as_str)
                == Some("scope-a")
        }));

        let key = ObjectKey::new("tenant-a::embedded-key");
        let request = RouteCasRequest {
            key: key.clone(),
            expected: None,
            next: Some(route_for(&key.0, &authority_b.runtime)),
        };
        let results = directory
            .compare_and_swap_object_routes(&observer, std::slice::from_ref(&request))
            .expect("embedded cas should succeed");
        assert_eq!(results.len(), 1);
        assert!(
            results[0]
                .as_ref()
                .expect("cas reply should decode")
                .applied
        );

        let route_on_a = authority_get(&namespace, &authority_a.runtime.stable_id, &key)
            .expect("route get should succeed");
        let route_on_b = authority_get(&namespace, &authority_b.runtime.stable_id, &key)
            .expect("route get should succeed");
        assert_eq!(route_on_a, route_on_b);

        assert!(directory
            .get_object_routes(&observer, &[])
            .expect("empty batch get should succeed")
            .is_empty());
        assert!(directory
            .compare_and_swap_object_routes(&observer, &[])
            .expect("empty batch cas should succeed")
            .is_empty());

        sleep(Duration::from_millis(150));
        let authority_new = lease("authority-new", 5, true, Some("scope-a"));
        metadata
            .upsert_client_lease(&authority_new)
            .expect("new lease should upsert");
        let refreshed = directory
            .authority_candidates(&observer)
            .expect("refreshed candidate set should succeed");
        assert!(refreshed
            .iter()
            .any(|lease| lease.runtime == authority_new.runtime));

        route_mesh(&namespace)
            .lock()
            .unregister_local(&authority_a.runtime.stable_id);
        route_mesh(&namespace)
            .lock()
            .unregister_local(&authority_b.runtime.stable_id);
    }
}
