use mooncake_store_core::{
    route_reuse_identity, CasResult, ClientLease, ClientLifecycleState, ClientRuntimeId,
    ClientStableId, MetadataBackend, NamespaceScope, ObjectKey, ObjectRoute, ReplicaRoute,
    ReplicaTier, Result, ReuseIdentity, RouteCasRequest, RouteControlMode, RouteDirectory,
    RouteState, RouteVersion, StoreError,
};
use parking_lot::{Mutex, RwLock};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

const ROUTE_SCOPE_LABEL: &str = "route_scope";
const ROUTE_REPAIR_MISSING_AUTHORITY: &str = "route_repair_missing_authority";
const ROUTE_REPAIR_STALE_AUTHORITY: &str = "route_repair_stale_authority";
const ROUTE_REPAIR_DIVERGENT_AUTHORITY: &str = "route_repair_divergent_authority";
const SUSPECT_AUTHORITY_TTL: Duration = Duration::from_secs(5);
const PER_KEY_DEBUG_SAMPLE_MODULUS: u64 = 128;

pub trait RouteAuthorityClient: Send + Sync {
    fn batch_get_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<Option<ObjectRoute>>>>;

    fn batch_compare_and_swap_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<CasResult>>>;

    fn batch_replace_routes(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Result<Vec<Result<()>>>;

    fn list_routes_by_replica_owner(
        &self,
        lease: &ClientLease,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>>;
}

pub trait RouteAuthorityService: Send + Sync {
    fn get_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>>;

    fn list_routes_by_replica_owner(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>>;

    fn compare_and_swap_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult>;

    fn replace_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> Result<()>;

    fn batch_get_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<Option<ObjectRoute>>> {
        keys.iter()
            .map(|key| self.get_route(namespace, authority, key))
            .collect()
    }

    fn batch_compare_and_swap_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<CasResult>> {
        requests
            .iter()
            .map(|request| {
                self.compare_and_swap_route(
                    namespace,
                    authority,
                    &request.key,
                    request.expected,
                    request.next.as_ref(),
                )
            })
            .collect()
    }

    fn batch_replace_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<()>> {
        requests
            .iter()
            .map(|request| {
                self.replace_route(namespace, authority, &request.key, request.next.as_ref())
            })
            .collect()
    }
}

pub trait RouteMembershipProvider: Send + Sync {
    fn live_clients(
        &self,
        force_refresh: bool,
        operation: &'static str,
    ) -> Result<Vec<ClientLease>>;

    fn reconcile_suspects(&self, leases: &[ClientLease]);

    fn is_suspect(&self, runtime: &ClientRuntimeId) -> bool;

    fn mark_suspect(
        &self,
        runtime: ClientRuntimeId,
        quarantine_until: Instant,
        observed: Option<&ClientLease>,
    );
}

pub trait RouteMetricsSink: Send + Sync {
    fn record_route_repair(&self, operation: &'static str);

    fn record_route_cas(&self, outcome: &'static str);

    fn record_route(&self, route: &ObjectRoute);

    fn remove_route(&self, key: &ObjectKey);
}

struct NoopRouteMetricsSink;

impl RouteMetricsSink for NoopRouteMetricsSink {
    fn record_route_repair(&self, _operation: &'static str) {}

    fn record_route_cas(&self, _outcome: &'static str) {}

    fn record_route(&self, _route: &ObjectRoute) {}

    fn remove_route(&self, _key: &ObjectKey) {}
}

pub fn set_route_metrics_sink(sink: Arc<dyn RouteMetricsSink>) {
    *route_metrics_sink().write() = sink;
}

fn route_metrics_sink() -> &'static RwLock<Arc<dyn RouteMetricsSink>> {
    static SINK: OnceLock<RwLock<Arc<dyn RouteMetricsSink>>> = OnceLock::new();
    SINK.get_or_init(|| RwLock::new(Arc::new(NoopRouteMetricsSink)))
}

pub fn build_route_directory(
    mode: RouteControlMode,
    route_topk: usize,
    metadata: Arc<dyn MetadataBackend>,
    lease: &ClientLease,
    authority_client: Arc<dyn RouteAuthorityClient>,
    membership: Arc<dyn RouteMembershipProvider>,
) -> Arc<dyn RouteDirectory> {
    match mode {
        RouteControlMode::MetadataOnly => Arc::new(MetadataRouteDirectory { metadata }),
        RouteControlMode::EmbeddedWrh => Arc::new(EmbeddedWrhRouteDirectory::new(
            route_topk,
            metadata,
            lease,
            authority_client,
            membership,
        )),
    }
}

pub fn bind_local_authority_service(
    namespace: &str,
    authority: &ClientStableId,
    service: Arc<dyn RouteAuthorityService>,
) {
    route_mesh(namespace)
        .lock()
        .bind_local_service(authority, service);
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
        let result = self
            .metadata
            .compare_and_swap_object_route(key, expected, next);
        if let Ok(cas) = &result {
            record_cas_outcome(cas, next, key);
        }
        result
    }

    fn list_routes_by_replica_owner(
        &self,
        _observer: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        Ok(self
            .metadata
            .list_object_routes()?
            .into_iter()
            .filter(|route| route.replicas.iter().any(|replica| replica.owner == *owner))
            .collect())
    }

    fn list_routes_in_scope(
        &self,
        _observer: &ClientLease,
        scope: &NamespaceScope,
    ) -> Result<Vec<ObjectRoute>> {
        self.metadata.list_object_routes_in_scope(scope)
    }

    fn list_reuse_candidates(
        &self,
        _observer: &ClientLease,
        reuse: &ReuseIdentity,
    ) -> Result<Vec<ObjectRoute>> {
        self.metadata.list_reuse_candidates(reuse)
    }
}

struct EmbeddedWrhRouteDirectory {
    route_topk: usize,
    namespace: String,
    local_stable_id: ClientStableId,
    authority_client: Arc<dyn RouteAuthorityClient>,
    membership: Arc<dyn RouteMembershipProvider>,
}

#[derive(Clone)]
struct RouteAuthoritySelection {
    authorities: Vec<ClientLease>,
}

type RouteReadRepairState = Vec<RouteAuthorityObservation>;

#[derive(Clone, Default)]
enum RouteAuthorityObservation {
    #[default]
    Unobserved,
    Missing,
    Present(Box<ObjectRoute>),
}

struct RouteCasAttempt {
    resolved: Vec<Option<Result<CasResult>>>,
    resolved_authorities: Vec<Option<ClientLease>>,
    last_errors: Vec<Option<StoreError>>,
}

impl EmbeddedWrhRouteDirectory {
    fn new(
        route_topk: usize,
        metadata: Arc<dyn MetadataBackend>,
        lease: &ClientLease,
        authority_client: Arc<dyn RouteAuthorityClient>,
        membership: Arc<dyn RouteMembershipProvider>,
    ) -> Self {
        let namespace = metadata.route_namespace();
        route_mesh(&namespace)
            .lock()
            .register_local(&lease.runtime.stable_id);
        Self {
            route_topk,
            namespace,
            local_stable_id: lease.runtime.stable_id.clone(),
            authority_client,
            membership,
        }
    }

    fn active_route_leases(&self, force_refresh: bool) -> Result<Vec<ClientLease>> {
        let leases = if force_refresh {
            self.membership
                .live_clients(true, "live_client_snapshot_force_refresh")?
        } else {
            self.membership
                .live_clients(false, "live_client_snapshot")?
        };
        self.membership.reconcile_suspects(&leases);
        Ok(leases
            .into_iter()
            .filter(|lease| lease.state == ClientLifecycleState::Active && route_capable(lease))
            .filter(|lease| !self.membership.is_suspect(&lease.runtime))
            .collect())
    }

    fn maybe_mark_authority_suspect(
        &self,
        authority: &ClientLease,
        error: &StoreError,
        context: &'static str,
    ) {
        if self.authority_is_local(authority)
            || !matches!(
                error,
                StoreError::Transport(_)
                    | StoreError::NotFound(_)
                    | StoreError::InvalidState(_)
                    | StoreError::Unsupported(_)
            )
        {
            return;
        }
        self.membership.mark_suspect(
            authority.runtime.clone(),
            Instant::now() + SUSPECT_AUTHORITY_TTL,
            Some(authority),
        );
        warn!(
            runtime = %authority.runtime,
            context,
            quarantine_ms = SUSPECT_AUTHORITY_TTL.as_millis() as u64,
            "marked route authority as suspect after request failure"
        );
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
        self.authority_candidates_once(observer, false)
    }

    fn select_authorities_for_requests(
        &self,
        candidates: &[ClientLease],
        requests: &[RouteCasRequest],
    ) -> Vec<RouteAuthoritySelection> {
        requests
            .iter()
            .map(|request| self.select_authorities_from_candidates(candidates, &request.key))
            .collect()
    }

    fn select_authorities_from_candidates(
        &self,
        candidates: &[ClientLease],
        key: &ObjectKey,
    ) -> RouteAuthoritySelection {
        let ranked = self.ranked_authorities_from_candidates(candidates, key);
        RouteAuthoritySelection {
            authorities: ranked.into_iter().take(self.route_topk).collect(),
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

    fn ranked_top_authorities_from_candidates(
        &self,
        candidates: &[ClientLease],
        key: &ObjectKey,
        limit: usize,
    ) -> Vec<ClientLease> {
        let mut top = Vec::<(f64, ClientLease)>::with_capacity(limit.min(candidates.len()));
        for lease in candidates {
            let score = weighted_rendezvous_score(
                &self.namespace,
                &key.0,
                &lease.runtime.stable_id.0,
                route_weight(lease),
            );
            let insert_at = top.partition_point(|(current_score, current_lease)| {
                current_score
                    .total_cmp(&score)
                    .then_with(|| {
                        current_lease
                            .runtime
                            .stable_id
                            .cmp(&lease.runtime.stable_id)
                    })
                    .is_lt()
            });
            if insert_at < limit {
                top.insert(insert_at, (score, lease.clone()));
                top.truncate(limit);
            }
        }
        top.into_iter().map(|(_, lease)| lease).collect()
    }

    fn local_authority_service(
        &self,
        authority: &ClientStableId,
    ) -> Option<Arc<dyn RouteAuthorityService>> {
        route_mesh(&self.namespace)
            .lock()
            .authority_service(authority)
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
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return Ok(service.batch_get_routes(
                    &self.namespace,
                    &authority.runtime.stable_id,
                    keys,
                ));
            }
            return self
                .read_local_batch(&authority.runtime.stable_id, keys)
                .map(|routes| routes.into_iter().map(Ok).collect());
        }
        self.authority_client.batch_get_routes(
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
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return Ok(service.batch_compare_and_swap_routes(
                    &self.namespace,
                    &authority.runtime.stable_id,
                    requests,
                ));
            }
            return self
                .cas_local_batch(&authority.runtime.stable_id, requests)
                .map(|results| results.into_iter().map(Ok).collect());
        }
        self.authority_client.batch_compare_and_swap_routes(
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
        let result = if self.authority_is_local(secondary) {
            if let Some(service) = self.local_authority_service(&secondary.runtime.stable_id) {
                Ok(service.batch_replace_routes(
                    &self.namespace,
                    &secondary.runtime.stable_id,
                    requests,
                ))
            } else {
                self.replace_local_batch(&secondary.runtime.stable_id, requests)
                    .map(|_| requests.iter().map(|_| Ok(())).collect())
            }
        } else {
            self.authority_client.batch_replace_routes(
                secondary,
                &self.namespace,
                &secondary.runtime.stable_id,
                requests,
            )
        };
        match result {
            Ok(results) => {
                for (request, result) in requests.iter().zip(results) {
                    if let Err(error) = result {
                        self.maybe_mark_authority_suspect(
                            secondary,
                            &error,
                            "route_secondary_mirror_failed",
                        );
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
                self.maybe_mark_authority_suspect(
                    secondary,
                    &error,
                    "route_secondary_mirror_batch_failed",
                );
                warn!(
                    authority = %secondary.runtime,
                    error = %error,
                    items = requests.len(),
                    "secondary route mirror batch failed"
                );
            }
        }
    }

    fn authority_is_local(&self, authority: &ClientLease) -> bool {
        route_mesh(&self.namespace)
            .lock()
            .is_local(&authority.runtime.stable_id)
    }

    fn list_routes_by_replica_owner_from_authority(
        &self,
        authority: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return service.list_routes_by_replica_owner(
                    &self.namespace,
                    &authority.runtime.stable_id,
                    owner,
                );
            }
            return authority_list_routes_by_replica_owner(
                &self.namespace,
                &authority.runtime.stable_id,
                owner,
            );
        }
        self.authority_client.list_routes_by_replica_owner(
            authority,
            &self.namespace,
            &authority.runtime.stable_id,
            owner,
        )
    }

    fn list_routes_in_scope_from_authority(
        &self,
        authority: &ClientLease,
        scope: &NamespaceScope,
    ) -> Result<Vec<ObjectRoute>> {
        if self.authority_is_local(authority) {
            return authority_list_routes_in_scope(
                &self.namespace,
                &authority.runtime.stable_id,
                scope,
            );
        }
        Err(StoreError::Unsupported(
            "remote route authority does not support list_routes_in_scope".to_string(),
        ))
    }

    fn list_reuse_candidates_from_authority(
        &self,
        authority: &ClientLease,
        reuse: &ReuseIdentity,
    ) -> Result<Vec<ObjectRoute>> {
        if self.authority_is_local(authority) {
            return authority_list_reuse_candidates(
                &self.namespace,
                &authority.runtime.stable_id,
                reuse,
            );
        }
        Err(StoreError::Unsupported(
            "remote route authority does not support list_reuse_candidates".to_string(),
        ))
    }

    fn read_ranked_authorities(
        &self,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
        rank: usize,
        resolved: &mut [Option<ObjectRoute>],
        repairs: Option<&mut [RouteReadRepairState]>,
        failure_message: &'static str,
    ) -> Result<bool> {
        let mut groups = BTreeMap::<String, (ClientLease, Vec<usize>)>::new();
        for (index, authorities) in ranked_authorities.iter().enumerate() {
            let Some(authority) = authorities.get(rank).cloned() else {
                continue;
            };
            if resolved[index].is_some()
                && (repairs.is_none() || !self.authority_is_local(&authority))
            {
                continue;
            }
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
                    for ((index, key), result) in indices.into_iter().zip(batch_keys).zip(results) {
                        match result {
                            Ok(Some(route)) => {
                                trace!(
                                    namespace = %self.namespace,
                                    key = %key.0,
                                    authority = %authority.runtime,
                                    rank,
                                    source = route_read_source(rank, self.route_topk),
                                    route_version = route.version.0,
                                    replica_count = route.replicas.len(),
                                    "route authority returned object route"
                                );
                                if sampled_per_key_debug_log(&[
                                    "route_authority_object_route",
                                    &self.namespace,
                                    &key.0,
                                    &authority.runtime.stable_id.0,
                                ]) {
                                    debug!(
                                        key = %key.0,
                                        authority = %authority.runtime,
                                        source = route_read_source(rank, self.route_topk),
                                        route_version = route.version.0,
                                        replica_count = route.replicas.len(),
                                        "sampled route authority object route"
                                    );
                                }
                                if let Some(states) = repairs.as_mut() {
                                    Self::set_repair_observation(
                                        &mut states[index],
                                        rank,
                                        RouteAuthorityObservation::Present(Box::new(route.clone())),
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
                                if let Some(states) = repairs.as_mut() {
                                    Self::set_repair_observation(
                                        &mut states[index],
                                        rank,
                                        RouteAuthorityObservation::Missing,
                                    );
                                }
                            }
                            Err(error) => {
                                self.maybe_mark_authority_suspect(
                                    &authority,
                                    &error,
                                    "route_batch_read_failed",
                                );
                                warn!(
                                    runtime = %authority.runtime,
                                    key = %key.0,
                                    error = %error,
                                    "{failure_message}"
                                );
                            }
                        }
                    }
                }
                Err(error) => {
                    self.maybe_mark_authority_suspect(
                        &authority,
                        &error,
                        "route_batch_read_transport_failed",
                    );
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

    fn query_authorities_with_merge<Q, L>(
        &self,
        observer: &ClientLease,
        suspect_context: &'static str,
        mut query: Q,
        mut log_error: L,
    ) -> Result<Vec<ObjectRoute>>
    where
        Q: FnMut(&ClientLease) -> Result<Vec<ObjectRoute>>,
        L: FnMut(&ClientLease, &StoreError),
    {
        let mut routes = BTreeMap::<String, ObjectRoute>::new();
        let mut attempted = 0usize;
        let mut successful_queries = 0usize;
        let mut last_error = None;
        for authority in self.authority_candidates(observer)? {
            attempted = attempted.saturating_add(1);
            match query(&authority) {
                Ok(found) => {
                    successful_queries = successful_queries.saturating_add(1);
                    for route in found {
                        Self::merge_route_listing(
                            &mut routes,
                            route,
                            &authority.runtime.to_string(),
                        );
                    }
                }
                Err(error) => {
                    self.maybe_mark_authority_suspect(&authority, &error, suspect_context);
                    log_error(&authority, &error);
                    last_error = Some(error);
                }
            }
        }
        if successful_queries == 0 && attempted > 0 {
            if let Some(error) = last_error {
                return Err(error);
            }
        }
        Ok(routes.into_values().collect())
    }

    fn merge_route_listing(
        routes: &mut BTreeMap<String, ObjectRoute>,
        candidate: ObjectRoute,
        authority: &str,
    ) {
        match routes.get(&candidate.key.0) {
            None => {
                routes.insert(candidate.key.0.clone(), candidate);
            }
            Some(current) if candidate.version > current.version => {
                routes.insert(candidate.key.0.clone(), candidate);
            }
            Some(current) if candidate.version == current.version && *current != candidate => {
                let choose_candidate =
                    canonical_route_key(&candidate) < canonical_route_key(current);
                if choose_candidate {
                    routes.insert(candidate.key.0.clone(), candidate.clone());
                }
                warn!(
                    key = %candidate.key.0,
                    authority,
                    version = candidate.version.0,
                    canonical = if choose_candidate { "candidate" } else { "existing" },
                    "route authorities disagree on the same route version; choosing canonical route"
                );
            }
            Some(_) => {}
        }
    }

    fn set_repair_observation(
        state: &mut RouteReadRepairState,
        rank: usize,
        observation: RouteAuthorityObservation,
    ) {
        if let Some(slot) = state.get_mut(rank) {
            *slot = observation;
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
                if current.version == best.version && current.as_ref() != best =>
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
            for (rank, authority) in selections[index].authorities.iter().cloned().enumerate() {
                if let Some(request) = repairs[index]
                    .get(rank)
                    .and_then(|observed| Self::repair_request(&keys[index], route, observed))
                {
                    backfills
                        .entry(authority.runtime.stable_id.0.clone())
                        .or_insert_with(|| (authority, Vec::new()))
                        .1
                        .push(request);
                }
            }
        }

        for (_, (authority, requests)) in backfills {
            match self.cas_authority_batch(&authority, &requests) {
                Ok(results) => {
                    for (request, result) in requests.iter().zip(results) {
                        if let Err(error) = result {
                            self.maybe_mark_authority_suspect(
                                &authority,
                                &error,
                                "route_backfill_failed",
                            );
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
                    self.maybe_mark_authority_suspect(
                        &authority,
                        &error,
                        "route_backfill_batch_failed",
                    );
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

    fn route_authority_error_should_refresh(error: &StoreError) -> bool {
        matches!(
            error,
            StoreError::Transport(_)
                | StoreError::NotFound(_)
                | StoreError::InvalidState(_)
                | StoreError::Unsupported(_)
        )
    }

    fn should_refresh_route_cas_attempt(attempt: &RouteCasAttempt) -> bool {
        attempt.resolved.iter().enumerate().any(|(index, result)| {
            if result.is_some() {
                return false;
            }
            match attempt.last_errors[index].as_ref() {
                Some(error) => Self::route_authority_error_should_refresh(error),
                None => true,
            }
        })
    }

    fn compare_and_swap_route_selections(
        &self,
        selections: &[RouteAuthoritySelection],
        requests: &[RouteCasRequest],
    ) -> RouteCasAttempt {
        let mut resolved = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<_>>();
        let mut resolved_authorities = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<Option<ClientLease>>>();
        let mut last_errors = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<Option<StoreError>>>();

        for rank in 0..self.route_topk {
            let mut groups = BTreeMap::<String, (ClientLease, Vec<usize>)>::new();
            for (index, selection) in selections.iter().enumerate() {
                if resolved[index].is_some() {
                    continue;
                }
                let Some(authority) = selection.authorities.get(rank).cloned() else {
                    continue;
                };
                groups
                    .entry(authority.runtime.stable_id.0.clone())
                    .or_insert_with(|| (authority, Vec::new()))
                    .1
                    .push(index);
            }

            for (_, (authority, indices)) in groups {
                let batch_requests = indices
                    .iter()
                    .map(|index| requests[*index].clone())
                    .collect::<Vec<_>>();
                if self.authority_is_local(&authority) {
                    if let Some(service) =
                        self.local_authority_service(&authority.runtime.stable_id)
                    {
                        let results = service.batch_compare_and_swap_routes(
                            &self.namespace,
                            &authority.runtime.stable_id,
                            &batch_requests,
                        );
                        for ((index, request), result) in
                            indices.into_iter().zip(batch_requests).zip(results)
                        {
                            match result {
                                Ok(result) => {
                                    resolved[index] = Some(Ok(result));
                                    resolved_authorities[index] = Some(authority.clone());
                                }
                                Err(error) => {
                                    self.maybe_mark_authority_suspect(
                                        &authority,
                                        &error,
                                        "route_batch_cas_local_failed",
                                    );
                                    warn!(
                                        runtime = %authority.runtime,
                                        key = %request.key.0,
                                        error = %error,
                                        "authority route cas failed; trying next mirrored authority"
                                    );
                                    last_errors[index] = Some(error);
                                }
                            }
                        }
                    } else {
                        match self.cas_local_batch(&authority.runtime.stable_id, &batch_requests) {
                            Ok(results) => {
                                for (index, result) in indices.into_iter().zip(results) {
                                    resolved[index] = Some(Ok(result));
                                    resolved_authorities[index] = Some(authority.clone());
                                }
                            }
                            Err(error) => {
                                self.maybe_mark_authority_suspect(
                                    &authority,
                                    &error,
                                    "route_batch_cas_local_failed",
                                );
                                warn!(
                                    runtime = %authority.runtime,
                                    error = %error,
                                    items = batch_requests.len(),
                                    "authority route batch cas failed; trying next mirrored authority"
                                );
                                for index in indices {
                                    last_errors[index] = Some(error.clone());
                                }
                            }
                        }
                    }
                    continue;
                }

                match self.authority_client.batch_compare_and_swap_routes(
                    &authority,
                    &self.namespace,
                    &authority.runtime.stable_id,
                    &batch_requests,
                ) {
                    Ok(results) => {
                        for ((index, request), result) in
                            indices.into_iter().zip(batch_requests).zip(results)
                        {
                            match result {
                                Ok(result) => {
                                    resolved[index] = Some(Ok(result));
                                    resolved_authorities[index] = Some(authority.clone());
                                }
                                Err(error) => {
                                    self.maybe_mark_authority_suspect(
                                        &authority,
                                        &error,
                                        "route_batch_cas_failed",
                                    );
                                    warn!(
                                        runtime = %authority.runtime,
                                        key = %request.key.0,
                                        error = %error,
                                        "authority route cas failed; trying next mirrored authority"
                                    );
                                    last_errors[index] = Some(error);
                                }
                            }
                        }
                    }
                    Err(error) => {
                        self.maybe_mark_authority_suspect(
                            &authority,
                            &error,
                            "route_batch_cas_transport_failed",
                        );
                        warn!(
                            runtime = %authority.runtime,
                            error = %error,
                            items = batch_requests.len(),
                            "authority route batch cas failed; trying next mirrored authority"
                        );
                        for index in indices {
                            last_errors[index] = Some(error.clone());
                        }
                    }
                }
            }
        }

        RouteCasAttempt {
            resolved,
            resolved_authorities,
            last_errors,
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
                authorities: authorities.iter().take(self.route_topk).cloned().collect(),
            })
            .collect::<Vec<_>>();
        let mut resolved = vec![None; keys.len()];
        let mut repairs =
            vec![vec![RouteAuthorityObservation::Unobserved; self.route_topk]; keys.len()];

        for rank in 0..self.route_topk {
            self.read_ranked_authorities(
                keys,
                &ranked_authorities,
                rank,
                &mut resolved,
                Some(repairs.as_mut_slice()),
                if rank == 0 {
                    "authority route batch read failed; trying mirrored authorities"
                } else {
                    "mirrored authority route batch read failed; trying other authorities"
                },
            )?;
        }

        let mut rank = self.route_topk;
        while self.read_ranked_authorities(
            keys,
            &ranked_authorities,
            rank,
            &mut resolved,
            None,
            "fallback authority route batch read failed; trying other authorities",
        )? {
            rank = rank.saturating_add(1);
        }

        self.backfill_missing_authorities(keys, &selections, &resolved, &repairs);
        Ok(resolved)
    }

    fn get_object_routes_bounded(
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
            .map(|key| {
                self.ranked_top_authorities_from_candidates(&candidates, key, self.route_topk)
            })
            .collect::<Vec<_>>();
        let mut resolved = vec![None; keys.len()];
        for rank in 0..self.route_topk {
            self.read_ranked_authorities(
                keys,
                &ranked_authorities,
                rank,
                &mut resolved,
                None,
                if rank == 0 {
                    "authority bounded route batch read failed; trying mirrored authorities"
                } else {
                    "mirrored bounded route batch read failed; trying other authorities"
                },
            )?;
        }
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
        let mut selections = self.select_authorities_for_requests(&candidates, requests);
        let mut attempt = self.compare_and_swap_route_selections(&selections, requests);

        if Self::should_refresh_route_cas_attempt(&attempt) {
            let retry_indices = attempt
                .resolved
                .iter()
                .enumerate()
                .filter_map(|(index, result)| result.is_none().then_some(index))
                .collect::<Vec<_>>();
            if !retry_indices.is_empty() {
                debug!(
                    runtime = %observer.runtime,
                    items = retry_indices.len(),
                    "refreshing live-client snapshot before retrying route authority selection"
                );
                let refreshed_candidates = self.authority_candidates_once(observer, true)?;
                let retry_requests = retry_indices
                    .iter()
                    .map(|index| requests[*index].clone())
                    .collect::<Vec<_>>();
                let retry_selections =
                    self.select_authorities_for_requests(&refreshed_candidates, &retry_requests);
                let retry_attempt =
                    self.compare_and_swap_route_selections(&retry_selections, &retry_requests);
                for (retry_pos, index) in retry_indices.into_iter().enumerate() {
                    selections[index] = retry_selections[retry_pos].clone();
                    attempt.resolved[index] = retry_attempt.resolved[retry_pos].clone();
                    attempt.resolved_authorities[index] =
                        retry_attempt.resolved_authorities[retry_pos].clone();
                    attempt.last_errors[index] = retry_attempt.last_errors[retry_pos].clone();
                }
            }
        }

        let mut mirrors = BTreeMap::<String, (ClientLease, Vec<RouteCasRequest>)>::new();
        let mut results = Vec::with_capacity(requests.len());
        for (index, request) in requests.iter().enumerate() {
            let result = match attempt.resolved[index].take() {
                Some(result) => result,
                None => Err(attempt.last_errors[index].take().unwrap_or_else(|| {
                    StoreError::NotFound(format!(
                        "no reachable mirrored route authority for {}",
                        request.key.0
                    ))
                })),
            };
            if result.as_ref().ok().is_some_and(|cas| cas.applied) {
                if let Some(served) = attempt.resolved_authorities[index].as_ref() {
                    for secondary in &selections[index].authorities {
                        if secondary.runtime.stable_id == served.runtime.stable_id {
                            continue;
                        }
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
            if let Ok(cas) = &result {
                record_cas_outcome(cas, request.next.as_ref(), &request.key);
            }
            results.push(result);
        }

        for (_, (secondary, requests)) in mirrors {
            self.mirror_secondary_batch(&secondary, &requests);
        }
        Ok(results)
    }

    fn list_routes_by_replica_owner(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        self.query_authorities_with_merge(
            observer,
            "route_list_by_replica_owner_failed",
            |authority| self.list_routes_by_replica_owner_from_authority(authority, owner),
            |authority, error| {
                warn!(
                    authority = %authority.runtime,
                    owner = %owner,
                    error = %error,
                    "route-owner listing failed on authority"
                );
            },
        )
    }

    fn list_routes_in_scope(
        &self,
        observer: &ClientLease,
        scope: &NamespaceScope,
    ) -> Result<Vec<ObjectRoute>> {
        self.query_authorities_with_merge(
            observer,
            "route_list_in_scope_failed",
            |authority| self.list_routes_in_scope_from_authority(authority, scope),
            |authority, error| {
                warn!(
                    authority = %authority.runtime,
                    tenant = %scope.tenant,
                    domain = %scope.domain,
                    object_set = %scope.object_set,
                    error = %error,
                    "route scope listing failed on authority"
                );
            },
        )
    }

    fn list_reuse_candidates(
        &self,
        observer: &ClientLease,
        reuse: &ReuseIdentity,
    ) -> Result<Vec<ObjectRoute>> {
        self.query_authorities_with_merge(
            observer,
            "route_list_reuse_candidates_failed",
            |authority| self.list_reuse_candidates_from_authority(authority, reuse),
            |authority, error| {
                warn!(
                    authority = %authority.runtime,
                    tenant = %reuse.tenant,
                    domain = %reuse.domain,
                    sharing_scope = %reuse.sharing_scope,
                    canonical_key = %reuse.canonical_key,
                    error = %error,
                    "route reuse listing failed on authority"
                );
            },
        )
    }

    fn get_version_floor(&self, observer: &ClientLease, key: &ObjectKey) -> Option<RouteVersion> {
        let candidates = self.authority_candidates(observer).ok()?;
        let ranked = self.ranked_authorities_from_candidates(&candidates, key);
        for authority in ranked.iter().take(self.route_topk) {
            if let Some(floor) =
                authority_get_version_floor(&self.namespace, &authority.runtime.stable_id, key)
            {
                return Some(floor);
            }
        }
        None
    }
}

pub fn authority_get(
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

pub fn authority_get_many(
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

pub fn authority_get_version_floor(
    namespace: &str,
    authority: &ClientStableId,
    key: &ObjectKey,
) -> Option<RouteVersion> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    guard
        .version_floors
        .get(&authority.0)
        .and_then(|floors| floors.get(&key.0))
        .copied()
}

pub fn authority_list_routes_by_replica_owner(
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

pub fn authority_list_routes(
    namespace: &str,
    authority: &ClientStableId,
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
        .cloned()
        .collect())
}

pub fn authority_list_routes_in_scope(
    namespace: &str,
    authority: &ClientStableId,
    scope: &NamespaceScope,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let routes = guard.routes_by_authority.get(&authority.0);
    Ok(guard
        .scope_index
        .get(&authority.0)
        .and_then(|index| index.get(scope))
        .into_iter()
        .flat_map(|keys| keys.iter())
        .filter_map(|key| routes.and_then(|routes| routes.get(key)).cloned())
        .collect())
}

pub fn authority_list_reuse_candidates(
    namespace: &str,
    authority: &ClientStableId,
    reuse: &ReuseIdentity,
) -> Result<Vec<ObjectRoute>> {
    let mesh = route_mesh(namespace);
    let guard = mesh.lock();
    if !guard.is_local(authority) {
        return Err(StoreError::NotFound(format!(
            "route authority {} is not attached locally",
            authority
        )));
    }
    let routes = guard.routes_by_authority.get(&authority.0);
    Ok(guard
        .reuse_index
        .get(&authority.0)
        .and_then(|index| index.get(reuse))
        .into_iter()
        .flat_map(|keys| keys.iter())
        .filter_map(|key| routes.and_then(|routes| routes.get(key)).cloned())
        .collect())
}

/// Returns true if a CAS(None→Some(route)) should be rejected because a version
/// floor exists that is >= the proposed route's version (anti-resurrection guard).
fn version_floor_blocks_insert(
    mesh: &ClusterRouteMesh,
    authority: &ClientStableId,
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
    mesh.version_floors
        .get(&authority.0)
        .and_then(|floors| floors.get(&key.0))
        .is_some_and(|floor| route.version <= *floor)
}

pub fn authority_compare_and_swap(
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
    if !matches || version_floor_blocks_insert(&guard, authority, key, expected, next) {
        let result = CasResult {
            applied: false,
            current,
        };
        record_cas_outcome(&result, next, key);
        return Ok(result);
    }
    apply_route_update(&mut guard, authority, key, next);
    let result = CasResult {
        applied: true,
        current: next.cloned(),
    };
    record_cas_outcome(&result, next, key);
    Ok(result)
}

pub fn authority_compare_and_swap_many(
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
        if !matches
            || version_floor_blocks_insert(
                &guard,
                authority,
                &request.key,
                request.expected,
                request.next.as_ref(),
            )
        {
            let result = CasResult {
                applied: false,
                current,
            };
            record_cas_outcome(&result, request.next.as_ref(), &request.key);
            results.push(result);
            continue;
        }
        apply_route_update(&mut guard, authority, &request.key, request.next.as_ref());
        let result = CasResult {
            applied: true,
            current: request.next.clone(),
        };
        record_cas_outcome(&result, request.next.as_ref(), &request.key);
        results.push(result);
    }
    Ok(results)
}

pub fn authority_replace(
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

pub fn authority_replace_many(
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
    let old = mesh
        .routes_by_authority
        .get(&authority.0)
        .and_then(|routes| routes.get(&key.0))
        .cloned();
    if let Some(route) = old.as_ref() {
        mesh.remove_route_indexes(authority, key, route);
    }
    {
        let routes = mesh
            .routes_by_authority
            .entry(authority.0.clone())
            .or_default();
        match next {
            Some(route) => {
                routes.insert(key.0.clone(), route.clone());
                if let Some(floors) = mesh.version_floors.get_mut(&authority.0) {
                    floors.remove(&key.0);
                }
            }
            None => {
                routes.remove(&key.0);
                if let Some(old) = old.as_ref() {
                    mesh.version_floors
                        .entry(authority.0.clone())
                        .or_default()
                        .insert(key.0.clone(), old.version);
                }
            }
        }
    }
    if let Some(route) = next {
        mesh.insert_route_indexes(authority, key, route);
    }
}

#[derive(Default)]
struct ClusterRouteMesh {
    attached_locals: BTreeMap<String, usize>,
    routes_by_authority: BTreeMap<String, BTreeMap<String, ObjectRoute>>,
    scope_index: BTreeMap<String, BTreeMap<NamespaceScope, BTreeSet<String>>>,
    reuse_index: BTreeMap<String, BTreeMap<ReuseIdentity, BTreeSet<String>>>,
    authority_services: BTreeMap<String, Arc<dyn RouteAuthorityService>>,
    version_floors: BTreeMap<String, BTreeMap<String, RouteVersion>>,
}

impl ClusterRouteMesh {
    fn register_local(&mut self, stable_id: &ClientStableId) {
        *self.attached_locals.entry(stable_id.0.clone()).or_default() += 1;
        self.routes_by_authority
            .entry(stable_id.0.clone())
            .or_default();
        self.scope_index.entry(stable_id.0.clone()).or_default();
        self.reuse_index.entry(stable_id.0.clone()).or_default();
    }

    fn bind_local_service(
        &mut self,
        stable_id: &ClientStableId,
        service: Arc<dyn RouteAuthorityService>,
    ) {
        self.routes_by_authority
            .entry(stable_id.0.clone())
            .or_default();
        self.scope_index.entry(stable_id.0.clone()).or_default();
        self.reuse_index.entry(stable_id.0.clone()).or_default();
        self.authority_services.insert(stable_id.0.clone(), service);
    }

    fn unregister_local(&mut self, stable_id: &ClientStableId) {
        let key = stable_id.0.clone();
        match self.attached_locals.get_mut(&key) {
            Some(count) if *count > 1 => *count -= 1,
            Some(_) => {
                self.attached_locals.remove(&key);
                self.routes_by_authority.remove(&key);
                self.scope_index.remove(&key);
                self.reuse_index.remove(&key);
                self.authority_services.remove(&key);
                self.version_floors.remove(&key);
            }
            None => {}
        }
    }

    fn is_local(&self, stable_id: &ClientStableId) -> bool {
        self.attached_locals
            .get(&stable_id.0)
            .is_some_and(|count| *count > 0)
    }

    fn authority_service(
        &self,
        stable_id: &ClientStableId,
    ) -> Option<Arc<dyn RouteAuthorityService>> {
        self.authority_services.get(&stable_id.0).cloned()
    }

    fn insert_route_indexes(
        &mut self,
        authority: &ClientStableId,
        key: &ObjectKey,
        route: &ObjectRoute,
    ) {
        if let Some(scope) = route.namespace.clone() {
            self.scope_index
                .entry(authority.0.clone())
                .or_default()
                .entry(scope)
                .or_default()
                .insert(key.0.clone());
        }
        if let Ok(reuse) = route_reuse_identity(route) {
            self.reuse_index
                .entry(authority.0.clone())
                .or_default()
                .entry(reuse)
                .or_default()
                .insert(key.0.clone());
        }
    }

    fn remove_route_indexes(
        &mut self,
        authority: &ClientStableId,
        key: &ObjectKey,
        route: &ObjectRoute,
    ) {
        if let Some(scope) = route.namespace.as_ref() {
            remove_index_key(
                self.scope_index.get_mut(&authority.0),
                scope,
                key.0.as_str(),
            );
        }
        if let Ok(reuse) = route_reuse_identity(route) {
            remove_index_key(
                self.reuse_index.get_mut(&authority.0),
                &reuse,
                key.0.as_str(),
            );
        }
    }
}

fn remove_index_key<K: Ord>(
    index: Option<&mut BTreeMap<K, BTreeSet<String>>>,
    identity: &K,
    key: &str,
) {
    let Some(index) = index else {
        return;
    };
    let Some(keys) = index.get_mut(identity) else {
        return;
    };
    keys.remove(key);
    if keys.is_empty() {
        index.remove(identity);
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
    left.compatibility.is_compatible_with(&right.compatibility)
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

fn route_read_source(rank: usize, route_topk: usize) -> &'static str {
    if rank == 0 {
        "primary"
    } else if rank < route_topk {
        "mirror"
    } else {
        "fallback"
    }
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
        "{}|{}|{}|{}|{}|{}|{}|",
        route.key.0,
        route.version.0,
        route_state_rank(route.state),
        route.compatibility.store_api_version,
        route.compatibility.store_api_minor_version,
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
    let offset = replica
        .offset
        .map(|offset| offset.to_string())
        .unwrap_or_else(|| "legacy".to_string());
    let _ = write!(
        key,
        "{}|{}|{}|{}|{}|{}|{}|{};",
        replica.owner,
        replica.segment_name.0,
        offset,
        replica.segment_offset,
        replica.length,
        replica.checksum.unwrap_or_default(),
        replica_tier_rank(replica.tier),
        replica.priority,
    );
}

fn record_route_repair_metric(operation: &'static str) {
    route_metrics_sink().read().record_route_repair(operation);
}

fn record_cas_outcome(cas: &CasResult, next: Option<&ObjectRoute>, key: &ObjectKey) {
    if cas.applied {
        route_metrics_sink().read().record_route_cas("ok");
        match next {
            Some(route) => route_metrics_sink().read().record_route(route),
            None => route_metrics_sink().read().remove_route(key),
        }
        return;
    }
    route_metrics_sink().read().record_route_cas("conflict");
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

fn sampled_per_key_debug_log(parts: &[&str]) -> bool {
    is_per_key_debug_sample(stable_hash(parts), PER_KEY_DEBUG_SAMPLE_MODULUS)
}

fn is_per_key_debug_sample(hash: u64, modulus: u64) -> bool {
    matches!(hash.checked_rem(modulus), Some(0))
}
