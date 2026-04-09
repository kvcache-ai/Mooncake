use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};

use mooncake_store_core::{
    CasResult, ClientLease, ClientLifecycleState, ClientStableId, MetadataBackend, ObjectKey,
    ObjectRoute, Result, RouteDirectory, RouteVersion, StoreError,
};
use parking_lot::Mutex;
use tracing::warn;

use crate::control_plane::ControlPlaneClient;

const ROUTE_SCOPE_LABEL: &str = "route_scope";

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
        }
    }

    fn select_authorities(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Vec<ClientLease>> {
        let route_scope = observer.endpoints.labels.get(ROUTE_SCOPE_LABEL).cloned();
        let mut candidates = BTreeMap::<String, ClientLease>::new();
        for lease in self.metadata.list_live_clients()? {
            if lease.state != ClientLifecycleState::Active {
                continue;
            }
            if !compatibility_matches(observer, &lease) {
                continue;
            }
            if !route_capable(&lease) {
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

        let mut ranked = candidates
            .into_values()
            .map(|lease| {
                let weight = route_weight(&lease);
                let score = weighted_rendezvous_score(
                    &self.namespace,
                    &key.0,
                    &lease.runtime.stable_id.0,
                    weight,
                );
                (score, lease)
            })
            .collect::<Vec<_>>();
        ranked.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.runtime.stable_id.cmp(&right.1.runtime.stable_id))
        });
        Ok(ranked.into_iter().map(|(_, lease)| lease).collect())
    }

    fn read_local(
        &self,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        authority_get(&self.namespace, authority, key)
    }

    fn cas_local(
        &self,
        authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        authority_compare_and_swap(&self.namespace, authority, key, expected, next)
    }

    fn mirror_secondary(
        &self,
        secondary: &ClientLease,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) {
        let result = if secondary.runtime.stable_id == self.local_stable_id {
            authority_replace(&self.namespace, &secondary.runtime.stable_id, key, next)
        } else {
            self.control_plane.replace_route(
                secondary,
                &self.namespace,
                &secondary.runtime.stable_id,
                key,
                next,
            )
        };
        if let Err(error) = result {
            warn!(
                authority = %secondary.runtime,
                key = %key.0,
                error = %error,
                "secondary route mirror failed"
            );
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
        let authorities = self.select_authorities(observer, key)?;
        let mut last_error = None;
        for authority in authorities.iter().take(2) {
            let result = if authority.runtime.stable_id == self.local_stable_id {
                self.read_local(&authority.runtime.stable_id, key)
            } else {
                self.control_plane.get_route(
                    authority,
                    &self.namespace,
                    &authority.runtime.stable_id,
                    key,
                )
            };
            match result {
                Ok(route) => return Ok(route),
                Err(error) => last_error = Some((authority.runtime.clone(), error)),
            }
        }
        if let Some((runtime, error)) = last_error {
            warn!(runtime = %runtime, key = %key.0, error = %error, "authority route read failed; falling back to metadata");
        }
        self.metadata.get_object_route(key)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let authorities = self.select_authorities(observer, key)?;
        let Some(primary) = authorities.first() else {
            return self
                .metadata
                .compare_and_swap_object_route(key, expected, next);
        };

        let primary_result = if primary.runtime.stable_id == self.local_stable_id {
            self.cas_local(&primary.runtime.stable_id, key, expected, next)
        } else {
            self.control_plane.compare_and_swap_route(
                primary,
                &self.namespace,
                &primary.runtime.stable_id,
                key,
                expected,
                next,
            )
        };

        let cas = match primary_result {
            Ok(cas) => cas,
            Err(error) => {
                warn!(runtime = %primary.runtime, key = %key.0, error = %error, "authority route cas failed; falling back to metadata");
                return self
                    .metadata
                    .compare_and_swap_object_route(key, expected, next);
            }
        };

        if cas.applied {
            if let Some(secondary) = authorities.get(1) {
                if secondary.runtime.stable_id != primary.runtime.stable_id {
                    self.mirror_secondary(secondary, key, next);
                }
            }
        }
        Ok(cas)
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
