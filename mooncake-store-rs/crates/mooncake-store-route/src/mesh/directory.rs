use crate::RouteOwnerPageQuery;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReference, AllocationRouteReferenceState, CasResult,
    ClientLease, ClientRuntimeId, ClientStableId, MetadataBackend, NamespaceScope, ObjectKey,
    ObjectRoute, ReplicaRoute, Result, ReuseIdentity, RouteCasRequest, RouteDirectory,
    RouteOwnerScanStatus, RouteReplicaReference, RouteState, RouteVersion, StoreError,
    ROUTE_FILTERED_PRESENCE_CAPABILITY, ROUTE_OWNER_LIST_PAGINATION_CAPABILITY,
    ROUTE_OWNER_LIST_SNAPSHOT_CAPABILITY,
};
use tracing::{debug, trace, warn};

use crate::control::{RouteControlAuthorityClient, RouteControlTransport};
use crate::mesh::async_mirror::{
    maybe_mark_authority_suspect, mirror_secondary_batch, AsyncRouteMirrorWorker,
};
use crate::mesh::read_repair::{
    compute_backfill_requests, merge_authority_value, merge_route_listing, set_repair_observation,
    RouteAuthorityObservation, RouteAuthorityValue, RouteReadRepairState,
};
use crate::mesh::registry::{
    authority_compare_and_swap, authority_compare_and_swap_many, authority_contains_many,
    authority_get_allocation_route_reference_keys, authority_get_allocation_route_references,
    authority_get_delete_marker, authority_get_delete_markers,
    authority_get_filtered_presence_many, authority_get_presence_many, authority_get_records,
    authority_is_local, authority_list_reuse_candidates, authority_list_routes,
    authority_list_routes_by_replica_owner_page, authority_list_routes_in_scope,
    local_authority_service, register_local_authority, unregister_local_authority,
};
use crate::mesh::selection::{
    authority_candidates, merge_authority_candidates, owner_scan_authority_candidates,
    ranked_authorities, ranked_merged_authorities, ranked_top_authorities,
    retained_authority_candidates, select_authorities_for_requests, RouteAuthoritySelection,
};
use crate::metrics::{record_cas_outcome, record_delete_marker_event};
use crate::shim::{
    RouteAuthorityClient, RouteAuthorityPresence, RouteAuthorityRecord, RouteAuthorityService,
    RouteMembershipProvider,
};
#[cfg(test)]
use crate::table::DEFAULT_DELETE_MARKER_BUDGET_BYTES;
use crate::util::{route_read_source, sampled_per_key_debug_log};
use crate::DEFAULT_ROUTE_OWNER_PAGE_SIZE;

pub(crate) fn build_embedded_wrh_route_directory(
    route_topk: usize,
    delete_marker_budget_bytes: usize,
    metadata: Arc<dyn MetadataBackend>,
    lease: &ClientLease,
    control_transport: Arc<dyn RouteControlTransport>,
    membership: Arc<dyn RouteMembershipProvider>,
) -> Arc<dyn RouteDirectory> {
    Arc::new(EmbeddedWrhRouteDirectory::new_with_delete_marker_budget(
        route_topk,
        delete_marker_budget_bytes,
        metadata,
        lease,
        Arc::new(RouteControlAuthorityClient::new(control_transport)),
        membership,
    ))
}

const BASELINE_TTL: Duration = Duration::from_secs(300);

/// Merges allocation observations from a sharded route directory. A route is
/// present only on its ranked authorities, so `Exact` from those authorities
/// naturally coexists with `Absent` from the remaining authorities. The exact
/// key is then verified by `confirm_replica_reference` before lifecycle state
/// is finalized.
fn merge_allocation_route_reference(
    previous: Option<&AllocationRouteReference>,
    observed: AllocationRouteReference,
) -> AllocationRouteReference {
    let Some(previous) = previous else {
        return observed;
    };
    if previous.state == AllocationRouteReferenceState::Conflict
        || observed.state == AllocationRouteReferenceState::Conflict
    {
        return AllocationRouteReference::conflict();
    }
    match (previous.state, observed.state) {
        (AllocationRouteReferenceState::Absent, AllocationRouteReferenceState::Absent) => {
            AllocationRouteReference::absent()
        }
        (AllocationRouteReferenceState::Absent, AllocationRouteReferenceState::Exact)
            if observed.route_key.is_some() =>
        {
            observed
        }
        (AllocationRouteReferenceState::Exact, AllocationRouteReferenceState::Absent)
            if previous.route_key.is_some() =>
        {
            previous.clone()
        }
        (AllocationRouteReferenceState::Exact, AllocationRouteReferenceState::Exact)
            if previous.route_key.is_some() && previous.route_key == observed.route_key =>
        {
            previous.clone()
        }
        _ => AllocationRouteReference::conflict(),
    }
}

/// Resolves one authority record with the cluster-wide version rule: a live
/// route wins only when its version is strictly newer than the delete marker.
fn value_from_authority_record(record: RouteAuthorityRecord) -> Option<RouteAuthorityValue> {
    if let Some(route) = record.route {
        if record
            .delete_version
            .is_none_or(|version| route.version > version)
        {
            return Some(RouteAuthorityValue::Route(Box::new(route)));
        }
        record_delete_marker_event("route_marker_conflict", 1);
    }
    record.delete_version.map(RouteAuthorityValue::DeleteMarker)
}

fn value_from_presence(presence: RouteAuthorityPresence) -> Option<ContainsVersion> {
    match (presence.live_version, presence.delete_version) {
        (Some(live), Some(delete)) => {
            if live > delete {
                Some(ContainsVersion::Live(live))
            } else {
                record_delete_marker_event("route_marker_conflict", 1);
                Some(ContainsVersion::Delete(delete))
            }
        }
        (Some(live), None) => Some(ContainsVersion::Live(live)),
        (None, Some(delete)) => Some(ContainsVersion::Delete(delete)),
        (None, None) => None,
    }
}

fn merge_contains_version(resolved: &mut Option<ContainsVersion>, candidate: ContainsVersion) {
    let should_replace = resolved.is_none_or(|current| {
        candidate.version() > current.version()
            || (candidate.version() == current.version()
                && matches!(candidate, ContainsVersion::Delete(_))
                && matches!(current, ContainsVersion::Live(_)))
    });
    if should_replace {
        *resolved = Some(candidate);
    }
}

fn observer_start_rank(observer: &ClientLease, topk: usize) -> usize {
    if topk <= 1 {
        return 0;
    }
    let id = &observer.runtime.stable_id.0;
    let hash = id
        .bytes()
        .fold(0u64, |h, b| h.wrapping_mul(31).wrapping_add(u64::from(b)));
    (hash as usize) % topk
}

struct BaselineState {
    member_ids: HashSet<String>,
    candidates: Vec<ClientLease>,
    created_at: Instant,
}

struct EmbeddedWrhRouteDirectory {
    route_topk: usize,
    namespace: String,
    local_stable_id: ClientStableId,
    metadata: Arc<dyn MetadataBackend>,
    authority_client: Arc<dyn RouteAuthorityClient>,
    membership: Arc<dyn RouteMembershipProvider>,
    async_mirror: AsyncRouteMirrorWorker,
    baseline: Mutex<BaselineState>,
}

struct RouteReadOptions<'a> {
    repairs: Option<&'a mut [RouteReadRepairState]>,
    read_all: bool,
    source_ranks: Option<&'a [usize]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PresenceObservation {
    Unobserved,
    Observed,
    Unsupported,
}

#[derive(Clone, Copy, Debug)]
enum ContainsVersion {
    Live(RouteVersion),
    Delete(RouteVersion),
}

impl ContainsVersion {
    fn version(self) -> RouteVersion {
        match self {
            Self::Live(version) | Self::Delete(version) => version,
        }
    }
}

fn contains_version_from_authority_value(value: RouteAuthorityValue) -> ContainsVersion {
    match value {
        RouteAuthorityValue::Route(route) => ContainsVersion::Live(route.version),
        RouteAuthorityValue::DeleteMarker(version) => ContainsVersion::Delete(version),
    }
}

fn merge_contains_authority_baselines(
    current: &[Vec<ClientLease>],
    baseline: Option<&[Vec<ClientLease>]>,
) -> Vec<Vec<ClientLease>> {
    let mut merged = current.to_vec();
    if let Some(baseline) = baseline {
        for (authorities, baseline) in merged.iter_mut().zip(baseline) {
            for authority in baseline {
                if !authorities
                    .iter()
                    .any(|existing| existing.runtime.stable_id == authority.runtime.stable_id)
                {
                    authorities.push(authority.clone());
                }
            }
        }
    }
    merged
}

struct RouteContainsProbe<'a> {
    should_probe: Option<&'a [bool]>,
    retry_on_error: Option<&'a mut [bool]>,
}

struct RouteCasAttempt {
    resolved: Vec<Option<Result<CasResult>>>,
    resolved_authorities: Vec<Option<ClientLease>>,
    last_errors: Vec<Option<StoreError>>,
}

struct OwnerRoutePageScan {
    authority: ClientLease,
    routes: VecDeque<ObjectRoute>,
    next_cursor: Option<String>,
    snapshot_epoch: Option<u64>,
    seen_cursors: HashSet<String>,
}

fn validate_owner_route_page(
    cursor: Option<&str>,
    page: &crate::RouteOwnerPage,
    seen_cursors: &mut HashSet<String>,
) -> Result<()> {
    if page.routes.len() > DEFAULT_ROUTE_OWNER_PAGE_SIZE {
        return Err(StoreError::Transport(format!(
            "route owner page exceeds the {DEFAULT_ROUTE_OWNER_PAGE_SIZE}-route limit"
        )));
    }
    let mut previous = cursor;
    for route in &page.routes {
        if previous.is_some_and(|previous| route.key.0.as_str() <= previous) {
            return Err(StoreError::Transport(
                "route owner page keys must advance strictly after the cursor".to_string(),
            ));
        }
        previous = Some(route.key.0.as_str());
    }
    if let Some(next_cursor) = page.next_cursor.as_ref() {
        if page.routes.last().map(|route| route.key.0.as_str()) != Some(next_cursor.as_str()) {
            return Err(StoreError::Transport(
                "route owner page cursor must equal its final route key".to_string(),
            ));
        }
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err(StoreError::Transport(
                "route owner page returned a previously seen cursor".to_string(),
            ));
        }
    }
    Ok(())
}

impl EmbeddedWrhRouteDirectory {
    #[cfg(test)]
    fn new(
        route_topk: usize,
        metadata: Arc<dyn MetadataBackend>,
        lease: &ClientLease,
        authority_client: Arc<dyn RouteAuthorityClient>,
        membership: Arc<dyn RouteMembershipProvider>,
    ) -> Self {
        Self::new_with_delete_marker_budget(
            route_topk,
            DEFAULT_DELETE_MARKER_BUDGET_BYTES,
            metadata,
            lease,
            authority_client,
            membership,
        )
    }

    fn new_with_delete_marker_budget(
        route_topk: usize,
        delete_marker_budget_bytes: usize,
        metadata: Arc<dyn MetadataBackend>,
        lease: &ClientLease,
        authority_client: Arc<dyn RouteAuthorityClient>,
        membership: Arc<dyn RouteMembershipProvider>,
    ) -> Self {
        let namespace = metadata.route_namespace();
        register_local_authority(
            &namespace,
            &lease.runtime.stable_id,
            delete_marker_budget_bytes,
        );
        let async_mirror = AsyncRouteMirrorWorker::spawn(
            namespace.clone(),
            lease.runtime.stable_id.clone(),
            authority_client.clone(),
            membership.clone(),
        );
        let baseline_candidates =
            authority_candidates(membership.as_ref(), lease, false).unwrap_or_default();
        let baseline_member_ids = baseline_candidates
            .iter()
            .map(|c| c.runtime.stable_id.0.clone())
            .collect();
        Self {
            route_topk,
            namespace,
            local_stable_id: lease.runtime.stable_id.clone(),
            metadata,
            authority_client,
            membership,
            async_mirror,
            baseline: Mutex::new(BaselineState {
                member_ids: baseline_member_ids,
                candidates: baseline_candidates,
                created_at: Instant::now(),
            }),
        }
    }

    fn maybe_mark_authority_suspect(
        &self,
        authority: &ClientLease,
        error: &StoreError,
        context: &'static str,
    ) {
        maybe_mark_authority_suspect(
            &self.namespace,
            self.membership.as_ref(),
            authority,
            error,
            context,
        );
    }

    fn authority_candidates(&self, observer: &ClientLease) -> Result<Vec<ClientLease>> {
        authority_candidates(self.membership.as_ref(), observer, false)
    }

    fn owner_scan_authority_candidates(&self, observer: &ClientLease) -> Result<Vec<ClientLease>> {
        owner_scan_authority_candidates(self.membership.as_ref(), observer)
    }

    fn authority_candidates_force_refresh(
        &self,
        observer: &ClientLease,
    ) -> Result<Vec<ClientLease>> {
        authority_candidates(self.membership.as_ref(), observer, true)
    }

    fn delete_marker_fanout_authorities(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        current_candidates: &[ClientLease],
    ) -> Vec<ClientLease> {
        let limit = self.route_topk.saturating_mul(2);
        let retained =
            retained_authority_candidates(self.membership.as_ref(), observer).unwrap_or_default();
        ranked_merged_authorities(&self.namespace, current_candidates, &retained, key, limit)
    }

    fn eviction_confirmation_authorities(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        current_candidates: &[ClientLease],
    ) -> Result<Vec<ClientLease>> {
        let retained_candidates =
            retained_authority_candidates(self.membership.as_ref(), observer)?;
        Ok(ranked_merged_authorities(
            &self.namespace,
            current_candidates,
            &retained_candidates,
            key,
            self.route_topk,
        ))
    }

    fn allocation_confirmation_authorities(
        &self,
        observer: &ClientLease,
        current_candidates: &[ClientLease],
    ) -> Result<Vec<ClientLease>> {
        let retained_candidates =
            retained_authority_candidates(self.membership.as_ref(), observer)?;
        Ok(merge_authority_candidates(
            current_candidates,
            &retained_candidates,
        ))
    }

    fn local_authority_service(
        &self,
        authority: &ClientStableId,
    ) -> Option<Arc<dyn RouteAuthorityService>> {
        local_authority_service(&self.namespace, authority)
    }

    fn read_local_batch(
        &self,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<RouteAuthorityRecord>> {
        authority_get_records(&self.namespace, authority, keys)
    }

    fn read_authority_batch(
        &self,
        authority: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<RouteAuthorityRecord>>> {
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return Ok(service.batch_get_route_records(
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

    fn presence_authority_batch(
        &self,
        authority: &ClientLease,
        keys: &[ObjectKey],
        filtered: bool,
    ) -> Result<Vec<Result<RouteAuthorityPresence>>> {
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return Ok(if filtered {
                    service.batch_get_filtered_route_presence(
                        &self.namespace,
                        &authority.runtime.stable_id,
                        keys,
                    )
                } else {
                    service.batch_get_route_presence(
                        &self.namespace,
                        &authority.runtime.stable_id,
                        keys,
                    )
                });
            }
            let results = if filtered {
                authority_get_filtered_presence_many(
                    &self.namespace,
                    &authority.runtime.stable_id,
                    keys,
                )
            } else {
                authority_get_presence_many(&self.namespace, &authority.runtime.stable_id, keys)
            };
            return results.map(|results| results.into_iter().map(Ok).collect());
        }
        if filtered {
            self.authority_client.batch_get_filtered_route_presence(
                authority,
                &self.namespace,
                &authority.runtime.stable_id,
                keys,
            )
        } else {
            self.authority_client.batch_get_route_presence(
                authority,
                &self.namespace,
                &authority.runtime.stable_id,
                keys,
            )
        }
    }

    fn get_allocation_route_references_from_authority(
        &self,
        authority: &ClientLease,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReferenceState>> {
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return service.get_allocation_route_references(
                    &self.namespace,
                    &authority.runtime.stable_id,
                    identities,
                );
            }
            return authority_get_allocation_route_references(
                &self.namespace,
                &authority.runtime.stable_id,
                identities,
            );
        }
        self.authority_client.get_allocation_route_references(
            authority,
            &self.namespace,
            &authority.runtime.stable_id,
            identities,
        )
    }

    fn get_allocation_route_reference_keys_from_authority(
        &self,
        authority: &ClientLease,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReference>> {
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return service.get_allocation_route_reference_keys(
                    &self.namespace,
                    &authority.runtime.stable_id,
                    identities,
                );
            }
            return authority_get_allocation_route_reference_keys(
                &self.namespace,
                &authority.runtime.stable_id,
                identities,
            );
        }
        self.authority_client.get_allocation_route_reference_keys(
            authority,
            &self.namespace,
            &authority.runtime.stable_id,
            identities,
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

    fn authority_is_local(&self, authority: &ClientLease) -> bool {
        authority_is_local(&self.namespace, &authority.runtime.stable_id)
    }

    fn list_routes_by_replica_owner_page_from_authority(
        &self,
        authority: &ClientLease,
        owner: &ClientRuntimeId,
        cursor: Option<&str>,
        snapshot_epoch: Option<u64>,
        require_snapshot: bool,
    ) -> Result<crate::RouteOwnerPage> {
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return service.list_routes_by_replica_owner_page(
                    &self.namespace,
                    &authority.runtime.stable_id,
                    owner,
                    cursor,
                    snapshot_epoch,
                    DEFAULT_ROUTE_OWNER_PAGE_SIZE,
                );
            }
            return authority_list_routes_by_replica_owner_page(
                &self.namespace,
                &authority.runtime.stable_id,
                owner,
                cursor,
                snapshot_epoch,
                DEFAULT_ROUTE_OWNER_PAGE_SIZE,
            );
        }
        let capability = if require_snapshot {
            ROUTE_OWNER_LIST_SNAPSHOT_CAPABILITY
        } else {
            ROUTE_OWNER_LIST_PAGINATION_CAPABILITY
        };
        if !authority.compatibility.supports(capability) {
            return Err(StoreError::Unsupported(format!(
                "route authority {} does not support required owner-route pagination capability {capability}",
                authority.runtime
            )));
        }
        let query = RouteOwnerPageQuery {
            owner: owner.clone(),
            cursor: cursor.map(ToOwned::to_owned),
            snapshot_epoch,
            limit: DEFAULT_ROUTE_OWNER_PAGE_SIZE,
        };
        self.authority_client.list_routes_by_replica_owner_page(
            authority,
            &self.namespace,
            &authority.runtime.stable_id,
            &query,
        )
    }

    fn visit_routes_by_replica_owner_pages(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
        visitor: &mut dyn FnMut(ObjectRoute) -> Result<()>,
        require_snapshot: bool,
    ) -> Result<RouteOwnerScanStatus> {
        let mut scans = Vec::new();
        let authorities = self.owner_scan_authority_candidates(observer)?;
        if authorities.is_empty() {
            return Err(StoreError::InvalidState(format!(
                "owner-route scan for {owner} has no route authorities"
            )));
        }
        let mut last_error = None;
        for authority in authorities {
            match self.list_routes_by_replica_owner_page_from_authority(
                &authority,
                owner,
                None,
                None,
                require_snapshot,
            ) {
                Ok(page) => {
                    self.membership.clear_suspect(&authority.runtime);
                    let mut seen_cursors = HashSet::new();
                    validate_owner_route_page(None, &page, &mut seen_cursors)?;
                    if require_snapshot && page.snapshot_epoch.is_none() {
                        return Err(StoreError::Unsupported(format!(
                            "route authority {} returned owner-route pages without a snapshot epoch",
                            authority.runtime
                        )));
                    }
                    scans.push(OwnerRoutePageScan {
                        authority,
                        routes: VecDeque::from(page.routes),
                        next_cursor: page.next_cursor,
                        snapshot_epoch: page.snapshot_epoch,
                        seen_cursors,
                    });
                }
                Err(error) => {
                    if !matches!(
                        error,
                        StoreError::Unsupported(_) | StoreError::StaleEpoch(_)
                    ) {
                        self.maybe_mark_authority_suspect(
                            &authority,
                            &error,
                            "route_list_by_replica_owner_failed",
                        );
                    }
                    warn!(
                        authority = %authority.runtime,
                        owner = %owner,
                        error = %error,
                        "route-owner scan is incomplete after authority page-list failure"
                    );
                    if require_snapshot {
                        return Err(error);
                    }
                    last_error = Some(error);
                }
            }
        }
        if scans.is_empty() {
            return Err(last_error.unwrap_or_else(|| {
                StoreError::InvalidState(format!(
                    "owner-route scan for {owner} has no reachable route authorities"
                ))
            }));
        }

        loop {
            let mut next_scans = Vec::with_capacity(scans.len());
            for mut scan in scans {
                if !scan.routes.is_empty() || scan.next_cursor.is_none() {
                    next_scans.push(scan);
                    continue;
                }
                let cursor = scan
                    .next_cursor
                    .take()
                    .expect("checked that the owner route page cursor exists");
                let authority = scan.authority.clone();
                let expected_epoch = require_snapshot.then(|| {
                    scan.snapshot_epoch
                        .expect("strict owner-route scan must establish a snapshot epoch")
                });
                match self.list_routes_by_replica_owner_page_from_authority(
                    &authority,
                    owner,
                    Some(&cursor),
                    expected_epoch,
                    require_snapshot,
                ) {
                    Ok(page) => {
                        self.membership.clear_suspect(&authority.runtime);
                        if require_snapshot && page.snapshot_epoch != expected_epoch {
                            return Err(StoreError::StaleEpoch(format!(
                                "route authority {} changed owner-route snapshot while scanning {owner}",
                                authority.runtime
                            )));
                        }
                        if page.routes.is_empty() && page.next_cursor.is_some() {
                            let error = StoreError::Transport(format!(
                                "route owner page returned an empty non-terminal page for authority {}",
                                authority.runtime
                            ));
                            self.maybe_mark_authority_suspect(
                                &authority,
                                &error,
                                "route_list_by_replica_owner_page_invalid",
                            );
                            warn!(
                                authority = %authority.runtime,
                                owner = %owner,
                                error = %error,
                                "route-owner page listing stopped after invalid page"
                            );
                            return Err(error);
                        }
                        validate_owner_route_page(Some(&cursor), &page, &mut scan.seen_cursors)?;
                        scan.routes = VecDeque::from(page.routes);
                        scan.next_cursor = page.next_cursor;
                        next_scans.push(scan);
                    }
                    Err(error) => {
                        if !matches!(
                            error,
                            StoreError::Unsupported(_) | StoreError::StaleEpoch(_)
                        ) {
                            self.maybe_mark_authority_suspect(
                                &authority,
                                &error,
                                "route_list_by_replica_owner_page_failed",
                            );
                        }
                        warn!(
                            authority = %authority.runtime,
                            owner = %owner,
                            error = %error,
                            "route-owner scan is incomplete after continuation page failure"
                        );
                        if require_snapshot {
                            return Err(error);
                        }
                        last_error = Some(error);
                    }
                }
            }
            scans = next_scans;

            let Some(next_key) = scans
                .iter()
                .filter_map(|scan| scan.routes.front().map(|route| route.key.0.as_str()))
                .min()
                .map(ToOwned::to_owned)
            else {
                if require_snapshot {
                    self.validate_owner_route_scan_snapshot(observer, owner, &scans)?;
                }
                return Ok(if last_error.is_some() {
                    RouteOwnerScanStatus::Incomplete
                } else {
                    RouteOwnerScanStatus::Complete
                });
            };
            let mut merged = BTreeMap::new();
            for scan in &mut scans {
                if let Some(route) = scan.routes.pop_front_if(|route| route.key.0 == next_key) {
                    merge_route_listing(
                        &mut merged,
                        route,
                        &scan.authority.runtime.to_string(),
                        &self.namespace,
                    );
                }
            }
            visitor(
                merged
                    .into_values()
                    .next()
                    .expect("a route matched the selected page key"),
            )?;
        }
    }

    fn validate_owner_route_scan_snapshot(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
        scans: &[OwnerRoutePageScan],
    ) -> Result<()> {
        // Re-read one bounded page per authority. This catches a mutation after a
        // terminal first/last page, where no continuation request would exist to
        // validate the epoch.
        for scan in scans {
            let snapshot_epoch = scan
                .snapshot_epoch
                .expect("strict owner-route scan must establish a snapshot epoch");
            let page = self.list_routes_by_replica_owner_page_from_authority(
                &scan.authority,
                owner,
                None,
                Some(snapshot_epoch),
                true,
            )?;
            if page.snapshot_epoch != Some(snapshot_epoch) {
                return Err(StoreError::StaleEpoch(format!(
                    "route authority {} changed owner-route snapshot while validating {owner}",
                    scan.authority.runtime
                )));
            }
        }

        let initial_authorities = scans
            .iter()
            .map(|scan| {
                (
                    scan.authority.runtime.stable_id.clone(),
                    scan.authority.runtime.epoch,
                )
            })
            .collect::<BTreeSet<_>>();
        let refreshed_authorities = self
            .authority_candidates_force_refresh(observer)?
            .into_iter()
            .map(|authority| (authority.runtime.stable_id, authority.runtime.epoch))
            .collect::<BTreeSet<_>>();
        if initial_authorities != refreshed_authorities {
            return Err(StoreError::StaleEpoch(format!(
                "route authority membership changed while scanning owner routes for {owner}"
            )));
        }
        Ok(())
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

    fn contains_local_batch(
        &self,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Result<Vec<bool>> {
        authority_contains_many(&self.namespace, authority, keys)
    }

    fn contains_authority_batch(
        &self,
        authority: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<Vec<Result<bool>>> {
        if self.authority_is_local(authority) {
            if let Some(service) = self.local_authority_service(&authority.runtime.stable_id) {
                return Ok(service.batch_contains_routes(
                    &self.namespace,
                    &authority.runtime.stable_id,
                    keys,
                ));
            }
            return self
                .contains_local_batch(&authority.runtime.stable_id, keys)
                .map(|results| results.into_iter().map(Ok).collect());
        }
        self.authority_client.batch_contains_routes(
            authority,
            &self.namespace,
            &authority.runtime.stable_id,
            keys,
        )
    }

    fn contains_ranked_authorities(
        &self,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
        rank: usize,
        resolved: &mut [bool],
        mut probe: RouteContainsProbe<'_>,
        failure_message: &'static str,
    ) -> Result<bool> {
        let mut groups = BTreeMap::<String, (ClientLease, Vec<usize>)>::new();
        for (index, authorities) in ranked_authorities.iter().enumerate() {
            if resolved[index] {
                continue;
            }
            if probe.should_probe.is_some_and(|probe| !probe[index]) {
                continue;
            }
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

        for (_, (authority, indices)) in groups {
            let batch_keys = indices
                .iter()
                .map(|index| keys[*index].clone())
                .collect::<Vec<_>>();
            match self.contains_authority_batch(&authority, &batch_keys) {
                Ok(results) => {
                    if results.len() != batch_keys.len() {
                        if let Some(retry) = probe.retry_on_error.as_deref_mut() {
                            for index in &indices {
                                retry[*index] = true;
                            }
                        }
                        warn!(
                            runtime = %authority.runtime,
                            expected = batch_keys.len(),
                            actual = results.len(),
                            "{failure_message}"
                        );
                        continue;
                    }
                    for ((index, key), result) in indices.into_iter().zip(batch_keys).zip(results) {
                        match result {
                            Ok(true) => {
                                trace!(
                                    namespace = %self.namespace,
                                    key = %key.0,
                                    authority = %authority.runtime,
                                    rank,
                                    source = route_read_source(rank, self.route_topk),
                                    "route authority contains object route"
                                );
                                resolved[index] = true;
                            }
                            Ok(false) => {}
                            Err(error) => {
                                if let Some(retry) = probe.retry_on_error.as_deref_mut() {
                                    retry[index] = true;
                                }
                                self.maybe_mark_authority_suspect(
                                    &authority,
                                    &error,
                                    "route_batch_contains_failed",
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
                    if let Some(retry) = probe.retry_on_error.as_deref_mut() {
                        for index in &indices {
                            retry[*index] = true;
                        }
                    }
                    self.maybe_mark_authority_suspect(
                        &authority,
                        &error,
                        "route_batch_contains_transport_failed",
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

    fn read_ranked_authorities(
        &self,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
        rank: usize,
        resolved: &mut [Option<RouteAuthorityValue>],
        mut options: RouteReadOptions<'_>,
        failure_message: &'static str,
    ) -> Result<bool> {
        let mut groups = BTreeMap::<String, (ClientLease, Vec<usize>)>::new();
        for (index, authorities) in ranked_authorities.iter().enumerate() {
            let Some(authority) = authorities.get(rank).cloned() else {
                continue;
            };
            if resolved[index].is_some()
                && !options.read_all
                && (options.repairs.is_none() || !self.authority_is_local(&authority))
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

        for (_, (authority, indices)) in groups {
            let batch_keys = indices
                .iter()
                .map(|index| keys[*index].clone())
                .collect::<Vec<_>>();
            match self.read_authority_batch(&authority, &batch_keys) {
                Ok(results) => {
                    for ((index, key), result) in indices.into_iter().zip(batch_keys).zip(results) {
                        let source_rank = options
                            .source_ranks
                            .and_then(|ranks| ranks.get(index))
                            .copied()
                            .unwrap_or(rank);
                        match result.map(value_from_authority_record) {
                            Ok(Some(route)) => {
                                trace!(
                                    namespace = %self.namespace,
                                    key = %key.0,
                                    authority = %authority.runtime,
                                    rank = source_rank,
                                    source = route_read_source(source_rank, self.route_topk),
                                    route_version = route.version().0,
                                    replica_count = match &route { RouteAuthorityValue::Route(route) => route.replicas.len(), RouteAuthorityValue::DeleteMarker(_) => 0 },
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
                                        source = route_read_source(source_rank, self.route_topk),
                                        route_version = route.version().0,
                                        replica_count = match &route { RouteAuthorityValue::Route(route) => route.replicas.len(), RouteAuthorityValue::DeleteMarker(_) => 0 },
                                        "sampled route authority object route"
                                    );
                                }
                                if let Some(states) = options.repairs.as_mut() {
                                    set_repair_observation(
                                        &mut states[index],
                                        rank,
                                        RouteAuthorityObservation::Present(route.clone()),
                                    );
                                }
                                merge_authority_value(
                                    &mut resolved[index],
                                    route,
                                    &authority.runtime.to_string(),
                                    &key,
                                    &self.namespace,
                                );
                            }
                            Ok(None) => {
                                if let Some(states) = options.repairs.as_mut() {
                                    set_repair_observation(
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
                        merge_route_listing(
                            &mut routes,
                            route,
                            &authority.runtime.to_string(),
                            &self.namespace,
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

    fn backfill_missing_authorities(
        &self,
        keys: &[ObjectKey],
        selections: &[RouteAuthoritySelection],
        resolved: &[Option<RouteAuthorityValue>],
        repairs: &[RouteReadRepairState],
    ) {
        let backfills = compute_backfill_requests(keys, selections, resolved, repairs);
        for (_, (authority, requests)) in backfills {
            let (markers, requests): (Vec<_>, Vec<_>) = requests
                .into_iter()
                .partition(|request| request.next.is_none());
            if !markers.is_empty() {
                mirror_secondary_batch(
                    &self.namespace,
                    self.authority_client.as_ref(),
                    self.membership.as_ref(),
                    &authority,
                    &markers,
                );
            }
            if requests.is_empty() {
                continue;
            }
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

    fn contains_object_routes_with_readable_filter(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
        baseline_ranked_authorities: Option<&[Vec<ClientLease>]>,
    ) -> Result<Vec<bool>> {
        let validation_ranked =
            merge_contains_authority_baselines(ranked_authorities, baseline_ranked_authorities);
        if validation_ranked.iter().flatten().all(|authority| {
            authority
                .compatibility
                .supports(ROUTE_FILTERED_PRESENCE_CAPABILITY)
        }) {
            if let Some(resolved) =
                self.validate_contains_object_routes_presence(keys, &validation_ranked, true)?
            {
                return Ok(resolved);
            }
        }

        let start_rank = observer_start_rank(observer, self.route_topk);
        let mut resolved = vec![false; keys.len()];
        let mut probe_failed = vec![false; keys.len()];
        for step in 0..self.route_topk {
            if step > 0 && resolved.iter().all(|present| *present) {
                break;
            }
            let rank = (start_rank + step) % self.route_topk;
            self.contains_ranked_authorities(
                keys,
                ranked_authorities,
                rank,
                &mut resolved,
                RouteContainsProbe {
                    should_probe: None,
                    retry_on_error: Some(probe_failed.as_mut_slice()),
                },
                if step == 0 {
                    "authority bounded route contains failed; trying mirrored authorities"
                } else {
                    "mirrored bounded route contains failed; trying other authorities"
                },
            )?;
        }
        if let Some(baseline_ranked) = baseline_ranked_authorities {
            for step in 0..self.route_topk {
                if step > 0 && resolved.iter().all(|present| *present) {
                    break;
                }
                let rank = (start_rank + step) % self.route_topk;
                self.contains_ranked_authorities(
                    keys,
                    baseline_ranked,
                    rank,
                    &mut resolved,
                    RouteContainsProbe {
                        should_probe: None,
                        retry_on_error: Some(probe_failed.as_mut_slice()),
                    },
                    "baseline authority fallback route contains failed",
                )?;
            }
        }
        if let Some((index, _)) = probe_failed.iter().enumerate().find(|(_, failed)| **failed) {
            return Err(StoreError::Transport(format!(
                "contains readable-filter probe could not confirm route state for key {}",
                keys[index].0
            )));
        }

        let positives = resolved
            .iter()
            .enumerate()
            .filter(|(_, present)| **present)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let positive_keys = positives
            .iter()
            .map(|&index| keys[index].clone())
            .collect::<Vec<_>>();
        let positive_ranked = positives
            .iter()
            .map(|&index| validation_ranked[index].clone())
            .collect::<Vec<_>>();
        let validated = self.validate_contains_object_routes(&positive_keys, &positive_ranked)?;
        for (index, present) in positives.into_iter().zip(validated) {
            resolved[index] = present;
        }
        Ok(resolved)
    }

    fn validate_contains_object_routes(
        &self,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
    ) -> Result<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(resolved) =
            self.validate_contains_object_routes_presence(keys, ranked_authorities, false)?
        {
            return Ok(resolved);
        }
        self.validate_contains_object_routes_full(keys, ranked_authorities)
    }

    fn validate_contains_object_routes_presence(
        &self,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
        filtered: bool,
    ) -> Result<Option<Vec<bool>>> {
        let rank_count = ranked_authorities.iter().map(Vec::len).max().unwrap_or(0);
        let mut resolved = vec![None; keys.len()];
        let mut observations = vec![vec![PresenceObservation::Unobserved; rank_count]; keys.len()];
        let mut groups =
            BTreeMap::<String, (ClientLease, Vec<ObjectKey>, Vec<(usize, usize)>)>::new();
        for (index, authorities) in ranked_authorities.iter().enumerate() {
            for (rank, authority) in authorities.iter().enumerate() {
                let entry = groups
                    .entry(authority.runtime.stable_id.0.clone())
                    .or_insert_with(|| (authority.clone(), Vec::new(), Vec::new()));
                entry.1.push(keys[index].clone());
                entry.2.push((index, rank));
            }
        }
        for (authority, batch_keys, assignments) in groups.into_values() {
            let outcome = self.presence_authority_batch(&authority, &batch_keys, filtered);
            match outcome {
                Ok(results) => {
                    if results.len() != batch_keys.len() {
                        warn!(
                            runtime = %authority.runtime,
                            expected = batch_keys.len(),
                            actual = results.len(),
                            "contains presence reply length mismatch"
                        );
                        continue;
                    }
                    for ((index, rank), result) in assignments.into_iter().zip(results) {
                        match result {
                            Ok(presence) => {
                                observations[index][rank] = PresenceObservation::Observed;
                                if let Some(value) = value_from_presence(presence) {
                                    merge_contains_version(&mut resolved[index], value);
                                }
                            }
                            Err(StoreError::Unsupported(error)) => {
                                observations[index][rank] = PresenceObservation::Unsupported;
                                debug!(
                                    runtime = %authority.runtime,
                                    error,
                                    "route authority does not support presence validation"
                                );
                            }
                            Err(error) => {
                                self.maybe_mark_authority_suspect(
                                    &authority,
                                    &error,
                                    "route_batch_presence_failed",
                                );
                                warn!(
                                    runtime = %authority.runtime,
                                    key = %keys[index].0,
                                    error = %error,
                                    "contains presence validation failed"
                                );
                            }
                        }
                    }
                }
                Err(StoreError::Unsupported(error)) => {
                    for (index, rank) in assignments {
                        observations[index][rank] = PresenceObservation::Unsupported;
                    }
                    debug!(
                        runtime = %authority.runtime,
                        error,
                        "route authority does not support presence validation"
                    );
                }
                Err(error) => {
                    self.maybe_mark_authority_suspect(
                        &authority,
                        &error,
                        "route_batch_presence_transport_failed",
                    );
                    warn!(
                        runtime = %authority.runtime,
                        error = %error,
                        items = batch_keys.len(),
                        "contains presence validation batch failed"
                    );
                }
            }
        }

        let mut has_unsupported = false;
        let mut unobserved_index = None;
        for (index, authorities) in ranked_authorities.iter().enumerate() {
            if authorities.is_empty() {
                unobserved_index.get_or_insert(index);
            }
            for observation in &observations[index][..authorities.len()] {
                match observation {
                    PresenceObservation::Unobserved => {
                        unobserved_index.get_or_insert(index);
                    }
                    PresenceObservation::Unsupported => has_unsupported = true,
                    PresenceObservation::Observed => {}
                }
            }
        }
        if has_unsupported && (filtered || unobserved_index.is_some()) {
            // Unconfirmed filtered presence or mixed failures require the complete fallback.
            return Ok(None);
        }
        if let Some(index) = unobserved_index {
            return Err(StoreError::Transport(format!(
                "contains presence validation could not confirm route state for key {}",
                keys[index].0
            )));
        }

        if has_unsupported {
            let mut fallback_keys = Vec::new();
            let mut fallback_authorities = Vec::new();
            let mut fallback_targets = Vec::new();
            for (index, authorities) in ranked_authorities.iter().enumerate() {
                for (rank, authority) in authorities.iter().enumerate() {
                    if observations[index][rank] == PresenceObservation::Unsupported {
                        fallback_keys.push(keys[index].clone());
                        fallback_authorities.push(vec![authority.clone()]);
                        fallback_targets.push((index, rank));
                    }
                }
            }

            let fallback_source_ranks = fallback_targets
                .iter()
                .map(|(_, rank)| *rank)
                .collect::<Vec<_>>();
            let mut fallback_resolved = vec![None; fallback_keys.len()];
            let mut fallback_observations =
                vec![vec![RouteAuthorityObservation::Unobserved]; fallback_keys.len()];
            self.read_ranked_authorities(
                &fallback_keys,
                &fallback_authorities,
                0,
                &mut fallback_resolved,
                RouteReadOptions {
                    repairs: Some(fallback_observations.as_mut_slice()),
                    read_all: true,
                    source_ranks: Some(&fallback_source_ranks),
                },
                "contains selective legacy fallback read failed",
            )?;
            for (work_index, (key_index, _)) in fallback_targets.into_iter().enumerate() {
                if matches!(
                    fallback_observations[work_index][0],
                    RouteAuthorityObservation::Unobserved
                ) {
                    return Err(StoreError::Transport(format!(
                        "contains legacy fallback could not confirm route state for key {}",
                        keys[key_index].0
                    )));
                }
                if let Some(value) = fallback_resolved[work_index].take() {
                    merge_contains_version(
                        &mut resolved[key_index],
                        contains_version_from_authority_value(value),
                    );
                }
            }
        }
        Ok(Some(
            resolved
                .into_iter()
                .map(|value| matches!(value, Some(ContainsVersion::Live(_))))
                .collect(),
        ))
    }

    fn validate_contains_object_routes_full(
        &self,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
    ) -> Result<Vec<bool>> {
        let rank_count = ranked_authorities.iter().map(Vec::len).max().unwrap_or(0);
        let mut resolved = vec![None; keys.len()];
        let mut observations =
            vec![vec![RouteAuthorityObservation::Unobserved; rank_count]; keys.len()];
        for rank in 0..rank_count {
            self.read_ranked_authorities(
                keys,
                ranked_authorities,
                rank,
                &mut resolved,
                RouteReadOptions {
                    repairs: Some(observations.as_mut_slice()),
                    read_all: true,
                    source_ranks: None,
                },
                "contains validation route batch read failed",
            )?;
        }
        for (index, key) in keys.iter().enumerate() {
            let selected = ranked_authorities[index].len();
            let incomplete = selected == 0
                || observations[index][..selected].iter().any(|observation| {
                    matches!(observation, RouteAuthorityObservation::Unobserved)
                });
            if incomplete {
                return Err(StoreError::Transport(format!(
                    "contains validation could not confirm route state for key {}",
                    key.0
                )));
            }
        }
        Ok(resolved
            .into_iter()
            .map(|value| matches!(value, Some(RouteAuthorityValue::Route(_))))
            .collect())
    }
}

impl Drop for EmbeddedWrhRouteDirectory {
    fn drop(&mut self) {
        self.async_mirror.shutdown();
        unregister_local_authority(&self.namespace, &self.local_stable_id);
    }
}

impl RouteDirectory for EmbeddedWrhRouteDirectory {
    fn metadata(&self) -> &dyn MetadataBackend {
        self.metadata.as_ref()
    }

    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        let mut routes = self.get_object_routes(observer, std::slice::from_ref(key))?;
        Ok(routes.pop().unwrap_or(None))
    }

    fn confirm_replica_reference(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        replica: &ReplicaRoute,
    ) -> Result<RouteReplicaReference> {
        let current_candidates = self.authority_candidates(observer)?;
        let expected_members = current_candidates
            .iter()
            .map(|authority| authority.runtime.clone())
            .collect::<BTreeSet<_>>();
        let authorities =
            self.eviction_confirmation_authorities(observer, key, &current_candidates)?;
        if authorities.is_empty() {
            return Err(StoreError::Transport(
                "exact replica confirmation has no live route authorities".to_string(),
            ));
        }

        let mut referenced: Option<ObjectRoute> = None;
        let mut last_error = None;
        for authority in authorities {
            let response = (|| {
                let mut responses =
                    self.read_authority_batch(&authority, std::slice::from_ref(key))?;
                responses.pop().ok_or_else(|| {
                    StoreError::InvalidState(
                        "route authority returned no result for exact replica confirmation"
                            .to_string(),
                    )
                })?
            })();
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    self.maybe_mark_authority_suspect(
                        &authority,
                        &error,
                        "exact_replica_confirmation_transport_failed",
                    );
                    debug!(
                        authority = %authority.runtime,
                        key = %key.0,
                        error = %error,
                        "exact replica confirmation authority is unavailable"
                    );
                    last_error = Some(error);
                    continue;
                }
            };
            let response =
                value_from_authority_record(response).and_then(RouteAuthorityValue::into_route);
            let references_replica = response.as_ref().is_some_and(|route| {
                route.replicas.iter().any(|candidate| {
                    candidate.owner == replica.owner
                        && candidate.segment_name == replica.segment_name
                        && candidate.segment_offset == replica.segment_offset
                        && candidate.allocation_id == replica.allocation_id
                        && replica
                            .allocation_id
                            .is_some_and(|allocation_id| allocation_id != 0)
                })
            });
            if references_replica {
                if let Some(route) = response {
                    match referenced.as_ref() {
                        Some(current) if current.version >= route.version => {}
                        _ => referenced = Some(route),
                    }
                }
            }
        }
        let refreshed_members = self
            .authority_candidates(observer)?
            .into_iter()
            .map(|authority| authority.runtime)
            .collect::<BTreeSet<_>>();
        if refreshed_members != expected_members {
            return Err(StoreError::StaleEpoch(
                "route authority membership changed during exact replica confirmation".to_string(),
            ));
        }
        if let Some(route) = referenced {
            return Ok(RouteReplicaReference::Referenced(Box::new(route)));
        }
        if let Some(error) = last_error {
            return Err(error);
        }
        Ok(RouteReplicaReference::Absent)
    }

    fn get_allocation_route_references_consistent(
        &self,
        observer: &ClientLease,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReferenceState>> {
        if identities.is_empty() {
            return Ok(Vec::new());
        }
        let current_candidates = self.authority_candidates(observer)?;
        let expected_members = current_candidates
            .iter()
            .map(|authority| authority.runtime.clone())
            .collect::<BTreeSet<_>>();
        let authorities =
            self.allocation_confirmation_authorities(observer, &current_candidates)?;
        if authorities.is_empty() {
            return Err(StoreError::Transport(
                "allocation reference query has no live route authorities".to_string(),
            ));
        }
        let mut consensus: Vec<Option<AllocationRouteReferenceState>> =
            vec![None; identities.len()];
        for authority in &authorities {
            let references =
                self.get_allocation_route_references_from_authority(authority, identities)?;
            if references.len() != identities.len() {
                return Err(StoreError::Transport(format!(
                    "route authority {} returned {} allocation references for {} identities",
                    authority.runtime,
                    references.len(),
                    identities.len()
                )));
            }
            for (slot, observed) in consensus.iter_mut().zip(references) {
                *slot = Some(match *slot {
                    None => observed,
                    Some(previous) if previous == observed => previous,
                    Some(_) => AllocationRouteReferenceState::Conflict,
                });
            }
        }
        let refreshed_members = self
            .authority_candidates(observer)?
            .into_iter()
            .map(|authority| authority.runtime)
            .collect::<BTreeSet<_>>();
        if refreshed_members != expected_members {
            return Err(StoreError::StaleEpoch(
                "route authority membership changed during allocation reference query".to_string(),
            ));
        }
        Ok(consensus
            .into_iter()
            .map(|state| state.expect("nonempty authorities populate every allocation reference"))
            .collect())
    }

    fn get_allocation_route_reference_keys_consistent(
        &self,
        observer: &ClientLease,
        identities: &[AllocationRouteIdentity],
    ) -> Result<Vec<AllocationRouteReference>> {
        if identities.is_empty() {
            return Ok(Vec::new());
        }
        let current_candidates = self.authority_candidates(observer)?;
        let expected_members = current_candidates
            .iter()
            .map(|authority| authority.runtime.clone())
            .collect::<BTreeSet<_>>();
        let authorities =
            self.allocation_confirmation_authorities(observer, &current_candidates)?;
        if authorities.is_empty() {
            return Err(StoreError::Transport(
                "allocation reference key query has no live route authorities".to_string(),
            ));
        }
        let mut consensus: Vec<Option<AllocationRouteReference>> = vec![None; identities.len()];
        let mut reachable_authorities = 0usize;
        let mut last_error = None;
        for authority in &authorities {
            let references = match self
                .get_allocation_route_reference_keys_from_authority(authority, identities)
            {
                Ok(references) => references,
                Err(error) => {
                    self.maybe_mark_authority_suspect(
                        authority,
                        &error,
                        "allocation_reference_key_query_transport_failed",
                    );
                    debug!(
                        authority = %authority.runtime,
                        identities = identities.len(),
                        error = %error,
                        "allocation reference key authority is unavailable"
                    );
                    last_error = Some(error);
                    continue;
                }
            };
            if references.len() != identities.len() {
                last_error = Some(StoreError::Transport(format!(
                    "route authority {} returned {} allocation reference keys for {} identities",
                    authority.runtime,
                    references.len(),
                    identities.len()
                )));
                continue;
            }
            reachable_authorities = reachable_authorities.saturating_add(1);
            for (slot, observed) in consensus.iter_mut().zip(references) {
                *slot = Some(merge_allocation_route_reference(slot.as_ref(), observed));
            }
        }
        let refreshed_members = self
            .authority_candidates(observer)?
            .into_iter()
            .map(|authority| authority.runtime)
            .collect::<BTreeSet<_>>();
        if refreshed_members != expected_members {
            return Err(StoreError::StaleEpoch(
                "route authority membership changed during allocation reference key query"
                    .to_string(),
            ));
        }
        if reachable_authorities == 0 {
            return Err(last_error.unwrap_or_else(|| {
                StoreError::Transport(
                    "allocation reference key query reached no authorities".to_string(),
                )
            }));
        }
        let has_unavailable_authority = last_error.is_some();
        Ok(consensus
            .into_iter()
            .map(|reference| {
                let reference = reference
                    .expect("reachable authorities populate every allocation reference key");
                if has_unavailable_authority
                    && reference.state == AllocationRouteReferenceState::Absent
                {
                    AllocationRouteReference::conflict()
                } else {
                    reference
                }
            })
            .collect())
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
            .map(|key| ranked_authorities(&self.namespace, &candidates, key))
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
                RouteReadOptions {
                    repairs: Some(repairs.as_mut_slice()),
                    read_all: false,
                    source_ranks: None,
                },
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
            RouteReadOptions {
                repairs: None,
                read_all: false,
                source_ranks: None,
            },
            "fallback authority route batch read failed; trying other authorities",
        )? {
            rank = rank.saturating_add(1);
        }

        self.backfill_missing_authorities(keys, &selections, &resolved, &repairs);
        Ok(resolved
            .into_iter()
            .map(|value| value.and_then(RouteAuthorityValue::into_route))
            .collect())
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
            .map(|key| ranked_top_authorities(&self.namespace, &candidates, key, self.route_topk))
            .collect::<Vec<_>>();
        let mut resolved = vec![None; keys.len()];
        for rank in 0..self.route_topk {
            self.read_ranked_authorities(
                keys,
                &ranked_authorities,
                rank,
                &mut resolved,
                RouteReadOptions {
                    repairs: None,
                    read_all: false,
                    source_ranks: None,
                },
                if rank == 0 {
                    "authority bounded route batch read failed; trying mirrored authorities"
                } else {
                    "mirrored bounded route batch read failed; trying other authorities"
                },
            )?;
        }
        Ok(resolved
            .into_iter()
            .map(|value| value.and_then(RouteAuthorityValue::into_route))
            .collect())
    }

    fn contains_object_route(&self, observer: &ClientLease, key: &ObjectKey) -> Result<bool> {
        let mut results =
            self.contains_object_routes_bounded(observer, std::slice::from_ref(key))?;
        Ok(results.pop().unwrap_or(false))
    }

    fn contains_object_routes_bounded(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Result<Vec<bool>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let candidates = self.authority_candidates(observer)?;
        if candidates.is_empty() {
            return Ok(vec![false; keys.len()]);
        }
        let ranked_authorities = keys
            .iter()
            .map(|key| ranked_top_authorities(&self.namespace, &candidates, key, self.route_topk))
            .collect::<Vec<_>>();
        let baseline_candidates = {
            let mut baseline = self.baseline.lock();
            let has_removed_members = baseline
                .member_ids
                .iter()
                .any(|id| !candidates.iter().any(|c| c.runtime.stable_id.0 == *id));
            if baseline.created_at.elapsed() >= BASELINE_TTL || has_removed_members {
                baseline.member_ids = candidates
                    .iter()
                    .map(|c| c.runtime.stable_id.0.clone())
                    .collect();
                baseline.candidates = candidates.clone();
                baseline.created_at = Instant::now();
                None
            } else {
                let has_new_members = candidates
                    .iter()
                    .any(|c| !baseline.member_ids.contains(&c.runtime.stable_id.0));
                if has_new_members && !baseline.candidates.is_empty() {
                    Some(baseline.candidates.clone())
                } else {
                    None
                }
            }
        };
        let baseline_ranked_authorities = baseline_candidates.map(|baseline_candidates| {
            keys.iter()
                .map(|key| {
                    ranked_top_authorities(
                        &self.namespace,
                        &baseline_candidates,
                        key,
                        self.route_topk,
                    )
                })
                .collect::<Vec<_>>()
        });
        self.contains_object_routes_with_readable_filter(
            observer,
            keys,
            &ranked_authorities,
            baseline_ranked_authorities.as_deref(),
        )
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

    fn compare_and_swap_local_route(
        &self,
        _observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        authority_compare_and_swap(&self.namespace, &self.local_stable_id, key, expected, next)
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
        let mut selections = select_authorities_for_requests(
            &self.namespace,
            &candidates,
            requests,
            self.route_topk,
        );
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
                let refreshed_candidates = self.authority_candidates_force_refresh(observer)?;
                let retry_requests = retry_indices
                    .iter()
                    .map(|index| requests[*index].clone())
                    .collect::<Vec<_>>();
                let retry_selections = select_authorities_for_requests(
                    &self.namespace,
                    &refreshed_candidates,
                    &retry_requests,
                    self.route_topk,
                );
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
                    let is_delete_marker = request
                        .next
                        .as_ref()
                        .is_none_or(|route| route.state == RouteState::Tombstone);
                    let delete_version = result
                        .as_ref()
                        .ok()
                        .and_then(|cas| cas.delete_version)
                        .or(request.expected);
                    let fanout = if is_delete_marker {
                        self.delete_marker_fanout_authorities(observer, &request.key, &candidates)
                    } else {
                        selections[index].authorities.clone()
                    };
                    for secondary in &fanout {
                        if secondary.runtime.stable_id == served.runtime.stable_id {
                            continue;
                        }
                        let next = if is_delete_marker {
                            None
                        } else {
                            request.next.clone()
                        };
                        mirrors
                            .entry(secondary.runtime.stable_id.0.clone())
                            .or_insert_with(|| (secondary.clone(), Vec::new()))
                            .1
                            .push(RouteCasRequest {
                                key: request.key.clone(),
                                expected: if is_delete_marker {
                                    delete_version
                                } else {
                                    request.expected
                                },
                                next,
                            });
                    }
                }
            }
            if let Ok(cas) = &result {
                record_cas_outcome(cas);
            }
            results.push(result);
        }

        for (_, (secondary, requests)) in mirrors {
            self.async_mirror.enqueue(secondary, requests);
        }
        Ok(results)
    }

    fn list_routes_by_replica_owner(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        let mut routes = Vec::new();
        let status = self.visit_routes_by_replica_owner_pages(observer, owner, &mut |route| {
            if routes.len() == DEFAULT_ROUTE_OWNER_PAGE_SIZE {
                return Err(StoreError::Unsupported(
                    "complete owner-route listing exceeds the bounded compatibility page; use the visitor API"
                        .to_string(),
                ));
            }
            routes.push(route);
            Ok(())
        }, false)?;
        match status {
            RouteOwnerScanStatus::Complete => Ok(routes),
            RouteOwnerScanStatus::Incomplete => Err(StoreError::Transport(
                "owner route scan incomplete".to_string(),
            )),
        }
    }

    fn visit_routes_by_replica_owner(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
        visitor: &mut dyn FnMut(ObjectRoute) -> Result<()>,
    ) -> Result<()> {
        match self.visit_routes_by_replica_owner_pages(observer, owner, visitor, false)? {
            RouteOwnerScanStatus::Complete => Ok(()),
            RouteOwnerScanStatus::Incomplete => Err(StoreError::Transport(
                "owner route scan incomplete".to_string(),
            )),
        }
    }

    fn visit_routes_by_replica_owner_consistent(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
        visitor: &mut dyn FnMut(ObjectRoute) -> Result<()>,
    ) -> Result<()> {
        match self.visit_routes_by_replica_owner_pages(observer, owner, visitor, true)? {
            RouteOwnerScanStatus::Complete => Ok(()),
            RouteOwnerScanStatus::Incomplete => Err(StoreError::Transport(
                "owner route scan incomplete".to_string(),
            )),
        }
    }

    fn visit_routes_by_replica_owner_with_status(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
        visitor: &mut dyn FnMut(ObjectRoute) -> Result<()>,
    ) -> Result<RouteOwnerScanStatus> {
        self.visit_routes_by_replica_owner_pages(observer, owner, visitor, false)
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
        self.get_delete_marker(observer, key)
    }

    fn get_version_floors(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Vec<Option<RouteVersion>> {
        self.get_delete_markers(observer, keys)
    }

    fn get_delete_marker(&self, observer: &ClientLease, key: &ObjectKey) -> Option<RouteVersion> {
        let candidates = self.authority_candidates(observer).ok()?;
        let ranked = ranked_authorities(&self.namespace, &candidates, key);
        ranked
            .iter()
            .take(self.route_topk)
            .filter_map(|authority| {
                authority_get_delete_marker(&self.namespace, &authority.runtime.stable_id, key)
            })
            .max()
    }

    fn get_delete_markers(
        &self,
        observer: &ClientLease,
        keys: &[ObjectKey],
    ) -> Vec<Option<RouteVersion>> {
        if keys.is_empty() {
            return Vec::new();
        }
        let Ok(candidates) = self.authority_candidates(observer) else {
            return vec![None; keys.len()];
        };
        let ranked_authorities = keys
            .iter()
            .map(|key| ranked_top_authorities(&self.namespace, &candidates, key, self.route_topk))
            .collect::<Vec<_>>();
        let mut delete_versions: Vec<Option<RouteVersion>> = vec![None; keys.len()];
        for rank in 0..self.route_topk {
            let mut by_authority = BTreeMap::<ClientStableId, Vec<(usize, ObjectKey)>>::new();
            for (index, authorities) in ranked_authorities.iter().enumerate() {
                let Some(authority) = authorities.get(rank) else {
                    continue;
                };
                by_authority
                    .entry(authority.runtime.stable_id.clone())
                    .or_default()
                    .push((index, keys[index].clone()));
            }
            if by_authority.is_empty() {
                break;
            }
            for (authority, entries) in by_authority {
                let lookup_keys = entries
                    .iter()
                    .map(|(_, key)| key.clone())
                    .collect::<Vec<_>>();
                for ((index, _), delete_version) in entries.into_iter().zip(
                    authority_get_delete_markers(&self.namespace, &authority, &lookup_keys),
                ) {
                    if let Some(delete_version) = delete_version {
                        delete_versions[index] = Some(
                            delete_versions[index]
                                .map_or(delete_version, |current| current.max(delete_version)),
                        );
                    }
                }
            }
        }
        delete_versions
    }

    fn flush_pending_route_mirrors(&self) {
        let routes = match authority_list_routes(&self.namespace, &self.local_stable_id) {
            Ok(routes) => routes,
            Err(error) => {
                warn!(error = %error, "flush_pending_route_mirrors: failed to list local routes");
                return;
            }
        };
        if routes.is_empty() {
            return;
        }
        // After drain, routes that still list the local runtime as a replica owner
        // are stale secondary copies — the CAS migration happened on the primary
        // authority but the async mirror back was dropped.  Skip them to avoid
        // overwriting correctly-migrated state on peers.
        let requests: Vec<RouteCasRequest> = routes
            .into_iter()
            .filter(|route| {
                !route
                    .replicas
                    .iter()
                    .any(|r| r.owner.stable_id == self.local_stable_id)
            })
            .map(|route| RouteCasRequest {
                key: route.key.clone(),
                expected: None,
                next: Some(route),
            })
            .collect();
        if requests.is_empty() {
            return;
        }

        let peers = match self
            .membership
            .live_clients(false, "flush_pending_route_mirrors")
        {
            Ok(peers) => peers,
            Err(error) => {
                warn!(error = %error, "flush_pending_route_mirrors: failed to list peers");
                return;
            }
        };

        let peer_count = peers
            .iter()
            .filter(|p| {
                p.runtime.stable_id != self.local_stable_id
                    && p.endpoints.labels.get("route").map(String::as_str) == Some("true")
            })
            .count();
        debug!(
            local_routes = requests.len(),
            peers = peer_count,
            "flushing local authority routes to peer authorities"
        );

        for peer in &peers {
            if peer.runtime.stable_id == self.local_stable_id {
                continue;
            }
            if peer.endpoints.labels.get("route").map(String::as_str) != Some("true") {
                continue;
            }
            mirror_secondary_batch(
                &self.namespace,
                self.authority_client.as_ref(),
                self.membership.as_ref(),
                peer,
                &requests,
            );
        }
    }
}

#[cfg(test)]
mod owner_page_contract_tests {
    use super::*;
    use crate::RouteOwnerPage;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLifecycleState, ColdBackingRoute, ColdBackingState,
        CompatibilityDescriptor, ReplicaRoute, ReplicaTier, RouteState, SegmentName,
    };
    use mooncake_store_rs_metadata::InMemoryMetadataBackend;

    struct TestMembership {
        leases: Vec<ClientLease>,
    }

    struct MutableMembership {
        leases: Mutex<Vec<ClientLease>>,
    }

    impl RouteMembershipProvider for MutableMembership {
        fn live_clients(
            &self,
            _force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            Ok(self.leases.lock().clone())
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn clear_suspect(&self, _runtime: &ClientRuntimeId) {}

        fn is_suspect(&self, _runtime: &ClientRuntimeId) -> bool {
            false
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    impl RouteMembershipProvider for TestMembership {
        fn live_clients(
            &self,
            _force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            Ok(self.leases.clone())
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn clear_suspect(&self, _runtime: &ClientRuntimeId) {}

        fn is_suspect(&self, _runtime: &ClientRuntimeId) -> bool {
            false
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    struct SnapshotChangingMembership {
        first: Vec<ClientLease>,
        refreshed: Vec<ClientLease>,
        snapshot_calls: AtomicUsize,
    }

    struct CachedOnlyMembership {
        leases: Vec<ClientLease>,
    }

    struct RetainedMembership {
        current: Vec<ClientLease>,
        retained: Vec<ClientLease>,
    }

    impl RouteMembershipProvider for SnapshotChangingMembership {
        fn live_clients(
            &self,
            _force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            if self.snapshot_calls.fetch_add(1, Ordering::SeqCst) > 1 {
                Ok(self.refreshed.clone())
            } else {
                Ok(self.first.clone())
            }
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn clear_suspect(&self, _runtime: &ClientRuntimeId) {}

        fn is_suspect(&self, _runtime: &ClientRuntimeId) -> bool {
            false
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    impl RouteMembershipProvider for CachedOnlyMembership {
        fn live_clients(
            &self,
            force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            if force_refresh {
                return Err(StoreError::Metadata(
                    "injected metadata refresh failure".to_string(),
                ));
            }
            Ok(self.leases.clone())
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn clear_suspect(&self, _runtime: &ClientRuntimeId) {}

        fn is_suspect(&self, _runtime: &ClientRuntimeId) -> bool {
            false
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    impl RouteMembershipProvider for RetainedMembership {
        fn live_clients(
            &self,
            force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            if force_refresh {
                return Err(StoreError::Metadata(
                    "injected metadata refresh failure".to_string(),
                ));
            }
            Ok(self.current.clone())
        }

        fn retained_live_clients(&self, _operation: &'static str) -> Result<Vec<ClientLease>> {
            Ok(self.retained.clone())
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn clear_suspect(&self, _runtime: &ClientRuntimeId) {}

        fn is_suspect(&self, _runtime: &ClientRuntimeId) -> bool {
            false
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    struct ConfirmationClient {
        routes: BTreeMap<String, Option<ObjectRoute>>,
        allocation_references: BTreeMap<String, Vec<AllocationRouteReferenceState>>,
    }

    struct RecordingConfirmationClient {
        routes: BTreeMap<String, Option<ObjectRoute>>,
        delete_versions: BTreeMap<String, Option<RouteVersion>>,
        read_failures: BTreeSet<String>,
        queried_authorities: Arc<Mutex<Vec<String>>>,
    }

    impl RouteAuthorityClient for RecordingConfirmationClient {
        fn batch_get_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<RouteAuthorityRecord>>> {
            self.queried_authorities.lock().push(authority.0.clone());
            if self.read_failures.contains(&authority.0) {
                return Err(StoreError::Transport(format!(
                    "injected route read failure for {}",
                    authority.0
                )));
            }
            let route = self.routes.get(&authority.0).cloned().unwrap_or(None);
            Ok(keys
                .iter()
                .map(|_| {
                    Ok(RouteAuthorityRecord {
                        route: route.clone(),
                        delete_version: self
                            .delete_versions
                            .get(&authority.0)
                            .cloned()
                            .unwrap_or(None),
                    })
                })
                .collect())
        }

        fn batch_contains_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<bool>>> {
            if self.read_failures.contains(&authority.0) {
                return Err(StoreError::Transport(format!(
                    "injected contains failure for {}",
                    authority.0
                )));
            }
            let present = self
                .routes
                .get(&authority.0)
                .cloned()
                .unwrap_or(None)
                .is_some();
            Ok(keys.iter().map(|_| Ok(present)).collect())
        }

        fn batch_compare_and_swap_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<CasResult>>> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn batch_replace_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<()>>> {
            Ok(requests.iter().map(|_| Ok(())).collect())
        }

        fn list_routes_by_replica_owner_page(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _query: &RouteOwnerPageQuery,
        ) -> Result<RouteOwnerPage> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }
    }

    type FullRouteQueries = Arc<Mutex<Vec<(String, Vec<ObjectKey>)>>>;

    struct PresenceConfirmationClient {
        inner: RecordingConfirmationClient,
        presence_queries: Arc<Mutex<Vec<String>>>,
        full_queries: Option<FullRouteQueries>,
        probe_queries: Option<Arc<Mutex<Vec<String>>>>,
        item_failures: BTreeSet<String>,
        item_unsupported: BTreeSet<(String, String)>,
        presence_misses: BTreeSet<(String, String)>,
        filtered_presence: bool,
        malformed: bool,
        contains_present: bool,
        unsupported: BTreeSet<String>,
    }

    impl RouteAuthorityClient for PresenceConfirmationClient {
        fn batch_get_route_presence(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<RouteAuthorityPresence>>> {
            self.presence_queries.lock().push(authority.0.clone());
            if self.unsupported.contains(&authority.0) {
                return Err(StoreError::Unsupported(format!(
                    "injected legacy presence response for {}",
                    authority.0
                )));
            }
            if self.inner.read_failures.contains(&authority.0) {
                return Err(StoreError::Transport(format!(
                    "injected presence failure for {}",
                    authority.0
                )));
            }
            if self.malformed {
                return Ok(Vec::new());
            }
            let route = self.inner.routes.get(&authority.0).cloned().unwrap_or(None);
            let delete_version = self
                .inner
                .delete_versions
                .get(&authority.0)
                .cloned()
                .unwrap_or(None);
            Ok(keys
                .iter()
                .map(|key| {
                    if self.item_failures.contains(&authority.0) {
                        return Err(StoreError::Transport(format!(
                            "injected presence item failure for {}",
                            authority.0
                        )));
                    }
                    if self
                        .item_unsupported
                        .contains(&(authority.0.clone(), key.0.clone()))
                    {
                        return Err(StoreError::Unsupported(format!(
                            "injected legacy presence item for {}/{}",
                            authority.0, key.0
                        )));
                    }
                    Ok(RouteAuthorityPresence {
                        live_version: route.as_ref().and_then(|route| {
                            (!self
                                .presence_misses
                                .contains(&(authority.0.clone(), key.0.clone())))
                            .then_some(route.version)
                        }),
                        delete_version,
                    })
                })
                .collect())
        }

        fn batch_get_filtered_route_presence(
            &self,
            lease: &ClientLease,
            namespace: &str,
            authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<RouteAuthorityPresence>>> {
            if !self.filtered_presence {
                return Err(StoreError::Unsupported(
                    "filtered presence is not confirmed".to_string(),
                ));
            }
            self.batch_get_route_presence(lease, namespace, authority, keys)
        }

        fn batch_get_routes(
            &self,
            lease: &ClientLease,
            namespace: &str,
            authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<RouteAuthorityRecord>>> {
            if let Some(queries) = &self.full_queries {
                queries.lock().push((authority.0.clone(), keys.to_vec()));
            }
            self.inner
                .batch_get_routes(lease, namespace, authority, keys)
        }

        fn batch_contains_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<bool>>> {
            if let Some(queries) = &self.probe_queries {
                queries.lock().push(authority.0.clone());
            }
            let present = self.contains_present;
            Ok(keys.iter().map(|_| Ok(present)).collect())
        }

        fn batch_compare_and_swap_routes(
            &self,
            lease: &ClientLease,
            namespace: &str,
            authority: &ClientStableId,
            requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<CasResult>>> {
            self.inner
                .batch_compare_and_swap_routes(lease, namespace, authority, requests)
        }

        fn batch_replace_routes(
            &self,
            lease: &ClientLease,
            namespace: &str,
            authority: &ClientStableId,
            requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<()>>> {
            self.inner
                .batch_replace_routes(lease, namespace, authority, requests)
        }

        fn list_routes_by_replica_owner_page(
            &self,
            lease: &ClientLease,
            namespace: &str,
            authority: &ClientStableId,
            query: &RouteOwnerPageQuery,
        ) -> Result<RouteOwnerPage> {
            self.inner
                .list_routes_by_replica_owner_page(lease, namespace, authority, query)
        }
    }

    impl RouteAuthorityClient for ConfirmationClient {
        fn batch_get_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<RouteAuthorityRecord>>> {
            let route = self.routes.get(&authority.0).cloned().ok_or_else(|| {
                StoreError::Transport(format!(
                    "injected route authority failure for {}",
                    authority.0
                ))
            })?;
            Ok(keys
                .iter()
                .map(|_| {
                    Ok(RouteAuthorityRecord {
                        route: route.clone(),
                        delete_version: None,
                    })
                })
                .collect())
        }

        fn batch_contains_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _keys: &[ObjectKey],
        ) -> Result<Vec<Result<bool>>> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn batch_compare_and_swap_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<CasResult>>> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn batch_replace_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<()>>> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn list_routes_by_replica_owner_page(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _query: &RouteOwnerPageQuery,
        ) -> Result<RouteOwnerPage> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn get_allocation_route_references(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            authority: &ClientStableId,
            identities: &[AllocationRouteIdentity],
        ) -> Result<Vec<AllocationRouteReferenceState>> {
            let references = self
                .allocation_references
                .get(&authority.0)
                .cloned()
                .ok_or_else(|| {
                    StoreError::Unsupported(
                        "missing allocation-reference test response".to_string(),
                    )
                })?;
            if references.len() != identities.len() {
                return Err(StoreError::InvalidState(
                    "allocation-reference test response length mismatch".to_string(),
                ));
            }
            Ok(references)
        }

        fn get_allocation_route_reference_keys(
            &self,
            lease: &ClientLease,
            namespace: &str,
            authority: &ClientStableId,
            identities: &[AllocationRouteIdentity],
        ) -> Result<Vec<AllocationRouteReference>> {
            self.get_allocation_route_references(lease, namespace, authority, identities)?
                .into_iter()
                .map(|state| match state {
                    AllocationRouteReferenceState::Absent => Ok(AllocationRouteReference::absent()),
                    AllocationRouteReferenceState::Conflict => {
                        Ok(AllocationRouteReference::conflict())
                    }
                    AllocationRouteReferenceState::Exact => self
                        .routes
                        .get(&authority.0)
                        .and_then(|route| route.as_ref())
                        .map(|route| AllocationRouteReference::exact(route.key.clone()))
                        .ok_or_else(|| {
                            StoreError::InvalidState(format!(
                                "exact allocation reference for {} has no route",
                                authority.0
                            ))
                        }),
                })
                .collect()
        }
    }

    fn route(key: &str) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new(key),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: None,
            nof_backing: None,
        }
    }

    #[test]
    fn owner_route_pages_reject_cursor_cycles() {
        let mut seen = HashSet::new();
        for (cursor, key) in [(None, "route-a"), (Some("route-a"), "route-b")] {
            validate_owner_route_page(
                cursor,
                &crate::RouteOwnerPage {
                    snapshot_epoch: None,
                    routes: vec![route(key)],
                    next_cursor: Some(key.to_string()),
                },
                &mut seen,
            )
            .expect("forward pages should satisfy the cursor contract");
        }

        let error = validate_owner_route_page(
            Some("route-b"),
            &crate::RouteOwnerPage {
                snapshot_epoch: None,
                routes: vec![route("route-a")],
                next_cursor: Some("route-a".to_string()),
            },
            &mut seen,
        )
        .expect_err("a cursor cycle must fail closed");
        assert!(matches!(error, StoreError::Transport(_)));
    }

    #[test]
    fn owner_route_pages_reject_oversized_responses() {
        let routes = (0..=DEFAULT_ROUTE_OWNER_PAGE_SIZE)
            .map(|index| route(&format!("route-{index:04}")))
            .collect();
        let error = validate_owner_route_page(
            None,
            &crate::RouteOwnerPage {
                snapshot_epoch: None,
                routes,
                next_cursor: None,
            },
            &mut HashSet::new(),
        )
        .expect_err("an oversized route page must fail closed");
        assert!(matches!(error, StoreError::Transport(_)));
    }

    fn test_lease(stable_id: &str) -> ClientLease {
        let mut labels = BTreeMap::new();
        labels.insert("route".to_string(), "true".to_string());
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            expires_at_ms: u64::MAX,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            endpoints: ClientEndpointSet {
                labels,
                ..ClientEndpointSet::default()
            },
        }
    }

    fn test_route(key: &str, owner: &ClientRuntimeId) -> ObjectRoute {
        ObjectRoute {
            nof_backing: None,
            key: ObjectKey::new(key.to_string()),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            replicas: vec![ReplicaRoute {
                owner: owner.clone(),
                segment_name: SegmentName::new("segment-a".to_string()),
                allocation_id: None,
                segment_offset: 0,
                length: 1,
                checksum: None,
                offset: Some(0),
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,
        }
    }

    #[test]
    fn exact_replica_confirmation_keeps_allocation_when_any_authority_references_it() {
        let observer = test_lease("observer");
        let replica_owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut referenced = test_route("confirmation-key", &replica_owner);
        referenced.replicas[0].allocation_id = Some(7);
        let absent = ObjectRoute {
            replicas: Vec::new(),
            ..referenced.clone()
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::from([
                    ("authority-a".to_string(), Some(referenced.clone())),
                    ("authority-b".to_string(), Some(absent)),
                ]),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert!(matches!(
            directory
                .confirm_replica_reference(&observer, &referenced.key, &referenced.replicas[0])
                .expect("an exact reference on any proof authority must retain the allocation"),
            RouteReplicaReference::Referenced(_)
        ));
    }

    #[test]
    fn exact_replica_confirmation_keeps_positive_proof_when_peer_is_unavailable() {
        let observer = test_lease("observer");
        let replica_owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut referenced = test_route("confirmation-partial-key", &replica_owner);
        referenced.replicas[0].allocation_id = Some(7);
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::from([("authority-a".to_string(), Some(referenced.clone()))]),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert!(matches!(
            directory
                .confirm_replica_reference(&observer, &referenced.key, &referenced.replicas[0])
                .expect("one exact authority must prove the live replica"),
            RouteReplicaReference::Referenced(_)
        ));
    }

    #[test]
    fn exact_replica_confirmation_accepts_consistent_absence() {
        let observer = test_lease("observer");
        let replica_owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut expected_route = test_route("confirmation-absent-key", &replica_owner);
        expected_route.replicas[0].allocation_id = Some(7);
        let absent = ObjectRoute {
            replicas: Vec::new(),
            ..expected_route.clone()
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::from([
                    ("authority-a".to_string(), Some(absent.clone())),
                    ("authority-b".to_string(), Some(absent)),
                ]),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert!(matches!(
            directory
                .confirm_replica_reference(
                    &observer,
                    &expected_route.key,
                    &expected_route.replicas[0],
                )
                .expect("consistent authorities should confirm absence"),
            RouteReplicaReference::Absent
        ));
    }

    #[test]
    fn exact_replica_confirmation_uses_cached_membership() {
        let observer = test_lease("observer");
        let replica_owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut expected_route = test_route("confirmation-cached-membership", &replica_owner);
        expected_route.replicas[0].allocation_id = Some(7);
        let absent = ObjectRoute {
            replicas: Vec::new(),
            ..expected_route.clone()
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::from([
                    ("authority-a".to_string(), Some(absent.clone())),
                    ("authority-b".to_string(), Some(absent)),
                ]),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(CachedOnlyMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert!(matches!(
            directory
                .confirm_replica_reference(
                    &observer,
                    &expected_route.key,
                    &expected_route.replicas[0],
                )
                .expect("cached membership must be sufficient for confirmation"),
            RouteReplicaReference::Absent
        ));
    }

    #[test]
    fn exact_replica_confirmation_treats_missing_and_unreferenced_as_absent() {
        let observer = test_lease("observer");
        let replica_owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut expected_route = test_route("confirmation-semantic-absence", &replica_owner);
        expected_route.replicas[0].allocation_id = Some(7);
        let absent = ObjectRoute {
            replicas: Vec::new(),
            ..expected_route.clone()
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::from([
                    ("authority-a".to_string(), Some(absent)),
                    ("authority-b".to_string(), None),
                ]),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert!(matches!(
            directory
                .confirm_replica_reference(
                    &observer,
                    &expected_route.key,
                    &expected_route.replicas[0],
                )
                .expect("semantic absence must not depend on complete route equality"),
            RouteReplicaReference::Absent
        ));
    }

    #[test]
    fn exact_replica_confirmation_ranks_the_merged_membership_once() {
        let observer = test_lease("observer");
        let replica_owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut referenced = test_route("confirmation-retained-authority", &replica_owner);
        referenced.replicas[0].allocation_id = Some(7);
        let absent = ObjectRoute {
            replicas: Vec::new(),
            ..referenced.clone()
        };
        let current = vec![
            test_lease("current-a"),
            test_lease("current-b"),
            test_lease("current-c"),
        ];
        let retained = vec![
            test_lease("retained-a"),
            test_lease("retained-b"),
            test_lease("retained-c"),
        ];
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let expected_ranked =
            ranked_merged_authorities(&namespace, &current, &retained, &referenced.key, 2);
        let referenced_authority = expected_ranked[0].runtime.stable_id.0.clone();
        let expected_authorities = expected_ranked
            .iter()
            .map(|lease| lease.runtime.stable_id.0.clone())
            .collect::<BTreeSet<_>>();
        let queried_authorities = Arc::new(Mutex::new(Vec::new()));
        let routes = expected_authorities
            .iter()
            .map(|authority| {
                (
                    authority.clone(),
                    Some(if *authority == referenced_authority {
                        referenced.clone()
                    } else {
                        absent.clone()
                    }),
                )
            })
            .collect();
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes,
                delete_versions: BTreeMap::new(),
                read_failures: BTreeSet::new(),
                queried_authorities: queried_authorities.clone(),
            }),
            Arc::new(RetainedMembership { current, retained }),
        );

        assert!(matches!(
            directory
                .confirm_replica_reference(&observer, &referenced.key, &referenced.replicas[0],)
                .expect("a merged-membership reference must keep the allocation"),
            RouteReplicaReference::Referenced(_)
        ));
        assert_eq!(
            queried_authorities
                .lock()
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            expected_authorities
        );
        assert!(queried_authorities.lock().len() <= 2);
    }

    #[test]
    fn delete_marker_fanout_ranks_the_merged_membership_once() {
        let observer = test_lease("observer-delete-fanout");
        let current = (0..6)
            .map(|index| test_lease(&format!("current-{index}")))
            .collect::<Vec<_>>();
        let retained = (0..6)
            .map(|index| test_lease(&format!("retained-{index}")))
            .collect::<Vec<_>>();
        let key = ObjectKey::new("delete-fanout-key".to_string());
        let membership = Arc::new(RetainedMembership {
            current: current.clone(),
            retained: retained.clone(),
        });
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::new(),
            }),
            membership,
        );

        let actual = directory
            .delete_marker_fanout_authorities(&observer, &key, &current)
            .into_iter()
            .map(|lease| lease.runtime.stable_id.0)
            .collect::<BTreeSet<_>>();
        let expected =
            ranked_merged_authorities(&directory.namespace, &current, &retained, &key, 4)
                .into_iter()
                .map(|lease| lease.runtime.stable_id.0)
                .collect::<BTreeSet<_>>();

        assert_eq!(actual, expected);
        assert!(actual.len() <= 4);
    }

    fn mirror_regression_directory(
        observer: &ClientLease,
        peers: Vec<ClientLease>,
    ) -> EmbeddedWrhRouteDirectory {
        EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership { leases: peers }),
        )
    }

    #[test]
    fn authority_mirror_ignores_delayed_older_live_route() {
        let observer = test_lease("observer-delayed-live-mirror");
        let directory = mirror_regression_directory(&observer, vec![observer.clone()]);
        let authority = crate::mesh::local_authority::LocalRouteAuthority::new(
            &directory.namespace,
            observer.runtime.stable_id.clone(),
        );
        let mut old = test_route("delayed-live-mirror", &observer.runtime);
        old.version = RouteVersion(3);
        let mut new = old.clone();
        new.version = RouteVersion(4);
        authority.replace_route(&old.key, Some(&old)).unwrap();
        authority.replace_route(&new.key, Some(&new)).unwrap();
        authority.replace_route(&old.key, Some(&old)).unwrap();
        assert_eq!(
            authority.get_route(&old.key).unwrap().unwrap().version,
            new.version
        );
    }

    #[test]
    fn authority_mirror_ignores_delayed_delete_of_older_live_route() {
        let observer = test_lease("observer-delayed-delete-mirror");
        let directory = mirror_regression_directory(&observer, vec![observer.clone()]);
        let authority = crate::mesh::local_authority::LocalRouteAuthority::new(
            &directory.namespace,
            observer.runtime.stable_id.clone(),
        );
        let mut route = test_route("delayed-delete-mirror", &observer.runtime);
        route.version = RouteVersion(4);
        authority.replace_route(&route.key, Some(&route)).unwrap();
        authority
            .replace_routes(&[RouteCasRequest {
                key: route.key.clone(),
                expected: Some(RouteVersion(3)),
                next: None,
            }])
            .unwrap();
        assert_eq!(authority.get_route(&route.key).unwrap(), Some(route));
    }

    #[test]
    fn public_route_cas_respects_configured_delete_marker_budget() {
        let observer = test_lease("budget-authority");
        let directory = EmbeddedWrhRouteDirectory::new_with_delete_marker_budget(
            1,
            256,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: vec![observer.clone()],
            }),
        );
        let keys = (0..32)
            .map(|index| ObjectKey::new(format!("budget-key-{index}")))
            .collect::<Vec<_>>();
        for key in &keys {
            let route = test_route(&key.0, &observer.runtime);
            assert!(
                directory
                    .compare_and_swap_object_route(&observer, key, None, Some(&route))
                    .unwrap()
                    .applied
            );
            assert!(
                directory
                    .compare_and_swap_object_route(&observer, key, Some(route.version), None)
                    .unwrap()
                    .applied
            );
        }
        let markers = directory.get_delete_markers(&observer, &keys);
        assert_eq!(markers.iter().filter(|marker| marker.is_some()).count(), 1);
        assert_eq!(markers.last(), Some(&Some(RouteVersion(1))));
        let last = keys.last().unwrap();
        let stale = test_route(&last.0, &observer.runtime);
        let result = directory
            .compare_and_swap_object_route(&observer, last, None, Some(&stale))
            .unwrap();
        assert!(!result.applied);
        assert_eq!(result.delete_version, Some(RouteVersion(1)));
    }

    #[test]
    fn version_floor_queries_merge_highest_authority_version() {
        let observer = test_lease("observer-version-floor-merge");
        let peer = test_lease("peer-version-floor-merge");
        let candidates = vec![observer.clone(), peer.clone()];
        let directory = mirror_regression_directory(&observer, candidates.clone());
        register_local_authority(
            &directory.namespace,
            &peer.runtime.stable_id,
            DEFAULT_DELETE_MARKER_BUDGET_BYTES,
        );
        let key = ObjectKey::new("version-floor-merge");
        let ranked = ranked_top_authorities(&directory.namespace, &candidates, &key, 2);
        for (lease, version) in ranked.iter().zip([RouteVersion(4), RouteVersion(9)]) {
            let authority = crate::mesh::local_authority::LocalRouteAuthority::new(
                &directory.namespace,
                lease.runtime.stable_id.clone(),
            );
            let mut route = test_route(&key.0, &observer.runtime);
            route.version = version;
            authority.replace_route(&key, Some(&route)).unwrap();
            assert!(
                authority
                    .compare_and_swap_route(&key, Some(version), None)
                    .unwrap()
                    .applied
            );
        }
        let single = directory.get_version_floor(&observer, &key);
        let batch = directory.get_version_floors(&observer, std::slice::from_ref(&key));
        unregister_local_authority(&directory.namespace, &peer.runtime.stable_id);
        assert_eq!(
            (single, batch),
            (Some(RouteVersion(9)), vec![Some(RouteVersion(9))]),
        );
    }

    #[test]
    fn public_reads_repair_delete_markers_without_exposing_tombstone_routes() {
        let observer = test_lease("typed-marker-observer");
        let peer = test_lease("typed-marker-peer");
        let candidates = vec![observer.clone(), peer.clone()];
        let key = ObjectKey::new("typed-marker-key".to_string());
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: candidates.clone(),
            }),
        );
        register_local_authority(
            &directory.namespace,
            &peer.runtime.stable_id,
            DEFAULT_DELETE_MARKER_BUDGET_BYTES,
        );
        let ranked = ranked_top_authorities(&directory.namespace, &candidates, &key, 2);
        let mut stale = test_route(&key.0, &observer.runtime);
        stale.version = RouteVersion(2);
        stale.replicas[0].allocation_id = Some(7);
        crate::mesh::registry::authority_replace_many(
            &directory.namespace,
            &ranked[0].runtime.stable_id,
            &[RouteCasRequest {
                key: key.clone(),
                expected: None,
                next: Some(stale.clone()),
            }],
        )
        .unwrap();
        crate::mesh::registry::authority_replace_many(
            &directory.namespace,
            &ranked[1].runtime.stable_id,
            &[RouteCasRequest {
                key: key.clone(),
                expected: Some(RouteVersion(5)),
                next: None,
            }],
        )
        .unwrap();
        assert!(directory
            .get_object_route(&observer, &key)
            .unwrap()
            .is_none());
        assert_eq!(
            authority_get_delete_marker(&directory.namespace, &ranked[0].runtime.stable_id, &key),
            Some(RouteVersion(5))
        );
        let rejected = directory
            .compare_and_swap_object_route(&observer, &key, None, Some(&stale))
            .unwrap();
        assert!(!rejected.applied);
        assert_eq!(rejected.delete_version, Some(RouteVersion(5)));
        stale.version = RouteVersion(6);
        stale.replicas[0].allocation_id = Some(8);
        assert!(
            directory
                .compare_and_swap_object_route(&observer, &key, Some(RouteVersion(5)), Some(&stale))
                .unwrap()
                .applied
        );
        let live = directory
            .get_object_route(&observer, &key)
            .unwrap()
            .unwrap();
        assert_eq!(live.version, RouteVersion(6));
        assert_eq!(live.replicas[0].allocation_id, Some(8));
        unregister_local_authority(&directory.namespace, &peer.runtime.stable_id);
    }

    #[test]
    fn contains_reads_merge_remote_delete_marker_versions() {
        for (live_version, expected) in [(2, false), (6, true)] {
            let observer = test_lease(&format!("contains-remote-observer-{live_version}"));
            let candidates = vec![
                test_lease(&format!("contains-remote-a-{live_version}")),
                test_lease(&format!("contains-remote-b-{live_version}")),
            ];
            let key = ObjectKey::new(format!("contains-remote-key-{live_version}"));
            let metadata = Arc::new(InMemoryMetadataBackend::new());
            let namespace = metadata.route_namespace();
            let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
            let mut live = test_route(&key.0, &observer.runtime);
            live.version = RouteVersion(live_version);
            let (live_rank, marker_rank) = if expected { (1, 0) } else { (0, 1) };
            let routes = BTreeMap::from([
                (ranked[live_rank].runtime.stable_id.0.clone(), Some(live)),
                (ranked[marker_rank].runtime.stable_id.0.clone(), None),
            ]);
            let delete_versions = BTreeMap::from([
                (ranked[live_rank].runtime.stable_id.0.clone(), None),
                (
                    ranked[marker_rank].runtime.stable_id.0.clone(),
                    Some(RouteVersion(5)),
                ),
            ]);
            let queried_authorities = Arc::new(Mutex::new(Vec::new()));
            let directory = EmbeddedWrhRouteDirectory::new(
                2,
                metadata,
                &observer,
                Arc::new(RecordingConfirmationClient {
                    routes,
                    delete_versions,
                    read_failures: BTreeSet::new(),
                    queried_authorities: queried_authorities.clone(),
                }),
                Arc::new(TestMembership {
                    leases: candidates.clone(),
                }),
            );

            assert_eq!(
                directory.contains_object_route(&observer, &key).unwrap(),
                expected
            );
            assert_eq!(
                queried_authorities
                    .lock()
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                ranked
                    .iter()
                    .map(|lease| lease.runtime.stable_id.0.clone())
                    .collect::<BTreeSet<_>>()
            );
        }
    }

    #[test]
    fn presence_value_resolves_delete_conflicts_within_one_snapshot() {
        for (live_version, expected) in [(2, false), (5, false), (6, true)] {
            let value = value_from_presence(RouteAuthorityPresence {
                live_version: Some(RouteVersion(live_version)),
                delete_version: Some(RouteVersion(5)),
            })
            .expect("presence should contain a version");
            assert_eq!(value.version(), RouteVersion(if expected { 6 } else { 5 }));
            assert_eq!(matches!(value, ContainsVersion::Live(_)), expected);
        }
    }

    #[test]
    fn contains_presence_merges_versions_without_full_route_reads() {
        for (live_version, expected) in [(2, false), (5, false), (6, true)] {
            let observer = test_lease(&format!("contains-presence-observer-{live_version}"));
            let candidates = vec![
                test_lease(&format!("contains-presence-a-{live_version}")),
                test_lease(&format!("contains-presence-b-{live_version}")),
            ];
            let key = ObjectKey::new(format!("contains-presence-key-{live_version}"));
            let metadata = Arc::new(InMemoryMetadataBackend::new());
            let namespace = metadata.route_namespace();
            let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
            let mut live = test_route(&key.0, &observer.runtime);
            live.version = RouteVersion(live_version);
            let (live_rank, marker_rank) = if expected { (1, 0) } else { (0, 1) };
            let full_queries = Arc::new(Mutex::new(Vec::new()));
            let probe_queries = Arc::new(Mutex::new(Vec::new()));
            let presence_queries = Arc::new(Mutex::new(Vec::new()));
            let directory = EmbeddedWrhRouteDirectory::new(
                2,
                metadata,
                &observer,
                Arc::new(PresenceConfirmationClient {
                    inner: RecordingConfirmationClient {
                        routes: BTreeMap::from([
                            (ranked[live_rank].runtime.stable_id.0.clone(), Some(live)),
                            (ranked[marker_rank].runtime.stable_id.0.clone(), None),
                        ]),
                        delete_versions: BTreeMap::from([
                            (ranked[live_rank].runtime.stable_id.0.clone(), None),
                            (
                                ranked[marker_rank].runtime.stable_id.0.clone(),
                                Some(RouteVersion(5)),
                            ),
                        ]),
                        read_failures: BTreeSet::new(),
                        queried_authorities: full_queries.clone(),
                    },
                    presence_queries: presence_queries.clone(),
                    full_queries: None,
                    probe_queries: Some(probe_queries.clone()),
                    presence_misses: BTreeSet::new(),
                    filtered_presence: true,
                    item_unsupported: BTreeSet::new(),
                    item_failures: BTreeSet::new(),
                    malformed: false,
                    contains_present: true,
                    unsupported: BTreeSet::new(),
                }),
                Arc::new(TestMembership {
                    leases: candidates.clone(),
                }),
            );

            assert_eq!(
                directory.contains_object_route(&observer, &key).unwrap(),
                expected
            );
            assert!(full_queries.lock().is_empty());
            assert!(probe_queries.lock().is_empty());
            assert_eq!(presence_queries.lock().len(), 2);
            assert_eq!(
                presence_queries
                    .lock()
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                ranked
                    .iter()
                    .map(|lease| lease.runtime.stable_id.0.clone())
                    .collect::<BTreeSet<_>>()
            );
        }
    }

    #[test]
    fn filtered_presence_evicts_unreadable_and_preserves_cold_routes() {
        let observer = test_lease("filtered-presence-registry-observer");
        let authority = test_lease("filtered-presence-registry-authority");
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let hot_key = ObjectKey::new("filtered-presence-registry-hot");
        let cold_key = ObjectKey::new("filtered-presence-registry-cold");
        register_local_authority(
            &namespace,
            &authority.runtime.stable_id,
            DEFAULT_DELETE_MARKER_BUDGET_BYTES,
        );
        let mut hot = test_route(&hot_key.0, &observer.runtime);
        hot.version = RouteVersion(6);
        let mut cold = test_route(&cold_key.0, &observer.runtime);
        cold.version = RouteVersion(7);
        cold.replicas.clear();
        cold.cold_backing = Some(ColdBackingRoute {
            owner: authority.runtime.clone(),
            cold_tier_id: "filtered-presence-device".to_string(),
            object_locator: "filtered-presence-object".to_string(),
            length: 1,
            checksum: None,
            state: ColdBackingState::Materialized,
            replicas: Vec::new(),
        });
        crate::mesh::registry::authority_replace_many(
            &namespace,
            &authority.runtime.stable_id,
            &[
                RouteCasRequest {
                    key: hot_key.clone(),
                    expected: None,
                    next: Some(hot),
                },
                RouteCasRequest {
                    key: cold_key.clone(),
                    expected: None,
                    next: Some(cold),
                },
            ],
        )
        .unwrap();
        crate::update_readable_filter(&namespace, Some(BTreeSet::new()));

        let presence = authority_get_filtered_presence_many(
            &namespace,
            &authority.runtime.stable_id,
            &[hot_key.clone(), cold_key.clone()],
        )
        .unwrap();
        assert_eq!(
            presence,
            vec![
                RouteAuthorityPresence {
                    live_version: None,
                    delete_version: Some(RouteVersion(6)),
                },
                RouteAuthorityPresence {
                    live_version: Some(RouteVersion(7)),
                    delete_version: None,
                },
            ]
        );
        assert_eq!(
            authority_get_delete_marker(&namespace, &authority.runtime.stable_id, &hot_key),
            Some(RouteVersion(6))
        );

        crate::update_readable_filter(&namespace, None);
        unregister_local_authority(&namespace, &authority.runtime.stable_id);
    }

    #[test]
    fn contains_presence_honors_remote_filter_when_observer_filter_is_inactive() {
        let observer = test_lease("contains-presence-remote-filter-observer");
        let candidates = vec![
            test_lease("contains-presence-remote-filter-a"),
            test_lease("contains-presence-remote-filter-b"),
        ];
        let key = ObjectKey::new("contains-presence-remote-filter-key");
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let presence_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: ranked
                        .iter()
                        .map(|authority| {
                            (
                                authority.runtime.stable_id.0.clone(),
                                Some(test_route(&key.0, &observer.runtime)),
                            )
                        })
                        .collect(),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::new(),
                    queried_authorities: full_queries.clone(),
                },
                presence_queries: presence_queries.clone(),
                full_queries: None,
                probe_queries: None,
                presence_misses: BTreeSet::new(),
                filtered_presence: false,
                item_unsupported: BTreeSet::new(),
                item_failures: BTreeSet::new(),
                malformed: false,
                contains_present: false,
                unsupported: BTreeSet::new(),
            }),
            Arc::new(TestMembership {
                leases: candidates.clone(),
            }),
        );
        assert!(!crate::is_readable_filter_active(&directory.namespace));

        assert!(!directory.contains_object_route(&observer, &key).unwrap());
        assert!(full_queries.lock().is_empty());
        assert!(presence_queries.lock().is_empty());
    }

    #[test]
    fn contains_filtered_presence_skips_probe_for_mixed_batches() {
        for topk in [1, 2] {
            let observer = test_lease(&format!("contains-filtered-presence-observer-{topk}"));
            let candidates = (0..4)
                .map(|index| test_lease(&format!("contains-filtered-presence-{topk}-{index}")))
                .collect::<Vec<_>>();
            let keys = (0..16)
                .map(|index| {
                    ObjectKey::new(format!("contains-filtered-presence-key-{topk}-{index}"))
                })
                .collect::<Vec<_>>();
            let expected = keys
                .iter()
                .enumerate()
                .map(|(index, _)| index % 2 == 1)
                .collect::<Vec<_>>();
            let metadata = Arc::new(InMemoryMetadataBackend::new());
            let namespace = metadata.route_namespace();
            let ranked = keys
                .iter()
                .map(|key| ranked_top_authorities(&namespace, &candidates, key, topk))
                .collect::<Vec<_>>();
            let expected_queries = ranked
                .iter()
                .flatten()
                .map(|authority| authority.runtime.stable_id.0.clone())
                .collect::<BTreeSet<_>>();
            let presence_misses = candidates
                .iter()
                .flat_map(|authority| {
                    keys.iter().enumerate().filter_map(move |(index, key)| {
                        (index % 2 == 0)
                            .then_some((authority.runtime.stable_id.0.clone(), key.0.clone()))
                    })
                })
                .collect();
            let live = test_route("contains-filtered-presence-route", &observer.runtime);
            let full_queries = Arc::new(Mutex::new(Vec::new()));
            let probe_queries = Arc::new(Mutex::new(Vec::new()));
            let presence_queries = Arc::new(Mutex::new(Vec::new()));
            let directory = EmbeddedWrhRouteDirectory::new(
                topk,
                metadata,
                &observer,
                Arc::new(PresenceConfirmationClient {
                    inner: RecordingConfirmationClient {
                        routes: candidates
                            .iter()
                            .map(|authority| {
                                (authority.runtime.stable_id.0.clone(), Some(live.clone()))
                            })
                            .collect(),
                        delete_versions: BTreeMap::new(),
                        read_failures: BTreeSet::new(),
                        queried_authorities: Arc::new(Mutex::new(Vec::new())),
                    },
                    presence_queries: presence_queries.clone(),
                    full_queries: Some(full_queries.clone()),
                    probe_queries: Some(probe_queries.clone()),
                    presence_misses,
                    filtered_presence: true,
                    item_failures: BTreeSet::new(),
                    item_unsupported: BTreeSet::new(),
                    malformed: false,
                    contains_present: true,
                    unsupported: BTreeSet::new(),
                }),
                Arc::new(TestMembership { leases: candidates }),
            );

            assert_eq!(
                directory
                    .contains_object_routes_bounded(&observer, &keys)
                    .unwrap(),
                expected
            );
            assert!(probe_queries.lock().is_empty());
            assert!(full_queries.lock().is_empty());
            assert_eq!(presence_queries.lock().len(), expected_queries.len());
            assert_eq!(
                presence_queries
                    .lock()
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                expected_queries
            );
        }
    }

    #[test]
    fn contains_unconfirmed_filtered_presence_retains_probe_first() {
        let observer = test_lease("contains-unconfirmed-filtered-presence-observer");
        let candidates = vec![
            test_lease("contains-unconfirmed-filtered-presence-a"),
            test_lease("contains-unconfirmed-filtered-presence-b"),
        ];
        let key = ObjectKey::new("contains-unconfirmed-filtered-presence-key");
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let probe_queries = Arc::new(Mutex::new(Vec::new()));
        let presence_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: candidates
                        .iter()
                        .map(|authority| {
                            (
                                authority.runtime.stable_id.0.clone(),
                                Some(test_route(&key.0, &observer.runtime)),
                            )
                        })
                        .collect(),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::new(),
                    queried_authorities: Arc::new(Mutex::new(Vec::new())),
                },
                presence_queries: presence_queries.clone(),
                full_queries: Some(full_queries.clone()),
                probe_queries: Some(probe_queries.clone()),
                presence_misses: BTreeSet::new(),
                filtered_presence: false,
                item_failures: BTreeSet::new(),
                item_unsupported: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::new(),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        assert!(directory.contains_object_route(&observer, &key).unwrap());
        assert_eq!(probe_queries.lock().len(), 1);
        assert_eq!(presence_queries.lock().len(), 2);
        assert!(full_queries.lock().is_empty());
    }

    #[test]
    fn contains_capability_less_authorities_keep_probe_first() {
        let observer = test_lease("contains-capability-less-presence-observer");
        let mut candidates = vec![
            test_lease("contains-capability-less-presence-a"),
            test_lease("contains-capability-less-presence-b"),
        ];
        for authority in &mut candidates {
            authority
                .compatibility
                .capabilities
                .remove(ROUTE_FILTERED_PRESENCE_CAPABILITY);
        }
        let key = ObjectKey::new("contains-capability-less-presence-key");
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let probe_queries = Arc::new(Mutex::new(Vec::new()));
        let presence_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: candidates
                        .iter()
                        .map(|authority| {
                            (
                                authority.runtime.stable_id.0.clone(),
                                Some(test_route(&key.0, &observer.runtime)),
                            )
                        })
                        .collect(),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::new(),
                    queried_authorities: Arc::new(Mutex::new(Vec::new())),
                },
                presence_queries: presence_queries.clone(),
                full_queries: Some(full_queries.clone()),
                probe_queries: Some(probe_queries.clone()),
                presence_misses: BTreeSet::new(),
                filtered_presence: true,
                item_failures: BTreeSet::new(),
                item_unsupported: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::new(),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        assert!(directory.contains_object_route(&observer, &key).unwrap());
        assert_eq!(probe_queries.lock().len(), 1);
        assert_eq!(presence_queries.lock().len(), 2);
        assert!(full_queries.lock().is_empty());
    }

    #[test]
    fn contains_presence_empty_snapshot_is_a_successful_miss() {
        let observer = test_lease("contains-presence-empty-observer");
        let candidates = vec![
            test_lease("contains-presence-empty-a"),
            test_lease("contains-presence-empty-b"),
        ];
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let presence_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: BTreeMap::new(),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::new(),
                    queried_authorities: full_queries.clone(),
                },
                presence_queries: presence_queries.clone(),
                full_queries: None,
                probe_queries: None,
                presence_misses: BTreeSet::new(),
                filtered_presence: false,
                item_unsupported: BTreeSet::new(),
                item_failures: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::new(),
            }),
            Arc::new(TestMembership {
                leases: candidates.clone(),
            }),
        );

        assert!(!directory
            .contains_object_route(&observer, &ObjectKey::new("contains-presence-empty-key"))
            .unwrap());
        assert!(full_queries.lock().is_empty());
        assert_eq!(presence_queries.lock().len(), 2);
    }

    #[test]
    fn contains_presence_rejects_incomplete_observations_without_full_route_reads() {
        let observer = test_lease("contains-presence-failure-observer");
        let candidates = vec![
            test_lease("contains-presence-failure-a"),
            test_lease("contains-presence-failure-b"),
        ];
        let key = ObjectKey::new("contains-presence-failure-key");
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let probe_queries = Arc::new(Mutex::new(Vec::new()));
        let presence_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: BTreeMap::from([(
                        ranked[0].runtime.stable_id.0.clone(),
                        Some(test_route(&key.0, &observer.runtime)),
                    )]),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::from([ranked[1].runtime.stable_id.0.clone()]),
                    queried_authorities: full_queries.clone(),
                },
                presence_queries: presence_queries.clone(),
                full_queries: None,
                probe_queries: Some(probe_queries.clone()),
                presence_misses: BTreeSet::new(),
                filtered_presence: true,
                item_unsupported: BTreeSet::new(),
                item_failures: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::new(),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        let error = directory
            .contains_object_route(&observer, &key)
            .expect_err("a live route must not hide an unavailable authority");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(full_queries.lock().is_empty());
        assert!(probe_queries.lock().is_empty());
        assert_eq!(presence_queries.lock().len(), 2);
    }

    #[test]
    fn contains_presence_rejects_item_errors_and_length_mismatches() {
        for (item_failure, malformed) in [(true, false), (false, true)] {
            let observer = test_lease(&format!(
                "contains-presence-item-error-observer-{item_failure}-{malformed}"
            ));
            let candidates = vec![
                test_lease(&format!(
                    "contains-presence-item-error-a-{item_failure}-{malformed}"
                )),
                test_lease(&format!(
                    "contains-presence-item-error-b-{item_failure}-{malformed}"
                )),
            ];
            let key = ObjectKey::new("contains-presence-item-error-key");
            let metadata = Arc::new(InMemoryMetadataBackend::new());
            let namespace = metadata.route_namespace();
            let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
            let full_queries = Arc::new(Mutex::new(Vec::new()));
            let probe_queries = Arc::new(Mutex::new(Vec::new()));
            let presence_queries = Arc::new(Mutex::new(Vec::new()));
            let directory = EmbeddedWrhRouteDirectory::new(
                2,
                metadata,
                &observer,
                Arc::new(PresenceConfirmationClient {
                    inner: RecordingConfirmationClient {
                        routes: BTreeMap::from([(
                            ranked[0].runtime.stable_id.0.clone(),
                            Some(test_route(&key.0, &observer.runtime)),
                        )]),
                        delete_versions: BTreeMap::new(),
                        read_failures: BTreeSet::new(),
                        queried_authorities: full_queries.clone(),
                    },
                    presence_queries: presence_queries.clone(),
                    full_queries: None,
                    probe_queries: Some(probe_queries.clone()),
                    presence_misses: BTreeSet::new(),
                    filtered_presence: true,
                    item_unsupported: BTreeSet::new(),
                    item_failures: if item_failure {
                        BTreeSet::from([ranked[1].runtime.stable_id.0.clone()])
                    } else {
                        BTreeSet::new()
                    },
                    malformed,
                    contains_present: true,
                    unsupported: BTreeSet::new(),
                }),
                Arc::new(TestMembership { leases: candidates }),
            );

            let error = directory
                .contains_object_route(&observer, &key)
                .expect_err("invalid presence observations must fail closed");
            assert!(matches!(error, StoreError::Transport(_)));
            assert!(full_queries.lock().is_empty());
            assert!(probe_queries.lock().is_empty());
            assert_eq!(presence_queries.lock().len(), 2);
        }
    }

    #[test]
    fn contains_presence_error_identifies_real_cell_in_variable_length_rows() {
        let observer = test_lease("contains-presence-variable-rows-observer");
        let candidates = vec![
            test_lease("contains-presence-variable-rows-a"),
            test_lease("contains-presence-variable-rows-b"),
        ];
        let keys = vec![
            ObjectKey::new("contains-presence-variable-rows-first"),
            ObjectKey::new("contains-presence-variable-rows-second"),
        ];
        let ranked = vec![
            vec![candidates[0].clone()],
            vec![candidates[0].clone(), candidates[1].clone()],
        ];
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: BTreeMap::new(),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::new(),
                    queried_authorities: Arc::new(Mutex::new(Vec::new())),
                },
                presence_queries: Arc::new(Mutex::new(Vec::new())),
                full_queries: None,
                probe_queries: None,
                presence_misses: BTreeSet::new(),
                filtered_presence: false,
                item_failures: BTreeSet::from([candidates[1].runtime.stable_id.0.clone()]),
                item_unsupported: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::new(),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        let error = directory
            .validate_contains_object_routes_presence(&keys, &ranked, false)
            .expect_err("the failed presence item should fail closed");
        assert!(error.to_string().contains(&keys[1].0));
        assert!(!error.to_string().contains(&keys[0].0));
    }

    #[test]
    fn contains_mixed_filtered_and_legacy_presence_reads_only_legacy() {
        let observer = test_lease("contains-presence-legacy-observer");
        let candidates = vec![
            test_lease("contains-presence-legacy-a"),
            test_lease("contains-presence-legacy-b"),
        ];
        let key = ObjectKey::new("contains-presence-legacy-key");
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
        let mut live = test_route(&key.0, &observer.runtime);
        live.version = RouteVersion(6);
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let probe_queries = Arc::new(Mutex::new(Vec::new()));
        let presence_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: BTreeMap::from([
                        (ranked[0].runtime.stable_id.0.clone(), Some(live)),
                        (ranked[1].runtime.stable_id.0.clone(), None),
                    ]),
                    delete_versions: BTreeMap::from([(
                        ranked[1].runtime.stable_id.0.clone(),
                        Some(RouteVersion(5)),
                    )]),
                    read_failures: BTreeSet::new(),
                    queried_authorities: Arc::new(Mutex::new(Vec::new())),
                },
                presence_queries: presence_queries.clone(),
                full_queries: Some(full_queries.clone()),
                probe_queries: Some(probe_queries.clone()),
                presence_misses: BTreeSet::new(),
                filtered_presence: true,
                item_unsupported: BTreeSet::new(),
                item_failures: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::from([ranked[0].runtime.stable_id.0.clone()]),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        assert!(directory.contains_object_route(&observer, &key).unwrap());
        assert_eq!(probe_queries.lock().len(), 1);
        assert_eq!(presence_queries.lock().len(), 4);
        assert_eq!(
            full_queries.lock().as_slice(),
            &[(ranked[0].runtime.stable_id.0.clone(), vec![key])]
        );
    }

    #[test]
    fn contains_presence_item_unsupported_reads_only_that_key() {
        let observer = test_lease("contains-presence-item-legacy-observer");
        let candidates = (0..4)
            .map(|index| test_lease(&format!("contains-presence-item-legacy-{index}")))
            .collect::<Vec<_>>();
        let keys = (0..16)
            .map(|index| ObjectKey::new(format!("contains-presence-item-legacy-key-{index}")))
            .collect::<Vec<_>>();
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let ranked = keys
            .iter()
            .map(|key| ranked_top_authorities(&namespace, &candidates, key, 2))
            .collect::<Vec<_>>();
        let (legacy_id, legacy_keys) = candidates
            .iter()
            .map(|authority| {
                let assigned = keys
                    .iter()
                    .zip(&ranked)
                    .filter(|(_, authorities)| {
                        authorities.iter().any(|selected| {
                            selected.runtime.stable_id == authority.runtime.stable_id
                        })
                    })
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                (authority.runtime.stable_id.0.clone(), assigned)
            })
            .max_by_key(|(_, assigned)| assigned.len())
            .expect("an authority should be selected");
        assert!(legacy_keys.len() >= 2);
        let unsupported_key = legacy_keys[0].clone();
        let mut live = test_route("contains-presence-item-legacy-route", &observer.runtime);
        live.version = RouteVersion(6);
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: candidates
                        .iter()
                        .map(|authority| {
                            (authority.runtime.stable_id.0.clone(), Some(live.clone()))
                        })
                        .collect(),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::new(),
                    queried_authorities: Arc::new(Mutex::new(Vec::new())),
                },
                presence_queries: Arc::new(Mutex::new(Vec::new())),
                full_queries: Some(full_queries.clone()),
                probe_queries: None,
                presence_misses: BTreeSet::new(),
                filtered_presence: false,
                item_unsupported: BTreeSet::from([(legacy_id.clone(), unsupported_key.0.clone())]),
                item_failures: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::new(),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        assert_eq!(
            directory
                .contains_object_routes_bounded(&observer, &keys)
                .unwrap(),
            vec![true; keys.len()]
        );
        assert_eq!(
            full_queries.lock().as_slice(),
            &[(legacy_id, vec![unsupported_key])]
        );
    }

    #[test]
    fn contains_presence_legacy_fallback_groups_keys_per_authority() {
        let observer = test_lease("contains-presence-legacy-grouping-observer");
        let candidates = (0..4)
            .map(|index| test_lease(&format!("contains-presence-legacy-grouping-{index}")))
            .collect::<Vec<_>>();
        let keys = (0..64)
            .map(|index| ObjectKey::new(format!("contains-presence-legacy-grouping-key-{index}")))
            .collect::<Vec<_>>();
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let ranked = keys
            .iter()
            .map(|key| ranked_top_authorities(&namespace, &candidates, key, 2))
            .collect::<Vec<_>>();
        let live_legacy = ranked[0][0].clone();
        let delete_legacy = ranked[0][1].clone();
        let expected_fallback_keys = |authority: &ClientLease| {
            keys.iter()
                .zip(&ranked)
                .filter(|(_, authorities)| {
                    authorities
                        .iter()
                        .any(|selected| selected.runtime.stable_id == authority.runtime.stable_id)
                })
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>()
        };
        let live_fallback_keys = expected_fallback_keys(&live_legacy);
        let delete_fallback_keys = expected_fallback_keys(&delete_legacy);
        assert!(live_fallback_keys.len() > 1);
        assert!(delete_fallback_keys.len() > 1);
        let expected = ranked
            .iter()
            .map(|authorities| {
                authorities
                    .iter()
                    .any(|authority| authority.runtime.stable_id == live_legacy.runtime.stable_id)
            })
            .collect::<Vec<_>>();
        assert!(expected.iter().any(|present| *present));
        assert!(expected.iter().any(|present| !*present));
        let mut live = test_route("contains-presence-legacy-grouping-route", &observer.runtime);
        live.version = RouteVersion(6);
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: BTreeMap::from([(live_legacy.runtime.stable_id.0.clone(), Some(live))]),
                    delete_versions: BTreeMap::from([(
                        delete_legacy.runtime.stable_id.0.clone(),
                        Some(RouteVersion(5)),
                    )]),
                    read_failures: BTreeSet::new(),
                    queried_authorities: Arc::new(Mutex::new(Vec::new())),
                },
                presence_queries: Arc::new(Mutex::new(Vec::new())),
                full_queries: Some(full_queries.clone()),
                item_unsupported: BTreeSet::new(),
                item_failures: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::from([
                    live_legacy.runtime.stable_id.0.clone(),
                    delete_legacy.runtime.stable_id.0.clone(),
                ]),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        assert_eq!(
            directory
                .contains_object_routes_bounded(&observer, &keys)
                .unwrap(),
            expected
        );
        let queries = full_queries.lock();
        assert_eq!(queries.len(), 2);
        let queries = queries.iter().cloned().collect::<BTreeMap<_, _>>();
        assert_eq!(queries.len(), 2);
        assert_eq!(
            queries.get(&live_legacy.runtime.stable_id.0),
            Some(&live_fallback_keys)
        );
        assert_eq!(
            queries.get(&delete_legacy.runtime.stable_id.0),
            Some(&delete_fallback_keys)
        );
    }

    #[test]
    fn contains_presence_legacy_fallback_merges_delete_versions() {
        for (live_version, expected) in [(2, false), (5, false), (6, true)] {
            let observer = test_lease(&format!(
                "contains-presence-legacy-version-observer-{live_version}"
            ));
            let candidates = vec![
                test_lease(&format!(
                    "contains-presence-legacy-version-a-{live_version}"
                )),
                test_lease(&format!(
                    "contains-presence-legacy-version-b-{live_version}"
                )),
            ];
            let key = ObjectKey::new(format!(
                "contains-presence-legacy-version-key-{live_version}"
            ));
            let metadata = Arc::new(InMemoryMetadataBackend::new());
            let namespace = metadata.route_namespace();
            let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
            let legacy = &ranked[0];
            let modern = &ranked[1];
            let mut live = test_route(&key.0, &observer.runtime);
            live.version = RouteVersion(live_version);
            let full_queries = Arc::new(Mutex::new(Vec::new()));
            let directory = EmbeddedWrhRouteDirectory::new(
                2,
                metadata,
                &observer,
                Arc::new(PresenceConfirmationClient {
                    inner: RecordingConfirmationClient {
                        routes: BTreeMap::from([
                            (legacy.runtime.stable_id.0.clone(), None),
                            (modern.runtime.stable_id.0.clone(), Some(live)),
                        ]),
                        delete_versions: BTreeMap::from([(
                            legacy.runtime.stable_id.0.clone(),
                            Some(RouteVersion(5)),
                        )]),
                        read_failures: BTreeSet::new(),
                        queried_authorities: Arc::new(Mutex::new(Vec::new())),
                    },
                    presence_queries: Arc::new(Mutex::new(Vec::new())),
                    full_queries: Some(full_queries.clone()),
                    probe_queries: None,
                    presence_misses: BTreeSet::new(),
                    filtered_presence: false,
                    item_unsupported: BTreeSet::new(),
                    item_failures: BTreeSet::new(),
                    malformed: false,
                    contains_present: true,
                    unsupported: BTreeSet::from([legacy.runtime.stable_id.0.clone()]),
                }),
                Arc::new(TestMembership { leases: candidates }),
            );

            assert_eq!(
                directory.contains_object_route(&observer, &key).unwrap(),
                expected
            );
            assert_eq!(
                full_queries.lock().as_slice(),
                &[(legacy.runtime.stable_id.0.clone(), vec![key])]
            );
        }
    }

    #[test]
    fn contains_presence_mixed_compatibility_and_errors_use_complete_fallback() {
        let observer = test_lease("contains-presence-mixed-fallback-observer");
        let candidates = vec![
            test_lease("contains-presence-mixed-fallback-a"),
            test_lease("contains-presence-mixed-fallback-b"),
        ];
        let key = ObjectKey::new("contains-presence-mixed-fallback-key");
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
        let legacy = &ranked[0];
        let failed = &ranked[1];
        let live = test_route(&key.0, &observer.runtime);
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: ranked
                        .iter()
                        .map(|authority| {
                            (authority.runtime.stable_id.0.clone(), Some(live.clone()))
                        })
                        .collect(),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::new(),
                    queried_authorities: Arc::new(Mutex::new(Vec::new())),
                },
                presence_queries: Arc::new(Mutex::new(Vec::new())),
                full_queries: Some(full_queries.clone()),
                probe_queries: None,
                presence_misses: BTreeSet::new(),
                filtered_presence: false,
                item_unsupported: BTreeSet::new(),
                item_failures: BTreeSet::from([failed.runtime.stable_id.0.clone()]),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::from([legacy.runtime.stable_id.0.clone()]),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        assert!(directory.contains_object_route(&observer, &key).unwrap());
        let full_queries = full_queries.lock();
        assert_eq!(full_queries.len(), 2);
        assert_eq!(
            full_queries
                .iter()
                .map(|(authority, _)| authority.clone())
                .collect::<BTreeSet<_>>(),
            ranked
                .iter()
                .map(|authority| authority.runtime.stable_id.0.clone())
                .collect::<BTreeSet<_>>()
        );
        assert!(full_queries
            .iter()
            .all(|(_, keys)| keys.as_slice() == std::slice::from_ref(&key)));
    }

    #[test]
    fn contains_presence_legacy_read_failure_fails_closed() {
        let observer = test_lease("contains-presence-legacy-read-error-observer");
        let candidates = vec![
            test_lease("contains-presence-legacy-read-error-a"),
            test_lease("contains-presence-legacy-read-error-b"),
        ];
        let key = ObjectKey::new("contains-presence-legacy-read-error-key");
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
        let legacy = &ranked[0];
        let modern = &ranked[1];
        let full_queries = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(PresenceConfirmationClient {
                inner: RecordingConfirmationClient {
                    routes: BTreeMap::from([(
                        modern.runtime.stable_id.0.clone(),
                        Some(test_route(&key.0, &observer.runtime)),
                    )]),
                    delete_versions: BTreeMap::new(),
                    read_failures: BTreeSet::from([legacy.runtime.stable_id.0.clone()]),
                    queried_authorities: Arc::new(Mutex::new(Vec::new())),
                },
                presence_queries: Arc::new(Mutex::new(Vec::new())),
                full_queries: Some(full_queries.clone()),
                probe_queries: None,
                presence_misses: BTreeSet::new(),
                filtered_presence: false,
                item_unsupported: BTreeSet::new(),
                item_failures: BTreeSet::new(),
                malformed: false,
                contains_present: true,
                unsupported: BTreeSet::from([legacy.runtime.stable_id.0.clone()]),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        let error = directory
            .contains_object_route(&observer, &key)
            .expect_err("a failed legacy full read must fail closed");
        assert!(matches!(error, StoreError::Transport(_)));
        assert_eq!(
            full_queries.lock().as_slice(),
            &[(legacy.runtime.stable_id.0.clone(), vec![key])]
        );
    }

    #[test]
    fn contains_without_authorities_returns_false() {
        let observer = test_lease("contains-no-authorities-observer");
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership { leases: Vec::new() }),
        );
        let keys = vec![
            ObjectKey::new("contains-no-authorities-a"),
            ObjectKey::new("contains-no-authorities-b"),
        ];

        assert_eq!(
            directory
                .contains_object_routes_bounded(&observer, &keys)
                .unwrap(),
            vec![false; keys.len()]
        );
    }

    #[test]
    fn contains_probe_first_preserves_hit_and_miss_behavior() {
        for present in [true, false] {
            for key_count in [1, 16] {
                let observer = test_lease(&format!(
                    "contains-single-pass-observer-{present}-{key_count}"
                ));
                let candidates = (0..4)
                    .map(|index| {
                        test_lease(&format!(
                            "contains-single-pass-authority-{present}-{key_count}-{index}"
                        ))
                    })
                    .collect::<Vec<_>>();
                let metadata = Arc::new(InMemoryMetadataBackend::new());
                let namespace = metadata.route_namespace();
                let keys = (0..key_count)
                    .map(|index| {
                        ObjectKey::new(format!(
                            "contains-single-pass-key-{present}-{key_count}-{index}"
                        ))
                    })
                    .collect::<Vec<_>>();
                let ranked = keys
                    .iter()
                    .map(|key| ranked_top_authorities(&namespace, &candidates, key, 2))
                    .collect::<Vec<_>>();
                let expected_record_queries = ranked
                    .iter()
                    .flatten()
                    .map(|authority| authority.runtime.stable_id.0.clone())
                    .collect::<BTreeSet<_>>()
                    .len();
                let route = test_route("contains-single-pass-route", &observer.runtime);
                let routes = candidates
                    .iter()
                    .map(|authority| {
                        (
                            authority.runtime.stable_id.0.clone(),
                            present.then(|| route.clone()),
                        )
                    })
                    .collect();
                let queried_authorities = Arc::new(Mutex::new(Vec::new()));
                let directory = EmbeddedWrhRouteDirectory::new(
                    2,
                    metadata,
                    &observer,
                    Arc::new(RecordingConfirmationClient {
                        routes,
                        delete_versions: BTreeMap::new(),
                        read_failures: BTreeSet::new(),
                        queried_authorities: queried_authorities.clone(),
                    }),
                    Arc::new(TestMembership { leases: candidates }),
                );

                assert_eq!(
                    directory
                        .contains_object_routes_bounded(&observer, &keys)
                        .unwrap(),
                    vec![present; key_count]
                );
                if present {
                    assert_eq!(queried_authorities.lock().len(), expected_record_queries);
                } else {
                    assert!(queried_authorities.lock().is_empty());
                }
            }
        }
    }

    #[test]
    fn full_route_reads_keep_remote_short_circuit() {
        let observer = test_lease("full-route-short-circuit-observer");
        let candidates = vec![
            test_lease("full-route-short-circuit-a"),
            test_lease("full-route-short-circuit-b"),
        ];
        let key = ObjectKey::new("full-route-short-circuit-key");
        let route = test_route(&key.0, &observer.runtime);
        let queried_authorities = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes: candidates
                    .iter()
                    .map(|authority| (authority.runtime.stable_id.0.clone(), Some(route.clone())))
                    .collect(),
                delete_versions: BTreeMap::new(),
                read_failures: BTreeSet::new(),
                queried_authorities: queried_authorities.clone(),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        assert_eq!(
            directory.get_object_route(&observer, &key).unwrap(),
            Some(route)
        );
        assert_eq!(queried_authorities.lock().len(), 1);
    }

    #[test]
    fn contains_validation_rejects_incomplete_live_route() {
        let observer = test_lease("contains-incomplete-live-observer");
        let candidates = vec![
            test_lease("contains-incomplete-live-a"),
            test_lease("contains-incomplete-live-b"),
        ];
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let key = ObjectKey::new("contains-incomplete-live-key");
        let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
        let live = &ranked[0];
        let failed = &ranked[1];
        let mut route = test_route(&key.0, &observer.runtime);
        route.version = RouteVersion(2);
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes: BTreeMap::from([
                    (live.runtime.stable_id.0.clone(), Some(route)),
                    (failed.runtime.stable_id.0.clone(), None),
                ]),
                delete_versions: BTreeMap::from([(
                    failed.runtime.stable_id.0.clone(),
                    Some(RouteVersion(5)),
                )]),
                read_failures: BTreeSet::from([failed.runtime.stable_id.0.clone()]),
                queried_authorities: Arc::new(Mutex::new(Vec::new())),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        let error = directory
            .contains_object_route(&observer, &key)
            .expect_err("a stale live route must not hide a failed delete-marker read");
        assert!(matches!(error, StoreError::Transport(_)));
    }

    #[test]
    fn readable_filter_contains_rejects_partial_probe_failure() {
        let observer = test_lease("contains-readable-filter-error-observer");
        let candidates = vec![
            test_lease("contains-readable-filter-error-a"),
            test_lease("contains-readable-filter-error-b"),
        ];
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let key = ObjectKey::new("contains-readable-filter-error-key");
        let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
        let failed = &ranked[0];
        let missing = &ranked[1];
        let route = test_route(&key.0, &observer.runtime);
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes: BTreeMap::from([
                    (failed.runtime.stable_id.0.clone(), Some(route)),
                    (missing.runtime.stable_id.0.clone(), None),
                ]),
                delete_versions: BTreeMap::new(),
                read_failures: BTreeSet::from([failed.runtime.stable_id.0.clone()]),
                queried_authorities: Arc::new(Mutex::new(Vec::new())),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        let error = directory
            .contains_object_routes_with_readable_filter(
                &observer,
                std::slice::from_ref(&key),
                std::slice::from_ref(&ranked),
                None,
            )
            .expect_err("a peer miss must not hide a failed readable-filter probe");
        assert!(matches!(error, StoreError::Transport(_)));
    }

    #[test]
    fn contains_validation_reads_baseline_when_current_authorities_hit() {
        let observer = test_lease("contains-current-hit-baseline-observer");
        let original = test_lease("contains-current-hit-baseline-original");
        let current = std::iter::once(original.clone())
            .chain(
                (1..3).map(|index| test_lease(&format!("contains-current-hit-baseline-{index}"))),
            )
            .collect::<Vec<_>>();
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let key = (0..100)
            .map(|index| ObjectKey::new(format!("contains-current-hit-baseline-key-{index}")))
            .find(|key| {
                ranked_top_authorities(&namespace, &current, key, 2)
                    .iter()
                    .all(|authority| authority.runtime.stable_id != original.runtime.stable_id)
            })
            .expect("a key should exclude the original authority from current top-k");
        let current_top = ranked_top_authorities(&namespace, &current, &key, 2);
        let mut route = test_route(&key.0, &observer.runtime);
        route.version = RouteVersion(2);
        let mut routes = current_top
            .iter()
            .map(|authority| (authority.runtime.stable_id.0.clone(), Some(route.clone())))
            .collect::<BTreeMap<_, _>>();
        routes.insert(original.runtime.stable_id.0.clone(), None);
        let membership = Arc::new(MutableMembership {
            leases: Mutex::new(vec![original.clone()]),
        });
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes,
                delete_versions: BTreeMap::from([(
                    original.runtime.stable_id.0.clone(),
                    Some(RouteVersion(5)),
                )]),
                read_failures: BTreeSet::new(),
                queried_authorities: Arc::new(Mutex::new(Vec::new())),
            }),
            membership.clone(),
        );
        *membership.leases.lock() = current;

        assert!(!directory.contains_object_route(&observer, &key).unwrap());

        let baseline_ranked = ranked_top_authorities(
            &directory.namespace,
            std::slice::from_ref(&original),
            &key,
            2,
        );
        assert_eq!(
            directory
                .contains_object_routes_with_readable_filter(
                    &observer,
                    std::slice::from_ref(&key),
                    std::slice::from_ref(&current_top),
                    Some(std::slice::from_ref(&baseline_ranked)),
                )
                .unwrap(),
            vec![false]
        );
    }

    #[test]
    fn contains_validation_propagates_route_read_failures() {
        let observer = test_lease("contains-read-error-observer");
        let authority = test_lease("contains-read-error-authority");
        let authority_id = authority.runtime.stable_id.0.clone();
        let key = ObjectKey::new("contains-read-error-key");
        let route = test_route(&key.0, &observer.runtime);
        let directory = EmbeddedWrhRouteDirectory::new(
            1,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes: BTreeMap::from([(authority_id.clone(), Some(route))]),
                delete_versions: BTreeMap::new(),
                read_failures: BTreeSet::from([authority_id]),
                queried_authorities: Arc::new(Mutex::new(Vec::new())),
            }),
            Arc::new(TestMembership {
                leases: vec![authority],
            }),
        );

        let error = directory
            .contains_object_route(&observer, &key)
            .expect_err("a raw hit without a successful route read must not prove absence");
        assert!(matches!(error, StoreError::Transport(_)));
    }

    #[test]
    fn contains_validation_rejects_partial_current_absence() {
        let observer = test_lease("contains-partial-current-observer");
        let candidates = vec![
            test_lease("contains-partial-current-a"),
            test_lease("contains-partial-current-b"),
        ];
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let key = ObjectKey::new("contains-partial-current-key");
        let ranked = ranked_top_authorities(&namespace, &candidates, &key, 2);
        let failed = &ranked[0];
        let missing = &ranked[1];
        let route = test_route(&key.0, &observer.runtime);
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes: BTreeMap::from([
                    (failed.runtime.stable_id.0.clone(), Some(route)),
                    (missing.runtime.stable_id.0.clone(), None),
                ]),
                delete_versions: BTreeMap::new(),
                read_failures: BTreeSet::from([failed.runtime.stable_id.0.clone()]),
                queried_authorities: Arc::new(Mutex::new(Vec::new())),
            }),
            Arc::new(TestMembership { leases: candidates }),
        );

        let error = directory
            .contains_object_route(&observer, &key)
            .expect_err("a peer miss must not hide a failed raw-positive authority read");
        assert!(matches!(error, StoreError::Transport(_)));
    }

    #[test]
    fn contains_validation_rejects_partial_baseline_delete_marker() {
        let observer = test_lease("contains-partial-baseline-observer");
        let original = test_lease("contains-partial-baseline-original");
        let current = std::iter::once(original.clone())
            .chain((1..3).map(|index| test_lease(&format!("contains-partial-baseline-{index}"))))
            .collect::<Vec<_>>();
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let key = (0..100)
            .map(|index| ObjectKey::new(format!("contains-partial-baseline-key-{index}")))
            .find(|key| {
                ranked_authorities(&namespace, &current, key)
                    .last()
                    .is_some_and(|authority| {
                        authority.runtime.stable_id == original.runtime.stable_id
                    })
            })
            .expect("a key should rank the original authority last");
        let current_top = ranked_top_authorities(&namespace, &current, &key, 2);
        let route = test_route(&key.0, &observer.runtime);
        let membership = Arc::new(MutableMembership {
            leases: Mutex::new(vec![original.clone()]),
        });
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes: BTreeMap::from([(original.runtime.stable_id.0.clone(), Some(route))]),
                delete_versions: BTreeMap::from([(
                    current_top[0].runtime.stable_id.0.clone(),
                    Some(RouteVersion(5)),
                )]),
                read_failures: BTreeSet::from([original.runtime.stable_id.0.clone()]),
                queried_authorities: Arc::new(Mutex::new(Vec::new())),
            }),
            membership.clone(),
        );
        *membership.leases.lock() = current;

        let error = directory
            .contains_object_route(&observer, &key)
            .expect_err("a delete marker must not hide a failed baseline authority read");
        assert!(matches!(error, StoreError::Transport(_)));
    }

    #[test]
    fn contains_validation_stays_bounded_during_baseline_fallback() {
        let observer = test_lease("contains-growth-observer");
        let original = test_lease("contains-growth-original");
        let current = std::iter::once(original.clone())
            .chain((1..32).map(|index| test_lease(&format!("contains-growth-{index}"))))
            .collect::<Vec<_>>();
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let namespace = metadata.route_namespace();
        let key = (0..1000)
            .map(|index| ObjectKey::new(format!("contains-growth-key-{index}")))
            .find(|key| {
                ranked_authorities(&namespace, &current, key)
                    .last()
                    .is_some_and(|authority| {
                        authority.runtime.stable_id == original.runtime.stable_id
                    })
            })
            .expect("a key should rank the original authority last");
        let current_top = ranked_top_authorities(&namespace, &current, &key, 2);
        let route = test_route(&key.0, &observer.runtime);
        let membership = Arc::new(MutableMembership {
            leases: Mutex::new(vec![original.clone()]),
        });
        let queried_authorities = Arc::new(Mutex::new(Vec::new()));
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(RecordingConfirmationClient {
                routes: BTreeMap::from([(original.runtime.stable_id.0.clone(), Some(route))]),
                delete_versions: BTreeMap::new(),
                read_failures: BTreeSet::new(),
                queried_authorities: queried_authorities.clone(),
            }),
            membership.clone(),
        );
        *membership.leases.lock() = current;

        assert!(directory.contains_object_route(&observer, &key).unwrap());

        let expected_queries = current_top
            .iter()
            .chain(std::iter::once(&original))
            .map(|authority| authority.runtime.stable_id.0.clone())
            .collect::<BTreeSet<_>>();
        let queried = queried_authorities.lock();
        assert_eq!(queried.len(), expected_queries.len());
        assert_eq!(
            queried.iter().cloned().collect::<BTreeSet<_>>(),
            expected_queries
        );
    }

    #[test]
    fn contains_presence_keeps_baseline_growth_bounded() {
        for (live_version, expected, legacy_baseline) in [
            (2, false, false),
            (6, true, false),
            (2, false, true),
            (6, true, true),
        ] {
            let observer = test_lease(&format!(
                "contains-presence-growth-observer-{live_version}-{legacy_baseline}"
            ));
            let original = test_lease(&format!(
                "contains-presence-growth-original-{live_version}-{legacy_baseline}"
            ));
            let current = std::iter::once(original.clone())
                .chain((1..32).map(|index| {
                    test_lease(&format!(
                        "contains-presence-growth-{live_version}-{legacy_baseline}-{index}"
                    ))
                }))
                .collect::<Vec<_>>();
            let metadata = Arc::new(InMemoryMetadataBackend::new());
            let namespace = metadata.route_namespace();
            let key = (0..1000)
                .map(|index| {
                    ObjectKey::new(format!(
                        "contains-presence-growth-key-{live_version}-{legacy_baseline}-{index}"
                    ))
                })
                .find(|key| {
                    ranked_authorities(&namespace, &current, key)
                        .last()
                        .is_some_and(|authority| {
                            authority.runtime.stable_id == original.runtime.stable_id
                        })
                })
                .expect("a key should rank the original authority last");
            let current_top = ranked_top_authorities(&namespace, &current, &key, 2);
            let mut live = test_route(&key.0, &observer.runtime);
            live.version = RouteVersion(live_version);
            let membership = Arc::new(MutableMembership {
                leases: Mutex::new(vec![original.clone()]),
            });
            let full_queries = Arc::new(Mutex::new(Vec::new()));
            let probe_queries = Arc::new(Mutex::new(Vec::new()));
            let presence_queries = Arc::new(Mutex::new(Vec::new()));
            let directory = EmbeddedWrhRouteDirectory::new(
                2,
                metadata,
                &observer,
                Arc::new(PresenceConfirmationClient {
                    inner: RecordingConfirmationClient {
                        routes: BTreeMap::from([
                            (current_top[0].runtime.stable_id.0.clone(), Some(live)),
                            (original.runtime.stable_id.0.clone(), None),
                        ]),
                        delete_versions: BTreeMap::from([(
                            original.runtime.stable_id.0.clone(),
                            Some(RouteVersion(5)),
                        )]),
                        read_failures: BTreeSet::new(),
                        queried_authorities: Arc::new(Mutex::new(Vec::new())),
                    },
                    presence_queries: presence_queries.clone(),
                    full_queries: Some(full_queries.clone()),
                    probe_queries: Some(probe_queries.clone()),
                    presence_misses: BTreeSet::new(),
                    filtered_presence: !legacy_baseline,
                    item_unsupported: BTreeSet::new(),
                    item_failures: BTreeSet::new(),
                    malformed: false,
                    contains_present: true,
                    unsupported: if legacy_baseline {
                        BTreeSet::from([original.runtime.stable_id.0.clone()])
                    } else {
                        BTreeSet::new()
                    },
                }),
                membership.clone(),
            );
            *membership.leases.lock() = current;

            assert_eq!(
                directory.contains_object_route(&observer, &key).unwrap(),
                expected
            );
            if legacy_baseline {
                assert_eq!(
                    full_queries.lock().as_slice(),
                    &[(original.runtime.stable_id.0.clone(), vec![key.clone()])]
                );
                assert!(!probe_queries.lock().is_empty());
            } else {
                assert!(full_queries.lock().is_empty());
                assert!(probe_queries.lock().is_empty());
            }
            let expected_queries = current_top
                .iter()
                .chain(std::iter::once(&original))
                .map(|authority| authority.runtime.stable_id.0.clone())
                .collect::<BTreeSet<_>>();
            assert_eq!(presence_queries.lock().len(), expected_queries.len());
            assert_eq!(
                presence_queries
                    .lock()
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                expected_queries
            );
        }
    }

    #[test]
    fn delete_marker_queries_merge_highest_authority_version() {
        let observer = test_lease("observer-delete-marker-merge");
        let peer = test_lease("peer-delete-marker-merge");
        let candidates = vec![observer.clone(), peer.clone()];
        let key = ObjectKey::new("delete-marker-merge-key".to_string());
        let metadata = Arc::new(InMemoryMetadataBackend::new());
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            metadata,
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: candidates.clone(),
            }),
        );
        register_local_authority(
            &directory.namespace,
            &peer.runtime.stable_id,
            DEFAULT_DELETE_MARKER_BUDGET_BYTES,
        );

        let ranked = ranked_top_authorities(&directory.namespace, &candidates, &key, 2);
        for (authority, version) in ranked.iter().zip([RouteVersion(4), RouteVersion(9)]) {
            crate::mesh::registry::authority_replace_many(
                &directory.namespace,
                &authority.runtime.stable_id,
                &[RouteCasRequest {
                    key: key.clone(),
                    expected: Some(version),
                    next: None,
                }],
            )
            .expect("delete marker should install on local authority");
        }

        assert_eq!(
            directory.get_delete_marker(&observer, &key),
            Some(RouteVersion(9))
        );
        assert_eq!(
            directory.get_delete_markers(&observer, std::slice::from_ref(&key)),
            vec![Some(RouteVersion(9))]
        );

        unregister_local_authority(&directory.namespace, &peer.runtime.stable_id);
    }

    #[test]
    fn exact_replica_confirmation_rejects_changed_authority_membership() {
        let observer = test_lease("observer");
        let replica_owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut expected_route = test_route("confirmation-membership-key", &replica_owner);
        expected_route.replicas[0].allocation_id = Some(7);
        let absent = ObjectRoute {
            replicas: Vec::new(),
            ..expected_route.clone()
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::from([("authority-a".to_string(), Some(absent))]),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(SnapshotChangingMembership {
                first: vec![test_lease("authority-a")],
                refreshed: vec![test_lease("authority-a"), test_lease("authority-b")],
                snapshot_calls: AtomicUsize::new(0),
            }),
        );

        let error = directory
            .confirm_replica_reference(&observer, &expected_route.key, &expected_route.replicas[0])
            .expect_err("membership churn must not prove replica absence");
        assert!(matches!(error, StoreError::StaleEpoch(_)));
    }

    #[test]
    fn allocation_reference_confirmation_fails_closed_on_authority_divergence() {
        let observer = test_lease("observer");
        let identity = AllocationRouteIdentity {
            owner: ClientRuntimeId::new("owner", ClientEpoch(1)),
            segment_name: SegmentName::new("segment-a".to_string()),
            offset_bytes: 64,
            length_bytes: 16,
            allocation_id: 7,
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::from([
                    (
                        "authority-a".to_string(),
                        vec![AllocationRouteReferenceState::Absent],
                    ),
                    (
                        "authority-b".to_string(),
                        vec![AllocationRouteReferenceState::Exact],
                    ),
                ]),
            }),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert_eq!(
            directory
                .get_allocation_route_references_consistent(&observer, &[identity])
                .expect("divergent observations should resolve fail-closed"),
            vec![AllocationRouteReferenceState::Conflict],
        );
    }

    #[test]
    fn allocation_reference_key_query_keeps_exact_proof_when_peer_is_unavailable() {
        let observer = test_lease("observer");
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut route = test_route("allocation-partial-key", &owner);
        route.replicas[0].allocation_id = Some(7);
        let identity = AllocationRouteIdentity {
            owner,
            segment_name: route.replicas[0].segment_name.clone(),
            offset_bytes: route.replicas[0].segment_offset,
            length_bytes: route.replicas[0].length,
            allocation_id: 7,
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::from([("authority-a".to_string(), Some(route.clone()))]),
                allocation_references: BTreeMap::from([(
                    "authority-a".to_string(),
                    vec![AllocationRouteReferenceState::Exact],
                )]),
            }),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert_eq!(
            directory
                .get_allocation_route_reference_keys_consistent(&observer, &[identity])
                .expect("an exact positive proof must survive an unavailable peer"),
            vec![AllocationRouteReference::exact(route.key)],
        );
    }

    #[test]
    fn allocation_reference_key_query_rejects_absence_when_peer_is_unavailable() {
        let observer = test_lease("observer");
        let identity = AllocationRouteIdentity {
            owner: ClientRuntimeId::new("owner", ClientEpoch(1)),
            segment_name: SegmentName::new("segment-a".to_string()),
            offset_bytes: 64,
            length_bytes: 16,
            allocation_id: 7,
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::from([(
                    "authority-a".to_string(),
                    vec![AllocationRouteReferenceState::Absent],
                )]),
            }),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert_eq!(
            directory
                .get_allocation_route_reference_keys_consistent(&observer, &[identity])
                .expect("partial absence must remain indeterminate"),
            vec![AllocationRouteReference::conflict()],
        );
    }

    #[test]
    fn allocation_reference_confirmation_uses_cached_membership() {
        let observer = test_lease("observer");
        let identity = AllocationRouteIdentity {
            owner: ClientRuntimeId::new("owner", ClientEpoch(1)),
            segment_name: SegmentName::new("segment-a".to_string()),
            offset_bytes: 64,
            length_bytes: 16,
            allocation_id: 7,
        };
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::from([
                    (
                        "authority-a".to_string(),
                        vec![AllocationRouteReferenceState::Absent],
                    ),
                    (
                        "authority-b".to_string(),
                        vec![AllocationRouteReferenceState::Absent],
                    ),
                ]),
            }),
            Arc::new(CachedOnlyMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        assert_eq!(
            directory
                .get_allocation_route_references_consistent(&observer, &[identity])
                .expect("historical allocation confirmation must use cached membership"),
            vec![AllocationRouteReferenceState::Absent],
        );
    }

    #[test]
    fn allocation_reference_key_merge_accepts_sharded_exact_observation() {
        let route_key = ObjectKey::new("route-key".to_string());
        let exact = AllocationRouteReference::exact(route_key.clone());

        assert_eq!(
            merge_allocation_route_reference(
                Some(&AllocationRouteReference::absent()),
                exact.clone(),
            ),
            exact
        );
        assert_eq!(
            merge_allocation_route_reference(
                Some(&AllocationRouteReference::exact(route_key.clone())),
                AllocationRouteReference::absent(),
            ),
            AllocationRouteReference::exact(route_key)
        );
    }

    #[test]
    fn allocation_reference_key_merge_rejects_ambiguous_observations() {
        let exact = AllocationRouteReference::exact(ObjectKey::new("route-a".to_string()));

        assert_eq!(
            merge_allocation_route_reference(
                Some(&exact),
                AllocationRouteReference::exact(ObjectKey::new("route-b".to_string())),
            ),
            AllocationRouteReference::conflict()
        );
        assert_eq!(
            merge_allocation_route_reference(Some(&exact), AllocationRouteReference::conflict(),),
            AllocationRouteReference::conflict()
        );
    }

    #[test]
    fn allocation_reverse_lookup_tracks_public_cas_generation_changes() {
        let observer = test_lease("allocation-cas-authority");
        let directory = EmbeddedWrhRouteDirectory::new(
            1,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::new(),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: vec![observer.clone()],
            }),
        );
        let mut route = test_route("allocation-cas-generation", &observer.runtime);
        route.replicas[0].allocation_id = Some(7);
        let identity = AllocationRouteIdentity {
            owner: observer.runtime.clone(),
            segment_name: route.replicas[0].segment_name.clone(),
            offset_bytes: route.replicas[0].segment_offset,
            length_bytes: route.replicas[0].length,
            allocation_id: 7,
        };
        assert!(
            directory
                .compare_and_swap_object_route(&observer, &route.key, None, Some(&route))
                .unwrap()
                .applied
        );
        assert_eq!(
            directory
                .get_allocation_route_reference_keys_consistent(
                    &observer,
                    std::slice::from_ref(&identity)
                )
                .unwrap(),
            vec![AllocationRouteReference::exact(route.key.clone())]
        );

        let mut next = route.clone();
        next.version = route.version.next();
        next.replicas[0].allocation_id = Some(8);
        assert!(
            directory
                .compare_and_swap_object_route(
                    &observer,
                    &route.key,
                    Some(route.version),
                    Some(&next)
                )
                .unwrap()
                .applied
        );
        assert!(
            !directory
                .compare_and_swap_object_route(&observer, &route.key, Some(route.version), None)
                .unwrap()
                .applied
        );
        assert_eq!(
            directory
                .get_allocation_route_references_consistent(
                    &observer,
                    std::slice::from_ref(&identity)
                )
                .unwrap(),
            vec![AllocationRouteReferenceState::Conflict]
        );

        let mut changed_length = next.clone();
        changed_length.version = next.version.next();
        changed_length.replicas[0].allocation_id = Some(7);
        changed_length.replicas[0].length += 1;
        assert!(
            directory
                .compare_and_swap_object_route(
                    &observer,
                    &route.key,
                    Some(next.version),
                    Some(&changed_length)
                )
                .unwrap()
                .applied
        );
        assert_eq!(
            directory
                .get_allocation_route_reference_keys_consistent(
                    &observer,
                    std::slice::from_ref(&identity)
                )
                .unwrap(),
            vec![AllocationRouteReference::conflict()]
        );
        assert!(
            directory
                .compare_and_swap_object_route(
                    &observer,
                    &route.key,
                    Some(changed_length.version),
                    None
                )
                .unwrap()
                .applied
        );
        assert_eq!(
            directory
                .get_allocation_route_references_consistent(&observer, &[identity])
                .unwrap(),
            vec![AllocationRouteReferenceState::Absent]
        );
    }

    #[test]
    fn exact_replica_confirmation_rejects_partial_absence() {
        let observer = test_lease("partial-absence-observer");
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let mut route = test_route("partial-absence-key", &owner);
        route.replicas[0].allocation_id = Some(7);
        let directory = EmbeddedWrhRouteDirectory::new(
            2,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            Arc::new(ConfirmationClient {
                routes: BTreeMap::from([("authority-a".to_string(), None)]),
                allocation_references: BTreeMap::new(),
            }),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );
        assert!(directory
            .confirm_replica_reference(&observer, &route.key, &route.replicas[0])
            .is_err());
    }

    #[test]
    fn allocation_reference_key_query_accepts_sharded_positive_proof() {
        for exact_authority in ["authority-a", "authority-b"] {
            let observer = test_lease("sharded-proof-observer");
            let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
            let mut route = test_route("sharded-proof-key", &owner);
            route.replicas[0].allocation_id = Some(7);
            let identity = AllocationRouteIdentity {
                owner,
                segment_name: route.replicas[0].segment_name.clone(),
                offset_bytes: route.replicas[0].segment_offset,
                length_bytes: route.replicas[0].length,
                allocation_id: 7,
            };
            let absent_authority = if exact_authority == "authority-a" {
                "authority-b"
            } else {
                "authority-a"
            };
            let directory = EmbeddedWrhRouteDirectory::new(
                2,
                Arc::new(InMemoryMetadataBackend::new()),
                &observer,
                Arc::new(ConfirmationClient {
                    routes: BTreeMap::from([
                        (exact_authority.to_string(), Some(route.clone())),
                        (absent_authority.to_string(), None),
                    ]),
                    allocation_references: BTreeMap::from([
                        (
                            exact_authority.to_string(),
                            vec![AllocationRouteReferenceState::Exact],
                        ),
                        (
                            absent_authority.to_string(),
                            vec![AllocationRouteReferenceState::Absent],
                        ),
                    ]),
                }),
                Arc::new(TestMembership {
                    leases: vec![test_lease("authority-a"), test_lease("authority-b")],
                }),
            );
            assert_eq!(
                directory
                    .get_allocation_route_reference_keys_consistent(&observer, &[identity])
                    .unwrap(),
                vec![AllocationRouteReference::exact(route.key)]
            );
        }
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::RouteOwnerPage;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLifecycleState, CompatibilityDescriptor,
        ReplicaRoute, ReplicaTier, RouteState, SegmentName,
    };
    use mooncake_store_rs_metadata::InMemoryMetadataBackend;

    struct TestMembership {
        leases: Vec<ClientLease>,
    }

    impl RouteMembershipProvider for TestMembership {
        fn live_clients(
            &self,
            _force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            Ok(self.leases.clone())
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn clear_suspect(&self, _runtime: &ClientRuntimeId) {}

        fn is_suspect(&self, _runtime: &ClientRuntimeId) -> bool {
            false
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    struct ChangingMembership {
        initial: Vec<ClientLease>,
        refreshed: Vec<ClientLease>,
    }

    impl RouteMembershipProvider for ChangingMembership {
        fn live_clients(
            &self,
            force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            Ok(if force_refresh {
                self.refreshed.clone()
            } else {
                self.initial.clone()
            })
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn clear_suspect(&self, _runtime: &ClientRuntimeId) {}

        fn is_suspect(&self, _runtime: &ClientRuntimeId) -> bool {
            false
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    #[derive(Clone, Copy)]
    enum ContinuationPageBehavior {
        Error,
        InvalidEmptyPage,
        Terminal,
        ChangedEpochOnFinalValidation,
    }

    struct ContinuationPageClient {
        page_calls: AtomicUsize,
        behavior: ContinuationPageBehavior,
        fail_initial_page_after: Option<usize>,
    }

    impl ContinuationPageClient {
        fn new(behavior: ContinuationPageBehavior) -> Arc<Self> {
            Arc::new(Self {
                page_calls: AtomicUsize::new(0),
                behavior,
                fail_initial_page_after: None,
            })
        }

        fn with_second_initial_page_failure(behavior: ContinuationPageBehavior) -> Arc<Self> {
            Arc::new(Self {
                page_calls: AtomicUsize::new(0),
                behavior,
                fail_initial_page_after: Some(1),
            })
        }
    }

    impl RouteAuthorityClient for ContinuationPageClient {
        fn batch_get_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _keys: &[ObjectKey],
        ) -> Result<Vec<Result<RouteAuthorityRecord>>> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn batch_contains_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _keys: &[ObjectKey],
        ) -> Result<Vec<Result<bool>>> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn batch_compare_and_swap_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<CasResult>>> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn batch_replace_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<()>>> {
            Err(StoreError::Unsupported("unused test operation".to_string()))
        }

        fn list_routes_by_replica_owner_page(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            query: &RouteOwnerPageQuery,
        ) -> Result<crate::RouteOwnerPage> {
            let owner = &query.owner;
            let cursor = query.cursor.as_deref();
            let snapshot_epoch = query.snapshot_epoch;
            let call = self.page_calls.fetch_add(1, Ordering::SeqCst);
            if cursor.is_none() && self.fail_initial_page_after == Some(call) {
                return Err(StoreError::Transport(
                    "injected initial page failure".to_string(),
                ));
            }
            if call == 0 {
                assert!(cursor.is_none());
                assert!(snapshot_epoch.is_none());
                let next_cursor = match self.behavior {
                    ContinuationPageBehavior::Terminal
                    | ContinuationPageBehavior::ChangedEpochOnFinalValidation => None,
                    ContinuationPageBehavior::Error
                    | ContinuationPageBehavior::InvalidEmptyPage => Some("route-a".to_string()),
                };
                return Ok(RouteOwnerPage {
                    routes: vec![test_route("route-a", owner)],
                    next_cursor,
                    snapshot_epoch: Some(1),
                });
            }
            if cursor.is_none() {
                assert_eq!(snapshot_epoch, Some(1));
                let snapshot_epoch = match self.behavior {
                    ContinuationPageBehavior::ChangedEpochOnFinalValidation => Some(2),
                    _ => Some(1),
                };
                return Ok(RouteOwnerPage {
                    routes: vec![test_route("route-a", owner)],
                    next_cursor: None,
                    snapshot_epoch,
                });
            }
            assert_eq!(cursor, Some("route-a"));
            assert_eq!(snapshot_epoch, Some(1));
            match self.behavior {
                ContinuationPageBehavior::Error => Err(StoreError::Transport(
                    "injected continuation page failure".to_string(),
                )),
                ContinuationPageBehavior::InvalidEmptyPage => Ok(RouteOwnerPage {
                    routes: Vec::new(),
                    next_cursor: Some("route-a".to_string()),
                    snapshot_epoch: Some(1),
                }),
                ContinuationPageBehavior::Terminal => {
                    unreachable!("terminal first page must not request a continuation")
                }
                ContinuationPageBehavior::ChangedEpochOnFinalValidation => Ok(RouteOwnerPage {
                    routes: Vec::new(),
                    next_cursor: None,
                    snapshot_epoch: Some(1),
                }),
            }
        }
    }

    fn test_lease(stable_id: &str) -> ClientLease {
        let mut labels = BTreeMap::new();
        labels.insert("route".to_string(), "true".to_string());
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            expires_at_ms: u64::MAX,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            endpoints: ClientEndpointSet {
                labels,
                ..ClientEndpointSet::default()
            },
        }
    }

    fn test_route(key: &str, owner: &ClientRuntimeId) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new(key.to_string()),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            replicas: vec![ReplicaRoute {
                owner: owner.clone(),
                segment_name: SegmentName::new("segment-a".to_string()),
                allocation_id: None,
                segment_offset: 0,
                length: 1,
                checksum: None,
                offset: Some(0),
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,
            nof_backing: None,
        }
    }

    #[test]
    fn owner_route_scan_fails_when_a_continuation_page_fails() {
        let observer = test_lease("observer");
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let authority_client = ContinuationPageClient::new(ContinuationPageBehavior::Error);
        let directory = EmbeddedWrhRouteDirectory::new(
            1,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            authority_client.clone(),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority")],
            }),
        );
        let mut visited = Vec::new();

        let error = directory
            .visit_routes_by_replica_owner_consistent(&observer, &owner, &mut |route| {
                visited.push(route.key.0);
                Ok(())
            })
            .expect_err("a failed continuation page must fail the owner scan");

        assert!(
            matches!(error, StoreError::Transport(message) if message == "injected continuation page failure")
        );
        assert_eq!(visited, vec!["route-a"]);
        assert_eq!(authority_client.page_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn owner_route_scan_fails_when_a_continuation_page_is_invalid() {
        let observer = test_lease("observer");
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let authority_client =
            ContinuationPageClient::new(ContinuationPageBehavior::InvalidEmptyPage);
        let directory = EmbeddedWrhRouteDirectory::new(
            1,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            authority_client.clone(),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority")],
            }),
        );

        let error = directory
            .visit_routes_by_replica_owner_consistent(&observer, &owner, &mut |_| Ok(()))
            .expect_err("an empty continuation page with a cursor must fail the owner scan");

        assert!(
            matches!(error, StoreError::Transport(message) if message.contains("empty non-terminal page"))
        );
        assert_eq!(authority_client.page_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn owner_route_scan_fails_closed_when_an_authority_initial_page_fails() {
        let observer = test_lease("observer");
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let authority_client = ContinuationPageClient::with_second_initial_page_failure(
            ContinuationPageBehavior::Terminal,
        );
        let directory = EmbeddedWrhRouteDirectory::new(
            1,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            authority_client.clone(),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );
        let mut visited = Vec::new();

        let error = directory
            .visit_routes_by_replica_owner_consistent(&observer, &owner, &mut |route| {
                visited.push(route.key.0);
                Ok(())
            })
            .expect_err("a partial owner-route listing must be incomplete");

        assert!(matches!(
            error,
            StoreError::Transport(message) if message == "injected initial page failure"
        ));
        assert!(
            visited.is_empty(),
            "an incomplete scan must not expose a partial route set to a destructive caller"
        );
        assert_eq!(authority_client.page_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn owner_route_scan_rejects_a_changed_final_snapshot_epoch() {
        let observer = test_lease("observer");
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let authority_client =
            ContinuationPageClient::new(ContinuationPageBehavior::ChangedEpochOnFinalValidation);
        let directory = EmbeddedWrhRouteDirectory::new(
            1,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            authority_client.clone(),
            Arc::new(TestMembership {
                leases: vec![test_lease("authority")],
            }),
        );

        let error = directory
            .visit_routes_by_replica_owner_consistent(&observer, &owner, &mut |_| Ok(()))
            .expect_err("a changed terminal epoch must invalidate the owner scan");

        assert!(matches!(error, StoreError::StaleEpoch(_)));
        assert_eq!(authority_client.page_calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn owner_route_scan_rejects_changed_authority_membership() {
        let observer = test_lease("observer");
        let owner = ClientRuntimeId::new("owner", ClientEpoch(1));
        let authority_client = ContinuationPageClient::new(ContinuationPageBehavior::Terminal);
        let directory = EmbeddedWrhRouteDirectory::new(
            1,
            Arc::new(InMemoryMetadataBackend::new()),
            &observer,
            authority_client.clone(),
            Arc::new(ChangingMembership {
                initial: vec![test_lease("authority-a")],
                refreshed: vec![test_lease("authority-a"), test_lease("authority-b")],
            }),
        );

        let error = directory
            .visit_routes_by_replica_owner_consistent(&observer, &owner, &mut |_| Ok(()))
            .expect_err("changed authority membership must invalidate the owner scan");

        assert!(matches!(error, StoreError::StaleEpoch(_)));
        assert_eq!(authority_client.page_calls.load(Ordering::SeqCst), 2);
    }
}
