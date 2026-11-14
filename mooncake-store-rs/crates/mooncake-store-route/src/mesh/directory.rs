use crate::RouteOwnerPageQuery;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReference, AllocationRouteReferenceState, CasResult,
    ClientLease, ClientRuntimeId, ClientStableId, MetadataBackend, NamespaceScope, ObjectKey,
    ObjectRoute, ReplicaRoute, Result, ReuseIdentity, RouteCasRequest, RouteDirectory,
    RouteReplicaReference, RouteState, RouteVersion, StoreError,
    ROUTE_OWNER_LIST_PAGINATION_CAPABILITY, ROUTE_OWNER_LIST_SNAPSHOT_CAPABILITY,
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
    authority_get_delete_marker, authority_get_delete_markers, authority_get_records,
    authority_is_local, authority_list_reuse_candidates, authority_list_routes,
    authority_list_routes_by_replica_owner_page, authority_list_routes_in_scope,
    local_authority_service, register_local_authority, unregister_local_authority,
};
use crate::mesh::selection::{
    authority_candidates, merge_authority_candidates, ranked_authorities,
    ranked_merged_authorities, ranked_top_authorities, retained_authority_candidates,
    select_authorities_for_requests, RouteAuthoritySelection,
};
use crate::metrics::{record_cas_outcome, record_delete_marker_event};
use crate::shim::{
    RouteAuthorityClient, RouteAuthorityRecord, RouteAuthorityService, RouteMembershipProvider,
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
    ) -> Result<()> {
        let mut scans = Vec::new();
        let authorities = self.authority_candidates(observer)?;
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
            for scan in &mut scans {
                if !scan.routes.is_empty() || scan.next_cursor.is_none() {
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
                        return Err(error);
                    }
                }
            }

            let Some(next_key) = scans
                .iter()
                .filter_map(|scan| scan.routes.front().map(|route| route.key.0.as_str()))
                .min()
                .map(ToOwned::to_owned)
            else {
                if require_snapshot {
                    self.validate_owner_route_scan_snapshot(observer, owner, &scans)?;
                }
                return Ok(());
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

    fn read_ranked_authorities(
        &self,
        keys: &[ObjectKey],
        ranked_authorities: &[Vec<ClientLease>],
        rank: usize,
        resolved: &mut [Option<RouteAuthorityValue>],
        repairs: Option<&mut [RouteReadRepairState]>,
        failure_message: &'static str,
    ) -> Result<bool> {
        let mut groups = BTreeMap::<String, (ClientLease, Vec<usize>)>::new();
        for (index, authorities) in ranked_authorities.iter().enumerate() {
            let Some(authority) = authorities.get(rank).cloned() else {
                continue;
            };
            if resolved[index].is_some() && repairs.is_none() {
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
                        match result.map(value_from_authority_record) {
                            Ok(Some(route)) => {
                                trace!(
                                    namespace = %self.namespace,
                                    key = %key.0,
                                    authority = %authority.runtime,
                                    rank,
                                    source = route_read_source(rank, self.route_topk),
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
                                        source = route_read_source(rank, self.route_topk),
                                        route_version = route.version().0,
                                        replica_count = match &route { RouteAuthorityValue::Route(route) => route.replicas.len(), RouteAuthorityValue::DeleteMarker(_) => 0 },
                                        "sampled route authority object route"
                                    );
                                }
                                if let Some(states) = repairs.as_mut() {
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
                                if let Some(states) = repairs.as_mut() {
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
                None,
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
        let ranked_authorities = keys
            .iter()
            .map(|key| ranked_top_authorities(&self.namespace, &candidates, key, self.route_topk))
            .collect::<Vec<_>>();
        let start_rank = observer_start_rank(observer, self.route_topk);
        let mut resolved = vec![false; keys.len()];
        for step in 0..self.route_topk {
            if step > 0 && resolved.iter().all(|r| *r) {
                break;
            }
            let rank = (start_rank + step) % self.route_topk;
            self.contains_ranked_authorities(
                keys,
                &ranked_authorities,
                rank,
                &mut resolved,
                RouteContainsProbe {
                    should_probe: None,
                    retry_on_error: None,
                },
                if step == 0 {
                    "authority bounded route contains failed; trying mirrored authorities"
                } else {
                    "mirrored bounded route contains failed; trying other authorities"
                },
            )?;
        }

        if !resolved.iter().all(|r| *r) {
            let fallback_candidates = {
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
            if let Some(baseline_candidates) = fallback_candidates {
                let baseline_ranked = keys
                    .iter()
                    .map(|key| {
                        ranked_top_authorities(
                            &self.namespace,
                            &baseline_candidates,
                            key,
                            self.route_topk,
                        )
                    })
                    .collect::<Vec<_>>();
                for step in 0..self.route_topk {
                    if resolved.iter().all(|r| *r) {
                        break;
                    }
                    let rank = (start_rank + step) % self.route_topk;
                    self.contains_ranked_authorities(
                        keys,
                        &baseline_ranked,
                        rank,
                        &mut resolved,
                        RouteContainsProbe {
                            should_probe: None,
                            retry_on_error: None,
                        },
                        "baseline authority fallback route contains failed",
                    )?;
                }
            }
        }

        // Raw presence ignores delete-marker versions; validate hits with canonical reads.
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
        let validated = self.get_object_routes(observer, &positive_keys)?;
        for (index, route) in positives.into_iter().zip(validated) {
            resolved[index] = route.is_some();
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
        self.visit_routes_by_replica_owner_pages(observer, owner, &mut |route| {
            if routes.len() == DEFAULT_ROUTE_OWNER_PAGE_SIZE {
                return Err(StoreError::Unsupported(
                    "complete owner-route listing exceeds the bounded compatibility page; use the visitor API"
                        .to_string(),
                ));
            }
            routes.push(route);
            Ok(())
        }, false)?;
        Ok(routes)
    }

    fn visit_routes_by_replica_owner(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
        visitor: &mut dyn FnMut(ObjectRoute) -> Result<()>,
    ) -> Result<()> {
        self.visit_routes_by_replica_owner_pages(observer, owner, visitor, false)
    }

    fn visit_routes_by_replica_owner_consistent(
        &self,
        observer: &ClientLease,
        owner: &ClientRuntimeId,
        visitor: &mut dyn FnMut(ObjectRoute) -> Result<()>,
    ) -> Result<()> {
        self.visit_routes_by_replica_owner_pages(observer, owner, visitor, true)
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

#[cfg(test)]
mod owner_page_contract_tests {
    use super::*;
    use crate::RouteOwnerPage;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
            let route = directory.get_object_route(&observer, &key).unwrap();
            assert_eq!(
                route.as_ref().map(|route| route.version),
                expected.then_some(RouteVersion(live_version))
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
