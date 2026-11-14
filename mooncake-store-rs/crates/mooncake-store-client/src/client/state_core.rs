pub(crate) type SharedLiveClientCache = Arc<Mutex<LiveClientCache>>;
pub(crate) type SharedSuspectRuntimeCache = Arc<Mutex<SuspectRuntimeCache>>;
include!("cold_tier_state.rs");

pub(crate) const RETAINED_MEMBERSHIP_TTL: Duration = Duration::from_secs(300);

#[derive(Default)]
pub(crate) struct LiveClientCache {
    refreshed_at: Option<Instant>,
    leases: Vec<ClientLease>,
    retained_leases: Vec<ClientLease>,
    retained_at: Option<Instant>,
    tenant_quota_policies: BTreeMap<String, TenantQuotaPolicyCacheEntry>,
}

impl LiveClientCache {
    pub(crate) fn snapshot(&self) -> Option<Vec<ClientLease>> {
        self.refreshed_at?;
        Some(filter_live_client_leases(&self.leases))
    }

    pub(crate) fn store(&mut self, leases: Vec<ClientLease>) {
        let leases = filter_live_client_leases(&leases);
        if self.refreshed_at.is_some() {
            if !membership_equivalent(&self.leases, &leases) {
                self.retained_leases = std::mem::take(&mut self.leases);
                self.retained_at = Some(Instant::now());
            } else if self
                .retained_at
                .is_some_and(|retained_at| retained_at.elapsed() >= RETAINED_MEMBERSHIP_TTL)
            {
                self.retained_leases = leases.clone();
                self.retained_at = None;
            }
        }
        self.refreshed_at = Some(Instant::now());
        self.leases = leases;
    }

    pub(crate) fn retained_snapshot(&self) -> Option<Vec<ClientLease>> {
        self.refreshed_at?;
        let leases = if self.retained_leases.is_empty() {
            &self.leases
        } else {
            &self.retained_leases
        };
        Some(leases.clone())
    }

    pub(crate) fn tenant_quota_policy(
        &mut self,
        tenant: &str,
        now_ms: u64,
        ttl_ms: u64,
    ) -> Option<Option<TenantQuotaPolicy>> {
        let entry = self.tenant_quota_policies.get_mut(tenant)?;
        entry.last_accessed_ms = now_ms;
        (now_ms.saturating_sub(entry.refreshed_at_ms) <= ttl_ms).then(|| entry.quota.clone())
    }

    pub(crate) fn store_tenant_quota_policy(
        &mut self,
        tenant: String,
        version: Option<u64>,
        quota: Option<TenantQuotaPolicy>,
        now_ms: u64,
    ) {
        self.tenant_quota_policies.insert(
            tenant,
            TenantQuotaPolicyCacheEntry {
                version,
                quota,
                refreshed_at_ms: now_ms,
                last_accessed_ms: now_ms,
            },
        );
    }

    pub(crate) fn due_tenant_quota_policy_refreshes(
        &mut self,
        now_ms: u64,
        refresh_ttl_ms: u64,
        idle_ttl_ms: u64,
    ) -> Vec<String> {
        self.tenant_quota_policies
            .retain(|_, entry| now_ms.saturating_sub(entry.last_accessed_ms) <= idle_ttl_ms);
        self.tenant_quota_policies
            .iter_mut()
            .filter_map(|(tenant, entry)| {
                let _ = entry.version;
                (now_ms.saturating_sub(entry.refreshed_at_ms) > refresh_ttl_ms
                    && now_ms.saturating_sub(entry.last_accessed_ms) <= refresh_ttl_ms)
                    .then(|| {
                        entry.last_accessed_ms = now_ms;
                        tenant.clone()
                    })
            })
            .collect()
    }
}

fn membership_equivalent(left: &[ClientLease], right: &[ClientLease]) -> bool {
    fn index_by_runtime(
        leases: &[ClientLease],
    ) -> Option<BTreeMap<&ClientRuntimeId, &ClientLease>> {
        let mut indexed = BTreeMap::new();
        for lease in leases {
            if indexed.insert(&lease.runtime, lease).is_some() {
                return None;
            }
        }
        Some(indexed)
    }

    if left.len() != right.len() {
        return false;
    }
    let (Some(left), Some(right)) = (index_by_runtime(left), index_by_runtime(right)) else {
        return false;
    };
    left.iter().all(|(runtime, expected)| {
        right.get(runtime).is_some_and(|candidate| {
                candidate.runtime == expected.runtime
                    && candidate.state == expected.state
                    && candidate.compatibility == expected.compatibility
                    && candidate.endpoints == expected.endpoints
        })
    })
}

#[derive(Default)]
pub(crate) struct SuspectRuntimeCache {
    suspects: BTreeMap<ClientRuntimeId, SuspectRuntimeEntry>,
}

#[derive(Clone)]
pub(crate) struct SuspectRuntimeEntry {
    quarantine_until: Instant,
    observed_expires_at_ms: Option<u64>,
    observed_state: Option<ClientLifecycleState>,
    observed_control_address: Option<String>,
}

impl SuspectRuntimeCache {
    pub(crate) fn mark(
        &mut self,
        runtime: ClientRuntimeId,
        quarantine_until: Instant,
        observed: Option<&ClientLease>,
    ) {
        self.suspects
            .entry(runtime)
            .and_modify(|entry| entry.extend(quarantine_until))
            .or_insert_with(|| SuspectRuntimeEntry::new(quarantine_until, observed));
    }

    pub(crate) fn contains(&mut self, runtime: &ClientRuntimeId) -> bool {
        self.prune_unobserved();
        self.suspects.contains_key(runtime)
    }

    pub(crate) fn clear(&mut self, runtime: &ClientRuntimeId) {
        self.suspects.remove(runtime);
    }

    pub(crate) fn reconcile_with_leases(&mut self, leases: &[ClientLease]) {
        let now = Instant::now();
        self.suspects.retain(|runtime, entry| {
            let Some(lease) = leases.iter().find(|lease| lease.runtime == *runtime) else {
                return false;
            };
            !entry.is_recovered_by(lease, now)
        });
    }

    fn prune_unobserved(&mut self) {
        let now = Instant::now();
        self.suspects
            .retain(|_, entry| entry.is_observed() || !entry.quarantine_elapsed(now));
    }
}

impl SuspectRuntimeEntry {
    fn new(quarantine_until: Instant, observed: Option<&ClientLease>) -> Self {
        Self {
            quarantine_until,
            observed_expires_at_ms: observed.map(|lease| lease.expires_at_ms),
            observed_state: observed.map(|lease| lease.state),
            observed_control_address: observed.and_then(lease_control_address),
        }
    }

    fn extend(&mut self, quarantine_until: Instant) {
        self.quarantine_until = self.quarantine_until.max(quarantine_until);
    }

    fn is_observed(&self) -> bool {
        self.observed_expires_at_ms.is_some()
            || self.observed_state.is_some()
            || self.observed_control_address.is_some()
    }

    fn quarantine_elapsed(&self, now: Instant) -> bool {
        now >= self.quarantine_until
    }

    fn is_recovered_by(&self, lease: &ClientLease, now: Instant) -> bool {
        if !self.quarantine_elapsed(now) {
            return false;
        }
        if !self.is_observed() {
            return true;
        }
        self.observed_expires_at_ms
            .is_some_and(|expires| lease.expires_at_ms > expires)
            || self
                .observed_state
                .is_some_and(|state| lease.state != state && lease.state.allows_new_writes())
            || self
                .observed_control_address
                .as_ref()
                .is_some_and(|address| {
                    lease_control_address(lease).is_some_and(|current| current != *address)
                })
    }
}

fn lease_control_address(lease: &ClientLease) -> Option<String> {
    lease.endpoints.labels.get(control_address_label()).cloned()
}

pub(crate) fn cached_live_client_snapshot(
    live_client_cache: &SharedLiveClientCache,
) -> Result<Vec<ClientLease>> {
    live_client_cache.lock().snapshot().ok_or_else(|| {
        StoreError::InvalidState(
            "live client snapshot is not initialized; build() must prewarm membership before serving requests"
                .to_string(),
        )
    })
}

pub(crate) fn cached_retained_live_client_snapshot(
    live_client_cache: &SharedLiveClientCache,
) -> Result<Vec<ClientLease>> {
    live_client_cache.lock().retained_snapshot().ok_or_else(|| {
        StoreError::InvalidState(
            "retained live client snapshot is not initialized; build() must prewarm membership before serving requests"
                .to_string(),
        )
    })
}

pub(crate) fn shared_live_client_cache(namespace: &str) -> SharedLiveClientCache {
    static LIVE_CLIENT_CACHES: OnceLock<Mutex<BTreeMap<String, SharedLiveClientCache>>> =
        OnceLock::new();
    let caches = LIVE_CLIENT_CACHES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = caches.lock();
    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(LiveClientCache::default())))
        .clone()
}

pub(crate) fn shared_suspect_runtime_cache(namespace: &str) -> SharedSuspectRuntimeCache {
    static SUSPECT_RUNTIME_CACHES: OnceLock<Mutex<BTreeMap<String, SharedSuspectRuntimeCache>>> =
        OnceLock::new();
    let caches = SUSPECT_RUNTIME_CACHES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = caches.lock();
    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(SuspectRuntimeCache::default())))
        .clone()
}

pub(crate) fn filter_live_client_leases(leases: &[ClientLease]) -> Vec<ClientLease> {
    let now = current_time_ms();
    let live = leases
        .iter()
        .filter(|lease| lease.expires_at_ms >= now)
        .cloned()
        .collect::<Vec<_>>();

    let mut newest_active_by_stable = BTreeMap::<String, ClientLease>::new();
    for lease in &live {
        if lease.state != ClientLifecycleState::Active {
            continue;
        }
        let stable_id = lease.runtime.stable_id.0.clone();
        let replace = newest_active_by_stable
            .get(&stable_id)
            .map(|current| {
                lease.runtime.epoch > current.runtime.epoch
                    || (lease.runtime.epoch == current.runtime.epoch
                        && lease.expires_at_ms > current.expires_at_ms)
            })
            .unwrap_or(true);
        if replace {
            newest_active_by_stable.insert(stable_id, lease.clone());
        }
    }

    live.into_iter()
        .filter(|lease| {
            if lease.state != ClientLifecycleState::Active {
                return true;
            }
            newest_active_by_stable
                .get(lease.runtime.stable_id.0.as_str())
                .is_some_and(|current| current.runtime == lease.runtime)
        })
        .collect()
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time should advance")
        .as_millis() as u64
}

#[cfg(test)]
mod state_core_tests {
    use super::*;
    use mooncake_store_core::{ClientEndpointSet, ClientEpoch, CompatibilityDescriptor};

    fn sample_lease(
        stable_id: &str,
        epoch: u64,
        state: ClientLifecycleState,
        expires_at_ms: u64,
    ) -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(epoch)),
            state,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms,
        }
    }

    #[test]
    fn filter_live_client_leases_keeps_latest_active_epoch_per_stable_id() {
        let now = current_time_ms();
        let leases = vec![
            sample_lease("node-a", 1, ClientLifecycleState::Active, now + 10_000),
            sample_lease("node-a", 2, ClientLifecycleState::Active, now + 20_000),
            sample_lease("node-a", 1, ClientLifecycleState::Draining, now + 30_000),
            sample_lease("node-b", 1, ClientLifecycleState::Active, now + 40_000),
            sample_lease("node-c", 9, ClientLifecycleState::Active, now - 1),
        ];

        let filtered = filter_live_client_leases(&leases);
        let runtimes = filtered
            .iter()
            .map(|lease| {
                (
                    lease.runtime.stable_id.0.clone(),
                    lease.runtime.epoch.0,
                    lease.state,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            runtimes,
            vec![
                ("node-a".to_string(), 2, ClientLifecycleState::Active),
                ("node-a".to_string(), 1, ClientLifecycleState::Draining),
                ("node-b".to_string(), 1, ClientLifecycleState::Active),
            ]
        );
    }

    #[test]
    fn membership_equivalent_ignores_order_and_lease_expiry() {
        let left = vec![
            sample_lease("node-a", 1, ClientLifecycleState::Active, 100),
            sample_lease("node-b", 2, ClientLifecycleState::Draining, 200),
        ];
        let right = vec![
            sample_lease("node-b", 2, ClientLifecycleState::Draining, 300),
            sample_lease("node-a", 1, ClientLifecycleState::Active, 400),
        ];

        assert!(membership_equivalent(&left, &right));
    }

    #[test]
    fn membership_equivalent_rejects_unmatched_duplicate_runtime() {
        let duplicate = sample_lease("node-a", 1, ClientLifecycleState::Active, 100);
        let left = vec![duplicate.clone(), duplicate];
        let right = vec![
            sample_lease("node-a", 1, ClientLifecycleState::Active, 200),
            sample_lease("node-b", 1, ClientLifecycleState::Active, 200),
        ];

        assert!(!membership_equivalent(&left, &right));
    }    #[test]
    fn stale_route_track_does_not_clear_reused_generation_pending_publication() {
        let owner = ClientRuntimeId::new("pending-owner", ClientEpoch(1));
        let segment_name = SegmentName::new("pending-segment");
        let mut allocator = LocalAllocatorState::default();
        allocator.upsert(&SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 4096,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: Vec::new(),
        });

        let old = allocator
            .reserve_specific(&owner, &segment_name, 1024)
            .expect("old allocation should reserve");
        allocator
            .release(
                &owner,
                &segment_name,
                old.offset_bytes,
                old.length_bytes,
                old.allocation_id,
            )
            .expect("old allocation should release before reuse");
        let reused = allocator
            .reserve_specific(&owner, &segment_name, 1024)
            .expect("same physical span should be reusable");
        assert_eq!(reused.offset_bytes, old.offset_bytes);
        assert_ne!(reused.allocation_id, old.allocation_id);
        allocator.mark_pending_publication(&reused, current_time_ms().saturating_add(10_000));

        let delayed_old_route = ObjectRoute {
            key: ObjectKey::new("default::delayed-old-route"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: owner.clone(),
                segment_name: segment_name.clone(),
                allocation_id: Some(old.allocation_id),
                offset: Some(old.offset_bytes),
                segment_offset: old.offset_bytes,
                length: old.length_bytes,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,
            nof_backing: None,
        };

        allocator.clear_pending_route(&delayed_old_route, &owner);

        assert!(
            allocator.is_pending_allocation(
                &AllocationSpan {
                    segment_name,
                    offset_bytes: reused.offset_bytes,
                    length_bytes: reused.length_bytes,
                },
                current_time_ms(),
            ),
            "a delayed old generation must not clear the reused generation publication fence"
        );
        assert_eq!(
            allocator.pending_publication_ready.len(),
            1,
            "a delayed old generation must not remove the reused publication from the ready queue"
        );
    }

    #[test]
    fn pending_publication_claim_is_exact_and_singleflighted() {
        let owner = ClientRuntimeId::new("claim-owner", ClientEpoch(1));
        let segment_name = SegmentName::new("claim-segment");
        let mut allocator = LocalAllocatorState::default();
        allocator.upsert(&SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 4096,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: Vec::new(),
        });
        let deadline_ms = current_time_ms().saturating_add(100);
        let reservation = allocator
            .reserve_any_with_pending_publication(&owner, 1024, deadline_ms)
            .expect("reservation should create a pending publication");

        assert!(allocator
            .claim_due_pending_publications(deadline_ms.saturating_sub(1), 1)
            .is_empty());
        let claimed = allocator.claim_due_pending_publications(deadline_ms, 1);
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].identity.segment_name, reservation.segment_name);
        assert_eq!(claimed[0].identity.offset_bytes, reservation.offset_bytes);
        assert_eq!(claimed[0].identity.length_bytes, reservation.length_bytes);
        assert_eq!(claimed[0].identity.allocation_id, reservation.allocation_id);
        assert!(allocator
            .claim_due_pending_publications(deadline_ms, 1)
            .is_empty());

        allocator.retry_pending_publication(&claimed[0], deadline_ms.saturating_add(10));
        assert!(allocator
            .claim_due_pending_publications(deadline_ms.saturating_add(9), 1)
            .is_empty());
        assert_eq!(
            allocator
                .claim_due_pending_publications(deadline_ms.saturating_add(10), 1)
                .len(),
            1
        );

        let specific = allocator
            .reserve_specific_with_pending_publication(&owner, &segment_name, 1024, deadline_ms)
            .expect("specific reservation should create a pending publication");
        assert_ne!(specific.offset_bytes, reservation.offset_bytes);
    }

    #[test]
    fn pending_publication_ready_queue_claims_a_bounded_due_batch() {
        let owner = ClientRuntimeId::new("queue-owner", ClientEpoch(1));
        let segment_name = SegmentName::new("queue-segment");
        let mut allocator = LocalAllocatorState::default();
        allocator.upsert(&SegmentAnnouncement {
            owner: owner.clone(),
            segment_name,
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 128 * 1024,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: Vec::new(),
        });

        let deadline_ms = current_time_ms().saturating_add(100);
        for _ in 0..96 {
            allocator
                .reserve_any_with_pending_publication(&owner, 1024, deadline_ms)
                .expect("each queued publication should reserve");
        }
        assert_eq!(
            allocator
                .pending_publication_ready
                .values()
                .map(|identities| identities.len())
                .sum::<usize>(),
            96
        );

        let first = allocator.claim_due_pending_publications(deadline_ms, 64);
        assert_eq!(
            first.len(),
            64,
            "recovery must claim only the configured batch"
        );
        assert_eq!(
            allocator
                .pending_publication_ready
                .values()
                .map(|identities| identities.len())
                .sum::<usize>(),
            32,
            "claimed entries leave the ready queue until they are finalized or retried"
        );

        let second = allocator.claim_due_pending_publications(deadline_ms, 64);
        assert_eq!(second.len(), 32);
        assert!(allocator.pending_publication_ready.is_empty());
    }

    #[test]
    fn pending_publication_default_capacity_keeps_byte_guard_independent() {
        let lifecycle = AllocationLifecycleConfig::default();
        assert_eq!(lifecycle.max_pending_publications, 8192);
        assert_eq!(lifecycle.max_pending_publication_bytes, 1024 * 1024 * 1024);
    }

    #[test]
    fn pending_publication_backpressure_rejects_before_physical_reserve() {
        let owner = ClientRuntimeId::new("backpressure-owner", ClientEpoch(1));
        let segment_name = SegmentName::new("backpressure-segment");
        let mut allocator = LocalAllocatorState::default();
        allocator.upsert(&SegmentAnnouncement {
            owner: owner.clone(),
            segment_name,
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 4096,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: Vec::new(),
        });
        let lifecycle = AllocationLifecycleConfig::default()
            .max_pending_publications(1)
            .max_pending_publication_bytes(1024);
        let deadline_ms = current_time_ms().saturating_add(10_000);

        allocator
            .reserve_any_with_pending_publication_limited(&owner, 1024, deadline_ms, lifecycle)
            .expect("first pending publication should reserve");
        let result = allocator.reserve_any_with_pending_publication_limited(
            &owner,
            1024,
            deadline_ms,
            lifecycle,
        );

        assert!(matches!(result, Err(StoreError::Backpressure(_))));
        assert_eq!(allocator.usage_bytes().0, 1024);
        assert_eq!(allocator.pending_publication_count, 1);
        assert_eq!(allocator.pending_publication_bytes, 1024);
    }

    #[test]
    fn exact_publication_tracking_removes_all_pending_indexes() {
        let owner = ClientRuntimeId::new("finalize-owner", ClientEpoch(1));
        let segment_name = SegmentName::new("finalize-segment");
        let mut allocator = LocalAllocatorState::default();
        allocator.upsert(&SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 4096,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: Vec::new(),
        });
        let deadline_ms = current_time_ms().saturating_add(10_000);
        let reservation = allocator
            .reserve_any_with_pending_publication(&owner, 1024, deadline_ms)
            .expect("publication reserve should succeed");
        assert!(
            allocator.pending_deadlines.is_empty(),
            "publication recovery must not duplicate the legacy deadline index"
        );
        assert_eq!(allocator.pending_publication_ready.len(), 1);
        let route = ObjectRoute {
            key: ObjectKey::new("default::finalize-pending"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: owner.clone(),
                segment_name,
                allocation_id: Some(reservation.allocation_id),
                offset: Some(reservation.offset_bytes),
                segment_offset: reservation.offset_bytes,
                length: reservation.length_bytes,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,
            nof_backing: None,
        };

        allocator.clear_pending_route(&route, &owner);

        assert!(allocator.pending_allocations.is_empty());
        assert!(allocator.pending_deadlines.is_empty());
        assert!(allocator.pending_publication_ready.is_empty());
        assert_eq!(allocator.pending_publication_count, 0);
        assert_eq!(allocator.pending_publication_bytes, 0);
        assert!(allocator
            .claim_due_pending_publications(deadline_ms, 1)
            .is_empty());
    }

    #[test]
    fn pending_publication_is_an_explicit_reserve_mode() {
        let owner = ClientRuntimeId::new("publication-mode-owner", ClientEpoch(1));
        let segment_name = SegmentName::new("publication-mode-segment");
        let mut allocator = LocalAllocatorState::default();
        allocator.upsert(&SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 4096,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 1,
            tags: Vec::new(),
        });
        let deadline_ms = current_time_ms().saturating_add(100);

        let legacy = allocator
            .reserve_any_with_pending(&owner, 1024, deadline_ms)
            .expect("legacy reserve should succeed");
        let legacy_span = AllocationSpan {
            segment_name: legacy.segment_name.clone(),
            offset_bytes: legacy.offset_bytes,
            length_bytes: legacy.length_bytes,
        };
        assert!(matches!(
            allocator.pending_allocations.get(&legacy_span),
            Some(PendingAllocation::Legacy { .. })
        ));

        let publication = allocator
            .reserve_specific_with_pending_publication(&owner, &segment_name, 1024, deadline_ms)
            .expect("publication reserve should succeed");
        let publication_span = AllocationSpan {
            segment_name: publication.segment_name.clone(),
            offset_bytes: publication.offset_bytes,
            length_bytes: publication.length_bytes,
        };
        assert!(matches!(
            allocator.pending_allocations.get(&publication_span),
            Some(PendingAllocation::Publication { allocation_id, .. })
                if *allocation_id == publication.allocation_id
        ));
    }

    #[test]
    fn pending_publication_authority_failures_share_exponential_backoff() {
        let runtime = ClientRuntimeId::new("publication-backoff-owner", ClientEpoch(1));
        let mut retry = PendingPublicationAuthorityRetry::default();

        let first = retry.defer(&runtime, 1_000);
        assert!(first >= 2_000, "first retry must include the base delay");
        assert!(!retry.is_ready(first.saturating_sub(1)));
        assert!(retry.is_ready(first));

        let second = retry.defer(&runtime, first);
        assert!(
            second.saturating_sub(first) > first.saturating_sub(1_000),
            "each consecutive authority failure must increase the shared retry delay"
        );

        retry.reset();
        assert!(retry.is_ready(0));
        assert_eq!(retry.consecutive_failures, 0);
    }

}

#[derive(Default)]
struct StoreState {
    memory: Option<LocalMemoryState>,
    registered_buffers: BTreeMap<usize, usize>,
    local_transports: BTreeMap<String, Arc<dyn StoreTransport>>,
    remote_segments: BTreeMap<String, u64>,
    remote_segment_infos: BTreeMap<String, SegmentInfo>,
    segment_target_metadata: BTreeMap<(ClientRuntimeId, SegmentName), SegmentTransportMetadata>,
    segment_open_metadata: BTreeMap<String, SegmentTransportMetadata>,
    pending_reclaims: VecDeque<PendingReclaim>,
    pending_delete_notifications: BTreeSet<DeletePendingEntry>,
    next_local_segment_id: u64,
}

#[derive(Clone, Debug, Default)]
struct SegmentTransportMetadata {
    target_chunks: Vec<SegmentTargetChunk>,
    transport_endpoint: Option<String>,
    transport_segment_descriptor: Option<String>,
}

/// Central state for a storage-owner runtime.  Cold tier fields
/// (cold_tier_devices, hot_replicas, offload_mode, offload_priority)
/// are consumed by the offload/restore pipeline.
struct StorageOwnerState {
    runtime: ClientRuntimeId,
    route_ops: RouteOperations,
    metadata: Arc<dyn MetadataBackend>,
    allocator: Arc<Mutex<LocalAllocatorState>>,
    state: Arc<Mutex<StoreState>>,
    cold_tier_devices: ColdTierDeviceManager,
    hot_replicas: HotReplicaTracker,
    nof_backing_tracking_enabled: bool,
    nof_backings: HotReplicaTracker,
    pending_offloads: ColdTierOffloadManager,
    cold_tier_cleanup: ColdTierCleanupManager,
    initial_cold_backing_repair_at_ms: AtomicU64,
    pending_evictions: Mutex<BTreeMap<AllocationIdentity, PendingEviction>>,
    pending_deletes: Mutex<PendingDeleteQueue>,
    pending_cold_deletes: Mutex<PendingColdDeleteQueue>,
    pending_delete_next_due_at_ms: AtomicU64,
    pending_publication_authority_retry: Mutex<PendingPublicationAuthorityRetry>,
    // Bounded candidate reconciliation is independent from owner-wide audit.
    // Normal CLOCK eviction only reaches it for a known stale candidate.
    stale_candidate_reconcile_completed_at_ms: AtomicU64,
    stale_candidate_reconcile_started_at_ms: AtomicU64,
    stale_candidate_reconcile_in_progress: AtomicBool,
    offload_mode: ColdTierOffloadMode,
    offload_priority: ColdTierOffloadPriorityConfig,
    owner_cold_restore_flights: OwnerColdRestoreFlightMap,
    cold_restore_io_tracker: ColdRestoreIoTracker,
    /// Signaled by offload when an entry becomes Materialized (clean).
    eviction_ready_signal: EvictionReadySignal,
    read_pin_registry: ReadPinRegistry,
    staging_pool: Mutex<Option<Arc<cold_tier::ColdRestoreStagingPool>>>,
    pending_staging_slots: Mutex<HashMap<(SegmentName, u64), (cold_tier::StagingSlot, Instant)>>,
    staging_pool_bytes: usize,
    staging_slot_ttl: Duration,
}

#[derive(Default)]
struct StorageClockState {
    entries: Vec<Option<ClockEntry>>,
    free_slots: Vec<usize>,
    by_id: BTreeMap<ClockEntryId, usize>,
    by_key: BTreeMap<ObjectKey, Vec<usize>>,
    pending_hot_keys: BTreeSet<ObjectKey>,
    hand: usize,
    stale_reclaim_hand: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ClockEntryId {
    route_key: ObjectKey,
    segment_name: SegmentName,
    segment_offset: u64,
    allocation_id: Option<u64>,
}

#[derive(Clone, Debug)]
struct ClockEntry {
    id: ClockEntryId,
    length_bytes: u64,
    hot: bool,
    hot_credit: u8,
    fresh_write: bool,
}

/// The exact local allocator record an eviction is allowed to release.
///
/// Segment names are unique for a runtime incarnation today. A future protocol
/// that permits same-name segment recreation must add its incarnation here and
/// to `ReplicaRoute` before treating it as destructively reusable.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
struct AllocationIdentity {
    segment_name: SegmentName,
    offset_bytes: u64,
    length_bytes: u64,
    allocation_id: u64,
}

#[derive(Clone, Debug)]
struct EvictionToken {
    identity: AllocationIdentity,
    kind: PendingEvictionKind,
    route_key: Option<ObjectKey>,
    expected_version: RouteVersion,
    evicted_replica: Option<ReplicaRoute>,
    clock_id: Option<ClockEntryId>,
    next_route: Option<ObjectRoute>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingEvictionKind {
    RouteCas,
    AllocationCandidate,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingEvictionPhase {
    Prepared,
    ReadyToCommit,
    PendingConfirmation,
    PendingAllocationConfirmation,
}

#[derive(Clone, Debug)]
struct PendingEviction {
    token: EvictionToken,
    phase: PendingEvictionPhase,
    confirmation_attempts: u32,
    next_attempt_at_ms: u64,
    confirmation_in_progress: bool,
    deferred: bool,
}

type PendingDelete = ReclaimToken<AllocationIdentity>;
type PendingDeleteQueue = ReclaimQueue<AllocationIdentity>;

/// Cold-backing instantiation of the same generic owner-local reclaim queue
/// used for DRAM allocation deletes. The queue carries only protocol state;
/// the physical cold payload deletion and used_bytes release reuse the
/// existing pending-delete GC reclaim path.
type PendingColdDelete = ReclaimToken<crate::control_plane::ColdDeleteTarget>;
type PendingColdDeleteQueue = ReclaimQueue<crate::control_plane::ColdDeleteTarget>;

#[derive(Default)]
struct RouteWriteGate {
    state: Mutex<()>,
}

struct RouteWritePermit<'a> {
    guard: Option<parking_lot::MutexGuard<'a, ()>>,
}

#[derive(Default)]
struct LocalAllocatorState {
    segments: BTreeMap<SegmentName, SegmentAllocator>,
    // Exact lookup only; the ready queues maintain retry order.
    pending_allocations: HashMap<AllocationSpan, PendingAllocation>,
    allocation_pins: BTreeMap<AllocationIdentity, AllocationPinState>,
    // Legacy reservations and publication recovery use separate ready indexes.
    pending_deadlines: BTreeSet<PendingDeadline>,
    // Order deadline buckets and deduplicate exact identities within each bucket.
    // Reserve avoids comparing full segment names in the ordered tree.
    pending_publication_ready: BTreeMap<u64, HashSet<AllocationIdentity>>,
    pending_publication_count: usize,
    pending_publication_bytes: u64,

    reconcile_candidate_cursor: Option<AllocationIdentity>,
}

#[derive(Clone, Debug)]
enum PendingAllocation {
    // Local publication is synchronously tracked by the allocation owner.
    Legacy {
        deadline_ms: u64,
    },
    // A remote owner needs an exact recovery fence when asynchronous tracking is lost.
    Publication {
        allocation_id: u64,
        deadline_ms: u64,
        next_attempt_at_ms: u64,
        confirmation_in_progress: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PendingPublication {
    identity: AllocationIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct PendingDeadline {
    deadline_ms: u64,
    allocation: AllocationSpan,
}

#[derive(Default)]
struct PendingPublicationAuthorityRetry {
    consecutive_failures: u32,
    next_attempt_at_ms: u64,
}

impl PendingPublicationAuthorityRetry {
    fn is_ready(&self, now_ms: u64) -> bool {
        self.next_attempt_at_ms <= now_ms
    }

    fn defer(&mut self, runtime: &ClientRuntimeId, now_ms: u64) -> u64 {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let shift = self.consecutive_failures.saturating_sub(1).min(6);
        let base_delay_ms = 1_000u64.saturating_mul(1u64 << shift).min(60_000);
        let seed = runtime
            .stable_id
            .0
            .bytes()
            .fold(u64::from(self.consecutive_failures), |hash, byte| {
                hash.wrapping_mul(31).wrapping_add(u64::from(byte))
            });
        let jitter_cap_ms = (base_delay_ms / 4).min(60_000 - base_delay_ms);
        self.next_attempt_at_ms = now_ms
            .saturating_add(base_delay_ms)
            .saturating_add(seed % jitter_cap_ms.saturating_add(1));
        self.next_attempt_at_ms
    }

    fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.next_attempt_at_ms = 0;
    }
}

#[derive(Clone, Debug, Default)]
struct AllocationPinState {
    refs: u32,
    deferred_release_owner: Option<ClientRuntimeId>,
}

struct UntrackedCandidateScan<'a> {
    tracked_identities: &'a BTreeSet<AllocationIdentity>,
    legacy_tracked_spans: &'a BTreeSet<AllocationSpan>,
    pending_evictions: &'a BTreeSet<AllocationIdentity>,
    now_ms: u64,
    preferred_segment: Option<&'a SegmentName>,
    candidate_limit: usize,
    inspection_limit: usize,
    start_after: Option<&'a AllocationIdentity>,
    stop_at: Option<&'a AllocationIdentity>,
}

#[derive(Default)]
struct UntrackedCandidateScanProgress {
    inspected: usize,
    last_scanned: Option<AllocationIdentity>,
}

impl LocalAllocatorState {
    fn upsert(&mut self, announcement: &SegmentAnnouncement) {
        match self.segments.get_mut(&announcement.segment_name) {
            Some(segment) => {
                segment.merge_announcement(announcement);
                registry::record_segment(&segment.announcement);
            }
            None => {
                self.segments.insert(
                    announcement.segment_name.clone(),
                    SegmentAllocator::new(announcement.clone()),
                );
                registry::record_segment(announcement);
            }
        }
    }

    fn announcement(&self, segment_name: &SegmentName) -> Option<SegmentAnnouncement> {
        self.segments
            .get(segment_name)
            .map(|segment| segment.announcement.clone())
    }

    fn announcements(&self) -> Vec<SegmentAnnouncement> {
        self.segments
            .values()
            .map(|segment| segment.announcement.clone())
            .collect()
    }

    fn usage_bytes(&self) -> (u64, u64) {
        self.segments
            .values()
            .fold((0u64, 0u64), |(used, capacity), segment| {
                (
                    used.saturating_add(segment.announcement.used_bytes),
                    capacity.saturating_add(segment.announcement.capacity_bytes),
                )
            })
    }

    fn allocations(&self) -> Vec<AllocationSpan> {
        self.segments
            .values()
            .flat_map(|segment| segment.allocations())
            .collect()
    }

    fn allocation_id(&self, allocation: &AllocationSpan) -> Option<u64> {
        self.segments
            .get(&allocation.segment_name)?
            .allocation_id(allocation.offset_bytes, allocation.length_bytes)
    }

    fn allocation_identity(&self, allocation: &AllocationSpan) -> Option<AllocationIdentity> {
        self.allocation_id(allocation)
            .map(|allocation_id| AllocationIdentity {
                segment_name: allocation.segment_name.clone(),
                offset_bytes: allocation.offset_bytes,
                length_bytes: allocation.length_bytes,
                allocation_id,
            })
    }

    /// Selects only a bounded number of allocator-owned records that neither
    /// CLOCK nor an in-flight eviction token currently owns. The cursor moves
    /// across calls so a large allocator is never converted into a full scan.
    fn untracked_reconcile_candidates(
        &mut self,
        tracked_identities: &BTreeSet<AllocationIdentity>,
        legacy_tracked_spans: &BTreeSet<AllocationSpan>,
        pending_evictions: &BTreeSet<AllocationIdentity>,
        now_ms: u64,
        preferred_segment: Option<&SegmentName>,
        limit: usize,
    ) -> Vec<AllocationIdentity> {
        const INSPECTION_LIMIT: usize = 256;
        if limit == 0 {
            return Vec::new();
        }
        let cursor = self.reconcile_candidate_cursor.clone();
        let mut candidates = Vec::with_capacity(limit);
        let mut progress = UntrackedCandidateScanProgress::default();
        self.append_untracked_reconcile_candidates(
            UntrackedCandidateScan {
                tracked_identities,
                legacy_tracked_spans,
                pending_evictions,
                now_ms,
                preferred_segment,
                candidate_limit: limit,
                inspection_limit: INSPECTION_LIMIT,
                start_after: cursor.as_ref(),
                stop_at: None,
            },
            &mut candidates,
            &mut progress,
        );
        if candidates.len() < limit && progress.inspected < INSPECTION_LIMIT && cursor.is_some() {
            self.append_untracked_reconcile_candidates(
                UntrackedCandidateScan {
                    tracked_identities,
                    legacy_tracked_spans,
                    pending_evictions,
                    now_ms,
                    preferred_segment,
                    candidate_limit: limit,
                    inspection_limit: INSPECTION_LIMIT,
                    start_after: None,
                    stop_at: cursor.as_ref(),
                },
                &mut candidates,
                &mut progress,
            );
        }
        if progress.last_scanned.is_some() {
            self.reconcile_candidate_cursor = progress.last_scanned;
        }
        candidates
    }

    fn append_untracked_reconcile_candidates(
        &self,
        scan: UntrackedCandidateScan<'_>,
        candidates: &mut Vec<AllocationIdentity>,
        progress: &mut UntrackedCandidateScanProgress,
    ) {
        let segment_start = scan
            .start_after
            .map_or(std::ops::Bound::Unbounded, |cursor| {
                std::ops::Bound::Included(&cursor.segment_name)
            });
        for (segment_name, segment) in self
            .segments
            .range((segment_start, std::ops::Bound::Unbounded))
        {
            if scan
                .stop_at
                .is_some_and(|cursor| segment_name > &cursor.segment_name)
            {
                return;
            }
            let offset_start = scan
                .start_after
                .filter(|cursor| cursor.segment_name == *segment_name)
                .map_or(std::ops::Bound::Unbounded, |cursor| {
                    std::ops::Bound::Included(cursor.offset_bytes)
                });
            for (offset_bytes, allocation) in segment
                .allocations
                .range((offset_start, std::ops::Bound::Unbounded))
            {
                if progress.inspected >= scan.inspection_limit
                    || candidates.len() >= scan.candidate_limit
                {
                    return;
                }
                let identity = AllocationIdentity {
                    segment_name: segment_name.clone(),
                    offset_bytes: *offset_bytes,
                    length_bytes: allocation.length_bytes,
                    allocation_id: allocation.allocation_id,
                };
                if scan.start_after.is_some_and(|cursor| identity <= *cursor) {
                    continue;
                }
                if scan.stop_at.is_some_and(|cursor| identity > *cursor) {
                    return;
                }
                progress.inspected = progress.inspected.saturating_add(1);
                progress.last_scanned = Some(identity.clone());
                let span = AllocationSpan {
                    segment_name: identity.segment_name.clone(),
                    offset_bytes: identity.offset_bytes,
                    length_bytes: identity.length_bytes,
                };
                if scan
                    .preferred_segment
                    .is_some_and(|segment| identity.segment_name != *segment)
                    || identity.allocation_id == 0
                    || scan.tracked_identities.contains(&identity)
                    || scan.legacy_tracked_spans.contains(&span)
                    || scan.pending_evictions.contains(&identity)
                    || self.is_pending_allocation(&span, scan.now_ms)
                {
                    continue;
                }
                candidates.push(identity);
            }
        }
    }

    fn update_state(
        &mut self,
        segment_name: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        let segment = self
            .segments
            .get_mut(segment_name)
            .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment_name.0)))?;
        segment.announcement.state = next;
        registry::record_segment(&segment.announcement);
        Ok(())
    }

    fn remove(&mut self, segment_name: &SegmentName) {
        if let Some(segment) = self.segments.remove(segment_name) {
            registry::record_segment_removed(&segment.announcement);
        }
        let pending = self
            .pending_allocations
            .keys()
            .filter(|allocation| allocation.segment_name == *segment_name)
            .cloned()
            .collect::<Vec<_>>();
        for allocation in pending {
            self.remove_pending_allocation(&allocation);
        }
        self.allocation_pins
            .retain(|identity, _| identity.segment_name != *segment_name);
    }

    fn reserve_any(
        &mut self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let mut candidates = self
            .segments
            .values()
            .filter(|segment| segment.announcement.state == SegmentLifecycleState::Active)
            .map(|segment| {
                (
                    segment.remaining_capacity(),
                    segment.announcement.segment_name.clone(),
                )
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
        let mut last_capacity_error = None;
        for (_, segment_name) in candidates {
            match self.reserve_specific(owner, &segment_name, length_bytes) {
                Ok(reservation) => return Ok(reservation),
                Err(StoreError::Allocator(message)) => {
                    last_capacity_error = Some(StoreError::Allocator(message));
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_capacity_error.unwrap_or_else(|| {
            StoreError::Allocator(format!(
                "no writable active segment available for {}",
                owner
            ))
        }))
    }

    fn reserve_any_with_pending(
        &mut self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
        deadline_ms: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let reservation = self.reserve_any(owner, length_bytes)?;
        self.mark_pending_reservation(&reservation, deadline_ms);
        Ok(reservation)
    }

    fn reserve_any_with_pending_publication_limited(
        &mut self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
        deadline_ms: u64,
        lifecycle: AllocationLifecycleConfig,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        self.ensure_pending_publication_capacity(length_bytes, lifecycle)?;
        let reservation = self.reserve_any(owner, length_bytes)?;
        self.mark_pending_publication(&reservation, deadline_ms);
        Ok(reservation)
    }

    #[cfg(test)]
    fn reserve_any_with_pending_publication(
        &mut self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
        deadline_ms: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        self.reserve_any_with_pending_publication_limited(
            owner,
            length_bytes,
            deadline_ms,
            AllocationLifecycleConfig::default(),
        )
    }

    fn reserve_specific(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let segment = self
            .segments
            .get_mut(segment_name)
            .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment_name.0)))?;
        let reservation = segment.reserve(owner, segment_name, length_bytes)?;
        registry::record_segment(&segment.announcement);
        Ok(reservation)
    }

    fn reserve_specific_with_pending(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
        deadline_ms: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let reservation = self.reserve_specific(owner, segment_name, length_bytes)?;
        self.mark_pending_reservation(&reservation, deadline_ms);
        Ok(reservation)
    }

    fn reserve_specific_with_pending_publication_limited(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
        deadline_ms: u64,
        lifecycle: AllocationLifecycleConfig,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        self.ensure_pending_publication_capacity(length_bytes, lifecycle)?;
        let reservation = self.reserve_specific(owner, segment_name, length_bytes)?;
        self.mark_pending_publication(&reservation, deadline_ms);
        Ok(reservation)
    }

    #[cfg(test)]
    fn reserve_specific_with_pending_publication(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
        deadline_ms: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        self.reserve_specific_with_pending_publication_limited(
            owner,
            segment_name,
            length_bytes,
            deadline_ms,
            AllocationLifecycleConfig::default(),
        )
    }

    fn release(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
        allocation_id: u64,
    ) -> Result<()> {
        let span = AllocationSpan {
            segment_name: segment_name.clone(),
            offset_bytes,
            length_bytes,
        };
        if let Some(identity) = self.allocation_identity(&span) {
            if (allocation_id == 0 || allocation_id == identity.allocation_id)
                && self.allocation_pins.contains_key(&identity)
            {
                let pin = self
                    .allocation_pins
                    .get_mut(&identity)
                    .expect("allocation pin checked above");
                if let Some(deferred_owner) = pin.deferred_release_owner.as_ref() {
                    if deferred_owner != owner {
                        return Err(StoreError::Allocator(format!(
                            "segment release already deferred for {}:{} offset={} len={}",
                            owner, segment_name.0, offset_bytes, length_bytes
                        )));
                    }
                    if pin.refs != 0 {
                        return Ok(());
                    }
                    let release_result = self
                        .segments
                        .get_mut(segment_name)
                        .ok_or_else(|| {
                            StoreError::NotFound(format!(
                                "segment {} not found while retrying deferred release",
                                segment_name.0
                            ))
                        })
                        .and_then(|segment| {
                            segment.release(
                                owner,
                                segment_name,
                                offset_bytes,
                                length_bytes,
                                allocation_id,
                            )?;
                            registry::record_segment(&segment.announcement);
                            Ok(())
                        });
                    if release_result.is_ok() {
                        self.allocation_pins.remove(&identity);
                        self.remove_pending_allocation(&span);
                    }
                    return release_result;
                }
                pin.deferred_release_owner = Some(owner.clone());
                self.remove_pending_allocation(&span);
                return Ok(());
            }
        }
        let segment = self
            .segments
            .get_mut(segment_name)
            .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment_name.0)))?;
        segment.release(
            owner,
            segment_name,
            offset_bytes,
            length_bytes,
            allocation_id,
        )?;
        registry::record_segment(&segment.announcement);
        self.remove_pending_allocation(&span);
        Ok(())
    }

    fn pin_allocation(&mut self, identity: &AllocationIdentity) -> Result<()> {
        let span = AllocationSpan {
            segment_name: identity.segment_name.clone(),
            offset_bytes: identity.offset_bytes,
            length_bytes: identity.length_bytes,
        };
        let current = self.allocation_identity(&span).ok_or_else(|| {
            StoreError::Allocator(format!(
                "cannot pin missing allocation {} offset={} len={}",
                identity.segment_name.0, identity.offset_bytes, identity.length_bytes
            ))
        })?;
        if current != *identity {
            return Err(StoreError::Allocator(format!(
                "cannot pin stale allocation {} offset={} expected={} actual={}",
                identity.segment_name.0,
                identity.offset_bytes,
                identity.allocation_id,
                current.allocation_id
            )));
        }
        let pin = self.allocation_pins.entry(identity.clone()).or_default();
        pin.refs = pin.refs.checked_add(1).ok_or_else(|| {
            StoreError::Allocator("allocation pin reference count overflow".to_string())
        })?;
        Ok(())
    }

    fn unpin_allocation(&mut self, identity: &AllocationIdentity) -> Result<()> {
        let Some(pin) = self.allocation_pins.get_mut(identity) else {
            return Err(StoreError::InvalidState(format!(
                "allocation pin is missing for {} offset={} generation={}",
                identity.segment_name.0, identity.offset_bytes, identity.allocation_id
            )));
        };
        pin.refs = pin.refs.checked_sub(1).ok_or_else(|| {
            StoreError::InvalidState("allocation pin reference count underflow".to_string())
        })?;
        if pin.refs != 0 {
            return Ok(());
        }
        let deferred_release_owner = pin.deferred_release_owner.clone();
        let Some(owner) = deferred_release_owner else {
            self.allocation_pins.remove(identity);
            return Ok(());
        };
        let release_result = self
            .segments
            .get_mut(&identity.segment_name)
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "segment {} not found while completing deferred release",
                    identity.segment_name.0
                ))
            })
            .and_then(|segment| {
                segment.release(
                    &owner,
                    &identity.segment_name,
                    identity.offset_bytes,
                    identity.length_bytes,
                    identity.allocation_id,
                )?;
                registry::record_segment(&segment.announcement);
                Ok(())
            });
        if let Err(error) = release_result {
            if let Some(pin) = self.allocation_pins.get_mut(identity) {
                // Keep the deferred owner with no active borrower. A later
                // stale-allocation reconciliation retries the physical
                // release before the range can be reused.
                pin.refs = 0;
            }
            return Err(error);
        }
        self.allocation_pins.remove(identity);
        self.remove_pending_allocation(&AllocationSpan {
            segment_name: identity.segment_name.clone(),
            offset_bytes: identity.offset_bytes,
            length_bytes: identity.length_bytes,
        });
        Ok(())
    }

    #[cfg(test)]
    fn allocation_is_pinned(&self, identity: &AllocationIdentity) -> bool {
        self.allocation_pins
            .get(identity)
            .is_some_and(|pin| pin.refs != 0)
    }

    fn mark_pending_allocation(
        &mut self,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
        deadline_ms: u64,
    ) {
        let allocation = AllocationSpan {
            segment_name: segment_name.clone(),
            offset_bytes,
            length_bytes,
        };
        self.remove_pending_allocation(&allocation);
        self.pending_deadlines.insert(PendingDeadline {
            deadline_ms,
            allocation: allocation.clone(),
        });
        self.pending_allocations
            .insert(allocation, PendingAllocation::Legacy { deadline_ms });
    }

    fn mark_pending_reservation(
        &mut self,
        reservation: &mooncake_store_core::SegmentReservation,
        deadline_ms: u64,
    ) {
        self.mark_pending_allocation(
            &reservation.segment_name,
            reservation.offset_bytes,
            reservation.length_bytes,
            deadline_ms,
        );
    }

    fn mark_pending_publication(
        &mut self,
        reservation: &mooncake_store_core::SegmentReservation,
        deadline_ms: u64,
    ) {
        let allocation = AllocationSpan {
            segment_name: reservation.segment_name.clone(),
            offset_bytes: reservation.offset_bytes,
            length_bytes: reservation.length_bytes,
        };
        let pending = PendingAllocation::Publication {
            allocation_id: reservation.allocation_id,
            deadline_ms,
            next_attempt_at_ms: deadline_ms,
            confirmation_in_progress: false,
        };
        self.remove_pending_allocation(&allocation);
        self.insert_pending_publication_ready(
            deadline_ms,
            AllocationIdentity {
                segment_name: allocation.segment_name.clone(),
                offset_bytes: allocation.offset_bytes,
                length_bytes: allocation.length_bytes,
                allocation_id: reservation.allocation_id,
            },
        );
        self.pending_allocations.insert(allocation, pending);
        self.pending_publication_count = self.pending_publication_count.saturating_add(1);
        self.pending_publication_bytes = self
            .pending_publication_bytes
            .saturating_add(reservation.length_bytes);
    }

    fn ensure_pending_publication_capacity(
        &self,
        length_bytes: u64,
        lifecycle: AllocationLifecycleConfig,
    ) -> Result<()> {
        let next_count = self.pending_publication_count.saturating_add(1);
        let next_bytes = self.pending_publication_bytes.saturating_add(length_bytes);
        if next_count > lifecycle.max_pending_publications
            || next_bytes > lifecycle.max_pending_publication_bytes
        {
            return Err(StoreError::Backpressure(format!(
                "pending publication ledger is full: entries={}/{} bytes={}/{}",
                self.pending_publication_count,
                lifecycle.max_pending_publications,
                self.pending_publication_bytes,
                lifecycle.max_pending_publication_bytes,
            )));
        }
        Ok(())
    }

    fn insert_pending_publication_ready(
        &mut self,
        next_attempt_at_ms: u64,
        identity: AllocationIdentity,
    ) {
        self.pending_publication_ready
            .entry(next_attempt_at_ms)
            .or_default()
            .insert(identity);
    }

    fn remove_pending_publication_ready(
        &mut self,
        next_attempt_at_ms: u64,
        identity: &AllocationIdentity,
    ) {
        let remove_bucket = self
            .pending_publication_ready
            .get_mut(&next_attempt_at_ms)
            .is_some_and(|entries| {
                entries.remove(identity);
                entries.is_empty()
            });
        if remove_bucket {
            self.pending_publication_ready.remove(&next_attempt_at_ms);
        }
    }

    fn remove_pending_allocation(
        &mut self,
        allocation: &AllocationSpan,
    ) -> Option<PendingAllocation> {
        let pending = self.pending_allocations.remove(allocation)?;
        match &pending {
            PendingAllocation::Legacy { deadline_ms } => {
                self.pending_deadlines.remove(&PendingDeadline {
                    deadline_ms: *deadline_ms,
                    allocation: allocation.clone(),
                });
            }
            PendingAllocation::Publication {
                allocation_id,
                next_attempt_at_ms,
                ..
            } => {
                self.pending_publication_count = self.pending_publication_count.saturating_sub(1);
                self.pending_publication_bytes = self
                    .pending_publication_bytes
                    .saturating_sub(allocation.length_bytes);
                self.remove_pending_publication_ready(
                    *next_attempt_at_ms,
                    &AllocationIdentity {
                        segment_name: allocation.segment_name.clone(),
                        offset_bytes: allocation.offset_bytes,
                        length_bytes: allocation.length_bytes,
                        allocation_id: *allocation_id,
                    },
                );
            }
        }
        Some(pending)
    }

    fn clear_pending_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        for replica in route
            .replicas
            .iter()
            .filter(|replica| replica.owner == *runtime)
        {
            let allocation = AllocationSpan {
                segment_name: replica.segment_name.clone(),
                offset_bytes: replica.segment_offset,
                length_bytes: replica.length,
            };
            let should_clear = match self.pending_allocations.get(&allocation) {
                Some(PendingAllocation::Legacy { .. }) => true,
                Some(PendingAllocation::Publication { allocation_id, .. }) => {
                    replica.allocation_id.filter(|id| *id != 0) == Some(*allocation_id)
                }
                None => false,
            };
            if should_clear {
                self.remove_pending_allocation(&allocation);
            }
        }
    }

    fn clear_pending_identity(&mut self, identity: &AllocationIdentity) {
        let allocation = AllocationSpan {
            segment_name: identity.segment_name.clone(),
            offset_bytes: identity.offset_bytes,
            length_bytes: identity.length_bytes,
        };
        if self.allocation_identity(&allocation).as_ref() == Some(identity) {
            self.remove_pending_allocation(&allocation);
        }
    }

    fn pending_allocations(&mut self, now_ms: u64) -> BTreeSet<AllocationSpan> {
        self.pending_allocations
            .iter()
            .filter(|(allocation, pending)| match pending {
                PendingAllocation::Legacy { deadline_ms } => *deadline_ms > now_ms,
                PendingAllocation::Publication { allocation_id, .. } => {
                    self.allocation_id(allocation) == Some(*allocation_id)
                }
            })
            .map(|(allocation, _)| allocation.clone())
            .collect()
    }

    fn is_pending_allocation(&self, allocation: &AllocationSpan, now_ms: u64) -> bool {
        self.pending_allocations
            .get(allocation)
            .is_some_and(|pending| match pending {
                PendingAllocation::Legacy { deadline_ms } => *deadline_ms > now_ms,
                PendingAllocation::Publication { allocation_id, .. } => {
                    self.allocation_id(allocation) == Some(*allocation_id)
                }
            })
    }

    fn next_pending_deadline_ms(&mut self, now_ms: u64) -> Option<u64> {
        let legacy_deadline = loop {
            let Some(deadline) = self.pending_deadlines.first().cloned() else {
                break None;
            };
            let is_current = self
                .pending_allocations
                .get(&deadline.allocation)
                .is_some_and(|pending| {
                    matches!(pending, PendingAllocation::Legacy { deadline_ms } if *deadline_ms == deadline.deadline_ms && *deadline_ms > now_ms)
                });
            if is_current {
                break Some(deadline.deadline_ms);
            }
            self.pending_deadlines.pop_first();
        };

        let publication_deadline = loop {
            let Some((ready_at_ms, identity)) = self
                .pending_publication_ready
                .first_key_value()
                .and_then(|(next_attempt_at_ms, identities)| {
                    identities
                        .iter()
                        .next()
                        .cloned()
                        .map(|identity| (*next_attempt_at_ms, identity))
                })
            else {
                break None;
            };
            let allocation = AllocationSpan {
                segment_name: identity.segment_name.clone(),
                offset_bytes: identity.offset_bytes,
                length_bytes: identity.length_bytes,
            };
            let current_allocation_id = self.allocation_id(&allocation);
            let is_current = self
                .pending_allocations
                .get(&allocation)
                .is_some_and(|pending| {
                    matches!(
                        pending,
                        PendingAllocation::Publication {
                            allocation_id,
                            next_attempt_at_ms,
                            confirmation_in_progress,
                            ..
                        } if *allocation_id == identity.allocation_id
                            && *next_attempt_at_ms == ready_at_ms
                            && !*confirmation_in_progress
                            && current_allocation_id == Some(*allocation_id)
                    )
                });
            if is_current {
                // A due entry is retried by the surrounding drain loop. Avoid
                // a tight loop when the authority-level backoff is active.
                break Some(ready_at_ms.max(now_ms.saturating_add(10)));
            }
            self.remove_pending_publication_ready(ready_at_ms, &identity);
        };

        match (legacy_deadline, publication_deadline) {
            (Some(legacy), Some(publication)) => Some(legacy.min(publication)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    fn claim_due_pending_publications(
        &mut self,
        now_ms: u64,
        limit: usize,
    ) -> Vec<PendingPublication> {
        if limit == 0 {
            return Vec::new();
        }
        let mut candidates = Vec::with_capacity(limit);
        let mut inspected = 0usize;
        let inspection_limit = limit.saturating_mul(4).max(limit);
        while candidates.len() < limit && inspected < inspection_limit {
            let Some((ready_at_ms, identity)) = self
                .pending_publication_ready
                .first_key_value()
                .and_then(|(next_attempt_at_ms, identities)| {
                    identities
                        .iter()
                        .next()
                        .cloned()
                        .map(|identity| (*next_attempt_at_ms, identity))
                })
            else {
                break;
            };
            if ready_at_ms > now_ms {
                break;
            }
            self.remove_pending_publication_ready(ready_at_ms, &identity);
            inspected = inspected.saturating_add(1);

            let allocation = AllocationSpan {
                segment_name: identity.segment_name.clone(),
                offset_bytes: identity.offset_bytes,
                length_bytes: identity.length_bytes,
            };
            let current_allocation_id = self.allocation_id(&allocation);
            let matches_current = self.pending_allocations.get_mut(&allocation).is_some_and(
                |pending| match pending {
                    PendingAllocation::Publication {
                        allocation_id,
                        deadline_ms,
                        next_attempt_at_ms,
                        confirmation_in_progress,
                    } if *allocation_id == identity.allocation_id
                        && *deadline_ms <= now_ms
                        && *next_attempt_at_ms == ready_at_ms
                        && !*confirmation_in_progress
                        && current_allocation_id == Some(*allocation_id) =>
                    {
                        *confirmation_in_progress = true;
                        true
                    }
                    PendingAllocation::Legacy { .. } | PendingAllocation::Publication { .. } => {
                        false
                    }
                },
            );
            if matches_current {
                candidates.push(PendingPublication {
                    identity,
                });
            }
        }
        candidates
    }

    fn retry_pending_publication(
        &mut self,
        publication: &PendingPublication,
        next_attempt_at_ms: u64,
    ) {
        let allocation = AllocationSpan {
            segment_name: publication.identity.segment_name.clone(),
            offset_bytes: publication.identity.offset_bytes,
            length_bytes: publication.identity.length_bytes,
        };
        let previous_next_attempt_at_ms =
            self.pending_allocations
                .get_mut(&allocation)
                .and_then(|pending| match pending {
                    PendingAllocation::Publication {
                        allocation_id,
                        next_attempt_at_ms: pending_next_attempt_at_ms,
                        confirmation_in_progress,
                        ..
                    } if *allocation_id == publication.identity.allocation_id => {
                        *confirmation_in_progress = false;
                        let previous = *pending_next_attempt_at_ms;
                        *pending_next_attempt_at_ms = next_attempt_at_ms;
                        Some(previous)
                    }
                    PendingAllocation::Legacy { .. } | PendingAllocation::Publication { .. } => {
                        None
                    }
        });
        if let Some(previous_next_attempt_at_ms) = previous_next_attempt_at_ms {
            self.remove_pending_publication_ready(
                previous_next_attempt_at_ms,
                &publication.identity,
            );
            self.insert_pending_publication_ready(next_attempt_at_ms, publication.identity.clone());
        }
    }

    fn stale_allocations(
        &mut self,
        live_allocations: &BTreeSet<AllocationSpan>,
        now_ms: u64,
    ) -> Vec<AllocationSpan> {
        let pending = self.pending_allocations(now_ms);
        self.allocations()
            .into_iter()
            .filter(|allocation| !live_allocations.contains(allocation))
            .filter(|allocation| !pending.contains(allocation))
            .filter(|allocation| {
                self.allocation_identity(allocation).is_none_or(|identity| {
                    self.allocation_pins
                        .get(&identity)
                        .is_none_or(|pin| pin.refs == 0)
                })
            })
            .collect()
    }

}

#[derive(Clone, Copy)]
struct SegmentAllocation {
    length_bytes: u64,
    allocation_id: u64,
}

struct SegmentAllocator {
    announcement: SegmentAnnouncement,
    cursor_bytes: u64,
    free_spans: FreeSpanSet,
    allocations: BTreeMap<u64, SegmentAllocation>,
    next_allocation_id: u64,
}

impl SegmentAllocator {
    fn new(announcement: SegmentAnnouncement) -> Self {
        Self {
            cursor_bytes: announcement.used_bytes,
            announcement,
            free_spans: FreeSpanSet::default(),
            allocations: BTreeMap::new(),
            next_allocation_id: 1,
        }
    }

    fn merge_announcement(&mut self, next: &SegmentAnnouncement) {
        self.announcement.owner = next.owner.clone();
        self.announcement.segment_name = next.segment_name.clone();
        self.announcement.transport_endpoint = next.transport_endpoint.clone();
        self.announcement.transport_segment_descriptor = next.transport_segment_descriptor.clone();
        self.announcement.capacity_bytes = next.capacity_bytes;
        self.announcement.target_chunks = next.target_chunks.clone();
        self.announcement.tags = next.tags.clone();
        self.announcement.state = next.state;
        self.announcement.alignment_bytes = next.alignment_bytes.max(1);
        self.announcement.used_bytes = self.announcement.used_bytes.max(next.used_bytes);
        self.cursor_bytes = self.cursor_bytes.max(next.used_bytes);
    }

    fn remaining_capacity(&self) -> u64 {
        self.announcement
            .capacity_bytes
            .saturating_sub(self.announcement.used_bytes)
    }

    fn allocations(&self) -> Vec<AllocationSpan> {
        self.allocations
            .iter()
            .map(|(offset_bytes, allocation)| AllocationSpan {
                segment_name: self.announcement.segment_name.clone(),
                offset_bytes: *offset_bytes,
                length_bytes: allocation.length_bytes,
            })
            .collect()
    }

    fn allocation_id(&self, offset_bytes: u64, length_bytes: u64) -> Option<u64> {
        self.allocations
            .get(&offset_bytes)
            .filter(|allocation| allocation.length_bytes == length_bytes)
            .map(|allocation| allocation.allocation_id)
    }

    fn take_allocation_id(&mut self) -> Result<u64> {
        let allocation_id = self.next_allocation_id.max(1);
        self.next_allocation_id = allocation_id.checked_add(1).ok_or_else(|| {
            StoreError::Allocator("segment allocation generation exhausted".to_string())
        })?;
        Ok(allocation_id)
    }

    fn reserve(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if self.announcement.state != SegmentLifecycleState::Active {
            return Err(StoreError::InvalidState(format!(
                "segment {}:{} is not active",
                owner, segment_name.0
            )));
        }
        if length_bytes == 0 {
            return Err(StoreError::Allocator(
                "zero-length segment reservation is not supported".to_string(),
            ));
        }
        let alignment = self.announcement.alignment_bytes.max(1);
        let reserved_len = align_up_u64(length_bytes, alignment);
        let allocation_id = self.take_allocation_id()?;

        if let Some(offset_bytes) = self.free_spans.take_first_fit(reserved_len) {
            self.announcement.used_bytes = self
                .announcement
                .used_bytes
                .checked_add(reserved_len)
                .ok_or_else(|| {
                StoreError::Allocator("segment reservation overflow".to_string())
            })?;
            self.allocations.insert(offset_bytes, SegmentAllocation { length_bytes, allocation_id });
            return Ok(mooncake_store_core::SegmentReservation {
                owner: owner.clone(),
                segment_name: segment_name.clone(),
                offset_bytes,
                length_bytes,
                allocation_id,
            });
        }

        let offset = align_up_u64(self.cursor_bytes, alignment);
        let next_cursor = offset
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment reservation overflow".to_string()))?;
        if next_cursor > self.announcement.capacity_bytes {
            return Err(StoreError::Allocator(format!(
                "segment capacity exhausted for {}:{} requested={} remaining={}",
                owner,
                segment_name.0,
                length_bytes,
                self.announcement.capacity_bytes.saturating_sub(offset)
            )));
        }
        self.cursor_bytes = next_cursor;
        self.announcement.used_bytes = self
            .announcement
            .used_bytes
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment reservation overflow".to_string()))?;
        self.allocations.insert(
            offset,
            SegmentAllocation {
                length_bytes,
                allocation_id,
            },
        );
        Ok(mooncake_store_core::SegmentReservation {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            offset_bytes: offset,
            length_bytes,
            allocation_id,
        })
    }

    fn release(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
        allocation_id: u64,
    ) -> Result<()> {
        let alignment = self.announcement.alignment_bytes.max(1);
        let reserved_len = align_up_u64(length_bytes, alignment);
        let end = offset_bytes
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment release overflow".to_string()))?;
        if end > self.announcement.capacity_bytes {
            return Err(StoreError::Allocator(format!(
                "segment release exceeds capacity for {}:{} offset={} len={}",
                owner, segment_name.0, offset_bytes, length_bytes
            )));
        }
        let reserved = self
            .allocations
            .get(&offset_bytes)
            .copied()
            .ok_or_else(|| {
                StoreError::Allocator(format!(
                    "segment release missing live allocation for {}:{} offset={} len={}",
                    owner, segment_name.0, offset_bytes, length_bytes
                ))
            })?;
        if reserved.length_bytes != length_bytes {
            return Err(StoreError::Allocator(format!(
                "segment release length mismatch for {}:{} offset={} expected={} actual={}",
                owner, segment_name.0, offset_bytes, reserved.length_bytes, length_bytes
            )));
        }
        if allocation_id != 0 && reserved.allocation_id != allocation_id {
            return Err(StoreError::Allocator(format!("segment release stale allocation generation for {}:{} offset={} expected={} actual={}", owner, segment_name.0, offset_bytes, reserved.allocation_id, allocation_id)));
        }
        self.allocations.remove(&offset_bytes);
        self.free_spans.insert(offset_bytes, reserved_len);
        self.announcement.used_bytes = self.announcement.used_bytes.saturating_sub(reserved_len);
        self.trim_tail();
        if self.announcement.used_bytes == 0 {
            self.cursor_bytes = 0;
            self.free_spans.clear();
            self.allocations.clear();
        }
        Ok(())
    }

    fn trim_tail(&mut self) {
        loop {
            let Some(offset_bytes) = self.free_spans.take_tail_ending(self.cursor_bytes) else {
                return;
            };
            self.cursor_bytes = offset_bytes;
        }
    }
}

#[derive(Clone)]
struct FreeSpan {
    offset_bytes: u64,
    length_bytes: u64,
}

/// Ordered free-space set shared by memory segments and managed block backends.
#[derive(Default)]
pub(in crate::client) struct FreeSpanSet {
    spans: Vec<FreeSpan>,
}

impl FreeSpanSet {
    #[cfg(feature = "nof-spdk")]
    pub(in crate::client) fn from_range(offset_bytes: u64, length_bytes: u64) -> Self {
        let mut set = Self::default();
        set.insert(offset_bytes, length_bytes);
        set
    }

    pub(in crate::client) fn take_first_fit(&mut self, length_bytes: u64) -> Option<u64> {
        let index = self
            .spans
            .iter()
            .position(|span| span.length_bytes >= length_bytes)?;
        let offset_bytes = self.spans[index].offset_bytes;
        self.spans[index].offset_bytes += length_bytes;
        self.spans[index].length_bytes -= length_bytes;
        if self.spans[index].length_bytes == 0 {
            self.spans.remove(index);
        }
        Some(offset_bytes)
    }

    pub(in crate::client) fn insert(&mut self, offset_bytes: u64, length_bytes: u64) {
        if length_bytes == 0 {
            return;
        }
        self.spans.push(FreeSpan {
            offset_bytes,
            length_bytes,
        });
        self.spans.sort_by_key(|span| span.offset_bytes);
        let mut merged: Vec<FreeSpan> = Vec::with_capacity(self.spans.len());
        for span in self.spans.drain(..) {
            if let Some(previous) = merged.last_mut() {
                let previous_end = previous.offset_bytes.saturating_add(previous.length_bytes);
                if previous_end >= span.offset_bytes {
                    let span_end = span.offset_bytes.saturating_add(span.length_bytes);
                    previous.length_bytes = previous_end.max(span_end) - previous.offset_bytes;
                    continue;
                }
            }
            merged.push(span);
        }
        self.spans = merged;
    }

    #[cfg(feature = "nof-spdk")]
    pub(in crate::client) fn contains(&self, offset_bytes: u64, length_bytes: u64) -> bool {
        let Some(end) = offset_bytes.checked_add(length_bytes) else {
            return false;
        };
        self.spans.iter().any(|span| {
            span.offset_bytes <= offset_bytes
                && end <= span.offset_bytes.saturating_add(span.length_bytes)
        })
    }

    #[cfg(feature = "nof-spdk")]
    pub(in crate::client) fn overlaps(&self, offset_bytes: u64, length_bytes: u64) -> bool {
        let Some(end) = offset_bytes.checked_add(length_bytes) else {
            return true;
        };
        self.spans.iter().any(|span| {
            offset_bytes < span.offset_bytes.saturating_add(span.length_bytes)
                && span.offset_bytes < end
        })
    }

    #[cfg(feature = "nof-spdk")]
    pub(in crate::client) fn total_bytes(&self) -> u64 {
        self.spans.iter().map(|span| span.length_bytes).sum()
    }

    fn take_tail_ending(&mut self, end: u64) -> Option<u64> {
        let last = self.spans.last()?;
        if last.offset_bytes.saturating_add(last.length_bytes) != end {
            return None;
        }
        self.spans.pop().map(|span| span.offset_bytes)
    }

    fn clear(&mut self) {
        self.spans.clear();
    }
}

#[derive(Clone)]
enum WriteMode {
    LocalOnly,
    Routed {
        planner: PlacementPlanner,
        replica_count: usize,
    },
}

#[derive(Clone, Debug)]
struct ReplicaWriteTarget {
    storage_runtime: ClientRuntimeId,
    segment_name: SegmentName,
    transport_endpoint: Option<String>,
    target_chunks: Vec<mooncake_store_core::SegmentTargetChunk>,
}

struct PreparedObjectWrite<'a> {
    tenant: &'a str,
    object_id: LogicalObjectId,
    qos_tier: Option<&'a str>,
    scoped_key: ObjectKey,
    value: Option<&'a [u8]>,
    value_len: usize,
    checksum: Option<u64>,
    registered_source: Option<*mut c_void>,
    quota_reservation: Option<mooncake_store_core::TenantQuotaReservationRequest>,
    targets: Vec<ReplicaWriteTarget>,
    reservations: Vec<mooncake_store_core::SegmentReservation>,
}

impl PreparedObjectWrite<'_> {
    fn value_tenant(&self) -> &str {
        self.tenant
    }
}

#[derive(Clone, Debug)]
struct StorageRuntimeReservationRequest {
    storage_runtime: ClientRuntimeId,
    segment_name: Option<SegmentName>,
    length_bytes: u64,
    require_local_memory: bool,
}

#[derive(Clone, Debug)]
struct AllocationReleaseRequest {
    storage_runtime: ClientRuntimeId,
    segment_name: SegmentName,
    offset_bytes: u64,
    length_bytes: u64,
    allocation_id: u64,
}

struct PendingRoutePublish {
    key: ObjectKey,
    expected_version: Option<RouteVersion>,
    previous: Option<ObjectRoute>,
    quota_reservation: Option<mooncake_store_core::TenantQuotaReservationRequest>,
    route: ObjectRoute,
}

#[derive(Clone, Debug)]
struct ResolvedReplicationPolicy {
    replica_count: usize,
    required_preferred_segments: Vec<SegmentName>,
    hint_preferred_segments: Vec<SegmentName>,
    preferred_storage_runtimes: Vec<ClientRuntimeId>,
    prefer_local: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PreferredSegmentSource {
    Request,
    TenantPolicy,
}

impl PreferredSegmentSource {
    fn label(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::TenantPolicy => "tenant_policy",
        }
    }
}

#[derive(Clone, Debug)]
enum ReplicaPlacementTarget {
    StorageRuntime(ClientRuntimeId),
    Segment {
        storage_runtime: ClientRuntimeId,
        segment_name: SegmentName,
    },
}

impl ReplicaPlacementTarget {
    fn storage_runtime(&self) -> &ClientRuntimeId {
        match self {
            Self::StorageRuntime(storage_runtime) => storage_runtime,
            Self::Segment {
                storage_runtime, ..
            } => storage_runtime,
        }
    }
}

#[derive(Clone, Debug)]
struct ReplicaPlacementCandidate {
    target: ReplicaPlacementTarget,
    soft: bool,
}

#[derive(Clone, Debug)]
struct PendingReclaim {
    due_at_ms: u64,
    policy_rank: u8,
    tenant: String,
    qos_tier: String,
    #[allow(dead_code)] // cold tier cleanup resolves backing for SSD deletion
    route_key: ObjectKey,
    storage_runtime: ClientRuntimeId,
    segment_name: SegmentName,
    offset_bytes: u64,
    length_bytes: u64,
    allocation_id: u64,
    #[allow(dead_code)] // cold tier cleanup schedules SSD deletion after route removal
    cold_backing: Option<mooncake_store_core::ColdBackingRoute>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct AllocationSpan {
    segment_name: SegmentName,
    offset_bytes: u64,
    length_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReclaimMode {
    Scheduled,
    Immediate,
    Deferred,

}

#[cfg(test)]
mod offload_pin_tests {
    use super::*;

    #[test]
    fn allocation_pin_defers_reuse_until_offload_io_completes() {
        let owner = ClientRuntimeId::new("pin-owner", ClientEpoch(1));
        let segment_name = SegmentName::new("pin-segment");
        let mut allocator = LocalAllocatorState::default();
        allocator.upsert(&SegmentAnnouncement {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: 8192,
            used_bytes: 0,
            target_chunks: Vec::new(),
            state: SegmentLifecycleState::Active,
            alignment_bytes: 4096,
            tags: Vec::new(),
        });
        let reservation = allocator
            .reserve_specific(&owner, &segment_name, 4096)
            .expect("allocation should reserve");
        let identity = AllocationIdentity {
            segment_name: segment_name.clone(),
            offset_bytes: reservation.offset_bytes,
            length_bytes: reservation.length_bytes,
            allocation_id: reservation.allocation_id,
        };
        allocator
            .pin_allocation(&identity)
            .expect("offload should pin the allocation");

        allocator
            .release(
                &owner,
                &segment_name,
                reservation.offset_bytes,
                reservation.length_bytes,
                reservation.allocation_id,
            )
            .expect("release should defer while the allocation is pinned");
        allocator
            .release(
                &owner,
                &segment_name,
                reservation.offset_bytes,
                reservation.length_bytes,
                reservation.allocation_id,
            )
            .expect("duplicate deferred release should be idempotent");
        assert_eq!(allocator.allocation_identity(&AllocationSpan {
            segment_name: segment_name.clone(),
            offset_bytes: reservation.offset_bytes,
            length_bytes: reservation.length_bytes,
        }), Some(identity.clone()));
        assert_eq!(allocator.usage_bytes().0, 4096);

        allocator
            .unpin_allocation(&identity)
            .expect("last unpin should complete the deferred release");
        assert_eq!(allocator.usage_bytes().0, 0);
        assert!(allocator
            .allocation_identity(&AllocationSpan {
                segment_name,
                offset_bytes: reservation.offset_bytes,
                length_bytes: reservation.length_bytes,
            })
            .is_none());
    }
}
