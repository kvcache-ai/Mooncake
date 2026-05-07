pub(crate) type SharedLiveClientCache = Arc<Mutex<LiveClientCache>>;
pub(crate) type SharedSuspectRuntimeCache = Arc<Mutex<SuspectRuntimeCache>>;
pub(crate) type SharedRemoteRuntimeProbeCache = Arc<Mutex<RemoteRuntimeProbeCache>>;

#[derive(Default)]
pub(crate) struct LiveClientCache {
    refreshed_at: Option<Instant>,
    leases: Vec<ClientLease>,
    tenant_quota_policies: BTreeMap<String, TenantQuotaPolicyCacheEntry>,
}

impl LiveClientCache {
    pub(crate) fn snapshot(&self) -> Option<Vec<ClientLease>> {
        self.refreshed_at?;
        Some(filter_live_client_leases(&self.leases))
    }

    pub(crate) fn store(&mut self, leases: Vec<ClientLease>) {
        self.refreshed_at = Some(Instant::now());
        self.leases = filter_live_client_leases(&leases);
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
        self.tenant_quota_policies.retain(|_, entry| {
            now_ms.saturating_sub(entry.last_accessed_ms) <= idle_ttl_ms
        });
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

#[derive(Default)]
pub(crate) struct SuspectRuntimeCache {
    suspects: BTreeMap<ClientRuntimeId, SuspectRuntimeEntry>,
}

#[derive(Default)]
pub(crate) struct RemoteRuntimeProbeCache {
    entries: BTreeMap<ClientRuntimeId, RemoteRuntimeProbeEntry>,
}

#[derive(Clone)]
pub(crate) struct RemoteRuntimeProbeEntry {
    reachable_until: Option<Instant>,
    retry_after: Instant,
}

#[derive(Clone)]
pub(crate) struct SuspectRuntimeEntry {
    quarantine_until: Instant,
    observed_expires_at_ms: Option<u64>,
    observed_state: Option<ClientLifecycleState>,
    observed_control_address: Option<String>,
}

impl RemoteRuntimeProbeCache {
    pub(crate) fn is_recently_reachable(&mut self, runtime: &ClientRuntimeId) -> bool {
        let now = Instant::now();
        self.prune(now);
        self.entries
            .get(runtime)
            .and_then(|entry| entry.reachable_until)
            .is_some_and(|until| now < until)
    }

    pub(crate) fn should_probe(&mut self, runtime: &ClientRuntimeId) -> bool {
        let now = Instant::now();
        self.prune(now);
        self.entries
            .get(runtime)
            .is_none_or(|entry| now >= entry.retry_after)
    }

    pub(crate) fn mark_reachable(
        &mut self,
        runtime: ClientRuntimeId,
        reachable_for: Duration,
        retry_after: Duration,
    ) {
        let now = Instant::now();
        self.entries.insert(
            runtime,
            RemoteRuntimeProbeEntry {
                reachable_until: Some(now + reachable_for),
                retry_after: now + retry_after,
            },
        );
    }

    pub(crate) fn mark_probe_deferred(&mut self, runtime: ClientRuntimeId, retry_after: Duration) {
        self.entries.insert(
            runtime,
            RemoteRuntimeProbeEntry {
                reachable_until: None,
                retry_after: Instant::now() + retry_after,
            },
        );
    }

    fn prune(&mut self, now: Instant) {
        self.entries.retain(|_, entry| {
            entry.reachable_until.is_some_and(|until| now < until) || now < entry.retry_after
        });
    }
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

pub(crate) fn shared_remote_runtime_probe_cache(namespace: &str) -> SharedRemoteRuntimeProbeCache {
    static REMOTE_RUNTIME_PROBE_CACHES: OnceLock<
        Mutex<BTreeMap<String, SharedRemoteRuntimeProbeCache>>,
    > = OnceLock::new();
    let caches = REMOTE_RUNTIME_PROBE_CACHES.get_or_init(|| Mutex::new(BTreeMap::new()));
    let mut guard = caches.lock();
    guard
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(RemoteRuntimeProbeCache::default())))
        .clone()
}

pub(crate) fn filter_live_client_leases(leases: &[ClientLease]) -> Vec<ClientLease> {
    let now = current_time_ms();
    leases
        .iter()
        .filter(|lease| lease.expires_at_ms >= now)
        .cloned()
        .collect()
}

fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time should advance")
        .as_millis() as u64
}

#[derive(Default)]
struct StoreState {
    memory: Option<LocalMemoryState>,
    registered_buffers: BTreeMap<usize, usize>,
    local_transports: BTreeMap<String, Arc<dyn StoreTransport>>,
    remote_segments: BTreeMap<String, u64>,
    remote_segment_infos: BTreeMap<String, SegmentInfo>,
    segment_target_chunks: BTreeMap<(ClientRuntimeId, SegmentName), Vec<SegmentTargetChunk>>,
    pending_reclaims: VecDeque<PendingReclaim>,
    next_local_segment_id: u64,
}

struct StorageOwnerState {
    runtime: ClientRuntimeId,
    observer: ClientLease,
    route_directory: Arc<dyn RouteDirectory>,
    allocator: Arc<Mutex<LocalAllocatorState>>,
    clock: Mutex<StorageClockState>,
}

#[derive(Default)]
struct StorageClockState {
    entries: Vec<Option<ClockEntry>>,
    by_id: BTreeMap<ClockEntryId, usize>,
    by_key: BTreeMap<ObjectKey, Vec<usize>>,
    pending_hot_keys: BTreeSet<ObjectKey>,
    hand: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct ClockEntryId {
    route_key: ObjectKey,
    segment_name: SegmentName,
    segment_offset: u64,
}

#[derive(Clone, Debug)]
struct ClockEntry {
    id: ClockEntryId,
    length_bytes: u64,
    hot: bool,
}

#[derive(Default)]
struct LocalAllocatorState {
    segments: BTreeMap<SegmentName, SegmentAllocator>,
    pending_allocations: BTreeMap<AllocationSpan, u64>,
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
        self.pending_allocations
            .retain(|allocation, _| allocation.segment_name != *segment_name);
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

    fn release(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let segment = self
            .segments
            .get_mut(segment_name)
            .ok_or_else(|| StoreError::NotFound(format!("segment {} not found", segment_name.0)))?;
        segment.release(owner, segment_name, offset_bytes, length_bytes)?;
        self.pending_allocations.remove(&AllocationSpan {
            segment_name: segment_name.clone(),
            offset_bytes,
            length_bytes,
        });
        registry::record_segment(&segment.announcement);
        Ok(())
    }

    fn mark_pending_allocation(
        &mut self,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
        deadline_ms: u64,
    ) {
        self.pending_allocations.insert(
            AllocationSpan {
                segment_name: segment_name.clone(),
                offset_bytes,
                length_bytes,
            },
            deadline_ms,
        );
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

    fn clear_pending_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        for replica in route.replicas.iter().filter(|replica| replica.owner == *runtime) {
            self.pending_allocations.remove(&AllocationSpan {
                segment_name: replica.segment_name.clone(),
                offset_bytes: replica.segment_offset,
                length_bytes: replica.length,
            });
        }
    }

    fn pending_allocations(&mut self, now_ms: u64) -> BTreeSet<AllocationSpan> {
        self.pending_allocations
            .retain(|_, deadline_ms| *deadline_ms > now_ms);
        self.pending_allocations.keys().cloned().collect()
    }

    fn next_pending_deadline_ms(&mut self, now_ms: u64) -> Option<u64> {
        self.pending_allocations
            .retain(|_, deadline_ms| *deadline_ms > now_ms);
        self.pending_allocations.values().min().copied()
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
            .collect()
    }
}

struct SegmentAllocator {
    announcement: SegmentAnnouncement,
    cursor_bytes: u64,
    free_spans: Vec<FreeSpan>,
    allocations: BTreeMap<u64, u64>,
}

impl SegmentAllocator {
    fn new(announcement: SegmentAnnouncement) -> Self {
        Self {
            cursor_bytes: announcement.used_bytes,
            announcement,
            free_spans: Vec::new(),
            allocations: BTreeMap::new(),
        }
    }

    fn merge_announcement(&mut self, next: &SegmentAnnouncement) {
        self.announcement.owner = next.owner.clone();
        self.announcement.segment_name = next.segment_name.clone();
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
            .map(|(offset_bytes, length_bytes)| AllocationSpan {
                segment_name: self.announcement.segment_name.clone(),
                offset_bytes: *offset_bytes,
                length_bytes: *length_bytes,
            })
            .collect()
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

        if let Some(index) = self
            .free_spans
            .iter()
            .position(|span| span.length_bytes >= reserved_len)
        {
            let span = self.free_spans.remove(index);
            if span.length_bytes > reserved_len {
                self.free_spans.push(FreeSpan {
                    offset_bytes: span.offset_bytes + reserved_len,
                    length_bytes: span.length_bytes - reserved_len,
                });
                self.free_spans.sort_by_key(|entry| entry.offset_bytes);
            }
            self.announcement.used_bytes = self
                .announcement
                .used_bytes
                .checked_add(reserved_len)
                .ok_or_else(|| {
                StoreError::Allocator("segment reservation overflow".to_string())
            })?;
            self.allocations.insert(span.offset_bytes, length_bytes);
            return Ok(mooncake_store_core::SegmentReservation {
                owner: owner.clone(),
                segment_name: segment_name.clone(),
                offset_bytes: span.offset_bytes,
                length_bytes,
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
        self.allocations.insert(offset, length_bytes);
        Ok(mooncake_store_core::SegmentReservation {
            owner: owner.clone(),
            segment_name: segment_name.clone(),
            offset_bytes: offset,
            length_bytes,
        })
    }

    fn release(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
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
        let reserved = self.allocations.remove(&offset_bytes).ok_or_else(|| {
            StoreError::Allocator(format!(
                "segment release missing live allocation for {}:{} offset={} len={}",
                owner, segment_name.0, offset_bytes, length_bytes
            ))
        })?;
        if reserved != length_bytes {
            return Err(StoreError::Allocator(format!(
                "segment release length mismatch for {}:{} offset={} expected={} actual={}",
                owner, segment_name.0, offset_bytes, reserved, length_bytes
            )));
        }
        self.insert_free_span(offset_bytes, reserved_len);
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
            let Some(last) = self.free_spans.last().cloned() else {
                return;
            };
            if last.offset_bytes + last.length_bytes != self.cursor_bytes {
                return;
            }
            self.cursor_bytes = last.offset_bytes;
            self.free_spans.pop();
        }
    }

    fn insert_free_span(&mut self, offset_bytes: u64, length_bytes: u64) {
        self.free_spans.push(FreeSpan {
            offset_bytes,
            length_bytes,
        });
        self.free_spans.sort_by_key(|entry| entry.offset_bytes);
        let mut merged: Vec<FreeSpan> = Vec::with_capacity(self.free_spans.len());
        for span in self.free_spans.drain(..) {
            if let Some(previous) = merged.last_mut() {
                let prev_end = previous.offset_bytes + previous.length_bytes;
                if prev_end >= span.offset_bytes {
                    let merged_end = prev_end.max(span.offset_bytes + span.length_bytes);
                    previous.length_bytes = merged_end - previous.offset_bytes;
                    continue;
                }
            }
            merged.push(span);
        }
        self.free_spans = merged;
    }
}

#[derive(Clone)]
struct FreeSpan {
    offset_bytes: u64,
    length_bytes: u64,
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
    target_chunks: Vec<mooncake_store_core::SegmentTargetChunk>,
}

struct PreparedObjectWrite<'a> {
    tenant: &'a str,
    object_id: LogicalObjectId,
    qos_tier: Option<&'a str>,
    scoped_key: ObjectKey,
    value: &'a [u8],
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
    with_soft_pin: bool,
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
    storage_runtime: ClientRuntimeId,
    segment_name: SegmentName,
    offset_bytes: u64,
    length_bytes: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
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
