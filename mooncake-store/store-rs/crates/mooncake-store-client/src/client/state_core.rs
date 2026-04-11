pub(crate) type SharedLiveClientCache = Arc<Mutex<LiveClientCache>>;

#[derive(Default)]
pub(crate) struct LiveClientCache {
    refreshed_at: Option<Instant>,
    leases: Vec<ClientLease>,
}

impl LiveClientCache {
    pub(crate) fn snapshot(&self) -> Option<Vec<ClientLease>> {
        self.refreshed_at?;
        Some(self.leases.clone())
    }

    pub(crate) fn store(&mut self, leases: Vec<ClientLease>) {
        self.refreshed_at = Some(Instant::now());
        self.leases = leases;
    }
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

#[derive(Default)]
struct StoreState {
    memory: Option<LocalMemoryState>,
    registered_buffers: BTreeMap<usize, usize>,
    local_transports: BTreeMap<String, Arc<dyn StoreTransport>>,
    remote_segments: BTreeMap<String, u64>,
    pending_reclaims: VecDeque<PendingReclaim>,
    next_local_segment_id: u64,
}

struct StorageOwnerState {
    runtime: ClientRuntimeId,
    observer: ClientLease,
    metadata: Arc<dyn MetadataBackend>,
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
        self.segments.values().fold((0u64, 0u64), |(used, capacity), segment| {
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
        registry::record_segment(&segment.announcement);
        Ok(())
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
}

struct PreparedObjectWrite<'a> {
    scoped_key: ObjectKey,
    value: &'a [u8],
    targets: Vec<ReplicaWriteTarget>,
    reservations: Vec<mooncake_store_core::SegmentReservation>,
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
    route: ObjectRoute,
}

#[derive(Clone, Debug)]
struct ResolvedReplicationPolicy {
    replica_count: usize,
    preferred_segments: Vec<SegmentName>,
    preferred_storage_runtimes: Vec<ClientRuntimeId>,
    with_soft_pin: bool,
    prefer_local: bool,
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
}
