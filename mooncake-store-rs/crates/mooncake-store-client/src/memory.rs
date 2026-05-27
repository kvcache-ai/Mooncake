use std::collections::{BTreeMap, BTreeSet};
use std::ffi::c_void;
use std::ops::Deref;
use std::ptr;
use std::sync::Arc;
use std::time::Duration;

use mooncake_store_core::{
    ClientRuntimeId, HugePageConfig, Result, SegmentAnnouncement, SegmentLifecycleState,
    SegmentName, SegmentTargetChunk, StoreError,
};
use mooncake_transport::SegmentInfo;
use parking_lot::Mutex;

use crate::transport::{registration_chunks, StoreTransport};
use mooncake_store_transport_core::MemoryRegistration;

const DEFAULT_STORAGE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_SCRATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_ALIGNMENT: usize = 64;
const DEFAULT_EVICTION_HIGH_WATERMARK_PERCENT: u8 = 90;
const DEFAULT_EVICTION_LOW_WATERMARK_PERCENT: u8 = 80;
const DEFAULT_EVICTION_POLL_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_NUMA_AWARE: bool = true;

#[derive(Clone, Debug)]
pub struct LocalMemoryConfig {
    pub storage_bytes: usize,
    pub scratch_bytes: usize,
    pub location: String,
    pub tags: Vec<String>,
    pub alignment: usize,
    pub reclaim_grace_ms: u64,
    pub eviction_high_watermark_percent: u8,
    pub eviction_low_watermark_percent: u8,
    pub eviction_poll_interval: Duration,
    pub hugepage_enabled: Option<bool>,
    pub hugepage_size_bytes: Option<usize>,
    pub numa_aware: bool,
}

impl LocalMemoryConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn storage_bytes(mut self, storage_bytes: usize) -> Self {
        self.storage_bytes = storage_bytes;
        self
    }

    pub fn scratch_bytes(mut self, scratch_bytes: usize) -> Self {
        self.scratch_bytes = scratch_bytes;
        self
    }

    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.location = location.into();
        self
    }

    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn alignment(mut self, alignment: usize) -> Self {
        self.alignment = alignment.max(1);
        self
    }

    pub fn reclaim_grace_ms(mut self, reclaim_grace_ms: u64) -> Self {
        self.reclaim_grace_ms = reclaim_grace_ms;
        self
    }

    pub fn eviction_watermarks(mut self, high_percent: u8, low_percent: u8) -> Self {
        self.eviction_high_watermark_percent = high_percent;
        self.eviction_low_watermark_percent = low_percent;
        self
    }

    pub fn eviction_poll_interval(mut self, interval: Duration) -> Self {
        self.eviction_poll_interval = interval;
        self
    }

    pub fn use_hugepage(mut self, enabled: bool) -> Self {
        self.hugepage_enabled = Some(enabled);
        self
    }

    pub fn hugepage_size_bytes(mut self, hugepage_size_bytes: usize) -> Self {
        self.hugepage_enabled = Some(true);
        self.hugepage_size_bytes = Some(hugepage_size_bytes);
        self
    }

    pub fn numa_aware(mut self, enabled: bool) -> Self {
        self.numa_aware = enabled;
        self
    }

    pub fn hugepage(&self) -> Result<Option<HugePageConfig>> {
        HugePageConfig::resolve(self.hugepage_enabled, self.hugepage_size_bytes)
    }

    pub fn has_storage(&self) -> bool {
        self.storage_bytes != 0
    }

    pub fn has_scratch(&self) -> bool {
        self.scratch_bytes != 0
    }

    pub fn validate(&self) -> Result<()> {
        if !self.has_scratch() {
            return Err(StoreError::Allocator(
                "scratch_bytes must be greater than zero".to_string(),
            ));
        }
        if self.location.is_empty() {
            return Err(StoreError::Allocator(
                "location must not be empty".to_string(),
            ));
        }
        if self.eviction_high_watermark_percent == 0 || self.eviction_high_watermark_percent > 100 {
            return Err(StoreError::Allocator(
                "eviction_high_watermark_percent must be in 1..=100".to_string(),
            ));
        }
        if self.eviction_low_watermark_percent >= self.eviction_high_watermark_percent {
            return Err(StoreError::Allocator(
                "eviction_low_watermark_percent must be lower than eviction_high_watermark_percent"
                    .to_string(),
            ));
        }
        let _ = self.hugepage()?;
        Ok(())
    }

    pub fn storage_region_plans(
        &self,
        max_segment_bytes: Option<usize>,
    ) -> Result<Vec<LocalRegionPlan>> {
        self.region_plans(self.storage_bytes, max_segment_bytes)
    }

    fn scratch_region_plans(
        &self,
        max_segment_bytes: Option<usize>,
    ) -> Result<Vec<LocalRegionPlan>> {
        self.region_plans(self.scratch_bytes, max_segment_bytes)
    }

    fn region_plans(
        &self,
        total_bytes: usize,
        max_segment_bytes: Option<usize>,
    ) -> Result<Vec<LocalRegionPlan>> {
        if total_bytes == 0 {
            return Ok(Vec::new());
        }
        let locations = self.region_locations();
        let base = total_bytes / locations.len();
        let remainder = total_bytes % locations.len();
        let chunk_limit = max_segment_bytes.filter(|value| *value > 0);
        let mut remaining = locations
            .iter()
            .enumerate()
            .map(|(index, _)| base + usize::from(index < remainder))
            .collect::<Vec<_>>();
        let mut plans = Vec::new();
        while remaining.iter().any(|bytes| *bytes != 0) {
            for (index, location) in locations.iter().enumerate() {
                if remaining[index] == 0 {
                    continue;
                }
                let chunk = chunk_limit
                    .unwrap_or(remaining[index])
                    .min(remaining[index]);
                plans.push(LocalRegionPlan {
                    capacity_bytes: chunk,
                    location: location.clone(),
                });
                remaining[index] = remaining[index].saturating_sub(chunk);
            }
        }
        Ok(plans)
    }

    fn region_locations(&self) -> Vec<String> {
        if !self.numa_aware || !is_host_memory_location(&self.location) {
            return vec![self.location.clone()];
        }
        let mut discovered = discovered_cpu_locations();
        if discovered.is_empty() {
            vec![self.location.clone()]
        } else {
            if let Some(preferred_index) = parse_cpu_location(&self.location)
                .map(|node| format!("cpu:{node}"))
                .and_then(|preferred| {
                    discovered
                        .iter()
                        .position(|candidate| candidate == &preferred)
                })
            {
                discovered.rotate_left(preferred_index);
            }
            discovered
        }
    }
}

impl Default for LocalMemoryConfig {
    fn default() -> Self {
        Self {
            storage_bytes: DEFAULT_STORAGE_BYTES,
            scratch_bytes: DEFAULT_SCRATCH_BYTES,
            location: "cpu:0".to_string(),
            tags: vec!["dram".to_string()],
            alignment: DEFAULT_ALIGNMENT,
            reclaim_grace_ms: 1_000,
            eviction_high_watermark_percent: DEFAULT_EVICTION_HIGH_WATERMARK_PERCENT,
            eviction_low_watermark_percent: DEFAULT_EVICTION_LOW_WATERMARK_PERCENT,
            eviction_poll_interval: DEFAULT_EVICTION_POLL_INTERVAL,
            hugepage_enabled: None,
            hugepage_size_bytes: None,
            numa_aware: DEFAULT_NUMA_AWARE,
        }
    }
}

#[derive(Debug)]
pub struct LocalMemoryState {
    storage: BTreeMap<SegmentName, StorageExtent>,
    scratch: ScratchSpace,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalRegionPlan {
    pub capacity_bytes: usize,
    pub location: String,
}

#[derive(Clone, Debug)]
pub struct StorageSegmentSpec {
    pub segment_name: SegmentName,
    pub capacity_bytes: usize,
    pub state: SegmentLifecycleState,
    pub tags: Vec<String>,
    pub location: String,
    pub alignment: usize,
    pub hugepage_enabled: Option<bool>,
    pub hugepage_size_bytes: Option<usize>,
}

impl LocalMemoryState {
    #[cfg(test)]
    pub fn register(
        transport: &dyn StoreTransport,
        primary_segment: &SegmentName,
        config: &LocalMemoryConfig,
    ) -> Result<Self> {
        Self::register_with_storage_mode(transport, primary_segment, config, false)
    }

    pub fn register_startup_storage(
        transport: &dyn StoreTransport,
        primary_segment: &SegmentName,
        config: &LocalMemoryConfig,
    ) -> Result<Self> {
        Self::register_with_storage_mode(transport, primary_segment, config, true)
    }

    fn register_with_storage_mode(
        transport: &dyn StoreTransport,
        primary_segment: &SegmentName,
        config: &LocalMemoryConfig,
        startup_storage: bool,
    ) -> Result<Self> {
        config.validate()?;
        let hugepage = config.hugepage()?;
        let storage = if config.has_storage() {
            Some(RegisteredRegion::register_with_mode(
                transport,
                config.storage_bytes,
                &config.location,
                config.alignment,
                hugepage,
                startup_storage,
            )?)
        } else {
            None
        };
        let registration_limit = transport
            .max_registration_bytes()
            .filter(|value| *value > 0);
        let scratch = match ScratchSpace::register(
            transport,
            &config.scratch_region_plans(registration_limit)?,
            config.alignment,
            hugepage,
        ) {
            Ok(scratch) => scratch,
            Err(error) => {
                if let Some(storage) = storage {
                    let _ = storage.release(transport);
                }
                return Err(error);
            }
        };
        let mut segments = BTreeMap::new();
        if let Some(storage) = storage {
            segments.insert(
                primary_segment.clone(),
                StorageExtent {
                    region: Arc::new(storage),
                    state: SegmentLifecycleState::Active,
                    tags: config.tags.clone(),
                },
            );
        }
        Ok(Self {
            storage: segments,
            scratch,
        })
    }

    pub fn add_storage_segment(
        &mut self,
        transport: &dyn StoreTransport,
        spec: StorageSegmentSpec,
    ) -> Result<()> {
        self.add_storage_segment_with_mode(transport, spec, false)
    }

    fn add_storage_segment_with_mode(
        &mut self,
        transport: &dyn StoreTransport,
        spec: StorageSegmentSpec,
        startup_storage: bool,
    ) -> Result<()> {
        if self.storage.contains_key(&spec.segment_name) {
            return Err(StoreError::Conflict(format!(
                "storage segment {} already exists",
                spec.segment_name.0
            )));
        }
        let region = RegisteredRegion::register_with_mode(
            transport,
            spec.capacity_bytes,
            &spec.location,
            spec.alignment,
            HugePageConfig::resolve(spec.hugepage_enabled, spec.hugepage_size_bytes)?,
            startup_storage,
        )?;
        self.storage.insert(
            spec.segment_name,
            StorageExtent {
                region: Arc::new(region),
                state: spec.state,
                tags: spec.tags,
            },
        );
        Ok(())
    }

    pub(crate) fn insert_registered_storage_segment(
        &mut self,
        segment_name: SegmentName,
        region: RegisteredRegion,
        state: SegmentLifecycleState,
        tags: Vec<String>,
    ) -> Result<()> {
        if self.storage.contains_key(&segment_name) {
            return Err(StoreError::Conflict(format!(
                "storage segment {} already exists",
                segment_name.0
            )));
        }
        self.storage.insert(
            segment_name,
            StorageExtent {
                region: Arc::new(region),
                state,
                tags,
            },
        );
        Ok(())
    }

    pub fn plan_scratch(&self, lengths: &[usize]) -> Result<ScratchReservation> {
        self.scratch.plan(lengths)
    }

    pub fn has_storage_segment(&self, segment: &SegmentName) -> bool {
        self.storage.contains_key(segment)
    }

    pub(crate) fn all_storage_segment_names(&self) -> BTreeSet<SegmentName> {
        self.storage.keys().cloned().collect()
    }

    pub fn storage_address(
        &self,
        segment: &SegmentName,
        relative_offset: usize,
    ) -> Result<*mut c_void> {
        self.storage
            .get(segment)
            .ok_or_else(|| {
                StoreError::NotFound(format!("storage segment {} not found", segment.0))
            })?
            .region
            .address_at(relative_offset)
    }

    pub fn copy_storage_target_to(
        &self,
        segment: &SegmentName,
        target_info: &SegmentInfo,
        target_offset: u64,
        destination: *mut u8,
        length: usize,
        max_registration_bytes: Option<usize>,
    ) -> Result<()> {
        self
            .storage
            .get(segment)
            .ok_or_else(|| {
                StoreError::NotFound(format!("storage segment {} not found", segment.0))
            })?
            .region
            .copy_target_to(
                target_info,
                target_offset,
                destination,
                length,
                max_registration_bytes,
            )
            .map_err(|error| {
                StoreError::Allocator(format!(
                    "target offset {target_offset} length {length} is outside local storage segment {}: {error}",
                    segment.0
                ))
            })
    }

    pub(crate) fn prepare_storage_copy_instructions(
        &self,
        segment: &SegmentName,
        target_info: &SegmentInfo,
        target_offset: u64,
        length: usize,
        max_registration_bytes: Option<usize>,
    ) -> Result<PreparedCopy> {
        let region = self
            .storage
            .get(segment)
            .ok_or_else(|| {
                StoreError::NotFound(format!("storage segment {} not found", segment.0))
            })?
            .region
            .clone();
        region
            .build_copy_instructions(
                target_info,
                target_offset,
                length,
                max_registration_bytes,
            )
            .map_err(|error| {
                StoreError::Allocator(format!(
                    "target offset {target_offset} length {length} is outside local storage segment {}: {error}",
                    segment.0
                ))
            })
    }

    pub fn update_storage_state(
        &mut self,
        segment: &SegmentName,
        next: SegmentLifecycleState,
    ) -> Result<()> {
        let extent = self.storage.get_mut(segment).ok_or_else(|| {
            StoreError::NotFound(format!("storage segment {} not found", segment.0))
        })?;
        extent.state = next;
        Ok(())
    }

    pub fn storage_segments(&self) -> Vec<StorageExtentInfo> {
        self.storage
            .iter()
            .map(|(segment_name, extent)| StorageExtentInfo {
                segment_name: segment_name.clone(),
                capacity_bytes: extent.region.capacity as u64,
                alignment_bytes: extent.region.alignment as u64,
                target_chunks: extent.region.target_chunks.clone(),
                state: extent.state,
                tags: extent.tags.clone(),
            })
            .collect()
    }

    pub fn remove_storage_segment(
        &mut self,
        transport: &dyn StoreTransport,
        segment: &SegmentName,
    ) -> Result<()> {
        let extent = self.storage.remove(segment).ok_or_else(|| {
            StoreError::NotFound(format!("storage segment {} not found", segment.0))
        })?;
        match Arc::try_unwrap(extent.region) {
            Ok(region) => region.release(transport),
            Err(region) => {
                self.storage.insert(
                    segment.clone(),
                    StorageExtent {
                        region,
                        state: extent.state,
                        tags: extent.tags,
                    },
                );
                Err(StoreError::InvalidState(format!(
                    "storage segment {} has active copy instructions",
                    segment.0
                )))
            }
        }
    }

    pub fn release_scratch(self, transport: &dyn StoreTransport) -> Result<()> {
        self.scratch.release(transport)
    }
}

#[derive(Clone, Debug)]
pub struct StorageExtentInfo {
    pub segment_name: SegmentName,
    pub capacity_bytes: u64,
    pub alignment_bytes: u64,
    pub target_chunks: Vec<SegmentTargetChunk>,
    pub state: SegmentLifecycleState,
    pub tags: Vec<String>,
}

impl StorageExtentInfo {
    pub(crate) fn announcement(
        &self,
        owner: ClientRuntimeId,
        used_bytes: u64,
    ) -> SegmentAnnouncement {
        SegmentAnnouncement {
            owner,
            segment_name: self.segment_name.clone(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            capacity_bytes: self.capacity_bytes,
            used_bytes,
            target_chunks: self.target_chunks.clone(),
            state: self.state,
            alignment_bytes: self.alignment_bytes,
            tags: self.tags.clone(),
        }
    }
}

#[derive(Debug)]
struct StorageExtent {
    region: Arc<RegisteredRegion>,
    state: SegmentLifecycleState,
    tags: Vec<String>,
}

#[derive(Debug)]
struct ScratchSpace {
    regions: Vec<RegisteredRegion>,
    state: Arc<Mutex<ScratchAllocatorState>>,
}

impl ScratchSpace {
    fn register(
        transport: &dyn StoreTransport,
        plans: &[LocalRegionPlan],
        alignment: usize,
        hugepage: Option<HugePageConfig>,
    ) -> Result<Self> {
        if plans.is_empty() {
            return Err(StoreError::Allocator(
                "scratch region plans must not be empty".to_string(),
            ));
        }
        let mut regions = Vec::with_capacity(plans.len());
        for plan in plans {
            match RegisteredRegion::register(
                transport,
                plan.capacity_bytes,
                &plan.location,
                alignment,
                hugepage,
            ) {
                Ok(region) => regions.push(region),
                Err(error) => {
                    let _ = release_registered_regions(transport, regions);
                    return Err(error);
                }
            }
        }
        Ok(Self {
            state: Arc::new(Mutex::new(ScratchAllocatorState::new(&regions))),
            regions,
        })
    }

    fn plan(&self, lengths: &[usize]) -> Result<ScratchReservation> {
        let mut state = self.state.lock();
        let mut allocations = Vec::with_capacity(lengths.len());
        let mut reservations = Vec::with_capacity(lengths.len());
        for (index, length) in lengths.iter().copied().enumerate() {
            let start = index % self.regions.len();
            let mut placed = None;
            for step in 0..self.regions.len() {
                let region_index = (start + step) % self.regions.len();
                let region = &self.regions[region_index];
                match state.reserve(region_index, region, length) {
                    Ok((allocation, reservation)) => {
                        placed = Some((allocation, reservation));
                        break;
                    }
                    Err(StoreError::Allocator(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            let Some(allocation) = placed else {
                state.release_all(&reservations);
                return Err(StoreError::Allocator(format!(
                    "scratch capacity exhausted: requested={length}"
                )));
            };
            allocations.push(allocation.0);
            reservations.push(allocation.1);
        }
        Ok(ScratchReservation {
            allocations,
            reservations,
            state: Arc::clone(&self.state),
        })
    }

    fn release(self, transport: &dyn StoreTransport) -> Result<()> {
        release_registered_regions(transport, self.regions)
    }
}

#[derive(Debug)]
struct ScratchAllocatorState {
    regions: Vec<ScratchRegionState>,
}

impl ScratchAllocatorState {
    fn new(regions: &[RegisteredRegion]) -> Self {
        Self {
            regions: regions
                .iter()
                .map(|region| ScratchRegionState::new(region.capacity))
                .collect(),
        }
    }

    fn reserve(
        &mut self,
        region_index: usize,
        region: &RegisteredRegion,
        length: usize,
    ) -> Result<(RegionAllocation, ScratchRegionReservation)> {
        self.regions[region_index].reserve(region, region_index, length)
    }

    fn release_all(&mut self, reservations: &[ScratchRegionReservation]) {
        for reservation in reservations.iter().rev().copied() {
            self.regions[reservation.region_index].release(reservation);
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ScratchRegionReservation {
    region_index: usize,
    offset: usize,
    len: usize,
}

#[derive(Clone, Copy, Debug)]
struct ScratchFreeRange {
    offset: usize,
    len: usize,
}

#[derive(Debug)]
struct ScratchRegionState {
    free_ranges: Vec<ScratchFreeRange>,
}

impl ScratchRegionState {
    fn new(capacity: usize) -> Self {
        Self {
            free_ranges: vec![ScratchFreeRange {
                offset: 0,
                len: capacity,
            }],
        }
    }

    fn reserve(
        &mut self,
        region: &RegisteredRegion,
        region_index: usize,
        length: usize,
    ) -> Result<(RegionAllocation, ScratchRegionReservation)> {
        if length == 0 {
            return Err(StoreError::Allocator(
                "zero-length allocations are not supported".to_string(),
            ));
        }

        let reserved_len = align_up(length, region.alignment);
        for range_index in 0..self.free_ranges.len() {
            let range = self.free_ranges[range_index];
            let start = align_up(range.offset, region.alignment);
            let Some(end) = start.checked_add(reserved_len) else {
                return Err(StoreError::Allocator("allocation overflow".to_string()));
            };
            let range_end = range.offset.saturating_add(range.len);
            if end > range_end {
                continue;
            }

            self.free_ranges.remove(range_index);
            if start > range.offset {
                self.free_ranges.insert(
                    range_index,
                    ScratchFreeRange {
                        offset: range.offset,
                        len: start - range.offset,
                    },
                );
            }
            if end < range_end {
                let insert_index = self
                    .free_ranges
                    .iter()
                    .position(|candidate| candidate.offset > end)
                    .unwrap_or(self.free_ranges.len());
                self.free_ranges.insert(
                    insert_index,
                    ScratchFreeRange {
                        offset: end,
                        len: range_end - end,
                    },
                );
            }

            return Ok((
                region.allocation_at(start)?,
                ScratchRegionReservation {
                    region_index,
                    offset: start,
                    len: reserved_len,
                },
            ));
        }

        Err(StoreError::Allocator(format!(
            "region capacity exhausted: requested={length}"
        )))
    }

    fn release(&mut self, reservation: ScratchRegionReservation) {
        let insert_index = self
            .free_ranges
            .iter()
            .position(|range| range.offset > reservation.offset)
            .unwrap_or(self.free_ranges.len());
        self.free_ranges.insert(
            insert_index,
            ScratchFreeRange {
                offset: reservation.offset,
                len: reservation.len,
            },
        );
        self.merge_neighbors();
    }

    fn merge_neighbors(&mut self) {
        if self.free_ranges.len() < 2 {
            return;
        }
        self.free_ranges.sort_by_key(|range| range.offset);
        let mut merged: Vec<ScratchFreeRange> = Vec::with_capacity(self.free_ranges.len());
        for range in self.free_ranges.drain(..) {
            if let Some(previous) = merged.last_mut() {
                let previous_end = previous.offset.saturating_add(previous.len);
                if previous_end >= range.offset {
                    let range_end = range.offset.saturating_add(range.len);
                    previous.len = range_end.saturating_sub(previous.offset);
                    continue;
                }
            }
            merged.push(range);
        }
        self.free_ranges = merged;
    }
}

#[derive(Debug)]
pub struct ScratchReservation {
    allocations: Vec<RegionAllocation>,
    reservations: Vec<ScratchRegionReservation>,
    state: Arc<Mutex<ScratchAllocatorState>>,
}

impl Deref for ScratchReservation {
    type Target = [RegionAllocation];

    fn deref(&self) -> &Self::Target {
        self.allocations.as_slice()
    }
}

impl Drop for ScratchReservation {
    fn drop(&mut self) {
        let mut state = self.state.lock();
        state.release_all(&self.reservations);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RegionAllocation {
    pub addr: *mut c_void,
}

fn storage_target_chunks(
    base: *mut c_void,
    capacity: usize,
    max_registration_bytes: Option<usize>,
) -> Result<Vec<SegmentTargetChunk>> {
    let base_addr = base as usize;
    registration_chunks(base, capacity, max_registration_bytes)?
        .into_iter()
        .map(|(chunk_addr, chunk_len)| {
            let chunk_base = chunk_addr as usize;
            let logical_offset = chunk_base.checked_sub(base_addr).ok_or_else(|| {
                StoreError::Allocator("storage registration chunk precedes region base".to_string())
            })?;
            Ok(SegmentTargetChunk {
                logical_offset: logical_offset as u64,
                target_offset: chunk_base as u64,
                length_bytes: chunk_len as u64,
            })
        })
        .collect()
}

#[derive(Debug)]
pub(crate) struct RegisteredRegion {
    base_addr: usize,
    capacity: usize,
    alignment: usize,
    target_chunks: Vec<SegmentTargetChunk>,
    owner: RegionOwner,
}

impl RegisteredRegion {
    fn base_ptr(&self) -> *mut c_void {
        self.base_addr as *mut c_void
    }

    fn register(
        transport: &dyn StoreTransport,
        capacity: usize,
        location: &str,
        alignment: usize,
        hugepage: Option<HugePageConfig>,
    ) -> Result<Self> {
        Self::register_with_mode(transport, capacity, location, alignment, hugepage, false)
    }

    pub(crate) fn register_startup_storage(
        transport: &dyn StoreTransport,
        capacity: usize,
        location: &str,
        alignment: usize,
        hugepage: Option<HugePageConfig>,
    ) -> Result<Self> {
        Self::register_with_mode(transport, capacity, location, alignment, hugepage, true)
    }

    fn register_with_mode(
        transport: &dyn StoreTransport,
        capacity: usize,
        location: &str,
        alignment: usize,
        hugepage: Option<HugePageConfig>,
        startup_storage: bool,
    ) -> Result<Self> {
        let alignment = alignment.max(1);
        let (base, capacity, owner) = match hugepage {
            Some(hugepage) => allocate_hugepage_region(capacity, alignment, location, hugepage)?,
            None => (
                transport.allocate_memory(capacity, location)?,
                capacity,
                RegionOwner::Transport,
            ),
        };
        if let Err(error) = transport.adopt_local_memory(base, capacity, location) {
            let _ = owner.release(transport, base);
            return Err(error);
        }
        let registrations = [MemoryRegistration {
            addr: base,
            size: capacity,
        }];
        let registration_result = if startup_storage {
            transport.register_startup_memory_batch(&registrations)
        } else {
            transport.register_memory(base, capacity)
        };
        if let Err(error) = registration_result {
            let _ = owner.release(transport, base);
            return Err(error);
        }
        let target_chunks =
            storage_target_chunks(base, capacity, transport.max_registration_bytes())?;
        Ok(Self {
            base_addr: base as usize,
            capacity,
            alignment,
            target_chunks,
            owner,
        })
    }

    #[cfg(test)]
    fn plan(&self, cursor: usize, length: usize) -> Result<RegionAllocation> {
        self.reserve(cursor, length)
            .map(|(allocation, _)| allocation)
    }

    #[cfg(test)]
    fn reserve(&self, cursor: usize, length: usize) -> Result<(RegionAllocation, usize)> {
        if length == 0 {
            return Err(StoreError::Allocator(
                "zero-length allocations are not supported".to_string(),
            ));
        }
        let reserved_len = align_up(length, self.alignment);
        let offset = align_up(cursor, self.alignment);
        let end = offset
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("allocation overflow".to_string()))?;
        if end > self.capacity {
            return Err(StoreError::Allocator(format!(
                "region capacity exhausted: requested={length} remaining={}",
                self.capacity.saturating_sub(offset)
            )));
        }
        Ok((self.allocation_at(offset)?, end))
    }

    fn allocation_at(&self, offset: usize) -> Result<RegionAllocation> {
        let addr = unsafe { (self.base_addr as *mut u8).add(offset).cast::<c_void>() };
        Ok(RegionAllocation { addr })
    }

    fn address_at(&self, offset: usize) -> Result<*mut c_void> {
        if offset >= self.capacity {
            return Err(StoreError::Allocator(format!(
                "offset {offset} exceeds region capacity {}",
                self.capacity
            )));
        }
        Ok(unsafe { (self.base_addr as *mut u8).add(offset).cast::<c_void>() })
    }

    fn build_copy_instructions(
        self: &Arc<Self>,
        target_info: &SegmentInfo,
        target_offset: u64,
        length: usize,
        max_registration_bytes: Option<usize>,
    ) -> Result<PreparedCopy> {
        let region_guard = Arc::clone(self);
        if length == 0 {
            return Ok(PreparedCopy {
                _region_guard: region_guard,
                instructions: Vec::new(),
            });
        }
        let mut mappings =
            self.target_mappings(target_info, target_offset, length, max_registration_bytes)?;
        mappings.sort_by_key(|mapping| mapping.target_start);

        let total_len = u64::try_from(length).map_err(|_| {
            StoreError::Allocator("copy length does not fit target coordinate".to_string())
        })?;
        let target_end = target_offset
            .checked_add(total_len)
            .ok_or_else(|| StoreError::Allocator("target range overflow".to_string()))?;
        let mut instructions = Vec::new();
        let mut cursor = target_offset;
        let mut written = 0usize;
        while cursor < target_end {
            let mapping = mappings
                .iter()
                .find(|mapping| cursor >= mapping.target_start && cursor < mapping.target_end)
                .ok_or_else(|| {
                    StoreError::Allocator(format!("target cursor {cursor} is outside target map"))
                })?;
            let target_available = mapping.target_end - cursor;
            let remaining = target_end - cursor;
            let copy_len = usize::try_from(target_available.min(remaining)).map_err(|_| {
                StoreError::Allocator("target copy span does not fit usize".to_string())
            })?;
            let local_offset = usize::try_from(cursor - mapping.target_start).map_err(|_| {
                StoreError::Allocator("target-to-local offset does not fit usize".to_string())
            })?;
            let source = mapping
                .local_start
                .checked_add(local_offset)
                .ok_or_else(|| StoreError::Allocator("local source overflow".to_string()))?
                as *const u8;
            instructions.push(CopyInstruction {
                source,
                dest_offset: written,
                copy_len,
            });
            written = written
                .checked_add(copy_len)
                .ok_or_else(|| StoreError::Allocator("destination offset overflow".to_string()))?;
            cursor = cursor
                .checked_add(copy_len as u64)
                .ok_or_else(|| StoreError::Allocator("target cursor overflow".to_string()))?;
        }
        Ok(PreparedCopy {
            _region_guard: region_guard,
            instructions,
        })
    }

    fn copy_target_to(
        self: &Arc<Self>,
        target_info: &SegmentInfo,
        target_offset: u64,
        destination: *mut u8,
        length: usize,
        max_registration_bytes: Option<usize>,
    ) -> Result<()> {
        let instructions = self.build_copy_instructions(
            target_info,
            target_offset,
            length,
            max_registration_bytes,
        )?;
        execute_copy_instructions(&instructions, destination);
        Ok(())
    }

    fn target_mappings(
        &self,
        target_info: &SegmentInfo,
        target_offset: u64,
        length: usize,
        max_registration_bytes: Option<usize>,
    ) -> Result<Vec<TargetMapping>> {
        let local_chunks =
            registration_chunks(self.base_ptr(), self.capacity, max_registration_bytes)?;
        let mut target_buffers = target_info.buffers.iter().collect::<Vec<_>>();
        target_buffers.sort_by_key(|buffer| buffer.base);
        if local_chunks.is_empty() || target_buffers.len() < local_chunks.len() {
            return Err(StoreError::Allocator(format!(
                "local chunk count {} cannot match target buffer count {}",
                local_chunks.len(),
                target_buffers.len()
            )));
        }
        let target_len = u64::try_from(length)
            .map_err(|_| StoreError::Allocator("copy length does not fit u64".to_string()))?;
        let target_end = target_offset
            .checked_add(target_len)
            .ok_or_else(|| StoreError::Allocator("target range overflow".to_string()))?;

        let mut selected = None;
        for window in target_buffers.windows(local_chunks.len()) {
            let Some(mappings) = build_target_mappings(&local_chunks, window)? else {
                continue;
            };
            if target_range_covered_by_mappings(&mappings, target_offset, target_end) {
                if selected.is_some() {
                    return Err(StoreError::Allocator(
                        "target range matches multiple local storage extents".to_string(),
                    ));
                }
                selected = Some(mappings);
            }
        }
        selected.ok_or_else(|| {
            StoreError::Allocator(format!(
                "target range [{target_offset}, {target_end}) does not match local storage extent \
                 local_chunks={:?} target_buffers={:?}",
                local_chunks
                    .iter()
                    .map(|(addr, len)| (*addr as usize, *len))
                    .collect::<Vec<_>>(),
                target_buffers
                    .iter()
                    .map(|buffer| (buffer.base, buffer.length))
                    .collect::<Vec<_>>()
            ))
        })
    }

    pub(crate) fn release(self, transport: &dyn StoreTransport) -> Result<()> {
        let base = self.base_ptr();
        transport.unregister_memory(base, self.capacity)?;
        self.owner.release(transport, base)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedCopy {
    _region_guard: Arc<RegisteredRegion>,
    instructions: Vec<CopyInstruction>,
}

#[derive(Clone, Debug)]
struct CopyInstruction {
    source: *const u8,
    dest_offset: usize,
    copy_len: usize,
}

/// Execute pre-computed copy instructions into `destination`.
///
/// # Safety
/// The prepared copy keeps its source region alive. Callers must ensure that
/// `destination` is large enough for every instruction.
pub(crate) fn execute_copy_instructions(copy: &PreparedCopy, destination: *mut u8) {
    for instr in &copy.instructions {
        unsafe {
            ptr::copy_nonoverlapping(
                instr.source,
                destination.add(instr.dest_offset),
                instr.copy_len,
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct TargetMapping {
    target_start: u64,
    target_end: u64,
    local_start: usize,
}

fn build_target_mappings(
    local_chunks: &[(*mut c_void, usize)],
    target_buffers: &[&mooncake_transport::SegmentBuffer],
) -> Result<Option<Vec<TargetMapping>>> {
    let mut mappings = Vec::with_capacity(local_chunks.len());
    for ((local_addr, local_len), target_buffer) in local_chunks.iter().zip(target_buffers.iter()) {
        let target_len = usize::try_from(target_buffer.length).map_err(|_| {
            StoreError::Allocator("target buffer length does not fit usize".to_string())
        })?;
        if *local_len != target_len {
            return Ok(None);
        }
        let target_end = target_buffer
            .base
            .checked_add(target_buffer.length)
            .ok_or_else(|| StoreError::Allocator("target buffer end overflow".to_string()))?;
        mappings.push(TargetMapping {
            target_start: target_buffer.base,
            target_end,
            local_start: *local_addr as usize,
        });
    }
    Ok(Some(mappings))
}

fn target_range_covered_by_mappings(
    mappings: &[TargetMapping],
    target_start: u64,
    target_end: u64,
) -> bool {
    let mut cursor = target_start;
    while cursor < target_end {
        let Some(mapping) = mappings
            .iter()
            .find(|mapping| cursor >= mapping.target_start && cursor < mapping.target_end)
        else {
            return false;
        };
        if mapping.target_end <= cursor {
            return false;
        }
        cursor = mapping.target_end.min(target_end);
    }
    true
}

#[derive(Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
enum RegionOwner {
    Transport,
    HugePageMmap { mapped_len: usize },
}

impl RegionOwner {
    fn release(self, transport: &dyn StoreTransport, base: *mut c_void) -> Result<()> {
        match self {
            Self::Transport => transport.free_memory(base),
            Self::HugePageMmap { mapped_len } => free_hugepage_region(base, mapped_len),
        }
    }
}

fn allocate_hugepage_region(
    capacity: usize,
    alignment: usize,
    location: &str,
    hugepage: HugePageConfig,
) -> Result<(*mut c_void, usize, RegionOwner)> {
    if capacity == 0 {
        return Err(StoreError::Allocator(
            "hugepage allocation size must be greater than zero".to_string(),
        ));
    }
    if !(location == "*" || location.starts_with("cpu")) {
        return Err(StoreError::Unsupported(format!(
            "hugepage local memory only supports cpu locations, got {location}"
        )));
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (alignment, hugepage);
        return Err(StoreError::Unsupported(
            "hugepage local memory is only supported on Linux".to_string(),
        ));
    }

    #[cfg(target_os = "linux")]
    {
        let effective_alignment = alignment.max(hugepage.bytes());
        let mapped_len = align_up(capacity, effective_alignment);
        let flags = libc::MAP_PRIVATE
            | libc::MAP_ANONYMOUS
            | libc::MAP_POPULATE
            | libc::MAP_HUGETLB
            | hugepage_map_flag(hugepage);
        let base = unsafe {
            libc::mmap(
                ptr::null_mut(),
                mapped_len,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(StoreError::Allocator(format!(
                "hugepage mmap failed for {} (size={}): {} (check /proc/sys/vm/nr_hugepages)",
                hugepage.label(),
                mapped_len,
                std::io::Error::last_os_error()
            )));
        }
        bind_mapped_region_to_location(base, mapped_len, location);
        Ok((base, mapped_len, RegionOwner::HugePageMmap { mapped_len }))
    }
}

fn free_hugepage_region(base: *mut c_void, mapped_len: usize) -> Result<()> {
    let rc = unsafe { libc::munmap(base, mapped_len) };
    if rc == 0 {
        return Ok(());
    }
    Err(StoreError::Allocator(format!(
        "munmap hugepage region failed: {}",
        std::io::Error::last_os_error()
    )))
}

#[cfg(target_os = "linux")]
fn hugepage_map_flag(hugepage: HugePageConfig) -> i32 {
    match hugepage.bytes() {
        size if size == 2 * 1024 * 1024 => libc::MAP_HUGE_2MB,
        size if size == 1024 * 1024 * 1024 => libc::MAP_HUGE_1GB,
        _ => 0,
    }
}

fn release_registered_regions(
    transport: &dyn StoreTransport,
    regions: Vec<RegisteredRegion>,
) -> Result<()> {
    let mut first_error = None;
    for region in regions.into_iter().rev() {
        if let Err(error) = region.release(transport) {
            first_error.get_or_insert(error);
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

fn is_host_memory_location(location: &str) -> bool {
    location == "*" || parse_cpu_location(location).is_some()
}

fn parse_cpu_location(location: &str) -> Option<i32> {
    let node = location.strip_prefix("cpu:")?.parse::<i32>().ok()?;
    (node >= 0).then_some(node)
}

fn discovered_cpu_locations() -> Vec<String> {
    #[cfg(test)]
    if let Some(locations) = test_numa_locations() {
        return locations;
    }
    discovered_cpu_locations_impl()
}

#[cfg(target_os = "linux")]
fn discovered_cpu_locations_impl() -> Vec<String> {
    if !linux_numa_available() {
        return Vec::new();
    }
    let configured = configured_linux_numa_nodes();
    let allowed = current_process_allowed_numa_nodes();
    let nodes = match (configured.is_empty(), allowed) {
        (_, Some(allowed)) if allowed.is_empty() => Vec::new(),
        (true, Some(allowed)) => allowed,
        (false, Some(allowed)) => configured
            .into_iter()
            .filter(|node| allowed.binary_search(node).is_ok())
            .collect(),
        (false, None) => configured,
        (true, None) => Vec::new(),
    };
    nodes
        .into_iter()
        .map(|node| format!("cpu:{node}"))
        .collect()
}

#[cfg(target_os = "linux")]
fn configured_linux_numa_nodes() -> Vec<i32> {
    let count = unsafe { linux_numa::numa_num_configured_nodes() };
    if count <= 0 {
        return Vec::new();
    }
    (0..count).collect()
}

#[cfg(target_os = "linux")]
fn current_process_allowed_numa_nodes() -> Option<Vec<i32>> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("Mems_allowed_list:")
            .and_then(parse_numa_node_list)
    })
}

#[cfg(target_os = "linux")]
fn parse_numa_node_list(value: &str) -> Option<Vec<i32>> {
    let mut nodes = Vec::new();
    for part in value.trim().split(',').filter(|part| !part.is_empty()) {
        let (start, end): (i32, i32) = match part.split_once('-') {
            Some((start, end)) => (
                start.trim().parse::<i32>().ok()?,
                end.trim().parse::<i32>().ok()?,
            ),
            None => {
                let node = part.trim().parse::<i32>().ok()?;
                (node, node)
            }
        };
        if start > end || start < 0 {
            return None;
        }
        nodes.extend(start..=end);
    }
    nodes.sort_unstable();
    nodes.dedup();
    Some(nodes)
}

#[cfg(not(target_os = "linux"))]
fn discovered_cpu_locations_impl() -> Vec<String> {
    Vec::new()
}

#[cfg(target_os = "linux")]
fn bind_mapped_region_to_location(base: *mut c_void, size: usize, location: &str) {
    let Some(node) = parse_cpu_location(location) else {
        return;
    };
    bind_mapped_region_to_numa_node(base, size, node);
}

#[cfg(target_os = "linux")]
fn bind_mapped_region_to_numa_node(base: *mut c_void, size: usize, node: i32) {
    if !linux_numa_available() {
        return;
    }
    unsafe { linux_numa::numa_tonode_memory(base, size, node) };
}

#[cfg(target_os = "linux")]
fn linux_numa_available() -> bool {
    unsafe { linux_numa::numa_available() >= 0 }
}

#[cfg(target_os = "linux")]
mod linux_numa {
    use std::ffi::c_void;

    #[link(name = "numa")]
    extern "C" {
        pub fn numa_available() -> i32;
        pub fn numa_num_configured_nodes() -> i32;
        pub fn numa_tonode_memory(start: *mut c_void, size: usize, node: i32);
    }
}

#[cfg(test)]
static TEST_NUMA_LOCATIONS: std::sync::Mutex<Option<Vec<String>>> = std::sync::Mutex::new(None);
#[cfg(test)]
static TEST_NUMA_GUARD: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
fn test_numa_locations() -> Option<Vec<String>> {
    TEST_NUMA_LOCATIONS
        .lock()
        .ok()
        .and_then(|guard| guard.clone())
}

#[cfg(test)]
pub(crate) fn with_test_numa_locations<T>(locations: &[&str], f: impl FnOnce() -> T) -> T {
    let _guard = TEST_NUMA_GUARD
        .lock()
        .expect("test numa guard lock should not be poisoned");
    {
        let mut guard = TEST_NUMA_LOCATIONS
            .lock()
            .expect("test numa locations lock should not be poisoned");
        *guard = Some(
            locations
                .iter()
                .map(|location| (*location).to_string())
                .collect(),
        );
    }
    let result = f();
    let mut guard = TEST_NUMA_LOCATIONS
        .lock()
        .expect("test numa locations lock should not be poisoned");
    *guard = None;
    result
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::c_void;
    use std::sync::Arc;

    use mooncake_store_core::{HugePageConfig, SegmentLifecycleState, SegmentName, StoreError};
    use mooncake_transport::{
        SegmentBuffer, SegmentInfo, SegmentKind, TransferProgress, TransferRequest, TransferStatus,
    };
    use parking_lot::Mutex;

    use super::{
        align_up, allocate_hugepage_region, free_hugepage_region, hugepage_map_flag,
        with_test_numa_locations, LocalMemoryConfig, LocalMemoryState, LocalRegionPlan,
        MemoryRegistration, RegionOwner, RegisteredRegion, StorageSegmentSpec, StoreTransport,
    };

    struct NoopTransport;

    #[derive(Default, Clone)]
    struct RecordingTransport {
        inner: Arc<Mutex<RecordingTransportState>>,
        max_registration_bytes: Option<usize>,
    }

    #[derive(Default)]
    struct RecordingTransportState {
        allocations: BTreeMap<usize, Box<[u8]>>,
        registered: BTreeMap<usize, usize>,
        register_calls: usize,
        startup_batch_calls: usize,
        startup_batch_entry_sizes: Vec<usize>,
    }

    #[derive(Clone)]
    struct FailingTransport {
        inner: Arc<Mutex<FailingTransportState>>,
        fail_adopt: bool,
        fail_register: bool,
    }

    #[derive(Default)]
    struct FailingTransportState {
        allocations: BTreeMap<usize, Box<[u8]>>,
    }

    impl StoreTransport for NoopTransport {
        fn segment_name(&self) -> mooncake_store_core::Result<String> {
            Err(mooncake_store_core::StoreError::Unsupported(
                "unused in hugepage test".to_string(),
            ))
        }

        fn rpc_server_address(&self) -> mooncake_store_core::Result<(String, u16)> {
            Err(mooncake_store_core::StoreError::Unsupported(
                "unused in hugepage test".to_string(),
            ))
        }

        fn open_segment(&self, _segment_name: &str) -> mooncake_store_core::Result<u64> {
            Err(mooncake_store_core::StoreError::Unsupported(
                "unused in hugepage test".to_string(),
            ))
        }

        fn close_segment(&self, _handle: u64) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn get_segment_info(&self, _handle: u64) -> mooncake_store_core::Result<SegmentInfo> {
            Err(mooncake_store_core::StoreError::Unsupported(
                "unused in hugepage test".to_string(),
            ))
        }

        fn allocate_memory(
            &self,
            _size: usize,
            _location: &str,
        ) -> mooncake_store_core::Result<*mut c_void> {
            Err(mooncake_store_core::StoreError::Unsupported(
                "unused in hugepage test".to_string(),
            ))
        }

        fn free_memory(&self, _addr: *mut c_void) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn register_memory(
            &self,
            _addr: *mut c_void,
            _size: usize,
        ) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn unregister_memory(
            &self,
            _addr: *mut c_void,
            _size: usize,
        ) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn allocate_batch(&self, _batch_size: usize) -> mooncake_store_core::Result<u64> {
            Err(mooncake_store_core::StoreError::Unsupported(
                "unused in hugepage test".to_string(),
            ))
        }

        fn free_batch(&self, _batch_id: u64) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn submit(
            &self,
            _batch_id: u64,
            _requests: &[TransferRequest],
        ) -> mooncake_store_core::Result<()> {
            Err(mooncake_store_core::StoreError::Unsupported(
                "unused in hugepage test".to_string(),
            ))
        }

        fn task_status(
            &self,
            _batch_id: u64,
            _task_id: usize,
        ) -> mooncake_store_core::Result<TransferProgress> {
            Ok(TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            })
        }

        fn overall_status(&self, _batch_id: u64) -> mooncake_store_core::Result<TransferProgress> {
            Ok(TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            })
        }
    }

    impl StoreTransport for RecordingTransport {
        fn segment_name(&self) -> mooncake_store_core::Result<String> {
            Ok("recording-segment".to_string())
        }

        fn rpc_server_address(&self) -> mooncake_store_core::Result<(String, u16)> {
            Ok(("127.0.0.1".to_string(), 0))
        }

        fn open_segment(&self, _segment_name: &str) -> mooncake_store_core::Result<u64> {
            Ok(1)
        }

        fn close_segment(&self, _handle: u64) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn get_segment_info(&self, _handle: u64) -> mooncake_store_core::Result<SegmentInfo> {
            Err(StoreError::Unsupported(
                "unused in memory tests".to_string(),
            ))
        }

        fn adopt_local_memory(
            &self,
            _addr: *mut c_void,
            _size: usize,
            _location: &str,
        ) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn allocate_memory(
            &self,
            size: usize,
            _location: &str,
        ) -> mooncake_store_core::Result<*mut c_void> {
            let mut memory = vec![0u8; size].into_boxed_slice();
            let base = memory.as_mut_ptr() as usize;
            self.inner.lock().allocations.insert(base, memory);
            Ok(base as *mut c_void)
        }

        fn free_memory(&self, addr: *mut c_void) -> mooncake_store_core::Result<()> {
            let base = addr as usize;
            self.inner
                .lock()
                .allocations
                .remove(&base)
                .ok_or_else(|| StoreError::NotFound(format!("allocation {base:#x} not found")))?;
            Ok(())
        }

        fn max_registration_bytes(&self) -> Option<usize> {
            self.max_registration_bytes
        }

        fn register_memory(
            &self,
            addr: *mut c_void,
            size: usize,
        ) -> mooncake_store_core::Result<()> {
            let mut state = self.inner.lock();
            state.register_calls += 1;
            state.registered.insert(addr as usize, size);
            Ok(())
        }

        fn register_startup_memory_batch(
            &self,
            entries: &[MemoryRegistration],
        ) -> mooncake_store_core::Result<()> {
            let mut state = self.inner.lock();
            state.startup_batch_calls += 1;
            state
                .startup_batch_entry_sizes
                .push(entries.iter().map(|entry| entry.size).sum());
            for entry in entries {
                state.registered.insert(entry.addr as usize, entry.size);
            }
            Ok(())
        }

        fn unregister_memory(
            &self,
            addr: *mut c_void,
            size: usize,
        ) -> mooncake_store_core::Result<()> {
            let base = addr as usize;
            match self.inner.lock().registered.remove(&base) {
                Some(recorded) if recorded == size => Ok(()),
                Some(recorded) => Err(StoreError::Allocator(format!(
                    "registered size mismatch: expected={recorded} actual={size}"
                ))),
                None => Err(StoreError::NotFound(format!(
                    "registered allocation {base:#x} not found"
                ))),
            }
        }

        fn allocate_batch(&self, _batch_size: usize) -> mooncake_store_core::Result<u64> {
            Ok(1)
        }

        fn free_batch(&self, _batch_id: u64) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn submit(
            &self,
            _batch_id: u64,
            _requests: &[TransferRequest],
        ) -> mooncake_store_core::Result<()> {
            Err(StoreError::Unsupported(
                "unused in memory tests".to_string(),
            ))
        }

        fn task_status(
            &self,
            _batch_id: u64,
            _task_id: usize,
        ) -> mooncake_store_core::Result<TransferProgress> {
            Ok(TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            })
        }

        fn overall_status(&self, _batch_id: u64) -> mooncake_store_core::Result<TransferProgress> {
            Ok(TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            })
        }
    }

    impl FailingTransport {
        fn adopt_failure() -> Self {
            Self {
                inner: Arc::new(Mutex::new(FailingTransportState::default())),
                fail_adopt: true,
                fail_register: false,
            }
        }

        fn register_failure() -> Self {
            Self {
                inner: Arc::new(Mutex::new(FailingTransportState::default())),
                fail_adopt: false,
                fail_register: true,
            }
        }
    }

    impl StoreTransport for FailingTransport {
        fn segment_name(&self) -> mooncake_store_core::Result<String> {
            Err(StoreError::Unsupported(
                "unused in failure tests".to_string(),
            ))
        }

        fn rpc_server_address(&self) -> mooncake_store_core::Result<(String, u16)> {
            Err(StoreError::Unsupported(
                "unused in failure tests".to_string(),
            ))
        }

        fn open_segment(&self, _segment_name: &str) -> mooncake_store_core::Result<u64> {
            Err(StoreError::Unsupported(
                "unused in failure tests".to_string(),
            ))
        }

        fn close_segment(&self, _handle: u64) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn get_segment_info(&self, _handle: u64) -> mooncake_store_core::Result<SegmentInfo> {
            Err(StoreError::Unsupported(
                "unused in failure tests".to_string(),
            ))
        }

        fn adopt_local_memory(
            &self,
            _addr: *mut c_void,
            _size: usize,
            _location: &str,
        ) -> mooncake_store_core::Result<()> {
            if self.fail_adopt {
                return Err(StoreError::Transport("adopt failed".to_string()));
            }
            Ok(())
        }

        fn allocate_memory(
            &self,
            size: usize,
            _location: &str,
        ) -> mooncake_store_core::Result<*mut c_void> {
            let mut memory = vec![0u8; size].into_boxed_slice();
            let base = memory.as_mut_ptr() as usize;
            self.inner.lock().allocations.insert(base, memory);
            Ok(base as *mut c_void)
        }

        fn free_memory(&self, addr: *mut c_void) -> mooncake_store_core::Result<()> {
            self.inner
                .lock()
                .allocations
                .remove(&(addr as usize))
                .ok_or_else(|| StoreError::NotFound("allocation not found".to_string()))?;
            Ok(())
        }

        fn register_memory(
            &self,
            _addr: *mut c_void,
            _size: usize,
        ) -> mooncake_store_core::Result<()> {
            if self.fail_register {
                return Err(StoreError::Transport("register failed".to_string()));
            }
            Ok(())
        }

        fn unregister_memory(
            &self,
            _addr: *mut c_void,
            _size: usize,
        ) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn allocate_batch(&self, _batch_size: usize) -> mooncake_store_core::Result<u64> {
            Ok(1)
        }

        fn free_batch(&self, _batch_id: u64) -> mooncake_store_core::Result<()> {
            Ok(())
        }

        fn submit(
            &self,
            _batch_id: u64,
            _requests: &[TransferRequest],
        ) -> mooncake_store_core::Result<()> {
            Err(StoreError::Unsupported(
                "unused in failure tests".to_string(),
            ))
        }

        fn task_status(
            &self,
            _batch_id: u64,
            _task_id: usize,
        ) -> mooncake_store_core::Result<TransferProgress> {
            Ok(TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            })
        }

        fn overall_status(&self, _batch_id: u64) -> mooncake_store_core::Result<TransferProgress> {
            Ok(TransferProgress {
                status: TransferStatus::Completed,
                transferred_bytes: 0,
            })
        }
    }

    fn hugepages_available(bytes: usize) -> bool {
        let path = if bytes == 2 * 1024 * 1024 {
            "/sys/kernel/mm/hugepages/hugepages-2048kB/free_hugepages"
        } else if bytes == 1024 * 1024 * 1024 {
            "/sys/kernel/mm/hugepages/hugepages-1048576kB/free_hugepages"
        } else {
            return false;
        };
        std::fs::read_to_string(path)
            .ok()
            .and_then(|value| value.trim().parse::<usize>().ok())
            .is_some_and(|count| count > 0)
    }

    #[test]
    fn alignment_rounds_up_without_branch_explosion() {
        assert_eq!(align_up(1, 64), 64);
        assert_eq!(align_up(64, 64), 64);
        assert_eq!(align_up(65, 64), 128);
    }

    #[test]
    fn local_memory_config_resolves_explicit_hugepage() {
        let config = LocalMemoryConfig::new()
            .numa_aware(false)
            .use_hugepage(true)
            .hugepage_size_bytes(2 * 1024 * 1024);
        let hugepage = config
            .hugepage()
            .expect("hugepage config should resolve")
            .expect("hugepage config should be enabled");
        assert_eq!(
            hugepage,
            HugePageConfig::new(2 * 1024 * 1024).expect("2MB is supported")
        );
    }

    #[test]
    fn native_hugepage_allocator_uses_real_hugetlb_when_available() {
        let hugepage = HugePageConfig::new(2 * 1024 * 1024).expect("2MB is supported");
        if !hugepages_available(hugepage.bytes()) {
            return;
        }
        let (base, mapped_len, owner) = allocate_hugepage_region(4096, 64, "cpu:0", hugepage)
            .expect("hugetlb mmap should succeed");
        assert_eq!(mapped_len, hugepage.bytes());
        assert!(matches!(owner, RegionOwner::HugePageMmap { .. }));
        owner
            .release(&NoopTransport, base)
            .expect("hugetlb region should unmap cleanly");
    }

    #[test]
    fn local_memory_config_validate_rejects_invalid_inputs() {
        LocalMemoryConfig::new()
            .numa_aware(false)
            .storage_bytes(0)
            .validate()
            .expect("zero storage should be allowed for rw-only clients");

        let error = LocalMemoryConfig::new()
            .numa_aware(false)
            .scratch_bytes(0)
            .validate()
            .expect_err("zero scratch must fail");
        assert!(matches!(error, StoreError::Allocator(_)));

        let error = LocalMemoryConfig::new()
            .numa_aware(false)
            .location("")
            .validate()
            .expect_err("empty location must fail");
        assert!(matches!(error, StoreError::Allocator(_)));
    }

    #[test]
    fn local_memory_config_builder_keeps_tags_and_alignment_floor() {
        let config = LocalMemoryConfig::new()
            .numa_aware(false)
            .tags(vec!["nvme".to_string(), "warm".to_string()])
            .alignment(0)
            .reclaim_grace_ms(7);
        assert_eq!(config.tags, vec!["nvme".to_string(), "warm".to_string()]);
        assert_eq!(config.alignment, 1);
        assert_eq!(config.reclaim_grace_ms, 7);
    }

    #[test]
    fn local_memory_config_distributes_regions_across_test_numa_nodes() {
        with_test_numa_locations(&["cpu:0", "cpu:1"], || {
            let config = LocalMemoryConfig::new()
                .storage_bytes(10)
                .scratch_bytes(6)
                .location("cpu:0")
                .numa_aware(true);
            assert_eq!(
                config
                    .storage_region_plans(Some(3))
                    .expect("storage plans should succeed"),
                vec![
                    LocalRegionPlan {
                        capacity_bytes: 3,
                        location: "cpu:0".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 3,
                        location: "cpu:1".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 2,
                        location: "cpu:0".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 2,
                        location: "cpu:1".to_string(),
                    },
                ]
            );
            assert_eq!(
                config
                    .scratch_region_plans(None)
                    .expect("scratch plans should succeed"),
                vec![
                    LocalRegionPlan {
                        capacity_bytes: 3,
                        location: "cpu:0".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 3,
                        location: "cpu:1".to_string(),
                    },
                ]
            );
        });
    }

    #[test]
    fn local_memory_config_honors_preferred_numa_start_node() {
        with_test_numa_locations(&["cpu:0", "cpu:1", "cpu:2"], || {
            let config = LocalMemoryConfig::new()
                .storage_bytes(9)
                .scratch_bytes(3)
                .location("cpu:1")
                .numa_aware(true);

            assert_eq!(
                config
                    .storage_region_plans(Some(2))
                    .expect("storage plans should succeed"),
                vec![
                    LocalRegionPlan {
                        capacity_bytes: 2,
                        location: "cpu:1".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 2,
                        location: "cpu:2".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 2,
                        location: "cpu:0".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 1,
                        location: "cpu:1".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 1,
                        location: "cpu:2".to_string(),
                    },
                    LocalRegionPlan {
                        capacity_bytes: 1,
                        location: "cpu:0".to_string(),
                    },
                ]
            );
        });
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn numa_node_list_parser_handles_cgroup_ranges() {
        assert_eq!(super::parse_numa_node_list("0"), Some(vec![0]));
        assert_eq!(super::parse_numa_node_list("0-2,4"), Some(vec![0, 1, 2, 4]));
        assert_eq!(super::parse_numa_node_list("2,0-1,2"), Some(vec![0, 1, 2]));
        assert_eq!(super::parse_numa_node_list("3-1"), None);
        assert_eq!(super::parse_numa_node_list("bad"), None);
    }

    #[test]
    fn registered_region_plan_rejects_zero_length_and_overflow() {
        let region = RegisteredRegion {
            base_addr: 0x1000,
            capacity: 64,
            alignment: 16,
            target_chunks: Vec::new(),
            owner: RegionOwner::Transport,
        };

        let error = region
            .plan(0, 0)
            .expect_err("zero-length allocations are invalid");
        assert!(matches!(error, StoreError::Allocator(_)));

        let allocation = region
            .plan(7, 15)
            .expect("aligned allocation should succeed");
        assert_eq!(allocation.addr as usize, 0x1010);

        let error = region
            .plan(64, 16)
            .expect_err("exhausted region should fail");
        assert!(matches!(error, StoreError::Allocator(_)));

        let error = region
            .address_at(64)
            .expect_err("address_at should enforce capacity");
        assert!(matches!(error, StoreError::Allocator(_)));
    }

    #[test]
    fn registered_region_copies_from_transport_target_map() {
        let mut storage = vec![0u8; 64];
        let payload = b"mapped-target";
        storage[16..16 + payload.len()].copy_from_slice(payload);
        let region = Arc::new(RegisteredRegion {
            base_addr: storage.as_mut_ptr() as usize,
            capacity: storage.len(),
            alignment: 8,
            target_chunks: Vec::new(),
            owner: RegionOwner::Transport,
        });
        let target_base = 0xabc0_0000u64;
        let target_info = SegmentInfo {
            kind: SegmentKind::Memory,
            buffers: vec![
                SegmentBuffer {
                    base: target_base - 0x1000,
                    length: 8,
                    location: "cpu:0".to_string(),
                },
                SegmentBuffer {
                    base: target_base,
                    length: storage.len() as u64,
                    location: "cpu:0".to_string(),
                },
            ],
        };
        let mut out = vec![0u8; payload.len()];

        region
            .copy_target_to(
                &target_info,
                target_base + 16,
                out.as_mut_ptr(),
                out.len(),
                None,
            )
            .expect("target map should translate to local storage");

        assert_eq!(out, payload);
    }

    #[test]
    fn local_memory_state_does_not_release_segment_with_active_copy_instructions() {
        let transport = RecordingTransport::default();
        let primary = SegmentName::new("primary");
        let mut state = LocalMemoryState::register(
            &transport,
            &primary,
            &LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(128)
                .scratch_bytes(64)
                .alignment(8),
        )
        .expect("initial local memory registration should succeed");
        let segment = state
            .storage_segments()
            .into_iter()
            .find(|segment| segment.segment_name == primary)
            .expect("primary segment should be present");
        let target_info = SegmentInfo {
            kind: SegmentKind::Memory,
            buffers: segment
                .target_chunks
                .iter()
                .map(|chunk| SegmentBuffer {
                    base: chunk.target_offset,
                    length: chunk.length_bytes,
                    location: "cpu:0".to_string(),
                })
                .collect(),
        };
        let instructions = state
            .prepare_storage_copy_instructions(
                &primary,
                &target_info,
                segment.target_chunks[0].target_offset,
                8,
                transport.max_registration_bytes(),
            )
            .expect("copy instructions should keep the region alive");

        let error = state
            .remove_storage_segment(&transport, &primary)
            .expect_err("active copy instructions must block segment release");
        assert!(matches!(error, StoreError::InvalidState(_)));
        assert!(state.has_storage_segment(&primary));

        drop(instructions);
        state
            .remove_storage_segment(&transport, &primary)
            .expect("segment release should succeed after copy instructions drop");
        state
            .release_scratch(&transport)
            .expect("scratch release should succeed");
    }

    #[test]
    fn registered_region_register_rolls_back_allocations_on_transport_failures() {
        let adopt_failure = FailingTransport::adopt_failure();
        let error = RegisteredRegion::register(&adopt_failure, 64, "cpu:0", 8, None)
            .expect_err("adopt failures must roll back transport allocations");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(adopt_failure.inner.lock().allocations.is_empty());

        let register_failure = FailingTransport::register_failure();
        let error = RegisteredRegion::register(&register_failure, 64, "cpu:0", 8, None)
            .expect_err("register failures must roll back transport allocations");
        assert!(matches!(error, StoreError::Transport(_)));
        assert!(register_failure.inner.lock().allocations.is_empty());
    }

    #[test]
    fn registered_region_startup_storage_uses_startup_batch_hook() {
        let transport = RecordingTransport::default();
        let region = RegisteredRegion::register_startup_storage(&transport, 64, "cpu:0", 8, None)
            .expect("startup registration should succeed");
        let state = transport.inner.lock();
        assert_eq!(state.startup_batch_calls, 1);
        assert_eq!(state.startup_batch_entry_sizes, vec![64]);
        assert_eq!(state.register_calls, 0);
        drop(state);
        region
            .release(&transport)
            .expect("startup-registered region should release");
    }

    #[test]
    fn local_memory_state_registers_and_manages_segments() {
        let transport = RecordingTransport::default();
        let primary = SegmentName::new("primary");
        let mut state = LocalMemoryState::register(
            &transport,
            &primary,
            &LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(256)
                .scratch_bytes(128)
                .alignment(16)
                .reclaim_grace_ms(0),
        )
        .expect("initial local memory registration should succeed");

        assert!(state.has_storage_segment(&primary));
        let allocations = state
            .plan_scratch(&[1, 17])
            .expect("scratch planning should succeed");
        assert_eq!(allocations.len(), 2);
        assert_eq!((allocations[1].addr as usize) % 16, 0);
        assert!(
            state
                .storage_address(&primary, 32)
                .expect("storage address should resolve") as usize
                > 0
        );

        state
            .update_storage_state(&primary, SegmentLifecycleState::Draining)
            .expect("state transition should succeed");
        let info = state.storage_segments();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].state, SegmentLifecycleState::Draining);

        let extra = SegmentName::new("extra");
        state
            .add_storage_segment(
                &transport,
                StorageSegmentSpec {
                    segment_name: extra.clone(),
                    capacity_bytes: 64,
                    state: SegmentLifecycleState::Active,
                    tags: vec!["nvme".to_string()],
                    location: "cpu:0".to_string(),
                    alignment: 8,
                    hugepage_enabled: None,
                    hugepage_size_bytes: None,
                },
            )
            .expect("adding a second segment should succeed");
        assert!(state.has_storage_segment(&extra));
        state
            .remove_storage_segment(&transport, &extra)
            .expect("removing extra segment should succeed");
        assert!(!state.has_storage_segment(&extra));

        state
            .remove_storage_segment(&transport, &primary)
            .expect("removing primary segment should succeed");
        state
            .release_scratch(&transport)
            .expect("releasing scratch region should succeed");
        assert!(transport.inner.lock().registered.is_empty());
    }

    #[test]
    fn local_memory_state_splits_scratch_by_registration_limit() {
        let transport = RecordingTransport {
            max_registration_bytes: Some(64),
            ..RecordingTransport::default()
        };
        let primary = SegmentName::new("primary");
        let state = LocalMemoryState::register(
            &transport,
            &primary,
            &LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(0)
                .scratch_bytes(160)
                .alignment(8)
                .reclaim_grace_ms(0),
        )
        .expect("scratch registration should split by MR limit");

        let mut registered_sizes = transport
            .inner
            .lock()
            .registered
            .values()
            .copied()
            .collect::<Vec<_>>();
        registered_sizes.sort_unstable();
        assert_eq!(registered_sizes, vec![32, 64, 64]);
        assert!(state.plan_scratch(&[65]).is_err());
    }

    #[test]
    fn local_memory_state_reports_missing_and_duplicate_segments() {
        let transport = RecordingTransport::default();
        let primary = SegmentName::new("primary");
        let mut state = LocalMemoryState::register(
            &transport,
            &primary,
            &LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(64)
                .scratch_bytes(64)
                .alignment(8),
        )
        .expect("initial registration should succeed");

        let error = state
            .add_storage_segment(
                &transport,
                StorageSegmentSpec {
                    segment_name: primary.clone(),
                    capacity_bytes: 64,
                    state: SegmentLifecycleState::Active,
                    tags: Vec::new(),
                    location: "cpu:0".to_string(),
                    alignment: 8,
                    hugepage_enabled: None,
                    hugepage_size_bytes: None,
                },
            )
            .expect_err("duplicate segment names must be rejected");
        assert!(matches!(error, StoreError::Conflict(_)));

        let missing = SegmentName::new("missing");
        assert!(matches!(
            state.storage_address(&missing, 0),
            Err(StoreError::NotFound(_))
        ));
        assert!(matches!(
            state.update_storage_state(&missing, SegmentLifecycleState::Retired),
            Err(StoreError::NotFound(_))
        ));
        assert!(matches!(
            state.remove_storage_segment(&transport, &missing),
            Err(StoreError::NotFound(_))
        ));

        state
            .remove_storage_segment(&transport, &primary)
            .expect("cleanup should succeed");
        state
            .release_scratch(&transport)
            .expect("scratch cleanup should succeed");
    }

    #[test]
    fn local_memory_state_supports_scratch_only_registration() {
        let transport = RecordingTransport::default();
        let primary = SegmentName::new("primary");
        let state = LocalMemoryState::register(
            &transport,
            &primary,
            &LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(0)
                .scratch_bytes(128)
                .alignment(16),
        )
        .expect("scratch-only registration should succeed");

        assert!(!state.has_storage_segment(&primary));
        assert!(state.storage_segments().is_empty());
        assert!(matches!(
            state.storage_address(&primary, 0),
            Err(StoreError::NotFound(_))
        ));
        assert_eq!(
            state
                .plan_scratch(&[1, 17])
                .expect("scratch planning should still work")
                .len(),
            2
        );
        assert_eq!(transport.inner.lock().registered.len(), 1);

        state
            .release_scratch(&transport)
            .expect("scratch region should release cleanly");
        assert!(transport.inner.lock().registered.is_empty());
    }

    #[test]
    fn local_memory_state_tracks_live_scratch_reservations() {
        let transport = RecordingTransport::default();
        let primary = SegmentName::new("primary");
        let state = LocalMemoryState::register(
            &transport,
            &primary,
            &LocalMemoryConfig::new()
                .numa_aware(false)
                .storage_bytes(0)
                .scratch_bytes(64)
                .alignment(8),
        )
        .expect("scratch-only registration should succeed");

        let first = state
            .plan_scratch(&[32])
            .expect("first scratch reservation should succeed");
        let second = state
            .plan_scratch(&[32])
            .expect("second scratch reservation should use a different slot");
        assert_ne!(first[0].addr as usize, second[0].addr as usize);
        assert!(state.plan_scratch(&[8]).is_err());

        let second_addr = second[0].addr as usize;
        drop(second);
        let third = state
            .plan_scratch(&[32])
            .expect("released scratch slot should be reusable");
        assert_eq!(third[0].addr as usize, second_addr);

        drop(third);
        drop(first);
        state
            .release_scratch(&transport)
            .expect("scratch region should release cleanly");
    }

    #[test]
    fn noop_transport_smoke_contract_covers_stubbed_methods() {
        let transport = NoopTransport;
        assert!(matches!(
            transport.segment_name(),
            Err(StoreError::Unsupported(_))
        ));
        assert!(matches!(
            transport.rpc_server_address(),
            Err(StoreError::Unsupported(_))
        ));
        assert!(matches!(
            transport.open_segment("segment"),
            Err(StoreError::Unsupported(_))
        ));
        transport.close_segment(1).expect("close should be a no-op");
        assert!(matches!(
            transport.get_segment_info(1),
            Err(StoreError::Unsupported(_))
        ));
        assert!(matches!(
            transport.allocate_memory(16, "cpu:0"),
            Err(StoreError::Unsupported(_))
        ));
        transport
            .free_memory(std::ptr::null_mut())
            .expect("free should be a no-op");
        transport
            .register_memory(std::ptr::null_mut(), 0)
            .expect("register should be a no-op");
        transport
            .unregister_memory(std::ptr::null_mut(), 0)
            .expect("unregister should be a no-op");
        assert!(matches!(
            transport.allocate_batch(1),
            Err(StoreError::Unsupported(_))
        ));
        transport
            .free_batch(1)
            .expect("free batch should be a no-op");
        assert!(matches!(
            transport.submit(1, &[]),
            Err(StoreError::Unsupported(_))
        ));
        assert_eq!(
            transport
                .task_status(1, 0)
                .expect("task status should succeed")
                .status,
            TransferStatus::Completed
        );
        assert_eq!(
            transport
                .overall_status(1)
                .expect("overall status should succeed")
                .status,
            TransferStatus::Completed
        );
    }

    #[test]
    fn recording_transport_smoke_contract_covers_stubbed_methods() {
        let transport = RecordingTransport::default();
        assert_eq!(
            transport
                .segment_name()
                .expect("segment name should resolve"),
            "recording-segment"
        );
        assert_eq!(
            transport
                .rpc_server_address()
                .expect("rpc address should resolve"),
            ("127.0.0.1".to_string(), 0)
        );
        assert_eq!(
            transport
                .open_segment("segment")
                .expect("open should succeed"),
            1
        );
        transport.close_segment(1).expect("close should be a no-op");
        assert!(matches!(
            transport.get_segment_info(1),
            Err(StoreError::Unsupported(_))
        ));
        transport
            .adopt_local_memory(std::ptr::null_mut(), 0, "cpu:0")
            .expect("adopt should be a no-op");
        let addr = transport
            .allocate_memory(16, "cpu:0")
            .expect("allocation should succeed");
        transport
            .register_memory(addr, 16)
            .expect("register should succeed");
        let batch_id = transport.allocate_batch(1).expect("batch should allocate");
        transport
            .free_batch(batch_id)
            .expect("free batch should succeed");
        assert!(matches!(
            transport.submit(batch_id, &[]),
            Err(StoreError::Unsupported(_))
        ));
        assert_eq!(
            transport
                .task_status(batch_id, 0)
                .expect("task status should succeed")
                .status,
            TransferStatus::Completed
        );
        assert_eq!(
            transport
                .overall_status(batch_id)
                .expect("overall status should succeed")
                .status,
            TransferStatus::Completed
        );
        transport
            .unregister_memory(addr, 16)
            .expect("unregister should succeed");
        transport.free_memory(addr).expect("free should succeed");
    }

    #[test]
    fn recording_transport_unregister_surfaces_mismatch_and_missing_paths() {
        let transport = RecordingTransport::default();
        let addr = transport
            .allocate_memory(16, "cpu:0")
            .expect("allocation should succeed");
        transport
            .register_memory(addr, 16)
            .expect("register should succeed");
        transport.inner.lock().registered.insert(addr as usize, 8);
        assert!(matches!(
            transport.unregister_memory(addr, 16),
            Err(StoreError::Allocator(_))
        ));
        assert!(matches!(
            transport.unregister_memory(addr, 16),
            Err(StoreError::NotFound(_))
        ));
        transport.free_memory(addr).expect("free should succeed");
        assert!(matches!(
            transport.free_memory(addr),
            Err(StoreError::NotFound(_))
        ));
    }

    #[test]
    fn failing_transport_smoke_contract_covers_stubbed_methods() {
        let register_failure = FailingTransport::register_failure();
        assert!(matches!(
            register_failure.segment_name(),
            Err(StoreError::Unsupported(_))
        ));
        assert!(matches!(
            register_failure.rpc_server_address(),
            Err(StoreError::Unsupported(_))
        ));
        assert!(matches!(
            register_failure.open_segment("segment"),
            Err(StoreError::Unsupported(_))
        ));
        register_failure
            .close_segment(1)
            .expect("close should be a no-op");
        assert!(matches!(
            register_failure.get_segment_info(1),
            Err(StoreError::Unsupported(_))
        ));
        register_failure
            .adopt_local_memory(std::ptr::null_mut(), 0, "cpu:0")
            .expect("adopt should succeed when fail_adopt is disabled");
        let addr = register_failure
            .allocate_memory(16, "cpu:0")
            .expect("allocation should succeed");
        assert!(matches!(
            register_failure.register_memory(addr, 16),
            Err(StoreError::Transport(_))
        ));
        register_failure
            .unregister_memory(addr, 16)
            .expect("unregister should be a no-op");
        let batch_id = register_failure
            .allocate_batch(1)
            .expect("batch allocation should succeed");
        register_failure
            .free_batch(batch_id)
            .expect("free batch should succeed");
        assert!(matches!(
            register_failure.submit(batch_id, &[]),
            Err(StoreError::Unsupported(_))
        ));
        assert_eq!(
            register_failure
                .task_status(batch_id, 0)
                .expect("task status should succeed")
                .status,
            TransferStatus::Completed
        );
        assert_eq!(
            register_failure
                .overall_status(batch_id)
                .expect("overall status should succeed")
                .status,
            TransferStatus::Completed
        );
        register_failure
            .free_memory(addr)
            .expect("free should succeed after allocation");

        let adopt_failure = FailingTransport::adopt_failure();
        assert!(matches!(
            adopt_failure.adopt_local_memory(std::ptr::null_mut(), 0, "cpu:0"),
            Err(StoreError::Transport(_))
        ));
        adopt_failure
            .register_memory(std::ptr::null_mut(), 0)
            .expect("register should succeed when fail_register is disabled");
    }

    #[test]
    fn registered_region_plan_reports_checked_add_overflow() {
        let region = RegisteredRegion {
            base_addr: 0x1000,
            capacity: usize::MAX,
            alignment: 8,
            target_chunks: Vec::new(),
            owner: RegionOwner::Transport,
        };
        let error = region
            .plan(usize::MAX, 8)
            .expect_err("checked_add overflow must surface as allocator error");
        assert!(matches!(error, StoreError::Allocator(_)));
    }

    #[test]
    fn hugepage_allocator_rejects_invalid_inputs_before_syscall() {
        let hugepage = HugePageConfig::new(2 * 1024 * 1024).expect("2MB hugepage should resolve");

        let error = allocate_hugepage_region(0, 64, "cpu:0", hugepage)
            .expect_err("zero-sized hugepage allocation must fail");
        assert!(matches!(error, StoreError::Allocator(_)));

        let error = allocate_hugepage_region(4096, 64, "gpu:0", hugepage)
            .expect_err("non-cpu hugepage allocation must fail");
        assert!(matches!(error, StoreError::Unsupported(_)));
    }

    #[test]
    fn hugepage_allocator_surfaces_kernel_result_without_hidden_branches() {
        let hugepage = HugePageConfig::new(2 * 1024 * 1024).expect("2MB hugepage should resolve");
        let result = allocate_hugepage_region(4096, 64, "cpu:0", hugepage);
        if hugepages_available(hugepage.bytes()) {
            let (base, mapped_len, owner) =
                result.expect("hugetlb mmap should succeed when pages are available");
            assert_eq!(mapped_len, hugepage.bytes());
            owner
                .release(&NoopTransport, base)
                .expect("hugetlb region should unmap cleanly");
        } else {
            assert!(matches!(result, Err(StoreError::Allocator(_))));
        }

        let one_gb = HugePageConfig::new(1024 * 1024 * 1024).expect("1GB hugepage should resolve");
        assert_eq!(hugepage_map_flag(hugepage), libc::MAP_HUGE_2MB);
        assert_eq!(hugepage_map_flag(one_gb), libc::MAP_HUGE_1GB);
        assert!(matches!(
            free_hugepage_region(std::ptr::dangling_mut::<c_void>(), 4096),
            Err(StoreError::Allocator(_))
        ));
    }
}
