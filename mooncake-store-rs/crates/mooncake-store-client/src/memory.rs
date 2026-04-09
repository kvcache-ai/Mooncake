use std::collections::BTreeMap;
use std::ffi::c_void;

use mooncake_store_core::{Result, SegmentLifecycleState, SegmentName, StoreError};

use crate::transport::StoreTransport;

const DEFAULT_STORAGE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_SCRATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_ALIGNMENT: usize = 64;

#[derive(Clone, Debug)]
pub struct LocalMemoryConfig {
    pub storage_bytes: usize,
    pub scratch_bytes: usize,
    pub location: String,
    pub tags: Vec<String>,
    pub alignment: usize,
    pub reclaim_grace_ms: u64,
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

    pub fn validate(&self) -> Result<()> {
        if self.storage_bytes == 0 {
            return Err(StoreError::Allocator(
                "storage_bytes must be greater than zero".to_string(),
            ));
        }
        if self.scratch_bytes == 0 {
            return Err(StoreError::Allocator(
                "scratch_bytes must be greater than zero".to_string(),
            ));
        }
        if self.location.is_empty() {
            return Err(StoreError::Allocator(
                "location must not be empty".to_string(),
            ));
        }
        Ok(())
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
        }
    }
}

#[derive(Debug)]
pub struct LocalMemoryState {
    storage: BTreeMap<SegmentName, StorageExtent>,
    scratch: RegisteredRegion,
}

#[derive(Clone, Debug)]
pub struct StorageSegmentSpec {
    pub segment_name: SegmentName,
    pub capacity_bytes: usize,
    pub state: SegmentLifecycleState,
    pub tags: Vec<String>,
    pub location: String,
    pub alignment: usize,
}

impl LocalMemoryState {
    pub fn register(
        transport: &dyn StoreTransport,
        primary_segment: &SegmentName,
        config: &LocalMemoryConfig,
    ) -> Result<Self> {
        config.validate()?;
        let storage = RegisteredRegion::register(
            transport,
            config.storage_bytes,
            &config.location,
            config.alignment,
        )?;
        let scratch = RegisteredRegion::register(
            transport,
            config.scratch_bytes,
            &config.location,
            config.alignment,
        )?;
        let mut segments = BTreeMap::new();
        segments.insert(
            primary_segment.clone(),
            StorageExtent {
                region: storage,
                state: SegmentLifecycleState::Active,
                tags: config.tags.clone(),
            },
        );
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
        if self.storage.contains_key(&spec.segment_name) {
            return Err(StoreError::Conflict(format!(
                "storage segment {} already exists",
                spec.segment_name.0
            )));
        }
        let region = RegisteredRegion::register(
            transport,
            spec.capacity_bytes,
            &spec.location,
            spec.alignment,
        )?;
        self.storage.insert(
            spec.segment_name,
            StorageExtent {
                region,
                state: spec.state,
                tags: spec.tags,
            },
        );
        Ok(())
    }

    pub fn plan_scratch(&self, lengths: &[usize]) -> Result<Vec<RegionAllocation>> {
        let mut cursor = 0usize;
        let mut allocations = Vec::with_capacity(lengths.len());
        for length in lengths {
            allocations.push(self.scratch.plan(cursor, *length)?);
            cursor = cursor
                .checked_add(align_up(*length, self.scratch.alignment))
                .ok_or_else(|| StoreError::Allocator("scratch cursor overflow".to_string()))?;
        }
        Ok(allocations)
    }

    pub fn has_storage_segment(&self, segment: &SegmentName) -> bool {
        self.storage.contains_key(segment)
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
        extent.region.release(transport)
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
    pub state: SegmentLifecycleState,
    pub tags: Vec<String>,
}

#[derive(Debug)]
struct StorageExtent {
    region: RegisteredRegion,
    state: SegmentLifecycleState,
    tags: Vec<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct RegionAllocation {
    pub addr: *mut c_void,
}

#[derive(Debug)]
struct RegisteredRegion {
    base: *mut c_void,
    capacity: usize,
    alignment: usize,
}

impl RegisteredRegion {
    fn register(
        transport: &dyn StoreTransport,
        capacity: usize,
        location: &str,
        alignment: usize,
    ) -> Result<Self> {
        let base = transport.allocate_memory(capacity, location)?;
        if let Err(error) = transport.register_memory(base, capacity) {
            let _ = transport.free_memory(base);
            return Err(error);
        }
        Ok(Self {
            base,
            capacity,
            alignment: alignment.max(1),
        })
    }

    fn plan(&self, cursor: usize, length: usize) -> Result<RegionAllocation> {
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
        self.allocation_at(offset)
    }

    fn allocation_at(&self, offset: usize) -> Result<RegionAllocation> {
        let addr = unsafe { self.base.cast::<u8>().add(offset).cast::<c_void>() };
        Ok(RegionAllocation { addr })
    }

    fn address_at(&self, offset: usize) -> Result<*mut c_void> {
        if offset >= self.capacity {
            return Err(StoreError::Allocator(format!(
                "offset {offset} exceeds region capacity {}",
                self.capacity
            )));
        }
        Ok(unsafe { self.base.cast::<u8>().add(offset).cast::<c_void>() })
    }

    fn release(self, transport: &dyn StoreTransport) -> Result<()> {
        transport.unregister_memory(self.base, self.capacity)?;
        transport.free_memory(self.base)
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

#[cfg(test)]
mod tests {
    use super::align_up;

    #[test]
    fn alignment_rounds_up_without_branch_explosion() {
        assert_eq!(align_up(1, 64), 64);
        assert_eq!(align_up(64, 64), 64);
        assert_eq!(align_up(65, 64), 128);
    }
}
