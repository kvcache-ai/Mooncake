use std::collections::BTreeMap;
use std::ffi::c_void;

use mooncake_store_core::{Result, StoreError};

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
    storage: RegisteredRegion,
    scratch: RegisteredRegion,
}

impl LocalMemoryState {
    pub fn register(transport: &dyn StoreTransport, config: &LocalMemoryConfig) -> Result<Self> {
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
        Ok(Self { storage, scratch })
    }

    pub fn allocate_storage(&mut self, length: usize, now_ms: u64) -> Result<RegionAllocation> {
        self.storage.allocate(length, now_ms)
    }

    pub fn retire_storage(
        &mut self,
        absolute_offset: u64,
        length: usize,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<()> {
        self.storage.retire(absolute_offset, length, now_ms, grace_ms)
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

    pub fn storage_capacity_bytes(&self) -> usize {
        self.storage.capacity
    }

    pub fn storage_used_bytes(&self) -> usize {
        self.storage.used_bytes()
    }

    pub fn storage_address(&self, relative_offset: usize) -> Result<*mut c_void> {
        self.storage.address_at(relative_offset)
    }

    pub fn release(self, transport: &dyn StoreTransport) -> Result<()> {
        self.scratch.release(transport)?;
        self.storage.release(transport)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RegionAllocation {
    pub addr: *mut c_void,
    pub absolute_offset: u64,
}

#[derive(Debug)]
struct RegisteredRegion {
    base: *mut c_void,
    capacity: usize,
    committed: usize,
    alignment: usize,
    free: BTreeMap<usize, usize>,
    retired: Vec<RetiredSpan>,
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
            committed: 0,
            alignment: alignment.max(1),
            free: BTreeMap::new(),
            retired: Vec::new(),
        })
    }

    fn allocate(&mut self, length: usize, now_ms: u64) -> Result<RegionAllocation> {
        self.reclaim_expired(now_ms);
        let reserved_len = align_up(length, self.alignment);
        if let Some((offset, span_len)) = self
            .free
            .iter()
            .find(|(_, span_len)| **span_len >= reserved_len)
            .map(|(offset, span_len)| (*offset, *span_len))
        {
            self.free.remove(&offset);
            if span_len > reserved_len {
                self.free.insert(offset + reserved_len, span_len - reserved_len);
            }
            return self.allocation_at(offset);
        }

        let allocation = self.plan(self.committed, length)?;
        self.committed = self
            .committed
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("storage cursor overflow".to_string()))?;
        Ok(allocation)
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

    fn retire(
        &mut self,
        absolute_offset: u64,
        length: usize,
        now_ms: u64,
        grace_ms: u64,
    ) -> Result<()> {
        let offset = self.relative_offset(absolute_offset)?;
        let reserved_len = align_up(length, self.alignment);
        if grace_ms == 0 {
            self.insert_free_span(offset, reserved_len);
            return Ok(());
        }
        self.retired.push(RetiredSpan {
            offset,
            len: reserved_len,
            reclaim_at_ms: now_ms.saturating_add(grace_ms),
        });
        Ok(())
    }

    fn used_bytes(&self) -> usize {
        self.committed
            .saturating_sub(self.free.values().copied().sum::<usize>())
    }

    fn reclaim_expired(&mut self, now_ms: u64) {
        let mut retained = Vec::with_capacity(self.retired.len());
        let mut reclaim = Vec::new();
        for span in self.retired.drain(..) {
            if span.reclaim_at_ms <= now_ms {
                reclaim.push((span.offset, span.len));
            } else {
                retained.push(span);
            }
        }
        self.retired = retained;
        for (offset, len) in reclaim {
            self.insert_free_span(offset, len);
        }
    }

    fn insert_free_span(&mut self, offset: usize, len: usize) {
        let mut merged_start = offset;
        let mut merged_len = len;

        if let Some((prev_start, prev_len)) = self
            .free
            .range(..=offset)
            .next_back()
            .map(|(start, len)| (*start, *len))
        {
            if prev_start + prev_len == offset {
                merged_start = prev_start;
                merged_len = prev_len + len;
                self.free.remove(&prev_start);
            }
        }

        if let Some((next_start, next_len)) = self
            .free
            .range(offset..)
            .next()
            .map(|(start, len)| (*start, *len))
        {
            if merged_start + merged_len == next_start {
                merged_len += next_len;
                self.free.remove(&next_start);
            }
        }

        self.free.insert(merged_start, merged_len);
    }

    fn allocation_at(&self, offset: usize) -> Result<RegionAllocation> {
        let addr = unsafe { self.base.cast::<u8>().add(offset).cast::<c_void>() };
        Ok(RegionAllocation {
            addr,
            absolute_offset: addr as u64,
        })
    }

    fn relative_offset(&self, absolute_offset: u64) -> Result<usize> {
        let base = self.base as usize;
        let absolute = usize::try_from(absolute_offset).map_err(|_| {
            StoreError::Allocator(format!("absolute offset {absolute_offset} does not fit usize"))
        })?;
        if absolute < base {
            return Err(StoreError::Allocator(format!(
                "absolute offset {absolute_offset} is before region base {base}"
            )));
        }
        let relative = absolute - base;
        if relative >= self.capacity {
            return Err(StoreError::Allocator(format!(
                "absolute offset {absolute_offset} exceeds region capacity {}",
                self.capacity
            )));
        }
        Ok(relative)
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

#[derive(Clone, Copy, Debug)]
struct RetiredSpan {
    offset: usize,
    len: usize,
    reclaim_at_ms: u64,
}

fn align_up(value: usize, alignment: usize) -> usize {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ptr;

    use super::{align_up, RegisteredRegion};

    #[test]
    fn alignment_rounds_up_without_branch_explosion() {
        assert_eq!(align_up(1, 64), 64);
        assert_eq!(align_up(64, 64), 64);
        assert_eq!(align_up(65, 64), 128);
    }

    #[test]
    fn free_spans_merge_without_special_cases() {
        let mut region = RegisteredRegion {
            base: ptr::null_mut(),
            capacity: 1024,
            committed: 256,
            alignment: 64,
            free: BTreeMap::new(),
            retired: Vec::new(),
        };
        region.insert_free_span(0, 64);
        region.insert_free_span(128, 64);
        region.insert_free_span(64, 64);

        let spans = region.free.into_iter().collect::<Vec<_>>();
        assert_eq!(spans, vec![(0, 192)]);
    }
}
