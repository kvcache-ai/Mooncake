use std::collections::BTreeMap;
use std::ffi::c_void;
use std::ptr;

use mooncake_store_core::{HugePageConfig, Result, SegmentLifecycleState, SegmentName, StoreError};

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
    pub hugepage_enabled: Option<bool>,
    pub hugepage_size_bytes: Option<usize>,
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

    pub fn use_hugepage(mut self, enabled: bool) -> Self {
        self.hugepage_enabled = Some(enabled);
        self
    }

    pub fn hugepage_size_bytes(mut self, hugepage_size_bytes: usize) -> Self {
        self.hugepage_enabled = Some(true);
        self.hugepage_size_bytes = Some(hugepage_size_bytes);
        self
    }

    pub fn hugepage(&self) -> Result<Option<HugePageConfig>> {
        HugePageConfig::resolve(self.hugepage_enabled, self.hugepage_size_bytes)
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
        let _ = self.hugepage()?;
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
            hugepage_enabled: None,
            hugepage_size_bytes: None,
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
    pub hugepage_enabled: Option<bool>,
    pub hugepage_size_bytes: Option<usize>,
}

impl LocalMemoryState {
    pub fn register(
        transport: &dyn StoreTransport,
        primary_segment: &SegmentName,
        config: &LocalMemoryConfig,
    ) -> Result<Self> {
        config.validate()?;
        let hugepage = config.hugepage()?;
        let storage = RegisteredRegion::register(
            transport,
            config.storage_bytes,
            &config.location,
            config.alignment,
            hugepage,
        )?;
        let scratch = RegisteredRegion::register(
            transport,
            config.scratch_bytes,
            &config.location,
            config.alignment,
            hugepage,
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
            HugePageConfig::resolve(spec.hugepage_enabled, spec.hugepage_size_bytes)?,
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
    base_addr: usize,
    capacity: usize,
    alignment: usize,
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
        if let Err(error) = transport.register_memory(base, capacity) {
            let _ = owner.release(transport, base);
            return Err(error);
        }
        Ok(Self {
            base_addr: base as usize,
            capacity,
            alignment,
            owner,
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

    fn release(self, transport: &dyn StoreTransport) -> Result<()> {
        let base = self.base_ptr();
        transport.unregister_memory(base, self.capacity)?;
        self.owner.release(transport, base)
    }
}

#[derive(Debug)]
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
    Ok((base, mapped_len, RegionOwner::HugePageMmap { mapped_len }))
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

fn hugepage_map_flag(hugepage: HugePageConfig) -> i32 {
    match hugepage.bytes() {
        size if size == 2 * 1024 * 1024 => libc::MAP_HUGE_2MB,
        size if size == 1024 * 1024 * 1024 => libc::MAP_HUGE_1GB,
        _ => 0,
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::c_void;
    use std::sync::Arc;

    use mooncake_store_core::{HugePageConfig, SegmentLifecycleState, SegmentName, StoreError};
    use mooncake_transport::{SegmentInfo, TransferProgress, TransferRequest, TransferStatus};
    use parking_lot::Mutex;

    use super::{
        align_up, allocate_hugepage_region, free_hugepage_region, hugepage_map_flag,
        LocalMemoryConfig, LocalMemoryState, RegionOwner, RegisteredRegion, StorageSegmentSpec,
        StoreTransport,
    };

    struct NoopTransport;

    #[derive(Default, Clone)]
    struct RecordingTransport {
        inner: Arc<Mutex<RecordingTransportState>>,
    }

    #[derive(Default)]
    struct RecordingTransportState {
        allocations: BTreeMap<usize, Box<[u8]>>,
        registered: BTreeMap<usize, usize>,
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

        fn register_memory(
            &self,
            addr: *mut c_void,
            size: usize,
        ) -> mooncake_store_core::Result<()> {
            self.inner.lock().registered.insert(addr as usize, size);
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
        let error = LocalMemoryConfig::new()
            .storage_bytes(0)
            .validate()
            .expect_err("zero storage must fail");
        assert!(matches!(error, StoreError::Allocator(_)));

        let error = LocalMemoryConfig::new()
            .scratch_bytes(0)
            .validate()
            .expect_err("zero scratch must fail");
        assert!(matches!(error, StoreError::Allocator(_)));

        let error = LocalMemoryConfig::new()
            .location("")
            .validate()
            .expect_err("empty location must fail");
        assert!(matches!(error, StoreError::Allocator(_)));
    }

    #[test]
    fn local_memory_config_builder_keeps_tags_and_alignment_floor() {
        let config = LocalMemoryConfig::new()
            .tags(vec!["nvme".to_string(), "warm".to_string()])
            .alignment(0)
            .reclaim_grace_ms(7);
        assert_eq!(config.tags, vec!["nvme".to_string(), "warm".to_string()]);
        assert_eq!(config.alignment, 1);
        assert_eq!(config.reclaim_grace_ms, 7);
    }

    #[test]
    fn registered_region_plan_rejects_zero_length_and_overflow() {
        let region = RegisteredRegion {
            base_addr: 0x1000,
            capacity: 64,
            alignment: 16,
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
    fn local_memory_state_registers_and_manages_segments() {
        let transport = RecordingTransport::default();
        let primary = SegmentName::new("primary");
        let mut state = LocalMemoryState::register(
            &transport,
            &primary,
            &LocalMemoryConfig::new()
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
    fn local_memory_state_reports_missing_and_duplicate_segments() {
        let transport = RecordingTransport::default();
        let primary = SegmentName::new("primary");
        let mut state = LocalMemoryState::register(
            &transport,
            &primary,
            &LocalMemoryConfig::new()
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
            free_hugepage_region(1usize as *mut c_void, 4096),
            Err(StoreError::Allocator(_))
        ));
    }
}
