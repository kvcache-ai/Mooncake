use std::collections::{HashMap, VecDeque};
use std::os::fd::OwnedFd;
use std::ptr;

use mooncake_store_core::{Result, StoreError};
use parking_lot::Mutex;
use tracing::{info, warn};

use crate::shm::{allocate_shared_region, free_shared_region, shared_region_for_registration};

const CACHE_SIZE_ENV: &str = "MC_STORE_LOCAL_HOT_CACHE_SIZE";
const BLOCK_SIZE_ENV: &str = "MC_STORE_LOCAL_HOT_BLOCK_SIZE";
const USE_SHM_ENV: &str = "MC_STORE_LOCAL_HOT_CACHE_USE_SHM";
const DEFAULT_BLOCK_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct HotCacheKey {
    tenant: String,
    key: String,
}

impl HotCacheKey {
    pub(crate) fn new(tenant: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            tenant: tenant.into(),
            key: key.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HotCacheHandle {
    pub(crate) block_id: u64,
    pub(crate) generation: u64,
    pub(crate) offset: usize,
    pub(crate) len: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct HotCacheConfig {
    pub(crate) total_size: usize,
    pub(crate) block_size: usize,
    pub(crate) use_shm: bool,
}

impl HotCacheConfig {
    pub(crate) fn from_env() -> Option<Self> {
        let total_size = match positive_usize_env(CACHE_SIZE_ENV) {
            EnvValue::Missing | EnvValue::Invalid => return None,
            EnvValue::Value(value) => value,
        };
        let block_size = match positive_usize_env(BLOCK_SIZE_ENV) {
            EnvValue::Missing | EnvValue::Invalid => DEFAULT_BLOCK_SIZE,
            EnvValue::Value(value) => value,
        };
        let use_shm = std::env::var(USE_SHM_ENV).is_ok_and(|value| value == "1");
        Some(Self {
            total_size,
            block_size,
            use_shm,
        })
    }
}

enum EnvValue {
    Missing,
    Invalid,
    Value(usize),
}

fn positive_usize_env(name: &str) -> EnvValue {
    let Ok(raw) = std::env::var(name) else {
        return EnvValue::Missing;
    };
    let invalid = || {
        warn!(
            name,
            value = raw,
            "invalid local hot cache env, disabling override"
        );
        EnvValue::Invalid
    };
    if raw.starts_with('-') {
        return invalid();
    }
    match raw.parse::<u64>() {
        Ok(value) if value > 0 && value <= usize::MAX as u64 => EnvValue::Value(value as usize),
        _ => invalid(),
    }
}

pub(crate) struct LocalHotCache {
    backing: CacheBacking,
    block_size: usize,
    inner: Mutex<CacheInner>,
}

enum CacheBacking {
    Private { base: usize },
    Shm { base: usize, len: usize },
}

struct CacheInner {
    blocks: Vec<CacheBlock>,
    lru: VecDeque<usize>,
    key_to_block: HashMap<HotCacheKey, usize>,
}

struct CacheBlock {
    key: Option<HotCacheKey>,
    len: usize,
    generation: u64,
    pins: usize,
}

impl LocalHotCache {
    pub(crate) fn from_env() -> Result<Option<Self>> {
        let Some(config) = HotCacheConfig::from_env() else {
            return Ok(None);
        };
        Self::new(config).map(Some)
    }

    pub(crate) fn new(config: HotCacheConfig) -> Result<Self> {
        if config.total_size == 0 || config.block_size == 0 {
            return Err(StoreError::InvalidState(
                "local hot cache size and block size must be positive".to_string(),
            ));
        }
        let block_count = config.total_size / config.block_size;
        if block_count == 0 {
            return Err(StoreError::InvalidState(format!(
                "local hot cache total size {} is smaller than block size {}",
                config.total_size, config.block_size
            )));
        }
        let total_size = block_count
            .checked_mul(config.block_size)
            .ok_or_else(|| StoreError::Allocator("local hot cache size overflow".to_string()))?;
        let backing = if config.use_shm {
            CacheBacking::Shm {
                base: allocate_shared_region(total_size)?,
                len: total_size,
            }
        } else {
            CacheBacking::Private {
                base: allocate_private_region(total_size)?,
            }
        };
        let blocks = (0..block_count)
            .map(|_| CacheBlock {
                key: None,
                len: 0,
                generation: 1,
                pins: 0,
            })
            .collect::<Vec<_>>();
        let lru = (0..block_count).collect::<VecDeque<_>>();
        info!(
            total_size,
            block_size = config.block_size,
            block_count,
            shm = config.use_shm,
            "local hot cache enabled"
        );
        Ok(Self {
            backing,
            block_size: config.block_size,
            inner: Mutex::new(CacheInner {
                blocks,
                lru,
                key_to_block: HashMap::new(),
            }),
        })
    }

    pub(crate) fn duplicate_shm_fd(&self) -> Result<Option<(OwnedFd, usize)>> {
        let CacheBacking::Shm { base, len } = self.backing else {
            return Ok(None);
        };
        let registration = shared_region_for_registration(base, len)?;
        Ok(Some((registration.fd, registration.registered_len)))
    }

    pub(crate) fn acquire(&self, key: &HotCacheKey) -> Option<HotCacheHandle> {
        let mut inner = self.inner.lock();
        let block_id = *inner.key_to_block.get(key)?;
        let block = inner.blocks.get_mut(block_id)?;
        block.pins = block.pins.saturating_add(1);
        let handle = HotCacheHandle {
            block_id: block_id as u64,
            generation: block.generation,
            offset: self.block_offset(block_id),
            len: block.len,
        };
        touch_lru(&mut inner.lru, block_id);
        Some(handle)
    }

    pub(crate) fn release(&self, handle: HotCacheHandle) {
        let Ok(block_id) = usize::try_from(handle.block_id) else {
            return;
        };
        let mut inner = self.inner.lock();
        let Some(block) = inner.blocks.get_mut(block_id) else {
            return;
        };
        if block.generation == handle.generation && block.pins > 0 {
            block.pins -= 1;
            if block.pins == 0 && block.key.is_none() {
                block.len = 0;
            }
        }
    }

    pub(crate) fn copy_handle_to_vec(&self, handle: HotCacheHandle) -> Option<Vec<u8>> {
        let data = self.handle_slice(handle)?;
        Some(data.to_vec())
    }

    pub(crate) fn copy_handle_to_slice(
        &self,
        handle: HotCacheHandle,
        target: &mut [u8],
    ) -> Result<usize> {
        if handle.len > target.len() {
            return Err(StoreError::Allocator(format!(
                "hot cache target too small: value={} target={}",
                handle.len,
                target.len()
            )));
        }
        let data = self
            .handle_slice(handle)
            .ok_or_else(|| StoreError::NotFound("hot cache handle is stale".to_string()))?;
        target[..handle.len].copy_from_slice(data);
        Ok(handle.len)
    }

    pub(crate) fn copy_handle_to_slices(
        &self,
        handle: HotCacheHandle,
        targets: &mut [&mut [u8]],
    ) -> Result<usize> {
        let capacity = targets.iter().map(|target| target.len()).sum::<usize>();
        if handle.len > capacity {
            return Err(StoreError::Allocator(format!(
                "hot cache multi-buffer target too small: value={} target={capacity}",
                handle.len
            )));
        }
        let data = self
            .handle_slice(handle)
            .ok_or_else(|| StoreError::NotFound("hot cache handle is stale".to_string()))?;
        scatter_copy(data, targets);
        Ok(handle.len)
    }

    pub(crate) fn get(&self, key: &HotCacheKey) -> Option<Vec<u8>> {
        let handle = self.acquire(key)?;
        let value = self.copy_handle_to_vec(handle);
        self.release(handle);
        value
    }

    pub(crate) fn copy_into(&self, key: &HotCacheKey, target: &mut [u8]) -> Result<Option<usize>> {
        let Some(handle) = self.acquire(key) else {
            return Ok(None);
        };
        let result = self.copy_handle_to_slice(handle, target).map(Some);
        self.release(handle);
        result
    }

    pub(crate) fn copy_into_multi(
        &self,
        key: &HotCacheKey,
        targets: &mut [&mut [u8]],
    ) -> Result<Option<usize>> {
        let Some(handle) = self.acquire(key) else {
            return Ok(None);
        };
        let result = self.copy_handle_to_slices(handle, targets).map(Some);
        self.release(handle);
        result
    }

    pub(crate) fn insert(&self, key: HotCacheKey, value: &[u8]) -> bool {
        if value.is_empty() || value.len() > self.block_size {
            return false;
        }
        let mut inner = self.inner.lock();
        if let Some(block_id) = inner.key_to_block.get(&key).copied() {
            touch_lru(&mut inner.lru, block_id);
            return true;
        }
        let Some(block_id) = evictable_tail(&inner) else {
            return false;
        };
        if let Some(old_key) = inner.blocks[block_id].key.take() {
            inner.key_to_block.remove(&old_key);
        }
        inner.blocks[block_id].generation = inner.blocks[block_id].generation.wrapping_add(1);
        inner.blocks[block_id].len = value.len();
        unsafe {
            ptr::copy_nonoverlapping(
                value.as_ptr(),
                (self.base_addr() + self.block_offset(block_id)) as *mut u8,
                value.len(),
            );
        }
        inner.blocks[block_id].key = Some(key.clone());
        inner.key_to_block.insert(key, block_id);
        touch_lru(&mut inner.lru, block_id);
        true
    }

    pub(crate) fn insert_from_slices(
        &self,
        key: HotCacheKey,
        sources: &[&[u8]],
        len: usize,
    ) -> bool {
        if len == 0 || len > self.block_size {
            return false;
        }
        let mut payload = Vec::with_capacity(len);
        let mut remaining = len;
        for source in sources {
            if remaining == 0 {
                break;
            }
            let copied = remaining.min(source.len());
            payload.extend_from_slice(&source[..copied]);
            remaining -= copied;
        }
        if remaining != 0 {
            return false;
        }
        self.insert(key, &payload)
    }

    pub(crate) fn invalidate(&self, key: &HotCacheKey) {
        let mut inner = self.inner.lock();
        let Some(block_id) = inner.key_to_block.remove(key) else {
            return;
        };
        let block = &mut inner.blocks[block_id];
        block.key = None;
        if block.pins == 0 {
            block.len = 0;
        }
    }

    pub(crate) fn invalidate_many(&self, keys: impl IntoIterator<Item = HotCacheKey>) {
        let mut inner = self.inner.lock();
        for key in keys {
            let Some(block_id) = inner.key_to_block.remove(&key) else {
                continue;
            };
            let block = &mut inner.blocks[block_id];
            block.generation = block.generation.saturating_add(1);
            if block.pins == 0 {
                block.len = 0;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, key: &HotCacheKey) -> bool {
        self.inner.lock().key_to_block.contains_key(key)
    }

    fn block_offset(&self, block_id: usize) -> usize {
        block_id * self.block_size
    }

    fn base_addr(&self) -> usize {
        match self.backing {
            CacheBacking::Private { base, .. } | CacheBacking::Shm { base, .. } => base,
        }
    }

    fn handle_slice(&self, handle: HotCacheHandle) -> Option<&[u8]> {
        let block_id = usize::try_from(handle.block_id).ok()?;
        let inner = self.inner.lock();
        let block = inner.blocks.get(block_id)?;
        if block.generation != handle.generation || handle.len > block.len {
            return None;
        }
        let offset = self.block_offset(block_id);
        let ptr = (self.base_addr() + offset) as *const u8;
        Some(unsafe { std::slice::from_raw_parts(ptr, handle.len) })
    }
}

impl Drop for LocalHotCache {
    fn drop(&mut self) {
        match self.backing {
            CacheBacking::Private { base, .. } => unsafe {
                libc::free(base as *mut libc::c_void);
            },
            CacheBacking::Shm { base, .. } => {
                let _ = free_shared_region(base);
            }
        }
    }
}

fn allocate_private_region(size: usize) -> Result<usize> {
    let ptr = unsafe { libc::malloc(size) };
    if ptr.is_null() {
        return Err(StoreError::Allocator(format!(
            "malloc local hot cache failed for {size} bytes"
        )));
    }
    Ok(ptr as usize)
}

fn evictable_tail(inner: &CacheInner) -> Option<usize> {
    inner
        .lru
        .iter()
        .rev()
        .copied()
        .find(|block_id| inner.blocks[*block_id].pins == 0)
}

fn touch_lru(lru: &mut VecDeque<usize>, block_id: usize) {
    let Some(position) = lru.iter().position(|candidate| *candidate == block_id) else {
        lru.push_front(block_id);
        return;
    };
    lru.remove(position);
    lru.push_front(block_id);
}

fn scatter_copy(source: &[u8], targets: &mut [&mut [u8]]) {
    let mut copied = 0usize;
    for target in targets {
        if copied == source.len() {
            break;
        }
        let len = target.len().min(source.len() - copied);
        target[..len].copy_from_slice(&source[copied..copied + len]);
        copied += len;
    }
}

#[cfg(test)]
mod tests {
    use crate::test_support::env_test_lock;

    use super::*;

    #[test]
    fn env_config_uses_upstream_names() {
        let _guard = env_test_lock().lock();
        clear_env();
        assert_eq!(HotCacheConfig::from_env(), None);

        std::env::set_var(CACHE_SIZE_ENV, "4096");
        std::env::set_var(BLOCK_SIZE_ENV, "1024");
        std::env::set_var(USE_SHM_ENV, "1");
        assert_eq!(
            HotCacheConfig::from_env(),
            Some(HotCacheConfig {
                total_size: 4096,
                block_size: 1024,
                use_shm: true,
            })
        );

        std::env::set_var(CACHE_SIZE_ENV, "-1");
        assert_eq!(HotCacheConfig::from_env(), None);
        clear_env();
    }

    #[test]
    fn cache_hits_lru_and_respects_pins() {
        let cache = LocalHotCache::new(HotCacheConfig {
            total_size: 8,
            block_size: 4,
            use_shm: false,
        })
        .expect("cache should allocate");
        let a = HotCacheKey::new("default", "a");
        let b = HotCacheKey::new("default", "b");
        let c = HotCacheKey::new("default", "c");

        assert!(cache.insert(a.clone(), b"aaaa"));
        assert!(cache.insert(b.clone(), b"bbbb"));
        let pinned = cache.acquire(&a).expect("a should hit");
        assert!(cache.insert(c.clone(), b"cccc"));

        assert_eq!(cache.get(&a).expect("a is pinned and mapped"), b"aaaa");
        assert!(cache.get(&b).is_none());
        assert_eq!(cache.get(&c).expect("c should hit"), b"cccc");
        cache.release(pinned);
    }

    #[test]
    fn cache_can_copy_multi_buffer_hits() {
        let cache = LocalHotCache::new(HotCacheConfig {
            total_size: 8,
            block_size: 8,
            use_shm: false,
        })
        .expect("cache should allocate");
        let key = HotCacheKey::new("default", "multi");
        assert!(cache.insert(key.clone(), b"abcdef"));
        let mut a = [0u8; 2];
        let mut b = [0u8; 4];
        let mut targets = vec![a.as_mut_slice(), b.as_mut_slice()];
        assert_eq!(
            cache
                .copy_into_multi(&key, targets.as_mut_slice())
                .expect("copy should succeed"),
            Some(6)
        );
        assert_eq!(&a, b"ab");
        assert_eq!(&b, b"cdef");
    }

    fn clear_env() {
        std::env::remove_var(CACHE_SIZE_ENV);
        std::env::remove_var(BLOCK_SIZE_ENV);
        std::env::remove_var(USE_SHM_ENV);
    }
}
