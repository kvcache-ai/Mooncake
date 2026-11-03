use std::sync::Arc;
use std::time::{Duration, Instant};

use mooncake_store_core::{SegmentName, SegmentTargetChunk, StoreError};
use mooncake_store_transport_core::StoreTransport;
use parking_lot::{Condvar, Mutex};
use tracing::{info, warn};

#[derive(Debug, Clone)]
pub(in super::super) struct StagingPoolUtilization {
    pub cursor: u64,
    pub capacity: u64,
    pub free_span_count: usize,
    pub free_span_bytes: u64,
    pub used_bytes: u64,
    pub used_pct: f64,
}

type Result<T> = std::result::Result<T, StoreError>;

const ALLOC_ALIGNMENT: u64 = 4096;

fn align_up(value: u64, alignment: u64) -> Option<u64> {
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|aligned| aligned & !(alignment - 1))
}

#[derive(Debug, Clone)]
struct FreeSpan {
    offset: u64,
    length: u64,
}

pub(in super::super) struct ColdRestoreStagingPool {
    base_addr: usize,
    capacity: u64,
    cursor: Mutex<u64>,
    free_spans: Mutex<Vec<FreeSpan>>,
    space_available: Condvar,
    segment_name: SegmentName,
    target_chunks: Vec<SegmentTargetChunk>,
    transport_endpoint: Option<String>,
    transport_segment_descriptor: Option<String>,
    transport: Arc<dyn StoreTransport>,
}

pub(in super::super) struct StagingSlot {
    pool: Arc<ColdRestoreStagingPool>,
    pub addr: *mut u8,
    #[allow(dead_code)]
    pub len: usize,
    pub segment_name: SegmentName,
    pub offset: u64,
    alloc_len: u64,
}

unsafe impl Send for StagingSlot {}

impl Drop for StagingSlot {
    fn drop(&mut self) {
        self.pool.release(self.offset, self.alloc_len);
    }
}

pub(in super::super) struct StagingPoolInitParams {
    pub transport: Arc<dyn StoreTransport>,
    pub total_bytes: usize,
}

impl ColdRestoreStagingPool {
    pub(in super::super) fn new(params: StagingPoolInitParams) -> Result<Arc<Self>> {
        let capacity = params.total_bytes.max(ALLOC_ALIGNMENT as usize);
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                capacity,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_POPULATE,
                -1,
                0,
            )
        };
        if base == libc::MAP_FAILED {
            return Err(StoreError::Allocator(format!(
                "staging pool mmap failed: size={capacity}: {}",
                std::io::Error::last_os_error()
            )));
        }

        if let Err(error) = params.transport.register_memory(base, capacity) {
            unsafe { libc::munmap(base, capacity) };
            return Err(error);
        }

        if let Err(error) = params
            .transport
            .adopt_local_memory(base, capacity, "staging_pool")
        {
            warn!(?error, "staging pool adopt_local_memory failed");
        }

        let base_addr = base as usize;
        let target_chunks = match crate::transport::registration_chunks(
            base,
            capacity,
            params.transport.max_registration_bytes(),
        )
        .and_then(|chunks| {
            chunks
                .into_iter()
                .map(|(chunk_addr, chunk_len)| {
                    let chunk_base = chunk_addr as usize;
                    let logical_offset = chunk_base.checked_sub(base_addr).ok_or_else(|| {
                        StoreError::Allocator(
                            "staging registration chunk precedes region base".to_string(),
                        )
                    })?;
                    Ok(SegmentTargetChunk {
                        logical_offset: logical_offset as u64,
                        target_offset: chunk_base as u64,
                        length_bytes: chunk_len as u64,
                    })
                })
                .collect::<Result<Vec<_>>>()
        }) {
            Ok(target_chunks) => target_chunks,
            Err(error) => {
                let _ = params.transport.unregister_memory(base, capacity);
                unsafe { libc::munmap(base, capacity) };
                return Err(error);
            }
        };
        let transport_endpoint = params
            .transport
            .segment_name()
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let transport_segment_descriptor =
            params.transport.local_segment_descriptor().ok().flatten();
        let segment_name = SegmentName::new("__staging_cold_restore__");

        info!(
            capacity,
            base_addr = base as usize,
            target_chunks = target_chunks.len(),
            transport_endpoint = transport_endpoint.as_deref().unwrap_or(""),
            "cold restore staging pool initialized"
        );

        Ok(Arc::new(Self {
            base_addr: base as usize,
            capacity: capacity as u64,
            cursor: Mutex::new(0),
            free_spans: Mutex::new(Vec::new()),
            space_available: Condvar::new(),
            segment_name,
            target_chunks,
            transport_endpoint,
            transport_segment_descriptor,
            transport: params.transport,
        }))
    }

    pub(in super::super) fn utilization(&self) -> StagingPoolUtilization {
        let free = self.free_spans.lock();
        let free_span_count = free.len();
        let free_span_bytes: u64 = free.iter().map(|s| s.length).sum();
        drop(free);
        let cursor = *self.cursor.lock();
        let used_bytes = cursor.saturating_sub(free_span_bytes);
        let used_pct = if self.capacity > 0 {
            (used_bytes as f64 / self.capacity as f64) * 100.0
        } else {
            0.0
        };
        StagingPoolUtilization {
            cursor,
            capacity: self.capacity,
            free_span_count,
            free_span_bytes,
            used_bytes,
            used_pct,
        }
    }

    pub(in super::super) fn try_allocate(
        self: &Arc<Self>,
        needed_bytes: usize,
    ) -> Option<StagingSlot> {
        let Some(alloc_len) = align_up(needed_bytes as u64, ALLOC_ALIGNMENT) else {
            warn!(needed_bytes, "staging allocation length overflow");
            return None;
        };
        if alloc_len > self.capacity {
            warn!(
                needed_bytes,
                capacity = self.capacity,
                "staging allocation exceeds capacity"
            );
            return None;
        }

        {
            let mut free = self.free_spans.lock();
            if let Some(idx) = free.iter().position(|span| span.length >= alloc_len) {
                let span = free.remove(idx);
                let offset = span.offset;
                if span.length > alloc_len {
                    free.push(FreeSpan {
                        offset: offset + alloc_len,
                        length: span.length - alloc_len,
                    });
                }
                return Some(self.build_slot(offset, needed_bytes, alloc_len));
            }
        }

        {
            let mut cursor = self.cursor.lock();
            if *cursor + alloc_len <= self.capacity {
                let offset = *cursor;
                *cursor += alloc_len;
                return Some(self.build_slot(offset, needed_bytes, alloc_len));
            }
        }

        let util = self.utilization();
        warn!(
            needed_bytes,
            alloc_len,
            cursor = util.cursor,
            capacity = util.capacity,
            free_span_count = util.free_span_count,
            free_span_bytes = util.free_span_bytes,
            used_bytes = util.used_bytes,
            used_pct = format_args!("{:.1}", util.used_pct),
            "staging pool exhausted"
        );
        None
    }

    pub(in super::super) fn allocate_blocking(
        self: &Arc<Self>,
        needed_bytes: usize,
        timeout: Duration,
    ) -> Option<StagingSlot> {
        let Some(alloc_len) = align_up(needed_bytes as u64, ALLOC_ALIGNMENT) else {
            warn!(needed_bytes, "staging allocation length overflow");
            return None;
        };
        if alloc_len > self.capacity {
            warn!(
                needed_bytes,
                capacity = self.capacity,
                "staging allocation exceeds capacity"
            );
            return None;
        }

        let t_start = Instant::now();
        let deadline = t_start + timeout;
        let mut free = self.free_spans.lock();
        loop {
            if let Some(idx) = free.iter().position(|span| span.length >= alloc_len) {
                let span = free.remove(idx);
                let offset = span.offset;
                if span.length > alloc_len {
                    free.push(FreeSpan {
                        offset: offset + alloc_len,
                        length: span.length - alloc_len,
                    });
                }
                let wait = t_start.elapsed();
                if !wait.is_zero() {
                    info!(
                        needed_bytes,
                        wait_ms = wait.as_millis() as u64,
                        "staging pool allocation waited for returned slot"
                    );
                }
                return Some(self.build_slot(offset, needed_bytes, alloc_len));
            }

            {
                let mut cursor = self.cursor.lock();
                if *cursor + alloc_len <= self.capacity {
                    let offset = *cursor;
                    *cursor += alloc_len;
                    let wait = t_start.elapsed();
                    if !wait.is_zero() {
                        info!(
                            needed_bytes,
                            wait_ms = wait.as_millis() as u64,
                            "staging pool allocation waited for cursor space"
                        );
                    }
                    return Some(self.build_slot(offset, needed_bytes, alloc_len));
                }
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drop(free);
                let util = self.utilization();
                warn!(
                    needed_bytes,
                    alloc_len,
                    timeout_ms = timeout.as_millis() as u64,
                    wait_ms = t_start.elapsed().as_millis() as u64,
                    cursor = util.cursor,
                    capacity = util.capacity,
                    free_span_count = util.free_span_count,
                    free_span_bytes = util.free_span_bytes,
                    used_bytes = util.used_bytes,
                    used_pct = format_args!("{:.1}", util.used_pct),
                    "staging pool allocation timed out"
                );
                return None;
            }
            self.space_available.wait_for(&mut free, remaining);
        }
    }

    fn build_slot(
        self: &Arc<Self>,
        offset: u64,
        needed_bytes: usize,
        alloc_len: u64,
    ) -> StagingSlot {
        let addr = (self.base_addr + offset as usize) as *mut u8;
        StagingSlot {
            pool: Arc::clone(self),
            addr,
            len: needed_bytes,
            segment_name: self.segment_name.clone(),
            offset,
            alloc_len,
        }
    }

    fn release(&self, offset: u64, alloc_len: u64) {
        let mut free = self.free_spans.lock();
        free.push(FreeSpan {
            offset,
            length: alloc_len,
        });
        Self::coalesce(&mut free);
        let free_span_count = free.len();
        let free_span_bytes: u64 = free.iter().map(|s| s.length).sum();
        drop(free);

        let cursor = *self.cursor.lock();
        let used_bytes = cursor.saturating_sub(free_span_bytes);
        let used_pct = if self.capacity > 0 {
            (used_bytes as f64 / self.capacity as f64) * 100.0
        } else {
            0.0
        };
        info!(
            offset,
            alloc_len,
            free_span_count,
            free_span_bytes,
            used_bytes,
            used_pct = format_args!("{:.1}", used_pct),
            capacity = self.capacity,
            "staging slot returned to pool"
        );
        self.space_available.notify_one();
    }

    fn coalesce(spans: &mut Vec<FreeSpan>) {
        if spans.len() < 2 {
            return;
        }
        spans.sort_by_key(|s| s.offset);
        let mut i = 0;
        while i + 1 < spans.len() {
            if spans[i].offset + spans[i].length == spans[i + 1].offset {
                spans[i].length += spans[i + 1].length;
                spans.remove(i + 1);
            } else {
                i += 1;
            }
        }
    }

    pub(in super::super) fn target_chunks(&self) -> &[SegmentTargetChunk] {
        &self.target_chunks
    }

    pub(in super::super) fn transport_endpoint(&self) -> Option<&str> {
        self.transport_endpoint.as_deref()
    }

    pub(in super::super) fn transport_segment_descriptor(&self) -> Option<&str> {
        self.transport_segment_descriptor.as_deref()
    }
}

impl Drop for ColdRestoreStagingPool {
    fn drop(&mut self) {
        info!(
            base_addr = self.base_addr,
            capacity = self.capacity,
            "cold restore staging pool shutting down"
        );
        let addr = self.base_addr as *mut libc::c_void;
        if let Err(error) = self
            .transport
            .unregister_memory(addr, self.capacity as usize)
        {
            warn!(?error, "cold restore staging pool unregister_memory failed");
        }
        unsafe {
            libc::munmap(addr, self.capacity as usize);
        }
    }
}

unsafe impl Send for ColdRestoreStagingPool {}
unsafe impl Sync for ColdRestoreStagingPool {}
