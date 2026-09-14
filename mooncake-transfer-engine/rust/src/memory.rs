// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Page-aligned host buffer for RDMA/TCP registration.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::ptr::NonNull;

/// Page-aligned, zeroed host memory suitable for `register_local_memory`.
///
/// Alignment is 4096 bytes so the region can be registered with RDMA NICs
/// without an extra copy. The pool is `Send + Sync`; concurrent access to
/// disjoint offsets is the caller's responsibility.
pub struct MemoryPool {
    ptr: NonNull<u8>,
    size: usize,
    align: usize,
}

impl MemoryPool {
    /// Allocate `size` bytes, page-aligned and zero-filled.
    pub fn new(size: usize) -> Self {
        Self::with_align(size, 4096)
    }

    /// Allocate `size` bytes with an explicit alignment (must be a power of two).
    pub fn with_align(size: usize, align: usize) -> Self {
        assert!(size > 0, "MemoryPool size must be non-zero");
        let layout = Layout::from_size_align(size, align).expect("invalid MemoryPool layout");
        let ptr = unsafe { alloc_zeroed(layout) };
        MemoryPool {
            ptr: NonNull::new(ptr).expect("failed to allocate MemoryPool"),
            size,
            align,
        }
    }

    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_void_ptr(&self) -> *mut std::ffi::c_void {
        self.ptr.as_ptr() as *mut std::ffi::c_void
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Pointer at byte `offset`. Panics if `offset` is out of range.
    #[inline]
    pub fn offset(&self, offset: usize) -> *mut u8 {
        assert!(offset < self.size, "MemoryPool offset out of range");
        unsafe { self.ptr.as_ptr().add(offset) }
    }

    /// Borrow the whole region as a slice. The caller must not register the
    /// same bytes for a concurrent RDMA write while this borrow is live.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    /// Mutable view of the whole region.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }
}

impl Drop for MemoryPool {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, self.align).expect("invalid layout");
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_zeroed_page_aligned_region() {
        let mut pool = MemoryPool::new(8192);
        assert_eq!(pool.len(), 8192);
        assert_eq!(pool.as_ptr() as usize % 4096, 0);
        assert!(pool.as_slice().iter().all(|&b| b == 0));
        pool.as_mut_slice()[0] = 0xAB;
        assert_eq!(pool.as_slice()[0], 0xAB);
        assert!(!pool.offset(4096).is_null());
    }

    #[test]
    #[should_panic]
    fn offset_out_of_range_panics() {
        let pool = MemoryPool::new(64);
        let _ = pool.offset(64);
    }
}
