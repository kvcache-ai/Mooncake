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

use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;

pub struct MemoryPool {
    ptr: NonNull<u8>,
    size: usize,
}

impl MemoryPool {
    pub fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 1).unwrap();
        let ptr = unsafe { alloc(layout) };
        MemoryPool {
            ptr: NonNull::new(ptr).expect("Failed to allocate memory"),
            size,
        }
    }

    pub fn offset(&self, offset: isize) -> *mut u8 {
        unsafe { self.ptr.as_ptr().offset(offset) }
    }
}

impl Drop for MemoryPool {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, 1).unwrap();
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

unsafe impl Send for MemoryPool {}
unsafe impl Sync for MemoryPool {}
