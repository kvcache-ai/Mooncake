/// AscendDirectTransport registers host memory through ADXL, which requires
/// 2 MiB-aligned host ranges; Rust heap `Vec<u8>` buffers are only 8/16-byte
/// aligned, so registering them fails with `rtsHostRegister ... internal
/// error`. Use an explicitly aligned allocation for every buffer handed to the
/// transfer engine.
pub(crate) const HOST_BUFFER_ALIGNMENT: usize = 2 * 1024 * 1024;

pub(crate) struct AlignedBuffer {
    ptr: *mut u8,
    len: usize,
    layout: std::alloc::Layout,
}

impl AlignedBuffer {
    pub(crate) fn zeroed(len: usize) -> Self {
        let layout = std::alloc::Layout::from_size_align(len.max(1), HOST_BUFFER_ALIGNMENT)
            .expect("host buffer layout must be valid");
        // SAFETY: layout is non-zero sized and alignment is a power of two.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Self { ptr, len, layout }
    }

    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: ptr/len describe the live allocation above.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }
}

// The allocation is uniquely owned by the wrapper; moving it across threads is
// sound (mirrors Box<[u8]>).
unsafe impl Send for AlignedBuffer {}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        // SAFETY: same layout used for allocation.
        unsafe { std::alloc::dealloc(self.ptr, self.layout) };
    }
}
