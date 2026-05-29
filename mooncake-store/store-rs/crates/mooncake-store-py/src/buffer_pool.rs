//! Registered buffer pool for zero-copy interfaces.
//!
//! Keeps a bounded set of registered scratch buffers for repeated zero-copy
//! operations, eliminating repeated `register_buffer` / `unregister_buffer`
//! overhead on hot paths.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

struct Region {
    ptr: *mut u8,
    size: usize,
}

// SAFETY: Region owns aligned heap memory and is only accessed under Mutex.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

/// Shared immutable configuration set once at construction.
struct PoolConfig {
    store: PyObject,
    max_bytes: usize,
    min_size_class: usize,
    max_size_class: usize,
    alignment: usize,
    block_on_exhaustion: bool,
    default_timeout: Option<f64>,
    max_regions: Option<usize>,
}

/// Mutable pool state protected by Mutex.
struct PoolState {
    free: HashMap<usize, VecDeque<Region>>,
    regions: HashMap<usize, usize>, // ptr → region_size
    in_use: HashSet<usize>,
    closed: bool,
    closing: bool,
    total_bytes: usize,
    reserved_bytes: usize,
    reserved_regions: usize,
}

impl PoolState {
    fn raise_if_not_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err("registered buffer pool is closed"));
        }
        if self.closing {
            return Err(PyRuntimeError::new_err("registered buffer pool is closing"));
        }
        Ok(())
    }

    fn has_capacity_for(
        &self,
        size_class: usize,
        max_bytes: usize,
        max_regions: Option<usize>,
    ) -> bool {
        let remaining = max_bytes
            .saturating_sub(self.reserved_bytes)
            .saturating_sub(self.total_bytes);
        if size_class > remaining {
            return false;
        }
        match max_regions {
            Some(max) => self.regions.len() + self.reserved_regions < max,
            None => true,
        }
    }

    fn should_unregister(&self, region_size: usize, max_size_class: usize) -> bool {
        self.closed || self.closing || region_size > max_size_class
    }
}

/// Computes allocation size class from a requested size.
fn compute_size_class(
    size: usize,
    min_size_class: usize,
    max_size_class: usize,
    alignment: usize,
) -> PyResult<(usize, bool)> {
    let size = size.max(1);
    if size > max_size_class {
        return Ok((align_up(size, alignment)?, true));
    }
    if size <= min_size_class {
        return Ok((min_size_class, false));
    }
    if size <= alignment {
        let mut power = 1usize;
        while power < size {
            power <<= 1;
        }
        return Ok((power, false));
    }
    Ok((align_up(size, alignment)?, false))
}

/// Round `size` up to the next multiple of `alignment`.
fn align_up(size: usize, alignment: usize) -> PyResult<usize> {
    size.checked_add(alignment - 1)
        .map(|s| (s / alignment) * alignment)
        .ok_or_else(|| PyRuntimeError::new_err("buffer size overflow"))
}

/// Shared pool handle: config (immutable) + state (Mutex) + condvar.
struct PoolShared {
    config: PoolConfig,
    state: Mutex<PoolState>,
    cvar: Condvar,
}

impl PoolShared {
    /// Allocate aligned memory and register it with the store.
    /// Caller must NOT hold the state mutex.
    fn allocate_and_register(&self, py: Python<'_>, size_class: usize) -> PyResult<Region> {
        let ptr = unsafe {
            let mut p: *mut libc::c_void = std::ptr::null_mut();
            if libc::posix_memalign(&mut p, self.config.alignment, size_class) != 0 || p.is_null() {
                return Err(PyRuntimeError::new_err("memory allocation failed"));
            }
            p as *mut u8
        };

        let ret: i32 =
            match self
                .config
                .store
                .call_method1(py, "register_buffer", (ptr as usize, size_class))
            {
                Ok(val) => val.extract(py)?,
                Err(e) => {
                    unsafe { libc::free(ptr as *mut libc::c_void) };
                    return Err(e);
                }
            };

        if ret != 0 {
            unsafe { libc::free(ptr as *mut libc::c_void) };
            return Err(PyRuntimeError::new_err("register_buffer failed"));
        }

        Ok(Region {
            ptr,
            size: size_class,
        })
    }

    /// Unregister a region with the store. Only frees memory if unregister succeeds.
    /// Caller must NOT hold the state mutex.
    fn unregister_region(&self, py: Python<'_>, region: &Region) -> PyResult<()> {
        let ret: i32 = self
            .config
            .store
            .call_method1(py, "unregister_buffer", (region.ptr as usize, region.size))?
            .extract(py)?;
        if ret != 0 {
            return Err(PyRuntimeError::new_err("unregister_buffer failed"));
        }
        unsafe { libc::free(region.ptr as *mut libc::c_void) };
        Ok(())
    }

    /// Reserve capacity under lock, allocate+register without lock, update bookkeeping.
    /// Returns None if capacity was consumed by a concurrent thread.
    fn reserve_allocate_register(
        &self,
        py: Python<'_>,
        size_class: usize,
    ) -> PyResult<Option<Region>> {
        // Reserve under lock.
        {
            let mut state = self.state.lock().unwrap();
            state.raise_if_not_open()?;
            if !state.has_capacity_for(size_class, self.config.max_bytes, self.config.max_regions) {
                return Ok(None);
            }
            state.reserved_bytes += size_class;
            state.reserved_regions += 1;
        }

        // Allocate (no lock held, GIL held for register_buffer).
        let result = self.allocate_and_register(py, size_class);

        // Unreserve and bookkeep under lock.
        let mut state = self.state.lock().unwrap();
        state.reserved_bytes -= size_class;
        state.reserved_regions -= 1;
        self.cvar.notify_one();

        if let Ok(ref region) = result {
            if state.closed || state.closing {
                drop(state);
                let _ = self.unregister_region(py, region);
                return Err(PyRuntimeError::new_err("registered buffer pool is closing"));
            }
            state.regions.insert(region.ptr as usize, region.size);
            state.total_bytes += size_class;
        }

        result.map(Some)
    }
}

// ---------------------------------------------------------------------------
// Python-exposed types
// ---------------------------------------------------------------------------

/// A lease on a registered buffer from the pool.
///
/// Exposes `ptr`, `size`, and `buffer` (a writable memoryview).
/// Use as a context manager or call `release()` explicitly.
/// While any memoryview obtained from `buffer` is alive, `release()` will
/// raise an error to prevent use-after-free.
#[pyclass]
pub struct RegisteredBufferLease {
    pool: Arc<PoolShared>,
    ptr: usize,
    requested_size: usize,
    region_size: usize,
    exports: Arc<AtomicUsize>,
    closed: bool,
}

#[pymethods]
impl RegisteredBufferLease {
    /// The registered memory address to pass to zero-copy APIs.
    #[getter]
    fn ptr(&self) -> PyResult<usize> {
        if self.closed {
            return Err(PyRuntimeError::new_err("registered buffer lease is closed"));
        }
        Ok(self.ptr)
    }

    /// The requested logical size in bytes.
    #[getter]
    fn size(&self) -> PyResult<usize> {
        if self.closed {
            return Err(PyRuntimeError::new_err("registered buffer lease is closed"));
        }
        Ok(self.requested_size)
    }

    /// A writable memoryview over the buffer. Do not use after `release()`.
    #[getter]
    fn buffer<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if self.closed {
            return Err(PyRuntimeError::new_err("registered buffer lease is closed"));
        }
        // Create a LeaseView wrapper that tracks exports.
        let view = LeaseView {
            ptr: self.ptr,
            size: self.requested_size,
            exports: Arc::clone(&self.exports),
        };
        // Increment export count *before* exposing the memoryview.
        self.exports.fetch_add(1, Ordering::Release);
        let py_view = Py::new(py, view)?;
        let mv = unsafe {
            let obj = py_view.as_ptr();
            let raw = ffi::PyMemoryView_FromObject(obj);
            if raw.is_null() {
                // Undo export count.
                self.exports.fetch_sub(1, Ordering::Release);
                return Err(PyErr::fetch(py));
            }
            Bound::from_owned_ptr(py, raw)
        };
        Ok(mv)
    }

    /// Release the buffer back to the pool (or unregister if oversized/closing).
    /// Raises if memoryviews obtained from `buffer` are still alive.
    fn release(&mut self, py: Python<'_>) -> PyResult<()> {
        self.do_release(py, true)
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.do_release(py, true)
    }
}

impl RegisteredBufferLease {
    /// Core release logic. If `check_exports` is true, raises on active views.
    fn do_release(&mut self, py: Python<'_>, check_exports: bool) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        if check_exports && self.exports.load(Ordering::Acquire) != 0 {
            return Err(PyRuntimeError::new_err(
                "cannot release registered buffer while exported views exist",
            ));
        }
        self.closed = true;

        let pool = &self.pool;
        let mut state = pool.state.lock().unwrap();
        state.in_use.remove(&self.ptr);

        if !state.should_unregister(self.region_size, pool.config.max_size_class) {
            // Return to free list.
            state
                .free
                .entry(self.region_size)
                .or_default()
                .push_back(Region {
                    ptr: self.ptr as *mut u8,
                    size: self.region_size,
                });
            pool.cvar.notify_one();
            return Ok(());
        }

        // Must unregister — drop lock first (Python call ahead).
        let region = Region {
            ptr: self.ptr as *mut u8,
            size: self.region_size,
        };
        drop(state);

        let unregister_result = pool.unregister_region(py, &region);

        // Update bookkeeping regardless.
        let mut state = pool.state.lock().unwrap();
        state.regions.remove(&self.ptr);
        state.total_bytes = state.total_bytes.saturating_sub(self.region_size);
        pool.cvar.notify_one();

        // If unregister failed, memory was NOT freed (by design).
        // We still mark the lease as closed to avoid double-release.
        unregister_result
    }
}

impl Drop for RegisteredBufferLease {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        // Best-effort release without export check (Python is GC'ing us).
        // We already have the GIL since PyO3 drops #[pyclass] during GC.
        Python::with_gil(|py| {
            let _ = self.do_release(py, false);
        });
    }
}

/// Internal buffer-protocol wrapper that tracks export lifetime.
/// When Python releases the memoryview's buffer, the export count decrements.
#[pyclass]
struct LeaseView {
    ptr: usize,
    size: usize,
    exports: Arc<AtomicUsize>,
}

impl Drop for LeaseView {
    fn drop(&mut self) {
        self.exports.fetch_sub(1, Ordering::Release);
    }
}

#[pymethods]
impl LeaseView {
    unsafe fn __getbuffer__(
        slf: PyRef<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: std::os::raw::c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyRuntimeError::new_err("null Py_buffer pointer"));
        }
        let buf = &*slf;
        unsafe {
            (*view).buf = buf.ptr as *mut std::os::raw::c_void;
            (*view).len = buf.size as isize;
            (*view).itemsize = 1;
            (*view).readonly = 0;
            (*view).ndim = 1;
            (*view).format = if flags & ffi::PyBUF_FORMAT != 0 {
                c"B".as_ptr() as *mut _
            } else {
                std::ptr::null_mut()
            };
            (*view).shape = if flags & ffi::PyBUF_ND != 0 {
                &mut (*view).len as *mut isize
            } else {
                std::ptr::null_mut()
            };
            (*view).strides = if flags & ffi::PyBUF_STRIDES != 0 {
                &mut (*view).itemsize as *mut isize
            } else {
                std::ptr::null_mut()
            };
            (*view).suboffsets = std::ptr::null_mut();
            (*view).internal = std::ptr::null_mut();
            (*view).obj = ffi::Py_NewRef(slf.as_ptr());
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

/// A pool of pre-registered buffers for zero-copy operations.
///
/// Reuses bounded registered scratch memory for `get_into()`, `get_into_ranges()`,
/// and `batch_get_into()`, avoiding repeated register/unregister overhead.
///
/// Usage:
///     pool = RegisteredBufferPool(store, max_bytes=256*1024*1024)
///     with pool.buffer(1024*1024) as lease:
///         n = store.get_into("key", lease.ptr, lease.size)
///         view = lease.buffer[:n]
///     pool.close()
#[pyclass]
pub struct RegisteredBufferPool {
    shared: Arc<PoolShared>,
}

#[pymethods]
impl RegisteredBufferPool {
    #[new]
    #[pyo3(signature = (
        store,
        max_bytes,
        *,
        min_size_class = 65536,
        max_size_class = None,
        alignment = 8388608,
        block_on_exhaustion = true,
        default_timeout = None,
        max_regions = None,
        prewarm_size = None,
        prewarm_count = 0
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        store: PyObject,
        max_bytes: usize,
        min_size_class: usize,
        max_size_class: Option<usize>,
        alignment: usize,
        block_on_exhaustion: bool,
        default_timeout: Option<f64>,
        max_regions: Option<usize>,
        prewarm_size: Option<usize>,
        prewarm_count: usize,
    ) -> PyResult<Self> {
        if store.is_none(py) {
            return Err(PyRuntimeError::new_err(
                "MooncakeDistributedStore is not initialized",
            ));
        }
        if max_bytes == 0 {
            return Err(PyRuntimeError::new_err("max_bytes must be positive"));
        }
        if min_size_class == 0 || alignment == 0 {
            return Err(PyRuntimeError::new_err(
                "min_size_class and alignment must be positive",
            ));
        }
        if alignment < std::mem::size_of::<*const ()>() || !alignment.is_power_of_two() {
            return Err(PyRuntimeError::new_err(
                "alignment must be a power of two and at least sizeof(void*)",
            ));
        }

        let max_size_class = max_size_class.unwrap_or(max_bytes).min(max_bytes);

        let pool = Self {
            shared: Arc::new(PoolShared {
                config: PoolConfig {
                    store,
                    max_bytes,
                    min_size_class,
                    max_size_class,
                    alignment,
                    block_on_exhaustion,
                    default_timeout,
                    max_regions,
                },
                state: Mutex::new(PoolState {
                    free: HashMap::new(),
                    regions: HashMap::new(),
                    in_use: HashSet::new(),
                    closed: false,
                    closing: false,
                    total_bytes: 0,
                    reserved_bytes: 0,
                    reserved_regions: 0,
                }),
                cvar: Condvar::new(),
            }),
        };

        if let Some(size) = prewarm_size {
            if prewarm_count > 0 {
                pool.prewarm(py, size, prewarm_count)?;
            }
        }

        Ok(pool)
    }

    /// Acquire a registered buffer of at least `size` bytes.
    /// Returns a `RegisteredBufferLease` that can be used as a context manager.
    #[pyo3(signature = (size, *, block = None, timeout = None))]
    fn acquire(
        &self,
        py: Python<'_>,
        size: usize,
        block: Option<bool>,
        timeout: Option<f64>,
    ) -> PyResult<RegisteredBufferLease> {
        let cfg = &self.shared.config;
        let (size_class, oversize) =
            compute_size_class(size, cfg.min_size_class, cfg.max_size_class, cfg.alignment)?;

        if size_class > cfg.max_bytes {
            return Err(PyRuntimeError::new_err(
                "requested buffer size exceeds pool capacity",
            ));
        }

        let should_block = block.unwrap_or(cfg.block_on_exhaustion);
        let timeout_s = timeout.or(cfg.default_timeout);
        if let Some(t) = timeout_s {
            if t < 0.0 {
                return Err(PyRuntimeError::new_err("timeout must be non-negative"));
            }
        }

        let deadline = timeout_s.map(|t| Instant::now() + Duration::from_secs_f64(t));

        // Fast path: try reuse from free list (no GIL release needed).
        if !oversize {
            let mut state = self.shared.state.lock().unwrap();
            state.raise_if_not_open()?;
            if let Some(queue) = state.free.get_mut(&size_class) {
                if let Some(region) = queue.pop_back() {
                    let ptr = region.ptr as usize;
                    state.in_use.insert(ptr);
                    return Ok(RegisteredBufferLease {
                        pool: Arc::clone(&self.shared),
                        ptr,
                        requested_size: size,
                        region_size: region.size,
                        exports: Arc::new(AtomicUsize::new(0)),
                        closed: false,
                    });
                }
            }
        }

        // Slow path: allocate or wait.
        loop {
            // Try allocate (needs GIL for register_buffer).
            if let Some(region) = self.shared.reserve_allocate_register(py, size_class)? {
                let ptr = region.ptr as usize;
                let mut state = self.shared.state.lock().unwrap();
                state.in_use.insert(ptr);
                return Ok(RegisteredBufferLease {
                    pool: Arc::clone(&self.shared),
                    ptr,
                    requested_size: size,
                    region_size: region.size,
                    exports: Arc::new(AtomicUsize::new(0)),
                    closed: false,
                });
            }

            // No capacity — block or fail.
            if !should_block {
                return Err(PyRuntimeError::new_err(
                    "registered buffer pool is exhausted",
                ));
            }

            // Wait (release GIL during condvar wait).
            py.allow_threads(|| {
                let state = self.shared.state.lock().unwrap();
                if let Some(dl) = deadline {
                    if Instant::now() >= dl {
                        return Err(PyRuntimeError::new_err(
                            "timed out waiting for registered buffer",
                        ));
                    }
                    drop(
                        self.shared
                            .cvar
                            .wait_timeout(state, dl - Instant::now())
                            .unwrap(),
                    );
                } else {
                    drop(self.shared.cvar.wait(state).unwrap());
                }
                Ok(())
            })?;

            // After wakeup, retry free list.
            if !oversize {
                let mut state = self.shared.state.lock().unwrap();
                if let Some(queue) = state.free.get_mut(&size_class) {
                    if let Some(region) = queue.pop_back() {
                        let ptr = region.ptr as usize;
                        state.in_use.insert(ptr);
                        return Ok(RegisteredBufferLease {
                            pool: Arc::clone(&self.shared),
                            ptr,
                            requested_size: size,
                            region_size: region.size,
                            exports: Arc::new(AtomicUsize::new(0)),
                            closed: false,
                        });
                    }
                }
            }
        }
    }

    /// Alias for `acquire()`.
    #[pyo3(signature = (size, *, block = None, timeout = None))]
    fn buffer(
        &self,
        py: Python<'_>,
        size: usize,
        block: Option<bool>,
        timeout: Option<f64>,
    ) -> PyResult<RegisteredBufferLease> {
        self.acquire(py, size, block, timeout)
    }

    /// Pre-allocate `count` buffers of the given `size` class.
    fn prewarm(&self, py: Python<'_>, size: usize, count: usize) -> PyResult<()> {
        let cfg = &self.shared.config;
        let (size_class, oversize) =
            compute_size_class(size, cfg.min_size_class, cfg.max_size_class, cfg.alignment)?;

        if oversize {
            return Err(PyRuntimeError::new_err("cannot prewarm oversize buffers"));
        }

        for _ in 0..count {
            let region = self
                .shared
                .reserve_allocate_register(py, size_class)?
                .ok_or_else(|| {
                    PyRuntimeError::new_err("registered buffer pool capacity exceeded")
                })?;

            let mut state = self.shared.state.lock().unwrap();
            if state.closed || state.closing {
                drop(state);
                let _ = self.shared.unregister_region(py, &region);
                return Err(PyRuntimeError::new_err("registered buffer pool is closing"));
            }
            state.free.entry(size_class).or_default().push_back(region);
            self.shared.cvar.notify_one();
        }

        Ok(())
    }

    /// Close the pool, unregistering all free buffers.
    /// Raises if any leases are still active.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let shared = &self.shared;

        // Mark closing and wait for in-flight reservations.
        {
            let mut state = shared.state.lock().unwrap();
            if state.closed {
                return Ok(());
            }
            state.closing = true;
        }

        // Wait for reservations to drain (release GIL).
        py.allow_threads(|| {
            let mut state = shared.state.lock().unwrap();
            while state.reserved_regions > 0 {
                state = shared.cvar.wait(state).unwrap();
            }
        });

        // Drain free list under lock.
        let to_unregister: Vec<Region>;
        {
            let mut state = shared.state.lock().unwrap();
            if !state.in_use.is_empty() {
                state.closing = false;
                shared.cvar.notify_one();
                return Err(PyRuntimeError::new_err(
                    "cannot close registered buffer pool with active leases",
                ));
            }
            to_unregister = state.free.drain().flat_map(|(_, q)| q).collect();
        }

        // Unregister all (no lock held, GIL held).
        for (i, region) in to_unregister.iter().enumerate() {
            if let Err(e) = shared.unregister_region(py, region) {
                // Put remaining back on failure.
                let mut state = shared.state.lock().unwrap();
                for remaining in &to_unregister[i + 1..] {
                    state
                        .free
                        .entry(remaining.size)
                        .or_default()
                        .push_back(Region {
                            ptr: remaining.ptr,
                            size: remaining.size,
                        });
                }
                state.closing = false;
                shared.cvar.notify_one();
                return Err(e);
            }
            // Bookkeep each successful unregister.
            let mut state = shared.state.lock().unwrap();
            state.regions.remove(&(region.ptr as usize));
            state.total_bytes = state.total_bytes.saturating_sub(region.size);
        }

        {
            let mut state = shared.state.lock().unwrap();
            state.closed = true;
            state.closing = false;
            shared.cvar.notify_one();
        }

        Ok(())
    }
}

/// Register buffer pool classes into the Python module.
pub fn register_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RegisteredBufferPool>()?;
    module.add_class::<RegisteredBufferLease>()?;
    Ok(())
}
