//! Python buffer pool backed by the store's setup-time local scratch buffer.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use mooncake_store_client::ScratchReservation;
use pyo3::exceptions::PyRuntimeError;
use pyo3::ffi;
use pyo3::prelude::*;

struct OverflowRegion {
    ptr: *mut u8,
}

// SAFETY: OverflowRegion owns an aligned heap allocation. The pointer is only
// registered/unregistered while the owning BufferLease is being acquired or
// released, and the lease state is synchronized through PoolShared.
unsafe impl Send for OverflowRegion {}
unsafe impl Sync for OverflowRegion {}

enum LeaseBacking {
    Local(PyObject),
    Overflow(OverflowRegion),
}

struct PoolConfig {
    store: PyObject,
    max_bytes: usize,
    alignment: usize,
    block_on_exhaustion: bool,
    default_timeout: Option<f64>,
    max_regions: Option<usize>,
}

struct PoolState {
    in_use: HashSet<usize>,
    closed: bool,
    closing: bool,
    total_bytes: usize,
    reserved_bytes: usize,
    reserved_regions: usize,
}

impl PoolState {
    fn raise_if_open(&self) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err("buffer pool is closed"));
        }
        if self.closing {
            return Err(PyRuntimeError::new_err("buffer pool is closing"));
        }
        Ok(())
    }

    fn has_capacity_for(&self, size: usize, max_bytes: usize, max_regions: Option<usize>) -> bool {
        if size > max_bytes {
            return false;
        }
        let Some(available) = max_bytes
            .checked_sub(self.total_bytes)
            .and_then(|value| value.checked_sub(self.reserved_bytes))
        else {
            return false;
        };
        if size > available {
            return false;
        }
        max_regions.is_none_or(|max| self.in_use.len() + self.reserved_regions < max)
    }
}

struct PoolShared {
    config: PoolConfig,
    state: Mutex<PoolState>,
    cvar: Condvar,
}

impl PoolShared {
    fn allocate_region(&self, py: Python<'_>, size: usize) -> PyResult<(usize, LeaseBacking)> {
        let local = self
            .config
            .store
            .call_method1(py, "local_buffer_pool_try_acquire", (size,))?;
        if local.is_none(py) {
            return self.allocate_overflow(py, size);
        }
        let ptr: usize = local.getattr(py, "ptr")?.extract(py)?;
        Ok((ptr, LeaseBacking::Local(local)))
    }

    fn allocate_overflow(&self, py: Python<'_>, size: usize) -> PyResult<(usize, LeaseBacking)> {
        let ptr = unsafe {
            let mut ptr: *mut libc::c_void = std::ptr::null_mut();
            if libc::posix_memalign(&mut ptr, self.config.alignment, size) != 0 || ptr.is_null() {
                return Err(PyRuntimeError::new_err("memory allocation failed"));
            }
            ptr as *mut u8
        };

        let ret: i32 =
            match self
                .config
                .store
                .call_method1(py, "register_buffer", (ptr as usize, size))
            {
                Ok(value) => value.extract(py)?,
                Err(error) => {
                    unsafe { libc::free(ptr as *mut libc::c_void) };
                    return Err(error);
                }
            };
        if ret != 0 {
            unsafe { libc::free(ptr as *mut libc::c_void) };
            return Err(PyRuntimeError::new_err(
                "overflow buffer registration failed",
            ));
        }

        Ok((ptr as usize, LeaseBacking::Overflow(OverflowRegion { ptr })))
    }

    fn unregister_overflow(&self, py: Python<'_>, region: &OverflowRegion) -> PyResult<()> {
        let ret: i32 = self
            .config
            .store
            .call_method1(py, "unregister_buffer", (region.ptr as usize,))?
            .extract(py)?;
        if ret != 0 {
            return Err(PyRuntimeError::new_err(
                "overflow buffer unregistration failed",
            ));
        }
        Ok(())
    }

    fn free_overflow(region: OverflowRegion) {
        unsafe { libc::free(region.ptr as *mut libc::c_void) };
    }

    fn reserve_allocate(
        &self,
        py: Python<'_>,
        size: usize,
    ) -> PyResult<Option<(usize, LeaseBacking)>> {
        {
            let mut state = self.state.lock().unwrap();
            state.raise_if_open()?;
            if !state.has_capacity_for(size, self.config.max_bytes, self.config.max_regions) {
                return Ok(None);
            }
            state.reserved_bytes += size;
            state.reserved_regions += 1;
        }

        let result = self.allocate_region(py, size);

        let mut state = self.state.lock().unwrap();
        state.reserved_bytes -= size;
        state.reserved_regions -= 1;
        self.cvar.notify_all();

        if state.closed || state.closing {
            drop(state);
            if let Ok((_, LeaseBacking::Overflow(region))) = result {
                let _ = self.unregister_overflow(py, &region);
                Self::free_overflow(region);
            }
            return Err(PyRuntimeError::new_err("buffer pool is closing"));
        }

        match result {
            Ok((ptr, backing)) => {
                state.in_use.insert(ptr);
                state.total_bytes += size;
                Ok(Some((ptr, backing)))
            }
            Err(error) => Err(error),
        }
    }
}

#[pyclass]
pub struct LocalBufferLease {
    reservation: Option<ScratchReservation>,
    ptr: usize,
    size: usize,
}

#[pymethods]
impl LocalBufferLease {
    #[getter]
    fn ptr(&self) -> PyResult<usize> {
        if self.reservation.is_none() {
            return Err(PyRuntimeError::new_err("local buffer lease is closed"));
        }
        Ok(self.ptr)
    }

    #[getter]
    fn size(&self) -> PyResult<usize> {
        if self.reservation.is_none() {
            return Err(PyRuntimeError::new_err("local buffer lease is closed"));
        }
        Ok(self.size)
    }

    fn release(&mut self) {
        self.reservation.take();
    }
}

impl LocalBufferLease {
    pub fn new(reservation: ScratchReservation, size: usize) -> PyResult<Self> {
        let ptr = reservation
            .first()
            .ok_or_else(|| PyRuntimeError::new_err("local buffer allocation is empty"))?
            .addr as usize;
        Ok(Self {
            reservation: Some(reservation),
            ptr,
            size,
        })
    }
}

impl Drop for LocalBufferLease {
    fn drop(&mut self) {
        self.reservation.take();
    }
}

#[pyclass]
pub struct BufferLease {
    pool: Arc<PoolShared>,
    ptr: usize,
    requested_size: usize,
    region_size: usize,
    backing: Option<LeaseBacking>,
    exports: Arc<AtomicUsize>,
    closed: bool,
}

#[pymethods]
impl BufferLease {
    #[getter]
    fn ptr(&self) -> PyResult<usize> {
        if self.closed {
            return Err(PyRuntimeError::new_err("buffer lease is closed"));
        }
        Ok(self.ptr)
    }

    #[getter]
    fn size(&self) -> PyResult<usize> {
        if self.closed {
            return Err(PyRuntimeError::new_err("buffer lease is closed"));
        }
        Ok(self.requested_size)
    }

    #[getter]
    fn buffer<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if slf.closed {
            return Err(PyRuntimeError::new_err("buffer lease is closed"));
        }
        let ptr = slf.ptr;
        let size = slf.requested_size;
        let exports = Arc::clone(&slf.exports);
        let owner = slf.into_pyobject(py)?.unbind().into_any();
        exports.fetch_add(1, Ordering::Release);
        let view = LeaseView {
            ptr,
            size,
            exports: Arc::clone(&exports),
            _owner: owner,
        };
        let py_view = match Py::new(py, view) {
            Ok(view) => view,
            Err(error) => {
                exports.fetch_sub(1, Ordering::Release);
                return Err(error);
            }
        };
        let raw = unsafe { ffi::PyMemoryView_FromObject(py_view.as_ptr()) };
        if raw.is_null() {
            exports.fetch_sub(1, Ordering::Release);
            return Err(PyErr::fetch(py));
        }
        Ok(unsafe { Bound::from_owned_ptr(py, raw) })
    }

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

impl BufferLease {
    fn do_release(&mut self, py: Python<'_>, check_exports: bool) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        if check_exports && self.exports.load(Ordering::Acquire) != 0 {
            return Err(PyRuntimeError::new_err(
                "cannot release buffer while exported views exist",
            ));
        }

        let mut release_result = match self.backing.as_ref() {
            Some(LeaseBacking::Local(lease)) => lease.call_method0(py, "release").map(|_| ()),
            Some(LeaseBacking::Overflow(region)) => self.pool.unregister_overflow(py, region),
            None => Ok(()),
        };
        if release_result.is_err() && check_exports {
            return release_result;
        }
        if release_result.is_err() {
            if let Some(LeaseBacking::Overflow(region)) = self.backing.as_ref() {
                for _ in 0..2 {
                    release_result = self.pool.unregister_overflow(py, region);
                    if release_result.is_ok() {
                        break;
                    }
                }
            }
        }

        let should_free_overflow = release_result.is_ok();
        self.closed = true;
        let backing = self.backing.take();
        if should_free_overflow {
            if let Some(LeaseBacking::Overflow(region)) = backing {
                PoolShared::free_overflow(region);
            }
        }
        {
            let mut state = self.pool.state.lock().unwrap();
            state.in_use.remove(&self.ptr);
            state.total_bytes = state.total_bytes.saturating_sub(self.region_size);
        }
        self.pool.cvar.notify_all();

        release_result
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        Python::with_gil(|py| {
            let _ = self.do_release(py, false);
        });
    }
}

#[pyclass]
struct LeaseView {
    ptr: usize,
    size: usize,
    exports: Arc<AtomicUsize>,
    _owner: Py<PyAny>,
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
        let buffer = &*slf;
        unsafe {
            (*view).buf = buffer.ptr as *mut std::os::raw::c_void;
            (*view).len = buffer.size as isize;
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

#[pyclass(name = "BufferPool")]
pub struct BufferPool {
    shared: Arc<PoolShared>,
}

#[pymethods]
impl BufferPool {
    #[new]
    #[pyo3(signature = (
        store,
        max_bytes = 0,
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
        if default_timeout.is_some_and(|timeout| timeout < 0.0) {
            return Err(PyRuntimeError::new_err("timeout must be non-negative"));
        }

        let local_capacity = store
            .call_method0(py, "local_buffer_pool_capacity")
            .and_then(|value| value.extract::<usize>(py))?;
        if local_capacity == 0 {
            return Err(PyRuntimeError::new_err(
                "BufferPool requires a store configured with a local buffer",
            ));
        }
        let _ = max_size_class;
        let max_bytes = if max_bytes == 0 {
            local_capacity.saturating_mul(2).max(local_capacity)
        } else {
            max_bytes.max(local_capacity)
        };

        let pool = Self {
            shared: Arc::new(PoolShared {
                config: PoolConfig {
                    store,
                    max_bytes,
                    alignment,
                    block_on_exhaustion,
                    default_timeout,
                    max_regions,
                },
                state: Mutex::new(PoolState {
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
        if let Some(prewarm_size) = prewarm_size.filter(|_| prewarm_count > 0) {
            pool.prewarm(prewarm_size, prewarm_count)?;
        }
        Ok(pool)
    }

    #[pyo3(signature = (size, *, block = None, timeout = None))]
    fn acquire(
        &self,
        py: Python<'_>,
        size: usize,
        block: Option<bool>,
        timeout: Option<f64>,
    ) -> PyResult<BufferLease> {
        let allocation_size = size.max(1);
        if allocation_size > self.shared.config.max_bytes {
            return Err(PyRuntimeError::new_err(
                "requested buffer size exceeds pool capacity",
            ));
        }
        let should_block = block.unwrap_or(self.shared.config.block_on_exhaustion);
        let timeout_s = timeout.or(self.shared.config.default_timeout);
        if timeout_s.is_some_and(|timeout| timeout < 0.0) {
            return Err(PyRuntimeError::new_err("timeout must be non-negative"));
        }
        let deadline = timeout_s.map(|timeout| Instant::now() + Duration::from_secs_f64(timeout));

        loop {
            if let Some((ptr, backing)) = self.shared.reserve_allocate(py, allocation_size)? {
                return Ok(BufferLease {
                    pool: Arc::clone(&self.shared),
                    ptr,
                    requested_size: size,
                    region_size: allocation_size,
                    backing: Some(backing),
                    exports: Arc::new(AtomicUsize::new(0)),
                    closed: false,
                });
            }
            if !should_block {
                return Err(PyRuntimeError::new_err("buffer pool is exhausted"));
            }
            py.allow_threads(|| {
                let state = self.shared.state.lock().unwrap();
                if let Some(deadline) = deadline {
                    if Instant::now() >= deadline {
                        return Err(PyRuntimeError::new_err("timed out waiting for buffer"));
                    }
                    drop(
                        self.shared
                            .cvar
                            .wait_timeout(state, deadline - Instant::now())
                            .unwrap(),
                    );
                } else {
                    drop(self.shared.cvar.wait(state).unwrap());
                }
                Ok(())
            })?;
        }
    }

    #[pyo3(signature = (size, *, block = None, timeout = None))]
    fn buffer(
        &self,
        py: Python<'_>,
        size: usize,
        block: Option<bool>,
        timeout: Option<f64>,
    ) -> PyResult<BufferLease> {
        self.acquire(py, size, block, timeout)
    }

    fn prewarm(&self, _size: usize, _count: usize) -> PyResult<()> {
        Ok(())
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            return Ok(());
        }
        state.closing = true;
        drop(state);
        py.allow_threads(|| {
            let mut state = self.shared.state.lock().unwrap();
            while state.reserved_regions > 0 {
                state = self.shared.cvar.wait(state).unwrap();
            }
        });
        let mut state = self.shared.state.lock().unwrap();
        if !state.in_use.is_empty() {
            state.closing = false;
            self.shared.cvar.notify_all();
            return Err(PyRuntimeError::new_err(
                "cannot close buffer pool with active leases",
            ));
        }
        state.closed = true;
        state.closing = false;
        self.shared.cvar.notify_all();
        Ok(())
    }
}

pub fn register_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<LocalBufferLease>()?;
    module.add_class::<BufferPool>()?;
    module.add_class::<BufferLease>()?;
    Ok(())
}
