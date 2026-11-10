impl ExtentStoreFixedBufferRegistry {
    fn new(
        ring: &mut IoUring,
        counters: std::sync::Arc<ExtentStoreIoCounters>,
    ) -> io::Result<Self> {
        let legacy_sparse_buffers = match ring
            .submitter()
            .register_buffers_sparse(EXTENT_STORE_IO_WORKER_FIXED_BUFFER_SLOTS)
        {
            Ok(()) => {
                counters.record_fixed_buffer_sparse_table_init(true, None);
                false
            }
            Err(sparse_error) => {
                counters.record_fixed_buffer_sparse_table_init(false, Some(&sparse_error));
                let buffers = vec![empty_iovec(); EXTENT_STORE_IO_WORKER_FIXED_BUFFER_SLOTS as usize];
                match register_buffers2(ring.as_raw_fd(), &buffers) {
                    Ok(()) => {
                        counters.record_fixed_buffer_array_table_init(true, None);
                        true
                    }
                    Err(array_error) => {
                        counters.record_fixed_buffer_array_table_init(false, Some(&array_error));
                        counters.record_fixed_buffer_table_init(false);
                        warn!(
                            sparse_error = %sparse_error,
                            array_error = %array_error,
                            "extent store fixed buffer table registration failed; using normal buffers"
                        );
                        return Err(array_error);
                    }
                }
            }
        };
        counters.record_fixed_buffer_table_init(true);
        Ok(Self {
            free_slots: (0..EXTENT_STORE_IO_WORKER_FIXED_BUFFER_SLOTS as u16)
                .rev()
                .collect(),
            retired_buffers: Vec::new(),
            legacy_sparse_buffers,
            disabled: false,
            counters,
        })
    }

    fn register(&mut self, ring: &mut IoUring, buffer: *mut u8, len: usize) -> Option<u16> {
        if self.disabled || len == 0 {
            self.counters.record_fixed_buffer_register(false);
            return None;
        }
        let Some(slot) = self.free_slots.pop() else {
            self.counters.record_fixed_buffer_slot_exhaustion();
            self.counters.record_fixed_buffer_register(false);
            return None;
        };
        let iovec = libc::iovec {
            iov_base: buffer.cast::<c_void>(),
            iov_len: len,
        };
        let update = if self.legacy_sparse_buffers {
            register_buffers_update(ring.as_raw_fd(), slot as u32, &[iovec])
        } else {
            unsafe {
                ring.submitter()
                    .register_buffers_update(slot as u32, &[iovec], None)
            }
        };
        if let Err(error) = update {
            warn!(
                error = %error,
                "extent store fixed buffer registration update failed; using normal buffers"
            );
            self.disabled = true;
            self.free_slots.clear();
            self.counters.record_fixed_buffer_register(false);
            self.counters.record_fixed_buffer_update_errno(&error);
            return None;
        }
        self.counters.record_fixed_buffer_register(true);
        Some(slot)
    }

    fn release(
        &mut self,
        ring: &mut IoUring,
        slot: u16,
        buffer: AlignedExtentStoreBuffer,
    ) -> bool {
        let update = if self.legacy_sparse_buffers {
            register_buffers_update(ring.as_raw_fd(), slot as u32, &[empty_iovec()])
        } else {
            unsafe {
                ring.submitter().register_buffers_update(
                    slot as u32,
                    &[empty_iovec()],
                    None,
                )
            }
        };
        if let Err(error) = update {
            warn!(
                error = %error,
                slot,
                "extent store fixed buffer release update failed; retaining backing allocation"
            );
            self.counters.record_fixed_buffer_update_errno(&error);
            self.retired_buffers.push(buffer);
            return false;
        }
        if !self.disabled {
            self.free_slots.push(slot);
        }
        true
    }

    fn unregister(mut self, ring: &IoUring) {
        if ring.submitter().unregister_buffers().is_err() {
            for buffer in self.retired_buffers.drain(..) {
                std::mem::forget(buffer);
            }
        }
    }
}

impl ExtentStoreFixedFileRegistry {
    fn new(
        ring: &mut IoUring,
        counters: std::sync::Arc<ExtentStoreIoCounters>,
    ) -> io::Result<Self> {
        let sparse_files = match ring
            .submitter()
            .register_files_sparse(EXTENT_STORE_IO_WORKER_FIXED_FILE_SLOTS)
        {
            Ok(()) => {
                counters.record_fixed_file_sparse_table_init(true, None);
                None
            }
            Err(sparse_error) => {
                counters.record_fixed_file_sparse_table_init(false, Some(&sparse_error));
                let files = vec![EXTENT_STORE_FIXED_FILE_SKIP; EXTENT_STORE_IO_WORKER_FIXED_FILE_SLOTS as usize];
                match register_files2(ring.as_raw_fd(), &files) {
                    Ok(()) => {
                        counters.record_fixed_file_array_table_init(true, None);
                        Some(files)
                    }
                    Err(array_error) => {
                        counters.record_fixed_file_array_table_init(false, Some(&array_error));
                        counters.record_fixed_file_table_init(false);
                        warn!(
                            sparse_error = %sparse_error,
                            array_error = %array_error,
                            "extent store fixed file table registration failed; using raw file descriptors"
                        );
                        return Err(array_error);
                    }
                }
            }
        };
        counters.record_fixed_file_table_init(true);
        Ok(Self {
            slots_by_key: BTreeMap::new(),
            free_slots: (0..EXTENT_STORE_IO_WORKER_FIXED_FILE_SLOTS).rev().collect(),
            sparse_files,
            disabled: false,
            counters,
        })
    }

    fn unregister(self, ring: &IoUring) {
        let _ = ring.submitter().unregister_files();
    }

    fn fixed(
        &mut self,
        ring: &mut IoUring,
        key: ExtentStoreFixedFileKey,
        fd: RawFd,
    ) -> Option<types::Fixed> {
        if self.disabled {
            return None;
        }
        if let Some(slot) = self.slots_by_key.get(&key) {
            self.counters.record_fixed_file_hit();
            return Some(types::Fixed(*slot));
        }
        let Some(slot) = self.free_slots.pop() else {
            self.counters.record_fixed_file_slot_exhaustion();
            return None;
        };
        let update = self.update_slot(ring, slot, fd);
        match update {
            Ok(()) => {
                self.slots_by_key.insert(key, slot);
                self.counters.record_fixed_file_register(true);
                self.counters.record_fixed_file_hit();
                Some(types::Fixed(slot))
            }
            Err(error) => {
                self.free_slots.push(slot);
                warn!(
                    error = %error,
                    "extent store fixed file registration update failed; using raw file descriptors"
                );
                self.disabled = true;
                self.free_slots.clear();
                self.counters.record_fixed_file_register(false);
                self.counters.record_fixed_file_update_errno(&error);
                None
            }
        }
    }

    fn unregister_key(&mut self, ring: &mut IoUring, key: ExtentStoreFixedFileKey) -> Result<usize> {
        if self.disabled {
            return Ok(0);
        }
        let Some(slot) = self.slots_by_key.remove(&key) else {
            return Ok(0);
        };
        if let Err(error) = self.update_slot(ring, slot, EXTENT_STORE_FIXED_FILE_SKIP) {
            self.slots_by_key.insert(key, slot);
            self.counters.record_fixed_file_update_errno(&error);
            return Err(StoreError::Transport(format!(
                "failed to unregister extent store fixed file slot {slot}: {error}"
            )));
        }
        self.free_slots.push(slot);
        Ok(0)
    }

    fn update_slot(&mut self, ring: &mut IoUring, slot: u32, fd: RawFd) -> io::Result<()> {
        let updated = if self.sparse_files.is_some() {
            register_files_update(ring.as_raw_fd(), slot, &[fd]).map(|value| value as usize)
        } else {
            ring.submitter().register_files_update(slot, &[fd])
        }?;
        if updated == 1 {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "fixed file update changed {updated} entries"
            )))
        }
    }
}

fn register_files2(ring_fd: RawFd, files: &[RawFd]) -> io::Result<()> {
    let request = ExtentStoreIoUringRsrcRegister {
        nr: files.len() as u32,
        data: files.as_ptr() as u64,
        ..Default::default()
    };
    io_uring_register_syscall(
        ring_fd,
        IORING_REGISTER_FILES2,
        ptr::from_ref(&request).cast(),
        std::mem::size_of::<ExtentStoreIoUringRsrcRegister>() as u32,
    )
    .map(drop)
}

fn register_buffers2(ring_fd: RawFd, buffers: &[libc::iovec]) -> io::Result<()> {
    let request = ExtentStoreIoUringRsrcRegister {
        nr: buffers.len() as u32,
        data: buffers.as_ptr() as u64,
        ..Default::default()
    };
    io_uring_register_syscall(
        ring_fd,
        IORING_REGISTER_BUFFERS2,
        ptr::from_ref(&request).cast(),
        std::mem::size_of::<ExtentStoreIoUringRsrcRegister>() as u32,
    )
    .map(drop)
}

fn register_files_update(ring_fd: RawFd, offset: u32, files: &[RawFd]) -> io::Result<i32> {
    let request = ExtentStoreIoUringRsrcUpdate {
        offset,
        data: files.as_ptr() as u64,
        ..Default::default()
    };
    io_uring_register_syscall(
        ring_fd,
        IORING_REGISTER_FILES_UPDATE,
        ptr::from_ref(&request).cast(),
        files.len() as u32,
    )
}

fn register_buffers_update(ring_fd: RawFd, offset: u32, buffers: &[libc::iovec]) -> io::Result<()> {
    let request = ExtentStoreIoUringRsrcUpdate2 {
        offset,
        data: buffers.as_ptr() as u64,
        nr: buffers.len() as u32,
        ..Default::default()
    };
    io_uring_register_syscall(
        ring_fd,
        IORING_REGISTER_BUFFERS_UPDATE,
        ptr::from_ref(&request).cast(),
        std::mem::size_of::<ExtentStoreIoUringRsrcUpdate2>() as u32,
    )
    .map(drop)
}

fn io_uring_register_syscall(
    ring_fd: RawFd,
    opcode: libc::c_uint,
    arg: *const libc::c_void,
    nr_args: libc::c_uint,
) -> io::Result<i32> {
    let result = unsafe {
        libc::syscall(
            libc::SYS_io_uring_register,
            ring_fd as libc::c_long,
            opcode as libc::c_long,
            arg as libc::c_long,
            nr_args as libc::c_long,
        )
    };
    if result >= 0 {
        Ok(result as i32)
    } else {
        Err(io::Error::last_os_error())
    }
}

fn empty_iovec() -> libc::iovec {
    libc::iovec {
        iov_base: ptr::null_mut(),
        iov_len: 0,
    }
}

fn error_errno(error: Option<&io::Error>) -> u64 {
    error.and_then(io::Error::raw_os_error).unwrap_or(0) as u64
}

impl<'a> ExtentStoreFixedFileGuard<'a> {
    fn new(
        ring: &'a mut IoUring,
        registry: Option<&'a mut ExtentStoreFixedFileRegistry>,
        fd: RawFd,
        fixed_file_key: Option<ExtentStoreFixedFileKey>,
    ) -> Self {
        Self {
            ring,
            registry,
            fd,
            fixed_file_key,
        }
    }

    fn target(&mut self) -> ExtentStoreIoTarget {
        if let Some(key) = self.fixed_file_key {
            if let Some(registry) = self.registry.as_deref_mut() {
                if let Some(fixed) = registry.fixed(self.ring, key, self.fd) {
                    return ExtentStoreIoTarget::Fixed(fixed);
                }
            }
        }
        ExtentStoreIoTarget::Fd(types::Fd(self.fd))
    }

    fn requested_fixed_file(&self) -> bool {
        self.fixed_file_key.is_some()
    }
}

enum ExtentStoreIoTarget {
    Fd(types::Fd),
    Fixed(types::Fixed),
}

impl ExtentStoreIoOp {
    fn build_entry(
        self,
        ring: &mut IoUring,
        fixed_files: Option<&mut ExtentStoreFixedFileRegistry>,
        fixed_buffers: Option<&mut ExtentStoreFixedBufferRegistry>,
        counters: &ExtentStoreIoCounters,
        user_data: u64,
    ) -> Option<io_uring::squeue::Entry> {
        let entry = match self {
            Self::Read {
                fd,
                buf,
                len,
                offset,
                fixed_file_key,
                fixed_buffer,
            } => {
                let mut file =
                    ExtentStoreFixedFileGuard::new(ring, fixed_files, fd, fixed_file_key);
                let requested_fixed_file = file.requested_fixed_file();
                match (file.target(), fixed_buffer) {
                    (ExtentStoreIoTarget::Fixed(fixed), Some(buf_index)) => {
                        counters.record_read_fixed();
                        opcode::ReadFixed::new(fixed, buf, len, buf_index)
                            .offset(offset)
                            .build()
                    }
                    (ExtentStoreIoTarget::Fd(fd), Some(buf_index)) => {
                        if requested_fixed_file {
                            counters.record_raw_fd_fallback();
                        }
                        counters.record_read_fixed();
                        opcode::ReadFixed::new(fd, buf, len, buf_index)
                            .offset(offset)
                            .build()
                    }
                    (ExtentStoreIoTarget::Fixed(fixed), None) => {
                        opcode::Read::new(fixed, buf, len).offset(offset).build()
                    }
                    (ExtentStoreIoTarget::Fd(fd), None) => {
                        if requested_fixed_file {
                            counters.record_raw_fd_fallback();
                        }
                        opcode::Read::new(fd, buf, len).offset(offset).build()
                    }
                }
            }
            Self::Write {
                fd,
                buf,
                len,
                offset,
                fixed_file_key,
                fixed_buffer,
            } => {
                let mut file =
                    ExtentStoreFixedFileGuard::new(ring, fixed_files, fd, fixed_file_key);
                let requested_fixed_file = file.requested_fixed_file();
                match (file.target(), fixed_buffer) {
                    (ExtentStoreIoTarget::Fixed(fixed), Some(buf_index)) => {
                        counters.record_write_fixed();
                        opcode::WriteFixed::new(fixed, buf, len, buf_index)
                            .offset(offset)
                            .build()
                    }
                    (ExtentStoreIoTarget::Fd(fd), Some(buf_index)) => {
                        if requested_fixed_file {
                            counters.record_raw_fd_fallback();
                        }
                        counters.record_write_fixed();
                        opcode::WriteFixed::new(fd, buf, len, buf_index)
                            .offset(offset)
                            .build()
                    }
                    (ExtentStoreIoTarget::Fixed(fixed), None) => {
                        opcode::Write::new(fixed, buf, len).offset(offset).build()
                    }
                    (ExtentStoreIoTarget::Fd(fd), None) => {
                        if requested_fixed_file {
                            counters.record_raw_fd_fallback();
                        }
                        opcode::Write::new(fd, buf, len).offset(offset).build()
                    }
                }
            }
            Self::Writev {
                fd,
                iovecs,
                len,
                offset,
                fixed_file_key,
            } => {
                let mut file =
                    ExtentStoreFixedFileGuard::new(ring, fixed_files, fd, fixed_file_key);
                let requested_fixed_file = file.requested_fixed_file();
                match file.target() {
                    ExtentStoreIoTarget::Fixed(fixed) => opcode::Writev::new(fixed, iovecs, len)
                        .offset(offset)
                        .build(),
                    ExtentStoreIoTarget::Fd(fd) => {
                        if requested_fixed_file {
                            counters.record_raw_fd_fallback();
                        }
                        opcode::Writev::new(fd, iovecs, len).offset(offset).build()
                    }
                }
            }
            Self::RegisterBuffer {
                buffer,
                len,
                completion,
            } => {
                let slot = fixed_buffers.and_then(|buffers| buffers.register(ring, buffer, len));
                let _ = completion.send(slot);
                return None;
            }
            Self::ReleaseBuffer {
                slot,
                buffer,
                completion,
            } => {
                let released = fixed_buffers
                    .map(|buffers| buffers.release(ring, slot, buffer))
                    .unwrap_or(true);
                let _ = completion.send(released);
                return None;
            }
            Self::Nop => return None,
            Self::UnregisterFile { .. } => return None,
        };
        let entry = entry.user_data(user_data);
        Some(match self {
            Self::Read { len, .. } if len >= EXTENT_STORE_IO_WORKER_FORCE_ASYNC_READ_BYTES => {
                entry.flags(squeue::Flags::ASYNC)
            }
            Self::Write { len, .. } | Self::Writev { len, .. }
                if len >= EXTENT_STORE_IO_WORKER_FORCE_ASYNC_WRITE_BYTES =>
            {
                entry.flags(squeue::Flags::ASYNC)
            }
            _ => entry,
        })
    }
}

struct ExtentStoreScratchArena {
    queue: std::sync::Arc<ExtentStoreIoWorkerQueue>,
    state: Mutex<ExtentStoreScratchArenaState>,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
    is_read: bool,
    max_bytes: usize,
}

struct ExtentStoreScratchArenaState {
    buffers: Vec<ExtentStoreScratchPooledBuffer>,
    retired_buffers: Vec<AlignedExtentStoreBuffer>,
    allocated_bytes: usize,
    leased_bytes: usize,
}

struct ExtentStoreScratchPooledBuffer {
    buffer: AlignedExtentStoreBuffer,
    fixed_slot: Option<u16>,
}

struct ExtentStoreScratchLease {
    buffer: Option<AlignedExtentStoreBuffer>,
    fixed_slot: Option<u16>,
    arena: std::sync::Arc<ExtentStoreScratchArena>,
}

pub(super) struct ExtentStoreBufferPool {
    state: Mutex<ExtentStoreBufferPoolState>,
    max_bytes: usize,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
}

struct ExtentStoreBufferPoolState {
    buffers: Vec<AlignedExtentStoreBuffer>,
    allocated_bytes: usize,
}

pub(super) struct ExtentStoreBufferLease {
    buffer: Option<AlignedExtentStoreBuffer>,
    len: usize,
    pool: std::sync::Arc<ExtentStoreBufferPool>,
}

struct ExtentStoreIoPriority {
    state: Mutex<ExtentStoreIoPriorityState>,
    changed: Condvar,
}

#[derive(Default)]
struct ExtentStoreIoPriorityState {
    active_reads: usize,
    waiting_reads: usize,
    waiting_writes: usize,
    active_write: bool,
}

enum ExtentStoreIoPriorityPermit<'a> {
    Read(&'a ExtentStoreIoPriority),
    Write(&'a ExtentStoreIoPriority),
}

impl ExtentStoreParallelBufferedReadPool {
    fn from_env() -> Self {
        let threads = parallel_buffered_read_threads();
        if threads <= 1 {
            return Self { workers: Vec::new() };
        }
        let workers = (0..threads)
            .map(ExtentStoreParallelBufferedReadWorker::new)
            .collect();
        Self { workers }
    }

    fn enabled(&self) -> bool {
        !self.workers.is_empty()
    }

    fn read_batch(
        &self,
        reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
        completed: &[bool],
        counters: &ExtentStoreIoCounters,
    ) -> Vec<Result<()>> {
        if !self.enabled() || reads.len() < 2 {
            return read_buffered_batch_blocking_serial(reads, completed, counters);
        }

        let chunk_size = reads.len().div_ceil(self.workers.len()).max(1);
        let mut receivers = Vec::new();
        for (worker, chunk_index) in self.workers.iter().zip(0..) {
            let start = chunk_index * chunk_size;
            if start >= reads.len() {
                break;
            }
            let end = (start + chunk_size).min(reads.len());
            let mut worker_reads = Vec::with_capacity(end - start);
            let mut completed_in_chunk = Vec::with_capacity(end - start);
            for (index, (_, read)) in reads.iter_mut().enumerate().take(end).skip(start) {
                let is_completed = completed.get(index).copied().unwrap_or(false);
                completed_in_chunk.push(is_completed);
                if is_completed {
                    continue;
                }
                let len = read.locator.value_len as usize;
                counters.record_buffered_read(len);
                worker_reads.push(ExtentStoreParallelBufferedRead {
                    file: read.file.clone(),
                    offset: read.locator.offset + read.locator.value_offset,
                    dst: read.dst[..len].as_mut_ptr(),
                    len,
                });
            }
            if worker_reads.is_empty() {
                receivers.push((completed_in_chunk, Ok(None)));
                continue;
            }
            let (tx, rx) = extent_store_channel();
            let completion = match worker.submit(ExtentStoreParallelBufferedReadRequest {
                reads: worker_reads,
                completion: tx,
            }) {
                Ok(()) => Ok(Some(rx)),
                Err(error) => Err(error),
            };
            receivers.push((completed_in_chunk, completion));
        }

        let mut results = Vec::with_capacity(reads.len());
        for (completed_in_chunk, completion) in receivers {
            let mut worker_results = match completion {
                Ok(Some(rx)) => match rx.recv() {
                    Ok(results) => results.into_iter(),
                    Err(error) => vec![Err(StoreError::Transport(format!(
                        "parallel buffered read worker completion channel closed: {error}"
                    )))]
                    .into_iter(),
                },
                Ok(None) => Vec::new().into_iter(),
                Err(error) => vec![Err(error)].into_iter(),
            };
            for completed in completed_in_chunk {
                if completed {
                    results.push(Ok(()));
                } else {
                    results.push(worker_results.next().unwrap_or_else(|| {
                        Err(StoreError::InvalidState(
                            "parallel buffered read worker returned too few results".to_string(),
                        ))
                    }));
                }
            }
            if worker_results.next().is_some() {
                results.push(Err(StoreError::InvalidState(
                    "parallel buffered read worker returned too many results".to_string(),
                )));
            }
        }
        results
    }
}

impl ExtentStoreParallelBufferedReadWorker {
    fn new(index: usize) -> Self {
        let queue = std::sync::Arc::new(ExtentStoreParallelBufferedReadQueue {
            state: Mutex::new(ExtentStoreParallelBufferedReadState::default()),
            changed: Condvar::new(),
        });
        let worker_queue = queue.clone();
        let handle = thread::Builder::new()
            .name(format!("extent-store-buffered-read-{index}"))
            .spawn(move || run_extent_store_parallel_buffered_read_worker(worker_queue))
            .expect("extent store parallel buffered read worker should spawn");
        Self {
            queue,
            handle: Some(handle),
        }
    }

    fn submit(&self, request: ExtentStoreParallelBufferedReadRequest) -> Result<()> {
        let mut state = self.queue.state.lock();
        if state.shutdown {
            return Err(StoreError::Transport(
                "parallel buffered read worker is shut down".to_string(),
            ));
        }
        debug_assert!(state.request.is_none());
        state.request = Some(request);
        self.queue.changed.notify_one();
        Ok(())
    }
}

impl Drop for ExtentStoreParallelBufferedReadPool {
    fn drop(&mut self) {
        for worker in &mut self.workers {
            {
                let mut state = worker.queue.state.lock();
                state.shutdown = true;
                worker.queue.changed.notify_all();
            }
            if let Some(handle) = worker.handle.take() {
                let _ = handle.join();
            }
        }
    }
}

fn run_extent_store_parallel_buffered_read_worker(
    queue: std::sync::Arc<ExtentStoreParallelBufferedReadQueue>,
) {
    loop {
        let request = {
            let mut state = queue.state.lock();
            loop {
                if let Some(request) = state.request.take() {
                    break request;
                }
                if state.shutdown {
                    return;
                }
                queue.changed.wait(&mut state);
            }
        };
        let results = request
            .reads
            .iter()
            .map(|read| {
                let dst = unsafe { std::slice::from_raw_parts_mut(read.dst, read.len) };
                read_exact_at_blocking(&read.file, read.offset, dst)
            })
            .collect();
        let _ = request.completion.send(results);
    }
}


impl ExtentStoreIoWorker {
    fn new(
        mut ring: IoUring,
        counters: std::sync::Arc<ExtentStoreIoCounters>,
        name: &str,
        is_read: bool,
    ) -> Self {
        let fixed_files = ExtentStoreFixedFileRegistry::new(&mut ring, counters.clone()).ok();
        let fixed_buffers = ExtentStoreFixedBufferRegistry::new(&mut ring, counters.clone()).ok();
        let queue = std::sync::Arc::new(ExtentStoreIoWorkerQueue {
            state: Mutex::new(ExtentStoreIoWorkerState::default()),
            changed: Condvar::new(),
            counters: counters.clone(),
            #[cfg(test)]
            fail_after_submit: AtomicBool::new(false),
        });
        let worker_queue = queue.clone();
        let scratch_arena = std::sync::Arc::new(ExtentStoreScratchArena::new(
            queue.clone(),
            counters.clone(),
            is_read,
        ));
        let thread_name = name.to_string();
        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run_extent_store_io_worker(ring, worker_queue, fixed_files, fixed_buffers, is_read)
            })
            .expect("extent store io worker should spawn");
        Self {
            queue,
            scratch_arena,
            counters,
            handle: Mutex::new(Some(handle)),
            is_read,
        }
    }

    fn scratch_arena(&self) -> std::sync::Arc<ExtentStoreScratchArena> {
        self.scratch_arena.clone()
    }

    fn submit_read(&self, op: ExtentStoreIoOp, len: usize) -> Result<usize> {
        self.submit(op, len)
    }

    fn submit(&self, op: ExtentStoreIoOp, len: usize) -> Result<usize> {
        self.submit_batch(vec![ExtentStoreIoSubmission { op, len }])
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                Err(StoreError::Transport(
                    "extent store io worker returned empty result".to_string(),
                ))
            })
    }

    fn unregister_segment_files(&self, segment_id: u64) -> Result<()> {
        let results = self.submit_batch(
            vec![
                ExtentStoreIoSubmission {
                    op: ExtentStoreIoOp::UnregisterFile {
                        key: ExtentStoreFixedFileKey::for_buffered(segment_id),
                    },
                    len: 0,
                },
                ExtentStoreIoSubmission {
                    op: ExtentStoreIoOp::UnregisterFile {
                        key: ExtentStoreFixedFileKey::for_direct(segment_id),
                    },
                    len: 0,
                },
            ],
        );
        for result in results {
            result?;
        }
        Ok(())
    }

    fn submit_batch(
        &self,
        entries: Vec<ExtentStoreIoSubmission>,
    ) -> Vec<Result<usize>> {
        if entries.is_empty() {
            return Vec::new();
        }
        let mut receivers = Vec::with_capacity(entries.len());
        {
            let mut state = self.queue.state.lock();
            if state.shutdown {
                return entries
                    .into_iter()
                    .map(|_| {
                        Err(StoreError::Transport(
                            "extent store io worker is shut down".to_string(),
                        ))
                    })
                    .collect();
            }
            let now = std::time::Instant::now();
            for submission in entries {
                let (tx, rx) = extent_store_channel();
                receivers.push(rx);
                let request = ExtentStoreIoRequest {
                    op: submission.op,
                    len: submission.len,
                    completion: tx,
                    enqueue_time: now,
                };
                if self.is_read {
                    if state.reads.is_empty() && state.first_read_wait.is_none() {
                        state.first_read_wait = Some(now);
                    }
                    state.reads.push_back(request);
                } else {
                    if state.writes.is_empty() && state.first_write_wait.is_none() {
                        state.first_write_wait = Some(now);
                    }
                    state.writes.push_back(request);
                }
            }
            self.counters
                .record_worker_queue_depths(state.reads.len(), state.writes.len());
            self.queue.changed.notify_one();
        }
        receivers
            .into_iter()
            .map(|rx| {
                rx.recv().map_err(|error| {
                    StoreError::Transport(format!(
                        "extent store io worker completion channel closed: {error}"
                    ))
                })?
            })
            .collect()
    }
}

impl ExtentStoreLocalReadLanes {
    fn from_env(counters: &ExtentStoreIoCounters) -> Self {
        Self {
            gds: ExtentStoreGdsLane::from_env(counters),
        }
    }

    fn choose_read_lane(&self, read: &ReservedExtentStoreRead<'_>) -> ExtentStoreLocalReadLane {
        match self.gds.eligible(read) {
            Ok(()) => ExtentStoreLocalReadLane::GdsCuFile,
            Err(_) => iouring_read_lane(read),
        }
    }
}

impl ExtentStoreGdsLane {
    fn enabled(&self) -> bool {
        self.config.enabled && Self::backend_available() && self.driver.is_some()
    }

    fn from_env(counters: &ExtentStoreIoCounters) -> Self {
        let config = ExtentStoreGdsConfig::from_env();
        if config.enabled && !Self::backend_available() {
            counters.record_gds_lane_degraded();
            warn!("extent store GDS requested but cuFile backend is unavailable; using io_uring fallback");
        }
        let driver = if config.enabled && Self::backend_available() {
            match ExtentStoreCuFileDriver::open() {
                Ok(driver) => Some(driver),
                Err(error) => {
                    counters.record_gds_lane_degraded();
                    warn!(
                        error = %error,
                        "extent store GDS cuFile driver unavailable; using io_uring fallback"
                    );
                    None
                }
            }
        } else {
            None
        };
        Self {
            config,
            backend: ExtentStoreGdsBackend,
            driver,
        }
    }

    fn backend_available() -> bool {
        cfg!(feature = "gds")
    }

    fn eligible(&self, read: &ReservedExtentStoreRead<'_>) -> std::result::Result<(), ExtentStoreGdsRejectReason> {
        let len = read.locator.value_len as usize;
        if !self.config.enabled || !Self::backend_available() || self.driver.is_none() {
            return Err(ExtentStoreGdsRejectReason::Disabled);
        }
        if len < self.config.min_read_size {
            return Err(ExtentStoreGdsRejectReason::Size);
        }
        if !self.config.allow_host_destination {
            return Err(ExtentStoreGdsRejectReason::HostDestination);
        }
        let offset = read.locator.offset + read.locator.value_offset;
        if read.direct_file.is_none()
            || !is_aligned_u64(offset)
            || !is_aligned_u64(read.locator.value_len)
            || !direct_iovec_compatible(&read.dst[..len])
        {
            return Err(ExtentStoreGdsRejectReason::Alignment);
        }
        Ok(())
    }

    fn try_read(&self, read: ExtentStoreGdsRead<'_>) -> Result<()> {
        self.backend.read(read)
    }
}

impl ExtentStoreGdsConfig {
    fn from_env() -> Self {
        Self {
            enabled: env_bool(EXTENT_STORE_GDS_ENABLED_ENV),
            min_read_size: env_usize(
                EXTENT_STORE_GDS_MIN_READ_SIZE_ENV,
                DEFAULT_EXTENT_STORE_GDS_MIN_READ_SIZE,
            ),
            allow_host_destination: env_bool(EXTENT_STORE_GDS_ALLOW_HOST_DST_ENV),
        }
    }
}

#[cfg(feature = "gds")]
impl ExtentStoreCuFileDriver {
    fn open() -> Result<Self> {
        let library = ExtentStoreDlopenLibrary::open("libcufile.so")?;
        let open: unsafe extern "C" fn() -> i32 = library.symbol("cuFileDriverOpen")?;
        let status = unsafe { open() };
        if status != 0 {
            return Err(StoreError::Unsupported(format!(
                "cuFileDriverOpen failed with status {status}"
            )));
        }
        Ok(Self)
    }
}

#[cfg(not(feature = "gds"))]
struct ExtentStoreCuFileDriver;

#[cfg(not(feature = "gds"))]
impl ExtentStoreCuFileDriver {
    fn open() -> Result<Self> {
        Err(StoreError::Unsupported(
            "extent store GDS cuFile backend is not compiled in".to_string(),
        ))
    }
}

#[cfg(feature = "gds")]
struct ExtentStoreDlopenLibrary {
    handle: *mut c_void,
}

#[cfg(feature = "gds")]
impl ExtentStoreDlopenLibrary {
    fn open(name: &str) -> Result<Self> {
        let name = CString::new(name).expect("library name should not contain NUL");
        let handle = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
        if handle.is_null() {
            return Err(StoreError::Unsupported(format!(
                "{} unavailable: {}",
                name.to_string_lossy(),
                dlerror_string()
            )));
        }
        Ok(Self { handle })
    }

    fn symbol<T>(&self, name: &str) -> Result<T> {
        let name = CString::new(name).expect("symbol name should not contain NUL");
        let raw_symbol = unsafe { libc::dlsym(self.handle, name.as_ptr()) };
        if raw_symbol.is_null() {
            return Err(StoreError::Unsupported(format!(
                "{} unavailable: {}",
                name.to_string_lossy(),
                dlerror_string()
            )));
        }
        Ok(unsafe { std::mem::transmute_copy(&raw_symbol) })
    }
}

#[cfg(feature = "gds")]
impl Drop for ExtentStoreDlopenLibrary {
    fn drop(&mut self) {
        unsafe {
            libc::dlclose(self.handle);
        }
    }
}

#[cfg(feature = "gds")]
fn dlerror_string() -> String {
    let error = unsafe { libc::dlerror() };
    if error.is_null() {
        return "unknown dlerror".to_string();
    }
    unsafe { std::ffi::CStr::from_ptr(error) }
        .to_string_lossy()
        .into_owned()
}

impl ExtentStoreGdsBackend {
    fn read(&self, read: ExtentStoreGdsRead<'_>) -> Result<()> {
        #[cfg(feature = "gds")]
        {
            let _ = self;
            let library = ExtentStoreDlopenLibrary::open("libcufile.so")?;
            let handle_register: unsafe extern "C" fn(
                *mut ExtentStoreCuFileHandle,
                *mut ExtentStoreCuFileDescr,
            ) -> ExtentStoreCuFileError = library.symbol("cuFileHandleRegister")?;
            let handle_deregister: unsafe extern "C" fn(ExtentStoreCuFileHandle) -> ExtentStoreCuFileError =
                library.symbol("cuFileHandleDeregister")?;
            let read_fn: unsafe extern "C" fn(
                ExtentStoreCuFileHandle,
                *mut c_void,
                usize,
                i64,
                i64,
            ) -> isize = library.symbol("cuFileRead")?;
            let mut handle: ExtentStoreCuFileHandle = ptr::null_mut();
            let mut descriptor = ExtentStoreCuFileDescr {
                handle_type: 1,
                fd: read.file.as_raw_fd(),
                fs_ops: ptr::null_mut(),
            };
            let register_error = unsafe { handle_register(&mut handle, &mut descriptor) };
            if register_error.err != 0 {
                return Err(StoreError::Unsupported(format!(
                    "cuFileHandleRegister failed for segment {} with err {} cu_err {}",
                    read.segment_id, register_error.err, register_error.cu_err
                )));
            }
            let read_len = unsafe {
                read_fn(
                    handle,
                    read.dst.as_mut_ptr().cast::<c_void>(),
                    read.dst.len(),
                    read.offset as i64,
                    0,
                )
            };
            let deregister_error = unsafe { handle_deregister(handle) };
            if deregister_error.err != 0 {
                return Err(StoreError::Transport(format!(
                    "cuFileHandleDeregister failed for segment {} with err {} cu_err {}",
                    read.segment_id, deregister_error.err, deregister_error.cu_err
                )));
            }
            if read_len < 0 {
                return Err(StoreError::Transport(format!(
                    "cuFileRead failed for segment {} with status {read_len}",
                    read.segment_id
                )));
            }
            if read_len as usize != read.dst.len() {
                return Err(StoreError::NotFound(format!(
                    "cuFileRead returned short read for segment {}: expected {} actual {}",
                    read.segment_id,
                    read.dst.len(),
                    read_len
                )));
            }
            Ok(())
        }
        #[cfg(not(feature = "gds"))]
        {
            let _ = self;
            let _ = read;
            Err(StoreError::Unsupported(
                "extent store GDS cuFile backend is not compiled in".to_string(),
            ))
        }
    }
}

fn iouring_read_lane(read: &ReservedExtentStoreRead<'_>) -> ExtentStoreLocalReadLane {
    let value_len = read.locator.value_len as usize;
    if direct_reads_enabled()
        && read.direct_file.as_ref().is_some_and(|_| {
            direct_payload_io_compatible(
                read.locator.offset + read.locator.value_offset,
                &read.dst[..value_len],
            )
        })
    {
        ExtentStoreLocalReadLane::IoUringDirect
    } else if read.direct_file.is_some() && direct_scratch_read_len(&read.locator).is_some() {
        ExtentStoreLocalReadLane::IoUringDirectScratch
    } else {
        ExtentStoreLocalReadLane::Buffered
    }
}

fn direct_scratch_read_len(locator: &ExtentStoreLocator) -> Option<usize> {
    let value_start = locator.offset.checked_add(locator.value_offset)?;
    if !is_aligned_u64(value_start) || locator.value_len > direct_scratch_max_read_len() {
        return None;
    }
    let aligned_len = align_up(locator.value_len, EXTENT_STORE_ALIGNMENT);
    let physical_available = locator.record_len.checked_sub(locator.value_offset)?;
    if aligned_len > physical_available {
        return None;
    }
    usize::try_from(aligned_len).ok()
}

fn direct_reads_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| env_bool_default(EXTENT_STORE_DIRECT_READS_ENV, true))
}

fn iouring_reads_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| env_bool_default(EXTENT_STORE_IOURING_READS_ENV, true))
}

fn direct_scratch_max_read_len() -> u64 {
    static CACHED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        if !direct_reads_enabled() {
            return 0;
        }
        env_u64(
            EXTENT_STORE_DIRECT_SCRATCH_MAX_READ_BYTES_ENV,
            EXTENT_STORE_DIRECT_SCRATCH_DEFAULT_MAX_READ_LEN,
        )
        .min(EXTENT_STORE_DIRECT_SCRATCH_MAX_RECORD_LEN)
    })
}

fn read_pipeline_max_inflight_ops() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        env_u64(
            EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_OPS_ENV,
            DEFAULT_EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_OPS as u64,
        )
        .clamp(1, 1024) as usize
    })
}

fn write_pipeline_max_inflight_ops() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        env_u64(
            EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_MAX_INFLIGHT_OPS_ENV,
            EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_MAX_INFLIGHT_OPS as u64,
        )
        .clamp(1, 1024) as usize
    })
}

fn tail_trace_threshold() -> Option<std::time::Duration> {
    static CACHED: std::sync::OnceLock<Option<std::time::Duration>> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var(EXTENT_STORE_TAIL_TRACE_MS_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map(std::time::Duration::from_millis)
    })
}

fn trace_if_slow(
    op: &'static str,
    stage: &'static str,
    elapsed: std::time::Duration,
    detail: impl FnOnce() -> String,
) {
    if tail_trace_threshold().is_some_and(|threshold| elapsed >= threshold) {
        println!(
            "extent_store_tail_trace,op={op},stage={stage},elapsed_ms={:.3},{}",
            elapsed.as_secs_f64() * 1000.0,
            detail(),
        );
    }
}

fn parallel_buffered_read_threads() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| env_u64(EXTENT_STORE_PARALLEL_BUFFERED_READS_ENV, 4).clamp(1, 32) as usize)
}

fn pinned_prefetch_mode() -> ExtentStorePinnedPrefetchMode {
    static CACHED: std::sync::OnceLock<ExtentStorePinnedPrefetchMode> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        match std::env::var(EXTENT_STORE_PINNED_PREFETCH_ENV).as_deref() {
            Ok("fadvise") | Ok("willneed") => ExtentStorePinnedPrefetchMode::Fadvise,
            Ok("readahead") => ExtentStorePinnedPrefetchMode::Readahead,
            Ok("madvise") => ExtentStorePinnedPrefetchMode::Madvise,
            Ok("touch") => ExtentStorePinnedPrefetchMode::Touch,
            _ => ExtentStorePinnedPrefetchMode::Disabled,
        }
    })
}

fn pinned_cold_ordinary_fallback_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| env_bool_default(EXTENT_STORE_PINNED_COLD_ORDINARY_FALLBACK_ENV, true))
}

fn segment_preallocate_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| env_bool_default(EXTENT_STORE_SEGMENT_PREALLOCATE_ENV, false))
}

fn env_bool(key: &str) -> bool {
    env_bool_default(key, false)
}

fn env_bool_default(key: &str, default: bool) -> bool {
    std::env::var(key)
        .map(|value| match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => true,
            "0" | "false" | "FALSE" | "no" | "NO" => false,
            _ => default,
        })
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

impl Drop for ExtentStoreIoWorker {
    fn drop(&mut self) {
        {
            let mut state = self.queue.state.lock();
            state.shutdown = true;
            self.queue.changed.notify_all();
        }
        if let Some(handle) = self.handle.lock().take() {
            let _ = handle.join();
        }
    }
}

fn run_extent_store_io_worker(
    mut ring: IoUring,
    queue: std::sync::Arc<ExtentStoreIoWorkerQueue>,
    mut fixed_files: Option<ExtentStoreFixedFileRegistry>,
    mut fixed_buffers: Option<ExtentStoreFixedBufferRegistry>,
    is_read: bool,
) {
    let direction_label = if is_read { "read" } else { "write" };
    loop {
        let (mut requests, byte_limited) = {
            let mut state = queue.state.lock();
            loop {
                if is_read {
                    if !state.reads.is_empty() {
                        let mut requests = Vec::with_capacity(state.reads.len());
                        while let Some(request) = state.reads.pop_front() {
                            requests.push(request);
                        }
                        state.first_read_wait = None;
                        queue
                            .counters
                            .record_worker_queue_depths(state.reads.len(), state.writes.len());
                        break (requests, false);
                    }
                } else {
                    if !state.writes.is_empty() {
                        let mut requests = Vec::with_capacity(state.writes.len());
                        let mut bytes = 0usize;
                        let mut byte_limited = false;
                        while let Some(request) = state.writes.pop_front() {
                            let next_bytes = bytes.saturating_add(request.len);
                            if !requests.is_empty()
                                && next_bytes > EXTENT_STORE_IO_WORKER_MAX_WRITE_BATCH_BYTES
                            {
                                state.writes.push_front(request);
                                byte_limited = true;
                                break;
                            }
                            bytes = next_bytes;
                            requests.push(request);
                        }
                        if state.writes.is_empty() {
                            state.first_write_wait = None;
                        }
                        queue
                            .counters
                            .record_worker_queue_depths(state.reads.len(), state.writes.len());
                        break (requests, byte_limited);
                    }
                }
                if state.shutdown {
                    break (Vec::new(), false);
                }
                queue.changed.wait(&mut state);
            }
        };
        if requests.is_empty() {
            if let Some(buffers) = fixed_buffers.take() {
                buffers.unregister(&ring);
            }
            if let Some(files) = fixed_files.take() {
                files.unregister(&ring);
            }
            return;
        }
        let batch_ops = requests.len();
        let batch_bytes = requests.iter().map(|request| request.len).sum::<usize>();
        let max_queue_wait = requests
            .iter()
            .map(|r| r.enqueue_time.elapsed())
            .max()
            .unwrap_or_default();
        let max_queue_wait_us = max_queue_wait.as_micros() as u64;
        crate::observability::registry::record_extent_store_queue_wait(
            direction_label,
            max_queue_wait,
        );
        if max_queue_wait_us > 500 {
            warn!(
                "io_worker_queue_wait: direction={direction_label}, batch_ops={batch_ops}, max_queue_wait_us={max_queue_wait_us}",
            );
        }
        queue
            .counters
            .record_worker_batch(is_read, batch_ops, batch_bytes);
        if byte_limited {
            queue
                .counters
                .record_worker_write_byte_limit(batch_ops, batch_bytes);
        }
        let pipeline_started = std::time::Instant::now();
        let outcome = if is_read {
            submit_extent_store_io_worker_read_pipeline(
                &mut ring,
                fixed_files.as_mut(),
                fixed_buffers.as_mut(),
                &queue,
                &mut requests,
            )
        } else {
            submit_extent_store_io_worker_write_pipeline(
                &mut ring,
                fixed_files.as_mut(),
                fixed_buffers.as_mut(),
                &queue,
                &mut requests,
            )
        };
        let pipeline_elapsed = pipeline_started.elapsed();
        crate::observability::registry::record_extent_store_pipeline_duration(
            direction_label,
            pipeline_elapsed,
        );
        let pipeline_ms = pipeline_elapsed.as_secs_f64() * 1000.0;
        if pipeline_ms > 5.0 {
            info!(
                "io_worker_pipeline_complete: direction={direction_label}, ops={batch_ops}, bytes={batch_bytes}, pipeline_ms={pipeline_ms:.3}",
            );
        }
        match outcome {
            ExtentStoreIoWorkerPipelineOutcome::Complete(results) => {
                for (request, result) in requests.drain(..).zip(results) {
                    let _ = request.completion.send(result);
                }
            }
            ExtentStoreIoWorkerPipelineOutcome::Fatal(results) => {
                let mut pending = {
                    let mut state = queue.state.lock();
                    state.shutdown = true;
                    state.first_read_wait = None;
                    state.first_write_wait = None;
                    let mut pending = state.reads.drain(..).collect::<Vec<_>>();
                    pending.extend(state.writes.drain(..));
                    queue.counters.record_worker_queue_depths(0, 0);
                    queue.changed.notify_all();
                    pending
                };
                if let Some(buffers) = fixed_buffers.take() {
                    buffers.unregister(&ring);
                }
                if let Some(files) = fixed_files.take() {
                    files.unregister(&ring);
                }
                // Closing the ring cancels and joins requests that may have
                // been accepted before submit/submit_and_wait failed. Keep
                // every caller-owned buffer alive until after this point.
                drop(ring);
                for (request, result) in requests.drain(..).zip(results) {
                    let _ = request.completion.send(result);
                }
                let stopped = StoreError::Transport(
                    "extent store io_uring worker stopped after a fatal ring error".to_string(),
                );
                for request in pending.drain(..) {
                    let _ = request.completion.send(Err(stopped.clone()));
                }
                return;
            }
            ExtentStoreIoWorkerPipelineOutcome::Preempted {
                results,
                next_unsubmitted,
            } => {
                // With split workers, preemption should not occur. Complete
                // any already-submitted results and requeue the rest.
                for (request, result) in requests.drain(..next_unsubmitted).zip(results) {
                    let _ = request.completion.send(result);
                }
                requeue_extent_store_io_worker_requests(&queue, is_read, requests);
            }
        }
    }
}

fn worker_queue_starved(
    first_wait: Option<std::time::Instant>,
    now: std::time::Instant,
    deadline: std::time::Duration,
) -> bool {
    first_wait.is_some_and(|started| now.duration_since(started) >= deadline)
}

fn extent_store_io_worker_should_preempt(
    queue: &ExtentStoreIoWorkerQueue,
    read_turn: bool,
) -> bool {
    let state = queue.state.lock();
    let now = std::time::Instant::now();
    if read_turn {
        !state.writes.is_empty()
            && worker_queue_starved(
                state.first_write_wait,
                now,
                EXTENT_STORE_IO_WORKER_WRITE_STARVATION,
            )
            && !worker_queue_starved(
                state.first_read_wait,
                now,
                EXTENT_STORE_IO_WORKER_READ_STARVATION,
            )
    } else {
        !state.reads.is_empty()
            && worker_queue_starved(
                state.first_read_wait,
                now,
                EXTENT_STORE_IO_WORKER_READ_STARVATION,
            )
    }
}

fn requeue_extent_store_io_worker_requests(
    queue: &ExtentStoreIoWorkerQueue,
    read_queue: bool,
    mut requests: Vec<ExtentStoreIoRequest>,
) {
    if requests.is_empty() {
        return;
    }
    let mut state = queue.state.lock();
    let now = std::time::Instant::now();
    if read_queue {
        if state.reads.is_empty() && state.first_read_wait.is_none() {
            state.first_read_wait = Some(now);
        }
        while let Some(request) = requests.pop() {
            state.reads.push_front(request);
        }
    } else {
        if state.writes.is_empty() && state.first_write_wait.is_none() {
            state.first_write_wait = Some(now);
        }
        while let Some(request) = requests.pop() {
            state.writes.push_front(request);
        }
    }
    queue
        .counters
        .record_worker_queue_depths(state.reads.len(), state.writes.len());
    queue.changed.notify_one();
}

fn submit_extent_store_io_worker_batch(
    ring: &mut IoUring,
    mut fixed_files: Option<&mut ExtentStoreFixedFileRegistry>,
    mut fixed_buffers: Option<&mut ExtentStoreFixedBufferRegistry>,
    counters: &ExtentStoreIoCounters,
    requests: &mut [ExtentStoreIoRequest],
) -> ExtentStoreIoWorkerBatchOutcome {
    let mut results = (0..requests.len())
        .map(|_| {
            Err(StoreError::Transport(
                "extent store io worker request not completed".to_string(),
            ))
        })
        .collect::<Vec<_>>();
    let mut submitted_indices = Vec::new();
    let mut submitted = vec![false; requests.len()];
    for (index, request) in requests.iter_mut().enumerate() {
        let op = std::mem::replace(
            &mut request.op,
            ExtentStoreIoOp::Nop,
        );
        if let ExtentStoreIoOp::UnregisterFile { key } = op {
            results[index] = fixed_files
                .as_deref_mut()
                .map_or(Ok(0), |files| files.unregister_key(ring, key));
            continue;
        }
        let Some(entry) = op.build_entry(
            ring,
            fixed_files.as_deref_mut(),
            fixed_buffers.as_deref_mut(),
            counters,
            index as u64,
        ) else {
            results[index] = Ok(0);
            continue;
        };
        let pushed = unsafe { ring.submission().push(&entry).is_ok() };
        if !pushed {
            results[index] = Err(StoreError::Transport(
                "extent store io_uring submission queue is full".to_string(),
            ));
            break;
        }
        submitted_indices.push(index);
        submitted[index] = true;
    }
    if submitted_indices.is_empty() {
        return ExtentStoreIoWorkerBatchOutcome {
            results,
            fatal: false,
        };
    }
    let submitted_count = submitted_indices.len();
    if let Err(error) = ring.submit_and_wait(submitted_count) {
        for index in submitted_indices {
            results[index] = Err(StoreError::Transport(format!(
                "extent store io_uring worker submit failed: {error}"
            )));
        }
        return ExtentStoreIoWorkerBatchOutcome {
            results,
            fatal: true,
        };
    }
    let mut completed = 0usize;
    let mut fatal = false;
    while completed < submitted_count {
        let completion = ring
            .completion()
            .next()
            .map(|completion| (completion.user_data(), completion.result()));
        let Some((user_data, result)) = completion else {
            if let Err(error) = ring.submit_and_wait(1) {
                for &index in &submitted_indices {
                    if submitted[index] {
                        results[index] = Err(StoreError::Transport(format!(
                            "extent store io_uring completion wait failed: {error}"
                        )));
                    }
                }
                fatal = true;
                break;
            }
            continue;
        };
        let index = user_data as usize;
        if index >= requests.len() || !submitted[index] {
            continue;
        }
        submitted[index] = false;
        completed += 1;
        if result < 0 {
            results[index] = Err(StoreError::Transport(format!(
                "extent store io_uring operation failed: {}",
                io::Error::from_raw_os_error(-result)
            )));
        } else {
            results[index] = Ok(result as usize);
        }
    }
    ExtentStoreIoWorkerBatchOutcome { results, fatal }
}

struct ExtentStoreIoWorkerBatchOutcome {
    results: Vec<Result<usize>>,
    fatal: bool,
}

fn submit_extent_store_io_worker_write_pipeline(
    ring: &mut IoUring,
    fixed_files: Option<&mut ExtentStoreFixedFileRegistry>,
    fixed_buffers: Option<&mut ExtentStoreFixedBufferRegistry>,
    queue: &ExtentStoreIoWorkerQueue,
    requests: &mut [ExtentStoreIoRequest],
) -> ExtentStoreIoWorkerPipelineOutcome {
    submit_extent_store_io_worker_pipeline(
        ring,
        fixed_files,
        fixed_buffers,
        requests,
        |op| matches!(op, ExtentStoreIoOp::Write { .. } | ExtentStoreIoOp::Writev { .. }),
        ExtentStoreIoWorkerPipelineContext {
            counters: &queue.counters,
            #[cfg(test)]
            fail_after_submit: &queue.fail_after_submit,
            preempt_queue: None,
            read_turn: false,
            config: ExtentStoreIoWorkerPipelineConfig {
                max_inflight_ops: write_pipeline_max_inflight_ops(),
                max_inflight_bytes: EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_MAX_INFLIGHT_BYTES,
                wait_min: EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_WAIT_MIN,
            },
        },
    )
}

fn submit_extent_store_io_worker_read_pipeline(
    ring: &mut IoUring,
    fixed_files: Option<&mut ExtentStoreFixedFileRegistry>,
    fixed_buffers: Option<&mut ExtentStoreFixedBufferRegistry>,
    queue: &ExtentStoreIoWorkerQueue,
    requests: &mut [ExtentStoreIoRequest],
) -> ExtentStoreIoWorkerPipelineOutcome {
    submit_extent_store_io_worker_pipeline(
        ring,
        fixed_files,
        fixed_buffers,
        requests,
        |op| matches!(op, ExtentStoreIoOp::Read { .. }),
        ExtentStoreIoWorkerPipelineContext {
            counters: &queue.counters,
            #[cfg(test)]
            fail_after_submit: &queue.fail_after_submit,
            preempt_queue: None,
            read_turn: true,
            config: ExtentStoreIoWorkerPipelineConfig {
                max_inflight_ops: read_pipeline_max_inflight_ops(),
                max_inflight_bytes: EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_BYTES,
                wait_min: EXTENT_STORE_IO_WORKER_READ_PIPELINE_WAIT_MIN,
            },
        },
    )
}

enum ExtentStoreIoWorkerPipelineOutcome {
    Complete(Vec<Result<usize>>),
    Fatal(Vec<Result<usize>>),
    Preempted {
        results: Vec<Result<usize>>,
        next_unsubmitted: usize,
    },
}

struct ExtentStoreIoWorkerPipelineConfig {
    max_inflight_ops: usize,
    max_inflight_bytes: usize,
    wait_min: usize,
}

struct ExtentStoreIoWorkerPipelineContext<'a> {
    counters: &'a ExtentStoreIoCounters,
    #[cfg(test)]
    fail_after_submit: &'a AtomicBool,
    /// When `Some`, pipeline will check the opposite queue for starvation and
    /// preempt if needed. Split workers pass `None` to disable preemption.
    preempt_queue: Option<&'a ExtentStoreIoWorkerQueue>,
    read_turn: bool,
    config: ExtentStoreIoWorkerPipelineConfig,
}

fn submit_extent_store_io_worker_pipeline(
    ring: &mut IoUring,
    mut fixed_files: Option<&mut ExtentStoreFixedFileRegistry>,
    mut fixed_buffers: Option<&mut ExtentStoreFixedBufferRegistry>,
    requests: &mut [ExtentStoreIoRequest],
    accepts: impl Fn(&ExtentStoreIoOp) -> bool,
    context: ExtentStoreIoWorkerPipelineContext<'_>,
) -> ExtentStoreIoWorkerPipelineOutcome {
    if requests.iter().any(|request| !accepts(&request.op)) {
        let outcome = submit_extent_store_io_worker_batch(
            ring,
            fixed_files,
            fixed_buffers,
            context.counters,
            requests,
        );
        return if outcome.fatal {
            ExtentStoreIoWorkerPipelineOutcome::Fatal(outcome.results)
        } else {
            ExtentStoreIoWorkerPipelineOutcome::Complete(outcome.results)
        };
    }

    let mut results = (0..requests.len())
        .map(|_| {
            Err(StoreError::Transport(
                "extent store io worker request not completed".to_string(),
            ))
        })
        .collect::<Vec<_>>();
    let mut submitted = vec![false; requests.len()];
    let mut next_index = 0usize;
    let mut completed_count = 0usize;
    let mut inflight_ops = 0usize;
    let mut inflight_bytes = 0usize;
    let mut last_submit_added = 0usize;
    let pipeline_start = std::time::Instant::now();
    let mut acc_build_sqe_us: u64 = 0;
    let mut acc_ring_submit_us: u64 = 0;
    let mut acc_ring_wait_us: u64 = 0;
    let mut acc_drain_cqe_us: u64 = 0;
    let mut fatal = false;

    while completed_count < requests.len() {
        let t_build = std::time::Instant::now();
        let mut built = 0usize;
        while next_index < requests.len()
            && inflight_ops < context.config.max_inflight_ops
            && (inflight_ops == 0
                || inflight_bytes.saturating_add(requests[next_index].len)
                    <= context.config.max_inflight_bytes)
        {
            let request_len = requests[next_index].len;
            let op = std::mem::replace(
                &mut requests[next_index].op,
                ExtentStoreIoOp::Nop,
            );
            let Some(entry) = op.build_entry(
                ring,
                fixed_files.as_deref_mut(),
                fixed_buffers.as_deref_mut(),
                context.counters,
                next_index as u64,
            ) else {
                results[next_index] = Ok(0);
                completed_count += 1;
                next_index += 1;
                continue;
            };
            let pushed = unsafe { ring.submission().push(&entry).is_ok() };
            if !pushed {
                results[next_index] = Err(StoreError::Transport(
                    "extent store io_uring submission queue is full".to_string(),
                ));
                completed_count += 1;
                next_index += 1;
                break;
            }
            submitted[next_index] = true;
            inflight_ops += 1;
            inflight_bytes = inflight_bytes.saturating_add(request_len);
            next_index += 1;
            built += 1;
        }
        acc_build_sqe_us += t_build.elapsed().as_micros() as u64;
        if built > 0 {
            let submit_started = std::time::Instant::now();
            if let Err(error) = ring.submit() {
                for index in 0..next_index {
                    if submitted[index] {
                        submitted[index] = false;
                        results[index] = Err(StoreError::Transport(format!(
                            "extent store io_uring worker submit failed: {error}"
                        )));
                    }
                }
                fatal = true;
                break;
            }
            #[cfg(test)]
            if context
                .fail_after_submit
                .swap(false, AtomicOrdering::SeqCst)
            {
                for index in 0..next_index {
                    if submitted[index] {
                        submitted[index] = false;
                        results[index] = Err(StoreError::Transport(
                            "injected extent store io_uring fatal submit error".to_string(),
                        ));
                    }
                }
                fatal = true;
                break;
            }
            let submit_elapsed = submit_started.elapsed();
            acc_ring_submit_us += submit_elapsed.as_micros() as u64;
            trace_if_slow(
                if context.read_turn { "read" } else { "write" },
                "io_worker_ring_submit",
                submit_elapsed,
                || {
                    format!(
                        "built={built},next_index={next_index},inflight_ops={inflight_ops},inflight_bytes={inflight_bytes}"
                    )
                },
            );
            last_submit_added = built;
        }

        let t_drain = std::time::Instant::now();
        let mut drained = 0usize;
        while let Some(completion) = ring
            .completion()
            .next()
            .map(|completion| (completion.user_data(), completion.result()))
        {
            if complete_extent_store_io_worker_request(
                &mut results,
                &mut submitted,
                requests,
                &mut inflight_ops,
                &mut inflight_bytes,
                &mut completed_count,
                completion,
            ) {
                drained += 1;
            }
        }
        acc_drain_cqe_us += t_drain.elapsed().as_micros() as u64;
        if drained > 0 {
            continue;
        }
        if inflight_ops == 0 {
            if next_index >= requests.len() {
                break;
            }
            if context.preempt_queue.is_some_and(|q| extent_store_io_worker_should_preempt(q, context.read_turn)) {
                // Record io_uring pipeline metrics (preempted)
                use crate::observability::registry::{record_io_uring_phase_duration, record_io_uring_ops};
                record_io_uring_phase_duration("pipeline", pipeline_start.elapsed());
                record_io_uring_phase_duration("build_sqe", Duration::from_micros(acc_build_sqe_us));
                record_io_uring_phase_duration("ring_submit", Duration::from_micros(acc_ring_submit_us));
                record_io_uring_phase_duration("ring_wait", Duration::from_micros(acc_ring_wait_us));
                record_io_uring_phase_duration("drain_cqe", Duration::from_micros(acc_drain_cqe_us));
                record_io_uring_ops("preempted", completed_count as u64);
                return ExtentStoreIoWorkerPipelineOutcome::Preempted {
                    results,
                    next_unsubmitted: next_index,
                };
            }
            continue;
        }

        let wait_min = if last_submit_added > 0
            && next_index >= requests.len()
            && last_submit_added == inflight_ops
        {
            inflight_ops
        } else {
            context.config.wait_min.min(inflight_ops)
        };
        last_submit_added = 0;
        let wait_started = std::time::Instant::now();
        if let Err(error) = ring.submit_and_wait(wait_min) {
            for index in 0..next_index {
                if submitted[index] {
                    submitted[index] = false;
                    results[index] = Err(StoreError::Transport(format!(
                        "extent store io_uring completion wait failed: {error}"
                    )));
                }
            }
            fatal = true;
            break;
        }
        let wait_elapsed = wait_started.elapsed();
        acc_ring_wait_us += wait_elapsed.as_micros() as u64;
        trace_if_slow(
            if context.read_turn { "read" } else { "write" },
            "io_worker_submit_and_wait",
            wait_elapsed,
            || {
                format!(
                    "wait_min={wait_min},next_index={next_index},inflight_ops={inflight_ops},inflight_bytes={inflight_bytes},completed_count={completed_count},requests={}",
                    requests.len(),
                )
            },
        );
    }

    // Record io_uring pipeline metrics (complete)
    use crate::observability::registry::{record_io_uring_phase_duration, record_io_uring_ops};
    record_io_uring_phase_duration("pipeline", pipeline_start.elapsed());
    record_io_uring_phase_duration("build_sqe", Duration::from_micros(acc_build_sqe_us));
    record_io_uring_phase_duration("ring_submit", Duration::from_micros(acc_ring_submit_us));
    record_io_uring_phase_duration("ring_wait", Duration::from_micros(acc_ring_wait_us));
    record_io_uring_phase_duration("drain_cqe", Duration::from_micros(acc_drain_cqe_us));
    record_io_uring_ops("complete", completed_count as u64);

    if fatal {
        ExtentStoreIoWorkerPipelineOutcome::Fatal(results)
    } else {
        ExtentStoreIoWorkerPipelineOutcome::Complete(results)
    }
}

fn complete_extent_store_io_worker_request(
    results: &mut [Result<usize>],
    submitted: &mut [bool],
    requests: &[ExtentStoreIoRequest],
    inflight_ops: &mut usize,
    inflight_bytes: &mut usize,
    completed_count: &mut usize,
    completion: (u64, i32),
) -> bool {
    let (user_data, result) = completion;
    let index = user_data as usize;
    if index >= requests.len() || !submitted[index] {
        return false;
    }
    submitted[index] = false;
    *inflight_ops = inflight_ops.saturating_sub(1);
    *inflight_bytes = inflight_bytes.saturating_sub(requests[index].len);
    *completed_count += 1;
    if result < 0 {
        results[index] = Err(StoreError::Transport(format!(
            "extent store io_uring operation failed: {}",
            io::Error::from_raw_os_error(-result)
        )));
    } else {
        results[index] = Ok(result as usize);
    }
    true
}

fn io_uring_unavailable(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::ENOSYS) | Some(libc::EPERM) | Some(libc::EACCES)
    )
}

fn submit_write_batch_io_worker(
    worker: &ExtentStoreIoWorker,
    writes: &mut [(usize, ReservedExtentStoreWrite<'_>)],
) -> Vec<Result<()>> {
    let mut results = (0..writes.len()).map(|_| Ok(())).collect::<Vec<_>>();
    let mut written = vec![0usize; writes.len()];
    let mut buffered_fallback = vec![false; writes.len()];
    let scratch_arena = worker.scratch_arena();
    let mut inflight_buffers = (0..writes.len()).map(|_| None).collect::<Vec<_>>();
    let mut remaining = writes.len();
    while remaining > 0 {
        let mut batch_indices = Vec::new();
        let mut entries = Vec::new();
        for index in 0..writes.len() {
            if results[index].is_err()
                || written[index] >= writes[index].1.record.record_len as usize
            {
                continue;
            }
            let write = &writes[index].1;
            let offset = write.locator.offset.saturating_add(written[index] as u64);
            let lane = if buffered_fallback[index] {
                ExtentStoreWriteLane::Buffered
            } else {
                write_lane(write)
            };
            let op = match lane {
                ExtentStoreWriteLane::DirectScratch => {
                    if inflight_buffers[index].is_none() {
                        let record_len = write.record.record_len as usize;
                        match scratch_arena.lease(record_len) {
                            Ok(mut buffer) => {
                                if let Err(error) =
                                    materialize_direct_record_into(&write.record, &mut buffer)
                                {
                                    results[index] = Err(error);
                                    remaining = remaining.saturating_sub(1);
                                    continue;
                                }
                                if written[index] == 0 {
                                    worker
                                        .counters
                                        .direct_scratch_write_ops
                                        .fetch_add(1, AtomicOrdering::Relaxed);
                                }
                                inflight_buffers[index] = Some(ExtentStoreWriteBuffers::Direct(
                                    ExtentStoreScratchRecord { buffer, record_len },
                                ));
                            }
                            Err(_) => {
                                buffered_fallback[index] = true;
                                worker
                                    .counters
                                    .buffered_write_ops
                                    .fetch_add(1, AtomicOrdering::Relaxed);
                                inflight_buffers[index] = Some(ExtentStoreWriteBuffers::Iovecs(
                                    write.record.iovecs_from(written[index]),
                                ));
                            }
                        }
                    }
                    if buffered_fallback[index] {
                        let iovecs = match inflight_buffers[index].as_ref() {
                            Some(ExtentStoreWriteBuffers::Iovecs(iovecs)) => iovecs,
                            _ => unreachable!("buffered fallback requires stable iovec storage"),
                        };
                        ExtentStoreIoOp::Writev {
                            fd: write.file.as_raw_fd(),
                            iovecs: iovecs.as_ptr(),
                            len: iovecs.len() as u32,
                            offset,
                            fixed_file_key: Some(ExtentStoreFixedFileKey::for_buffered(
                                write.locator.segment_id,
                            )),
                        }
                    } else {
                        let record = match inflight_buffers[index].as_mut() {
                            Some(ExtentStoreWriteBuffers::Direct(record)) => record,
                            _ => unreachable!("direct scratch record should be materialized before submission"),
                        };
                        let file = write
                            .direct_file
                            .as_ref()
                            .expect("direct scratch lane requires direct file");
                        ExtentStoreIoOp::Write {
                            fd: file.as_raw_fd(),
                            buf: record.buffer[written[index]..].as_ptr(),
                            len: (record.record_len - written[index]) as u32,
                            offset,
                            fixed_file_key: Some(ExtentStoreFixedFileKey::for_direct(
                                write.locator.segment_id,
                            )),
                            fixed_buffer: record.buffer.fixed_slot,
                        }
                    }
                }
                ExtentStoreWriteLane::Direct => {
                    if written[index] == 0 {
                        worker
                            .counters
                            .direct_write_ops
                            .fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    inflight_buffers[index] = Some(ExtentStoreWriteBuffers::Iovecs(
                        write.record.iovecs_from(written[index]),
                    ));
                    let iovecs = match inflight_buffers[index].as_ref() {
                        Some(ExtentStoreWriteBuffers::Iovecs(iovecs)) => iovecs,
                        _ => unreachable!("submitted write should have stable iovec storage"),
                    };
                    let file = write
                        .direct_file
                        .as_ref()
                        .expect("direct lane requires direct file");
                    ExtentStoreIoOp::Writev {
                        fd: file.as_raw_fd(),
                        iovecs: iovecs.as_ptr(),
                        len: iovecs.len() as u32,
                        offset,
                        fixed_file_key: Some(ExtentStoreFixedFileKey::for_direct(
                            write.locator.segment_id,
                        )),
                    }
                }
                ExtentStoreWriteLane::Buffered => {
                    if written[index] == 0 && !buffered_fallback[index] {
                        worker
                            .counters
                            .buffered_write_ops
                            .fetch_add(1, AtomicOrdering::Relaxed);
                    }
                    inflight_buffers[index] = Some(ExtentStoreWriteBuffers::Iovecs(
                        write.record.iovecs_from(written[index]),
                    ));
                    let iovecs = match inflight_buffers[index].as_ref() {
                        Some(ExtentStoreWriteBuffers::Iovecs(iovecs)) => iovecs,
                        _ => unreachable!("submitted write should have stable iovec storage"),
                    };
                    ExtentStoreIoOp::Writev {
                        fd: write.file.as_raw_fd(),
                        iovecs: iovecs.as_ptr(),
                        len: iovecs.len() as u32,
                        offset,
                        fixed_file_key: Some(ExtentStoreFixedFileKey::for_buffered(
                            write.locator.segment_id,
                        )),
                    }
                }
            };
            let len = write.record.record_len as usize - written[index];
            batch_indices.push(index);
            entries.push(ExtentStoreIoSubmission { op, len });
        }
        if entries.is_empty() {
            for result in results.iter_mut().filter(|result| result.is_ok()) {
                *result = Err(StoreError::Transport(
                    "extent store io worker write batch made no progress".to_string(),
                ));
            }
            break;
        }
        let submit_started = std::time::Instant::now();
        let entry_count = entries.len();
        let entry_bytes = entries.iter().map(|entry| entry.len).sum::<usize>();
        let completions = worker.submit_batch(entries);
        let submit_elapsed = submit_started.elapsed();
        trace_if_slow("write", "submit_write_batch_io_worker", submit_elapsed, || {
            format!("entries={entry_count},bytes={entry_bytes},remaining={remaining}")
        });
        for (index, completion) in batch_indices.into_iter().zip(completions) {
            inflight_buffers[index] = None;
            if results[index].is_err() {
                continue;
            }
            let result = match completion {
                Ok(result) => result,
                Err(error) => {
                    results[index] = Err(error);
                    remaining = remaining.saturating_sub(1);
                    continue;
                }
            };
                            if result == 0 {
                results[index] = Err(StoreError::Transport(
                    "extent store write completed with zero bytes".to_string(),
                ));
                remaining = remaining.saturating_sub(1);
                continue;
            }
            let record_len = writes[index].1.record.record_len as usize;
            let remaining_len = record_len.saturating_sub(written[index]);
            if result > remaining_len {
                results[index] = Err(StoreError::Transport(format!(
                    "extent store write completed beyond request: remaining {remaining_len} actual {result}"
                )));
                remaining = remaining.saturating_sub(1);
                continue;
            }
            if !buffered_fallback[index]
                && !matches!(write_lane(&writes[index].1), ExtentStoreWriteLane::Buffered)
                && result < remaining_len
                && !result.is_multiple_of(EXTENT_STORE_ALIGNMENT as usize)
            {
                results[index] = Err(StoreError::Transport(format!(
                    "extent store direct write completed at unaligned boundary: {result} bytes"
                )));
                remaining = remaining.saturating_sub(1);
                continue;
            }
            written[index] = written[index].saturating_add(result);
            if written[index] >= record_len {
                remaining = remaining.saturating_sub(1);
            }
        }
    }
    results
}

fn submit_read_batch_io_worker(
    worker: &ExtentStoreReadWorker,
    reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
    completed: &[bool],
) -> Vec<Result<()>> {
    let scratch_arena = worker.scratch_arena();
    let mut results = (0..reads.len()).map(|_| Ok(())).collect::<Vec<_>>();
    let mut read_offsets = vec![0usize; reads.len()];
    let mut direct_scratch_buffers = (0..reads.len()).map(|_| None).collect::<Vec<_>>();
    let mut remaining = reads
        .iter()
        .enumerate()
        .filter(|(index, _)| !completed[*index])
        .count();
    while remaining > 0 {
        let mut batch_entries = Vec::new();
        let mut entries = Vec::new();
        let plan = ExtentStoreBatchReadPlan {
            completed,
            results: &results,
            read_offsets: &read_offsets,
        };
        if generic_span_reads_enabled() {
            plan_generic_span_reads(
                worker,
                &scratch_arena,
                reads,
                &plan,
                &mut batch_entries,
                &mut entries,
            );
        }
        plan_coalesced_direct_scratch_reads(
            worker,
            &scratch_arena,
            reads,
            &plan,
            &mut batch_entries,
            &mut entries,
        );
        let mut staged_bytes = entries.iter().map(|entry| entry.len).sum::<usize>();
        for index in 0..reads.len() {
            if completed.get(index).copied().unwrap_or(false)
                || results[index].is_err()
                || read_offsets[index] >= reads[index].1.locator.value_len as usize
                || batch_entries.iter().any(|entry| read_batch_entry_contains(entry, index))
            {
                continue;
            }
            let read = &mut reads[index].1;
            let offset = read
                .locator
                .offset
                .saturating_add(read.locator.value_offset)
                .saturating_add(read_offsets[index] as u64);
            let value_len = read.locator.value_len as usize;
            let payload_offset = read_offsets[index];
            let payload_start =
                read.locator.offset + read.locator.value_offset + payload_offset as u64;
            let use_direct = direct_reads_enabled()
                && read.direct_file.as_ref().is_some_and(|_| {
                    direct_payload_io_compatible(payload_start, &read.dst[payload_offset..value_len])
                });
            let mut direct_scratch_len = (!use_direct && read.direct_file.is_some())
                .then(|| direct_scratch_read_len(&read.locator))
                .flatten();
            if let Some(aligned_len) = direct_scratch_len {
                if staged_bytes != 0
                    && staged_bytes.saturating_add(aligned_len)
                        > EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_BYTES
                {
                    continue;
                }
            }
            let op = if use_direct {
                let file = read
                    .direct_file
                    .as_ref()
                    .expect("direct file checked above");
                let payload = &mut read.dst[payload_offset..value_len];
                ExtentStoreIoOp::Read {
                    fd: file.as_raw_fd(),
                    buf: payload.as_mut_ptr(),
                    len: payload.len() as u32,
                    offset,
                    fixed_file_key: Some(ExtentStoreFixedFileKey::for_direct(
                        read.locator.segment_id,
                    )),
                    fixed_buffer: None,
                }
            } else if direct_scratch_len.is_some() {
                let aligned_len = direct_scratch_len
                    .expect("direct scratch length checked before allocating buffer");
                if direct_scratch_buffers[index].is_none() {
                    match scratch_arena.lease(aligned_len) {
                        Ok(buffer) => {
                            direct_scratch_buffers[index] = Some(buffer);
                            staged_bytes = staged_bytes.saturating_add(aligned_len);
                        }
                        Err(_) => {
                            direct_scratch_len = None;
                        }
                    }
                }
                if let Some(aligned_len) = direct_scratch_len {
                    let file = read
                        .direct_file
                        .as_ref()
                        .expect("direct scratch file checked above");
                    let scratch = direct_scratch_buffers[index]
                        .as_mut()
                        .expect("direct scratch read buffer should be allocated before submission");
                    ExtentStoreIoOp::Read {
                        fd: file.as_raw_fd(),
                        buf: scratch[payload_offset..aligned_len].as_mut_ptr(),
                        len: (aligned_len - payload_offset) as u32,
                        offset,
                        fixed_file_key: Some(ExtentStoreFixedFileKey::for_direct(
                            read.locator.segment_id,
                        )),
                        fixed_buffer: scratch.fixed_slot,
                    }
                } else {
                    let payload = &mut read.dst[payload_offset..value_len];
                    ExtentStoreIoOp::Read {
                        fd: read.file.as_raw_fd(),
                        buf: payload.as_mut_ptr(),
                        len: payload.len() as u32,
                        offset,
                        fixed_file_key: Some(ExtentStoreFixedFileKey::for_buffered(
                            read.locator.segment_id,
                        )),
                        fixed_buffer: None,
                    }
                }
            } else {
                let payload = &mut read.dst[payload_offset..value_len];
                ExtentStoreIoOp::Read {
                    fd: read.file.as_raw_fd(),
                    buf: payload.as_mut_ptr(),
                    len: payload.len() as u32,
                    offset,
                    fixed_file_key: Some(ExtentStoreFixedFileKey::for_buffered(
                        read.locator.segment_id,
                    )),
                    fixed_buffer: None,
                }
            };
            let len = if let Some(aligned_len) = direct_scratch_len {
                aligned_len - payload_offset
            } else {
                value_len - payload_offset
            };
            if use_direct {
                worker.counters().record_direct_read(len);
            } else if direct_scratch_len.is_some() {
                worker.counters().record_direct_scratch_read(len);
            } else {
                worker.counters().record_buffered_read(len);
            }
            batch_entries.push(ExtentStoreReadBatchEntry::Single { index });
            entries.push(ExtentStoreIoSubmission { op, len });
        }
        if entries.is_empty() {
            for result in results.iter_mut().filter(|result| result.is_ok()) {
                *result = Err(StoreError::Transport(
                    "extent store io worker read batch made no progress".to_string(),
                ));
            }
            break;
        }
        let submit_started = std::time::Instant::now();
        let entry_count = entries.len();
        let entry_bytes = entries.iter().map(|entry| entry.len).sum::<usize>();
        let completions = worker.submit_batch(entries);
        let submit_elapsed = submit_started.elapsed();
        trace_if_slow("read", "submit_read_batch_io_worker", submit_elapsed, || {
            format!(
                "entries={entry_count},bytes={entry_bytes},remaining={remaining},direct_scratch_max_read_bytes={}",
                direct_scratch_max_read_len(),
            )
        });
        for (entry, completion) in batch_entries.into_iter().zip(completions) {
            match entry {
                ExtentStoreReadBatchEntry::Single { index } => {
                    apply_single_read_completion(
                        reads,
                        &mut results,
                        &mut read_offsets,
                        &mut direct_scratch_buffers,
                        &mut remaining,
                        index,
                        completion,
                    );
                }
                ExtentStoreReadBatchEntry::Coalesced { group } => {
                    apply_coalesced_read_completion(
                        reads,
                        &mut results,
                        &mut read_offsets,
                        &mut remaining,
                        group,
                        completion,
                    );
                }
            }
        }
    }
    results
}

fn packed_block_entry_count(
    reads: &[(usize, ReservedExtentStoreRead<'_>)],
    indices: &[usize],
) -> Option<usize> {
    let locator = &reads[*indices.first()?].1.locator;
    let index_bytes = locator.value_offset.checked_sub(EXTENT_STORE_HEADER_LEN as u64)?;
    Some((index_bytes / EXTENT_STORE_PACKED_ENTRY_INDEX_LEN) as usize)
}

fn read_batch_entry_contains(entry: &ExtentStoreReadBatchEntry, index: usize) -> bool {
    match entry {
        ExtentStoreReadBatchEntry::Single { index: entry_index } => *entry_index == index,
        ExtentStoreReadBatchEntry::Coalesced { group } => group.indices.contains(&index),
    }
}

fn generic_span_reads_enabled() -> bool {
    static CACHED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        std::env::var_os(EXTENT_STORE_GENERIC_SPAN_ENABLED_ENV).is_some_and(|value| value != "0")
    })
}

type PackedBlockReadGroup = ((u64, u64, u64), Vec<usize>);

fn packed_span_max_read_bytes(
    reads: &[(usize, ReservedExtentStoreRead<'_>)],
    packed_blocks: &[PackedBlockReadGroup],
) -> u64 {
    let configured = env_u64(
        EXTENT_STORE_PACKED_SPAN_MAX_READ_BYTES_ENV,
        EXTENT_STORE_PACKED_SPAN_TARGET_READ_BYTES,
    )
    .min(EXTENT_STORE_PACKED_SPAN_MAX_READ_BYTES);
    let max_record_len = packed_blocks
        .iter()
        .map(|((_, _, record_len), _)| *record_len)
        .max()
        .unwrap_or(0);
    let max_value_len = packed_blocks
        .iter()
        .flat_map(|(_, indices)| indices.iter())
        .map(|index| reads[*index].1.locator.value_len)
        .max()
        .unwrap_or(0);
    configured.max(max_record_len).max(max_value_len)
}

struct ExtentStoreBatchReadPlan<'a> {
    completed: &'a [bool],
    results: &'a [Result<()>],
    read_offsets: &'a [usize],
}

struct SpanCoalesceConfig {
    max_gap: u64,
    max_read_bytes: u64,
    max_amplification_num: u64,
    min_group_reads: usize,
}

/// Shared coalescing engine: sorts candidates by (segment_id, start), greedily merges
/// adjacent reads into spans respecting gap/size/amplification limits, and submits each
/// coalesced group as a single scratch-backed io_uring read.
///
/// Each candidate is (segment_id, span_start, span_end, read_index, requested_value_bytes).
fn coalesce_and_submit_span_reads(
    config: &SpanCoalesceConfig,
    candidates: &mut [(u64, u64, u64, usize, u64)],
    worker: &ExtentStoreReadWorker,
    scratch_arena: &std::sync::Arc<ExtentStoreScratchArena>,
    reads: &[(usize, ReservedExtentStoreRead<'_>)],
    batch_entries: &mut Vec<ExtentStoreReadBatchEntry>,
    entries: &mut Vec<ExtentStoreIoSubmission>,
) {
    if candidates.len() < config.min_group_reads {
        return;
    }
    candidates.sort_by_key(|(segment_id, start, _, _, _)| (*segment_id, *start));
    let mut staged_bytes = entries.iter().map(|entry| entry.len).sum::<usize>();
    let mut cursor = 0usize;
    while cursor < candidates.len() {
        let (segment_id, span_start, first_end, first_index, first_requested) = candidates[cursor];
        let mut indices = vec![first_index];
        let mut span_end = first_end;
        let mut requested_bytes = first_requested;
        cursor += 1;
        while cursor < candidates.len() {
            let (next_segment_id, next_start, next_end, next_index, next_value_len) =
                candidates[cursor];
            if next_segment_id != segment_id {
                break;
            }
            let gap = next_start.saturating_sub(span_end);
            let next_span_end = span_end.max(next_end);
            let next_span_len = next_span_end - span_start;
            let next_requested = requested_bytes.saturating_add(next_value_len);
            if gap > config.max_gap
                || next_span_len > config.max_read_bytes
                || next_span_len > next_requested.saturating_mul(config.max_amplification_num)
            {
                break;
            }
            indices.push(next_index);
            span_end = next_span_end;
            requested_bytes = next_requested;
            cursor += 1;
        }
        if indices.len() < config.min_group_reads {
            continue;
        }
        let span_len = (span_end - span_start) as usize;
        if staged_bytes != 0
            && staged_bytes.saturating_add(span_len)
                > EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_BYTES
        {
            continue;
        }
        let mut scratch = match scratch_arena.lease(span_len) {
            Ok(scratch) => scratch,
            Err(_) => continue,
        };
        staged_bytes = staged_bytes.saturating_add(span_len);
        let fixed_slot = scratch.fixed_slot;
        let fd = reads[indices[0]]
            .1
            .direct_file
            .as_ref()
            .expect("coalesced span read requires direct file")
            .as_raw_fd();
        let op = ExtentStoreIoOp::Read {
            fd,
            buf: scratch.as_mut_ptr(),
            len: span_len as u32,
            offset: span_start,
            fixed_file_key: Some(ExtentStoreFixedFileKey::for_direct(segment_id)),
            fixed_buffer: fixed_slot,
        };
        worker
            .counters()
            .record_coalesced_read_group(indices.len(), requested_bytes as usize, span_len);
        batch_entries.push(ExtentStoreReadBatchEntry::Coalesced {
            group: ExtentStoreCoalescedReadGroup {
                span_start,
                span_len,
                indices,
                scratch,
            },
        });
        entries.push(ExtentStoreIoSubmission { op, len: span_len });
    }
}

fn plan_generic_span_reads(
    worker: &ExtentStoreReadWorker,
    scratch_arena: &std::sync::Arc<ExtentStoreScratchArena>,
    reads: &[(usize, ReservedExtentStoreRead<'_>)],
    plan: &ExtentStoreBatchReadPlan<'_>,
    batch_entries: &mut Vec<ExtentStoreReadBatchEntry>,
    entries: &mut Vec<ExtentStoreIoSubmission>,
) {
    let mut candidates = reads
        .iter()
        .enumerate()
        .filter_map(|(index, (_, read))| {
            if plan.completed.get(index).copied().unwrap_or(false)
                || plan.results[index].is_err()
                || plan.read_offsets[index] != 0
                || read.locator.value_len == 0
                || read.locator.value_len > EXTENT_STORE_GENERIC_SPAN_MAX_VALUE_LEN
                || !is_aligned_u64(read.locator.offset + read.locator.value_offset)
                || !is_aligned_u64(read.locator.value_len)
                || read.direct_file.is_none()
            {
                return None;
            }
            let payload_start = read.locator.offset + read.locator.value_offset;
            Some((
                read.locator.segment_id,
                payload_start,
                payload_start + read.locator.value_len,
                index,
                read.locator.value_len,
            ))
        })
        .collect::<Vec<_>>();
    coalesce_and_submit_span_reads(
        &SpanCoalesceConfig {
            max_gap: EXTENT_STORE_GENERIC_SPAN_MAX_GAP,
            max_read_bytes: EXTENT_STORE_GENERIC_SPAN_MAX_READ_BYTES,
            max_amplification_num: EXTENT_STORE_GENERIC_SPAN_MAX_READ_AMPLIFICATION_NUM,
            min_group_reads: EXTENT_STORE_GENERIC_SPAN_MIN_READS,
        },
        &mut candidates,
        worker,
        scratch_arena,
        reads,
        batch_entries,
        entries,
    );
}

fn plan_coalesced_direct_scratch_reads(
    worker: &ExtentStoreReadWorker,
    scratch_arena: &std::sync::Arc<ExtentStoreScratchArena>,
    reads: &[(usize, ReservedExtentStoreRead<'_>)],
    plan: &ExtentStoreBatchReadPlan<'_>,
    batch_entries: &mut Vec<ExtentStoreReadBatchEntry>,
    entries: &mut Vec<ExtentStoreIoSubmission>,
) {
    let mut candidates = reads
        .iter()
        .enumerate()
        .filter_map(|(index, (_, read))| {
            if plan.completed.get(index).copied().unwrap_or(false)
                || plan.results[index].is_err()
                || plan.read_offsets[index] != 0
                || read.locator.value_len == 0
            {
                return None;
            }
            read.direct_file.as_ref()?;
            let payload_start = read.locator.offset + read.locator.value_offset;
            let direct_compatible = direct_reads_enabled()
                && direct_payload_io_compatible(
                    payload_start,
                    &read.dst[..read.locator.value_len as usize],
                );
            let aligned_start = is_aligned_u64(payload_start);
            let direct_scratch_sized = read.locator.value_len <= direct_scratch_max_read_len();
            let dense_payload = read.locator.value_len <= EXTENT_STORE_DENSE_COALESCE_MAX_VALUE_LEN;
            if (!aligned_start && !dense_payload)
                || (aligned_start && !direct_scratch_sized && !direct_compatible)
                || (direct_compatible
                    && read.locator.value_len > EXTENT_STORE_COALESCE_DIRECT_COMPATIBLE_MAX_VALUE_LEN)
            {
                return None;
            }
            let span_start = if aligned_start {
                payload_start
            } else {
                align_down(payload_start, EXTENT_STORE_ALIGNMENT)
            };
            let span_end = align_up(
                payload_start + read.locator.value_len,
                EXTENT_STORE_ALIGNMENT,
            );
            let record_end = read.locator.offset.checked_add(read.locator.record_len)?;
            if span_end > record_end {
                return None;
            }
            Some((
                read.locator.segment_id,
                span_start,
                span_end,
                index,
                read.locator.value_len,
            ))
        })
        .collect::<Vec<_>>();
    coalesce_and_submit_span_reads(
        &SpanCoalesceConfig {
            max_gap: EXTENT_STORE_COALESCE_MAX_GAP,
            max_read_bytes: EXTENT_STORE_COALESCE_MAX_READ_BYTES,
            max_amplification_num: EXTENT_STORE_COALESCE_MAX_READ_AMPLIFICATION_NUM,
            min_group_reads: EXTENT_STORE_COALESCE_MIN_READS,
        },
        &mut candidates,
        worker,
        scratch_arena,
        reads,
        batch_entries,
        entries,
    );
}

fn apply_single_read_completion(
    reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
    results: &mut [Result<()>],
    read_offsets: &mut [usize],
    direct_scratch_buffers: &mut [Option<ExtentStoreScratchLease>],
    remaining: &mut usize,
    index: usize,
    completion: Result<usize>,
) {
    if results[index].is_err() {
        return;
    }
    let result = match completion {
        Ok(result) => result,
        Err(error) => {
            direct_scratch_buffers[index].take();
            results[index] = Err(error);
            *remaining = remaining.saturating_sub(1);
            return;
        }
    };
    if result == 0 {
        direct_scratch_buffers[index].take();
        results[index] = Err(StoreError::NotFound(
            "extent store read reached EOF".to_string(),
        ));
        *remaining = remaining.saturating_sub(1);
        return;
    }
    if let Some(scratch) = direct_scratch_buffers[index].take() {
        let value_len = reads[index].1.locator.value_len as usize;
        let aligned_len = align_up(value_len as u64, EXTENT_STORE_ALIGNMENT) as usize;
        if result != aligned_len {
            results[index] = Err(StoreError::NotFound(format!(
                "extent store direct scratch read completed short: expected {aligned_len} actual {result}"
            )));
            *remaining = remaining.saturating_sub(1);
            return;
        }
        reads[index].1.dst[..value_len].copy_from_slice(&scratch[..value_len]);
        read_offsets[index] = value_len;
        *remaining = remaining.saturating_sub(1);
        return;
    }
    read_offsets[index] = read_offsets[index].saturating_add(result);
    if read_offsets[index] >= reads[index].1.locator.value_len as usize {
        *remaining = remaining.saturating_sub(1);
    }
}

fn apply_coalesced_read_completion(
    reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
    results: &mut [Result<()>],
    read_offsets: &mut [usize],
    remaining: &mut usize,
    group: ExtentStoreCoalescedReadGroup,
    completion: Result<usize>,
) {
    let result = match completion {
        Ok(result) => result,
        Err(error) => {
            for index in group.indices {
                if results[index].is_ok() {
                    results[index] = Err(error.clone());
                    *remaining = remaining.saturating_sub(1);
                }
            }
            return;
        }
    };
    if result == 0 {
        for index in group.indices {
            if results[index].is_ok() {
                results[index] = Err(StoreError::NotFound(
                    "extent store read reached EOF".to_string(),
                ));
                *remaining = remaining.saturating_sub(1);
            }
        }
        return;
    }
    if result < group.span_len {
        for index in group.indices {
            if results[index].is_ok() {
                results[index] = Err(StoreError::Transport(format!(
                    "extent store coalesced read completed short: expected {} actual {result}",
                    group.span_len
                )));
                *remaining = remaining.saturating_sub(1);
            }
        }
        return;
    }
    for index in group.indices {
        if results[index].is_err() {
            continue;
        }
        let payload_start = reads[index].1.locator.offset + reads[index].1.locator.value_offset;
        let value_len = reads[index].1.locator.value_len as usize;
        let group_offset = (payload_start - group.span_start) as usize;
        let payload_end = group_offset + value_len;
        if payload_end > result || payload_end > group.span_len {
            results[index] = Err(StoreError::NotFound(
                "extent store coalesced read reached EOF".to_string(),
            ));
            *remaining = remaining.saturating_sub(1);
            continue;
        }
        reads[index].1.dst[..value_len].copy_from_slice(&group.scratch[group_offset..payload_end]);
        read_offsets[index] = value_len;
        *remaining = remaining.saturating_sub(1);
    }
}

// ---------------------------------------------------------------------------
// Scratch arena
// ---------------------------------------------------------------------------

impl ExtentStoreScratchArena {
    fn new(
        queue: std::sync::Arc<ExtentStoreIoWorkerQueue>,
        counters: std::sync::Arc<ExtentStoreIoCounters>,
        is_read: bool,
    ) -> Self {
        Self {
            queue,
            state: Mutex::new(ExtentStoreScratchArenaState {
                buffers: Vec::new(),
                retired_buffers: Vec::new(),
                allocated_bytes: 0,
                leased_bytes: 0,
            }),
            counters,
            is_read,
            max_bytes: if is_read {
                EXTENT_STORE_IO_WORKER_READ_PIPELINE_MAX_INFLIGHT_BYTES
            } else {
                EXTENT_STORE_IO_WORKER_WRITE_PIPELINE_MAX_INFLIGHT_BYTES
            },
        }
    }

    fn lease(self: &std::sync::Arc<Self>, len: usize) -> Result<ExtentStoreScratchLease> {
        if len > self.max_bytes {
            return Err(StoreError::Backpressure(format!(
                "extent store scratch request {len} exceeds arena budget {}",
                self.max_bytes
            )));
        }
        let max_reuse_len = if len == 0 { 0 } else { len.saturating_mul(2) };
        let pooled = loop {
            let mut state = self.state.lock();
            let reusable = state
                .buffers
                .iter()
                .enumerate()
                .filter(|(_, pooled)| {
                    pooled.buffer.len() >= len && pooled.buffer.len() <= max_reuse_len
                })
                .min_by_key(|(_, pooled)| pooled.buffer.len())
                .map(|(index, _)| index);
            if let Some(index) = reusable {
                let buffer = state.buffers.swap_remove(index);
                let pool_bytes = state
                    .buffers
                    .iter()
                    .map(|pooled| pooled.buffer.len())
                    .sum::<usize>();
                self.counters
                    .record_scratch_pool(state.buffers.len(), pool_bytes);
                state.leased_bytes = state.leased_bytes.saturating_add(buffer.buffer.len());
                break Some(buffer);
            }
            if state.allocated_bytes.saturating_add(len) <= self.max_bytes {
                state.allocated_bytes = state.allocated_bytes.saturating_add(len);
                state.leased_bytes = state.leased_bytes.saturating_add(len);
                let pool_bytes = state
                    .buffers
                    .iter()
                    .map(|pooled| pooled.buffer.len())
                    .sum::<usize>();
                self.counters
                    .record_scratch_pool(state.buffers.len(), pool_bytes);
                break None;
            }
            let Some(index) = state
                .buffers
                .iter()
                .enumerate()
                .max_by_key(|(_, pooled)| pooled.buffer.len())
                .map(|(index, _)| index)
            else {
                return Err(StoreError::Backpressure(format!(
                    "extent store scratch arena budget {} is fully leased",
                    self.max_bytes
                )));
            };
            let evicted = state.buffers.swap_remove(index);
            let pool_bytes = state
                .buffers
                .iter()
                .map(|pooled| pooled.buffer.len())
                .sum::<usize>();
            self.counters
                .record_scratch_pool(state.buffers.len(), pool_bytes);
            drop(state);
            self.retire_buffer(evicted);
        };
        let (buffer, fixed_slot) = match pooled {
            Some(mut pooled) => {
                self.counters.record_scratch_reuse(len);
                (pooled.buffer, pooled.fixed_slot.take())
            }
            None => {
                self.counters.record_scratch_alloc(len);
                // Every caller either materializes the complete submitted
                // write range or lets DMA overwrite the complete read range.
                // Avoid an otherwise redundant full-buffer memset here.
                let buffer = match AlignedExtentStoreBuffer::uninitialized(len) {
                    Ok(buffer) => buffer,
                    Err(error) => {
                        self.release_allocation(len);
                        return Err(error);
                    }
                };
                let fixed_slot = self.register_fixed_buffer(&buffer);
                (buffer, fixed_slot)
            }
        };
        debug_assert!(buffer.len() >= len);
        Ok(ExtentStoreScratchLease {
            buffer: Some(buffer),
            fixed_slot,
            arena: self.clone(),
        })
    }

    fn register_fixed_buffer(&self, buffer: &AlignedExtentStoreBuffer) -> Option<u16> {
        if buffer.len() > EXTENT_STORE_IO_WORKER_FIXED_BUFFER_MAX_BYTES {
            return None;
        }
        let (tx, rx) = extent_store_channel();
        {
            let mut state = self.queue.state.lock();
            if state.shutdown {
                return None;
            }
            let request = ExtentStoreIoRequest {
                op: ExtentStoreIoOp::RegisterBuffer {
                    buffer: buffer.ptr,
                    len: buffer.len,
                    completion: tx,
                },
                len: 0,
                completion: extent_store_channel().0,
                enqueue_time: std::time::Instant::now(),
            };
            if self.is_read {
                state.reads.push_back(request);
            } else {
                state.writes.push_back(request);
            }
            self.queue.changed.notify_one();
        }
        rx.recv().ok().flatten()
    }

    fn release_fixed_buffer(&self, slot: u16, buffer: AlignedExtentStoreBuffer) -> bool {
        let (release_tx, release_rx) = extent_store_channel();
        let mut state = self.queue.state.lock();
        if state.shutdown {
            drop(state);
            self.state.lock().retired_buffers.push(buffer);
            return false;
        }
        let request = ExtentStoreIoRequest {
            op: ExtentStoreIoOp::ReleaseBuffer {
                slot,
                buffer,
                completion: release_tx,
            },
            len: 0,
            completion: extent_store_channel().0,
            enqueue_time: std::time::Instant::now(),
        };
        if self.is_read {
            state.reads.push_back(request);
        } else {
            state.writes.push_back(request);
        }
        self.queue.changed.notify_one();
        drop(state);
        release_rx.recv().unwrap_or(false)
    }

    fn retire_buffer(&self, buffer: ExtentStoreScratchPooledBuffer) {
        let buffer_len = buffer.buffer.len();
        let released = if let Some(slot) = buffer.fixed_slot {
            self.release_fixed_buffer(slot, buffer.buffer)
        } else {
            drop(buffer.buffer);
            true
        };
        if released {
            let mut state = self.state.lock();
            state.allocated_bytes = state.allocated_bytes.saturating_sub(buffer_len);
        }
    }

    fn release(&self, buffer: AlignedExtentStoreBuffer, fixed_slot: Option<u16>) {
        let mut state = self.state.lock();
        state.leased_bytes = state.leased_bytes.saturating_sub(buffer.len());
        let pool_bytes = state
            .buffers
            .iter()
            .map(|pooled| pooled.buffer.len())
            .sum::<usize>();
        if state.buffers.len() < EXTENT_STORE_IO_WORKER_FIXED_BUFFER_SLOTS as usize
            && pool_bytes.saturating_add(buffer.len()) <= self.max_bytes
        {
            state
                .buffers
                .push(ExtentStoreScratchPooledBuffer { buffer, fixed_slot });
        } else {
            drop(state);
            self.retire_buffer(ExtentStoreScratchPooledBuffer { buffer, fixed_slot });
            return;
        }
        let pool_bytes = state
            .buffers
            .iter()
            .map(|pooled| pooled.buffer.len())
            .sum::<usize>();
        self.counters
            .record_scratch_pool(state.buffers.len(), pool_bytes);
        drop(state);
    }

    fn release_allocation(&self, len: usize) {
        let mut state = self.state.lock();
        state.allocated_bytes = state.allocated_bytes.saturating_sub(len);
        state.leased_bytes = state.leased_bytes.saturating_sub(len);
        drop(state);
    }
}

// ---------------------------------------------------------------------------
// IO counter record methods
// ---------------------------------------------------------------------------

#[cfg_attr(not(test), allow(dead_code))]
impl ExtentStoreIoCounters {
    fn record_segment_rollover(&self, elapsed: std::time::Duration) {
        self.segment_rollover_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
        let elapsed_us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.segment_rollover_us
            .fetch_add(elapsed_us, AtomicOrdering::Relaxed);
        self.segment_rollover_max_us
            .fetch_max(elapsed_us, AtomicOrdering::Relaxed);
    }

    fn record_buffered_write_blocking_batch(&self, ops: usize, bytes: usize) {
        if ops != 0 {
            self.buffered_write_blocking_batches
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.buffered_write_blocking_bytes
                .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
        }
    }

    fn record_read_lane(&self, lane: ExtentStoreLocalReadLane, bytes: usize) {
        match lane {
            ExtentStoreLocalReadLane::GdsCuFile => {}
            ExtentStoreLocalReadLane::IoUringDirect => self.record_direct_read(bytes),
            ExtentStoreLocalReadLane::IoUringDirectScratch => self.record_direct_scratch_read(bytes),
            ExtentStoreLocalReadLane::Buffered => self.record_buffered_read(bytes),
        }
    }

    fn record_direct_read(&self, bytes: usize) {
        self.direct_read_ops.fetch_add(1, AtomicOrdering::Relaxed);
        self.direct_read_bytes
            .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
    }

    fn record_direct_scratch_read(&self, bytes: usize) {
        self.direct_scratch_read_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.direct_scratch_read_bytes
            .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
    }

    fn record_buffered_read(&self, bytes: usize) {
        self.buffered_read_ops.fetch_add(1, AtomicOrdering::Relaxed);
        self.buffered_read_bytes
            .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
    }

    fn record_scratch_alloc(&self, len: usize) {
        if len != 0 {
            self.scratch_alloc_ops.fetch_add(1, AtomicOrdering::Relaxed);
            self.scratch_alloc_bytes
                .fetch_add(len as u64, AtomicOrdering::Relaxed);
        }
    }

    fn record_scratch_reuse(&self, len: usize) {
        if len != 0 {
            self.scratch_reuse_ops.fetch_add(1, AtomicOrdering::Relaxed);
            self.scratch_reuse_bytes
                .fetch_add(len as u64, AtomicOrdering::Relaxed);
        }
    }

    fn record_scratch_pool(&self, buffers: usize, bytes: usize) {
        self.scratch_pool_buffers
            .store(buffers as u64, AtomicOrdering::Relaxed);
        self.scratch_pool_bytes
            .store(bytes as u64, AtomicOrdering::Relaxed);
    }

    fn record_worker_queue_depths(&self, reads: usize, writes: usize) {
        self.worker_read_queue_depth
            .store(reads as u64, AtomicOrdering::Relaxed);
        self.worker_write_queue_depth
            .store(writes as u64, AtomicOrdering::Relaxed);
    }

    fn record_worker_batch(&self, read: bool, ops: usize, bytes: usize) {
        if read {
            self.worker_read_batches
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.worker_read_batch_ops
                .fetch_add(ops as u64, AtomicOrdering::Relaxed);
            self.worker_read_batch_bytes
                .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
        } else {
            self.worker_write_batches
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.worker_write_batch_ops
                .fetch_add(ops as u64, AtomicOrdering::Relaxed);
            self.worker_write_batch_bytes
                .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
        }
    }

    fn record_worker_write_byte_limit(&self, ops: usize, bytes: usize) {
        self.worker_write_byte_limited_turns
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.worker_write_byte_limited_ops
            .fetch_add(ops as u64, AtomicOrdering::Relaxed);
        self.worker_write_byte_limited_bytes
            .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
    }

    fn record_fixed_file_table_init(&self, success: bool) {
        if success {
            self.fixed_file_table_init_success
                .fetch_add(1, AtomicOrdering::Relaxed);
        } else {
            self.fixed_file_table_init_failure
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    fn record_fixed_file_sparse_table_init(&self, success: bool, error: Option<&io::Error>) {
        if success {
            self.fixed_file_sparse_table_init_success
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.fixed_file_last_errno.store(0, AtomicOrdering::Relaxed);
        } else {
            self.fixed_file_sparse_table_init_failure
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.record_fixed_file_errno(error);
        }
    }

    fn record_fixed_file_array_table_init(&self, success: bool, error: Option<&io::Error>) {
        if success {
            self.fixed_file_array_table_init_success
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.fixed_file_last_errno.store(0, AtomicOrdering::Relaxed);
        } else {
            self.fixed_file_array_table_init_failure
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.record_fixed_file_errno(error);
        }
    }

    fn record_fixed_file_errno(&self, error: Option<&io::Error>) {
        self.fixed_file_last_errno
            .store(error_errno(error), AtomicOrdering::Relaxed);
    }

    fn record_fixed_file_update_errno(&self, error: &io::Error) {
        self.fixed_file_update_last_errno
            .store(error_errno(Some(error)), AtomicOrdering::Relaxed);
    }

    fn record_fixed_file_register(&self, success: bool) {
        if success {
            self.fixed_file_register_success
                .fetch_add(1, AtomicOrdering::Relaxed);
        } else {
            self.fixed_file_register_failure
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    fn record_fixed_file_slot_exhaustion(&self) {
        self.fixed_file_slot_exhaustions
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_fixed_file_hit(&self) {
        self.fixed_file_hit_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_fixed_buffer_table_init(&self, success: bool) {
        if success {
            self.fixed_buffer_table_init_success
                .fetch_add(1, AtomicOrdering::Relaxed);
        } else {
            self.fixed_buffer_table_init_failure
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    fn record_fixed_buffer_sparse_table_init(&self, success: bool, error: Option<&io::Error>) {
        if success {
            self.fixed_buffer_sparse_table_init_success
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.fixed_buffer_last_errno
                .store(0, AtomicOrdering::Relaxed);
        } else {
            self.fixed_buffer_sparse_table_init_failure
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.record_fixed_buffer_errno(error);
        }
    }

    fn record_fixed_buffer_array_table_init(&self, success: bool, error: Option<&io::Error>) {
        if success {
            self.fixed_buffer_array_table_init_success
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.fixed_buffer_last_errno
                .store(0, AtomicOrdering::Relaxed);
        } else {
            self.fixed_buffer_array_table_init_failure
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.record_fixed_buffer_errno(error);
        }
    }

    fn record_fixed_buffer_errno(&self, error: Option<&io::Error>) {
        self.fixed_buffer_last_errno
            .store(error_errno(error), AtomicOrdering::Relaxed);
    }

    fn record_fixed_buffer_update_errno(&self, error: &io::Error) {
        self.fixed_buffer_update_last_errno
            .store(error_errno(Some(error)), AtomicOrdering::Relaxed);
    }

    fn record_fixed_buffer_register(&self, success: bool) {
        if success {
            self.fixed_buffer_register_success
                .fetch_add(1, AtomicOrdering::Relaxed);
        } else {
            self.fixed_buffer_register_failure
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    fn record_fixed_buffer_slot_exhaustion(&self) {
        self.fixed_buffer_slot_exhaustions
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_read_fixed(&self) {
        self.read_fixed_ops.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_write_fixed(&self) {
        self.write_fixed_ops.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_raw_fd_fallback(&self) {
        self.raw_fd_fallback_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_coalesced_read_group(&self, logical_reads: usize, requested_bytes: usize, physical_bytes: usize) {
        self.coalesced_read_groups
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.coalesced_logical_reads
            .fetch_add(logical_reads as u64, AtomicOrdering::Relaxed);
        self.coalesced_requested_bytes
            .fetch_add(requested_bytes as u64, AtomicOrdering::Relaxed);
        self.coalesced_physical_bytes
            .fetch_add(physical_bytes as u64, AtomicOrdering::Relaxed);
        self.coalesced_gap_bytes.fetch_add(
            physical_bytes.saturating_sub(requested_bytes) as u64,
            AtomicOrdering::Relaxed,
        );
    }

    fn record_gds_read(&self, len: usize) {
        self.gds_read_ops.fetch_add(1, AtomicOrdering::Relaxed);
        self.gds_read_bytes
            .fetch_add(len as u64, AtomicOrdering::Relaxed);
    }

    fn record_gds_submit_error(&self) {
        self.gds_submit_error_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_gds_reject(&self, reason: ExtentStoreGdsRejectReason) {
        match reason {
            ExtentStoreGdsRejectReason::Disabled => &self.gds_disabled_reject_ops,
            ExtentStoreGdsRejectReason::Size => &self.gds_size_reject_ops,
            ExtentStoreGdsRejectReason::Alignment => &self.gds_alignment_reject_ops,
            ExtentStoreGdsRejectReason::HostDestination => &self.gds_host_destination_reject_ops,
        }
        .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_gds_fallback_to_iouring(&self) {
        self.gds_fallback_to_iouring_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_gds_lane_degraded(&self) {
        self.gds_lane_degraded_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_pinned_read(&self, bytes: usize) {
        self.pinned_read_ops.fetch_add(1, AtomicOrdering::Relaxed);
        self.pinned_read_bytes
            .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
    }

    fn record_pinned_fallback(&self, reason: ExtentStorePinnedFallbackReason) {
        match reason {
            ExtentStorePinnedFallbackReason::Disabled => &self.pinned_fallback_disabled_ops,
            ExtentStorePinnedFallbackReason::BelowMinValue => &self.pinned_fallback_below_min_value_ops,
            ExtentStorePinnedFallbackReason::QuotaExceeded => &self.pinned_fallback_quota_exceeded_ops,
            ExtentStorePinnedFallbackReason::MappingTooShort => &self.pinned_fallback_mapping_too_short_ops,
            ExtentStorePinnedFallbackReason::MmapFailed => &self.pinned_fallback_mmap_failed_ops,
            ExtentStorePinnedFallbackReason::UnsupportedLocator => &self.pinned_fallback_unsupported_locator_ops,
            ExtentStorePinnedFallbackReason::ColdOrdinary => &self.pinned_fallback_cold_ordinary_ops,
        }
        .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_mmap_cache_hit(&self) {
        self.mmap_cache_hit_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_mmap_cache_miss(&self) {
        self.mmap_cache_miss_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_mmap_cache_state(&self, segments: usize, bytes: u64) {
        self.mmap_active_segments
            .store(segments as u64, AtomicOrdering::Relaxed);
        self.mmap_cache_bytes.store(bytes, AtomicOrdering::Relaxed);
    }

    fn record_mmap_create(&self, success: bool) {
        if success {
            self.mmap_create_ops.fetch_add(1, AtomicOrdering::Relaxed);
        } else {
            self.mmap_create_failures
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
    }

    fn record_pinned_payload_create(&self, bytes: usize) {
        self.pinned_payload_active_refs
            .fetch_add(1, AtomicOrdering::Relaxed);
        self.pinned_payload_active_bytes
            .fetch_add(bytes as u64, AtomicOrdering::Relaxed);
        self.pinned_payload_ref_created_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn record_pinned_payload_drop(&self, bytes: usize) {
        let prev_refs = self.pinned_payload_active_refs
            .fetch_sub(1, AtomicOrdering::Relaxed);
        let prev_bytes = self.pinned_payload_active_bytes
            .fetch_sub(bytes as u64, AtomicOrdering::Relaxed);
        debug_assert!(prev_refs > 0, "pinned_payload_active_refs underflow");
        debug_assert!(prev_bytes >= bytes as u64, "pinned_payload_active_bytes underflow");
        self.pinned_payload_ref_dropped_ops
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn pinned_payload_active_refs(&self) -> u64 {
        self.pinned_payload_active_refs
            .load(AtomicOrdering::Relaxed)
    }

    fn pinned_payload_active_bytes(&self) -> u64 {
        self.pinned_payload_active_bytes
            .load(AtomicOrdering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Multi-thread io_uring read pool
// ---------------------------------------------------------------------------

const EXTENT_STORE_READ_IO_WORKER_THREADS_ENV: &str = "MC_STORE_RS_EXTENT_READ_IO_WORKER_THREADS";
const DEFAULT_EXTENT_STORE_READ_IO_WORKER_THREADS: u64 = 1;
/// Maximum number of requests a single pool thread dequeues at once.  This
/// limits how much work one thread grabs before letting siblings pick up the
/// rest.
const EXTENT_STORE_POOL_MAX_BATCH_PER_THREAD: usize = 32;

fn read_io_worker_thread_count() -> usize {
    static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CACHED.get_or_init(|| {
        env_u64(
            EXTENT_STORE_READ_IO_WORKER_THREADS_ENV,
            DEFAULT_EXTENT_STORE_READ_IO_WORKER_THREADS,
        )
        .clamp(1, 32) as usize
    })
}

struct ExtentStoreIoWorkerPool {
    queue: std::sync::Arc<ExtentStoreIoWorkerQueue>,
    scratch_arena: std::sync::Arc<ExtentStoreScratchArena>,
    counters: std::sync::Arc<ExtentStoreIoCounters>,
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl ExtentStoreIoWorkerPool {
    fn new(
        ring_depth: u32,
        num_threads: usize,
        counters: std::sync::Arc<ExtentStoreIoCounters>,
        name_prefix: &str,
    ) -> Self {
        let queue = std::sync::Arc::new(ExtentStoreIoWorkerQueue {
            state: Mutex::new(ExtentStoreIoWorkerState::default()),
            changed: Condvar::new(),
            counters: counters.clone(),
            #[cfg(test)]
            fail_after_submit: AtomicBool::new(false),
        });
        let scratch_arena = std::sync::Arc::new(ExtentStoreScratchArena::new(
            queue.clone(),
            counters.clone(),
            true, // is_read
        ));
        let mut handles = Vec::with_capacity(num_threads);
        for thread_index in 0..num_threads {
            let worker_queue = queue.clone();
            let worker_counters = counters.clone();
            let thread_name = format!("{name_prefix}-{thread_index}");
            let handle = thread::Builder::new()
                .name(thread_name)
                .spawn(move || {
                    run_pool_read_worker(ring_depth, worker_queue, worker_counters, thread_index)
                })
                .expect("extent store io pool worker should spawn");
            handles.push(handle);
        }
        info!(
            "extent store io pool: spawned {num_threads} read worker threads (ring_depth={ring_depth})"
        );
        Self {
            queue,
            scratch_arena,
            counters,
            handles: Mutex::new(handles),
        }
    }

    fn scratch_arena(&self) -> std::sync::Arc<ExtentStoreScratchArena> {
        self.scratch_arena.clone()
    }

    fn submit_read(&self, op: ExtentStoreIoOp, len: usize) -> Result<usize> {
        self.submit_batch(vec![ExtentStoreIoSubmission { op, len }])
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                Err(StoreError::Transport(
                    "extent store io pool returned empty result".to_string(),
                ))
            })
    }

    fn unregister_segment_files(&self, segment_id: u64) -> Result<()> {
        let results = self.submit_batch(vec![
            ExtentStoreIoSubmission {
                op: ExtentStoreIoOp::UnregisterFile {
                    key: ExtentStoreFixedFileKey::for_buffered(segment_id),
                },
                len: 0,
            },
            ExtentStoreIoSubmission {
                op: ExtentStoreIoOp::UnregisterFile {
                    key: ExtentStoreFixedFileKey::for_direct(segment_id),
                },
                len: 0,
            },
        ]);
        for result in results {
            result?;
        }
        Ok(())
    }

    fn submit_batch(&self, entries: Vec<ExtentStoreIoSubmission>) -> Vec<Result<usize>> {
        if entries.is_empty() {
            return Vec::new();
        }
        let mut receivers = Vec::with_capacity(entries.len());
        {
            let mut state = self.queue.state.lock();
            if state.shutdown {
                return entries
                    .into_iter()
                    .map(|_| {
                        Err(StoreError::Transport(
                            "extent store io pool is shut down".to_string(),
                        ))
                    })
                    .collect();
            }
            let now = std::time::Instant::now();
            for submission in entries {
                let (tx, rx) = extent_store_channel();
                receivers.push(rx);
                let request = ExtentStoreIoRequest {
                    op: submission.op,
                    len: submission.len,
                    completion: tx,
                    enqueue_time: now,
                };
                if state.reads.is_empty() && state.first_read_wait.is_none() {
                    state.first_read_wait = Some(now);
                }
                state.reads.push_back(request);
            }
            self.queue.counters.record_worker_queue_depths(
                state.reads.len(),
                state.writes.len(),
            );
            // Wake all idle pool threads so they can share the work.
            self.queue.changed.notify_all();
        }
        receivers
            .into_iter()
            .map(|rx| {
                rx.recv().map_err(|error| {
                    StoreError::Transport(format!(
                        "extent store io pool completion channel closed: {error}"
                    ))
                })?
            })
            .collect()
    }
}

impl Drop for ExtentStoreIoWorkerPool {
    fn drop(&mut self) {
        {
            let mut state = self.queue.state.lock();
            state.shutdown = true;
            self.queue.changed.notify_all();
        }
        for handle in self.handles.lock().drain(..) {
            let _ = handle.join();
        }
    }
}

fn run_pool_read_worker(
    ring_depth: u32,
    queue: std::sync::Arc<ExtentStoreIoWorkerQueue>,
    _counters: std::sync::Arc<ExtentStoreIoCounters>,
    thread_index: usize,
) {
    let mut ring = match IoUring::new(ring_depth) {
        Ok(ring) => ring,
        Err(error) => {
            warn!(
                thread_index,
                error = %error,
                "extent store io pool worker failed to create io_uring; thread exiting"
            );
            return;
        }
    };
    // Phase 1: skip fixed file/buffer registration to keep things simple.
    // Each pool thread uses raw fd paths.  This costs ~5% vs fixed, but avoids
    // the complexity of broadcasting register/unregister to all threads.
    let mut fixed_files: Option<ExtentStoreFixedFileRegistry> = None;
    let mut fixed_buffers: Option<ExtentStoreFixedBufferRegistry> = None;

    loop {
        let requests = {
            let mut state = queue.state.lock();
            loop {
                if !state.reads.is_empty() {
                    // Take a bounded batch — leave the rest for sibling threads.
                    let count = state.reads.len().min(EXTENT_STORE_POOL_MAX_BATCH_PER_THREAD);
                    let requests: Vec<_> = state.reads.drain(..count).collect();
                    if state.reads.is_empty() {
                        state.first_read_wait = None;
                    }
                    // If more requests remain, wake another sibling.
                    if !state.reads.is_empty() {
                        queue.changed.notify_one();
                    }
                    queue
                        .counters
                        .record_worker_queue_depths(state.reads.len(), state.writes.len());
                    break requests;
                }
                if state.shutdown {
                    break Vec::new();
                }
                queue.changed.wait(&mut state);
            }
        };
        if requests.is_empty() {
            // Shutdown.
            return;
        }
        let batch_ops = requests.len();
        let batch_bytes = requests.iter().map(|r| r.len).sum::<usize>();
        let max_queue_wait = requests
            .iter()
            .map(|r| r.enqueue_time.elapsed())
            .max()
            .unwrap_or_default();
        let max_queue_wait_us = max_queue_wait.as_micros() as u64;
        crate::observability::registry::record_extent_store_queue_wait("read-pool", max_queue_wait);
        if max_queue_wait_us > 500 {
            warn!(
                "io_pool_queue_wait: thread={thread_index}, batch_ops={batch_ops}, max_queue_wait_us={max_queue_wait_us}",
            );
        }
        queue
            .counters
            .record_worker_batch(true, batch_ops, batch_bytes);

        let pipeline_started = std::time::Instant::now();
        let mut requests = requests;
        let outcome = submit_extent_store_io_worker_read_pipeline(
            &mut ring,
            fixed_files.as_mut(),
            fixed_buffers.as_mut(),
            &queue,
            &mut requests,
        );
        let pipeline_elapsed = pipeline_started.elapsed();
        crate::observability::registry::record_extent_store_pipeline_duration(
            "read-pool",
            pipeline_elapsed,
        );
        let pipeline_ms = pipeline_elapsed.as_secs_f64() * 1000.0;
        if pipeline_ms > 5.0 {
            info!(
                "io_pool_pipeline_complete: thread={thread_index}, ops={batch_ops}, bytes={batch_bytes}, pipeline_ms={pipeline_ms:.3}",
            );
        }
        match outcome {
            ExtentStoreIoWorkerPipelineOutcome::Complete(results) => {
                for (request, result) in requests.drain(..).zip(results) {
                    let _ = request.completion.send(result);
                }
            }
            ExtentStoreIoWorkerPipelineOutcome::Fatal(results) => {
                let mut pending = {
                    let mut state = queue.state.lock();
                    state.shutdown = true;
                    state.first_read_wait = None;
                    state.first_write_wait = None;
                    let mut pending = state.reads.drain(..).collect::<Vec<_>>();
                    pending.extend(state.writes.drain(..));
                    queue.counters.record_worker_queue_depths(0, 0);
                    queue.changed.notify_all();
                    pending
                };
                if let Some(buffers) = fixed_buffers.take() {
                    buffers.unregister(&ring);
                }
                if let Some(files) = fixed_files.take() {
                    files.unregister(&ring);
                }
                drop(ring);
                for (request, result) in requests.drain(..).zip(results) {
                    let _ = request.completion.send(result);
                }
                let stopped = StoreError::Transport(
                    "extent store io_uring read pool stopped after a fatal ring error".to_string(),
                );
                for request in pending.drain(..) {
                    let _ = request.completion.send(Err(stopped.clone()));
                }
                return;
            }
            ExtentStoreIoWorkerPipelineOutcome::Preempted {
                results,
                next_unsubmitted,
            } => {
                for (request, result) in requests.drain(..next_unsubmitted).zip(results) {
                    let _ = request.completion.send(result);
                }
                // Re-queue unsubmitted requests for sibling threads.
                requeue_extent_store_io_worker_requests(&queue, true, requests);
            }
        }
    }
}

/// Unified read worker abstraction: either a single-thread worker or a
/// multi-thread pool.  Exposes the same API so call sites don't branch.
enum ExtentStoreReadWorker {
    Single(ExtentStoreIoWorker),
    Pool(ExtentStoreIoWorkerPool),
}

impl ExtentStoreReadWorker {
    fn scratch_arena(&self) -> std::sync::Arc<ExtentStoreScratchArena> {
        match self {
            Self::Single(w) => w.scratch_arena(),
            Self::Pool(p) => p.scratch_arena(),
        }
    }

    fn submit_read(&self, op: ExtentStoreIoOp, len: usize) -> Result<usize> {
        match self {
            Self::Single(w) => w.submit_read(op, len),
            Self::Pool(p) => p.submit_read(op, len),
        }
    }

    fn submit_batch(&self, entries: Vec<ExtentStoreIoSubmission>) -> Vec<Result<usize>> {
        match self {
            Self::Single(w) => w.submit_batch(entries),
            Self::Pool(p) => p.submit_batch(entries),
        }
    }

    fn unregister_segment_files(&self, segment_id: u64) -> Result<()> {
        match self {
            Self::Single(w) => w.unregister_segment_files(segment_id),
            Self::Pool(p) => p.unregister_segment_files(segment_id),
        }
    }

    fn counters(&self) -> &std::sync::Arc<ExtentStoreIoCounters> {
        match self {
            Self::Single(w) => &w.counters,
            Self::Pool(p) => &p.counters,
        }
    }
}
