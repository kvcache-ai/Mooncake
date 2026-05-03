use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use mooncake_store_client::{
    GetRequest, MooncakeCompatibilityFacade, ObjectRef, PutFromRequest, PutRequest,
    ReplicationPolicy, StoreClient,
};
use mooncake_store_core::{Result as StoreResult, StoreError};
use tracing::{debug, info, warn};

use crate::cli::{BenchArgs, BenchMode, GlobalArgs, ReadInterface, WriteInterface};
use crate::datagen::{make_key, make_seed, payload};
use crate::latency::LatencyRecorder;
use crate::reporter::BenchReport;
use crate::setup::{now_ms, BenchCluster, LEASE_MS};

const HEARTBEAT_EVERY: usize = 64;
const PREFILL_RETRY_TIMEOUT: Duration = Duration::from_secs(15);
const PREFILL_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const READ_RETRY_TIMEOUT: Duration = Duration::from_secs(15);
const READ_RETRY_INTERVAL: Duration = Duration::from_millis(50);

fn is_heartbeat_tick(iteration: usize) -> bool {
    iteration.checked_rem(HEARTBEAT_EVERY) == Some(0)
}

fn heartbeat_client(client_ptr: usize) {
    let expires_at = now_ms().saturating_add(LEASE_MS);
    // SAFETY: called only for per-worker clients that are not shared by other workers.
    let client = unsafe { &mut *(client_ptr as *mut StoreClient) };
    let _ = client.heartbeat(expires_at);
}

fn heartbeat_worker_clients(
    writer_ptr: usize,
    reader_ptr: usize,
    writer_enabled: bool,
    reader_enabled: bool,
) {
    if writer_enabled {
        heartbeat_client(writer_ptr);
    }
    if reader_enabled {
        heartbeat_client(reader_ptr);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerKeyShard {
    start: usize,
    count: usize,
}

#[derive(Clone, Copy)]
struct OperationTarget<'a> {
    worker_id: usize,
    shard: WorkerKeyShard,
    key_offset: usize,
    width: usize,
    tenant: &'a str,
}

#[derive(Clone, Copy)]
struct WriteOperation<'a> {
    target: OperationTarget<'a>,
    global_seed: u64,
    iteration: u64,
    value_size: usize,
}

fn worker_key_shard(worker_id: usize, concurrency: usize, key_space_size: usize) -> WorkerKeyShard {
    let start = (worker_id * key_space_size) / concurrency;
    let end = ((worker_id + 1) * key_space_size) / concurrency;
    WorkerKeyShard {
        start,
        count: (end - start).max(1),
    }
}

fn worker_key_index(shard: WorkerKeyShard, offset: usize) -> usize {
    shard.start + (offset % shard.count)
}

fn operation_key_offset(iteration: usize, width: usize, shard: WorkerKeyShard) -> usize {
    iteration.saturating_mul(width.max(1)) % shard.count
}

fn split_mixed_shards(shard: WorkerKeyShard) -> (WorkerKeyShard, WorkerKeyShard) {
    let read_count = (shard.count / 2).max(1);
    let write_count = shard.count.saturating_sub(read_count).max(1);
    let read_shard = WorkerKeyShard {
        start: shard.start,
        count: read_count,
    };
    let write_shard = WorkerKeyShard {
        start: shard.start + shard.count.saturating_sub(write_count),
        count: write_count,
    };
    (read_shard, write_shard)
}

fn request_replication_policy(replica_count: usize) -> Option<ReplicationPolicy> {
    (replica_count > 1).then(|| ReplicationPolicy::default().replica_count(replica_count))
}

fn operation_keys(target: OperationTarget<'_>) -> Vec<String> {
    (0..target.width.max(1))
        .map(|offset| {
            make_key(
                "bench",
                target.worker_id,
                worker_key_index(target.shard, target.key_offset + offset),
            )
        })
        .collect()
}

fn operation_objects<'a>(keys: &'a [String], tenant: &'a str) -> Vec<ObjectRef<'a>> {
    keys.iter()
        .map(|key| ObjectRef::new(key).tenant(tenant))
        .collect()
}

fn write_operation_width(interface: WriteInterface, batch_size: usize) -> usize {
    match interface {
        WriteInterface::Put => 1,
        WriteInterface::BatchPut | WriteInterface::BatchPutFrom => batch_size,
    }
}

fn read_operation_width(interface: ReadInterface, batch_size: usize) -> usize {
    match interface {
        ReadInterface::Get => 1,
        ReadInterface::BatchGet | ReadInterface::BatchGetInto => batch_size,
    }
}

struct RegisteredWriteBuffers<'a> {
    client: &'a StoreClient,
    storage: Vec<u8>,
    slot_size: usize,
}

impl<'a> RegisteredWriteBuffers<'a> {
    fn new(
        client: &'a StoreClient,
        slots: usize,
        slot_size: usize,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let total_bytes = slots.saturating_mul(slot_size);
        let mut storage = vec![0u8; total_bytes];
        client.register_buffer(storage.as_mut_ptr().cast(), storage.len())?;
        Ok(Self {
            client,
            storage,
            slot_size,
        })
    }

    fn fill_slot(&mut self, slot: usize, value: &[u8]) {
        let start = slot * self.slot_size;
        let end = start + self.slot_size;
        self.storage[start..end].copy_from_slice(value);
    }

    fn request<'b>(
        &'b self,
        slot: usize,
        key: &'b str,
        tenant: &'b str,
        policy: Option<&ReplicationPolicy>,
    ) -> PutFromRequest<'b> {
        let start = slot * self.slot_size;
        let request = PutFromRequest::new(
            key,
            unsafe { self.storage.as_ptr().add(start).cast() },
            self.slot_size,
        )
        .tenant(tenant);
        if let Some(policy) = policy {
            request.replication(policy.clone())
        } else {
            request
        }
    }
}

impl Drop for RegisteredWriteBuffers<'_> {
    fn drop(&mut self) {
        if self.storage.is_empty() {
            return;
        }
        if let Err(error) = self
            .client
            .unregister_buffer(self.storage.as_mut_ptr().cast(), self.storage.len())
        {
            warn!("bench unregister_buffer failed during cleanup: {error}");
        }
    }
}

struct ReadBuffers {
    slots: Vec<Vec<u8>>,
}

impl ReadBuffers {
    fn new(slots: usize, slot_size: usize) -> Self {
        Self {
            slots: (0..slots).map(|_| vec![0u8; slot_size]).collect(),
        }
    }

    fn requests<'a>(&'a mut self, keys: &'a [String], tenant: &'a str) -> Vec<GetRequest<'a>> {
        keys.iter()
            .zip(self.slots.iter_mut())
            .map(|(key, buffer)| GetRequest::new(key, buffer.as_mut_slice()).tenant(tenant))
            .collect()
    }
}

fn execute_write(
    writer: &StoreClient,
    interface: WriteInterface,
    operation: WriteOperation<'_>,
    policy: Option<&ReplicationPolicy>,
    batch_put_from_buffers: Option<&mut RegisteredWriteBuffers<'_>>,
) -> StoreResult<u64> {
    let keys = operation_keys(operation.target);
    match interface {
        WriteInterface::Put => {
            let key = &keys[0];
            let value = payload(
                &make_seed(operation.global_seed, key, operation.iteration),
                operation.value_size,
            );
            if let Some(policy) = policy {
                writer.put_in_tenant_with_policy(operation.target.tenant, key, &value, policy)?;
            } else {
                writer.put_in_tenant(operation.target.tenant, key, &value)?;
            }
            Ok(value.len() as u64)
        }
        WriteInterface::BatchPut => {
            let values: Vec<Vec<u8>> = keys
                .iter()
                .map(|key| {
                    payload(
                        &make_seed(operation.global_seed, key, operation.iteration),
                        operation.value_size,
                    )
                })
                .collect();
            let requests: Vec<PutRequest<'_>> = keys
                .iter()
                .zip(values.iter())
                .map(|(key, value)| put_request(key, value, operation.target.tenant, policy))
                .collect();
            writer.batch_put(&requests)?;
            Ok(values.iter().map(|value| value.len() as u64).sum())
        }
        WriteInterface::BatchPutFrom => {
            let Some(buffers) = batch_put_from_buffers else {
                return Err(StoreError::InvalidState(
                    "batch_put_from buffers are not initialized".to_string(),
                ));
            };
            for (slot, key) in keys.iter().enumerate() {
                let value = payload(
                    &make_seed(operation.global_seed, key, operation.iteration),
                    operation.value_size,
                );
                buffers.fill_slot(slot, &value);
            }
            let requests = keys
                .iter()
                .enumerate()
                .map(|(slot, key)| buffers.request(slot, key, operation.target.tenant, policy))
                .collect::<Vec<_>>();
            writer.batch_put_from(&requests)?;
            Ok((requests.len() * operation.value_size) as u64)
        }
    }
}

fn execute_read(
    reader: &StoreClient,
    interface: ReadInterface,
    target: OperationTarget<'_>,
    batch_get_into_buffers: Option<&mut ReadBuffers>,
) -> StoreResult<u64> {
    let keys = operation_keys(target);
    match interface {
        ReadInterface::Get => reader
            .get_in_tenant(target.tenant, &keys[0])
            .map(|value| value.len() as u64),
        ReadInterface::BatchGet => reader
            .batch_get(&operation_objects(&keys, target.tenant))
            .map(|values| values.iter().map(|value| value.len() as u64).sum()),
        ReadInterface::BatchGetInto => {
            let Some(buffers) = batch_get_into_buffers else {
                return Err(StoreError::InvalidState(
                    "batch_get_into buffers are not initialized".to_string(),
                ));
            };
            let mut requests = buffers.requests(&keys, target.tenant);
            let sizes = reader.batch_get_into(&mut requests)?;
            Ok(sizes.iter().map(|size| *size as u64).sum())
        }
    }
}

fn put_request<'a>(
    key: &'a str,
    value: &'a [u8],
    tenant: &'a str,
    policy: Option<&ReplicationPolicy>,
) -> PutRequest<'a> {
    let request = PutRequest::new(key, value).tenant(tenant);
    if let Some(policy) = policy {
        request.replication(policy.clone())
    } else {
        request
    }
}

fn is_retryable_prefill_error(message: &str) -> bool {
    message.contains("transport error")
        || message.contains("tent_get_segment_info")
        || message.contains("not connected")
}

fn is_retryable_read_error(message: &str) -> bool {
    is_retryable_prefill_error(message)
        || message.contains("is not readable")
        || message.contains("object not found")
        || message.contains("has no readable replica owner")
}

fn execute_read_with_retry(
    reader: &StoreClient,
    interface: ReadInterface,
    target: OperationTarget<'_>,
    batch_get_into_buffers: Option<&mut ReadBuffers>,
) -> StoreResult<u64> {
    let retry_started = Instant::now();
    let mut attempts = 0usize;
    let mut batch_get_into_buffers = batch_get_into_buffers;
    loop {
        attempts += 1;
        let buffers = match batch_get_into_buffers {
            Some(ref mut buffers) => Some(&mut **buffers),
            None => None,
        };
        match execute_read(reader, interface, target, buffers) {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                let message = error.to_string();
                if !is_retryable_read_error(&message)
                    || retry_started.elapsed() >= READ_RETRY_TIMEOUT
                {
                    return Err(error);
                }
                if attempts == 1 {
                    debug!(
                        worker = target.worker_id,
                        key_offset = target.key_offset,
                        width = target.width,
                        "read route not ready; retrying until route becomes readable"
                    );
                }
                thread::sleep(READ_RETRY_INTERVAL);
            }
        }
    }
}

struct LiveCounters {
    put_ops: AtomicU64,
    get_ops: AtomicU64,
    put_bytes: AtomicU64,
    get_bytes: AtomicU64,
    errors: AtomicU64,
    logged_errors: AtomicU64,
}

impl LiveCounters {
    fn new() -> Self {
        Self {
            put_ops: AtomicU64::new(0),
            get_ops: AtomicU64::new(0),
            put_bytes: AtomicU64::new(0),
            get_bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            logged_errors: AtomicU64::new(0),
        }
    }
}

pub fn run_bench(
    global: GlobalArgs,
    args: BenchArgs,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = BenchCluster::new(&global, args.writers, args.readers)?;

    let duration = args.duration.map(Duration::from_secs);

    let total_ops = if duration.is_some() {
        usize::MAX
    } else {
        args.iterations
    };

    let value_size = args.value_size;
    let batch_size = args.batch_size;
    let concurrency = args.concurrency;
    let read_ratio = args.read_ratio;
    let key_space_size = args.key_space_size;
    let global_seed = global.seed;
    let tenant = global.tenant.clone();
    let warmup = args.warmup;
    let replica_count = global.replica_count;
    let mode = args.mode;
    let write_interface = args.write_interface;
    let read_interface = args.read_interface;
    let report_interval = Duration::from_secs(args.report_interval);

    let counters = Arc::new(LiveCounters::new());
    let workers_stop = Arc::clone(&shutdown);
    let reporter_stop = Arc::clone(&shutdown);
    let reporter_counters = Arc::clone(&counters);

    let report_thread = thread::spawn(move || {
        let start = Instant::now();
        let mut prev_put_ops = 0u64;
        let mut prev_get_ops = 0u64;
        let mut prev_put_bytes = 0u64;
        let mut prev_get_bytes = 0u64;
        loop {
            thread::sleep(report_interval);
            if reporter_stop.load(Ordering::Relaxed) {
                break;
            }
            let elapsed = start.elapsed().as_secs();
            let cur_put_ops = reporter_counters.put_ops.load(Ordering::Relaxed);
            let cur_get_ops = reporter_counters.get_ops.load(Ordering::Relaxed);
            let cur_put_bytes = reporter_counters.put_bytes.load(Ordering::Relaxed);
            let cur_get_bytes = reporter_counters.get_bytes.load(Ordering::Relaxed);
            let cur_errors = reporter_counters.errors.load(Ordering::Relaxed);

            let interval_secs = report_interval.as_secs_f64();
            let d_put = cur_put_ops - prev_put_ops;
            let d_get = cur_get_ops - prev_get_ops;
            let d_put_bytes = cur_put_bytes - prev_put_bytes;
            let d_get_bytes = cur_get_bytes - prev_get_bytes;

            let mut parts = Vec::new();
            if d_put > 0 {
                let qps = d_put as f64 / interval_secs;
                let mib_s = d_put_bytes as f64 / interval_secs / (1024.0 * 1024.0);
                parts.push(format!("put: {qps:.0} ops/s {mib_s:.1} MiB/s"));
            }
            if d_get > 0 {
                let qps = d_get as f64 / interval_secs;
                let mib_s = d_get_bytes as f64 / interval_secs / (1024.0 * 1024.0);
                parts.push(format!("get: {qps:.0} ops/s {mib_s:.1} MiB/s"));
            }
            if parts.is_empty() {
                info!("[{elapsed}s] warming up...");
            } else {
                info!(
                    "[{elapsed}s] {} | total: {} put + {} get | errors: {cur_errors}",
                    parts.join(" | "),
                    cur_put_ops,
                    cur_get_ops,
                );
            }

            prev_put_ops = cur_put_ops;
            prev_get_ops = cur_get_ops;
            prev_put_bytes = cur_put_bytes;
            prev_get_bytes = cur_get_bytes;
        }
    });

    // Collect raw pointer addresses before entering scope — clients live in `cluster`
    // which outlives the scope. The pointers are only dereferenced as shared references.
    let writer_ptrs: Vec<usize> = (0..concurrency)
        .map(|i| cluster.writer(i) as *const StoreClient as usize)
        .collect();
    let reader_ptrs: Vec<usize> = (0..concurrency)
        .map(|i| cluster.reader(i) as *const StoreClient as usize)
        .collect();
    let worker_heartbeats_enabled = cluster.writers.len() >= concurrency;
    let reader_heartbeats_enabled = cluster.readers.len() >= concurrency;

    let scoped_result = thread::scope(|scope| {
        let mut handles = Vec::new();

        for worker_id in 0..concurrency {
            let writer_ptr = writer_ptrs[worker_id];
            let reader_ptr = reader_ptrs[worker_id];
            let worker_mode = mode;
            let tenant = tenant.clone();
            let stop = Arc::clone(&workers_stop);
            let ctrs = Arc::clone(&counters);

            let handle = scope.spawn(move || {
                // SAFETY: cluster (and its StoreClients) lives for the entire thread::scope.
                // Worker heartbeats take &mut StoreClient and therefore run only when
                // each worker maps to distinct writer/reader clients.
                let writer = unsafe { &*(writer_ptr as *const StoreClient) };
                let reader = unsafe { &*(reader_ptr as *const StoreClient) };
                heartbeat_worker_clients(
                    writer_ptr,
                    reader_ptr,
                    worker_heartbeats_enabled,
                    reader_heartbeats_enabled,
                );

                let mut put_rec = LatencyRecorder::with_capacity(total_ops.min(1 << 20));
                let mut get_rec = LatencyRecorder::with_capacity(total_ops.min(1 << 20));

                let shard = worker_key_shard(worker_id, concurrency, key_space_size);
                let (mixed_read_shard, mixed_write_shard) = split_mixed_shards(shard);
                let replication_policy = request_replication_policy(replica_count);
                let mut batch_put_from_buffers =
                    matches!(write_interface, WriteInterface::BatchPutFrom).then(|| {
                        RegisteredWriteBuffers::new(writer, batch_size.max(1), value_size)
                    });
                let mut batch_get_into_buffers =
                    matches!(read_interface, ReadInterface::BatchGetInto)
                        .then(|| ReadBuffers::new(batch_size.max(1), value_size));
                let batch_put_from_buffers = match batch_put_from_buffers.take() {
                    Some(Ok(buffers)) => Some(buffers),
                    Some(Err(error)) => return Err(error.to_string()),
                    None => None,
                };
                let mut batch_put_from_buffers = batch_put_from_buffers;

                // Pre-populate keys for get-only or mixed modes.
                if matches!(worker_mode, BenchMode::Get | BenchMode::Mixed) {
                    let prefill_shard = if matches!(worker_mode, BenchMode::Mixed) {
                        mixed_read_shard
                    } else {
                        shard
                    };

                    for ki in 0..prefill_shard.count {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }

                        let key = make_key(
                            "bench",
                            worker_id,
                            worker_key_index(prefill_shard, ki),
                        );
                        let retry_started = Instant::now();
                        let mut attempts = 0usize;

                        loop {
                            attempts += 1;
                            let operation = WriteOperation {
                                target: OperationTarget {
                                    worker_id,
                                    shard: prefill_shard,
                                    key_offset: ki,
                                    width: 1,
                                    tenant: &tenant,
                                },
                                global_seed,
                                iteration: 0,
                                value_size,
                            };

                            match execute_write(
                                writer,
                                write_interface,
                                operation,
                                replication_policy.as_ref(),
                                batch_put_from_buffers.as_mut(),
                            ) {
                                Ok(_) => {
                                    if is_heartbeat_tick(ki + 1) {
                                        heartbeat_worker_clients(
                                            writer_ptr,
                                            reader_ptr,
                                            worker_heartbeats_enabled,
                                            reader_heartbeats_enabled,
                                        );
                                    }
                                    break;
                                }
                                Err(error) => {
                                    let message = error.to_string();
                                    if !is_retryable_prefill_error(&message)
                                        || retry_started.elapsed() >= PREFILL_RETRY_TIMEOUT
                                    {
                                        stop.store(true, Ordering::Relaxed);
                                        return Err(format!(
                                            "prefill failed: worker={worker_id} tenant={tenant} key={key} attempts={attempts}: {message}"
                                        ));
                                    }
                                    if ctrs.logged_errors.fetch_add(1, Ordering::Relaxed) < 8 {
                                        debug!(
                                            "prefill retry: worker={worker_id} tenant={tenant} key={key} attempts={attempts}: {message}"
                                        );
                                    }
                                    thread::sleep(PREFILL_RETRY_INTERVAL);
                                    if is_heartbeat_tick(attempts) {
                                        heartbeat_worker_clients(
                                            writer_ptr,
                                            reader_ptr,
                                            worker_heartbeats_enabled,
                                            reader_heartbeats_enabled,
                                        );
                                    }
                                }
                            }
                        }
                    }

                    heartbeat_worker_clients(
                        writer_ptr,
                        reader_ptr,
                        worker_heartbeats_enabled,
                        reader_heartbeats_enabled,
                    );
                }

                // Warmup (excluded from stats)
                for wi in 0..warmup {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let warmup_shard = if matches!(worker_mode, BenchMode::Mixed) {
                        mixed_write_shard
                    } else {
                        shard
                    };
                    let _ = execute_write(
                        writer,
                        write_interface,
                        WriteOperation {
                            target: OperationTarget {
                                worker_id,
                                shard: warmup_shard,
                                key_offset: operation_key_offset(
                                    wi,
                                    write_operation_width(write_interface, batch_size),
                                    warmup_shard,
                                ),
                                width: write_operation_width(write_interface, batch_size),
                                tenant: &tenant,
                            },
                            global_seed,
                            iteration: 0,
                            value_size,
                        },
                        replication_policy.as_ref(),
                        batch_put_from_buffers.as_mut(),
                    );
                    if is_heartbeat_tick(wi + 1) {
                        heartbeat_worker_clients(
                            writer_ptr,
                            reader_ptr,
                            worker_heartbeats_enabled,
                            reader_heartbeats_enabled,
                        );
                    }
                }

                put_rec.reset_start();
                get_rec.reset_start();
                let deadline = duration.map(|value| Instant::now() + value);

                let mut iter = 0usize;
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    if let Some(dl) = deadline {
                        if Instant::now() >= dl {
                            break;
                        }
                    } else if iter >= total_ops {
                        break;
                    }

                    let is_read = match worker_mode {
                        BenchMode::Put => false,
                        BenchMode::Get => true,
                        BenchMode::Mixed => (iter % 100) < read_ratio as usize,
                    };

                    if is_read {
                        let read_shard = if matches!(worker_mode, BenchMode::Mixed) {
                            mixed_read_shard
                        } else {
                            shard
                        };
                        let read_width = read_operation_width(read_interface, batch_size);
                        let t0 = Instant::now();
                        match execute_read_with_retry(
                            reader,
                            read_interface,
                            OperationTarget {
                                worker_id,
                                shard: read_shard,
                                key_offset: operation_key_offset(iter, read_width, read_shard),
                                width: read_width,
                                tenant: &tenant,
                            },
                            batch_get_into_buffers.as_mut(),
                        ) {
                            Ok(len) => {
                                get_rec.record(t0.elapsed(), len);
                                ctrs.get_ops.fetch_add(1, Ordering::Relaxed);
                                ctrs.get_bytes.fetch_add(len, Ordering::Relaxed);
                            }
                            Err(e) => {
                                get_rec.record_error();
                                ctrs.errors.fetch_add(1, Ordering::Relaxed);
                                if ctrs.logged_errors.fetch_add(1, Ordering::Relaxed) < 8 {
                                    warn!("get error: {e}");
                                }
                            }
                        }
                    } else {
                        let write_shard = if matches!(worker_mode, BenchMode::Mixed) {
                            mixed_write_shard
                        } else {
                            shard
                        };
                        let write_width = write_operation_width(write_interface, batch_size);
                        let t0 = Instant::now();
                        match execute_write(
                            writer,
                            write_interface,
                            WriteOperation {
                                target: OperationTarget {
                                    worker_id,
                                    shard: write_shard,
                                    key_offset: operation_key_offset(
                                        iter,
                                        write_width,
                                        write_shard,
                                    ),
                                    width: write_width,
                                    tenant: &tenant,
                                },
                                global_seed,
                                iteration: iter as u64,
                                value_size,
                            },
                            replication_policy.as_ref(),
                            batch_put_from_buffers.as_mut(),
                        ) {
                            Ok(written_bytes) => {
                                put_rec.record(t0.elapsed(), written_bytes);
                                ctrs.put_ops.fetch_add(1, Ordering::Relaxed);
                                ctrs.put_bytes.fetch_add(written_bytes, Ordering::Relaxed);
                            }
                            Err(e) => {
                                put_rec.record_error();
                                ctrs.errors.fetch_add(1, Ordering::Relaxed);
                                if ctrs.logged_errors.fetch_add(1, Ordering::Relaxed) < 8 {
                                    warn!("put error: {e}");
                                }
                            }
                        }
                    }

                    iter += 1;
                    if is_heartbeat_tick(iter) {
                        heartbeat_worker_clients(
                            writer_ptr,
                            reader_ptr,
                            worker_heartbeats_enabled,
                            reader_heartbeats_enabled,
                        );
                    }
                }

                put_rec.finish();
                get_rec.finish();
                Ok((put_rec, get_rec))
            });
            handles.push(handle);
        }

        let mut agg_put = LatencyRecorder::new();
        let mut agg_get = LatencyRecorder::new();
        for handle in handles {
            let (put_rec, get_rec) = handle
                .join()
                .map_err(|_| "worker thread panicked".to_string())??;
            agg_put.merge(&put_rec);
            agg_get.merge(&get_rec);
        }
        Ok::<_, String>((agg_put, agg_get))
    });

    shutdown.store(true, Ordering::Relaxed);
    let _ = report_thread.join();
    let (put_agg, get_agg) = scoped_result.map_err(std::io::Error::other)?;

    let elapsed = put_agg.elapsed().max(get_agg.elapsed());
    let has_puts = put_agg.total_ops() > 0;
    let has_gets = get_agg.total_ops() > 0;
    let put_errors = put_agg.error_count();
    let get_errors = get_agg.error_count();

    let mut report = BenchReport {
        mode: format!("{mode:?}").to_lowercase(),
        write_label: write_interface.as_label().to_string(),
        read_label: read_interface.as_label().to_string(),
        elapsed,
        concurrency,
        value_size,
        batch_size,
        put_stats: if has_puts { Some(put_agg) } else { None },
        get_stats: if has_gets { Some(get_agg) } else { None },
    };
    report.print(&args.output_format);

    cluster.heartbeat_all().ok();
    if put_errors > 0 || get_errors > 0 {
        Err(std::io::Error::other(format!(
            "benchmark completed with errors: put_errors={put_errors} get_errors={get_errors}"
        ))
        .into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_key_shards_partition_global_key_space() {
        let shard = worker_key_shard(2, 8, 10_000);

        assert_eq!(
            shard,
            WorkerKeyShard {
                start: 2_500,
                count: 1_250
            }
        );
        assert_eq!(worker_key_index(shard, 0), 2_500);
        assert_eq!(worker_key_index(shard, 1_249), 3_749);
        assert_eq!(worker_key_index(shard, 1_250), 2_500);
    }

    #[test]
    fn batch_offsets_stay_inside_worker_shard() {
        let shard = worker_key_shard(2, 8, 10_000);
        let keys = (0..8)
            .map(|offset| worker_key_index(shard, offset))
            .collect::<Vec<_>>();

        assert_eq!(
            keys,
            vec![2_500, 2_501, 2_502, 2_503, 2_504, 2_505, 2_506, 2_507]
        );
    }

    #[test]
    fn batched_operations_advance_by_batch_width() {
        let shard = worker_key_shard(0, 1, 128);

        assert_eq!(operation_key_offset(0, 6, shard), 0);
        assert_eq!(operation_key_offset(1, 6, shard), 6);
        assert_eq!(operation_key_offset(21, 6, shard), 126);
        assert_eq!(operation_key_offset(22, 6, shard), 4);
    }

    #[test]
    fn mixed_mode_splits_stable_read_and_write_shards() {
        let shard = worker_key_shard(2, 8, 10_000);
        let (read_shard, write_shard) = split_mixed_shards(shard);

        assert_eq!(
            read_shard,
            WorkerKeyShard {
                start: 2_500,
                count: 625
            }
        );
        assert_eq!(
            write_shard,
            WorkerKeyShard {
                start: 3_125,
                count: 625
            }
        );
    }

    #[test]
    fn batch_interfaces_use_batch_width() {
        assert_eq!(write_operation_width(WriteInterface::BatchPut, 8), 8);
        assert_eq!(write_operation_width(WriteInterface::BatchPutFrom, 8), 8);
        assert_eq!(read_operation_width(ReadInterface::BatchGet, 8), 8);
        assert_eq!(read_operation_width(ReadInterface::BatchGetInto, 8), 8);
    }

    #[test]
    fn prefill_retries_transport_readiness_errors_only() {
        assert!(is_retryable_prefill_error(
            "transport error: tent_get_segment_info failed with rc=-1"
        ));
        assert!(is_retryable_prefill_error(
            "RpcServiceError: message: not connected"
        ));
        assert!(!is_retryable_prefill_error(
            "invalid state: no placement candidates available"
        ));
    }

    #[test]
    fn read_retry_accepts_route_readiness_errors_only() {
        assert!(is_retryable_read_error(
            "object not found: tenant=bench key=bench-w0-k0 is not readable"
        ));
        assert!(is_retryable_read_error(
            "object not found: tenant=bench key=bench-w0-k0 has no readable replica owner"
        ));
        assert!(is_retryable_read_error(
            "transport error: tent_get_segment_info failed with rc=-1"
        ));
        assert!(!is_retryable_read_error(
            "checksum mismatch for tenant=bench key=bench-w0-k0"
        ));
    }

    #[test]
    fn heartbeat_tick_uses_stable_multiple_check() {
        assert!(is_heartbeat_tick(0));
        assert!(is_heartbeat_tick(64));
        assert!(!is_heartbeat_tick(65));
    }
}
