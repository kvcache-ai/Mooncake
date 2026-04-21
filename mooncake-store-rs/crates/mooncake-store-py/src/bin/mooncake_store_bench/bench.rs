use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use mooncake_store_client::{
    MooncakeCompatibilityFacade, PutRequest, ReplicationPolicy, StoreClient,
};
use tracing::{debug, info, warn};

use crate::cli::{BenchArgs, BenchMode, GlobalArgs};
use crate::datagen::{make_key, make_seed, payload};
use crate::latency::LatencyRecorder;
use crate::reporter::BenchReport;
use crate::setup::{now_ms, BenchCluster, LEASE_MS};

const HEARTBEAT_EVERY: usize = 64;
const PREFILL_RETRY_TIMEOUT: Duration = Duration::from_secs(15);
const PREFILL_RETRY_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerKeyShard {
    start: usize,
    count: usize,
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

fn request_replication_policy(replica_count: usize) -> Option<ReplicationPolicy> {
    (replica_count > 1).then(|| ReplicationPolicy::default().replica_count(replica_count))
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
    let mode = args.mode.clone();
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

    let scoped_result = thread::scope(|scope| {
        let mut handles = Vec::new();

        for worker_id in 0..concurrency {
            let writer_ptr = writer_ptrs[worker_id];
            let reader_ptr = reader_ptrs[worker_id];
            let mode = mode.clone();
            let tenant = tenant.clone();
            let stop = Arc::clone(&workers_stop);
            let ctrs = Arc::clone(&counters);

            let handle = scope.spawn(move || {
                // SAFETY: cluster (and its StoreClients) lives for the entire thread::scope.
                // Worker heartbeats take &mut StoreClient and therefore run only when
                // each worker maps to a distinct writer client.
                let writer = unsafe { &*(writer_ptr as *const StoreClient) };
                let reader = unsafe { &*(reader_ptr as *const StoreClient) };

                let mut put_rec = LatencyRecorder::with_capacity(total_ops.min(1 << 20));
                let mut get_rec = LatencyRecorder::with_capacity(total_ops.min(1 << 20));

                let shard = worker_key_shard(worker_id, concurrency, key_space_size);
                let replication_policy = request_replication_policy(replica_count);

                // Pre-populate keys for get-only or mixed modes
                if matches!(mode, BenchMode::Get | BenchMode::Mixed) {
                    for ki in 0..shard.count {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        let key = make_key("bench", worker_id, worker_key_index(shard, ki));
                        let s = make_seed(global_seed, &key, 0);
                        let value = payload(&s, value_size);
                        let retry_started = Instant::now();
                        let mut attempts = 0usize;
                        loop {
                            attempts += 1;
                            let request =
                                put_request(&key, &value, &tenant, replication_policy.as_ref());
                            match writer.batch_put(&[request]) {
                                Ok(_) => break,
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
                                }
                            }
                        }
                    }
                }

                // Warmup (excluded from stats)
                for wi in 0..warmup {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let key = make_key("bench-warm", worker_id, wi % shard.count);
                    let s = make_seed(global_seed, &key, 0);
                    let value = payload(&s, value_size);
                    let req = put_request(&key, &value, &tenant, replication_policy.as_ref());
                    let _ = writer.batch_put(&[req]);
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

                    let key_offset = iter % shard.count;
                    let is_read = match mode {
                        BenchMode::Put => false,
                        BenchMode::Get => true,
                        BenchMode::Mixed => (iter % 100) < read_ratio as usize,
                    };

                    if is_read {
                        let key =
                            make_key("bench", worker_id, worker_key_index(shard, key_offset));
                        let t0 = Instant::now();
                        match reader.get(&key) {
                            Ok(v) => {
                                let len = v.len() as u64;
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
                        // Build batch of keys+values (all in scope for the duration of batch_put)
                        let keys: Vec<String> = (0..batch_size)
                            .map(|bi| {
                                make_key(
                                    "bench",
                                    worker_id,
                                    worker_key_index(shard, key_offset + bi),
                                )
                            })
                            .collect();
                        let values: Vec<Vec<u8>> = keys
                            .iter()
                            .map(|k| payload(&make_seed(global_seed, k, iter as u64), value_size))
                            .collect();
                        let puts: Vec<PutRequest<'_>> = keys
                            .iter()
                            .zip(values.iter())
                            .map(|(k, v)| {
                                put_request(k, v, &tenant, replication_policy.as_ref())
                            })
                            .collect();
                        let t0 = Instant::now();
                        let batch_bytes = (batch_size * value_size) as u64;
                        match writer.batch_put(&puts) {
                            Ok(_) => {
                                put_rec.record(t0.elapsed(), batch_bytes);
                                ctrs.put_ops.fetch_add(1, Ordering::Relaxed);
                                ctrs.put_bytes.fetch_add(batch_bytes, Ordering::Relaxed);
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
                    if worker_heartbeats_enabled && iter.is_multiple_of(HEARTBEAT_EVERY) {
                        let expires_at = now_ms().saturating_add(LEASE_MS);
                        let writer_mut = unsafe { &mut *(writer_ptr as *mut StoreClient) };
                        let _ = writer_mut.heartbeat(expires_at);
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
}
