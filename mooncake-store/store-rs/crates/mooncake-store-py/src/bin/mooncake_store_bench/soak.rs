use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mooncake_store_client::{MooncakeCompatibilityFacade, PutRequest};
use tracing::{debug, error, info, warn};

use crate::cli::{GlobalArgs, SoakArgs};
use crate::datagen::{ensure_payload, make_key, make_seed, payload};
use crate::fault::FaultInjector;
use crate::latency::LatencyRecorder;
use crate::reporter::print_progress_detailed;
use crate::setup::{now_ms, BenchCluster, LEASE_MS};

const HEARTBEAT_EVERY: usize = 64;

pub fn run_soak(
    global: GlobalArgs,
    args: SoakArgs,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = BenchCluster::new(&global, 1, 1)?;

    let fault = FaultInjector::new(args.fault.clone());
    let deadline = Instant::now() + Duration::from_secs(args.duration);
    let report_interval = Duration::from_secs(args.report_interval);

    let value_size = args.value_size;
    let key_space_size = args.key_space_size;
    let global_seed = global.seed;
    let tenant = global.tenant.clone();
    let read_ratio = args.read_ratio;
    let verify_reads = args.verify_reads;

    let mut key_generations: HashMap<String, u64> = HashMap::new();
    let mut put_stats = LatencyRecorder::new();
    let mut get_stats = LatencyRecorder::new();
    let mut total_errors = 0u64;
    let mut last_report = Instant::now();
    let start = Instant::now();

    let fault_desc = if fault.is_empty() {
        "none".to_string()
    } else {
        format!("{} spec(s)", args.fault.len())
    };
    info!(
        "Soak test: duration={}s value_size={}B key_space={} faults={}",
        args.duration, value_size, key_space_size, fault_desc,
    );

    let mut iteration = 0usize;
    loop {
        if shutdown.load(Ordering::Relaxed) || Instant::now() >= deadline {
            break;
        }

        let key_index = iteration % key_space_size.max(1);
        let key = make_key("soak", 0, key_index);
        let is_read = (iteration % 100) < read_ratio as usize;

        // Apply fault injection before each operation
        if let Some(fault_msg) = fault.pre_op() {
            debug!(fault = %fault_msg, "fault injected");
            total_errors += 1;
            iteration += 1;
            continue;
        }

        if is_read {
            if let Some(&gen) = key_generations.get(&key) {
                let t0 = Instant::now();
                match cluster.readers[0].runtime.client.get(&key) {
                    Ok(got) => {
                        get_stats.record(t0.elapsed(), got.len() as u64);
                        if verify_reads {
                            // Verify against the generation that was last written
                            let last_gen = gen.saturating_sub(1);
                            let s = make_seed(global_seed, &key, last_gen);
                            let expected = payload(&s, value_size);
                            if let Err(e) = ensure_payload("soak-verify", &expected, &got) {
                                error!("VERIFY FAIL: {e}");
                                total_errors += 1;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, key = %key, "get failed");
                        get_stats.record_error();
                        total_errors += 1;
                    }
                }
            } else {
                // Key not yet written — skip this read
                iteration += 1;
                continue;
            }
        } else {
            let gen = key_generations.entry(key.clone()).or_insert(0);
            let s = make_seed(global_seed, &key, *gen);
            let value = payload(&s, value_size);
            *gen += 1;

            let t0 = Instant::now();
            let req = PutRequest::new(&key, &value).tenant(&tenant);
            match cluster.writers[0].runtime.client.batch_put(&[req]) {
                Ok(_) => put_stats.record(t0.elapsed(), value.len() as u64),
                Err(e) => {
                    debug!(error = %e, key = %key, "put failed");
                    put_stats.record_error();
                    total_errors += 1;
                }
            }
        }

        if iteration.is_multiple_of(HEARTBEAT_EVERY) {
            let expires_at = now_ms().saturating_add(LEASE_MS);
            let _ = cluster.writers[0].runtime.client.heartbeat(expires_at);
            let _ = cluster.readers[0].runtime.client.heartbeat(expires_at);
        }

        // Periodic progress report
        if last_report.elapsed() >= report_interval {
            let elapsed_secs = start.elapsed().as_secs();
            print_progress_detailed(elapsed_secs, &mut Some(put_stats), &mut Some(get_stats));
            put_stats = LatencyRecorder::new();
            get_stats = LatencyRecorder::new();
            last_report = Instant::now();
        }

        iteration += 1;
    }

    let elapsed = start.elapsed();
    info!("");
    info!("=== Soak Test Complete ===");
    info!(
        "Duration: {:.1}s | Total ops: {} | Total errors: {}",
        elapsed.as_secs_f64(),
        iteration,
        total_errors,
    );
    if total_errors > 0 {
        warn!("{total_errors} errors occurred during soak test");
    }

    cluster.heartbeat_all().ok();
    Ok(())
}
