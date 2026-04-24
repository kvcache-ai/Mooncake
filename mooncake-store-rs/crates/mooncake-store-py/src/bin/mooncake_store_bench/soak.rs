use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use mooncake_store_client::{
    GetRequest, MooncakeCompatibilityFacade, ObjectRef, PutFromRequest, PutRequest, StoreClient,
};
use tracing::{debug, error, info, warn};

use crate::cli::{GlobalArgs, ReadInterface, SoakArgs, WriteInterface};
use crate::datagen::{ensure_payload, make_key, make_seed, payload};
use crate::fault::FaultInjector;
use crate::latency::LatencyRecorder;
use crate::reporter::print_progress_detailed;
use crate::setup::{now_ms, BenchCluster, LEASE_MS};

const HEARTBEAT_EVERY: usize = 64;

fn is_heartbeat_tick(iteration: usize) -> bool {
    iteration.checked_rem(HEARTBEAT_EVERY) == Some(0)
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

fn soak_keys(start: usize, width: usize, key_space_size: usize) -> Vec<String> {
    let bounded_key_space = key_space_size.max(1);
    (0..width.max(1))
        .map(|offset| make_key("soak", 0, (start + offset) % bounded_key_space))
        .collect()
}

struct RegisteredWriteBuffers {
    client: *const StoreClient,
    storage: Vec<u8>,
    slot_size: usize,
}

impl RegisteredWriteBuffers {
    fn new(
        client: &StoreClient,
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

    fn request<'b>(&'b self, slot: usize, key: &'b str, tenant: &'b str) -> PutFromRequest<'b> {
        let start = slot * self.slot_size;
        PutFromRequest::new(
            key,
            unsafe { self.storage.as_ptr().add(start).cast() },
            self.slot_size,
        )
        .tenant(tenant)
    }
}

impl Drop for RegisteredWriteBuffers {
    fn drop(&mut self) {
        if self.storage.is_empty() {
            return;
        }
        if let Err(error) = unsafe { &*self.client }
            .unregister_buffer(self.storage.as_mut_ptr().cast(), self.storage.len())
        {
            warn!("soak unregister_buffer failed during cleanup: {error}");
        }
    }
}

struct ReadBuffers {
    slots: Vec<Vec<u8>>,
    slot_size: usize,
}

impl ReadBuffers {
    fn new(slots: usize, slot_size: usize) -> Self {
        Self {
            slots: (0..slots).map(|_| vec![0u8; slot_size]).collect(),
            slot_size,
        }
    }

    fn requests<'a>(&'a mut self, keys: &'a [String], tenant: &'a str) -> Vec<GetRequest<'a>> {
        keys.iter()
            .zip(self.slots.iter_mut())
            .map(|(key, buffer)| GetRequest::new(key, buffer.as_mut_slice()).tenant(tenant))
            .collect()
    }
}

fn execute_soak_write(
    writer: &StoreClient,
    interface: WriteInterface,
    tenant: &str,
    entries: &[(&str, &[u8])],
    batch_put_from_buffers: Option<&mut RegisteredWriteBuffers>,
) -> Result<u64, Box<dyn std::error::Error>> {
    match interface {
        WriteInterface::Put => {
            let (key, value) = entries[0];
            writer.put_in_tenant(tenant, key, value)?;
            Ok(value.len() as u64)
        }
        WriteInterface::BatchPut => {
            let requests = entries
                .iter()
                .map(|(key, value)| PutRequest::new(key, value).tenant(tenant))
                .collect::<Vec<_>>();
            writer.batch_put(&requests)?;
            Ok(entries.iter().map(|(_, value)| value.len() as u64).sum())
        }
        WriteInterface::BatchPutFrom => {
            let Some(buffers) = batch_put_from_buffers else {
                return Err(
                    std::io::Error::other("batch_put_from buffers are not initialized").into(),
                );
            };
            for (slot, (_, value)) in entries.iter().enumerate() {
                buffers.fill_slot(slot, value);
            }
            let requests = entries
                .iter()
                .enumerate()
                .map(|(slot, (key, _))| buffers.request(slot, key, tenant))
                .collect::<Vec<_>>();
            writer.batch_put_from(&requests)?;
            Ok(entries.iter().map(|(_, value)| value.len() as u64).sum())
        }
    }
}

fn execute_soak_read(
    reader: &StoreClient,
    interface: ReadInterface,
    tenant: &str,
    keys: &[String],
    batch_get_into_buffers: Option<&mut ReadBuffers>,
) -> Result<Vec<Vec<u8>>, Box<dyn std::error::Error>> {
    match interface {
        ReadInterface::Get => Ok(vec![reader.get_in_tenant(tenant, &keys[0])?]),
        ReadInterface::BatchGet => {
            let objects = keys
                .iter()
                .map(|key| ObjectRef::new(key).tenant(tenant))
                .collect::<Vec<_>>();
            Ok(reader.batch_get(&objects)?)
        }
        ReadInterface::BatchGetInto => {
            let Some(buffers) = batch_get_into_buffers else {
                return Err(
                    std::io::Error::other("batch_get_into buffers are not initialized").into(),
                );
            };
            let mut requests = buffers.requests(keys, tenant);
            let sizes = reader.batch_get_into(&mut requests)?;
            drop(requests);
            let values = buffers
                .slots
                .iter_mut()
                .take(keys.len())
                .zip(sizes)
                .map(|(buffer, size)| {
                    buffer.truncate(size);
                    buffer.clone()
                })
                .collect::<Vec<_>>();
            for buffer in buffers.slots.iter_mut().take(keys.len()) {
                buffer.resize(buffers.slot_size, 0);
            }
            Ok(values)
        }
    }
}

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
    let batch_size = args.batch_size;
    let key_space_size = args.key_space_size;
    let global_seed = global.seed;
    let tenant = global.tenant.clone();
    let read_ratio = args.read_ratio;
    let verify_reads = args.verify_reads;
    let write_interface = args.write_interface;
    let read_interface = args.read_interface;
    let mut batch_put_from_buffers = if matches!(write_interface, WriteInterface::BatchPutFrom) {
        Some(RegisteredWriteBuffers::new(
            &cluster.writers[0].runtime.client,
            batch_size.max(1),
            value_size,
        )?)
    } else {
        None
    };
    let mut batch_get_into_buffers = matches!(read_interface, ReadInterface::BatchGetInto)
        .then(|| ReadBuffers::new(batch_size.max(1), value_size));

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
        "Soak test: duration={}s value_size={}B key_space={} write={} read={} faults={}",
        args.duration,
        value_size,
        key_space_size,
        write_interface.as_label(),
        read_interface.as_label(),
        fault_desc,
    );

    let mut iteration = 0usize;
    loop {
        if shutdown.load(Ordering::Relaxed) || Instant::now() >= deadline {
            break;
        }

        let key_index = iteration % key_space_size.max(1);
        let is_read = (iteration % 100) < read_ratio as usize;

        // Apply fault injection before each operation
        if let Some(fault_msg) = fault.pre_op() {
            debug!(fault = %fault_msg, "fault injected");
            total_errors += 1;
            iteration += 1;
            continue;
        }

        if is_read {
            let keys = soak_keys(
                key_index,
                read_operation_width(read_interface, batch_size),
                key_space_size,
            );
            let existing_keys = keys
                .into_iter()
                .filter(|key| key_generations.contains_key(key))
                .collect::<Vec<_>>();
            if !existing_keys.is_empty() {
                let t0 = Instant::now();
                match execute_soak_read(
                    &cluster.readers[0].runtime.client,
                    read_interface,
                    &tenant,
                    &existing_keys,
                    batch_get_into_buffers.as_mut(),
                ) {
                    Ok(values) => {
                        let total_bytes = values.iter().map(|value| value.len() as u64).sum();
                        get_stats.record(t0.elapsed(), total_bytes);
                        if verify_reads {
                            for (key, got) in existing_keys.iter().zip(values.iter()) {
                                let gen = key_generations
                                    .get(key)
                                    .copied()
                                    .expect("existing read key should have generation");
                                let last_gen = gen.saturating_sub(1);
                                let s = make_seed(global_seed, key, last_gen);
                                let expected = payload(&s, value_size);
                                if let Err(e) = ensure_payload("soak-verify", &expected, got) {
                                    error!("VERIFY FAIL: {e}");
                                    total_errors += 1;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!(
                            error = %e,
                            keys = ?existing_keys,
                            read_interface = read_interface.as_label(),
                            "get failed"
                        );
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
            let keys = soak_keys(
                key_index,
                write_operation_width(write_interface, batch_size),
                key_space_size,
            );
            let generations = keys
                .iter()
                .map(|key| (*key_generations.get(key).unwrap_or(&0), key.clone()))
                .collect::<Vec<_>>();
            let values = generations
                .iter()
                .map(|(generation, key)| {
                    let seed = make_seed(global_seed, key, *generation);
                    payload(&seed, value_size)
                })
                .collect::<Vec<_>>();
            let entries = keys
                .iter()
                .zip(values.iter())
                .map(|(key, value)| (key.as_str(), value.as_slice()))
                .collect::<Vec<_>>();

            let t0 = Instant::now();
            match execute_soak_write(
                &cluster.writers[0].runtime.client,
                write_interface,
                &tenant,
                &entries,
                batch_put_from_buffers.as_mut(),
            ) {
                Ok(total_bytes) => {
                    for (generation, key) in generations {
                        key_generations.insert(key, generation + 1);
                    }
                    put_stats.record(t0.elapsed(), total_bytes)
                }
                Err(e) => {
                    debug!(
                        error = %e,
                        keys = ?keys,
                        write_interface = write_interface.as_label(),
                        "put failed"
                    );
                    put_stats.record_error();
                    total_errors += 1;
                }
            }
        }

        if is_heartbeat_tick(iteration) {
            let expires_at = now_ms().saturating_add(LEASE_MS);
            let _ = cluster.writers[0].runtime.client.heartbeat(expires_at);
            let _ = cluster.readers[0].runtime.client.heartbeat(expires_at);
        }

        // Periodic progress report
        if last_report.elapsed() >= report_interval {
            let elapsed_secs = start.elapsed().as_secs();
            print_progress_detailed(
                elapsed_secs,
                write_interface.as_label(),
                read_interface.as_label(),
                &mut Some(put_stats),
                &mut Some(get_stats),
            );
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

#[cfg(test)]
mod tests {
    use crate::cli::{ReadInterface, WriteInterface};

    #[test]
    fn single_item_interfaces_ignore_batch_size() {
        assert_eq!(super::write_operation_width(WriteInterface::Put, 8), 1);
        assert_eq!(super::read_operation_width(ReadInterface::Get, 8), 1);
    }

    #[test]
    fn batch_interfaces_use_batch_size() {
        assert_eq!(super::write_operation_width(WriteInterface::BatchPut, 8), 8);
        assert_eq!(super::read_operation_width(ReadInterface::BatchGet, 8), 8);
        assert_eq!(
            super::write_operation_width(WriteInterface::BatchPutFrom, 8),
            8
        );
        assert_eq!(
            super::read_operation_width(ReadInterface::BatchGetInto, 8),
            8
        );
    }

    #[test]
    fn heartbeat_tick_uses_stable_multiple_check() {
        assert!(super::is_heartbeat_tick(0));
        assert!(super::is_heartbeat_tick(64));
        assert!(!super::is_heartbeat_tick(65));
    }
}
