use mooncake_store_client::{
    GetRequest, MooncakeCompatibilityFacade, ObjectRef, PutFromRequest, PutRequest, StoreClient,
};
use tracing::{error, info, warn};

use crate::cli::{GlobalArgs, ReadInterface, VerifyArgs, WriteInterface};
use crate::datagen::{ensure_payload, make_key, make_seed, payload};
use crate::setup::BenchCluster;

struct VerifyPut<'a> {
    tenant: &'a str,
    key: &'a str,
    value: &'a [u8],
}

#[derive(Clone, Copy)]
struct VerifyGet<'a> {
    tenant: &'a str,
    key: &'a str,
}

struct CheckResult {
    passed: bool,
}

fn run_check<F>(name: &'static str, f: F) -> CheckResult
where
    F: FnOnce() -> Result<(), String>,
{
    match f() {
        Ok(()) => {
            info!("PASS {name}");
            CheckResult { passed: true }
        }
        Err(err) => {
            error!("FAIL {name}: {err}");
            CheckResult { passed: false }
        }
    }
}

fn verify_write_width(interface: WriteInterface, batch_size: usize) -> usize {
    match interface {
        WriteInterface::Put => 1,
        WriteInterface::BatchPut | WriteInterface::BatchPutFrom => batch_size,
    }
}

fn verify_read_width(interface: ReadInterface, batch_size: usize) -> usize {
    match interface {
        ReadInterface::Get => 1,
        ReadInterface::BatchGet | ReadInterface::BatchGetInto => batch_size,
    }
}

fn with_registered_buffer<T, F>(client: &StoreClient, size: usize, f: F) -> Result<T, String>
where
    F: FnOnce(&mut [u8]) -> Result<T, String>,
{
    let mut storage = vec![0u8; size];
    client
        .register_buffer(storage.as_mut_ptr().cast(), storage.len())
        .map_err(|error| error.to_string())?;
    let result = f(storage.as_mut_slice());
    if let Err(error) = client.unregister_buffer(storage.as_mut_ptr().cast(), storage.len()) {
        warn!("verify unregister_buffer failed during cleanup: {error}");
    }
    result
}

fn execute_verify_write(
    writer: &StoreClient,
    interface: WriteInterface,
    requests: &[VerifyPut<'_>],
) -> Result<(), String> {
    match interface {
        WriteInterface::Put => {
            for request in requests {
                writer
                    .put_in_tenant(request.tenant, request.key, request.value)
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        }
        WriteInterface::BatchPut => {
            let puts = requests
                .iter()
                .map(|request| PutRequest::new(request.key, request.value).tenant(request.tenant))
                .collect::<Vec<_>>();
            writer.batch_put(&puts).map_err(|error| error.to_string())?;
            Ok(())
        }
        WriteInterface::BatchPutFrom => {
            let total_bytes = requests.iter().map(|request| request.value.len()).sum();
            with_registered_buffer(writer, total_bytes, |storage| {
                let mut offset = 0usize;
                let puts = requests
                    .iter()
                    .map(|request| {
                        let start = offset;
                        let end = start + request.value.len();
                        storage[start..end].copy_from_slice(request.value);
                        offset = end;
                        PutFromRequest::new(
                            request.key,
                            unsafe { storage.as_ptr().add(start).cast() },
                            request.value.len(),
                        )
                        .tenant(request.tenant)
                    })
                    .collect::<Vec<_>>();
                writer
                    .batch_put_from(&puts)
                    .map_err(|error| error.to_string())?;
                Ok(())
            })
        }
    }
}

fn execute_verify_read(
    reader: &StoreClient,
    interface: ReadInterface,
    value_size: usize,
    requests: &[VerifyGet<'_>],
) -> Result<Vec<Vec<u8>>, String> {
    match interface {
        ReadInterface::Get => requests
            .iter()
            .map(|request| {
                reader
                    .get_in_tenant(request.tenant, request.key)
                    .map_err(|error| error.to_string())
            })
            .collect(),
        ReadInterface::BatchGet => {
            let objects = requests
                .iter()
                .map(|request| ObjectRef::new(request.key).tenant(request.tenant))
                .collect::<Vec<_>>();
            reader
                .batch_get(&objects)
                .map_err(|error| error.to_string())
        }
        ReadInterface::BatchGetInto => {
            let mut buffers = requests
                .iter()
                .map(|_| vec![0u8; value_size])
                .collect::<Vec<_>>();
            let mut gets = requests
                .iter()
                .zip(buffers.iter_mut())
                .map(|(request, buffer)| {
                    GetRequest::new(request.key, buffer.as_mut_slice()).tenant(request.tenant)
                })
                .collect::<Vec<_>>();
            let sizes = reader
                .batch_get_into(&mut gets)
                .map_err(|error| error.to_string())?;
            for (request, (buffer, size)) in requests.iter().zip(buffers.iter_mut().zip(sizes)) {
                if size != value_size {
                    return Err(format!(
                        "batch_get_into returned {size} bytes for key {}, expected {value_size}",
                        request.key
                    ));
                }
                buffer.truncate(size);
            }
            Ok(buffers)
        }
    }
}

fn write_verify_all(
    writer: &StoreClient,
    interface: WriteInterface,
    batch_size: usize,
    requests: &[VerifyPut<'_>],
) -> Result<(), String> {
    let width = verify_write_width(interface, batch_size);
    for chunk in requests.chunks(width) {
        execute_verify_write(writer, interface, chunk)?;
    }
    Ok(())
}

fn read_verify_all(
    reader: &StoreClient,
    interface: ReadInterface,
    batch_size: usize,
    value_size: usize,
    requests: &[VerifyGet<'_>],
) -> Result<Vec<Vec<u8>>, String> {
    let width = verify_read_width(interface, batch_size);
    let mut values = Vec::with_capacity(requests.len());
    for chunk in requests.chunks(width) {
        values.extend(execute_verify_read(reader, interface, value_size, chunk)?);
    }
    Ok(values)
}

pub fn run_verify(global: GlobalArgs, args: VerifyArgs) -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = BenchCluster::new(&global, 1, 1)?;

    // Convert to raw pointer addresses so closures can borrow `cluster` mutably
    // for heartbeat_all() without aliasing the client references.
    let writer_addr = &cluster.writers[0].runtime.client as *const StoreClient as usize;
    let reader_addr = &cluster.readers[0].runtime.client as *const StoreClient as usize;

    let value_size = args.value_size;
    let key_count = args.key_count;
    let batch_size = args.batch_size;
    let tenant = global.tenant.clone();
    let seed = global.seed;
    let write_interface = args.write_interface;
    let read_interface = args.read_interface;

    let mut results = Vec::new();

    // 1. Single round-trip put/get
    {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        let tenant = tenant.clone();
        results.push(run_check("single-round-trip", move || {
            let key = make_key("verify-single", 0, 0);
            let s = make_seed(seed, &key, 0);
            let value = payload(&s, value_size);
            write_verify_all(
                writer,
                write_interface,
                batch_size,
                &[VerifyPut {
                    tenant: &tenant,
                    key: &key,
                    value: &value,
                }],
            )?;
            let got = read_verify_all(
                reader,
                read_interface,
                batch_size,
                value_size,
                &[VerifyGet {
                    tenant: &tenant,
                    key: &key,
                }],
            )?
            .into_iter()
            .next()
            .expect("single read should return one value");
            ensure_payload("single-round-trip", &value, &got)
        }));
    }

    // 2. get_into path
    {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        let tenant = tenant.clone();
        results.push(run_check("get-into-buffer", move || {
            let key = make_key("verify-get-into", 0, 0);
            let s = make_seed(seed, &key, 0);
            let value = payload(&s, value_size);
            write_verify_all(
                writer,
                write_interface,
                batch_size,
                &[VerifyPut {
                    tenant: &tenant,
                    key: &key,
                    value: &value,
                }],
            )?;
            let mut buf = vec![0u8; value_size];
            let size = reader.get_into(&key, &mut buf).map_err(|e| e.to_string())?;
            if size != value_size {
                return Err(format!(
                    "get_into returned {size} bytes, expected {value_size}"
                ));
            }
            ensure_payload("get-into-buffer", &value, &buf)
        }));
    }

    // 3. Batch put/get — keys and values kept alive through the call
    {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        let tenant = tenant.clone();
        results.push(run_check("batch-put-get", move || {
            let keys: Vec<String> = (0..batch_size)
                .map(|i| make_key("verify-batch", 0, i))
                .collect();
            let values: Vec<Vec<u8>> = keys
                .iter()
                .map(|k| payload(&make_seed(seed, k, 0), value_size))
                .collect();
            let puts: Vec<VerifyPut<'_>> = keys
                .iter()
                .zip(values.iter())
                .map(|(k, v)| VerifyPut {
                    tenant: &tenant,
                    key: k,
                    value: v,
                })
                .collect();
            let gets: Vec<VerifyGet<'_>> = keys
                .iter()
                .map(|key| VerifyGet {
                    tenant: &tenant,
                    key,
                })
                .collect();
            write_verify_all(writer, write_interface, batch_size, &puts)?;
            let got_values =
                read_verify_all(reader, read_interface, batch_size, value_size, &gets)?;
            for (expected, got) in values.iter().zip(got_values.iter()) {
                ensure_payload("batch-get", expected, got)?;
            }
            Ok(())
        }));
    }

    // 4. is_exist
    {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        let tenant = tenant.clone();
        results.push(run_check("is-exist", move || {
            let key = make_key("verify-exist", 0, 0);
            let s = make_seed(seed, &key, 0);
            let value = payload(&s, value_size);
            write_verify_all(
                writer,
                write_interface,
                batch_size,
                &[VerifyPut {
                    tenant: &tenant,
                    key: &key,
                    value: &value,
                }],
            )?;
            let exists = reader.is_exist(&key).map_err(|e| e.to_string())?;
            if !exists {
                return Err(format!("is_exist returned false for key {key} after put"));
            }
            Ok(())
        }));
    }

    // 5. Multiple key round-trips — heartbeat between write and read phases
    {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        let tenant = tenant.clone();
        let pairs: Vec<(String, Vec<u8>)> = (0..key_count)
            .map(|i| {
                let key = make_key("verify-multi", 0, i);
                let s = make_seed(seed, &key, 0);
                (key, payload(&s, value_size))
            })
            .collect();
        // Write phase
        let write_ok = run_check("multi-key-write", || {
            let writes = pairs
                .iter()
                .map(|(key, value)| VerifyPut {
                    tenant: &tenant,
                    key,
                    value,
                })
                .collect::<Vec<_>>();
            write_verify_all(writer, write_interface, batch_size, &writes)
        });
        let reads = pairs
            .iter()
            .map(|(key, _)| VerifyGet {
                tenant: &tenant,
                key,
            })
            .collect::<Vec<_>>();
        let expected_values = pairs.iter().map(|(_, value)| value).collect::<Vec<_>>();
        results.push(write_ok);
        cluster.heartbeat_all().ok();
        results.push(run_check("multi-key-read", move || {
            let got_values =
                read_verify_all(reader, read_interface, batch_size, value_size, &reads)?;
            for (expected, got) in expected_values.iter().zip(got_values.iter()) {
                ensure_payload("multi-key", expected, got)?;
            }
            Ok(())
        }));
    }

    // 6. Overwrite correctness (opt-in)
    if args.verify_overwrite {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        let tenant = tenant.clone();
        results.push(run_check("overwrite-correctness", move || {
            let key = make_key("verify-overwrite", 0, 0);
            let mut last_value = Vec::new();
            for gen in 0..64usize {
                let s = make_seed(seed, &key, gen as u64);
                let value = payload(&s, value_size);
                write_verify_all(
                    writer,
                    write_interface,
                    batch_size,
                    &[VerifyPut {
                        tenant: &tenant,
                        key: &key,
                        value: &value,
                    }],
                )?;
                last_value = value;
            }
            let got = read_verify_all(
                reader,
                read_interface,
                batch_size,
                value_size,
                &[VerifyGet {
                    tenant: &tenant,
                    key: &key,
                }],
            )?
            .into_iter()
            .next()
            .expect("overwrite read should return one value");
            ensure_payload("overwrite", &last_value, &got)
        }));
    }

    // 7. Delete + reclaim (opt-in)
    if args.verify_delete {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        let tenant = tenant.clone();
        results.push(run_check("delete-reclaim", move || {
            let key = make_key("verify-delete", 0, 0);
            let s = make_seed(seed, &key, 0);
            let value = payload(&s, value_size);
            write_verify_all(
                writer,
                write_interface,
                batch_size,
                &[VerifyPut {
                    tenant: &tenant,
                    key: &key,
                    value: &value,
                }],
            )?;
            writer.remove(&key, false).map_err(|e| e.to_string())?;
            let exists = reader.is_exist(&key).map_err(|e| e.to_string())?;
            if exists {
                return Err(format!("is_exist returned true for key {key} after remove"));
            }
            write_verify_all(
                writer,
                write_interface,
                batch_size,
                &[VerifyPut {
                    tenant: &tenant,
                    key: &key,
                    value: &value,
                }],
            )?;
            let got = read_verify_all(
                reader,
                read_interface,
                batch_size,
                value_size,
                &[VerifyGet {
                    tenant: &tenant,
                    key: &key,
                }],
            )?
            .into_iter()
            .next()
            .expect("delete-reclaim read should return one value");
            ensure_payload("delete-reclaim-re-put", &value, &got)
        }));
    }

    // 8. Multi-tenant isolation (opt-in)
    if args.verify_multi_tenant {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        let tenant = tenant.clone();
        results.push(run_check("multi-tenant-isolation", move || {
            let key = "shared-isolation-key";
            let tenant_a = format!("{tenant}-a");
            let tenant_b = format!("{tenant}-b");
            let value_a = payload("tenant-a-data", value_size);
            let value_b = payload("tenant-b-data", value_size);
            let writes = [
                VerifyPut {
                    tenant: &tenant_a,
                    key,
                    value: &value_a,
                },
                VerifyPut {
                    tenant: &tenant_b,
                    key,
                    value: &value_b,
                },
            ];
            write_verify_all(writer, write_interface, batch_size, &writes)?;
            let reads = [
                VerifyGet {
                    tenant: &tenant_a,
                    key,
                },
                VerifyGet {
                    tenant: &tenant_b,
                    key,
                },
            ];
            let got = read_verify_all(reader, read_interface, batch_size, value_size, &reads)?;
            let got_a = &got[0];
            let got_b = &got[1];
            ensure_payload("tenant-a", &value_a, got_a)?;
            ensure_payload("tenant-b", &value_b, got_b)?;
            if got_a == got_b {
                return Err("tenant isolation failed: both tenants returned same data".to_string());
            }
            Ok(())
        }));
    }

    let total = results.len();
    let passed = results.iter().filter(|r| r.passed).count();
    let failed = total - passed;
    info!("");
    if failed > 0 {
        warn!("Results: {passed}/{total} passed, {failed} failed");
    } else {
        info!("Results: {passed}/{total} passed, {failed} failed");
    }

    if failed > 0 {
        Err(format!("{failed} verification check(s) failed").into())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::{ReadInterface, WriteInterface};

    #[test]
    fn verify_single_item_interfaces_ignore_batch_size() {
        assert_eq!(super::verify_write_width(WriteInterface::Put, 8), 1);
        assert_eq!(super::verify_read_width(ReadInterface::Get, 8), 1);
    }

    #[test]
    fn verify_batch_interfaces_use_batch_size() {
        assert_eq!(super::verify_write_width(WriteInterface::BatchPut, 8), 8);
        assert_eq!(super::verify_read_width(ReadInterface::BatchGet, 8), 8);
        assert_eq!(
            super::verify_write_width(WriteInterface::BatchPutFrom, 8),
            8
        );
        assert_eq!(super::verify_read_width(ReadInterface::BatchGetInto, 8), 8);
    }
}
