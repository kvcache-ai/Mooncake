use mooncake_store_client::{MooncakeCompatibilityFacade, PutRequest, StoreClient};
use tracing::{error, info, warn};

use crate::cli::{GlobalArgs, VerifyArgs};
use crate::datagen::{ensure_payload, make_key, make_seed, payload};
use crate::setup::BenchCluster;

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

    let mut results = Vec::new();

    // 1. Single round-trip put/get
    {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        results.push(run_check("single-round-trip", move || {
            let key = make_key("verify-single", 0, 0);
            let s = make_seed(seed, &key, 0);
            let value = payload(&s, value_size);
            writer.put(&key, &value).map_err(|e| e.to_string())?;
            let got = reader.get(&key).map_err(|e| e.to_string())?;
            ensure_payload("single-round-trip", &value, &got)
        }));
    }

    // 2. get_into path
    {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        results.push(run_check("get-into-buffer", move || {
            let key = make_key("verify-get-into", 0, 0);
            let s = make_seed(seed, &key, 0);
            let value = payload(&s, value_size);
            writer.put(&key, &value).map_err(|e| e.to_string())?;
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
            let puts: Vec<PutRequest<'_>> = keys
                .iter()
                .zip(values.iter())
                .map(|(k, v)| PutRequest::new(k, v).tenant(&tenant))
                .collect();
            writer.batch_put(&puts).map_err(|e| e.to_string())?;
            for (key, expected) in keys.iter().zip(values.iter()) {
                let got = reader.get(key).map_err(|e| e.to_string())?;
                ensure_payload("batch-get", expected, &got)?;
            }
            Ok(())
        }));
    }

    // 4. is_exist
    {
        let writer = unsafe { &*(writer_addr as *const StoreClient) };
        let reader = unsafe { &*(reader_addr as *const StoreClient) };
        results.push(run_check("is-exist", move || {
            let key = make_key("verify-exist", 0, 0);
            let s = make_seed(seed, &key, 0);
            let value = payload(&s, value_size);
            writer.put(&key, &value).map_err(|e| e.to_string())?;
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
            for (key, value) in &pairs {
                let req = PutRequest::new(key, value).tenant(&tenant);
                writer.batch_put(&[req]).map_err(|e| e.to_string())?;
            }
            Ok(())
        });
        results.push(write_ok);
        // Heartbeat between phases
        cluster.heartbeat_all().ok();
        // Read phase
        results.push(run_check("multi-key-read", move || {
            for (key, expected) in &pairs {
                let got = reader.get(key).map_err(|e| e.to_string())?;
                ensure_payload("multi-key", expected, &got)?;
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
                let req = PutRequest::new(&key, &value).tenant(&tenant);
                writer.batch_put(&[req]).map_err(|e| e.to_string())?;
                last_value = value;
            }
            let got = reader.get(&key).map_err(|e| e.to_string())?;
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
            let req = PutRequest::new(&key, &value).tenant(&tenant);
            writer.batch_put(&[req]).map_err(|e| e.to_string())?;
            writer.remove(&key, false).map_err(|e| e.to_string())?;
            let exists = reader.is_exist(&key).map_err(|e| e.to_string())?;
            if exists {
                return Err(format!("is_exist returned true for key {key} after remove"));
            }
            let req2 = PutRequest::new(&key, &value).tenant(&tenant);
            writer.batch_put(&[req2]).map_err(|e| e.to_string())?;
            let got = reader.get(&key).map_err(|e| e.to_string())?;
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
            writer
                .batch_put(&[PutRequest::new(key, &value_a).tenant(&tenant_a)])
                .map_err(|e| e.to_string())?;
            writer
                .batch_put(&[PutRequest::new(key, &value_b).tenant(&tenant_b)])
                .map_err(|e| e.to_string())?;
            let got_a = reader
                .get_in_tenant(&tenant_a, key)
                .map_err(|e| e.to_string())?;
            let got_b = reader
                .get_in_tenant(&tenant_b, key)
                .map_err(|e| e.to_string())?;
            ensure_payload("tenant-a", &value_a, &got_a)?;
            ensure_payload("tenant-b", &value_b, &got_b)?;
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
