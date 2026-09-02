//! Cold Tier backend for executor-owned NoF physical objects.

use mooncake_store_core::{ColdBackingRoute, ColdBackingState, Result, StoreError};

use crate::client::cold_tier::layout::{derive_physical_key, OpaquePhysicalKey, PhysicalKeyInput};
use crate::client::{
    ColdObjectWrite, PersistentObjectProbe, PersistentStorageBackend,
    PersistentStorageBackendHealth,
};

use super::backend::NofBackend;
use super::backing::validate_payload;
use super::ensure_batch_len;
use super::object::repeated_error;
use super::physical::{NofPhysicalDeleteRequest, NofPhysicalReadRequest, NofPhysicalWriteRequest};

const PHYSICAL_KEY_DOMAIN: &[u8] = b"mooncake:nof:physical";
const ROOT_KEY_DOMAIN: &[u8] = b"root";

struct PreparedWrite<'a> {
    index: usize,
    key: OpaquePhysicalKey,
    cold_backing: &'a ColdBackingRoute,
    payload: &'a [u8],
}

pub(in crate::client) struct NofPhysicalStorageBackend {
    target: NofBackend,
}

impl NofPhysicalStorageBackend {
    pub(in crate::client) fn new(target: NofBackend) -> Self {
        Self { target }
    }

    fn root_key(&self, cold_backing: &ColdBackingRoute) -> Result<OpaquePhysicalKey> {
        let fields: [&[u8]; 2] = [ROOT_KEY_DOMAIN, cold_backing.object_locator.as_bytes()];
        let key = derive_physical_key(PhysicalKeyInput {
            domain: PHYSICAL_KEY_DOMAIN,
            fields: &fields,
            chunk_index: None,
        })?;
        Ok(key)
    }

    fn request(&self, cold_backing: &ColdBackingRoute) -> Result<NofPhysicalReadRequest> {
        Ok(NofPhysicalReadRequest {
            key: self.root_key(cold_backing)?,
        })
    }

    fn put_objects(&self, writes: &[ColdObjectWrite<'_>]) -> Vec<Result<ColdBackingRoute>> {
        let mut results = (0..writes.len()).map(|_| None).collect::<Vec<_>>();
        let mut prepared = Vec::with_capacity(writes.len());
        for (index, write) in writes.iter().enumerate() {
            let key = validate_payload(write.cold_backing, write.payload)
                .and_then(|_| self.root_key(write.cold_backing));
            match key {
                Ok(key) => prepared.push(PreparedWrite {
                    index,
                    key,
                    cold_backing: write.cold_backing,
                    payload: write.payload,
                }),
                Err(error) => results[index] = Some(Err(error)),
            }
        }

        let requests = prepared
            .iter()
            .map(|write| NofPhysicalWriteRequest {
                key: write.key.clone(),
                value: write.payload,
            })
            .collect::<Vec<_>>();
        let put_results = self
            .target
            .backing
            .physical_write()
            .expect("physical write capability was checked during construction")
            .put_batch(&requests);
        let put_results = ensure_batch_len("physical put", prepared.len(), put_results.len())
            .map(|_| put_results)
            .unwrap_or_else(|error| repeated_error(prepared.len(), error));
        for (write, status) in prepared.iter().zip(put_results) {
            match status {
                Ok(()) => {
                    results[write.index] = Some(Ok(ColdBackingRoute {
                        state: ColdBackingState::Materialized,
                        ..write.cold_backing.clone()
                    }));
                }
                Err(error) => results[write.index] = Some(Err(error)),
            }
        }

        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(StoreError::Transport(
                        "NoF physical write omitted a positional result".to_string(),
                    ))
                })
            })
            .collect()
    }
}

impl PersistentStorageBackend for NofPhysicalStorageBackend {
    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        self.target.health_snapshot()
    }

    fn put_object(
        &self,
        cold_backing: &ColdBackingRoute,
        payload: &[u8],
    ) -> Result<ColdBackingRoute> {
        one_batch_result(
            "single put",
            self.put_objects(&[ColdObjectWrite {
                route: None,
                cold_backing,
                payload,
            }]),
        )
    }

    fn put_objects_batch_profiled(
        &self,
        writes: &[ColdObjectWrite<'_>],
        _record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<ColdBackingRoute>> {
        self.put_objects(writes)
    }

    fn get_object(&self, cold_backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        let request = self.request(cold_backing)?;
        let value = one_batch_result(
            "physical get",
            self.target
                .backing
                .physical_read()
                .expect("physical read capability was checked during construction")
                .get_batch(&[request]),
        )?;
        if cold_backing.length != 0 {
            if let Some(value) = value.as_deref() {
                validate_payload(cold_backing, value)?;
            }
        }
        Ok(value)
    }

    fn probe_object(
        &self,
        cold_backing: &ColdBackingRoute,
    ) -> Result<Option<PersistentObjectProbe>> {
        let Some(query) = self.target.backing.physical_query() else {
            return Ok(self
                .get_object(cold_backing)?
                .map(|value| PersistentObjectProbe {
                    length: Some(value.len() as u64),
                }));
        };
        let found = one_batch_result(
            "physical query",
            query.query_batch(&[self.request(cold_backing)?]),
        )?;
        Ok(found.then_some(PersistentObjectProbe { length: None }))
    }

    fn delete_object(&self, cold_backing: &ColdBackingRoute) -> Result<bool> {
        let request = self.request(cold_backing)?;
        match one_batch_result(
            "physical delete",
            self.target
                .backing
                .physical_delete()
                .expect("physical delete capability was checked during construction")
                .delete_batch(&[NofPhysicalDeleteRequest { key: request.key }]),
        ) {
            Ok(()) => Ok(true),
            Err(StoreError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn put_pending_source(&self, _cold_backing: &ColdBackingRoute, _payload: &[u8]) -> Result<()> {
        Ok(())
    }

    fn get_pending_source(&self, _cold_backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn delete_pending_source(&self, _cold_backing: &ColdBackingRoute) -> Result<bool> {
        Ok(false)
    }

    fn disable_pending_source(&self) -> bool {
        true
    }
}

fn one_batch_result<T>(operation: &str, results: Vec<Result<T>>) -> Result<T> {
    ensure_batch_len(operation, 1, results.len())?;
    results.into_iter().next().expect("length checked above")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        NofBacking, NofHealth, NofPhysicalDelete, NofPhysicalRead, NofPhysicalWrite,
        NofStorageHealth,
    };
    use mooncake_store_core::{ClientEpoch, ClientRuntimeId, ClientStableId};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryExecutor {
        values: Mutex<HashMap<OpaquePhysicalKey, Vec<u8>>>,
    }

    impl NofBacking for MemoryExecutor {
        fn physical_write(&self) -> Option<&dyn NofPhysicalWrite> {
            Some(self)
        }

        fn physical_read(&self) -> Option<&dyn NofPhysicalRead> {
            Some(self)
        }

        fn physical_delete(&self) -> Option<&dyn NofPhysicalDelete> {
            Some(self)
        }

        fn health_capability(&self) -> Option<&dyn NofHealth> {
            Some(self)
        }
    }

    impl NofHealth for MemoryExecutor {
        fn health(&self) -> Result<NofStorageHealth> {
            Ok(NofStorageHealth::default())
        }
    }

    impl NofPhysicalWrite for MemoryExecutor {
        fn put_batch(&self, requests: &[NofPhysicalWriteRequest<'_>]) -> Vec<Result<()>> {
            let mut values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|request| {
                    values.insert(request.key.clone(), request.value.to_vec());
                    Ok(())
                })
                .collect()
        }
    }

    impl NofPhysicalRead for MemoryExecutor {
        fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>> {
            let values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|request| Ok(values.get(&request.key).cloned()))
                .collect()
        }
    }

    impl NofPhysicalDelete for MemoryExecutor {
        fn delete_batch(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>> {
            let mut values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|request| {
                    values
                        .remove(&request.key)
                        .map(|_| ())
                        .ok_or_else(|| StoreError::NotFound("test key".to_string()))
                })
                .collect()
        }
    }

    fn route(locator: &str, payload: &[u8]) -> ColdBackingRoute {
        ColdBackingRoute {
            owner: ClientRuntimeId {
                stable_id: ClientStableId::new("owner"),
                epoch: ClientEpoch(1),
            },
            cold_tier_id: "nof-test".to_string(),
            object_locator: locator.to_string(),
            length: payload.len() as u64,
            checksum: Some(crate::client::payload_checksum(payload)),
            state: ColdBackingState::PendingOffload,
            replicas: Vec::new(),
        }
    }

    fn memory_backend(executor: Arc<MemoryExecutor>) -> NofPhysicalStorageBackend {
        NofPhysicalStorageBackend::new(NofBackend::new(executor).unwrap())
    }

    #[test]
    fn executor_receives_the_complete_value_and_owns_its_layout() {
        let executor = Arc::new(MemoryExecutor::default());
        let backend = memory_backend(executor.clone());
        let payload = vec![7; 150];
        let materialized = backend
            .put_object(&route("logical@v1", &payload), &payload)
            .unwrap();

        assert_eq!(materialized.object_locator, "logical@v1");
        assert_eq!(executor.values.lock().unwrap().len(), 1);
        assert_eq!(backend.get_object(&materialized).unwrap(), Some(payload));
        assert!(backend.delete_object(&materialized).unwrap());
        assert!(executor.values.lock().unwrap().is_empty());
    }

    #[test]
    fn backing_health_reports_no_capacity_when_executor_omits_it() {
        let executor = Arc::new(MemoryExecutor::default());
        let backend = memory_backend(executor);

        let health = backend.health().unwrap();
        assert_eq!(health.capacity_bytes, None);
        assert_eq!(health.available_bytes, None);
    }
}
