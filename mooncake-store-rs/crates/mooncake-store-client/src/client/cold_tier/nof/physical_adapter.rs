//! Adapts executor-owned NoF physical objects to the existing Cold Tier backend contract.

use std::collections::HashSet;

use mooncake_store_core::{
    ColdBackingRoute, ColdBackingState, NofBackingState, ObjectRoute, Result, StoreError,
};

use crate::client::cold_tier::layout::{
    decode_hex, encode_hex, OpaquePhysicalKey, PhysicalKeyInput,
};
use crate::client::{
    ColdObjectWrite, PersistentStorageBackend, PersistentStorageBackendHealth,
    PersistentStorageManagement,
};

use super::backend::NofBackend;
use super::backing::validate_payload;
use super::ensure_batch_len;
use super::object::repeated_error;
use super::physical::{
    NofPhysicalDelete, NofPhysicalDeleteRequest, NofPhysicalLimits, NofPhysicalLocator,
    NofPhysicalRead, NofPhysicalReadRequest, NofPhysicalRecoveredRecord, NofPhysicalWrite,
    NofPhysicalWriteRequest, MAX_ENCODED_LOCATOR_HEX_LEN,
};

const LOCATOR_PREFIX: &str = "nof-physical:v3:";
const ROOT_KEY_DOMAIN: &[u8] = b"root";

struct PreparedWrite<'a> {
    index: usize,
    key: OpaquePhysicalKey,
    cold_backing: &'a ColdBackingRoute,
    payload: &'a [u8],
}

pub(in crate::client) struct NofPhysicalAdapter {
    target_id: String,
    target: NofBackend,
    limits: NofPhysicalLimits,
}

impl NofPhysicalAdapter {
    pub(in crate::client) fn new(target_id: impl Into<String>, target: NofBackend) -> Result<Self> {
        let target_id = target_id.into();
        if target_id.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF physical target ID must not be empty".to_string(),
            ));
        }
        if target.backing.physical_write().is_none()
            || target.backing.physical_read().is_none()
            || target.backing.physical_delete().is_none()
        {
            return Err(StoreError::InvalidState(
                "NoF physical adapter requires read, write and delete capabilities".to_string(),
            ));
        }
        let limits = target.physical_limits.ok_or_else(|| {
            StoreError::InvalidState(
                "NoF physical adapter requires advertised physical limits".to_string(),
            )
        })?;
        Ok(Self {
            target_id,
            target,
            limits,
        })
    }

    fn writer(&self) -> &dyn NofPhysicalWrite {
        self.target
            .backing
            .physical_write()
            .expect("physical write capability was checked during construction")
    }

    fn reader(&self) -> &dyn NofPhysicalRead {
        self.target
            .backing
            .physical_read()
            .expect("physical read capability was checked during construction")
    }

    fn deleter(&self) -> &dyn NofPhysicalDelete {
        self.target
            .backing
            .physical_delete()
            .expect("physical delete capability was checked during construction")
    }

    pub(in crate::client) fn requires_recovery(&self) -> bool {
        self.target.backing.physical_recovery().is_some()
    }

    pub(in crate::client) fn recover_routes(&self, routes: &[ObjectRoute]) -> Result<()> {
        let Some(recovery) = self.target.backing.physical_recovery() else {
            return Ok(());
        };
        let mut seen = HashSet::new();
        let mut records = Vec::new();
        for route in routes {
            let Some(backing) = route.nof_backing.as_ref() else {
                continue;
            };
            if backing.state != NofBackingState::Materialized {
                continue;
            }
            let expected_value_size = usize::try_from(backing.length).map_err(|_| {
                StoreError::InvalidState(
                    "NoF recovered physical value length does not fit usize".to_string(),
                )
            })?;
            for target in backing
                .all_targets()
                .into_iter()
                .filter(|target| target.target_id == self.target_id)
            {
                let (key, locator) = Self::decode_locator(target.object_locator)?;
                if seen.insert((key.clone(), locator.clone())) {
                    records.push(NofPhysicalRecoveredRecord {
                        key,
                        locator,
                        expected_value_size,
                    });
                }
            }
        }
        recovery.recover(&records)
    }

    fn root_key(
        &self,
        route: Option<&ObjectRoute>,
        cold_backing: &ColdBackingRoute,
        payload: &[u8],
    ) -> Result<OpaquePhysicalKey> {
        let route_version = route.map(|route| route.version.0).unwrap_or_default();
        let route_version = route_version.to_le_bytes();
        let length = cold_backing.length.to_le_bytes();
        let checksum = cold_backing.checksum.unwrap_or_default().to_le_bytes();
        let route_key = route
            .map(|route| route.key.0.as_bytes())
            .unwrap_or_else(|| cold_backing.object_locator.as_bytes());
        let fields: [&[u8]; 7] = [
            ROOT_KEY_DOMAIN,
            self.target_id.as_bytes(),
            route_key,
            &route_version,
            &length,
            &checksum,
            payload,
        ];
        let key = self.target.key_codec.encode(PhysicalKeyInput {
            domain: &self.target.key_domain,
            fields: &fields,
            chunk_index: None,
        })?;
        if key.as_bytes().is_empty() {
            return Err(StoreError::InvalidState(
                "NoF physical key codec returned an empty root key".to_string(),
            ));
        }
        Ok(key)
    }

    fn encode_locator(key: &OpaquePhysicalKey, locator: &NofPhysicalLocator) -> String {
        format!(
            "{LOCATOR_PREFIX}{}:{}",
            key.to_hex(),
            encode_hex(locator.as_bytes())
        )
    }

    fn decode_locator(locator: &str) -> Result<(OpaquePhysicalKey, NofPhysicalLocator)> {
        let encoded = locator.strip_prefix(LOCATOR_PREFIX).ok_or_else(|| {
            StoreError::InvalidState(format!(
                "NoF physical locator has an unknown prefix: {locator}"
            ))
        })?;
        let (key, executor_locator) = encoded.split_once(':').ok_or_else(|| {
            StoreError::InvalidState("NoF physical locator is missing its executor field".into())
        })?;
        if executor_locator.contains(':') || executor_locator.len() > MAX_ENCODED_LOCATOR_HEX_LEN {
            return Err(StoreError::InvalidState(
                "NoF physical locator has invalid executor fields".to_string(),
            ));
        }
        let key = OpaquePhysicalKey::new(decode_hex(key, "NoF physical locator key")?);
        if key.as_bytes().is_empty() {
            return Err(StoreError::InvalidState(
                "NoF physical locator contains an empty key".to_string(),
            ));
        }
        let locator = NofPhysicalLocator::new(decode_hex(
            executor_locator,
            "NoF executor physical locator",
        )?)?;
        Ok((key, locator))
    }

    fn materialized_route(
        cold_backing: &ColdBackingRoute,
        key: &OpaquePhysicalKey,
        locator: &NofPhysicalLocator,
    ) -> ColdBackingRoute {
        ColdBackingRoute {
            object_locator: Self::encode_locator(key, locator),
            state: ColdBackingState::Materialized,
            ..cold_backing.clone()
        }
    }

    fn run_batches<T, U>(
        &self,
        operation: &str,
        requests: &[T],
        size_of: impl Fn(&T) -> u64,
        mut run: impl FnMut(&[T]) -> Vec<Result<U>>,
    ) -> Vec<Result<U>> {
        let sizes = requests.iter().map(size_of).collect::<Vec<_>>();
        let ranges = match bounded_ranges(&sizes, self.limits) {
            Ok(ranges) => ranges,
            Err(error) => return repeated_error(requests.len(), error),
        };
        let mut results = Vec::with_capacity(requests.len());
        for range in ranges {
            let expected = range.len();
            let batch = run(&requests[range]);
            if let Err(error) = ensure_batch_len(operation, expected, batch.len()) {
                results.extend(repeated_error(expected, error));
            } else {
                results.extend(batch);
            }
        }
        results
    }

    fn put_objects(&self, writes: &[ColdObjectWrite<'_>]) -> Vec<Result<ColdBackingRoute>> {
        let mut results = (0..writes.len()).map(|_| None).collect::<Vec<_>>();
        let mut prepared = Vec::with_capacity(writes.len());
        for (index, write) in writes.iter().enumerate() {
            let key = validate_payload(write.cold_backing, write.payload)
                .and_then(|_| self.root_key(write.route, write.cold_backing, write.payload));
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

        let mut seen = HashSet::new();
        let mut duplicates = HashSet::new();
        for write in &prepared {
            if !seen.insert(write.key.clone()) {
                duplicates.insert(write.key.clone());
            }
        }
        prepared.retain(|write| {
            if duplicates.contains(&write.key) {
                results[write.index] = Some(Err(StoreError::Conflict(
                    "NoF write batch contains duplicate physical keys".to_string(),
                )));
                false
            } else {
                true
            }
        });

        let put_results = self.run_batches(
            "physical put",
            &prepared,
            |write| write.payload.len() as u64,
            |writes| {
                let requests = writes
                    .iter()
                    .map(|write| NofPhysicalWriteRequest {
                        key: write.key.clone(),
                        value: write.payload,
                    })
                    .collect::<Vec<_>>();
                self.writer().put_batch(&requests)
            },
        );
        let mut successful = Vec::new();
        for (write, status) in prepared.iter().zip(put_results) {
            match status {
                Ok(locator) => {
                    results[write.index] = Some(Ok(Self::materialized_route(
                        write.cold_backing,
                        &write.key,
                        &locator,
                    )));
                    successful.push((
                        write.index,
                        NofPhysicalDeleteRequest {
                            key: write.key.clone(),
                            locator,
                            expected_value_size: write.payload.len(),
                        },
                    ));
                }
                Err(error) => results[write.index] = Some(Err(error)),
            }
        }

        if !successful.is_empty() {
            if let Err(error) = self.writer().flush() {
                self.discard_unpublished(&successful);
                for (index, _) in successful {
                    results[index] = Some(Err(StoreError::Transport(format!(
                        "NoF physical flush failed before route publication: {error}"
                    ))));
                }
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

    fn discard_unpublished(&self, successful: &[(usize, NofPhysicalDeleteRequest)]) {
        let requests = successful
            .iter()
            .map(|(_, request)| request.clone())
            .collect::<Vec<_>>();
        let cleanup = self.writer().discard_unpublished(&requests);
        let cleanup = ensure_batch_len("physical discard", requests.len(), cleanup.len())
            .map(|_| cleanup)
            .unwrap_or_else(|error| repeated_error(requests.len(), error));
        let failures = cleanup
            .into_iter()
            .filter_map(Result::err)
            .collect::<Vec<_>>();
        if let Some(error) = failures.first() {
            tracing::warn!(
                target_id = %self.target_id,
                failures = failures.len(),
                error = %error,
                "NoF failed to discard unpublished physical objects"
            );
        }
    }

    fn get_one(&self, cold_backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        let (key, locator) = Self::decode_locator(&cold_backing.object_locator)?;
        let expected_value_size = usize::try_from(cold_backing.length).map_err(|_| {
            StoreError::InvalidState("NoF physical value length does not fit usize".to_string())
        })?;
        let value = one_batch_result(
            "physical get",
            self.reader().get_batch(&[NofPhysicalReadRequest {
                key,
                locator,
                expected_value_size,
            }]),
        )?;
        if let Some(value) = value.as_deref() {
            validate_payload(cold_backing, value)?;
        }
        Ok(value)
    }

    fn delete_one(&self, cold_backing: &ColdBackingRoute) -> Result<bool> {
        let (key, locator) = Self::decode_locator(&cold_backing.object_locator)?;
        let expected_value_size = usize::try_from(cold_backing.length).map_err(|_| {
            StoreError::InvalidState("NoF physical value length does not fit usize".to_string())
        })?;
        match one_batch_result(
            "physical delete",
            self.deleter().delete_batch(&[NofPhysicalDeleteRequest {
                key,
                locator,
                expected_value_size,
            }]),
        ) {
            Ok(()) => {
                self.writer().flush()?;
                Ok(true)
            }
            Err(StoreError::NotFound(_)) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

impl PersistentStorageBackend for NofPhysicalAdapter {
    fn storage_management(&self) -> PersistentStorageManagement {
        self.target.management_mode()
    }

    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        self.target.health_snapshot()
    }

    fn put_object(
        &self,
        cold_backing: &ColdBackingRoute,
        payload: &[u8],
    ) -> Result<ColdBackingRoute> {
        self.put_object_with_route(None, cold_backing, payload)
    }

    fn put_object_with_route(
        &self,
        route: Option<&ObjectRoute>,
        cold_backing: &ColdBackingRoute,
        payload: &[u8],
    ) -> Result<ColdBackingRoute> {
        one_batch_result(
            "single put",
            self.put_objects(&[ColdObjectWrite {
                route,
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
        self.get_one(cold_backing)
    }

    fn delete_object(&self, cold_backing: &ColdBackingRoute) -> Result<bool> {
        self.delete_one(cold_backing)
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
    results
        .into_iter()
        .next()
        .ok_or_else(|| StoreError::Transport(format!("NoF {operation} returned no result")))?
}

fn bounded_ranges(
    item_sizes: &[u64],
    limits: NofPhysicalLimits,
) -> Result<Vec<std::ops::Range<usize>>> {
    if item_sizes.is_empty() {
        return Ok(Vec::new());
    }
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut bytes = 0u64;
    for (index, size) in item_sizes.iter().copied().enumerate() {
        if size > limits.max_batch_bytes {
            return Err(StoreError::InvalidState(format!(
                "NoF request size {size} exceeds executor batch byte limit {}",
                limits.max_batch_bytes
            )));
        }
        let exceeds_items = index - start >= limits.max_batch_items;
        let exceeds_bytes = bytes
            .checked_add(size)
            .is_none_or(|total| total > limits.max_batch_bytes);
        if index > start && (exceeds_items || exceeds_bytes) {
            ranges.push(start..index);
            start = index;
            bytes = 0;
        }
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| StoreError::InvalidState("NoF batch byte count overflow".to_string()))?;
    }
    ranges.push(start..item_sizes.len());
    Ok(ranges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        NofBacking, NofHealth, NofPhysicalDelete, NofPhysicalRead, NofPhysicalWrite,
        NofStorageHealth, PhysicalKeyCodec, PhysicalKeyInput,
    };
    use mooncake_store_core::{ClientEpoch, ClientRuntimeId, ClientStableId};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    const MEMORY_LAYOUT: &[u8] = b"memory:whole-object:v1";

    #[derive(Default)]
    struct MemoryExecutor {
        values: Mutex<HashMap<OpaquePhysicalKey, Vec<u8>>>,
    }

    impl NofBacking for MemoryExecutor {
        fn physical_limits(&self) -> Option<NofPhysicalLimits> {
            Some(NofPhysicalLimits {
                max_batch_items: 64,
                max_batch_bytes: u64::MAX,
            })
        }

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
        fn put_batch(
            &self,
            requests: &[NofPhysicalWriteRequest<'_>],
        ) -> Vec<Result<NofPhysicalLocator>> {
            let mut values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|request| {
                    values.insert(request.key.clone(), request.value.to_vec());
                    NofPhysicalLocator::new(MEMORY_LAYOUT)
                })
                .collect()
        }
    }

    impl NofPhysicalRead for MemoryExecutor {
        fn get_batch(&self, requests: &[NofPhysicalReadRequest]) -> Vec<Result<Option<Vec<u8>>>> {
            let values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|request| {
                    if request.locator.as_bytes() != MEMORY_LAYOUT {
                        return Err(StoreError::InvalidState(
                            "unexpected memory executor layout".to_string(),
                        ));
                    }
                    Ok(values.get(&request.key).cloned())
                })
                .collect()
        }
    }

    impl NofPhysicalDelete for MemoryExecutor {
        fn delete_batch(&self, requests: &[NofPhysicalDeleteRequest]) -> Vec<Result<()>> {
            let mut values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|request| {
                    if request.locator.as_bytes() != MEMORY_LAYOUT {
                        return Err(StoreError::InvalidState(
                            "unexpected memory executor layout".to_string(),
                        ));
                    }
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

    #[test]
    fn executor_receives_the_complete_value_and_owns_its_layout() {
        let executor = Arc::new(MemoryExecutor::default());
        let backend =
            NofPhysicalAdapter::new("nof-test", NofBackend::new(executor.clone()).unwrap())
                .unwrap();
        let payload = vec![7; 150];
        let materialized = backend
            .put_object(&route("logical@v1", &payload), &payload)
            .unwrap();

        assert!(materialized.object_locator.starts_with(LOCATOR_PREFIX));
        assert_eq!(executor.values.lock().unwrap().len(), 1);
        assert_eq!(backend.get_object(&materialized).unwrap(), Some(payload));
        assert!(backend.delete_object(&materialized).unwrap());
        assert!(executor.values.lock().unwrap().is_empty());
    }

    #[test]
    fn backing_health_does_not_claim_mooncake_storage_management() {
        let executor = Arc::new(MemoryExecutor::default());
        let backend =
            NofPhysicalAdapter::new("nof-test", NofBackend::new(executor).unwrap()).unwrap();

        assert_eq!(
            backend.storage_management(),
            PersistentStorageManagement::BackendManaged
        );
        let health = backend.health().unwrap();
        assert_eq!(health.capacity_bytes, None);
        assert_eq!(health.available_bytes, None);
    }

    #[test]
    fn malformed_locator_fails_closed() {
        let executor = Arc::new(MemoryExecutor::default());
        let backend =
            NofPhysicalAdapter::new("nof-test", NofBackend::new(executor).unwrap()).unwrap();
        let mut backing = route("logical@v1", b"value");
        backing.object_locator = "file:v1:abcd".to_string();
        assert!(matches!(
            backend.get_object(&backing),
            Err(StoreError::InvalidState(_))
        ));
    }

    struct CollidingCodec;

    impl PhysicalKeyCodec for CollidingCodec {
        fn encode(&self, _input: PhysicalKeyInput<'_>) -> Result<OpaquePhysicalKey> {
            Ok(OpaquePhysicalKey::new([1]))
        }
    }

    #[test]
    fn codec_collision_rejects_every_conflicting_object_before_write() {
        let executor = Arc::new(MemoryExecutor::default());
        let target = NofBackend::new(executor.clone())
            .unwrap()
            .key_codec(Arc::new(CollidingCodec));
        let backend = NofPhysicalAdapter::new("nof-test", target).unwrap();
        let payload = vec![9; 150];
        let first = route("first", &payload);
        let second = route("second", &payload);
        let results = backend.put_objects_batch(&[
            ColdObjectWrite {
                route: None,
                cold_backing: &first,
                payload: &payload,
            },
            ColdObjectWrite {
                route: None,
                cold_backing: &second,
                payload: &payload,
            },
        ]);
        assert!(results
            .iter()
            .all(|result| matches!(result, Err(StoreError::Conflict(_)))));
        assert!(executor.values.lock().unwrap().is_empty());
    }
}
