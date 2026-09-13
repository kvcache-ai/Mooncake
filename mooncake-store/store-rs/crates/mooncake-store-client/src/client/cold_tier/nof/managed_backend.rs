//! Thin adapter from the existing Cold Tier backend contract to managed NoF capabilities.

use mooncake_store_core::{ColdBackingRoute, ColdBackingState, Result, StoreError};

use crate::client::{
    ColdObjectRead, ColdObjectWrite, PersistentStorageBackend, PersistentStorageBackendHealth,
};

use super::backing::validate_payload;
use super::ensure_batch_len;
use super::managed::{NofManagedReadRequest, NofManagedWriteRequest};
use super::object::repeated_error;
use super::NofBackend;

pub(in crate::client) struct NofManagedStorageBackend {
    target: NofBackend,
}

impl NofManagedStorageBackend {
    pub(in crate::client) fn new(target: NofBackend) -> Self {
        Self { target }
    }

    fn read_request(&self, backing: &ColdBackingRoute) -> Result<NofManagedReadRequest> {
        Ok(NofManagedReadRequest {
            locator: super::managed::NofManagedLocator::from_hex(&backing.object_locator)?,
            length: backing.length,
            checksum: backing.checksum,
        })
    }

    fn write_objects(&self, writes: &[ColdObjectWrite<'_>]) -> Vec<Result<ColdBackingRoute>> {
        let mut results = (0..writes.len()).map(|_| None).collect::<Vec<_>>();
        let mut requests = Vec::with_capacity(writes.len());
        let mut indices = Vec::with_capacity(writes.len());
        for (index, write) in writes.iter().enumerate() {
            let request = validate_payload(write.cold_backing, write.payload).and_then(|_| {
                Ok(NofManagedWriteRequest {
                    locator: super::managed::NofManagedLocator::from_hex(
                        &write.cold_backing.object_locator,
                    )?,
                    value: write.payload,
                    checksum: write.cold_backing.checksum,
                })
            });
            match request {
                Ok(request) => {
                    indices.push(index);
                    requests.push(request);
                }
                Err(error) => results[index] = Some(Err(error)),
            }
        }
        if requests.is_empty() {
            return results
                .into_iter()
                .map(|result| {
                    result.unwrap_or_else(|| {
                        Err(StoreError::InvalidState(
                            "managed NoF write omitted a positional result".to_string(),
                        ))
                    })
                })
                .collect();
        }

        let writer = self
            .target
            .backing
            .managed_write()
            .expect("managed write capability was checked during construction");
        let statuses = writer.put_batch(&requests);
        let statuses = ensure_batch_len("managed put", requests.len(), statuses.len())
            .map(|_| statuses)
            .unwrap_or_else(|error| repeated_error(requests.len(), error));
        let flush = if statuses.iter().any(Result::is_ok) {
            Some(writer.flush())
        } else {
            None
        };
        for (index, status) in indices.into_iter().zip(statuses) {
            let write = &writes[index];
            results[index] = Some(match status {
                Ok(()) => match flush.as_ref() {
                    Some(Ok(())) => Ok(ColdBackingRoute {
                        state: ColdBackingState::Materialized,
                        ..write.cold_backing.clone()
                    }),
                    Some(Err(error)) => Err(error.clone()),
                    None => Err(StoreError::Transport(
                        "managed NoF flush was not attempted".to_string(),
                    )),
                },
                Err(error) => Err(error),
            });
        }
        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(StoreError::InvalidState(
                        "managed NoF write omitted a positional result".to_string(),
                    ))
                })
            })
            .collect()
    }
}

impl PersistentStorageBackend for NofManagedStorageBackend {
    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        self.target.health_snapshot()
    }

    fn put_object(&self, backing: &ColdBackingRoute, payload: &[u8]) -> Result<ColdBackingRoute> {
        self.write_objects(&[ColdObjectWrite {
            route: None,
            cold_backing: backing,
            payload,
        }])
        .into_iter()
        .next()
        .expect("single managed put result")
    }

    fn put_objects_batch_profiled(
        &self,
        writes: &[ColdObjectWrite<'_>],
        _record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<ColdBackingRoute>> {
        self.write_objects(writes)
    }

    fn get_object(&self, backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        let value = self
            .target
            .backing
            .managed_read()
            .expect("managed read capability was checked during construction")
            .get_batch(&[self.read_request(backing)?]);
        ensure_batch_len("managed get", 1, value.len())?;
        let value = value.into_iter().next().expect("length checked above")?;
        if let Some(value) = value.as_deref() {
            validate_payload(backing, value)?;
        }
        Ok(value)
    }

    fn query_object_length(&self, backing: &ColdBackingRoute) -> Result<Option<u64>> {
        Ok(Some(backing.length))
    }

    fn get_objects_into_batch(
        &self,
        reads: &mut [ColdObjectRead<'_, '_>],
    ) -> Vec<Result<Option<usize>>> {
        let requests = match reads
            .iter()
            .map(|read| self.read_request(read.cold_backing))
            .collect::<Result<Vec<_>>>()
        {
            Ok(requests) => requests,
            Err(error) => return repeated_error(reads.len(), error),
        };
        let values = self
            .target
            .backing
            .managed_read()
            .expect("managed read capability was checked during construction")
            .get_batch(&requests);
        if let Err(error) = ensure_batch_len("managed get", reads.len(), values.len()) {
            return repeated_error(reads.len(), error);
        }
        reads
            .iter_mut()
            .zip(values)
            .map(|(read, value)| {
                let Some(value) = value? else {
                    return Ok(None);
                };
                validate_payload(read.cold_backing, &value)?;
                if value.len() > read.dst.len() {
                    return Err(StoreError::InvalidState(format!(
                        "managed NoF value length {} exceeds destination length {}",
                        value.len(),
                        read.dst.len()
                    )));
                }
                read.dst[..value.len()].copy_from_slice(&value);
                Ok(Some(value.len()))
            })
            .collect()
    }

    fn delete_object(&self, backing: &ColdBackingRoute) -> Result<bool> {
        let results = self
            .target
            .backing
            .managed_allocator()
            .expect("managed allocator capability was checked during construction")
            .release_batch(&[self.read_request(backing)?]);
        ensure_batch_len("managed release", 1, results.len())?;
        results.into_iter().next().expect("length checked above")?;
        Ok(true)
    }

    fn put_pending_source(&self, _backing: &ColdBackingRoute, _payload: &[u8]) -> Result<()> {
        Ok(())
    }

    fn get_pending_source(&self, _backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    fn delete_pending_source(&self, _backing: &ColdBackingRoute) -> Result<bool> {
        Ok(false)
    }

    fn disable_pending_source(&self) -> bool {
        true
    }
}
