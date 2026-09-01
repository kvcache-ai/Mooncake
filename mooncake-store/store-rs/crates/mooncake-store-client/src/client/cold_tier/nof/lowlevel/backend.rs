//! NoF low-level adapter for the existing Cold Tier backend contract.

use std::borrow::Cow;
use std::collections::HashSet;

use mooncake_store_core::{ColdBackingRoute, ColdBackingState, ObjectRoute, Result, StoreError};

use crate::client::cold_tier::layout::ValueChunkPlan;
use crate::client::{
    ColdObjectWrite, PersistentStorageBackend, PersistentStorageBackendHealth,
    PersistentStorageManagement,
};

use super::super::ensure_batch_len;
use super::{
    repeated_error, NofLowLevelCapabilities, NofLowLevelDeleteRequest, NofLowLevelGetRequest,
    NofLowLevelTarget, OpaquePhysicalKey, PhysicalKeyInput,
};

const LOCATOR_PREFIX: &str = "nof-ll:v1:";
const ROOT_KEY_DOMAIN: &[u8] = b"root";
const CHUNK_KEY_DOMAIN: &[u8] = b"chunk";
const MANIFEST_MAGIC: &[u8; 8] = b"MCKNOF01";
const MANIFEST_VERSION: u8 = 1;
const MANIFEST_LEN: usize = 48;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LayoutKind {
    Inline,
    Chunked,
}

impl LayoutKind {
    fn marker(self) -> char {
        match self {
            Self::Inline => 'i',
            Self::Chunked => 'c',
        }
    }

    fn from_marker(marker: &str) -> Result<Self> {
        match marker {
            "i" => Ok(Self::Inline),
            "c" => Ok(Self::Chunked),
            _ => Err(StoreError::InvalidState(
                "NoF low-level locator has an unknown layout marker".to_string(),
            )),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ChunkManifest {
    logical_len: u64,
    checksum: Option<u64>,
    chunk_size: u64,
    chunk_count: u64,
}

impl ChunkManifest {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![0; MANIFEST_LEN];
        bytes[..8].copy_from_slice(MANIFEST_MAGIC);
        bytes[8] = MANIFEST_VERSION;
        bytes[9] = u8::from(self.checksum.is_some());
        bytes[16..24].copy_from_slice(&self.logical_len.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.checksum.unwrap_or_default().to_le_bytes());
        bytes[32..40].copy_from_slice(&self.chunk_size.to_le_bytes());
        bytes[40..48].copy_from_slice(&self.chunk_count.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8], route: &ColdBackingRoute) -> Result<Self> {
        if bytes.len() != MANIFEST_LEN || &bytes[..8] != MANIFEST_MAGIC {
            return Err(StoreError::InvalidState(
                "NoF low-level chunk manifest has invalid length or magic".to_string(),
            ));
        }
        if bytes[8] != MANIFEST_VERSION
            || bytes[9] & !1 != 0
            || bytes[10..16].iter().any(|byte| *byte != 0)
        {
            return Err(StoreError::InvalidState(
                "NoF low-level chunk manifest has unsupported flags or version".to_string(),
            ));
        }
        let read_u64 = |offset: usize| {
            let mut value = [0; 8];
            value.copy_from_slice(&bytes[offset..offset + 8]);
            u64::from_le_bytes(value)
        };
        let manifest = Self {
            logical_len: read_u64(16),
            checksum: (bytes[9] != 0).then(|| read_u64(24)),
            chunk_size: read_u64(32),
            chunk_count: read_u64(40),
        };
        if manifest.logical_len != route.length || manifest.checksum != route.checksum {
            return Err(StoreError::InvalidState(
                "NoF low-level manifest does not match Cold Tier route metadata".to_string(),
            ));
        }
        let plan = ValueChunkPlan::new(manifest.logical_len, manifest.chunk_size)?;
        if plan.chunk_count() != manifest.chunk_count || manifest.chunk_count == 0 {
            return Err(StoreError::InvalidState(
                "NoF low-level manifest has an inconsistent chunk count".to_string(),
            ));
        }
        Ok(manifest)
    }
}

struct PhysicalRecord<'a> {
    key: OpaquePhysicalKey,
    value: Cow<'a, [u8]>,
}

struct WriteLayout<'a> {
    kind: LayoutKind,
    root_key: OpaquePhysicalKey,
    data_records: Vec<PhysicalRecord<'a>>,
    root_record: Option<PhysicalRecord<'a>>,
}

impl WriteLayout<'_> {
    fn keys(&self) -> impl Iterator<Item = OpaquePhysicalKey> + '_ {
        self.data_records
            .iter()
            .map(|record| record.key.clone())
            .chain(self.root_record.iter().map(|record| record.key.clone()))
    }

    fn records(&self) -> impl Iterator<Item = &PhysicalRecord<'_>> {
        self.data_records.iter().chain(self.root_record.iter())
    }
}

pub(in crate::client) struct NofLowLevelBackend {
    device_id: String,
    target: NofLowLevelTarget,
    capabilities: NofLowLevelCapabilities,
}

impl NofLowLevelBackend {
    pub(in crate::client) fn new(
        device_id: impl Into<String>,
        target: NofLowLevelTarget,
    ) -> Result<Self> {
        let device_id = device_id.into();
        if device_id.is_empty() {
            return Err(StoreError::InvalidState(
                "NoF low-level device ID must not be empty".to_string(),
            ));
        }
        if target.executor.ownership().metadata
            != super::super::NofMetadataOwnership::ExternalMetadata
        {
            return Err(StoreError::InvalidState(
                "NoF provider-managed metadata must use the shared high-level metadata path"
                    .to_string(),
            ));
        }
        let capabilities = target.executor.capabilities().validate()?;
        Ok(Self {
            device_id,
            target,
            capabilities,
        })
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
            self.device_id.as_bytes(),
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

    fn chunk_key(&self, root_key: &OpaquePhysicalKey, index: u64) -> Result<OpaquePhysicalKey> {
        // The root key already contains the device identity. Deriving chunks from the root alone
        // also keeps existing locators readable when Cold Tier registers a device-ID alias.
        let fields: [&[u8]; 2] = [CHUNK_KEY_DOMAIN, root_key.as_bytes()];
        let key = self.target.key_codec.encode(PhysicalKeyInput {
            domain: &self.target.key_domain,
            fields: &fields,
            chunk_index: Some(index),
        })?;
        if key.as_bytes().is_empty() {
            return Err(StoreError::InvalidState(
                "NoF physical key codec returned an empty chunk key".to_string(),
            ));
        }
        Ok(key)
    }

    fn encode_locator(kind: LayoutKind, key: &OpaquePhysicalKey) -> String {
        format!("{LOCATOR_PREFIX}{}:{}", kind.marker(), key.to_hex())
    }

    fn decode_locator(locator: &str) -> Result<(LayoutKind, OpaquePhysicalKey)> {
        let encoded = locator.strip_prefix(LOCATOR_PREFIX).ok_or_else(|| {
            StoreError::InvalidState(format!(
                "NoF low-level locator has an unknown prefix: {locator}"
            ))
        })?;
        let mut fields = encoded.split(':');
        let marker = fields.next().unwrap_or_default();
        let key = fields.next().unwrap_or_default();
        if fields.next().is_some() {
            return Err(StoreError::InvalidState(
                "NoF low-level locator has trailing fields".to_string(),
            ));
        }
        let kind = LayoutKind::from_marker(marker)?;
        let key = decode_hex(key, "key", usize::MAX)?;
        Ok((kind, OpaquePhysicalKey::new(key)))
    }

    fn build_layout<'a>(
        &self,
        route: Option<&ObjectRoute>,
        cold_backing: &ColdBackingRoute,
        payload: &'a [u8],
    ) -> Result<WriteLayout<'a>> {
        validate_payload(cold_backing, payload)?;
        let root_key = self.root_key(route, cold_backing, payload)?;
        if cold_backing.length <= self.capabilities.max_value_size {
            return Ok(WriteLayout {
                kind: LayoutKind::Inline,
                root_key: root_key.clone(),
                data_records: vec![PhysicalRecord {
                    key: root_key,
                    value: Cow::Borrowed(payload),
                }],
                root_record: None,
            });
        }

        let plan = ValueChunkPlan::new(cold_backing.length, self.capabilities.max_value_size)?;
        if self.capabilities.max_value_size < MANIFEST_LEN as u64 {
            return Err(StoreError::InvalidState(format!(
                "NoF low-level value limit {} cannot hold the {MANIFEST_LEN}-byte chunk manifest",
                self.capabilities.max_value_size
            )));
        }
        let capacity = usize::try_from(plan.chunk_count()).map_err(|_| {
            StoreError::InvalidState("NoF chunk count does not fit this platform".to_string())
        })?;
        let mut data_records = Vec::with_capacity(capacity);
        for index in 0..plan.chunk_count() {
            let range = plan.range(index)?;
            let start = usize::try_from(range.start).map_err(|_| {
                StoreError::InvalidState("NoF chunk start does not fit usize".to_string())
            })?;
            let end = usize::try_from(range.end).map_err(|_| {
                StoreError::InvalidState("NoF chunk end does not fit usize".to_string())
            })?;
            data_records.push(PhysicalRecord {
                key: self.chunk_key(&root_key, index)?,
                value: Cow::Borrowed(&payload[start..end]),
            });
        }
        let manifest = ChunkManifest {
            logical_len: cold_backing.length,
            checksum: cold_backing.checksum,
            chunk_size: plan.max_chunk_len(),
            chunk_count: plan.chunk_count(),
        };
        Ok(WriteLayout {
            kind: LayoutKind::Chunked,
            root_key: root_key.clone(),
            data_records,
            root_record: Some(PhysicalRecord {
                key: root_key,
                value: Cow::Owned(manifest.encode()),
            }),
        })
    }

    fn materialized_route(
        &self,
        cold_backing: &ColdBackingRoute,
        layout: &WriteLayout<'_>,
    ) -> ColdBackingRoute {
        let mut materialized = cold_backing.clone();
        materialized.object_locator = Self::encode_locator(layout.kind, &layout.root_key);
        materialized.state = ColdBackingState::Materialized;
        materialized
    }

    fn put_records(&self, records: &[&PhysicalRecord<'_>]) -> Vec<Result<()>> {
        let sizes = records
            .iter()
            .map(|record| record.value.len() as u64)
            .collect::<Vec<_>>();
        let ranges = match bounded_ranges(&sizes, self.capabilities) {
            Ok(ranges) => ranges,
            Err(error) => return repeated_error(records.len(), error),
        };
        let mut results = Vec::with_capacity(records.len());
        for range in ranges {
            let requests = records[range.clone()]
                .iter()
                .map(|record| (record.key.clone(), record.value.as_ref()))
                .collect::<Vec<_>>();
            let batch = self.target.executor.put_batch(&requests);
            if let Err(error) = ensure_batch_len("low-level put", requests.len(), batch.len()) {
                results.extend(repeated_error(requests.len(), error));
            } else {
                results.extend(batch);
            }
        }
        results
    }

    fn get_records(&self, requests: &[NofLowLevelGetRequest]) -> Vec<Result<Option<Vec<u8>>>> {
        let sizes = requests
            .iter()
            .map(|request| request.expected_value_size as u64)
            .collect::<Vec<_>>();
        let ranges = match bounded_ranges(&sizes, self.capabilities) {
            Ok(ranges) => ranges,
            Err(error) => return repeated_error(requests.len(), error),
        };
        let mut results = Vec::with_capacity(requests.len());
        for range in ranges {
            let batch = self.target.executor.get_batch(&requests[range.clone()]);
            if let Err(error) = ensure_batch_len("low-level get", range.len(), batch.len()) {
                results.extend(repeated_error(range.len(), error));
            } else {
                results.extend(batch);
            }
        }
        results
    }

    fn delete_records(&self, requests: &[NofLowLevelDeleteRequest]) -> Vec<Result<()>> {
        let sizes = requests
            .iter()
            .map(|request| request.key.as_bytes().len() as u64)
            .collect::<Vec<_>>();
        let ranges = match bounded_ranges(&sizes, self.capabilities) {
            Ok(ranges) => ranges,
            Err(error) => return repeated_error(requests.len(), error),
        };
        let mut results = Vec::with_capacity(requests.len());
        for range in ranges {
            let batch = self.target.executor.delete_batch(&requests[range.clone()]);
            if let Err(error) = ensure_batch_len("low-level delete", range.len(), batch.len()) {
                results.extend(repeated_error(range.len(), error));
            } else {
                results.extend(batch);
            }
        }
        results
    }

    fn cleanup_records(&self, records: &[&PhysicalRecord<'_>]) {
        let requests = records
            .iter()
            .map(|record| NofLowLevelDeleteRequest {
                key: record.key.clone(),
            })
            .collect::<Vec<_>>();
        let _ = self.delete_records(&requests);
        let _ = self.target.executor.flush();
    }

    fn put_objects(&self, writes: &[ColdObjectWrite<'_>]) -> Vec<Result<ColdBackingRoute>> {
        let mut results = (0..writes.len()).map(|_| None).collect::<Vec<_>>();
        let mut layouts = Vec::new();
        let mut seen_keys = HashSet::new();
        for (index, write) in writes.iter().enumerate() {
            let layout = match self.build_layout(write.route, write.cold_backing, write.payload) {
                Ok(layout) => layout,
                Err(error) => {
                    results[index] = Some(Err(error));
                    continue;
                }
            };
            let layout_keys = layout.keys().collect::<Vec<_>>();
            let layout_unique = layout_keys.iter().collect::<HashSet<_>>();
            if layout_unique.len() != layout_keys.len()
                || layout_keys.iter().any(|key| seen_keys.contains(key))
            {
                results[index] = Some(Err(StoreError::Conflict(
                    "NoF write batch contains duplicate physical keys".to_string(),
                )));
                continue;
            }
            seen_keys.extend(layout_keys);
            layouts.push((index, layout));
        }

        let data_positions = layouts
            .iter()
            .enumerate()
            .flat_map(|(layout_index, (write_index, layout))| {
                layout
                    .data_records
                    .iter()
                    .enumerate()
                    .map(move |(record_index, _)| (layout_index, *write_index, record_index))
            })
            .collect::<Vec<_>>();
        let data_results = {
            let records = data_positions
                .iter()
                .map(|(layout_index, _, record_index)| {
                    &layouts[*layout_index].1.data_records[*record_index]
                })
                .collect::<Vec<_>>();
            self.put_records(&records)
        };
        let mut layout_ok = vec![true; writes.len()];
        for ((_, write_index, _), status) in data_positions.into_iter().zip(data_results) {
            if let Err(error) = status {
                layout_ok[write_index] = false;
                if results[write_index].is_none() {
                    results[write_index] = Some(Err(error));
                }
            }
        }
        for (index, layout) in &layouts {
            if !layout_ok[*index] {
                self.cleanup_records(&layout.records().collect::<Vec<_>>());
            }
        }

        let root_positions = layouts
            .iter()
            .enumerate()
            .filter(|(_, (write_index, _))| layout_ok[*write_index])
            .filter_map(|(layout_index, (write_index, layout))| {
                layout
                    .root_record
                    .as_ref()
                    .map(|_| (layout_index, *write_index))
            })
            .collect::<Vec<_>>();
        let root_results = {
            let roots = root_positions
                .iter()
                .map(|(layout_index, _)| {
                    layouts[*layout_index]
                        .1
                        .root_record
                        .as_ref()
                        .expect("root positions contain only chunked layouts")
                })
                .collect::<Vec<_>>();
            self.put_records(&roots)
        };
        for ((_, write_index), status) in root_positions.into_iter().zip(root_results) {
            if let Err(error) = status {
                layout_ok[write_index] = false;
                results[write_index] = Some(Err(error));
            }
        }
        for (index, layout) in &layouts {
            if !layout_ok[*index] && layout.root_record.is_some() {
                self.cleanup_records(&layout.records().collect::<Vec<_>>());
            }
        }

        let successful = layouts
            .iter()
            .filter(|(index, _)| layout_ok[*index])
            .collect::<Vec<_>>();
        if !successful.is_empty() {
            if let Err(error) = self.target.executor.flush() {
                let cleanup = successful
                    .iter()
                    .flat_map(|(_, layout)| layout.records())
                    .collect::<Vec<_>>();
                self.cleanup_records(&cleanup);
                for (index, _) in successful {
                    layout_ok[*index] = false;
                    results[*index] = Some(Err(StoreError::Transport(format!(
                        "NoF low-level flush failed before route publication: {error}"
                    ))));
                }
            }
        }
        for (index, layout) in &layouts {
            if layout_ok[*index] && results[*index].is_none() {
                results[*index] = Some(Ok(
                    self.materialized_route(writes[*index].cold_backing, layout)
                ));
            }
        }
        results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(StoreError::Transport(
                        "NoF low-level write omitted a positional result".to_string(),
                    ))
                })
            })
            .collect()
    }

    fn get_one(&self, cold_backing: &ColdBackingRoute) -> Result<Option<Vec<u8>>> {
        let (kind, root_key) = Self::decode_locator(&cold_backing.object_locator)?;
        let root_hint = match kind {
            LayoutKind::Inline => usize::try_from(cold_backing.length).map_err(|_| {
                StoreError::InvalidState("NoF inline length does not fit usize".to_string())
            })?,
            LayoutKind::Chunked => MANIFEST_LEN,
        };
        let mut root_result = self.get_records(&[NofLowLevelGetRequest {
            key: root_key.clone(),
            expected_value_size: root_hint,
        }]);
        let Some(root) = root_result.pop().ok_or_else(|| {
            StoreError::Transport("NoF root read returned no result".to_string())
        })??
        else {
            return Ok(None);
        };
        if kind == LayoutKind::Inline {
            validate_payload(cold_backing, &root)?;
            return Ok(Some(root));
        }

        let manifest = ChunkManifest::decode(&root, cold_backing)?;
        let plan = ValueChunkPlan::new(manifest.logical_len, manifest.chunk_size)?;
        let mut requests =
            Vec::with_capacity(usize::try_from(plan.chunk_count()).map_err(|_| {
                StoreError::InvalidState("NoF chunk count does not fit usize".to_string())
            })?);
        for index in 0..plan.chunk_count() {
            let range = plan.range(index)?;
            let length = range
                .end
                .checked_sub(range.start)
                .ok_or_else(|| StoreError::InvalidState("NoF chunk range underflow".to_string()))?;
            requests.push(NofLowLevelGetRequest {
                key: self.chunk_key(&root_key, index)?,
                expected_value_size: usize::try_from(length).map_err(|_| {
                    StoreError::InvalidState("NoF chunk length does not fit usize".to_string())
                })?,
            });
        }
        let chunks = self.get_records(&requests);
        let capacity = usize::try_from(manifest.logical_len).map_err(|_| {
            StoreError::InvalidState("NoF logical length does not fit usize".to_string())
        })?;
        let mut payload = Vec::with_capacity(capacity);
        for (request, chunk) in requests.iter().zip(chunks) {
            let chunk = chunk?.ok_or_else(|| {
                StoreError::InvalidState(
                    "NoF chunk is missing while its root manifest is visible".to_string(),
                )
            })?;
            if chunk.len() != request.expected_value_size {
                return Err(StoreError::InvalidState(format!(
                    "NoF chunk length {} does not match manifest length {}",
                    chunk.len(),
                    request.expected_value_size
                )));
            }
            payload.extend_from_slice(&chunk);
        }
        validate_payload(cold_backing, &payload)?;
        Ok(Some(payload))
    }

    fn delete_one(&self, cold_backing: &ColdBackingRoute) -> Result<bool> {
        let (kind, root_key) = Self::decode_locator(&cold_backing.object_locator)?;
        let root_hint = if kind == LayoutKind::Chunked {
            MANIFEST_LEN
        } else {
            usize::try_from(cold_backing.length).map_err(|_| {
                StoreError::InvalidState("NoF inline length does not fit usize".to_string())
            })?
        };
        let mut root_result = self.get_records(&[NofLowLevelGetRequest {
            key: root_key.clone(),
            expected_value_size: root_hint,
        }]);
        let Some(root) = root_result.pop().ok_or_else(|| {
            StoreError::Transport("NoF delete lookup returned no result".to_string())
        })??
        else {
            return Ok(false);
        };
        if kind == LayoutKind::Chunked {
            let manifest = ChunkManifest::decode(&root, cold_backing)?;
            let mut chunk_requests =
                Vec::with_capacity(usize::try_from(manifest.chunk_count).map_err(|_| {
                    StoreError::InvalidState("NoF chunk count does not fit usize".to_string())
                })?);
            for index in 0..manifest.chunk_count {
                chunk_requests.push(NofLowLevelDeleteRequest {
                    key: self.chunk_key(&root_key, index)?,
                });
            }
            for result in self.delete_records(&chunk_requests) {
                if let Err(error) = result {
                    if !matches!(error, StoreError::NotFound(_)) {
                        return Err(error);
                    }
                }
            }
        } else {
            validate_payload(cold_backing, &root)?;
        }
        match self
            .delete_records(&[NofLowLevelDeleteRequest { key: root_key }])
            .pop()
        {
            Some(Ok(())) | Some(Err(StoreError::NotFound(_))) => {
                self.target.executor.flush()?;
                Ok(true)
            }
            Some(Err(error)) => Err(error),
            None => Err(StoreError::Transport(
                "NoF root delete returned no result".to_string(),
            )),
        }
    }
}

impl PersistentStorageBackend for NofLowLevelBackend {
    fn storage_management(&self) -> PersistentStorageManagement {
        match self.target.executor.ownership().maintenance {
            super::super::NofStorageMaintenance::ProviderManaged => {
                PersistentStorageManagement::BackendManaged
            }
            super::super::NofStorageMaintenance::MooncakeManaged => {
                PersistentStorageManagement::MooncakeManaged
            }
        }
    }

    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        let health = self
            .target
            .executor
            .storage_health()?
            .validate(self.target.executor.ownership())?;
        Ok(PersistentStorageBackendHealth {
            capacity_bytes: health.capacity_bytes,
            available_bytes: health.available_bytes,
        })
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
        self.put_objects(&[ColdObjectWrite {
            route,
            cold_backing,
            payload,
        }])
        .pop()
        .ok_or_else(|| StoreError::Transport("NoF single put returned no result".to_string()))?
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

fn validate_payload(route: &ColdBackingRoute, payload: &[u8]) -> Result<()> {
    let actual_len = u64::try_from(payload.len())
        .map_err(|_| StoreError::InvalidState("NoF payload length does not fit u64".to_string()))?;
    if actual_len != route.length {
        return Err(StoreError::InvalidState(format!(
            "NoF payload length {actual_len} does not match route length {}",
            route.length
        )));
    }
    if let Some(expected) = route.checksum {
        let actual = crate::client::payload_checksum(payload);
        if actual != expected {
            return Err(StoreError::InvalidState(format!(
                "NoF payload checksum {actual} does not match route checksum {expected}"
            )));
        }
    }
    Ok(())
}

fn decode_hex(encoded: &str, field: &str, max_bytes: usize) -> Result<Vec<u8>> {
    if encoded.is_empty()
        || !encoded.len().is_multiple_of(2)
        || !encoded.is_ascii()
        || encoded.len() / 2 > max_bytes
    {
        return Err(StoreError::InvalidState(format!(
            "NoF low-level locator has invalid hex {field} length"
        )));
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for offset in (0..encoded.len()).step_by(2) {
        bytes.push(
            u8::from_str_radix(&encoded[offset..offset + 2], 16).map_err(|_| {
                StoreError::InvalidState(format!(
                    "NoF low-level locator contains non-hex {field} bytes"
                ))
            })?,
        );
    }
    Ok(bytes)
}

fn bounded_ranges(
    item_sizes: &[u64],
    capabilities: NofLowLevelCapabilities,
) -> Result<Vec<std::ops::Range<usize>>> {
    if item_sizes.is_empty() {
        return Ok(Vec::new());
    }
    let mut ranges = Vec::new();
    let mut start = 0;
    let mut bytes = 0u64;
    for (index, size) in item_sizes.iter().copied().enumerate() {
        if size > capabilities.max_batch_bytes {
            return Err(StoreError::InvalidState(format!(
                "NoF request size {size} exceeds executor batch byte limit {}",
                capabilities.max_batch_bytes
            )));
        }
        let exceeds_items = index - start >= capabilities.max_batch_items;
        let exceeds_bytes = bytes
            .checked_add(size)
            .is_none_or(|total| total > capabilities.max_batch_bytes);
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
        NofDeviceManagement, NofLowLevelExecutor, NofMetadataOwnership, NofOwnership,
        NofStorageMaintenance, PhysicalKeyCodec, PhysicalKeyInput,
    };
    use mooncake_store_core::{ClientEpoch, ClientRuntimeId, ClientStableId};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct MemoryExecutor {
        values: Mutex<HashMap<OpaquePhysicalKey, Vec<u8>>>,
        fail_manifest: Mutex<bool>,
        provider_metadata: bool,
    }

    impl NofLowLevelExecutor for MemoryExecutor {
        fn capabilities(&self) -> NofLowLevelCapabilities {
            NofLowLevelCapabilities {
                max_value_size: 64,
                max_batch_items: 64,
                max_batch_bytes: u64::MAX,
            }
        }

        fn ownership(&self) -> NofOwnership {
            NofOwnership {
                metadata: if self.provider_metadata {
                    NofMetadataOwnership::ProviderManaged
                } else {
                    NofMetadataOwnership::ExternalMetadata
                },
                maintenance: NofStorageMaintenance::ProviderManaged,
                devices: NofDeviceManagement::ProviderManaged,
            }
        }

        fn put_batch(&self, requests: &[(OpaquePhysicalKey, &[u8])]) -> Vec<Result<()>> {
            let mut values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|(key, value)| {
                    if value.starts_with(MANIFEST_MAGIC)
                        && std::mem::take(&mut *self.fail_manifest.lock().unwrap())
                    {
                        return Err(StoreError::Transport(
                            "injected manifest failure".to_string(),
                        ));
                    }
                    values.insert(key.clone(), value.to_vec());
                    Ok(())
                })
                .collect()
        }

        fn get_batch(&self, requests: &[NofLowLevelGetRequest]) -> Vec<Result<Option<Vec<u8>>>> {
            let values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|request| Ok(values.get(&request.key).cloned()))
                .collect()
        }

        fn delete_batch(&self, requests: &[NofLowLevelDeleteRequest]) -> Vec<Result<()>> {
            let mut values = self.values.lock().unwrap();
            requests
                .iter()
                .map(|request| {
                    if values.remove(&request.key).is_some() {
                        Ok(())
                    } else {
                        Err(StoreError::NotFound("test key".to_string()))
                    }
                })
                .collect()
        }
    }

    fn route(payload: &[u8]) -> ColdBackingRoute {
        ColdBackingRoute {
            owner: ClientRuntimeId {
                stable_id: ClientStableId::new("owner"),
                epoch: ClientEpoch(1),
            },
            cold_tier_id: "nof-test".to_string(),
            object_locator: "logical@v1".to_string(),
            length: payload.len() as u64,
            checksum: Some(crate::client::payload_checksum(payload)),
            state: ColdBackingState::PendingOffload,
            replicas: Vec::new(),
        }
    }

    #[test]
    fn chunks_large_values_and_reassembles_them() {
        let executor = Arc::new(MemoryExecutor::default());
        let backend =
            NofLowLevelBackend::new("nof-test", NofLowLevelTarget::new(executor.clone())).unwrap();
        let payload = vec![7; 150];
        let materialized = backend.put_object(&route(&payload), &payload).unwrap();
        assert!(materialized.object_locator.starts_with("nof-ll:v1:c:"));
        assert_eq!(backend.get_object(&materialized).unwrap().unwrap(), payload);
        assert!(backend.delete_object(&materialized).unwrap());
        assert!(executor.values.lock().unwrap().is_empty());
    }

    #[test]
    fn provider_metadata_reuses_high_level_path() {
        let executor = Arc::new(MemoryExecutor {
            provider_metadata: true,
            ..MemoryExecutor::default()
        });
        assert!(matches!(
            NofLowLevelBackend::new("nof-test", NofLowLevelTarget::new(executor)),
            Err(StoreError::InvalidState(message))
                if message.contains("shared high-level metadata path")
        ));
    }

    #[test]
    fn manifest_failure_cleans_unpublished_chunks() {
        let executor = Arc::new(MemoryExecutor::default());
        *executor.fail_manifest.lock().unwrap() = true;
        let backend =
            NofLowLevelBackend::new("nof-test", NofLowLevelTarget::new(executor.clone())).unwrap();
        let payload = vec![3; 150];
        assert!(backend.put_object(&route(&payload), &payload).is_err());
        assert!(executor.values.lock().unwrap().is_empty());
    }

    #[test]
    fn malformed_locator_fails_closed() {
        let executor = Arc::new(MemoryExecutor::default());
        let backend =
            NofLowLevelBackend::new("nof-test", NofLowLevelTarget::new(executor)).unwrap();
        let mut backing = route(b"value");
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
    fn codec_collision_is_rejected_before_any_physical_write() {
        let executor = Arc::new(MemoryExecutor::default());
        let target = NofLowLevelTarget::new(executor.clone()).key_codec(Arc::new(CollidingCodec));
        let backend = NofLowLevelBackend::new("nof-test", target).unwrap();
        let payload = vec![9; 150];
        assert!(matches!(
            backend.put_object(&route(&payload), &payload),
            Err(StoreError::Conflict(_))
        ));
        assert!(executor.values.lock().unwrap().is_empty());
    }
}
