//! Cold tier persistent storage backend.
//!
//! This module defines the binary payload format, the `PersistentStorageBackend` trait, and the
//! `ColdTierBackendResolver` that maps device IDs to backend implementations. Concrete backends
//! (`LocalDirPersistentStorageBackend`, `ExtentStoreColdBackend`) are defined in submodules
//! that share the codec and trait definitions via `use super::*`.
//!
//! # Binary payload format (`.bin`)
//!
//! Every cold tier object is stored as a self-describing binary file. Two wire versions exist:
//!
//! **Version 1** — payload only (36-byte header):
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────┐
//! │  0..8   magic          "MCKCOLD\0"                                  │
//! │  8..10  version        u16 LE = 1                                   │
//! │ 10..12  reserved       u16 LE = 0                                   │
//! │ 12..16  header_len     u32 LE = 36                                  │
//! │ 16..24  payload_len    u64 LE                                       │
//! │ 24..26  checksum_kind  u16 LE (0 = none, 1 = xxh3)                 │
//! │ 26..28  reserved       u16 LE = 0                                   │
//! │ 28..36  checksum       u64 LE (xxh3 of payload, or 0)              │
//! │ 36..    payload bytes                                               │
//! └──────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! **Version 2** — payload + embedded manifest (44-byte header):
//!
//! ```text
//! │  0..36  same as v1 (version field = 2, header_len = 44)            │
//! │ 36..40  manifest_len   u32 LE                                      │
//! │ 40..44  reserved       u32 LE = 0                                  │
//! │ 44..44+manifest_len   manifest bytes (len-prefixed fields)         │
//! │ rest    payload bytes                                               │
//! ```
//!
//! The manifest encodes enough route metadata (`ObjectKey`, `NamespaceScope`, `RouteVersion`,
//! `cold_tier_id`, `object_locator`) for reverse recovery — reconstructing a route from disk
//! when the metadata backend is lost or stale.
//!
//! # Architecture
//!
//! ```text
//! ColdTierTargetConfig (startup)
//!         │
//!    resolve_cold_tier_target()
//!         │
//!    ResolvedColdTierTarget
//!         │
//!    construct backend (LocalDir / ExtentStore)
//!         │
//!    ColdTierBackendResolver { device_id → Arc<dyn PersistentStorageBackend> }
//!         │
//!    stored in StorageOwnerState
//!         │
//!    used by offload / restore / GC paths (later PRs)
//! ```

use super::*;

const COLD_OBJECT_MANIFEST_SCHEMA_VERSION: u16 = 1;

/// Metadata extracted from a cold payload header (length + optional xxh3 checksum).
#[derive(Clone, Debug, Eq, PartialEq)]
struct ColdPayloadMetadata {
    length: u64,
    checksum: Option<u64>,
}

/// Route metadata embedded in a version-2 cold payload for reverse recovery.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ColdObjectManifest {
    key: ObjectKey,
    namespace: Option<NamespaceScope>,
    logical_key: Option<String>,
    canonical_key: Option<String>,
    sharing_scope: Option<String>,
    qos_tier: Option<String>,
    route_version: RouteVersion,
    cold_tier_id: String,
    object_locator: String,
    length: u64,
    checksum: Option<u64>,
}

/// A cold object recovered from disk during startup scan.
#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveredColdObject {
    manifest: ColdObjectManifest,
    path: std::path::PathBuf,
    metadata: ColdPayloadMetadata,
}

fn write_backend_payload_atomically(
    path: &std::path::Path,
    encoded: &[u8],
    label: &str,
) -> Result<()> {
    static TEMPFILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    let nonce = TEMPFILE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let temp_path = path.with_extension(format!(
        "tmp-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        nonce,
    ));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .map_err(|error| {
            StoreError::Transport(format!(
                "failed to create {label} temp file {}: {error}",
                temp_path.display()
            ))
        })?;
    use std::io::Write;
    if let Err(error) = file.write_all(encoded) {
        drop(file);
        let _ = std::fs::remove_file(&temp_path);
        return Err(StoreError::Transport(format!(
            "failed to write {label} temp file {}: {error}",
            temp_path.display()
        )));
    }
    if let Err(error) = file.sync_all() {
        drop(file);
        let _ = std::fs::remove_file(&temp_path);
        return Err(StoreError::Transport(format!(
            "failed to sync {label} temp file {}: {error}",
            temp_path.display()
        )));
    }
    drop(file);
    if let Err(error) = std::fs::rename(&temp_path, path) {
        let _ = std::fs::remove_file(&temp_path);
        return Err(StoreError::Transport(format!(
            "failed to persist {label} {} via {}: {error}",
            path.display(),
            temp_path.display()
        )));
    }
    let parent = path.parent().ok_or_else(|| {
        StoreError::InvalidState(format!("{label} path is missing parent directory"))
    })?;
    let parent_dir = std::fs::File::open(parent).map_err(|error| {
        StoreError::Transport(format!(
            "failed to open {label} parent directory {}: {error}",
            parent.display()
        ))
    })?;
    parent_dir.sync_all().map_err(|error| {
        StoreError::Transport(format!(
            "failed to sync {label} parent directory {}: {error}",
            parent.display()
        ))
    })
}

fn write_len_prefixed_string(encoded: &mut Vec<u8>, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).map_err(|_| {
        StoreError::InvalidState(format!(
            "cold object manifest field is too large: {}",
            value.len()
        ))
    })?;
    encoded.extend_from_slice(&len.to_le_bytes());
    encoded.extend_from_slice(value.as_bytes());
    Ok(())
}

fn read_len_prefixed_string(cursor: &mut &[u8], field: &str) -> Result<String> {
    if cursor.len() < 4 {
        return Err(StoreError::InvalidState(format!(
            "cold object manifest is missing {field} length"
        )));
    }
    let len = u32::from_le_bytes([cursor[0], cursor[1], cursor[2], cursor[3]]) as usize;
    *cursor = &cursor[4..];
    if cursor.len() < len {
        return Err(StoreError::InvalidState(format!(
            "cold object manifest {field} length {len} exceeds remaining bytes {}",
            cursor.len()
        )));
    }
    let value = std::str::from_utf8(&cursor[..len]).map_err(|error| {
        StoreError::InvalidState(format!("cold object manifest {field} is not utf8: {error}"))
    })?;
    *cursor = &cursor[len..];
    Ok(value.to_string())
}

fn write_optional_len_prefixed_string(encoded: &mut Vec<u8>, value: Option<&str>) -> Result<()> {
    match value {
        Some(value) => {
            encoded.push(1);
            write_len_prefixed_string(encoded, value)
        }
        None => {
            encoded.push(0);
            Ok(())
        }
    }
}

fn read_optional_len_prefixed_string(cursor: &mut &[u8], field: &str) -> Result<Option<String>> {
    if cursor.is_empty() {
        return Err(StoreError::InvalidState(format!(
            "cold object manifest is missing {field} option tag"
        )));
    }
    let tag = cursor[0];
    *cursor = &cursor[1..];
    match tag {
        0 => Ok(None),
        1 => read_len_prefixed_string(cursor, field).map(Some),
        other => Err(StoreError::InvalidState(format!(
            "cold object manifest {field} has invalid option tag {other}"
        ))),
    }
}

fn write_optional_namespace(
    encoded: &mut Vec<u8>,
    namespace: Option<&NamespaceScope>,
) -> Result<()> {
    match namespace {
        Some(namespace) => {
            encoded.push(1);
            write_len_prefixed_string(encoded, &namespace.tenant)?;
            write_len_prefixed_string(encoded, &namespace.domain)?;
            write_len_prefixed_string(encoded, &namespace.object_set)
        }
        None => {
            encoded.push(0);
            Ok(())
        }
    }
}

fn read_optional_namespace(cursor: &mut &[u8]) -> Result<Option<NamespaceScope>> {
    if cursor.is_empty() {
        return Err(StoreError::InvalidState(
            "cold object manifest is missing namespace option tag".to_string(),
        ));
    }
    let tag = cursor[0];
    *cursor = &cursor[1..];
    match tag {
        0 => Ok(None),
        1 => Ok(Some(NamespaceScope {
            tenant: read_len_prefixed_string(cursor, "namespace.tenant")?,
            domain: read_len_prefixed_string(cursor, "namespace.domain")?,
            object_set: read_len_prefixed_string(cursor, "namespace.object_set")?,
        })),
        other => Err(StoreError::InvalidState(format!(
            "cold object manifest namespace has invalid option tag {other}"
        ))),
    }
}

fn encode_cold_object_manifest(manifest: &ColdObjectManifest) -> Result<Vec<u8>> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&COLD_OBJECT_MANIFEST_SCHEMA_VERSION.to_le_bytes());
    write_len_prefixed_string(&mut encoded, &manifest.key.0)?;
    write_optional_namespace(&mut encoded, manifest.namespace.as_ref())?;
    write_optional_len_prefixed_string(&mut encoded, manifest.logical_key.as_deref())?;
    write_optional_len_prefixed_string(&mut encoded, manifest.canonical_key.as_deref())?;
    write_optional_len_prefixed_string(&mut encoded, manifest.sharing_scope.as_deref())?;
    write_optional_len_prefixed_string(&mut encoded, manifest.qos_tier.as_deref())?;
    encoded.extend_from_slice(&manifest.route_version.0.to_le_bytes());
    write_len_prefixed_string(&mut encoded, &manifest.cold_tier_id)?;
    write_len_prefixed_string(&mut encoded, &manifest.object_locator)?;
    encoded.extend_from_slice(&manifest.length.to_le_bytes());
    match manifest.checksum {
        Some(checksum) => {
            encoded.push(1);
            encoded.extend_from_slice(&checksum.to_le_bytes());
        }
        None => encoded.push(0),
    }
    Ok(encoded)
}

fn decode_cold_object_manifest(mut encoded: &[u8]) -> Result<ColdObjectManifest> {
    if encoded.len() < 2 {
        return Err(StoreError::InvalidState(
            "cold object manifest is missing schema version".to_string(),
        ));
    }
    let schema_version = u16::from_le_bytes([encoded[0], encoded[1]]);
    if schema_version != COLD_OBJECT_MANIFEST_SCHEMA_VERSION {
        return Err(StoreError::InvalidState(format!(
            "cold object manifest has unsupported schema version {schema_version}"
        )));
    }
    encoded = &encoded[2..];
    let key = ObjectKey::new(read_len_prefixed_string(&mut encoded, "key")?);
    let namespace = read_optional_namespace(&mut encoded)?;
    let logical_key = read_optional_len_prefixed_string(&mut encoded, "logical_key")?;
    let canonical_key = read_optional_len_prefixed_string(&mut encoded, "canonical_key")?;
    let sharing_scope = read_optional_len_prefixed_string(&mut encoded, "sharing_scope")?;
    let qos_tier = read_optional_len_prefixed_string(&mut encoded, "qos_tier")?;
    if encoded.len() < 8 {
        return Err(StoreError::InvalidState(
            "cold object manifest is missing route version".to_string(),
        ));
    }
    let route_version = RouteVersion(u64::from_le_bytes([
        encoded[0], encoded[1], encoded[2], encoded[3], encoded[4], encoded[5], encoded[6],
        encoded[7],
    ]));
    encoded = &encoded[8..];
    let cold_tier_id = read_len_prefixed_string(&mut encoded, "cold_tier_id")?;
    let object_locator = read_len_prefixed_string(&mut encoded, "object_locator")?;
    if encoded.len() < 9 {
        return Err(StoreError::InvalidState(
            "cold object manifest is missing length/checksum".to_string(),
        ));
    }
    let length = u64::from_le_bytes([
        encoded[0], encoded[1], encoded[2], encoded[3], encoded[4], encoded[5], encoded[6],
        encoded[7],
    ]);
    encoded = &encoded[8..];
    let checksum = match encoded[0] {
        0 => {
            encoded = &encoded[1..];
            None
        }
        1 => {
            if encoded.len() < 9 {
                return Err(StoreError::InvalidState(
                    "cold object manifest checksum is truncated".to_string(),
                ));
            }
            let checksum = u64::from_le_bytes([
                encoded[1], encoded[2], encoded[3], encoded[4], encoded[5], encoded[6], encoded[7],
                encoded[8],
            ]);
            encoded = &encoded[9..];
            Some(checksum)
        }
        other => {
            return Err(StoreError::InvalidState(format!(
                "cold object manifest has invalid checksum tag {other}"
            )))
        }
    };
    if !encoded.is_empty() {
        return Err(StoreError::InvalidState(format!(
            "cold object manifest has {} trailing bytes",
            encoded.len()
        )));
    }
    Ok(ColdObjectManifest {
        key,
        namespace,
        logical_key,
        canonical_key,
        sharing_scope,
        qos_tier,
        route_version,
        cold_tier_id,
        object_locator,
        length,
        checksum,
    })
}

fn manifest_from_route(
    route: &ObjectRoute,
    cold_backing: &mooncake_store_core::ColdBackingRoute,
) -> ColdObjectManifest {
    ColdObjectManifest {
        key: route.key.clone(),
        namespace: route.namespace.clone(),
        logical_key: route.logical_key.clone(),
        canonical_key: route.canonical_key.clone(),
        sharing_scope: route.sharing_scope.clone(),
        qos_tier: route.qos_tier.clone(),
        route_version: route.version.next(),
        cold_tier_id: cold_backing.cold_tier_id.clone(),
        object_locator: cold_backing.object_locator.clone(),
        length: cold_backing.length,
        checksum: cold_backing.checksum,
    }
}

/// A batched cold tier write request: route (for manifest embedding), backing descriptor, and
/// the raw payload bytes.
pub(super) struct ColdObjectWrite<'a> {
    route: Option<&'a ObjectRoute>,
    cold_backing: &'a mooncake_store_core::ColdBackingRoute,
    payload: &'a [u8],
}

/// A batched cold tier read request that copies the payload into a caller-provided buffer.
pub(super) struct ColdObjectRead<'a, 'b> {
    cold_backing: &'a mooncake_store_core::ColdBackingRoute,
    dst: &'b mut [u8],
}

/// A batched cold tier read request that returns an owned or pinned payload without copying.
pub(super) struct ColdObjectPinnedRead<'a> {
    cold_backing: &'a mooncake_store_core::ColdBackingRoute,
}

/// Reference-counted handle to a cold tier payload that avoids copying when the backend can
/// return a pinned buffer (e.g. memory-mapped extent store segments).
pub struct ColdPayloadRef {
    data: Arc<dyn ColdPayloadRefData>,
}

impl ColdPayloadRef {
    fn new(data: Arc<dyn ColdPayloadRefData>) -> Self {
        Self { data }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data.as_slice()
    }
}

impl AsRef<[u8]> for ColdPayloadRef {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for ColdPayloadRef {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

trait ColdPayloadRefData: Send + Sync {
    fn as_slice(&self) -> &[u8];
}

/// Cold tier read result — either a zero-copy pinned reference or an owned buffer.
pub enum ColdObjectPayload {
    Borrowed(ColdPayloadRef),
    Owned(Vec<u8>),
}

impl ColdObjectPayload {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(payload) => payload.as_slice(),
            Self::Owned(payload) => payload.as_slice(),
        }
    }

    pub fn into_owned(self) -> Vec<u8> {
        match self {
            Self::Borrowed(payload) => payload.as_slice().to_vec(),
            Self::Owned(payload) => payload,
        }
    }
}

impl AsRef<[u8]> for ColdObjectPayload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

/// Health snapshot returned by `PersistentStorageBackend::health()`.
#[derive(Clone, Debug)]
pub(super) struct PersistentStorageBackendHealth {
    /// Total filesystem capacity in bytes.
    capacity_bytes: Option<u64>,
    /// Available (non-privileged) filesystem space in bytes.
    available_bytes: Option<u64>,
}

/// Result of a compaction pass on a cold tier backend.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BackendCompactionResult {
    pub(crate) supported: bool,
    pub(crate) candidate_bytes: u64,
    pub(crate) reclaimed_bytes: u64,
}

/// Aggregate storage statistics collected during a maintenance sweep.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct BackendMaintenanceStats {
    pub(crate) object_count: u64,
    pub(crate) object_bytes: u64,
    pub(crate) pending_source_count: u64,
    pub(crate) pending_source_bytes: u64,
    pub(crate) orphan_quarantine_count: u64,
    pub(crate) orphan_quarantine_bytes: u64,
    pub(crate) extent_live_bytes: u64,
    pub(crate) extent_dead_bytes: u64,
    pub(crate) extent_segment_count: u64,
}

/// Trait for cold tier persistent storage I/O.
///
/// Each implementation maps a `ColdBackingRoute` (device ID + object locator) to durable storage.
/// The trait provides default batch implementations that delegate to single-object methods;
/// backends like `ExtentStore` override these for batched I/O.
///
/// Write atomicity: `put_object` must be atomic (temp-file + rename on POSIX backends) so that
/// a crash never leaves a half-written payload visible to readers.
///
/// Pending sources: `put_pending_source` / `get_pending_source` support a staging area for
/// offload payloads that have been written to disk but not yet CAS'd into the route. Backends
/// that do not need staging (extent store) can override `disable_pending_source()` to return
/// `true`.
pub(super) trait PersistentStorageBackend: Send + Sync {
    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        Ok(PersistentStorageBackendHealth {
            capacity_bytes: None,
            available_bytes: None,
        })
    }

    fn maintenance_stats(&self) -> Result<BackendMaintenanceStats> {
        Ok(BackendMaintenanceStats::default())
    }

    fn compact(&self, _max_candidate_bytes: u64) -> Result<BackendCompactionResult> {
        Ok(BackendCompactionResult::default())
    }

    #[allow(dead_code)]
    fn profiling_counters(&self) -> Vec<(&'static str, u64)> {
        Vec::new()
    }

    fn put_object(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> Result<mooncake_store_core::ColdBackingRoute>;

    fn put_object_with_route(
        &self,
        route: Option<&ObjectRoute>,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> Result<mooncake_store_core::ColdBackingRoute> {
        let _ = route;
        self.put_object(cold_backing, payload)
    }

    fn put_objects_batch(
        &self,
        writes: &[ColdObjectWrite<'_>],
    ) -> Vec<Result<mooncake_store_core::ColdBackingRoute>> {
        self.put_objects_batch_profiled(writes, &mut |_, _| {})
    }

    fn put_objects_batch_profiled(
        &self,
        writes: &[ColdObjectWrite<'_>],
        _record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<mooncake_store_core::ColdBackingRoute>> {
        writes
            .iter()
            .map(|write| self.put_object_with_route(write.route, write.cold_backing, write.payload))
            .collect()
    }

    fn get_object(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<Vec<u8>>>;

    fn get_object_pinned(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<ColdObjectPayload>> {
        Ok(self.get_object(cold_backing)?.map(ColdObjectPayload::Owned))
    }

    fn get_objects_pinned_batch(
        &self,
        reads: &[ColdObjectPinnedRead<'_>],
    ) -> Vec<Result<Option<ColdObjectPayload>>> {
        reads
            .iter()
            .map(|read| self.get_object_pinned(read.cold_backing))
            .collect()
    }

    fn get_object_into(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        dst: &mut [u8],
    ) -> Result<Option<usize>> {
        let Some(payload) = self.get_object(cold_backing)? else {
            return Ok(None);
        };
        if payload.len() > dst.len() {
            return Err(StoreError::InvalidState(format!(
                "cold tier payload {} length {} exceeds destination length {}",
                cold_backing.object_locator,
                payload.len(),
                dst.len()
            )));
        }
        dst[..payload.len()].copy_from_slice(&payload);
        Ok(Some(payload.len()))
    }

    fn get_objects_into_batch(
        &self,
        reads: &mut [ColdObjectRead<'_, '_>],
    ) -> Vec<Result<Option<usize>>> {
        reads
            .iter_mut()
            .map(|read| self.get_object_into(read.cold_backing, read.dst))
            .collect()
    }

    fn get_objects_into_batch_profiled(
        &self,
        reads: &mut [ColdObjectRead<'_, '_>],
        _record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<Option<usize>>> {
        self.get_objects_into_batch(reads)
    }

    fn delete_object(&self, cold_backing: &mooncake_store_core::ColdBackingRoute) -> Result<bool>;

    fn delete_objects_batch(
        &self,
        cold_backings: &[&mooncake_store_core::ColdBackingRoute],
    ) -> Vec<Result<bool>> {
        cold_backings
            .iter()
            .map(|cold_backing| self.delete_object(cold_backing))
            .collect()
    }

    fn put_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> Result<()>;
    fn get_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<Vec<u8>>>;
    fn delete_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<bool>;
    fn disable_pending_source(&self) -> bool {
        false
    }
}

/// Maps cold tier device IDs to their `PersistentStorageBackend` implementations.
///
/// Constructed at startup from `ColdTierTargetConfig` entries (or dynamically via Admin HTTP).
/// Offload and restore paths call `backend_for(&cold_backing)` to obtain the correct backend
/// for a given `ColdBackingRoute`.
pub(super) struct ColdTierBackendResolver {
    backends: BTreeMap<String, Arc<dyn PersistentStorageBackend>>,
}

impl ColdTierBackendResolver {
    #[cfg(test)]
    fn local_dirs<I>(
        default_cold_tier_id: String,
        backend: Arc<dyn PersistentStorageBackend>,
        cold_tier_ids: I,
    ) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let mut backends = BTreeMap::new();
        for cold_tier_id in cold_tier_ids {
            backends.insert(cold_tier_id, backend.clone());
        }
        backends
            .entry(default_cold_tier_id)
            .or_insert_with(|| backend.clone());
        Self { backends }
    }

    pub(super) fn from_handles(
        _default_cold_tier_id: String,
        backends: BTreeMap<String, Arc<dyn PersistentStorageBackend>>,
    ) -> Self {
        Self { backends }
    }

    #[cfg(test)]
    fn single_local_dir(cold_tier_id: String, backend: Arc<dyn PersistentStorageBackend>) -> Self {
        Self::local_dirs(cold_tier_id.clone(), backend, [cold_tier_id])
    }

    pub(super) fn has_backend(&self, cold_tier_id: &str) -> bool {
        self.backends.contains_key(cold_tier_id)
    }

    pub(super) fn backend_ids(&self) -> Vec<String> {
        self.backends.keys().cloned().collect()
    }

    pub(super) fn insert_backend(
        &mut self,
        cold_tier_id: String,
        backend: Arc<dyn PersistentStorageBackend>,
    ) {
        self.backends.insert(cold_tier_id, backend);
    }

    pub(super) fn backend_for(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Arc<dyn PersistentStorageBackend>> {
        self.backends
            .get(&cold_backing.cold_tier_id)
            .cloned()
            .ok_or_else(|| {
                StoreError::InvalidState(format!(
                    "cold tier backend {} is not registered",
                    cold_backing.cold_tier_id
                ))
            })
    }
}

pub(super) fn cold_tier_backend_from_device_record(
    device: &ColdTierDeviceRecord,
) -> Result<Arc<dyn PersistentStorageBackend>> {
    let root_dir = match &device.target {
        mooncake_store_core::ColdTierTargetSpec::Directory { path } => {
            std::path::PathBuf::from(path)
        }
        mooncake_store_core::ColdTierTargetSpec::Uuid { uuid } => device
            .root_dir
            .as_ref()
            .map(std::path::PathBuf::from)
            .map(Ok)
            .unwrap_or_else(|| resolve_mount_point_from_uuid(uuid, &device.device_id))?,
    };
    Ok(Arc::new(LocalDirPersistentStorageBackend::new_with_root(
        root_dir,
    )))
}

/// A cold tier target after validation and mount resolution (UUID → directory).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ResolvedColdTierTarget {
    pub(crate) cold_tier_id: String,
    pub(crate) kind: ColdTierKind,
    pub(crate) target: mooncake_store_core::ColdTierTargetSpec,
    pub(crate) root_dir: std::path::PathBuf,
    pub(crate) ssd_engine: ColdTierSsdEngine,
    pub(crate) capacity_override_bytes: Option<u64>,
    pub(crate) tags: Vec<String>,
}

/// One parsed entry from `/proc/self/mountinfo`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MountInfoEntry {
    pub(crate) mount_point: std::path::PathBuf,
    pub(crate) mount_source: String,
    pub(crate) fs_type: String,
}

#[path = "local_dir_cold_backend.rs"]
mod local_dir_cold_backend;
pub(crate) use local_dir_cold_backend::LocalDirPersistentStorageBackend;

pub(super) fn default_cold_tier_root() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "mooncake-store-client-cold-backend-{}",
        std::process::id()
    ))
}

pub(super) fn resolve_cold_tier_target(
    config: &ColdTierTargetConfig,
) -> Result<ResolvedColdTierTarget> {
    validate_cold_tier_id(&config.cold_tier_id)?;
    let (target, root_dir) = match &config.target {
        ColdTierTarget::Directory(directory) => {
            let root_dir = ensure_backend_directory(directory, &config.cold_tier_id)?;
            (
                mooncake_store_core::ColdTierTargetSpec::Directory {
                    path: root_dir.display().to_string(),
                },
                root_dir,
            )
        }
        ColdTierTarget::Uuid(uuid) => {
            let resolved = resolve_mount_point_from_uuid(uuid, &config.cold_tier_id)?;
            let root_dir = ensure_backend_directory(&resolved, &config.cold_tier_id)?;
            (
                mooncake_store_core::ColdTierTargetSpec::Uuid {
                    uuid: uuid.trim().to_string(),
                },
                root_dir,
            )
        }
    };
    validate_backend_kind_mount(&root_dir, config.kind, &config.cold_tier_id)?;
    Ok(ResolvedColdTierTarget {
        cold_tier_id: config.cold_tier_id.clone(),
        kind: config.kind,
        target,
        root_dir,
        ssd_engine: config.ssd_engine,
        capacity_override_bytes: config.capacity_override_bytes,
        tags: config.tags.clone(),
    })
}

fn validate_cold_tier_id(cold_tier_id: &str) -> Result<()> {
    if cold_tier_id.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "cold tier cold_tier_id must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn cold_tier_device_id(cold_backing: &mooncake_store_core::ColdBackingRoute) -> &str {
    cold_backing.cold_tier_id.as_str()
}

fn cold_tier_kind_name(kind: ColdTierKind) -> &'static str {
    match kind {
        ColdTierKind::Ssd => "ssd",
        ColdTierKind::Nfs => "nfs",
    }
}

pub(super) fn find_existing_device_by_target(
    metadata: &dyn MetadataBackend,
    resolved: &ResolvedColdTierTarget,
) -> Result<Option<mooncake_store_core::ColdTierDeviceRecord>> {
    let devices =
        metadata.list_cold_tier_devices(&mooncake_store_core::ColdTierDeviceFilter::default())?;
    Ok(devices
        .into_iter()
        .find(|d| d.target == resolved.target && d.device_id != resolved.cold_tier_id))
}

pub(super) fn bootstrap_cold_tier_device(
    metadata: &dyn MetadataBackend,
    runtime: &ClientRuntimeId,
    resolved: &ResolvedColdTierTarget,
) -> Result<()> {
    let device_id = resolved.cold_tier_id.clone();
    let record = mooncake_store_core::ColdTierDeviceRecord {
        device_id: device_id.clone(),
        stable_id: runtime.stable_id.0.clone(),
        epoch: Some(runtime.epoch.0),
        cold_tier_id: resolved.cold_tier_id.clone(),
        kind: cold_tier_kind_name(resolved.kind).to_string(),
        target: resolved.target.clone(),
        root_dir: Some(resolved.root_dir.display().to_string()),
        state: mooncake_store_core::ColdTierDeviceState::Healthy,
        capacity_bytes: resolved.capacity_override_bytes,
        used_bytes: 0,
        reserved_bytes: 0,
        failure_count: 0,
        last_error: None,
        tags: resolved.tags.clone(),
        updated_at_ms: super::current_time_ms(),
    };
    match metadata.put_cold_tier_device_if_absent(&record)? {
        mooncake_store_core::ColdTierPutDeviceResult::Created(_) => Ok(()),
        mooncake_store_core::ColdTierPutDeviceResult::Existing(existing) => {
            if existing.kind != record.kind || existing.target != record.target {
                return Err(StoreError::Conflict(format!(
                    "cold tier bootstrap device {} already exists with a different definition",
                    resolved.cold_tier_id
                )));
            }
            let mut update =
                mooncake_store_core::ColdTierDeviceUpdate::new(super::current_time_ms());
            update.expected_updated_at_ms = Some(existing.updated_at_ms);
            update.stable_id = Some(runtime.stable_id.0.clone());
            update.epoch = Some(Some(runtime.epoch.0));
            update.root_dir = Some(Some(resolved.root_dir.display().to_string()));
            update.state = Some(mooncake_store_core::ColdTierDeviceState::Healthy);
            update.capacity_bytes = Some(resolved.capacity_override_bytes);
            update.tags = Some(resolved.tags.clone());
            update.last_error = Some(None);
            metadata.update_cold_tier_device(&device_id, update)?;
            Ok(())
        }
    }
}

/// Returns `(total_capacity_bytes, available_bytes)` for the filesystem containing `directory`.
fn filesystem_capacity_bytes(directory: &std::path::Path) -> Result<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(directory.as_os_str().as_bytes()).map_err(|_| {
        StoreError::InvalidState(format!(
            "cold tier path {} contains an interior nul byte",
            directory.display()
        ))
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return Err(StoreError::Transport(format!(
            "failed to stat filesystem capacity for {}: {}",
            directory.display(),
            std::io::Error::last_os_error()
        )));
    }
    let stats = unsafe { stats.assume_init() };
    let total = stats.f_blocks.saturating_mul(stats.f_frsize);
    let available = stats.f_bavail.saturating_mul(stats.f_frsize);
    Ok((total, available))
}

fn ensure_backend_directory(
    directory: &std::path::Path,
    cold_tier_id: &str,
) -> Result<std::path::PathBuf> {
    reject_unsafe_cold_tier_directory(directory, cold_tier_id)?;
    if directory.exists() {
        let metadata = std::fs::symlink_metadata(directory).map_err(|error| {
            StoreError::Transport(format!(
                "failed to stat cold tier directory {} for {}: {error}",
                directory.display(),
                cold_tier_id
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(StoreError::InvalidState(format!(
                "cold tier {} path {} must not be a symlink",
                cold_tier_id,
                directory.display()
            )));
        }
        if !metadata.is_dir() {
            return Err(StoreError::InvalidState(format!(
                "cold tier {} path {} is not a directory",
                cold_tier_id,
                directory.display()
            )));
        }
    } else {
        std::fs::create_dir_all(directory).map_err(|error| {
            StoreError::Transport(format!(
                "failed to create cold tier directory {} for {}: {error}",
                directory.display(),
                cold_tier_id
            ))
        })?;
    }
    let canonical = std::fs::canonicalize(directory).map_err(|error| {
        StoreError::Transport(format!(
            "failed to canonicalize cold tier directory {} for {}: {error}",
            directory.display(),
            cold_tier_id
        ))
    })?;
    reject_unsafe_cold_tier_directory(&canonical, cold_tier_id)?;
    Ok(canonical)
}

fn reject_unsafe_cold_tier_directory(
    directory: &std::path::Path,
    cold_tier_id: &str,
) -> Result<()> {
    if !directory.is_absolute() {
        return Err(StoreError::InvalidState(format!(
            "cold tier {} directory {} must be absolute",
            cold_tier_id,
            directory.display()
        )));
    }
    if directory == std::path::Path::new("/") {
        return Err(StoreError::InvalidState(format!(
            "cold tier {} directory must not be filesystem root",
            cold_tier_id
        )));
    }
    for component in directory.components() {
        if matches!(component, std::path::Component::ParentDir) {
            return Err(StoreError::InvalidState(format!(
                "cold tier {} directory {} must not contain parent components",
                cold_tier_id,
                directory.display()
            )));
        }
    }
    Ok(())
}

fn resolve_mount_point_from_uuid(uuid: &str, cold_tier_id: &str) -> Result<std::path::PathBuf> {
    let uuid = uuid.trim();
    if uuid.is_empty() {
        return Err(StoreError::InvalidState(format!(
            "cold tier {} uuid must not be empty",
            cold_tier_id
        )));
    }
    let by_uuid = std::path::Path::new("/dev/disk/by-uuid").join(uuid);
    let device = std::fs::canonicalize(&by_uuid).map_err(|error| {
        StoreError::Transport(format!(
            "failed to resolve cold tier {} uuid {} via {}: {error}",
            cold_tier_id,
            uuid,
            by_uuid.display()
        ))
    })?;
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").map_err(|error| {
        StoreError::Transport(format!(
            "failed to read /proc/self/mountinfo while resolving cold tier {}: {error}",
            cold_tier_id
        ))
    })?;
    let mounts = parse_mountinfo(&mountinfo)?;
    find_mount_point_for_device(&device, &mounts).ok_or_else(|| {
        StoreError::InvalidState(format!(
            "cold tier {} uuid {} resolved to {}, but no mounted filesystem was found",
            cold_tier_id,
            uuid,
            device.display()
        ))
    })
}

fn validate_backend_kind_mount(
    root_dir: &std::path::Path,
    kind: ColdTierKind,
    cold_tier_id: &str,
) -> Result<()> {
    if kind != ColdTierKind::Nfs {
        return Ok(());
    }
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").map_err(|error| {
        StoreError::Transport(format!(
            "failed to read /proc/self/mountinfo while validating cold tier {}: {error}",
            cold_tier_id
        ))
    })?;
    let mounts = parse_mountinfo(&mountinfo)?;
    let Some(entry) = find_mount_for_path(root_dir, &mounts) else {
        return Err(StoreError::InvalidState(format!(
            "cold tier {} path {} is not on a mounted filesystem",
            cold_tier_id,
            root_dir.display()
        )));
    };
    if matches!(entry.fs_type.as_str(), "nfs" | "nfs4") {
        return Ok(());
    }
    Err(StoreError::InvalidState(format!(
        "cold tier {} requires an nfs mount, but {} is on {} ({})",
        cold_tier_id,
        root_dir.display(),
        entry.mount_point.display(),
        entry.fs_type
    )))
}

pub(crate) fn parse_mountinfo(contents: &str) -> Result<Vec<MountInfoEntry>> {
    let mut entries = Vec::new();
    for line in contents.lines() {
        if line.trim().is_empty() {
            continue;
        }
        entries.push(parse_mountinfo_line(line)?);
    }
    Ok(entries)
}

fn parse_mountinfo_line(line: &str) -> Result<MountInfoEntry> {
    let Some((left, right)) = line.split_once(" - ") else {
        return Err(StoreError::InvalidState(format!(
            "invalid mountinfo line missing separator: {line}"
        )));
    };
    let left_fields: Vec<&str> = left.split_whitespace().collect();
    if left_fields.len() < 5 {
        return Err(StoreError::InvalidState(format!(
            "invalid mountinfo line missing fields: {line}"
        )));
    }
    let right_fields: Vec<&str> = right.split_whitespace().collect();
    if right_fields.len() < 2 {
        return Err(StoreError::InvalidState(format!(
            "invalid mountinfo line missing filesystem fields: {line}"
        )));
    }
    Ok(MountInfoEntry {
        mount_point: std::path::PathBuf::from(decode_mountinfo_field(left_fields[4])),
        fs_type: right_fields[0].to_string(),
        mount_source: decode_mountinfo_field(right_fields[1]),
    })
}

fn decode_mountinfo_field(value: &str) -> String {
    let mut decoded = String::with_capacity(value.len());
    let mut chars = value.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let mut octal = String::new();
            for _ in 0..3 {
                match chars.peek() {
                    Some(next) if next.is_ascii_digit() => octal.push(chars.next().unwrap()),
                    _ => break,
                }
            }
            if octal.len() == 3 {
                if let Ok(byte) = u8::from_str_radix(&octal, 8) {
                    decoded.push(byte as char);
                    continue;
                }
            }
            decoded.push('\\');
            decoded.push_str(&octal);
            continue;
        }
        decoded.push(ch);
    }
    decoded
}

pub(crate) fn find_mount_point_for_device(
    device: &std::path::Path,
    mounts: &[MountInfoEntry],
) -> Option<std::path::PathBuf> {
    mounts
        .iter()
        .filter(|entry| entry.mount_source == device.to_string_lossy())
        .max_by_key(|entry| entry.mount_point.as_os_str().len())
        .map(|entry| entry.mount_point.clone())
}

pub(crate) fn find_mount_for_path<'a>(
    path: &std::path::Path,
    mounts: &'a [MountInfoEntry],
) -> Option<&'a MountInfoEntry> {
    mounts
        .iter()
        .filter(|entry| path.starts_with(&entry.mount_point))
        .max_by_key(|entry| entry.mount_point.as_os_str().len())
}

fn encode_backend_component(value: &str) -> String {
    if value.is_empty() {
        return "~".to_string();
    }
    let mut component = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'-') {
            component.push(*byte as char);
        } else {
            component.push('~');
            component.push_str(&format!("{byte:02X}"));
        }
    }
    match component.as_str() {
        "." => "~2E".to_string(),
        ".." => "~2E~2E".to_string(),
        _ => component,
    }
}

#[cfg(test)]
mod cold_tier_storage_backend_tests {
    use super::*;

    struct TestTempDir(std::path::PathBuf);
    impl TestTempDir {
        fn new(path: std::path::PathBuf) -> Self {
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("test temp dir should be creatable");
            Self(path)
        }
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TestTempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn write_backend_payload_atomically_creates_file() {
        let root = TestTempDir::new(default_cold_tier_root().join("atomic-write-test"));
        let path = root.path().join("subdir").join("payload.bin");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let data = b"atomic payload content";
        write_backend_payload_atomically(&path, data, "test").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), data);
    }

    #[test]
    fn encode_backend_component_handles_special_cases() {
        assert_eq!(encode_backend_component(""), "~");
        assert_eq!(encode_backend_component("."), "~2E");
        assert_eq!(encode_backend_component(".."), "~2E~2E");
        assert_eq!(encode_backend_component("normal-id.1"), "normal-id.1");
        assert_eq!(encode_backend_component("a/b"), "a~2Fb");
        assert_eq!(encode_backend_component("a:b"), "a~3Ab");
    }

    #[test]
    fn backend_resolver_returns_error_for_unregistered_device() {
        let backend = Arc::new(LocalDirPersistentStorageBackend::new_with_root(
            std::path::PathBuf::from("/tmp/resolver-test"),
        ));
        let resolver =
            ColdTierBackendResolver::single_local_dir("registered-device".to_string(), backend);
        let cold_backing = mooncake_store_core::ColdBackingRoute {
            owner: mooncake_store_core::ClientRuntimeId::new(
                "resolver",
                mooncake_store_core::ClientEpoch(1),
            ),
            cold_tier_id: "unregistered-device".to_string(),
            object_locator: "obj".to_string(),
            length: 10,
            checksum: None,
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: Vec::new(),
        };
        let result = resolver.backend_for(&cold_backing);
        match result {
            Err(e) => assert!(e.to_string().contains("not registered")),
            Ok(_) => panic!("should fail for unregistered device"),
        }
    }

    #[test]
    fn backend_resolver_returns_backend_for_registered_device() {
        let root = TestTempDir::new(default_cold_tier_root().join("resolver-registered"));
        let backend = Arc::new(LocalDirPersistentStorageBackend::new_with_root(root.path()));
        let resolver = ColdTierBackendResolver::single_local_dir("my-device".to_string(), backend);
        let cold_backing = mooncake_store_core::ColdBackingRoute {
            owner: mooncake_store_core::ClientRuntimeId::new(
                "resolver",
                mooncake_store_core::ClientEpoch(1),
            ),
            cold_tier_id: "my-device".to_string(),
            object_locator: "obj".to_string(),
            length: 10,
            checksum: None,
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: Vec::new(),
        };
        assert!(resolver.backend_for(&cold_backing).is_ok());
    }

    #[test]
    fn backend_resolver_has_backend_check() {
        let backend = Arc::new(LocalDirPersistentStorageBackend::new_with_root(
            std::path::PathBuf::from("/tmp/has-backend-test"),
        ));
        let resolver = ColdTierBackendResolver::single_local_dir("present".to_string(), backend);
        assert!(resolver.has_backend("present"));
        assert!(!resolver.has_backend("absent"));
    }

    #[test]
    fn backend_resolver_insert_backend_adds_new_device() {
        let backend = Arc::new(LocalDirPersistentStorageBackend::new_with_root(
            std::path::PathBuf::from("/tmp/insert-backend-test"),
        ));
        let mut resolver =
            ColdTierBackendResolver::single_local_dir("initial".to_string(), backend.clone());
        assert!(!resolver.has_backend("added"));
        resolver.insert_backend("added".to_string(), backend);
        assert!(resolver.has_backend("added"));
    }

    #[test]
    fn decode_mountinfo_field_handles_octal_escapes() {
        // \040 = space (octal 40)
        let decoded = decode_mountinfo_field("hello\\040world");
        assert_eq!(decoded, "hello world");
    }

    #[test]
    fn decode_mountinfo_field_handles_no_escapes() {
        let decoded = decode_mountinfo_field("/mnt/data");
        assert_eq!(decoded, "/mnt/data");
    }

    #[test]
    fn decode_mountinfo_field_handles_incomplete_octal() {
        // Only 2 digits after backslash — should preserve the backslash and digits
        let decoded = decode_mountinfo_field("path\\04end");
        assert_eq!(decoded, "path\\04end");
    }

    #[test]
    fn reject_unsafe_cold_tier_directory_rejects_relative() {
        let result =
            reject_unsafe_cold_tier_directory(std::path::Path::new("relative/path"), "test-device");
        assert!(result.is_err());
    }

    #[test]
    fn reject_unsafe_cold_tier_directory_rejects_root() {
        let result = reject_unsafe_cold_tier_directory(std::path::Path::new("/"), "test-device");
        assert!(result.is_err());
    }

    #[test]
    fn reject_unsafe_cold_tier_directory_rejects_parent_dir() {
        let result =
            reject_unsafe_cold_tier_directory(std::path::Path::new("/data/../etc"), "test-device");
        assert!(result.is_err());
    }

    #[test]
    fn reject_unsafe_cold_tier_directory_accepts_valid() {
        let result = reject_unsafe_cold_tier_directory(
            std::path::Path::new("/data/cold-tier"),
            "test-device",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn validate_cold_tier_id_rejects_empty() {
        assert!(validate_cold_tier_id("").is_err());
        assert!(validate_cold_tier_id("   ").is_err());
    }

    #[test]
    fn validate_cold_tier_id_accepts_normal() {
        assert!(validate_cold_tier_id("ssd-device-01").is_ok());
    }

    // ---- ColdTierTargetSpec → ColdTierTargetConfig TryFrom tests ----

    #[test]
    fn cold_tier_target_spec_try_from_rejects_empty_id() {
        let spec = ColdTierTargetSpec {
            cold_tier_id: "".to_string(),
            kind: ColdTierKind::Ssd,
            directory: Some("/data".into()),
            uuid: None,
            ssd_engine: None,
            capacity_override_bytes: None,
            tags: Vec::new(),
        };
        let result = ColdTierTargetConfig::try_from(spec);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must not be empty"));
    }

    #[test]
    fn cold_tier_target_spec_try_from_rejects_no_directory_no_uuid() {
        let spec = ColdTierTargetSpec {
            cold_tier_id: "dev1".to_string(),
            kind: ColdTierKind::Ssd,
            directory: None,
            uuid: None,
            ssd_engine: None,
            capacity_override_bytes: None,
            tags: Vec::new(),
        };
        let result = ColdTierTargetConfig::try_from(spec);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must configure one of"));
    }

    #[test]
    fn cold_tier_target_spec_try_from_rejects_both_directory_and_uuid() {
        let spec = ColdTierTargetSpec {
            cold_tier_id: "dev1".to_string(),
            kind: ColdTierKind::Ssd,
            directory: Some("/data".into()),
            uuid: Some("abc-123".to_string()),
            ssd_engine: None,
            capacity_override_bytes: None,
            tags: Vec::new(),
        };
        let result = ColdTierTargetConfig::try_from(spec);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("exactly one of"));
    }

    #[test]
    fn cold_tier_target_spec_try_from_rejects_nfs_with_uuid() {
        let spec = ColdTierTargetSpec {
            cold_tier_id: "dev1".to_string(),
            kind: ColdTierKind::Nfs,
            directory: None,
            uuid: Some("abc-123".to_string()),
            ssd_engine: None,
            capacity_override_bytes: None,
            tags: Vec::new(),
        };
        let result = ColdTierTargetConfig::try_from(spec);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not support uuid"));
    }

    #[test]
    fn cold_tier_target_spec_try_from_rejects_non_ssd_with_extent_store_engine() {
        let spec = ColdTierTargetSpec {
            cold_tier_id: "dev1".to_string(),
            kind: ColdTierKind::Nfs,
            directory: Some("/data".into()),
            uuid: None,
            ssd_engine: Some(ColdTierSsdEngine::ExtentStore),
            capacity_override_bytes: None,
            tags: Vec::new(),
        };
        let result = ColdTierTargetConfig::try_from(spec);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("can only configure ssd_engine for ssd"));
    }

    #[test]
    fn cold_tier_target_spec_try_from_accepts_valid_directory() {
        let spec = ColdTierTargetSpec {
            cold_tier_id: "dev1".to_string(),
            kind: ColdTierKind::Ssd,
            directory: Some("/data/cold".into()),
            uuid: None,
            ssd_engine: None,
            capacity_override_bytes: Some(1024),
            tags: vec!["fast".to_string()],
        };
        let config = ColdTierTargetConfig::try_from(spec).unwrap();
        assert_eq!(config.cold_tier_id, "dev1");
        assert_eq!(config.kind, ColdTierKind::Ssd);
        assert_eq!(config.ssd_engine, ColdTierSsdEngine::LocalDir);
        assert_eq!(config.capacity_override_bytes, Some(1024));
        assert_eq!(config.tags, vec!["fast".to_string()]);
    }

    // ---- ColdTierTargetConfig builder chain tests ----

    #[test]
    fn cold_tier_target_config_builder_chain() {
        let config = ColdTierTargetConfig::directory("dev1", ColdTierKind::Ssd, "/data")
            .ssd_engine(ColdTierSsdEngine::ExtentStore)
            .capacity_override_bytes(4096)
            .tags(["fast", "nvme"]);
        assert_eq!(config.cold_tier_id, "dev1");
        assert_eq!(config.kind, ColdTierKind::Ssd);
        assert_eq!(config.ssd_engine, ColdTierSsdEngine::ExtentStore);
        assert_eq!(config.capacity_override_bytes, Some(4096));
        assert_eq!(config.tags, vec!["fast".to_string(), "nvme".to_string()]);
    }

    #[test]
    fn cold_tier_target_config_uuid_constructor() {
        let config = ColdTierTargetConfig::uuid("dev2", ColdTierKind::Ssd, "abc-123-def");
        assert_eq!(config.cold_tier_id, "dev2");
        assert!(matches!(
            config.target,
            ColdTierTarget::Uuid(ref uuid) if uuid == "abc-123-def"
        ));
    }

    #[test]
    fn cold_tier_target_config_extent_store_shortcut() {
        let config = ColdTierTargetConfig::directory("dev3", ColdTierKind::Ssd, "/data")
            .extent_store_engine();
        assert_eq!(config.ssd_engine, ColdTierSsdEngine::ExtentStore);
    }

    #[test]
    fn cold_tier_target_config_capacity_override_clamps_to_one() {
        let config = ColdTierTargetConfig::directory("dev4", ColdTierKind::Ssd, "/data")
            .capacity_override_bytes(0);
        assert_eq!(config.capacity_override_bytes, Some(1));
    }

    // ---- filesystem_capacity_bytes smoke test ----

    #[test]
    fn filesystem_capacity_bytes_returns_total_and_available() {
        let root = TestTempDir::new(default_cold_tier_root().join("capacity-smoke"));
        let (total, available) = filesystem_capacity_bytes(root.path()).unwrap();
        assert!(total > 0);
        assert!(available > 0);
        assert!(available <= total);
    }

    #[test]
    fn cold_tier_backend_from_device_record_resolves_directory_target() {
        let root = TestTempDir::new(default_cold_tier_root().join("backend-from-record"));
        let device = mooncake_store_core::ColdTierDeviceRecord {
            device_id: "test-dir-device".to_string(),
            stable_id: "stable-1".to_string(),
            epoch: Some(1),
            cold_tier_id: "test-dir-device".to_string(),
            kind: "ssd".to_string(),
            target: mooncake_store_core::ColdTierTargetSpec::Directory {
                path: root.path().display().to_string(),
            },
            root_dir: None,
            state: mooncake_store_core::ColdTierDeviceState::Healthy,
            capacity_bytes: None,
            used_bytes: 0,
            reserved_bytes: 0,
            failure_count: 0,
            last_error: None,
            tags: vec![],
            updated_at_ms: 0,
        };
        let backend = cold_tier_backend_from_device_record(&device);
        assert!(backend.is_ok());
    }
}
