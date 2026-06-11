//! LocalDir persistent cold tier backend.
//!
//! File-per-object cold tier backend backed by a local directory tree. The MCKCOLD binary format
//! (magic + header + optional manifest + payload) is specific to this backend. ExtentStore uses
//! its own extent-based storage and only shares the manifest codec defined in the parent module.

use super::*;

const COLD_OBJECT_MAGIC: &[u8; 8] = b"MCKCOLD\0";
const COLD_OBJECT_VERSION: u16 = 1;
const COLD_OBJECT_MANIFEST_VERSION: u16 = 2;
const COLD_OBJECT_HEADER_LEN: usize = 36;
const COLD_OBJECT_MANIFEST_HEADER_LEN: usize = 44;
const COLD_OBJECT_CHECKSUM_NONE: u16 = 0;
const COLD_OBJECT_CHECKSUM_XXH3: u16 = 1;

fn encode_backend_payload(
    payload: &[u8],
    expected_length: u64,
    checksum: Option<u64>,
) -> Result<Vec<u8>> {
    encode_backend_payload_with_manifest(payload, expected_length, checksum, None)
}

fn encode_backend_payload_with_manifest(
    payload: &[u8],
    expected_length: u64,
    checksum: Option<u64>,
    manifest: Option<&ColdObjectManifest>,
) -> Result<Vec<u8>> {
    if payload.len() as u64 != expected_length {
        return Err(StoreError::InvalidState(format!(
            "backend payload length mismatch before encode: expected {} actual {}",
            expected_length,
            payload.len()
        )));
    }
    if let Some(expected) = checksum {
        let actual = payload_checksum(payload);
        if actual != expected {
            return Err(StoreError::InvalidState(format!(
                "backend payload checksum mismatch before encode: expected {} actual {}",
                expected, actual
            )));
        }
    }

    let checksum_kind = if checksum.is_some() {
        COLD_OBJECT_CHECKSUM_XXH3
    } else {
        COLD_OBJECT_CHECKSUM_NONE
    };
    let manifest_bytes = manifest.map(encode_cold_object_manifest).transpose()?;
    let version = if manifest_bytes.is_some() {
        COLD_OBJECT_MANIFEST_VERSION
    } else {
        COLD_OBJECT_VERSION
    };
    let header_len = if manifest_bytes.is_some() {
        COLD_OBJECT_MANIFEST_HEADER_LEN
    } else {
        COLD_OBJECT_HEADER_LEN
    };
    let manifest_len = manifest_bytes
        .as_ref()
        .map(|bytes| bytes.len())
        .unwrap_or(0);
    let manifest_len_u32 = u32::try_from(manifest_len).map_err(|_| {
        StoreError::InvalidState(format!("cold object manifest is too large: {manifest_len}"))
    })?;
    let mut encoded = Vec::with_capacity(header_len + manifest_len + payload.len());
    encoded.extend_from_slice(COLD_OBJECT_MAGIC);
    encoded.extend_from_slice(&version.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&(header_len as u32).to_le_bytes());
    encoded.extend_from_slice(&expected_length.to_le_bytes());
    encoded.extend_from_slice(&checksum_kind.to_le_bytes());
    encoded.extend_from_slice(&0u16.to_le_bytes());
    encoded.extend_from_slice(&checksum.unwrap_or(0).to_le_bytes());
    if let Some(manifest_bytes) = manifest_bytes {
        encoded.extend_from_slice(&manifest_len_u32.to_le_bytes());
        encoded.extend_from_slice(&0u32.to_le_bytes());
        encoded.extend_from_slice(&manifest_bytes);
    }
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

fn cold_object_payload_metadata(
    path: &std::path::Path,
    label: &str,
    encoded: &[u8],
) -> Result<(u64, Option<u64>)> {
    cold_object_payload_metadata_and_manifest(path, label, encoded)
        .map(|(metadata, _)| (metadata.length, metadata.checksum))
}

fn cold_object_payload_metadata_and_manifest(
    path: &std::path::Path,
    label: &str,
    encoded: &[u8],
) -> Result<(ColdPayloadMetadata, Option<ColdObjectManifest>)> {
    if encoded.len() < COLD_OBJECT_HEADER_LEN {
        return Err(StoreError::InvalidState(format!(
            "{label} {} is shorter than cold object header",
            path.display()
        )));
    }
    if &encoded[..8] != COLD_OBJECT_MAGIC {
        return Err(StoreError::InvalidState(format!(
            "{label} {} has invalid cold object magic",
            path.display()
        )));
    }
    let version = u16::from_le_bytes([encoded[8], encoded[9]]);
    if version != COLD_OBJECT_VERSION && version != COLD_OBJECT_MANIFEST_VERSION {
        return Err(StoreError::InvalidState(format!(
            "{label} {} has unsupported cold object version {}",
            path.display(),
            version
        )));
    }
    let header_len =
        u32::from_le_bytes([encoded[12], encoded[13], encoded[14], encoded[15]]) as usize;
    let expected_header_len = if version == COLD_OBJECT_MANIFEST_VERSION {
        COLD_OBJECT_MANIFEST_HEADER_LEN
    } else {
        COLD_OBJECT_HEADER_LEN
    };
    if header_len != expected_header_len || encoded.len() < header_len {
        return Err(StoreError::InvalidState(format!(
            "{label} {} has invalid cold object header length {}",
            path.display(),
            header_len
        )));
    }
    let stored_length = u64::from_le_bytes([
        encoded[16],
        encoded[17],
        encoded[18],
        encoded[19],
        encoded[20],
        encoded[21],
        encoded[22],
        encoded[23],
    ]);
    let manifest_len = if version == COLD_OBJECT_MANIFEST_VERSION {
        u32::from_le_bytes([encoded[36], encoded[37], encoded[38], encoded[39]]) as usize
    } else {
        0
    };
    let payload_offset = header_len.checked_add(manifest_len).ok_or_else(|| {
        StoreError::InvalidState(format!(
            "{label} {} has overflowing manifest length",
            path.display()
        ))
    })?;
    if encoded.len() < payload_offset {
        return Err(StoreError::InvalidState(format!(
            "{label} {} manifest length {} exceeds file length {}",
            path.display(),
            manifest_len,
            encoded.len()
        )));
    }
    let payload = &encoded[payload_offset..];
    if payload.len() as u64 != stored_length {
        return Err(StoreError::InvalidState(format!(
            "{label} {} payload length mismatch: header {} actual {}",
            path.display(),
            stored_length,
            payload.len()
        )));
    }
    let checksum_kind = u16::from_le_bytes([encoded[24], encoded[25]]);
    let stored_checksum = u64::from_le_bytes([
        encoded[28],
        encoded[29],
        encoded[30],
        encoded[31],
        encoded[32],
        encoded[33],
        encoded[34],
        encoded[35],
    ]);
    let checksum = match checksum_kind {
        COLD_OBJECT_CHECKSUM_NONE => None,
        COLD_OBJECT_CHECKSUM_XXH3 => {
            let actual = payload_checksum(payload);
            if actual != stored_checksum {
                return Err(StoreError::InvalidState(format!(
                    "{label} {} stored checksum mismatch: expected {} actual {}",
                    path.display(),
                    stored_checksum,
                    actual
                )));
            }
            Some(stored_checksum)
        }
        other => {
            return Err(StoreError::InvalidState(format!(
                "{label} {} has unsupported checksum kind {}",
                path.display(),
                other
            )))
        }
    };
    let manifest = if manifest_len == 0 {
        None
    } else {
        let manifest = decode_cold_object_manifest(&encoded[header_len..payload_offset])?;
        if manifest.length != stored_length || manifest.checksum != checksum {
            return Err(StoreError::InvalidState(format!(
                "{label} {} manifest payload metadata does not match header",
                path.display()
            )));
        }
        Some(manifest)
    };
    Ok((
        ColdPayloadMetadata {
            length: stored_length,
            checksum,
        },
        manifest,
    ))
}

fn decode_backend_payload(
    path: &std::path::Path,
    label: &str,
    mut encoded: Vec<u8>,
    expected_length: u64,
    expected_checksum: Option<u64>,
) -> Result<Vec<u8>> {
    let (stored_metadata, manifest) =
        cold_object_payload_metadata_and_manifest(path, label, &encoded)?;
    if stored_metadata.length != expected_length {
        return Err(StoreError::InvalidState(format!(
            "{label} {} length mismatch: expected {} actual {}",
            path.display(),
            expected_length,
            stored_metadata.length
        )));
    }
    let header_len = if manifest.is_some() {
        COLD_OBJECT_MANIFEST_HEADER_LEN
    } else {
        COLD_OBJECT_HEADER_LEN
    };
    let manifest_len = if manifest.is_some() {
        u32::from_le_bytes([encoded[36], encoded[37], encoded[38], encoded[39]]) as usize
    } else {
        0
    };
    let payload_offset = header_len.checked_add(manifest_len).ok_or_else(|| {
        StoreError::InvalidState(format!(
            "{label} {} has overflowing manifest length",
            path.display()
        ))
    })?;
    if let Some(expected) = expected_checksum {
        let payload = &encoded[payload_offset..];
        let actual = payload_checksum(payload);
        if actual != expected {
            return Err(StoreError::InvalidState(format!(
                "{label} {} checksum mismatch: expected {} actual {}",
                path.display(),
                expected,
                actual
            )));
        }
    }
    Ok(encoded.split_off(payload_offset))
}

// ---------------------------------------------------------------------------
// LocalDir backend implementation
// ---------------------------------------------------------------------------

/// File-per-object cold tier backend using a local directory tree.
///
/// Layout under `root`:
///
/// ```text
/// <root>/
///   <encoded_device_id>/
///     <encoded_object_locator>.bin    ← materialized payloads
///   __pending__/
///     <encoded_device_id>/
///       <encoded_object_locator>.bin  ← staging area for offload sources
///   __orphan_quarantine__/            ← quarantined orphan files (GC)
/// ```
///
/// All writes use atomic temp-file + rename via `write_backend_payload_atomically`.
pub(crate) struct LocalDirPersistentStorageBackend {
    root: std::path::PathBuf,
}

impl LocalDirPersistentStorageBackend {
    #[cfg(test)]
    fn new() -> Self {
        Self::new_with_root(default_cold_tier_root())
    }

    pub(crate) fn new_with_root(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn object_path(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> std::path::PathBuf {
        self.root
            .join(encode_backend_component(&cold_backing.cold_tier_id))
            .join(format!(
                "{}.bin",
                encode_backend_component(&cold_backing.object_locator)
            ))
    }

    fn pending_source_path(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> std::path::PathBuf {
        self.root
            .join("__pending__")
            .join(encode_backend_component(&cold_backing.cold_tier_id))
            .join(format!(
                "{}.bin",
                encode_backend_component(&cold_backing.object_locator)
            ))
    }

    fn object_metadata(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<(u64, Option<u64>)>> {
        let path = self.object_path(cold_backing);
        let Some(payload) = read_file_optional(&path, "backend object")? else {
            return Ok(None);
        };
        cold_object_payload_metadata(&path, "backend object", &payload).map(Some)
    }

    fn scan_recovered_objects(
        &self,
        device: &ResolvedColdTierTarget,
    ) -> Result<Vec<RecoveredColdObject>> {
        let directory = self
            .root
            .join(encode_backend_component(&device.cold_tier_id));
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(StoreError::Transport(format!(
                    "failed to scan cold tier manifest directory {}: {error}",
                    directory.display()
                )))
            }
        };
        let mut recovered = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                StoreError::Transport(format!(
                    "failed to scan cold tier manifest directory {}: {error}",
                    directory.display()
                ))
            })?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("bin") {
                continue;
            }
            let Some(encoded) = read_file_optional(&path, "backend object")? else {
                continue;
            };
            let parsed = match cold_object_payload_metadata_and_manifest(
                &path,
                "backend object",
                &encoded,
            ) {
                Ok(parsed) => parsed,
                Err(error) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %error,
                        "skipping unreadable cold tier object during recovery scan"
                    );
                    continue;
                }
            };
            let (metadata, Some(manifest)) = parsed else {
                continue;
            };
            if manifest.cold_tier_id != device.cold_tier_id {
                continue;
            }
            let expected_file_name =
                format!("{}.bin", encode_backend_component(&manifest.object_locator));
            if path.file_name().and_then(|name| name.to_str()) != Some(expected_file_name.as_str())
            {
                tracing::warn!(
                    path = %path.display(),
                    expected_file_name,
                    "skipping cold tier object with mismatched manifest locator during recovery scan"
                );
                continue;
            }
            recovered.push(RecoveredColdObject {
                manifest,
                path,
                metadata,
            });
        }
        Ok(recovered)
    }

    fn ensure_parent_dir(&self, path: &std::path::Path) -> Result<()> {
        let parent = path.parent().ok_or_else(|| {
            StoreError::InvalidState("backend object path is missing parent directory".to_string())
        })?;
        std::fs::create_dir_all(parent).map_err(|error| {
            StoreError::Transport(format!(
                "failed to create backend directory {}: {error}",
                parent.display()
            ))
        })
    }

    fn collect_file_stats(root: &std::path::Path) -> Result<(u64, u64)> {
        let mut count = 0u64;
        let mut bytes = 0u64;
        collect_local_dir_file_stats(root, &mut count, &mut bytes)?;
        Ok((count, bytes))
    }
}

fn collect_local_dir_quarantine_stats(root: &std::path::Path) -> Result<(u64, u64)> {
    let mut count = 0u64;
    let mut bytes = 0u64;
    collect_local_dir_quarantine_stats_inner(root, &mut count, &mut bytes)?;
    Ok((count, bytes))
}

fn collect_local_dir_quarantine_stats_inner(
    root: &std::path::Path,
    count: &mut u64,
    bytes: &mut u64,
) -> Result<()> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(StoreError::Transport(format!(
                "failed to read backend directory {}: {error}",
                root.display()
            )))
        }
    };
    for entry in entries {
        let entry = entry.map_err(|error| {
            StoreError::Transport(format!(
                "failed to read backend directory entry {}: {error}",
                root.display()
            ))
        })?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|error| {
            StoreError::Transport(format!(
                "failed to stat backend path {}: {error}",
                path.display()
            ))
        })?;
        if !metadata.is_dir() {
            continue;
        }
        if path.file_name().and_then(|name| name.to_str()) == Some("__orphan_quarantine__") {
            collect_local_dir_file_stats(&path, count, bytes)?;
        } else {
            collect_local_dir_quarantine_stats_inner(&path, count, bytes)?;
        }
    }
    Ok(())
}

fn collect_local_dir_file_stats(
    root: &std::path::Path,
    count: &mut u64,
    bytes: &mut u64,
) -> Result<()> {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(StoreError::Transport(format!(
                "failed to read backend directory {}: {error}",
                root.display()
            )))
        }
    };
    for entry in entries {
        let entry = entry.map_err(|error| {
            StoreError::Transport(format!(
                "failed to read backend directory entry {}: {error}",
                root.display()
            ))
        })?;
        let metadata = entry.metadata().map_err(|error| {
            StoreError::Transport(format!(
                "failed to stat backend path {}: {error}",
                entry.path().display()
            ))
        })?;
        if metadata.is_dir() {
            collect_local_dir_file_stats(&entry.path(), count, bytes)?;
        } else if metadata.is_file() {
            *count = count.saturating_add(1);
            *bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok(())
}

fn read_file_optional(path: &std::path::Path, label: &str) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(data) => Ok(Some(data)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(StoreError::Transport(format!(
            "failed to read {label} {}: {error}",
            path.display()
        ))),
    }
}

fn remove_file_optional(path: &std::path::Path, label: &str) -> Result<bool> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(StoreError::Transport(format!(
            "failed to delete {label} {}: {error}",
            path.display()
        ))),
    }
}

impl PersistentStorageBackend for LocalDirPersistentStorageBackend {
    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        ensure_backend_directory(&self.root, "local_dir")?;
        let (capacity_bytes, available_bytes) = filesystem_capacity_bytes(&self.root)?;
        Ok(PersistentStorageBackendHealth {
            capacity_bytes: Some(capacity_bytes),
            available_bytes: Some(available_bytes),
        })
    }

    fn maintenance_stats(&self) -> Result<BackendMaintenanceStats> {
        let (total_count, total_bytes) = Self::collect_file_stats(&self.root)?;
        let (pending_source_count, pending_source_bytes) =
            Self::collect_file_stats(&self.root.join("__pending__"))?;
        let (orphan_quarantine_count, orphan_quarantine_bytes) =
            collect_local_dir_quarantine_stats(&self.root)?;
        Ok(BackendMaintenanceStats {
            object_count: total_count
                .saturating_sub(pending_source_count)
                .saturating_sub(orphan_quarantine_count),
            object_bytes: total_bytes
                .saturating_sub(pending_source_bytes)
                .saturating_sub(orphan_quarantine_bytes),
            pending_source_count,
            pending_source_bytes,
            orphan_quarantine_count,
            orphan_quarantine_bytes,
            ..BackendMaintenanceStats::default()
        })
    }

    fn put_object(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> Result<mooncake_store_core::ColdBackingRoute> {
        self.put_object_with_route(None, cold_backing, payload)
    }

    fn put_object_with_route(
        &self,
        route: Option<&ObjectRoute>,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> Result<mooncake_store_core::ColdBackingRoute> {
        let path = self.object_path(cold_backing);
        self.ensure_parent_dir(&path)?;
        let manifest = route.map(|route| manifest_from_route(route, cold_backing));
        let encoded = encode_backend_payload_with_manifest(
            payload,
            cold_backing.length,
            cold_backing.checksum,
            manifest.as_ref(),
        )?;
        let t0 = std::time::Instant::now();
        write_backend_payload_atomically(&path, &encoded, "backend object")?;
        let elapsed = t0.elapsed();
        tracing::info!(
            object_locator = %cold_backing.object_locator,
            cold_tier_id = %cold_backing.cold_tier_id,
            length_bytes = cold_backing.length,
            elapsed_ms = elapsed.as_millis() as u64,
            "cold_tier_ssd_write_complete"
        );
        Ok(mooncake_store_core::ColdBackingRoute {
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: Vec::new(),
            ..cold_backing.clone()
        })
    }

    fn get_object(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<Vec<u8>>> {
        let path = self.object_path(cold_backing);
        let Some(encoded) = read_file_optional(&path, "backend object")? else {
            return Ok(None);
        };
        Ok(Some(decode_backend_payload(
            &path,
            "backend object",
            encoded,
            cold_backing.length,
            cold_backing.checksum,
        )?))
    }

    fn delete_object(&self, cold_backing: &mooncake_store_core::ColdBackingRoute) -> Result<bool> {
        remove_file_optional(&self.object_path(cold_backing), "backend object")
    }

    fn put_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> Result<()> {
        let path = self.pending_source_path(cold_backing);
        self.ensure_parent_dir(&path)?;
        let encoded = encode_backend_payload(payload, cold_backing.length, cold_backing.checksum)?;
        write_backend_payload_atomically(&path, &encoded, "pending backend source")?;
        Ok(())
    }

    fn get_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<Vec<u8>>> {
        let path = self.pending_source_path(cold_backing);
        let Some(encoded) = read_file_optional(&path, "pending backend source")? else {
            return Ok(None);
        };
        Ok(Some(decode_backend_payload(
            &path,
            "pending backend source",
            encoded,
            cold_backing.length,
            cold_backing.checksum,
        )?))
    }

    fn delete_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<bool> {
        remove_file_optional(
            &self.pending_source_path(cold_backing),
            "pending backend source",
        )
    }
}

#[cfg(test)]
mod tests {
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
    fn encode_backend_payload_roundtrips_with_checksum() {
        let payload = b"hello cold tier";
        let checksum = payload_checksum(payload);
        let encoded =
            encode_backend_payload(payload, payload.len() as u64, Some(checksum)).unwrap();
        let decoded = decode_backend_payload(
            std::path::Path::new("/test"),
            "test",
            encoded,
            payload.len() as u64,
            Some(checksum),
        )
        .unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_backend_payload_roundtrips_without_checksum() {
        let payload = b"no checksum payload";
        let encoded = encode_backend_payload(payload, payload.len() as u64, None).unwrap();
        let decoded = decode_backend_payload(
            std::path::Path::new("/test"),
            "test",
            encoded,
            payload.len() as u64,
            None,
        )
        .unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_backend_payload_with_manifest_roundtrips_manifest() {
        let payload = b"manifest payload";
        let checksum = payload_checksum(payload);
        let manifest = ColdObjectManifest {
            key: ObjectKey::new("tenant::key"),
            namespace: Some(NamespaceScope {
                tenant: "tenant".to_string(),
                domain: "domain".to_string(),
                object_set: "set".to_string(),
            }),
            logical_key: Some("logical".to_string()),
            canonical_key: Some("canonical".to_string()),
            sharing_scope: Some("sharing".to_string()),
            qos_tier: Some("qos".to_string()),
            route_version: RouteVersion(7),
            cold_tier_id: "device-manifest".to_string(),
            object_locator: "tenant::key@v7".to_string(),
            length: payload.len() as u64,
            checksum: Some(checksum),
        };
        let encoded = encode_backend_payload_with_manifest(
            payload,
            payload.len() as u64,
            Some(checksum),
            Some(&manifest),
        )
        .unwrap();
        let (metadata, decoded_manifest) = cold_object_payload_metadata_and_manifest(
            std::path::Path::new("/test"),
            "test",
            &encoded,
        )
        .unwrap();
        assert_eq!(metadata.length, payload.len() as u64);
        assert_eq!(metadata.checksum, Some(checksum));
        assert_eq!(decoded_manifest, Some(manifest));
        let decoded = decode_backend_payload(
            std::path::Path::new("/test"),
            "test",
            encoded,
            payload.len() as u64,
            Some(checksum),
        )
        .unwrap();
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_backend_payload_rejects_length_mismatch() {
        let payload = b"mismatch";
        let result = encode_backend_payload(payload, 999, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("length mismatch"));
    }

    #[test]
    fn encode_backend_payload_rejects_checksum_mismatch() {
        let payload = b"payload";
        let result = encode_backend_payload(payload, payload.len() as u64, Some(12345));
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));
    }

    #[test]
    fn cold_object_payload_metadata_validates_magic() {
        let mut bad = vec![0u8; COLD_OBJECT_HEADER_LEN + 4];
        bad[..8].copy_from_slice(b"BADMAGIC");
        let result = cold_object_payload_metadata(std::path::Path::new("/test"), "test", &bad);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid cold object magic"));
    }

    #[test]
    fn cold_object_payload_metadata_validates_version() {
        let payload = b"data";
        let encoded = encode_backend_payload(payload, payload.len() as u64, None).unwrap();
        let mut bad = encoded;
        // Corrupt version field (bytes 8-9)
        bad[8] = 99;
        bad[9] = 0;
        let result = cold_object_payload_metadata(std::path::Path::new("/test"), "test", &bad);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsupported cold object version"));
    }

    #[test]
    fn cold_object_payload_metadata_validates_truncated_header() {
        let short = vec![0u8; 10]; // shorter than COLD_OBJECT_HEADER_LEN
        let result = cold_object_payload_metadata(std::path::Path::new("/test"), "test", &short);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("shorter than cold object header"));
    }

    #[test]
    fn decode_backend_payload_rejects_length_mismatch() {
        let payload = b"short";
        let encoded = encode_backend_payload(payload, payload.len() as u64, None).unwrap();
        // Expect length 999, but payload is 5 bytes
        let result =
            decode_backend_payload(std::path::Path::new("/test"), "test", encoded, 999, None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("length mismatch"));
    }

    #[test]
    fn decode_backend_payload_rejects_caller_checksum_mismatch() {
        let payload = b"checksum-test";
        let checksum = payload_checksum(payload);
        let encoded =
            encode_backend_payload(payload, payload.len() as u64, Some(checksum)).unwrap();
        // Provide a different expected checksum from the caller
        let wrong_checksum = checksum.wrapping_add(1);
        let result = decode_backend_payload(
            std::path::Path::new("/test"),
            "test",
            encoded,
            payload.len() as u64,
            Some(wrong_checksum),
        );
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch"));
    }

    #[test]
    fn cold_object_payload_metadata_rejects_unsupported_checksum_kind() {
        let payload = b"data";
        let mut encoded = encode_backend_payload(payload, payload.len() as u64, None).unwrap();
        // Corrupt checksum kind field (bytes 24-25) to an unsupported value
        encoded[24] = 99;
        encoded[25] = 0;
        let result = cold_object_payload_metadata(std::path::Path::new("/test"), "test", &encoded);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsupported checksum kind"));
    }

    #[test]
    fn cold_object_payload_metadata_rejects_invalid_header_length() {
        let payload = b"data";
        let mut encoded = encode_backend_payload(payload, payload.len() as u64, None).unwrap();
        // Corrupt header_len field (bytes 12-15) to a value != COLD_OBJECT_HEADER_LEN
        encoded[12] = 99;
        encoded[13] = 0;
        encoded[14] = 0;
        encoded[15] = 0;
        let result = cold_object_payload_metadata(std::path::Path::new("/test"), "test", &encoded);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid cold object header length"));
    }

    #[test]
    fn cold_object_payload_metadata_detects_stored_checksum_mismatch() {
        let payload = b"original";
        let checksum = payload_checksum(payload);
        let mut encoded =
            encode_backend_payload(payload, payload.len() as u64, Some(checksum)).unwrap();
        // Corrupt one payload byte to cause stored checksum mismatch
        let last = encoded.len() - 1;
        encoded[last] ^= 0xFF;
        let result = cold_object_payload_metadata(std::path::Path::new("/test"), "test", &encoded);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("stored checksum mismatch"));
    }

    #[test]
    fn local_dir_backend_put_get_delete_cycle() {
        let root = TestTempDir::new(default_cold_tier_root().join("local-dir-put-get-delete"));
        let backend = LocalDirPersistentStorageBackend::new_with_root(root.path());
        let payload = b"cycle-payload";
        let checksum = payload_checksum(payload);
        let cold_backing = mooncake_store_core::ColdBackingRoute {
            owner: mooncake_store_core::ClientRuntimeId::new(
                "cycle",
                mooncake_store_core::ClientEpoch(1),
            ),
            cold_tier_id: "device-cycle".to_string(),
            object_locator: "object-cycle".to_string(),
            length: payload.len() as u64,
            checksum: Some(checksum),
            state: mooncake_store_core::ColdBackingState::PendingOffload,
            replicas: Vec::new(),
        };
        let materialized = backend.put_object(&cold_backing, payload).unwrap();
        assert_eq!(
            materialized.state,
            mooncake_store_core::ColdBackingState::Materialized
        );

        let read = backend.get_object(&cold_backing).unwrap().unwrap();
        assert_eq!(read, payload);

        let deleted = backend.delete_object(&cold_backing).unwrap();
        assert!(deleted);
        assert!(backend.get_object(&cold_backing).unwrap().is_none());
    }

    #[test]
    fn local_dir_scan_recovered_objects_reads_self_describing_bin() {
        let root = TestTempDir::new(default_cold_tier_root().join("local-dir-scan-manifest"));
        let backend = LocalDirPersistentStorageBackend::new_with_root(root.path());
        let payload = b"recover-me";
        let checksum = payload_checksum(payload);
        let owner = mooncake_store_core::ClientRuntimeId::new(
            "scan-owner",
            mooncake_store_core::ClientEpoch(1),
        );
        let cold_backing = mooncake_store_core::ColdBackingRoute {
            owner: owner.clone(),
            cold_tier_id: "device-scan".to_string(),
            object_locator: "recover-key@v3".to_string(),
            length: payload.len() as u64,
            checksum: Some(checksum),
            state: mooncake_store_core::ColdBackingState::PendingOffload,
            replicas: Vec::new(),
        };
        let route = ObjectRoute {
            key: ObjectKey::new("recover-key"),
            namespace: Some(NamespaceScope {
                tenant: "tenant".to_string(),
                domain: "domain".to_string(),
                object_set: "set".to_string(),
            }),
            logical_key: Some("logical-recover-key".to_string()),
            canonical_key: Some("canonical-recover-key".to_string()),
            sharing_scope: Some("sharing-scope".to_string()),
            qos_tier: Some("qos-tier".to_string()),
            version: RouteVersion(3),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: Some(cold_backing.clone()),
        };
        backend
            .put_object_with_route(Some(&route), &cold_backing, payload)
            .unwrap();
        let device = ResolvedColdTierTarget {
            cold_tier_id: "device-scan".to_string(),
            kind: ColdTierKind::Ssd,
            target: mooncake_store_core::ColdTierTargetSpec::Directory {
                path: root.path().display().to_string(),
            },
            root_dir: root.path().to_path_buf(),
            ssd_engine: ColdTierSsdEngine::LocalDir,
            capacity_override_bytes: None,
            tags: Vec::new(),
        };
        let recovered = backend.scan_recovered_objects(&device).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].manifest.key, ObjectKey::new("recover-key"));
        assert_eq!(recovered[0].manifest.namespace, route.namespace);
        assert_eq!(recovered[0].manifest.logical_key, route.logical_key);
        assert_eq!(recovered[0].manifest.canonical_key, route.canonical_key);
        assert_eq!(recovered[0].manifest.sharing_scope, route.sharing_scope);
        assert_eq!(recovered[0].manifest.qos_tier, route.qos_tier);
        assert_eq!(recovered[0].manifest.route_version, RouteVersion(4));
        assert_eq!(recovered[0].metadata.length, payload.len() as u64);
        assert_eq!(recovered[0].metadata.checksum, Some(checksum));
    }

    #[test]
    fn local_dir_backend_pending_source_lifecycle() {
        let root = TestTempDir::new(default_cold_tier_root().join("local-dir-pending-source"));
        let backend = LocalDirPersistentStorageBackend::new_with_root(root.path());
        let payload = b"pending-src";
        let checksum = payload_checksum(payload);
        let cold_backing = mooncake_store_core::ColdBackingRoute {
            owner: mooncake_store_core::ClientRuntimeId::new(
                "pending",
                mooncake_store_core::ClientEpoch(1),
            ),
            cold_tier_id: "device-pending".to_string(),
            object_locator: "object-pending".to_string(),
            length: payload.len() as u64,
            checksum: Some(checksum),
            state: mooncake_store_core::ColdBackingState::PendingOffload,
            replicas: Vec::new(),
        };
        backend.put_pending_source(&cold_backing, payload).unwrap();
        let read = backend.get_pending_source(&cold_backing).unwrap().unwrap();
        assert_eq!(read, payload);
        let deleted = backend.delete_pending_source(&cold_backing).unwrap();
        assert!(deleted);
        assert!(backend.get_pending_source(&cold_backing).unwrap().is_none());
    }

    #[test]
    fn collect_local_dir_file_stats_counts_recursive_files() {
        let root = TestTempDir::new(default_cold_tier_root().join("file-stats-recursive"));
        let sub = root.path().join("a").join("b");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(root.path().join("top.bin"), b"12345").unwrap();
        std::fs::write(sub.join("nested.bin"), b"abc").unwrap();
        let (count, bytes) =
            LocalDirPersistentStorageBackend::collect_file_stats(root.path()).unwrap();
        assert_eq!(count, 2);
        assert_eq!(bytes, 8); // 5 + 3
    }

    #[test]
    fn collect_local_dir_file_stats_returns_zero_for_missing_dir() {
        let root = TestTempDir::new(default_cold_tier_root().join("file-stats-missing"));
        let missing = root.path().join("nonexistent");
        let (count, bytes) =
            LocalDirPersistentStorageBackend::collect_file_stats(&missing).unwrap();
        assert_eq!(count, 0);
        assert_eq!(bytes, 0);
    }

    #[test]
    fn read_file_optional_returns_none_for_missing() {
        let path = default_cold_tier_root().join("read-optional-missing-file.bin");
        let result = read_file_optional(&path, "test").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn remove_file_optional_returns_false_for_missing() {
        let path = default_cold_tier_root().join("remove-optional-missing-file.bin");
        let result = remove_file_optional(&path, "test").unwrap();
        assert!(!result);
    }

    #[test]
    fn remove_file_optional_removes_existing_file() {
        let root = TestTempDir::new(default_cold_tier_root().join("remove-optional-existing"));
        let path = root.path().join("to-remove.bin");
        std::fs::write(&path, b"removable").unwrap();
        let result = remove_file_optional(&path, "test").unwrap();
        assert!(result);
        assert!(!path.exists());
    }
}
