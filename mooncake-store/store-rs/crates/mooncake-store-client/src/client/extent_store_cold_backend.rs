// ExtentStore-backed PersistentStorageBackend implementation.
// Included via `include!()` at module level in extent_store_engine.rs.

#[cfg_attr(not(test), allow(dead_code))]
pub(super) struct ExtentStoreStorageBackend {
    engine: ExtentStoreEngine,
    pinned_access_tracker: Mutex<ExtentStorePinnedAccessTracker>,
    pinned_buffer_pool: std::sync::Arc<ExtentStoreBufferPool>,
}

#[derive(Default)]
struct ExtentStorePinnedAccessTracker {
    last_batch: BTreeSet<String>,
}

impl ExtentStoreStorageBackend {
    pub(super) fn new(root: impl Into<std::path::PathBuf>) -> Result<Self> {
        Self::new_with_segment_size(root, DEFAULT_EXTENT_STORE_SEGMENT_SIZE)
    }

    fn from_engine(engine: ExtentStoreEngine) -> Self {
        let pinned_buffer_pool = std::sync::Arc::new(ExtentStoreBufferPool::new(
            engine.io_counters.clone(),
        ));
        Self {
            engine,
            pinned_access_tracker: Mutex::new(ExtentStorePinnedAccessTracker::default()),
            pinned_buffer_pool,
        }
    }

    fn new_with_segment_size(
        root: impl Into<std::path::PathBuf>,
        segment_size: u64,
    ) -> Result<Self> {
        Ok(Self::from_engine(ExtentStoreEngine::new_with_segment_size(
            root,
            segment_size,
        )?))
    }
}

impl ExtentStoreStorageBackend {
    fn manifest_path(&self, object_locator: &str) -> std::path::PathBuf {
        self.engine
            .root
            .join("manifests")
            .join(format!("{}.bin", encode_backend_component(object_locator)))
    }

    fn ensure_manifest_dir(&self) -> Result<()> {
        let directory = self.engine.root.join("manifests");
        std::fs::create_dir_all(&directory).map_err(|error| {
            StoreError::Transport(format!(
                "failed to create extent store manifest directory {}: {error}",
                directory.display()
            ))
        })
    }

    fn write_manifest(&self, manifest: &ColdObjectManifest) -> Result<()> {
        self.ensure_manifest_dir()?;
        let encoded = encode_cold_object_manifest(manifest)?;
        write_backend_payload_atomically(
            &self.manifest_path(&manifest.object_locator),
            &encoded,
            "extent store manifest",
        )
    }

    fn remove_manifest(&self, object_locator: &str) -> Result<bool> {
        remove_file_optional(&self.manifest_path(object_locator), "extent store manifest")
    }

    pub(super) fn scan_recovered_objects(&self, device: &ResolvedColdTierTarget) -> Result<Vec<RecoveredColdObject>> {
        let directory = self.engine.root.join("manifests");
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(StoreError::Transport(format!(
                    "failed to scan extent store manifest directory {}: {error}",
                    directory.display()
                )))
            }
        };
        let mut recovered = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                StoreError::Transport(format!(
                    "failed to scan extent store manifest directory {}: {error}",
                    directory.display()
                ))
            })?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("bin") {
                continue;
            }
            let Some(encoded) = read_file_optional(&path, "extent store manifest")? else {
                continue;
            };
            let manifest = decode_cold_object_manifest(&encoded)?;
            if manifest.cold_tier_id != device.cold_tier_id {
                continue;
            }
            if !ExtentStoreEngine::is_extent_store_locator(&manifest.object_locator) {
                continue;
            }
            let expected_file_name = format!("{}.bin", encode_backend_component(&manifest.object_locator));
            if path.file_name().and_then(|name| name.to_str()) != Some(expected_file_name.as_str()) {
                return Err(StoreError::InvalidState(format!(
                    "extent store manifest {} locator does not match file name",
                    path.display()
                )));
            }
            match self.engine.contains(&manifest.object_locator, manifest.length) {
                Ok(true) => {}
                Ok(false) | Err(StoreError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            }
            recovered.push(RecoveredColdObject {
                metadata: ColdPayloadMetadata {
                    length: manifest.length,
                    checksum: manifest.checksum,
                },
                manifest,
                path,
            });
        }
        Ok(recovered)
    }

    pub(super) fn contains_materialized(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<bool> {
        if !ExtentStoreEngine::is_extent_store_locator(&cold_backing.object_locator) {
            return Ok(false);
        }
        self.engine
            .contains(&cold_backing.object_locator, cold_backing.length)
    }

    fn get_extent_object_pinned_with_owned_fallback(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<ColdObjectPayload>> {
        match self
            .engine
            .get_pinned(&cold_backing.object_locator, cold_backing.length)
        {
            Err(error) if extent_store_pinned_error_allows_owned_fallback(&error) => self
                .get_object(cold_backing)
                .map(|payload| payload.map(ColdObjectPayload::Owned)),
            result => result,
        }
    }

    fn get_objects_pooled_for_pinned_batch(
        &self,
        reads: &[ColdObjectPinnedRead<'_>],
    ) -> Vec<Result<Option<ColdObjectPayload>>> {
        let mut buffers = Vec::with_capacity(reads.len());
        for read in reads {
            match self.pinned_buffer_pool.lease(read.cold_backing.length as usize) {
                Ok(buffer) => buffers.push(buffer),
                Err(error) => {
                    return std::iter::repeat_with(|| Err(error.clone()))
                        .take(reads.len())
                        .collect()
                }
            }
        }

        let mut batch_reads = reads
            .iter()
            .zip(buffers.iter_mut())
            .map(|(read, buffer)| ColdObjectRead {
                cold_backing: read.cold_backing,
                dst: &mut buffer[..],
            })
            .collect::<Vec<_>>();
        let read_results = self.get_objects_into_batch(&mut batch_reads);
        drop(batch_reads);

        read_results
            .into_iter()
            .zip(buffers)
            .map(|(result, mut buffer)| {
                result.and_then(|size| {
                    size.map(|size| {
                        buffer.set_len(size)?;
                        self.engine.io_counters.record_pinned_payload_create(size);
                        Ok(ColdObjectPayload::Borrowed(ColdPayloadRef::new(
                            std::sync::Arc::new(ExtentStorePooledPayloadRef {
                                buffer,
                                counters: self.engine.io_counters.clone(),
                            }),
                        )))
                    })
                    .transpose()
                })
            })
            .collect()
    }
}

impl PersistentStorageBackend for ExtentStoreStorageBackend {
    fn health(&self) -> Result<PersistentStorageBackendHealth> {
        let (capacity_bytes, available_bytes) = filesystem_capacity_bytes(&self.engine.root)?;
        Ok(PersistentStorageBackendHealth {
            capacity_bytes: Some(capacity_bytes),
            available_bytes: Some(available_bytes),
        })
    }

    fn maintenance_stats(&self) -> Result<BackendMaintenanceStats> {
        let stats = self.engine.maintenance_stats();
        Ok(BackendMaintenanceStats {
            extent_live_bytes: stats.live_bytes,
            extent_dead_bytes: stats.dead_bytes,
            extent_segment_count: stats.segment_count,
            ..BackendMaintenanceStats::default()
        })
    }

    fn compact(&self, max_candidate_bytes: u64) -> Result<BackendCompactionResult> {
        let stats = self.engine.maintenance_stats();
        Ok(BackendCompactionResult {
            supported: false,
            candidate_bytes: stats.dead_bytes.min(max_candidate_bytes),
            reclaimed_bytes: 0,
        })
    }

    fn profiling_counters(&self) -> Vec<(&'static str, u64)> {
        self.engine.io_stats().to_profiling_pairs()
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
        let t_write = std::time::Instant::now();
        let put_result = if let Some(checksum) = cold_backing.checksum {
            self.engine
                .put_batch(&[ExtentStoreWrite {
                    logical_locator: &cold_backing.object_locator,
                    payload,
                    checksum: Some(checksum),
                }])
                .into_iter()
                .next()
                .unwrap_or_else(|| {
                    Err(StoreError::InvalidState(
                        "empty extent store put result".to_string(),
                    ))
                })
        } else {
            self.engine.put(&cold_backing.object_locator, payload)
        };
        let write_elapsed = t_write.elapsed();
        crate::observability::registry::record_cold_tier_ssd_write(
            if put_result.is_ok() { "ok" } else { "error" },
            write_elapsed,
        );
        let write_ms = write_elapsed.as_secs_f64() * 1000.0;
        if write_ms > 50.0 {
            warn!(
                "ssd_write_slow: write_ms={write_ms:.3}, payload_len={}, locator={}",
                payload.len(),
                cold_backing.object_locator,
            );
        }
        let locator = put_result?;
        let materialized = mooncake_store_core::ColdBackingRoute {
            object_locator: locator,
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: Vec::new(),
            ..cold_backing.clone()
        };
        if let Some(route) = route {
            let manifest = manifest_from_route(route, &materialized);
            if let Err(error) = self.write_manifest(&manifest) {
                let _ = self.engine.delete(&materialized.object_locator);
                return Err(error);
            }
        }
        Ok(materialized)
    }

    fn put_objects_batch_profiled(
        &self,
        writes: &[ColdObjectWrite<'_>],
        record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<mooncake_store_core::ColdBackingRoute>> {
        let build_started = std::time::Instant::now();
        let extent_writes = writes
            .iter()
            .map(|write| ExtentStoreWrite {
                logical_locator: &write.cold_backing.object_locator,
                payload: write.payload,
                checksum: write.cold_backing.checksum,
            })
            .collect::<Vec<_>>();
        record_stage("backend_build_extent_writes", build_started.elapsed());

        let engine_started = std::time::Instant::now();
        let results = self.engine.put_batch_profiled(&extent_writes, record_stage);
        record_stage("engine_put_batch", engine_started.elapsed());

        let merge_started = std::time::Instant::now();
        let materialized = results
            .into_iter()
            .zip(writes.iter())
            .map(|(result, write)| {
                let materialized = result.map(|locator| mooncake_store_core::ColdBackingRoute {
                    object_locator: locator,
                    state: mooncake_store_core::ColdBackingState::Materialized,
                    replicas: Vec::new(),
                    ..write.cold_backing.clone()
                })?;
                if let Some(route) = write.route {
                    let manifest = manifest_from_route(route, &materialized);
                    if let Err(error) = self.write_manifest(&manifest) {
                        let _ = self.engine.delete(&materialized.object_locator);
                        return Err(error);
                    }
                }
                Ok(materialized)
            })
            .collect();
        record_stage("backend_materialize_routes", merge_started.elapsed());
        materialized
    }

    fn get_object(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<Vec<u8>>> {
        if ExtentStoreEngine::is_extent_store_locator(&cold_backing.object_locator) {
            return self
                .engine
                .get(&cold_backing.object_locator, cold_backing.length);
        }
        Ok(None)
    }

    fn get_object_pinned(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<ColdObjectPayload>> {
        if ExtentStoreEngine::is_extent_store_locator(&cold_backing.object_locator) {
            return self.get_extent_object_pinned_with_owned_fallback(cold_backing);
        }
        Ok(None)
    }

    fn get_objects_pinned_batch(
        &self,
        reads: &[ColdObjectPinnedRead<'_>],
    ) -> Vec<Result<Option<ColdObjectPayload>>> {
        if !should_use_pinned_mmap_for_pinned_reads(reads, &self.pinned_access_tracker) {
            for _ in reads {
                self.engine
                    .io_counters
                    .record_pinned_fallback(ExtentStorePinnedFallbackReason::ColdOrdinary);
            }
            return self.get_objects_pooled_for_pinned_batch(reads);
        }
        let mut results = (0..reads.len())
            .map(|_| {
                Err(StoreError::InvalidState(
                    "backend pinned read not submitted".to_string(),
                ))
            })
            .collect::<Vec<_>>();
        let mut extent_reads = Vec::new();
        let mut extent_indices = Vec::new();
        for (index, read) in reads.iter().enumerate() {
            if ExtentStoreEngine::is_extent_store_locator(&read.cold_backing.object_locator) {
                extent_indices.push(index);
                extent_reads.push((
                    read.cold_backing.object_locator.as_str(),
                    read.cold_backing.length,
                ));
            } else {
                self.engine
                    .io_counters
                    .record_pinned_fallback(ExtentStorePinnedFallbackReason::UnsupportedLocator);
                results[index] = self.get_object_pinned(read.cold_backing);
            }
        }
        for (index, result) in extent_indices.into_iter().zip(self.engine.get_pinned_batch(&extent_reads)) {
            results[index] = match result {
                Err(error) if extent_store_pinned_error_allows_owned_fallback(&error) => self
                    .get_object(reads[index].cold_backing)
                    .map(|payload| payload.map(ColdObjectPayload::Owned)),
                result => result,
            };
        }
        results
    }

    fn get_object_into(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        dst: &mut [u8],
    ) -> Result<Option<usize>> {
        if ExtentStoreEngine::is_extent_store_locator(&cold_backing.object_locator) {
            return self
                .engine
                .get_into(&cold_backing.object_locator, cold_backing.length, dst);
        }
        Ok(None)
    }

    fn get_objects_into_batch(
        &self,
        reads: &mut [ColdObjectRead<'_, '_>],
    ) -> Vec<Result<Option<usize>>> {
        self.get_objects_into_batch_profiled(reads, &mut |_, _| {})
    }

    fn get_objects_into_batch_profiled(
        &self,
        reads: &mut [ColdObjectRead<'_, '_>],
        record_stage: &mut dyn FnMut(&'static str, std::time::Duration),
    ) -> Vec<Result<Option<usize>>> {
        let mut results = (0..reads.len())
            .map(|_| {
                Err(StoreError::InvalidState(
                    "backend read not submitted".to_string(),
                ))
            })
            .collect::<Vec<_>>();
        let select_started = std::time::Instant::now();
        let mut extent_indices = Vec::new();
        for (index, read) in reads.iter_mut().enumerate() {
            if ExtentStoreEngine::is_extent_store_locator(&read.cold_backing.object_locator) {
                extent_indices.push(index);
            } else {
                results[index] = self.get_object_into(read.cold_backing, read.dst);
            }
        }
        record_stage("backend_select_extent_reads", select_started.elapsed());
        let disjoint_started = std::time::Instant::now();
        let extent_results =
            with_disjoint_cold_read_buffers(reads, extent_indices, |extent_reads| {
                record_stage("backend_build_extent_reads", disjoint_started.elapsed());
                let engine_started = std::time::Instant::now();
                let results = self.engine.get_batch_into_profiled(extent_reads, record_stage);
                record_stage("engine_get_batch_into", engine_started.elapsed());
                results
            });
        let merge_started = std::time::Instant::now();
        for (index, result) in extent_results {
            results[index] = result;
        }
        record_stage("backend_merge_results", merge_started.elapsed());
        results
    }

    fn delete_object(&self, cold_backing: &mooncake_store_core::ColdBackingRoute) -> Result<bool> {
        if ExtentStoreEngine::is_extent_store_locator(&cold_backing.object_locator) {
            let deleted = self.engine.delete(&cold_backing.object_locator)?;
            let _ = self.remove_manifest(&cold_backing.object_locator);
            return Ok(deleted);
        }
        Ok(false)
    }

    fn put_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
        payload: &[u8],
    ) -> Result<()> {
        let _ = (cold_backing, payload);
        Ok(())
    }

    fn get_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<Option<Vec<u8>>> {
        let _ = cold_backing;
        Ok(None)
    }

    fn delete_pending_source(
        &self,
        cold_backing: &mooncake_store_core::ColdBackingRoute,
    ) -> Result<bool> {
        let _ = cold_backing;
        Ok(false)
    }

    fn disable_pending_source(&self) -> bool {
        true
    }
}
