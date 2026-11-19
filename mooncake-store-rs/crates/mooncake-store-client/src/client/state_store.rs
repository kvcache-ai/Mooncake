include!("cold_tier_restore_state.rs");

const BACKGROUND_EVICTION_BATCH_SIZE: usize = 256;

const PENDING_EVICTION_CONFIRMATION_BATCH: usize = 32;
const PENDING_EVICTION_MAX_ENTRIES: usize = 1024;
const PENDING_EVICTION_RETRY_BASE_MS: u64 = 100;
const PENDING_EVICTION_RETRY_MAX_MS: u64 = 10_000;
const PENDING_DELETE_CONFIRMATION_BATCH: usize = 64;
const PENDING_DELETE_MAX_ENTRIES: usize = 4096;
const PENDING_DELETE_RETRY_BASE_MS: u64 = 100;
const PENDING_DELETE_RETRY_MAX_MS: u64 = 60_000;
const PENDING_DELETE_WARN_AFTER_ATTEMPTS: u32 = 10;
const PENDING_DELETE_RECLAIM_PREPARE_TTL_MS: u64 = 60_000;
const STALE_CANDIDATE_RECONCILE_MIN_INTERVAL_MS: u64 = 100;

#[derive(Clone, Copy, Debug, Default)]
pub(in super) struct EvictionAttempt {
    pub(in super) made_progress: bool,
    pub(in super) dropped_without_cold: bool,
}

struct StaleCandidateReconcileGuard<'a> {
    storage_owner: &'a StorageOwnerState,
}

impl Drop for StaleCandidateReconcileGuard<'_> {
    fn drop(&mut self) {
        self.storage_owner
            .stale_candidate_reconcile_in_progress
            .store(false, std::sync::atomic::Ordering::Release);
    }
}

impl RouteWriteGate {
    fn lock(&self) -> RouteWritePermit<'_> {
        RouteWritePermit {
            guard: Some(self.state.lock()),
        }
    }
}

impl Drop for RouteWritePermit<'_> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            parking_lot::MutexGuard::unlock_fair(guard);
        }
    }
}

impl StoreState {
    fn memory_ref(&self) -> Result<&LocalMemoryState> {
        self.memory
            .as_ref()
            .ok_or_else(|| StoreError::InvalidState("local memory is not registered".to_string()))
    }

    fn memory_mut(&mut self) -> Result<&mut LocalMemoryState> {
        self.memory
            .as_mut()
            .ok_or_else(|| StoreError::InvalidState("local memory is not registered".to_string()))
    }

    fn next_segment_name(&mut self, primary: &SegmentName) -> SegmentName {
        loop {
            let segment_name =
                SegmentName::new(format!("{}-ext-{}", primary.0, self.next_local_segment_id));
            self.next_local_segment_id = self.next_local_segment_id.saturating_add(1);
            if self
                .memory
                .as_ref()
                .is_none_or(|memory| !memory.has_storage_segment(&segment_name))
            {
                return segment_name;
            }
        }
    }

    fn open_segment(&mut self, transport: &dyn StoreTransport, segment_name: &str) -> Result<u64> {
        if let Some(handle) = self.remote_segments.get(segment_name) {
            return Ok(*handle);
        }
        self.preload_remote_segment_descriptor(transport, segment_name)?;
        let handle = transport.open_segment(segment_name)?;
        self.remote_segments
            .insert(segment_name.to_string(), handle);
        Ok(handle)
    }

    fn preload_remote_segment_descriptor(
        &self,
        transport: &dyn StoreTransport,
        segment_name: &str,
    ) -> Result<()> {
        if let Some(metadata) = self.segment_open_metadata.get(segment_name) {
            if let Some(descriptor) = metadata.transport_segment_descriptor.as_deref() {
                transport.cache_remote_segment_descriptor(segment_name, descriptor)?;
            }
        }
        Ok(())
    }

    fn invalidate_remote_segment(&mut self, segment_name: &str) {
        let mut open_names = vec![segment_name.to_string()];
        for ((_, cached_segment), metadata) in &self.segment_target_metadata {
            if cached_segment.0 == segment_name {
                if let Some(endpoint) = metadata.transport_endpoint.as_deref() {
                    let endpoint = endpoint.trim();
                    if !endpoint.is_empty() {
                        open_names.push(endpoint.to_string());
                    }
                }
            }
        }
        for open_name in open_names {
            self.remote_segments.remove(&open_name);
            self.remote_segment_infos.remove(&open_name);
            self.segment_open_metadata.remove(&open_name);
        }
        self.segment_target_metadata
            .retain(|(_, segment), _| segment.0 != segment_name);
    }

    fn reopen_segment(
        &mut self,
        transport: &dyn StoreTransport,
        segment_name: &str,
    ) -> Result<u64> {
        self.remote_segment_infos.remove(segment_name);
        self.segment_target_metadata
            .retain(|(_, segment), _| segment.0 != segment_name);
        if let Some(handle) = self.remote_segments.remove(segment_name) {
            let _ = transport.close_segment(handle);
        }
        self.open_segment(transport, segment_name)
    }

    fn cached_segment_info(&self, segment_name: &str) -> Option<SegmentInfo> {
        self.remote_segment_infos.get(segment_name).cloned()
    }

    fn cached_segment_target_metadata(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
    ) -> Option<SegmentTransportMetadata> {
        self.segment_target_metadata
            .get(&(owner.clone(), segment_name.clone()))
            .cloned()
    }

    fn cached_segment_target_chunks(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
    ) -> Option<Vec<SegmentTargetChunk>> {
        self.cached_segment_target_metadata(owner, segment_name)
            .map(|metadata| metadata.target_chunks)
    }

    fn cache_segment_target_metadata(
        &mut self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        target_chunks: &[SegmentTargetChunk],
        transport_endpoint: Option<String>,
        transport_segment_descriptor: Option<String>,
    ) {
        let key = (owner.clone(), segment_name.clone());
        if let Some(previous) = self.segment_target_metadata.remove(&key) {
            for open_name in Self::segment_open_names(segment_name, &previous) {
                self.segment_open_metadata.remove(&open_name);
            }
        }
        let metadata = SegmentTransportMetadata {
            target_chunks: target_chunks.to_vec(),
            transport_endpoint,
            transport_segment_descriptor,
        };
        for open_name in Self::segment_open_names(segment_name, &metadata) {
            self.segment_open_metadata
                .insert(open_name, metadata.clone());
        }
        self.segment_target_metadata.insert(key, metadata);
    }

    fn segment_open_names(
        segment_name: &SegmentName,
        metadata: &SegmentTransportMetadata,
    ) -> Vec<String> {
        let mut names = vec![segment_name.0.clone()];
        if let Some(endpoint) = metadata.transport_endpoint.as_deref() {
            let endpoint = endpoint.trim();
            if !endpoint.is_empty() && endpoint != segment_name.0 {
                names.push(endpoint.to_string());
            }
        }
        names
    }

    fn open_segment_with_info(
        &mut self,
        transport: &dyn StoreTransport,
        segment_name: &str,
    ) -> Result<(u64, SegmentInfo)> {
        if let (Some(handle), Some(info)) = (
            self.remote_segments.get(segment_name),
            self.remote_segment_infos.get(segment_name),
        ) {
            return Ok((*handle, info.clone()));
        }
        let mut refreshed = false;
        loop {
            let handle = if refreshed {
                self.reopen_segment(transport, segment_name)?
            } else {
                self.open_segment(transport, segment_name)?
            };
            match transport.get_segment_info(handle) {
                Ok(info) => {
                    self.remote_segment_infos
                        .insert(segment_name.to_string(), info.clone());
                    return Ok((handle, info));
                }
                Err(error) if !refreshed && remote_segment_cache_refreshable(&error) => {
                    self.remote_segments.remove(segment_name);
                    self.remote_segment_infos.remove(segment_name);
                    let _ = transport.close_segment(handle);
                    refreshed = true;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn register_external_buffer(
        &mut self,
        transport: &dyn StoreTransport,
        buffer: *mut c_void,
        size: usize,
    ) -> Result<()> {
        self.ensure_non_overlapping(buffer, size)?;
        transport.register_memory(buffer, size)?;
        self.registered_buffers.insert(buffer as usize, size);
        Ok(())
    }

    fn unregister_external_buffer(
        &mut self,
        transport: &dyn StoreTransport,
        buffer: *mut c_void,
        size: usize,
    ) -> Result<()> {
        match self.registered_buffers.get(&(buffer as usize)).copied() {
            Some(registered) if registered == size => {
                transport.unregister_memory(buffer, size)?;
                self.registered_buffers.remove(&(buffer as usize));
                Ok(())
            }
            Some(registered) => Err(StoreError::Allocator(format!(
                "registered buffer size mismatch: requested={size} registered={registered}"
            ))),
            None => Err(StoreError::NotFound(format!(
                "registered buffer {:p} not found",
                buffer
            ))),
        }
    }

    fn registered_buffer_size(&self, buffer: *mut c_void) -> Result<usize> {
        self.registered_buffers
            .get(&(buffer as usize))
            .copied()
            .ok_or_else(|| {
                StoreError::NotFound(format!("registered buffer {:p} not found", buffer))
            })
    }

    fn buffer_is_registered(&self, buffer: *mut c_void, size: usize) -> bool {
        let start = buffer as usize;
        let Some(end) = start.checked_add(size) else {
            return false;
        };
        self.registered_buffers
            .range(..=start)
            .next_back()
            .is_some_and(|(registered_start, registered_size)| {
                let registered_end = registered_start.saturating_add(*registered_size);
                start >= *registered_start && end <= registered_end
            })
    }

    fn ensure_non_overlapping(&self, buffer: *mut c_void, size: usize) -> Result<()> {
        let start = buffer as usize;
        let end = start
            .checked_add(size)
            .ok_or_else(|| StoreError::Allocator("registered buffer range overflow".to_string()))?;
        if let Some((other_start, other_size)) = self
            .registered_buffers
            .range(..=start)
            .next_back()
            .map(|(key, value)| (*key, *value))
        {
            let other_end = other_start.saturating_add(other_size);
            if start < other_end {
                return Err(StoreError::Allocator(format!(
                    "registered buffer overlaps existing range {:x}..{:x}",
                    other_start, other_end
                )));
            }
        }
        if let Some((other_start, _)) = self
            .registered_buffers
            .range(start..)
            .next()
            .map(|(key, value)| (*key, *value))
        {
            if end > other_start {
                return Err(StoreError::Allocator(format!(
                    "registered buffer overlaps existing range starting at {:x}",
                    other_start
                )));
            }
        }
        Ok(())
    }

    fn enqueue_pending_reclaim(&mut self, reclaim: PendingReclaim) {
        let insertion_index = self
            .pending_reclaims
            .iter()
            .position(|pending| pending.due_at_ms > reclaim.due_at_ms)
            .unwrap_or(self.pending_reclaims.len());
        self.pending_reclaims.insert(insertion_index, reclaim);
    }

    fn take_due_reclaims(&mut self, now_ms: u64) -> Vec<PendingReclaim> {
        let mut ready = Vec::new();
        while self
            .pending_reclaims
            .front()
            .is_some_and(|entry| entry.due_at_ms <= now_ms)
        {
            if let Some(entry) = self.pending_reclaims.pop_front() {
                ready.push(entry);
            }
        }
        ready.sort_by(|left, right| {
            left.policy_rank
                .cmp(&right.policy_rank)
                .then_with(|| left.due_at_ms.cmp(&right.due_at_ms))
                .then_with(|| left.tenant.cmp(&right.tenant))
                .then_with(|| left.qos_tier.cmp(&right.qos_tier))
        });
        ready
    }
}

#[cfg(test)]
mod state_store_tests {
    use super::*;
    use mooncake_store_rs_transport::{SegmentBuffer, SegmentKind, TransferProgress};
    use parking_lot::Mutex;
    use std::ffi::c_void;
    use std::sync::Arc;

    #[derive(Default)]
    struct CountingTransportState {
        open_calls: usize,
        close_calls: usize,
        info_calls: usize,
        cached_descriptors: Vec<(String, String)>,
    }

    #[derive(Default, Clone)]
    struct CountingTransport {
        state: Arc<Mutex<CountingTransportState>>,
    }

    impl CountingTransport {
        fn counts(&self) -> (usize, usize, usize) {
            let state = self.state.lock();
            (state.open_calls, state.close_calls, state.info_calls)
        }

        fn cached_descriptors(&self) -> Vec<(String, String)> {
            self.state.lock().cached_descriptors.clone()
        }
    }

    impl StoreTransport for CountingTransport {
        fn segment_name(&self) -> Result<String> {
            Ok("counting-segment".to_string())
        }

        fn rpc_server_address(&self) -> Result<(String, u16)> {
            Ok(("127.0.0.1".to_string(), 0))
        }

        fn open_segment(&self, _segment_name: &str) -> Result<u64> {
            let mut state = self.state.lock();
            state.open_calls += 1;
            Ok(7)
        }

        fn close_segment(&self, _handle: u64) -> Result<()> {
            self.state.lock().close_calls += 1;
            Ok(())
        }

        fn get_segment_info(&self, _handle: u64) -> Result<SegmentInfo> {
            let mut state = self.state.lock();
            state.info_calls += 1;
            Ok(SegmentInfo {
                kind: SegmentKind::Memory,
                buffers: vec![SegmentBuffer {
                    base: 64,
                    length: 4096,
                    location: "cpu:0".to_string(),
                }],
            })
        }

        fn cache_remote_segment_descriptor(
            &self,
            segment_name: &str,
            descriptor_json: &str,
        ) -> Result<()> {
            self.state
                .lock()
                .cached_descriptors
                .push((segment_name.to_string(), descriptor_json.to_string()));
            Ok(())
        }

        fn allocate_memory(&self, _size: usize, _location: &str) -> Result<*mut c_void> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn free_memory(&self, _addr: *mut c_void) -> Result<()> {
            Ok(())
        }

        fn register_memory(&self, _addr: *mut c_void, _size: usize) -> Result<()> {
            Ok(())
        }

        fn unregister_memory(&self, _addr: *mut c_void, _size: usize) -> Result<()> {
            Ok(())
        }

        fn allocate_batch(&self, _batch_size: usize) -> Result<u64> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn free_batch(&self, _batch_id: u64) -> Result<()> {
            Ok(())
        }

        fn submit(&self, _batch_id: u64, _requests: &[TransferRequest]) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn submit_with_hints(
            &self,
            _batch_id: u64,
            _requests: &[TransferRequest],
            _hints: &TransferBatchHints,
        ) -> Result<()> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn task_status(&self, _batch_id: u64, _task_id: usize) -> Result<TransferProgress> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }

        fn overall_status(&self, _batch_id: u64) -> Result<TransferProgress> {
            Err(StoreError::Unsupported("unused in test".to_string()))
        }
    }

    #[test]
    fn open_segment_with_info_reuses_cached_segment_info() {
        let transport = CountingTransport::default();
        let mut state = StoreState::default();

        let (handle_a, info_a) = state
            .open_segment_with_info(&transport, "remote-a")
            .expect("first open should succeed");
        let (handle_b, info_b) = state
            .open_segment_with_info(&transport, "remote-a")
            .expect("cached open should succeed");

        assert_eq!(handle_a, handle_b);
        assert_eq!(info_a, info_b);
        assert_eq!(transport.counts(), (1, 0, 1));

        state.invalidate_remote_segment("remote-a");
        let (_handle_c, _info_c) = state
            .open_segment_with_info(&transport, "remote-a")
            .expect("reopen after invalidation should succeed");

        assert_eq!(transport.counts(), (2, 0, 2));
    }

    #[test]
    fn invalidate_remote_segment_clears_cached_transport_endpoint_handle() {
        let transport = CountingTransport::default();
        let mut state = StoreState::default();
        let owner = ClientRuntimeId::new("remote-owner", ClientEpoch(1));
        let segment_name = SegmentName::new("logical-segment");
        state.cache_segment_target_metadata(
            &owner,
            &segment_name,
            &[],
            Some("10.0.0.8:12001".to_string()),
            None,
        );

        state
            .open_segment_with_info(&transport, "10.0.0.8:12001")
            .expect("endpoint open should succeed");
        state.invalidate_remote_segment(&segment_name.0);
        assert!(state
            .cached_segment_target_metadata(&owner, &segment_name)
            .is_none());
        state
            .open_segment_with_info(&transport, "10.0.0.8:12001")
            .expect("endpoint should reopen after logical invalidation");

        assert_eq!(transport.counts(), (2, 0, 2));
    }

    #[test]
    fn preload_remote_segment_descriptor_uses_exact_transport_endpoint_metadata() {
        let transport = CountingTransport::default();
        let mut state = StoreState::default();
        let owner = ClientRuntimeId::new("remote-owner", ClientEpoch(1));
        let primary = SegmentName::new("logical-segment");
        let extra = SegmentName::new("logical-segment-ext-1");
        state.cache_segment_target_metadata(
            &owner,
            &primary,
            &[],
            Some("10.0.0.8:12001".to_string()),
            Some("primary-descriptor".to_string()),
        );
        state.cache_segment_target_metadata(
            &owner,
            &extra,
            &[],
            Some("10.0.0.8:12002".to_string()),
            Some("extra-descriptor".to_string()),
        );

        state
            .open_segment_with_info(&transport, "10.0.0.8:12002")
            .expect("endpoint open should succeed");

        assert_eq!(
            transport.cached_descriptors(),
            vec![("10.0.0.8:12002".to_string(), "extra-descriptor".to_string())]
        );
    }

    #[test]
    fn cache_stale_detects_segment_handle_error() {
        let error = StoreError::Transport("stale segment handle".to_string());
        assert!(
            remote_segment_cache_stale(&error),
            "errors containing 'segment handle' should be detected as stale"
        );
    }

    #[test]
    fn cache_stale_detects_outside_segment_error() {
        let error = StoreError::Transport(
            "segment offset 53686206464 length 1540096 is outside segment \
             sm-16--487fdbe0-ext-1 (total_capacity=34359738368, num_buffers=1)"
                .to_string(),
        );
        assert!(
            remote_segment_cache_stale(&error),
            "'outside segment' errors should trigger cache refresh"
        );
    }

    #[test]
    fn cache_stale_rejects_unrelated_transport_error() {
        let error = StoreError::Transport("connection refused".to_string());
        assert!(
            !remote_segment_cache_stale(&error),
            "unrelated transport errors should not be treated as stale cache"
        );
    }

    #[test]
    fn cache_stale_rejects_allocator_error() {
        let error = StoreError::Allocator("out of memory".to_string());
        assert!(
            !remote_segment_cache_stale(&error),
            "allocator errors should not be treated as stale cache"
        );
    }

    #[test]
    fn cache_refreshable_accepts_transport_errors() {
        let error = StoreError::Transport("any transport error".to_string());
        assert!(remote_segment_cache_refreshable(&error));
    }

    #[test]
    fn cache_refreshable_accepts_not_found_errors() {
        let error = StoreError::NotFound("segment not found".to_string());
        assert!(remote_segment_cache_refreshable(&error));
    }

    #[test]
    fn cache_refreshable_rejects_allocator_errors() {
        let error = StoreError::Allocator("allocator failure".to_string());
        assert!(!remote_segment_cache_refreshable(&error));
    }
    fn test_runtime() -> ClientRuntimeId {
        ClientRuntimeId::new("test-owner", ClientEpoch(1))
    }

    fn test_route(key: &str, runtime: &ClientRuntimeId) -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey(key.to_string()),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: runtime.clone(),
                segment_name: SegmentName("seg-0".to_string()),
                allocation_id: None,
                offset: Some(0),
                segment_offset: 0,
                length: 1024,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,
            nof_backing: None,
        }
    }

    #[test]
    fn clock_reuses_slots_after_mixed_replica_and_key_removal() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();
        let mut route = test_route("multi", &runtime);
        let mut second = route.replicas[0].clone();
        second.segment_offset = 1024;
        route.replicas.push(second);
        clock.track_route(&route, &runtime);
        let id = clock.entries[0].as_ref().unwrap().id.clone();
        clock.remove_id(&id);
        clock.remove_id(&id);
        clock.remove_key(&route.key);
        clock.remove_key(&route.key);
        assert_eq!(clock.free_slots.len(), 2);
        clock.track_route(&test_route("new-a", &runtime), &runtime);
        clock.track_route(&test_route("new-b", &runtime), &runtime);
        assert_eq!(clock.entries.len(), 2);
        assert!(clock.free_slots.is_empty());
        assert_eq!(clock.by_id.len(), 2);
        assert_eq!(clock.by_key.len(), 2);
        assert!(clock.entries.iter().all(Option::is_some));
    }

    #[test]
    fn clock_slot_reuse_preserves_live_indexes_during_churn() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();
        let mut live = BTreeSet::new();
        let mut seed = 7u64;
        for _ in 0..4096 {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let route = test_route(&format!("key-{}", (seed >> 32) % 64), &runtime);
            if seed & 1 == 0 {
                clock.sync_route(&route, &runtime);
                live.insert(route.key.clone());
            } else {
                clock.untrack_route(&route, &runtime);
                live.remove(&route.key);
            }
            let vacant = clock.free_slots.iter().copied().collect::<BTreeSet<_>>();
            assert_eq!(vacant.len(), clock.free_slots.len());
            assert_eq!(clock.by_key.keys().cloned().collect::<BTreeSet<_>>(), live);
            assert_eq!(clock.by_id.len(), live.len());
            for (index, entry) in clock.entries.iter().enumerate() {
                match entry {
                    Some(entry) => {
                        assert!(!vacant.contains(&index));
                        assert_eq!(clock.by_id.get(&entry.id), Some(&index));
                        assert!(clock.by_key[&entry.id.route_key].contains(&index));
                    }
                    None => assert!(vacant.contains(&index)),
                }
            }
        }
        assert!(clock.entries.len() <= 64);
    }

    #[test]
    fn clock_reused_slots_reset_heat_without_resetting_scan_cursors() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();
        let hot = test_route("hot", &runtime);
        let removed_hot = test_route("removed-hot", &runtime);
        let removed_fresh = test_route("removed-fresh", &runtime);
        clock.track_route(&hot, &runtime);
        clock.track_route(&removed_hot, &runtime);
        clock.track_fresh_route(&removed_fresh, &runtime);
        clock.mark_hot_keys(&[hot.key.clone(), removed_hot.key.clone()]);
        clock.hand = 2;
        clock.stale_reclaim_hand = 1;
        clock.remove_key(&removed_hot.key);
        clock.untrack_route(&removed_fresh, &runtime);

        let cold = test_route("new-cold", &runtime);
        let fresh = test_route("new-fresh", &runtime);
        clock.track_route(&cold, &runtime);
        clock.track_fresh_route(&fresh, &runtime);
        assert_eq!(clock.entries.len(), 3);
        assert_eq!((clock.hand, clock.stale_reclaim_hand), (2, 1));
        let cold_entry = clock.entries[clock.by_key[&cold.key][0]].as_ref().unwrap();
        assert!(!cold_entry.hot && !cold_entry.fresh_write);
        assert_eq!(cold_entry.hot_credit, 0);
        let hot_entry = clock.entries[clock.by_key[&hot.key][0]].as_ref().unwrap();
        assert_eq!(hot_entry.hot_credit, StorageClockState::READ_HOT_CLOCK_CREDIT);

        // Reuse may change the order of equally cold entries, but must not
        // transfer a removed entry's heat or bypass the fresh-write grace.
        let victim = clock.pick_victim_clock(None).unwrap();
        assert_eq!(victim.route_key, cold.key);
        clock.remove_id(&victim);
        let victim = clock.pick_victim_clock(None).unwrap();
        assert_eq!(victim.route_key, fresh.key);
        clock.remove_id(&victim);
        let victim = clock.pick_victim_clock(None).unwrap();
        assert_eq!(victim.route_key, hot.key);
        clock.remove_id(&victim);
        assert!(clock.pick_victim_clock(None).is_none());
        assert!(clock.reclaim_candidate_keys(None, 3).is_empty());
    }

    #[test]
    fn clock_nof_resync_reuses_slots_and_removes_stale_targets() {
        let runtime = test_runtime();
        let mut route = test_route("nof-reuse", &runtime);
        route.replicas.clear();
        route.nof_backing = Some(mooncake_store_core::NofBackingRoute {
            owner: runtime.clone(),
            target_id: "primary".to_string(),
            object_locator: "primary-locator".to_string(),
            length: 1024,
            checksum: None,
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: vec![mooncake_store_core::NofBackingReplica {
                owner: runtime.clone(),
                target_id: "secondary".to_string(),
                object_locator: "secondary-locator".to_string(),
            }],
        });
        let mut clock = StorageClockState::default();
        clock.sync_nof_route(&route, &runtime, true);
        for _ in 0..8 {
            clock.sync_nof_route(&route, &runtime, false);
            assert_eq!(clock.entries.len(), 2);
            assert_eq!(clock.by_id.len(), 2);
            assert!(clock.free_slots.is_empty());
        }
        route.nof_backing.as_mut().unwrap().replicas.clear();
        clock.sync_nof_route(&route, &runtime, false);
        assert_eq!(clock.free_slots.len(), 1);
        assert!(clock
            .pick_victim_clock(Some(&nof_backing_clock_segment("secondary")))
            .is_none());
        let victim = clock
            .pick_victim_clock(Some(&nof_backing_clock_segment("primary")))
            .unwrap();
        assert_eq!(victim.route_key, route.key);
        clock.remove_id(&victim);
        clock.remove_key(&route.key);
        assert_eq!(clock.free_slots.len(), 2);
        assert!(clock.by_id.is_empty() && clock.by_key.is_empty());
    }

    #[test]
    fn clock_rebuild_discards_old_vacancies() {
        let runtime = test_runtime();
        let tracker = HotReplicaTracker::default();
        for key in ["old-a", "old-b", "old-c"] {
            tracker.track_route(&test_route(key, &runtime), &runtime);
        }
        tracker.remove_key(&ObjectKey::new("old-c"));
        assert_eq!(tracker.clock.lock().free_slots, vec![2]);

        tracker.rebuild(&[test_route("rebuilt", &runtime)], &runtime);
        assert!(tracker.clock.lock().free_slots.is_empty());
        tracker.track_route(&test_route("after-rebuild", &runtime), &runtime);
        let clock = tracker.clock.lock();
        assert_eq!(clock.entries.len(), 2);
        assert_eq!(clock.by_id.len(), 2);
        assert!(clock.entries.iter().all(Option::is_some));
    }

    #[test]
    fn tracked_route_hits_do_not_accumulate_pending_hot_keys() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();

        for round in 0..100 {
            let key = format!("object-{round}");
            let route = test_route(&key, &runtime);
            clock.track_route(&route, &runtime);
            clock.mark_hot_keys(&[ObjectKey(key)]);
        }

        assert_eq!(
            clock.pending_hot_keys.len(),
            0,
            "hits for already tracked routes should be applied directly"
        );
    }

    #[test]
    fn pending_route_hit_is_consumed_when_route_arrives() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();
        let mut route = test_route("delayed-route", &runtime);
        let mut second_replica = route.replicas[0].clone();
        second_replica.segment_name = SegmentName("seg-1".to_string());
        route.replicas.push(second_replica);

        clock.mark_hot_keys(std::slice::from_ref(&route.key));
        assert_eq!(clock.pending_hot_keys.len(), 1);

        clock.track_route(&route, &runtime);

        assert!(clock.pending_hot_keys.is_empty());
        assert_eq!(
            clock
                .entries
                .iter()
                .filter_map(Option::as_ref)
                .filter(|entry| entry.hot)
                .count(),
            2,
            "the pending hit should mark every local replica hot"
        );
    }

    #[test]
    fn removed_clock_slots_are_recorded_for_constant_time_reuse() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();
        let first = test_route("first", &runtime);
        let second = test_route("second", &runtime);
        clock.track_route(&first, &runtime);
        clock.track_route(&second, &runtime);

        clock.remove_key(&first.key);

        assert_eq!(clock.free_slots, vec![0]);

        let third = test_route("third", &runtime);
        clock.track_route(&third, &runtime);

        assert!(clock.free_slots.is_empty());
        assert_eq!(clock.entries.len(), 2, "the vacant slot should be reused");
        let mut occupied_slots = clock.by_id.values().copied().collect::<Vec<_>>();
        occupied_slots.sort_unstable();
        assert_eq!(occupied_slots, vec![0, 1]);
    }

    #[test]
    fn rebuild_without_clear_degrades_clock_to_all_hot() {
        let runtime = test_runtime();

        let mut clock = StorageClockState::default();
        let mut routes = Vec::new();
        for index in 0..8 {
            let route = test_route(&format!("k-{index}"), &runtime);
            clock.track_route(&route, &runtime);
            routes.push(route);
        }
        for index in 0..8 {
            clock
                .pending_hot_keys
                .insert(ObjectKey(format!("k-{index}")));
        }
        assert_eq!(clock.pending_hot_keys.len(), 8);

        // Simulate rebuild WITHOUT clearing pending_hot_keys (old behavior).
        let mut rebuilt = StorageClockState {
            pending_hot_keys: clock.pending_hot_keys.clone(),
            ..StorageClockState::default()
        };
        for route in &routes {
            rebuilt.track_route(route, &runtime);
        }

        let cold_before_rotation = rebuilt
            .entries
            .iter()
            .filter_map(Option::as_ref)
            .filter(|entry| !entry.hot)
            .count();
        assert_eq!(
            cold_before_rotation, 0,
            "without clear, every rebuilt entry starts hot — clock loses temporal discrimination"
        );
    }

    #[test]
    fn rebuild_with_fresh_clock_restores_cold_entries() {
        let runtime = test_runtime();

        let mut clock = StorageClockState::default();
        let mut routes = Vec::new();
        for index in 0..8 {
            let route = test_route(&format!("k-{index}"), &runtime);
            clock.track_route(&route, &runtime);
            routes.push(route);
        }
        for index in 0..8 {
            clock.mark_hot_keys(&[ObjectKey(format!("k-{index}"))]);
        }

        // Simulate rebuild WITHOUT inheriting pending_hot_keys (fixed behavior).
        let mut rebuilt = StorageClockState::default();
        for route in &routes {
            rebuilt.track_route(route, &runtime);
        }

        let cold_count = rebuilt
            .entries
            .iter()
            .filter_map(Option::as_ref)
            .filter(|entry| !entry.hot)
            .count();
        assert_eq!(
            cold_count, 8,
            "fresh rebuild starts all entries cold — no stale hotness inherited"
        );

        // After rebuild, mark_hot_keys only protects genuinely active keys.
        rebuilt.mark_hot_keys(&[ObjectKey("k-0".to_string())]);

        let hot_count = rebuilt
            .entries
            .iter()
            .filter_map(Option::as_ref)
            .filter(|entry| entry.hot)
            .count();
        assert_eq!(
            hot_count, 1,
            "only k-0 is hot — clock distinguishes one genuinely active key"
        );

        // pick_victim should return a cold entry, never the hot k-0.
        let victim = rebuilt
            .pick_victim(None, ColdTierEvictionPriorityPolicy::Clock, 0)
            .expect("should find a cold victim");
        assert_ne!(
            victim.route_key,
            ObjectKey("k-0".to_string()),
            "genuinely hot key k-0 must survive eviction"
        );
    }

    #[test]
    fn rebuilt_storage_clocks_are_stable_per_runtime_and_desynchronized_across_runtimes() {
        let runtime_a = ClientRuntimeId::new("storage-owner-a", ClientEpoch(1));
        let runtime_a_same = ClientRuntimeId::new("storage-owner-a", ClientEpoch(1));
        let runtime_a_next_epoch = ClientRuntimeId::new("storage-owner-a", ClientEpoch(2));
        let runtime_b = ClientRuntimeId::new("storage-owner-b", ClientEpoch(1));
        let runtime_c = ClientRuntimeId::new("storage-owner-c", ClientEpoch(1));
        let first_batch = |runtime: &ClientRuntimeId| {
            let routes = (0..4096)
                .map(|index| test_route(&format!("shared-{index:04}"), runtime))
                .collect::<Vec<_>>();
            let tracker = HotReplicaTracker::default();
            tracker.rebuild(&routes, runtime);
            (0..BACKGROUND_EVICTION_BATCH_SIZE)
                .map(|_| {
                    tracker
                        .pick_victim(None, ColdTierEvictionPriorityPolicy::Clock, 0)
                        .expect("runtime should select a full eviction batch")
                        .route_key
                })
                .collect::<BTreeSet<_>>()
        };

        let victims_a = first_batch(&runtime_a);
        let victims_a_rebuilt = first_batch(&runtime_a_same);
        let victims_a_next_epoch = first_batch(&runtime_a_next_epoch);
        let victims_b = first_batch(&runtime_b);
        let victims_c = first_batch(&runtime_c);

        assert_eq!(victims_a.len(), BACKGROUND_EVICTION_BATCH_SIZE);
        assert_eq!(victims_a, victims_a_rebuilt);
        assert_ne!(victims_a, victims_a_next_epoch);
        assert_eq!(victims_b.len(), BACKGROUND_EVICTION_BATCH_SIZE);
        assert_eq!(victims_c.len(), BACKGROUND_EVICTION_BATCH_SIZE);
        assert!(
            victims_a.is_disjoint(&victims_b)
                && victims_a.is_disjoint(&victims_c)
                && victims_b.is_disjoint(&victims_c),
            "replica owners with the same ordered routes must start with disjoint eviction batches"
        );
    }

    #[test]
    fn stale_reclaim_candidate_cursor_advances_with_scan_limit() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();
        for index in 0..3 {
            let route = test_route(&format!("k-{index}"), &runtime);
            clock.track_route(&route, &runtime);
        }

        assert_eq!(
            clock.reclaim_candidate_keys(None, 1),
            vec![ObjectKey("k-0".to_string())]
        );
        assert_eq!(
            clock.reclaim_candidate_keys(None, 1),
            vec![ObjectKey("k-1".to_string())]
        );
        assert_eq!(
            clock.reclaim_candidate_keys(None, 1),
            vec![ObjectKey("k-2".to_string())]
        );
        assert_eq!(
            clock.reclaim_candidate_keys(None, 1),
            vec![ObjectKey("k-0".to_string())],
            "bounded reclaim pre-pass should make cursor progress instead of rescanning one key"
        );
    }

    #[test]
    fn sync_route_after_fresh_rebuild_does_not_resurrect_stale_hotness() {
        let runtime = test_runtime();

        let mut clock = StorageClockState::default();
        let mut routes = Vec::new();
        for index in 0..4 {
            let route = test_route(&format!("k-{index}"), &runtime);
            clock.track_route(&route, &runtime);
            routes.push(route);
        }
        clock.mark_hot_keys(&[
            ObjectKey("k-0".to_string()),
            ObjectKey("k-1".to_string()),
            ObjectKey("k-2".to_string()),
            ObjectKey("k-3".to_string()),
        ]);

        // Rebuild with fresh clock (no inherited pending_hot_keys).
        let mut rebuilt = StorageClockState::default();
        for route in &routes {
            rebuilt.track_route(route, &runtime);
        }

        // Simulate CAS failure: sync_route re-inserts k-2.
        rebuilt.sync_route(&routes[2], &runtime);

        let k2_entry = rebuilt
            .entries
            .iter()
            .filter_map(Option::as_ref)
            .find(|entry| entry.id.route_key == ObjectKey("k-2".to_string()))
            .expect("k-2 should be tracked after sync_route");
        assert!(
            !k2_entry.hot,
            "sync_route must not resurrect stale hotness for k-2 after fresh rebuild"
        );
    }

    #[test]
    fn fresh_writes_get_one_clock_grace_but_read_hot_still_wins() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();
        let hot_route = test_route("hot", &runtime);
        let fresh_route = test_route("fresh", &runtime);

        clock.track_fresh_route(&hot_route, &runtime);
        clock.track_fresh_route(&fresh_route, &runtime);
        clock.mark_hot_keys(std::slice::from_ref(&hot_route.key));

        let victim = clock
            .pick_victim(None, ColdTierEvictionPriorityPolicy::Clock, 0)
            .expect("clock should pick a victim after grace expires");
        assert_eq!(
            victim.route_key, fresh_route.key,
            "read-hot routes should outlive write-fresh routes during the same CLOCK scan"
        );
    }

    #[test]
    fn read_hot_route_gets_more_clock_credit_than_fresh_write() {
        let runtime = test_runtime();
        let mut clock = StorageClockState::default();
        let hot_route = test_route("hot", &runtime);
        let fresh_a = test_route("fresh-a", &runtime);
        let fresh_b = test_route("fresh-b", &runtime);

        clock.track_fresh_route(&hot_route, &runtime);
        clock.track_fresh_route(&fresh_a, &runtime);
        clock.track_fresh_route(&fresh_b, &runtime);
        clock.mark_hot_keys(std::slice::from_ref(&hot_route.key));

        let first = clock
            .pick_victim(None, ColdTierEvictionPriorityPolicy::Clock, 0)
            .expect("clock should evict a fresh route before read-hot route");
        let second = clock
            .pick_victim(None, ColdTierEvictionPriorityPolicy::Clock, 0)
            .expect("clock should still evict fresh routes before read-hot route");

        assert_ne!(first.route_key, hot_route.key);
        assert_ne!(second.route_key, hot_route.key);
    }

    #[test]
    fn managed_nof_clock_tracks_only_locally_owned_targets() {
        let runtime = test_runtime();
        let remote = ClientRuntimeId::new("remote-owner", ClientEpoch(1));
        let mut route = test_route("managed-nof", &runtime);
        route.replicas.clear();
        route.nof_backing = Some(mooncake_store_core::NofBackingRoute {
            owner: runtime.clone(),
            target_id: "nof-local".to_string(),
            object_locator: "local-locator".to_string(),
            length: 1024,
            checksum: None,
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: vec![mooncake_store_core::NofBackingReplica {
                owner: remote,
                target_id: "nof-remote".to_string(),
                object_locator: "remote-locator".to_string(),
            }],
        });

        let mut clock = StorageClockState::default();
        let unrelated = ObjectKey("unrelated".to_string());
        clock.mark_tracked_hot_keys(std::slice::from_ref(&unrelated));
        assert!(clock.pending_hot_keys.is_empty());

        clock.sync_nof_route(&route, &runtime, false);
        clock.mark_tracked_hot_keys(&[route.key.clone(), unrelated.clone()]);
        assert!(!clock.pending_hot_keys.contains(&unrelated));
        assert!(clock
            .entries
            .iter()
            .filter_map(Option::as_ref)
            .any(|entry| entry.id.route_key == route.key && entry.hot));

        let mut pending_delete = route.clone();
        pending_delete.nof_backing.as_mut().unwrap().state =
            mooncake_store_core::ColdBackingState::PendingDelete;
        clock.sync_nof_route(&pending_delete, &runtime, false);
        assert!(
            !clock.by_key.contains_key(&route.key),
            "a non-materialized update must remove the old NoF clock entry"
        );

        clock.sync_nof_route(&route, &runtime, false);
        let mut without_nof = route.clone();
        without_nof.nof_backing = None;
        clock.sync_nof_route(&without_nof, &runtime, false);
        assert!(
            !clock.by_key.contains_key(&route.key),
            "removing NoF backing must remove the old NoF clock entry"
        );

        clock.sync_nof_route(&route, &runtime, false);

        let victim = clock
            .pick_victim(
                Some(&nof_backing_clock_segment("nof-local")),
                ColdTierEvictionPriorityPolicy::Clock,
                0,
            )
            .expect("locally owned NoF target should be tracked");
        assert_eq!(victim.route_key, route.key);
        assert!(clock
            .pick_victim(
                Some(&nof_backing_clock_segment("nof-remote")),
                ColdTierEvictionPriorityPolicy::Clock,
                0,
            )
            .is_none());
    }

    #[test]
    fn nof_clock_consumes_read_before_tracking_for_all_local_targets() {
        let runtime = test_runtime();
        let remote = ClientRuntimeId::new("remote-owner", ClientEpoch(1));
        let mut route = test_route("managed-nof-read-before-track", &runtime);
        route.replicas.clear();
        route.nof_backing = Some(mooncake_store_core::NofBackingRoute {
            owner: runtime.clone(),
            target_id: "nof-local-primary".to_string(),
            object_locator: "local-primary-locator".to_string(),
            length: 1024,
            checksum: None,
            state: mooncake_store_core::ColdBackingState::Materialized,
            replicas: vec![
                mooncake_store_core::NofBackingReplica {
                    owner: runtime.clone(),
                    target_id: "nof-local-replica".to_string(),
                    object_locator: "local-replica-locator".to_string(),
                },
                mooncake_store_core::NofBackingReplica {
                    owner: remote,
                    target_id: "nof-remote".to_string(),
                    object_locator: "remote-locator".to_string(),
                },
            ],
        });

        let mut clock = StorageClockState::default();
        clock.mark_hot_keys(std::slice::from_ref(&route.key));
        assert!(clock.pending_hot_keys.contains(&route.key));

        clock.sync_nof_route(&route, &runtime, false);

        assert!(!clock.pending_hot_keys.contains(&route.key));
        let indices = clock
            .by_key
            .get(&route.key)
            .expect("local NoF targets should be tracked");
        assert_eq!(indices.len(), 2);
        for index in indices {
            let entry = clock
                .entries
                .get(*index)
                .and_then(Option::as_ref)
                .expect("tracked NoF target should have a clock entry");
            assert!(entry.hot);
            assert_eq!(entry.hot_credit, StorageClockState::READ_HOT_CLOCK_CREDIT);
            assert!(!entry.fresh_write);
        }

        clock.sync_nof_route(&route, &runtime, false);

        let indices = clock
            .by_key
            .get(&route.key)
            .expect("local NoF targets should remain tracked");
        for index in indices {
            let entry = clock
                .entries
                .get(*index)
                .and_then(Option::as_ref)
                .expect("tracked NoF target should have a clock entry");
            assert!(!entry.hot);
            assert_eq!(entry.hot_credit, 0);
            assert!(!entry.fresh_write);
        }
    }
}

impl StorageOwnerState {
    fn report_route_hits(&self, keys: &[ObjectKey]) -> RouteTrafficReport {
        let report = self.hot_replicas.clock.lock().mark_hot_keys(keys);
        if !self.nof_backing_tracking_enabled {
            return report;
        }
        self.nof_backings.clock.lock().mark_tracked_hot_keys(keys);
        report
    }

    fn track_routes(&self, routes: &[ObjectRoute]) -> RouteTrafficReport {
        let bytes = routes
            .iter()
            .map(|route| route_storage_bytes(route, &self.runtime))
            .sum();
        let mut allocator = self.allocator.lock();
        let mut clock = self.hot_replicas.clock.lock();
        for route in routes {
            allocator.clear_pending_route(route, &self.runtime);
            clock.sync_fresh_route(route, &self.runtime);
        }
        if self.nof_backing_tracking_enabled {
            let mut nof_clock = self.nof_backings.clock.lock();
            for route in routes {
                nof_clock.sync_nof_route(route, &self.runtime, true);
            }
        }
        RouteTrafficReport::new(routes.len(), bytes)
    }

    fn track_routes_with_initial_cold_backing(
        &self,
        routes: &[ObjectRoute],
    ) -> Result<RouteTrafficReport> {
        let mut tracked_routes = Vec::with_capacity(routes.len());
        for route in routes {
            tracked_routes.push(
                cold_tier::publish_initial_write_cold_backing(self, route)?
                    .unwrap_or_else(|| route.clone()),
            );
        }
        Ok(self.track_routes(&tracked_routes))
    }

    fn track_route(&self, route: &ObjectRoute) {
        let mut allocator = self.allocator.lock();
        let mut clock = self.hot_replicas.clock.lock();
        allocator.clear_pending_route(route, &self.runtime);
        clock.track_fresh_route(route, &self.runtime);
        if self.nof_backing_tracking_enabled {
            self.nof_backings
                .clock
                .lock()
                .sync_nof_route(route, &self.runtime, true);
        }
    }

    fn untrack_route(&self, route: &ObjectRoute) {
        self.hot_replicas
            .clock
            .lock()
            .untrack_route(route, &self.runtime);
        if self.nof_backing_tracking_enabled {
            self.nof_backings.clock.lock().remove_key(&route.key);
        }
    }

    fn sync_route(&self, route: &ObjectRoute) {
        let mut allocator = self.allocator.lock();
        let mut clock = self.hot_replicas.clock.lock();
        allocator.clear_pending_route(route, &self.runtime);
        clock.sync_route(route, &self.runtime);
        if self.nof_backing_tracking_enabled {
            self.nof_backings
                .clock
                .lock()
                .sync_nof_route(route, &self.runtime, false);
        }
    }

    fn enqueue_delete_pending(&self, entries: &[DeletePendingEntry]) -> Result<usize> {
        let now_ms = now_ms();
        let mut accepted = 0usize;
        for entry in entries {
            if let Some(cold) = entry.cold_backing.as_ref() {
                accepted = accepted
                    .saturating_add(self.enqueue_cold_delete_pending(entry, cold, now_ms)?);
                continue;
            }
            let Some(route_identity) = entry.identity.as_ref() else {
                continue;
            };
            if route_identity.owner != self.runtime {
                return Err(StoreError::InvalidState(format!(
                    "delete pending identity owner {} does not match allocator owner {}",
                    route_identity.owner, self.runtime
                )));
            }
            let identity = AllocationIdentity {
                segment_name: route_identity.segment_name.clone(),
                offset_bytes: route_identity.offset_bytes,
                length_bytes: route_identity.length_bytes,
                allocation_id: route_identity.allocation_id,
            };
            let allocation = AllocationSpan {
                segment_name: identity.segment_name.clone(),
                offset_bytes: identity.offset_bytes,
                length_bytes: identity.length_bytes,
            };
            let is_live = {
                let allocator = self.allocator.lock();
                allocator.allocation_identity(&allocation).as_ref() == Some(&identity)
            };
            if !is_live {
                // A duplicate notification may arrive after the local allocator has
                // already released this generation. Treat it as idempotently accepted.
                accepted = accepted.saturating_add(1);
                continue;
            }

            let accepted_delete_pending = {
                let mut pending = self.pending_deletes.lock();
                if !pending.entries.contains_key(&identity)
                    && pending.entries.len() >= PENDING_DELETE_MAX_ENTRIES
                {
                    debug!(
                        runtime = %self.runtime,
                        limit = PENDING_DELETE_MAX_ENTRIES,
                        "dropping best-effort delete pending notification because owner queue is full"
                    );
                    false
                } else {
                    pending.insert(PendingDelete {
                        route_key: entry.route_key.clone(),
                        identity: identity.clone(),
                        grace_ms: entry.reclaim_grace_ms,
                        expected_route_version: entry.expected_route_version,
                        prepare_only: entry.prepare_only,
                        prepared_at_ms: now_ms,
                        confirmed_absent_at_ms: None,
                        confirmation_attempts: 0,
                        next_attempt_at_ms: now_ms.saturating_add(entry.reclaim_grace_ms),
                        confirmation_in_progress: false,
                        deferred: false,
                    });
                    self.refresh_pending_delete_next_due_at_ms(&pending);
                    true
                }
            };
            if !accepted_delete_pending {
                continue;
            }

            // Retain the PendingPublish fence if a best-effort notification is dropped.
            // Clear it only after DeletePending owns the exact generation. Release
            // the queue lock before reacquiring the allocator to preserve lock order.
            let mut allocator = self.allocator.lock();
            if allocator.allocation_identity(&allocation).as_ref() == Some(&identity) {
                allocator.clear_pending_identity(&identity);
            }
            accepted = accepted.saturating_add(1);
        }
        Ok(accepted)
    }

    fn refresh_pending_delete_next_due_at_ms(&self, pending: &PendingDeleteQueue) {
        let next_due_at_ms = pending
            .ready
            .first()
            .map_or(u64::MAX, |entry| entry.next_attempt_at_ms);
        self.pending_delete_next_due_at_ms
            .store(next_due_at_ms, Ordering::Release);
    }

    pub(super) fn process_due_delete_pending_for_reserve(&self) -> Result<usize> {
        let next_due_at_ms = self.pending_delete_next_due_at_ms.load(Ordering::Acquire);
        if next_due_at_ms == u64::MAX || next_due_at_ms > now_ms() {
            return Ok(0);
        }
        self.process_pending_deletes(PENDING_DELETE_CONFIRMATION_BATCH)
    }

    fn delete_pending_contains(&self, identity: &AllocationIdentity) -> bool {
        self.pending_deletes.lock().entries.contains_key(identity)
    }

    fn claim_due_pending_deletes(&self, now_ms: u64, limit: usize) -> Vec<PendingDelete> {
        let mut pending = self.pending_deletes.lock();
        let claimed = pending.claim_due(now_ms, limit);
        self.refresh_pending_delete_next_due_at_ms(&pending);
        claimed
    }

    fn complete_pending_delete(&self, identity: &AllocationIdentity) {
        let mut pending = self.pending_deletes.lock();
        pending.complete(identity);
        self.refresh_pending_delete_next_due_at_ms(&pending);
    }

    fn defer_pending_delete(&self, identity: &AllocationIdentity, reason: &str) {
        let now_ms = now_ms();
        let (attempts, became_deferred) = {
            let mut pending = self.pending_deletes.lock();
            let Some((_, attempts, became_deferred)) = pending.defer(
                identity,
                now_ms,
                PENDING_DELETE_RETRY_BASE_MS,
                PENDING_DELETE_RETRY_MAX_MS,
                PENDING_DELETE_WARN_AFTER_ATTEMPTS,
            ) else {
                return;
            };
            self.refresh_pending_delete_next_due_at_ms(&pending);
            (attempts, became_deferred)
        };
        if became_deferred {
            warn!(
                runtime = %self.runtime,
                segment = %identity.segment_name.0,
                offset_bytes = identity.offset_bytes,
                length_bytes = identity.length_bytes,
                allocation_id = identity.allocation_id,
                attempts,
                reason,
                "delete pending remains deferred after bounded retries"
            );
        } else {
            debug!(
                runtime = %self.runtime,
                segment = %identity.segment_name.0,
                offset_bytes = identity.offset_bytes,
                allocation_id = identity.allocation_id,
                attempts,
                reason,
                "delete pending retry scheduled"
            );
        }
    }

    fn enqueue_cold_delete_pending(
        &self,
        entry: &DeletePendingEntry,
        cold: &crate::control_plane::ColdDeleteTarget,
        now_ms: u64,
    ) -> Result<usize> {
        if cold.owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "cold delete pending owner {} does not match storage owner {}",
                cold.owner, self.runtime
            )));
        }
        let identity = cold.clone();
        let mut pending = self.pending_cold_deletes.lock();
        if !pending.entries.contains_key(&identity)
            && pending.entries.len() >= PENDING_DELETE_MAX_ENTRIES
        {
            // Unlike DRAM (which has the allocator PendingPublish fence as a
            // backstop), a dropped cold token under EmbeddedWrh has no fallback
            // index scan, so surface the drop.
            warn!(
                runtime = %self.runtime,
                limit = PENDING_DELETE_MAX_ENTRIES,
                cold_tier_id = %identity.cold_tier_id,
                "dropping best-effort cold delete pending notification because owner queue is full"
            );
            return Ok(0);
        }
        let inserted = pending.insert(PendingColdDelete {
            route_key: entry.route_key.clone(),
            expected_route_version: entry.expected_route_version,
            prepare_only: entry.prepare_only,
            identity: identity.clone(),
            grace_ms: entry.reclaim_grace_ms,
            prepared_at_ms: now_ms,
            confirmed_absent_at_ms: None,
            confirmation_attempts: 0,
            next_attempt_at_ms: now_ms.saturating_add(entry.reclaim_grace_ms),
            confirmation_in_progress: false,
            deferred: false,
        });
        Ok(usize::from(inserted))
    }

    fn claim_due_cold_deletes(&self, now_ms: u64, limit: usize) -> Vec<PendingColdDelete> {
        self.pending_cold_deletes.lock().claim_due(now_ms, limit)
    }

    fn complete_cold_delete(&self, identity: &crate::control_plane::ColdDeleteTarget) {
        self.pending_cold_deletes.lock().complete(identity);
    }

    fn defer_cold_delete(
        &self,
        identity: &crate::control_plane::ColdDeleteTarget,
        reason: &str,
    ) {
        let now_ms = now_ms();
        let (attempts, became_deferred) = {
            let mut pending = self.pending_cold_deletes.lock();
            let Some((_, attempts, became_deferred)) = pending.defer(
                identity,
                now_ms,
                PENDING_DELETE_RETRY_BASE_MS,
                PENDING_DELETE_RETRY_MAX_MS,
                PENDING_DELETE_WARN_AFTER_ATTEMPTS,
            ) else {
                return;
            };
            (attempts, became_deferred)
        };
        if became_deferred {
            warn!(
                runtime = %self.runtime,
                cold_tier_id = %identity.cold_tier_id,
                object_locator = %identity.object_locator,
                attempts,
                reason,
                "cold delete pending remains deferred after bounded retries"
            );
        } else {
            debug!(
                runtime = %self.runtime,
                cold_tier_id = %identity.cold_tier_id,
                attempts,
                reason,
                "cold delete pending retry scheduled"
            );
        }
    }

    fn process_cold_pending_deletes(&self, limit: usize) -> Result<usize> {
        let pending = self.claim_due_cold_deletes(now_ms(), limit);
        if pending.is_empty() {
            return Ok(0);
        }
        let mut reclaimed = 0usize;
        for token in &pending {
            match self.reclaim_queued_cold_delete(&token.route_key, &token.identity) {
                Ok(true) => {
                    self.complete_cold_delete(&token.identity);
                    reclaimed = reclaimed.saturating_add(1);
                }
                Ok(false) => {
                    self.defer_cold_delete(&token.identity, "cold route not deletable yet");
                }
                Err(error) => {
                    self.defer_cold_delete(&token.identity, "cold delete reclaim failed");
                    debug!(
                        runtime = %self.runtime,
                        error = %error,
                        "cold delete pending reclaim is indeterminate"
                    );
                }
            }
        }
        Ok(reclaimed)
    }

    fn process_pending_deletes(&self, limit: usize) -> Result<usize> {
        let pending = self.claim_due_pending_deletes(now_ms(), limit);
        if pending.is_empty() {
            return Ok(0);
        }
        let identities = pending
            .iter()
            .map(|entry| mooncake_store_core::AllocationRouteIdentity {
                owner: self.runtime.clone(),
                segment_name: entry.identity.segment_name.clone(),
                offset_bytes: entry.identity.offset_bytes,
                length_bytes: entry.identity.length_bytes,
                allocation_id: entry.identity.allocation_id,
            })
            .collect::<Vec<_>>();
        let references = match self
            .route_ops
            .get_allocation_route_reference_keys_consistent(&identities)
        {
            Ok(references) if references.len() == pending.len() => references,
            Ok(references) => {
                for entry in &pending {
                    self.defer_pending_delete(
                        &entry.identity,
                        "incomplete exact delete reference reply",
                    );
                }
                debug!(
                    runtime = %self.runtime,
                    expected = pending.len(),
                    actual = references.len(),
                    "delete pending exact reference query returned an incomplete result"
                );
                return Ok(0);
            }
            Err(error) => {
                for entry in &pending {
                    self.defer_pending_delete(&entry.identity, "delete reference query failed");
                }
                debug!(
                    runtime = %self.runtime,
                    error = %error,
                    entries = pending.len(),
                    "delete pending exact reference query is indeterminate"
                );
                return Ok(0);
            }
        };

        let mut released = 0usize;
        for (entry, reference) in pending.iter().zip(references) {
            match reference.state {
                mooncake_store_core::AllocationRouteReferenceState::Absent => {
                    let now = now_ms();
                    let grace_until = self
                        .pending_deletes
                        .lock()
                        .mark_absent(&entry.identity, now, 0)
                        .unwrap_or(now);
                    if now < grace_until {
                        let mut pending = self.pending_deletes.lock();
                        pending.reschedule(&entry.identity, grace_until);
                        self.refresh_pending_delete_next_due_at_ms(&pending);
                        continue;
                    }
                    if self
                        .read_pin_registry
                        .is_pinned(&entry.identity.segment_name, entry.identity.offset_bytes)
                    {
                        self.defer_pending_delete(
                            &entry.identity,
                            "allocation still has read pins",
                        );
                        continue;
                    }
                    let allocation = AllocationSpan {
                        segment_name: entry.identity.segment_name.clone(),
                        offset_bytes: entry.identity.offset_bytes,
                        length_bytes: entry.identity.length_bytes,
                    };
                    let release = {
                        let mut allocator = self.allocator.lock();
                        match allocator.allocation_identity(&allocation) {
                            Some(current) if current == entry.identity => allocator.release(
                                &self.runtime,
                                &entry.identity.segment_name,
                                entry.identity.offset_bytes,
                                entry.identity.length_bytes,
                                entry.identity.allocation_id,
                            ),
                            Some(_) | None => Ok(()),
                        }
                    };
                    match release {
                        Ok(()) => {
                            self.hot_replicas.clock.lock().remove_id(&ClockEntryId {
                                route_key: entry.route_key.clone(),
                                segment_name: entry.identity.segment_name.clone(),
                                segment_offset: entry.identity.offset_bytes,
                                allocation_id: Some(entry.identity.allocation_id),
                            });
                            self.complete_pending_delete(&entry.identity);
                            released = released.saturating_add(1);
                            registry::record_eviction_guard_event("delete_pending", "released");
                        }
                        Err(error) => {
                            self.defer_pending_delete(
                                &entry.identity,
                                "local allocator release failed",
                            );
                            debug!(
                                runtime = %self.runtime,
                                allocation_id = entry.identity.allocation_id,
                                error = %error,
                                "delete pending local release failed"
                            );
                        }
                    }
                }
                mooncake_store_core::AllocationRouteReferenceState::Exact => {
                    self.pending_deletes
                        .lock()
                        .clear_absent(&entry.identity);
                    let Some(route_key) = reference.route_key else {
                        self.defer_pending_delete(
                            &entry.identity,
                            "exact delete reference omitted route key",
                        );
                        continue;
                    };
                    let replica = ReplicaRoute {
                        owner: self.runtime.clone(),
                        segment_name: entry.identity.segment_name.clone(),
                        allocation_id: Some(entry.identity.allocation_id),
                        offset: None,
                        segment_offset: entry.identity.offset_bytes,
                        length: entry.identity.length_bytes,
                        checksum: None,
                        tier: ReplicaTier::Dram,
                        priority: 0,
                    };
                    match self
                        .route_ops
                        .confirm_replica_reference(&route_key, &replica)
                    {
                        Ok(mooncake_store_core::RouteReplicaReference::Referenced(route)) => {
                            self.track_route(&route);
                            let prepared_expired = entry.prepare_only
                                && now_ms().saturating_sub(entry.prepared_at_ms)
                                    >= PENDING_DELETE_RECLAIM_PREPARE_TTL_MS;
                            if entry.prepare_only && !prepared_expired {
                                self.defer_pending_delete(
                                    &entry.identity,
                                    "prepared delete still references old route",
                                );
                            } else {
                                self.complete_pending_delete(&entry.identity);
                            }
                            registry::record_eviction_guard_event("delete_pending", "retained");
                        }
                        Ok(mooncake_store_core::RouteReplicaReference::Absent) => {
                            self.defer_pending_delete(
                                &entry.identity,
                                "delete reference changed during confirmation",
                            );
                        }
                        Err(error) => {
                            self.defer_pending_delete(
                                &entry.identity,
                                "delete reference confirmation failed",
                            );
                            debug!(
                                runtime = %self.runtime,
                                allocation_id = entry.identity.allocation_id,
                                error = %error,
                                "delete pending exact reference confirmation is indeterminate"
                            );
                        }
                    }
                }
                mooncake_store_core::AllocationRouteReferenceState::Conflict => {
                    self.pending_deletes
                        .lock()
                        .clear_absent(&entry.identity);
                    self.defer_pending_delete(
                        &entry.identity,
                        "delete reference authorities disagree",
                    );
                }
            }
        }
        Ok(released)
    }

    fn evict_until_low_watermark(&self, high_percent: u8, low_percent: u8) -> Result<usize> {
        let tracker = OperationTracker::new("storage_owner_background_eviction");
        let started = Instant::now();
        let used_before = self.allocator.lock().usage_bytes().0;
        let result = (|| {
            let mut evicted = self.process_pending_deletes(PENDING_DELETE_CONFIRMATION_BATCH)?;
            let _ = self.recover_due_pending_publications()?;
            evicted = evicted.saturating_add(
                self.process_pending_evictions(PENDING_EVICTION_CONFIRMATION_BATCH)?,
            );
            let (used_bytes, capacity_bytes) = self.allocator.lock().usage_bytes();
            if capacity_bytes == 0 {
                return Ok(evicted);
            }
            let high_watermark = watermark_bytes(capacity_bytes, high_percent);
            if used_bytes < high_watermark {
                return Ok(evicted);
            }
            let low_watermark = watermark_bytes(capacity_bytes, low_percent);
            loop {
                let (used_bytes, _) = self.allocator.lock().usage_bytes();
                if used_bytes <= low_watermark {
                    break;
                }
                let reclaimed = self.evict_background_batch(
                    used_bytes.saturating_sub(low_watermark),
                    BACKGROUND_EVICTION_BATCH_SIZE,
                )?;
                if reclaimed == 0 {
                    break;
                }
                evicted = evicted.saturating_add(reclaimed);
            }
            if evicted != 0 {
                let (used_bytes, _) = self.allocator.lock().usage_bytes();
                info!(
                    runtime = %self.runtime,
                    evicted,
                    high_percent,
                    low_percent,
                    used_bytes,
                    capacity_bytes,
                    "background storage-owner eviction completed"
                );
            }
            Ok(evicted)
        })();
        let used_after = self.allocator.lock().usage_bytes().0;
        tracker.finish(&result, used_before.saturating_sub(used_after));
        match &result {
            Ok(evicted) if *evicted != 0 => registry::record_eviction("ok", started.elapsed()),
            Err(_) => registry::record_eviction("error", started.elapsed()),
            Ok(_) => {}
        }
        result
    }

    fn evict_background_batch(&self, bytes_to_reclaim: u64, batch_size: usize) -> Result<usize> {
        if batch_size <= 1 {
            return Ok(usize::from(self.evict_one_blocking(None)?));
        }
        let has_usable_cold_tier_device = self.has_usable_cold_tier_device()?;

        let budget = self.hot_replicas.clock.lock().eviction_budget();
        let mut victims = Vec::with_capacity(batch_size.min(budget));
        let mut seen = BTreeSet::new();
        for _ in 0..budget.max(1) {
            let victim = self.hot_replicas.clock.lock().pick_victim(
                None,
                self.offload_priority.eviction_policy,
                self.offload_priority.eviction_scan_limit,
            );
            let Some(victim) = victim else {
                break;
            };
            if seen.insert(victim.clone()) {
                victims.push(victim);
                if victims.len() == batch_size {
                    break;
                }
            }
        }
        if victims.is_empty() {
            return Ok(usize::from(self.evict_one_blocking(None)?));
        }

        let keys = victims
            .iter()
            .map(|victim| victim.route_key.clone())
            .collect::<Vec<_>>();
        let mut routes = self.route_ops.load_routes_bounded(&keys)?;
        if routes.len() != victims.len() {
            return Err(StoreError::InvalidState(format!(
                "background eviction route lookup returned {} results for {} candidates",
                routes.len(),
                victims.len()
            )));
        }
        if has_usable_cold_tier_device {
            let mut pending_routes = Vec::new();
            for route in routes.iter().filter_map(Option::as_ref) {
                if cold_tier::pending_persistent_backing(route).is_some() {
                    pending_routes.push(route.clone());
                    continue;
                }
                if route.state != RouteState::Active
                    || route.cold_backing.is_some()
                    || route.nof_backing.is_some()
                {
                    continue;
                }
                if let Some(published) =
                    cold_tier::publish_pending_cold_backing_for_eviction(self, route)?
                {
                    if cold_tier::pending_persistent_backing(&published).is_some() {
                        pending_routes.push(published);
                    }
                }
            }
            if !pending_routes.is_empty() {
                cold_tier::materialize_pending_offload_routes_bounded(self, &pending_routes)?;
                routes = self.route_ops.load_routes_bounded(&keys)?;
                if routes.len() != victims.len() {
                    return Err(StoreError::InvalidState(format!(
                        "background eviction route reload returned {} results for {} candidates",
                        routes.len(),
                        victims.len()
                    )));
                }
            }
        }

        let mut reclaimed = 0usize;
        let mut planned_bytes = 0u64;
        let mut prepared = Vec::new();
        for (victim, route) in victims.into_iter().zip(routes) {
            let Some(route) = route else {
                if self.reclaim_stale_local_allocations_for_key(&victim.route_key, None)? > 0 {
                    reclaimed = reclaimed.saturating_add(1);
                }
                continue;
            };
            let Some(replica_index) = self.replica_index_for_victim(&route, &victim) else {
                if self.reclaim_stale_local_allocations_for_key(&victim.route_key, Some(&route))?
                    > 0
                {
                    reclaimed = reclaimed.saturating_add(1);
                } else {
                    self.sync_route(&route);
                }
                continue;
            };
            let cold_backing_not_on_disk = route.cold_backing.is_none()
                || route.cold_backing.as_ref().is_some_and(|backing| {
                    backing.state == mooncake_store_core::ColdBackingState::PendingOffload
                });
            if route.state != RouteState::Active {
                if self.evict_candidate(&victim)? {
                    reclaimed = reclaimed.saturating_add(1);
                }
                continue;
            }
            if cold_backing_not_on_disk && has_usable_cold_tier_device {
                if cold_tier::pending_persistent_backing(&route).is_some() {
                    self.hot_replicas
                        .clock
                        .lock()
                        .mark_hot_keys(std::slice::from_ref(&route.key));
                    continue;
                }
                if self.evict_candidate(&victim)? {
                    reclaimed = reclaimed.saturating_add(1);
                }
                continue;
            }

            let next = cold_tier::route_after_replica_eviction(
                &route,
                replica_index,
                cold_backing_not_on_disk,
            );
            let Some(token) = self.begin_eviction(&victim, &route, replica_index, next.clone())?
            else {
                continue;
            };
            planned_bytes = planned_bytes.saturating_add(token.identity.length_bytes);
            prepared.push((route, token, next));
            if planned_bytes >= bytes_to_reclaim {
                break;
            }
        }
        if prepared.is_empty() {
            return Ok(reclaimed);
        }

        let requests = prepared
            .iter()
            .map(|(route, _, next)| RouteCasRequest {
                key: route.key.clone(),
                expected: Some(route.version),
                next: next.clone(),
            })
            .collect::<Vec<_>>();
        let results = match self.route_ops.publish_routes(&requests) {
            Ok(results) => results,
            Err(error) => {
                for (_, token, _) in &prepared {
                    self.defer_pending_eviction(
                        &token.identity,
                        PendingEvictionPhase::PendingConfirmation,
                    );
                    registry::record_eviction_guard_event("route_cas", "outcome_unknown");
                    debug!(
                        runtime = %self.runtime,
                        key = ?token.route_key,
                        segment = %token.identity.segment_name.0,
                        offset_bytes = token.identity.offset_bytes,
                        length_bytes = token.identity.length_bytes,
                        allocation_id = token.identity.allocation_id,
                        error = %error,
                        "batched route eviction CAS outcome is unknown; retaining pending token"
                    );
                }
                return Ok(reclaimed);
            }
        };
        if results.len() != prepared.len() {
            for (_, token, _) in &prepared {
                self.defer_pending_eviction(
                    &token.identity,
                    PendingEvictionPhase::PendingConfirmation,
                );
                registry::record_eviction_guard_event("route_cas", "outcome_unknown");
            }
            return Err(StoreError::InvalidState(format!(
                "background eviction route CAS returned {} results for {} requests",
                results.len(),
                prepared.len()
            )));
        }

        let mut first_error = None;
        let mut evicted_replicas = 0usize;
        let mut evicted_bytes = 0u64;
        for ((_, token, _), result) in prepared.into_iter().zip(results) {
            match result {
                Ok(cas) if cas.applied => match self.commit_eviction(&token) {
                    Ok(true) => {
                        reclaimed = reclaimed.saturating_add(1);
                        evicted_replicas = evicted_replicas.saturating_add(1);
                        evicted_bytes = evicted_bytes.saturating_add(token.identity.length_bytes);
                    }
                    Ok(false) => {}
                    Err(error) => {
                        first_error.get_or_insert(error);
                    }
                },
                Ok(cas) => self.abort_eviction(&token, cas.current.as_ref()),
                Err(error) => {
                    self.defer_pending_eviction(
                        &token.identity,
                        PendingEvictionPhase::PendingConfirmation,
                    );
                    registry::record_eviction_guard_event("route_cas", "outcome_unknown");
                    debug!(
                        runtime = %self.runtime,
                        key = ?token.route_key,
                        segment = %token.identity.segment_name.0,
                        offset_bytes = token.identity.offset_bytes,
                        length_bytes = token.identity.length_bytes,
                        allocation_id = token.identity.allocation_id,
                        error = %error,
                        "batched route eviction CAS outcome is unknown; retaining pending token"
                    );
                }
            }
        }
        if evicted_replicas != 0 {
            registry::record_evicted_replicas(evicted_replicas, evicted_bytes);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(reclaimed),
        }
    }

    fn evict_one_for_reservation(
        self: &Arc<Self>,
        preferred_segment: Option<&SegmentName>,
    ) -> Result<bool> {
        let preferred_segment = preferred_segment.cloned();
        if tokio::runtime::Handle::try_current().is_ok() {
            let storage_owner = Arc::clone(self);
            let thread = std::thread::Builder::new()
                .name(format!(
                    "mooncake-storage-owner-reserve-evict-{}",
                    self.runtime.stable_id.0
                ))
                .spawn(move || {
                    storage_owner.evict_one_for_reservation_blocking(preferred_segment.as_ref())
                })
                .map_err(|error| {
                    StoreError::Transport(format!(
                        "failed to spawn storage-owner eviction worker: {error}"
                    ))
                })?;
            return thread.join().map_err(|_| {
                StoreError::Transport("storage-owner eviction worker panicked".to_string())
            })?;
        }
        self.evict_one_for_reservation_blocking(preferred_segment.as_ref())
    }

    pub(super) fn evict_one_clean_and_reserve_bounded(
        &self,
        length_bytes: u64,
        preferred_segment: Option<&SegmentName>,
    ) -> Result<Option<mooncake_store_core::SegmentReservation>> {
        if self.process_pending_deletes(PENDING_DELETE_CONFIRMATION_BATCH)? > 0 {
            if let Some(reservation) =
                self.reserve_local_after_stale_reclaim(length_bytes, preferred_segment)?
            {
                return Ok(Some(reservation));
            }
        }
        if self.process_pending_evictions(PENDING_EVICTION_CONFIRMATION_BATCH)? > 0 {
            if let Some(reservation) =
                self.reserve_local_after_stale_reclaim(length_bytes, preferred_segment)?
            {
                return Ok(Some(reservation));
            }
        }
        let budget = self.hot_replicas.eviction_budget().max(1);
        for _ in 0..budget {
            let victim = self.hot_replicas.pick_victim(
                preferred_segment,
                self.offload_priority.eviction_policy,
                self.offload_priority.eviction_scan_limit,
            );
            let Some(victim) = victim else {
                break;
            };
            if let Some(reservation) =
                self.evict_candidate_clean_and_reserve(&victim, length_bytes, preferred_segment)?
            {
                return Ok(Some(reservation));
            }
        }
        Ok(None)
    }

    fn reserve_local_after_stale_reclaim(
        &self,
        length_bytes: u64,
        preferred_segment: Option<&SegmentName>,
    ) -> Result<Option<mooncake_store_core::SegmentReservation>> {
        let reservation = {
            let mut allocator = self.allocator.lock();
            let deadline_ms =
                pending_publish_deadline_ms(length_bytes, DEFAULT_TRANSFER_STALL_TIMEOUT, None);
            match preferred_segment {
                Some(segment) => allocator.reserve_specific_with_pending(
                    &self.runtime,
                    segment,
                    length_bytes,
                    deadline_ms,
                ),
                None => {
                    allocator.reserve_any_with_pending(&self.runtime, length_bytes, deadline_ms)
                }
            }
        };
        match reservation {
            Ok(reservation) => Ok(Some(reservation)),
            Err(StoreError::Allocator(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn evict_candidate_clean_and_reserve(
        &self,
        victim: &ClockEntryId,
        length_bytes: u64,
        preferred_segment: Option<&SegmentName>,
    ) -> Result<Option<mooncake_store_core::SegmentReservation>> {
        if self.evict_candidate(victim)? {
            return self.reserve_local_after_stale_reclaim(length_bytes, preferred_segment);
        }
        Ok(None)
    }

    fn evict_one_for_reservation_blocking(
        &self,
        preferred_segment: Option<&SegmentName>,
    ) -> Result<bool> {
        if self.process_pending_deletes(PENDING_DELETE_CONFIRMATION_BATCH)? > 0 {
            return Ok(true);
        }
        if self.recover_due_pending_publications()? > 0 {
            return Ok(true);
        }
        if self.process_pending_evictions(PENDING_EVICTION_CONFIRMATION_BATCH)? > 0 {
            return Ok(true);
        }
        if preferred_segment.is_none()
            && self.has_usable_cold_tier_device()?
            && self.evict_background_batch(u64::MAX, DEFAULT_OFFLOAD_MATERIALIZE_BATCH)? > 0
        {
            return Ok(true);
        }
        if self.evict_one_blocking(preferred_segment)? {
            return Ok(true);
        }
        // A reserve miss that cannot make progress through normal CLOCK
        // eviction is a real capacity-pressure signal. It may bypass the
        // successful-pass interval, but the candidate reconciler still
        // enforces its 100ms singleflight cadence and all safety fences.
        Ok(self.reconcile_stale_clock_candidates(preferred_segment, true)? > 0)
    }

    fn pending_publication_authority_retry_ready(&self, now_ms: u64) -> bool {
        self.pending_publication_authority_retry
            .lock()
            .is_ready(now_ms)
    }

    fn defer_pending_publications_after_authority_failure(
        &self,
        publications: &[PendingPublication],
        now_ms: u64,
    ) {
        if publications.is_empty() {
            return;
        }
        let next_attempt_at_ms = {
            let mut retry = self.pending_publication_authority_retry.lock();
            retry.defer(&self.runtime, now_ms)
        };
        self.retry_pending_publications(publications, next_attempt_at_ms);
    }

    fn reset_pending_publication_authority_retry(&self) {
        let mut retry = self.pending_publication_authority_retry.lock();
        retry.reset();
    }

    pub(super) fn recover_due_pending_publications(&self) -> Result<usize> {
        const PENDING_PUBLICATION_CONFIRMATION_BATCH: usize = 64;
        const PENDING_PUBLICATION_LOCAL_RETRY_DELAY_MS: u64 = 1_000;

        let now_ms = now_ms();
        if !self.pending_publication_authority_retry_ready(now_ms) {
            return Ok(0);
        }

        let publications = self
            .allocator
            .lock()
            .claim_due_pending_publications(now_ms, PENDING_PUBLICATION_CONFIRMATION_BATCH);
        if publications.is_empty() {
            return Ok(0);
        }
        let identities = publications
            .iter()
            .map(|publication| AllocationRouteIdentity {
                owner: self.runtime.clone(),
                segment_name: publication.identity.segment_name.clone(),
                offset_bytes: publication.identity.offset_bytes,
                length_bytes: publication.identity.length_bytes,
                allocation_id: publication.identity.allocation_id,
            })
            .collect::<Vec<_>>();
        let references = match self
            .route_ops
            .get_allocation_route_reference_keys_consistent(&identities)
        {
            Ok(references) if references.len() == publications.len() => references,
            Ok(references) => {
                debug!(
                    runtime = %self.runtime,
                    expected = publications.len(),
                    actual = references.len(),
                    "pending publication confirmation returned an incomplete result"
                );
                self.defer_pending_publications_after_authority_failure(&publications, now_ms);
                return Ok(0);
            }
            Err(error) => {
                debug!(
                    runtime = %self.runtime,
                    pending = publications.len(),
                    error = %error,
                    "pending publication confirmation is indeterminate; retaining reservations"
                );
                self.defer_pending_publications_after_authority_failure(&publications, now_ms);
                return Ok(0);
            }
        };

        self.reset_pending_publication_authority_retry();
        let mut recovered = 0usize;
        let mut authority_retries = Vec::new();
        for (publication, reference) in publications.iter().zip(references) {
            match reference.state {
                mooncake_store_core::AllocationRouteReferenceState::Exact => {
                    let Some(route_key) = reference.route_key else {
                        debug!(
                            runtime = %self.runtime,
                            allocation_id = publication.identity.allocation_id,
                            "exact pending publication reference is missing its route key"
                        );
                        authority_retries.push(publication.clone());
                        continue;
                    };
                    let replica = ReplicaRoute {
                        owner: self.runtime.clone(),
                        segment_name: publication.identity.segment_name.clone(),
                        allocation_id: Some(publication.identity.allocation_id),
                        offset: None,
                        segment_offset: publication.identity.offset_bytes,
                        length: publication.identity.length_bytes,
                        checksum: None,
                        tier: mooncake_store_core::ReplicaTier::Dram,
                        priority: 0,
                    };
                    match self
                        .route_ops
                        .confirm_replica_reference(&route_key, &replica)
                    {
                        Ok(mooncake_store_core::RouteReplicaReference::Referenced(route)) => {
                            match self.track_routes_with_initial_cold_backing(
                                std::slice::from_ref(&route),
                            ) {
                                Ok(_) => registry::record_eviction_guard_event(
                                    "pending_publication",
                                    "finalized",
                                ),
                                Err(error) => {
                                    debug!(
                                        runtime = %self.runtime,
                                        key = %route.key.0,
                                        allocation_id = publication.identity.allocation_id,
                                        error = %error,
                                        "pending publication cold backing preparation failed; retaining reservation"
                                    );
                                    self.retry_pending_publications(
                                        std::slice::from_ref(publication),
                                        now_ms.saturating_add(
                                            PENDING_PUBLICATION_LOCAL_RETRY_DELAY_MS,
                                        ),
                                    );
                                }
                            }
                        }
                        Ok(mooncake_store_core::RouteReplicaReference::Absent) => {
                            self.retry_pending_publications(
                                std::slice::from_ref(publication),
                                now_ms.saturating_add(PENDING_PUBLICATION_LOCAL_RETRY_DELAY_MS),
                            );
                        }
                        Err(error) => {
                            debug!(
                                runtime = %self.runtime,
                                key = %route_key.0,
                                allocation_id = publication.identity.allocation_id,
                                error = %error,
                                "pending publication route confirmation is indeterminate"
                            );
                            authority_retries.push(publication.clone());
                        }
                    }
                }
                mooncake_store_core::AllocationRouteReferenceState::Absent => {
                    let release = self.allocator.lock().release(
                        &self.runtime,
                        &publication.identity.segment_name,
                        publication.identity.offset_bytes,
                        publication.identity.length_bytes,
                        publication.identity.allocation_id,
                    );
                    match release {
                        Ok(()) => {
                            registry::record_eviction_guard_event(
                                "pending_publication",
                                "released",
                            );
                            recovered = recovered.saturating_add(1);
                        }
                        Err(error) => {
                            debug!(
                                runtime = %self.runtime,
                                allocation_id = publication.identity.allocation_id,
                                error = %error,
                                "pending publication release failed; retaining reservation"
                            );
                            self.retry_pending_publications(
                                std::slice::from_ref(publication),
                                now_ms.saturating_add(PENDING_PUBLICATION_LOCAL_RETRY_DELAY_MS),
                            );
                        }
                    }
                }
                mooncake_store_core::AllocationRouteReferenceState::Conflict => {
                    authority_retries.push(publication.clone());
                }
            }
        }
        self.defer_pending_publications_after_authority_failure(&authority_retries, now_ms);
        Ok(recovered)
    }

    fn retry_pending_publications(
        &self,
        publications: &[PendingPublication],
        next_attempt_at_ms: u64,
    ) {
        let mut allocator = self.allocator.lock();
        for publication in publications {
            allocator.retry_pending_publication(publication, next_attempt_at_ms);
        }
    }

    fn evict_one_blocking(&self, preferred_segment: Option<&SegmentName>) -> Result<bool> {
        Ok(self
            .evict_one_blocking_with_outcome(preferred_segment)?
            .made_progress)
    }

    pub(in super) fn evict_one_blocking_with_outcome(
        &self,
        preferred_segment: Option<&SegmentName>,
    ) -> Result<EvictionAttempt> {
        let tracker = OperationTracker::new("storage_owner_evict_one");
        let result = (|| {
            if self.process_pending_deletes(PENDING_DELETE_CONFIRMATION_BATCH)? > 0 {
                return Ok(EvictionAttempt {
                    made_progress: true,
                    ..EvictionAttempt::default()
                });
            }
            if self.process_pending_evictions(PENDING_EVICTION_CONFIRMATION_BATCH)? > 0 {
                return Ok(EvictionAttempt {
                    made_progress: true,
                    ..EvictionAttempt::default()
                });
            }
            let budget = {
                let clock = self.hot_replicas.clock.lock();
                clock.eviction_budget()
            };
            for _ in 0..budget.max(1) {
                let victim = {
                    let mut clock = self.hot_replicas.clock.lock();
                    clock.pick_victim(
                        preferred_segment,
                        self.offload_priority.eviction_policy,
                        self.offload_priority.eviction_scan_limit,
                    )
                };
                let Some(victim) = victim else {
                    break;
                };
                let outcome = self.evict_candidate_with_outcome(&victim)?;
                if outcome.made_progress {
                    return Ok(outcome);
                }
            }
            // No CLOCK victim made progress. A bounded allocator-candidate pass
            // can recover a historical untracked allocation without listing an
            // owner's full route set.
            if self.reconcile_stale_clock_candidates(preferred_segment, false)? > 0 {
                return Ok(EvictionAttempt {
                    made_progress: true,
                    ..EvictionAttempt::default()
                });
            }
            Ok(EvictionAttempt::default())
        })();
        tracker.finish(&result, 0);
        result
    }

    fn reconcile_stale_clock_candidates(
        &self,
        preferred_segment: Option<&SegmentName>,
        force_scan: bool,
    ) -> Result<usize> {
        let requested_at_ms = now_ms();
        let last_completed = self
            .stale_candidate_reconcile_completed_at_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if !force_scan
            && last_completed != 0
            && requested_at_ms.saturating_sub(last_completed)
                < self.offload_priority.stale_candidate_reconcile_interval_ms
        {
            return Ok(0);
        }

        if self
            .stale_candidate_reconcile_in_progress
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            return Ok(0);
        }
        let _scan_guard = StaleCandidateReconcileGuard {
            storage_owner: self,
        };

        let scan_started_at_ms = now_ms();
        let last_completed = self
            .stale_candidate_reconcile_completed_at_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if !force_scan
            && last_completed != 0
            && scan_started_at_ms.saturating_sub(last_completed)
                < self.offload_priority.stale_candidate_reconcile_interval_ms
        {
            return Ok(0);
        }
        // force_scan bypasses the successful-pass interval, not the minimum
        // cadence. This prevents repeated mismatch signals from becoming a
        // tight bounded-query loop while an earlier pass is still settling.
        let last_started = self
            .stale_candidate_reconcile_started_at_ms
            .load(std::sync::atomic::Ordering::Relaxed);
        if last_completed != 0
            && scan_started_at_ms.saturating_sub(last_started)
                < STALE_CANDIDATE_RECONCILE_MIN_INTERVAL_MS
        {
            return Ok(0);
        }
        self.stale_candidate_reconcile_started_at_ms
            .store(scan_started_at_ms, std::sync::atomic::Ordering::Relaxed);

        let (candidate_keys, tracked_identities, legacy_tracked_spans) = {
            let mut clock = self.hot_replicas.clock.lock();
            (
                clock.reclaim_candidate_keys(
                    preferred_segment,
                    self.offload_priority.eviction_scan_limit,
                ),
                clock.tracked_allocation_identities(),
                clock.tracked_allocation_spans(),
            )
        };
        let mut released = 0usize;
        if !candidate_keys.is_empty() {
            let current_routes = match self.route_ops.load_routes_bounded(&candidate_keys) {
                Ok(routes) => routes,
                Err(error) => {
                    warn!(
                        runtime = %self.runtime,
                        keys = candidate_keys.len(),
                        error = %error,
                        "stale/orphan reclaim pre-pass skipped after route lookup failure"
                    );
                    return Ok(0);
                }
            };
            for (key, current) in candidate_keys.iter().zip(current_routes) {
                released = released.saturating_add(
                    self.reclaim_stale_local_allocations_for_key(key, current.as_ref())?,
                );
            }
        }
        let (pending_before, pending_evictions) = {
            let pending = self.pending_evictions.lock();
            (
                pending.len(),
                pending.keys().cloned().collect::<BTreeSet<_>>(),
            )
        };
        let candidates = self.allocator.lock().untracked_reconcile_candidates(
            &tracked_identities,
            &legacy_tracked_spans,
            &pending_evictions,
            scan_started_at_ms,
            preferred_segment,
            PENDING_EVICTION_CONFIRMATION_BATCH,
        );
        let allocator_candidate_count = candidates.len();
        for identity in candidates {
            self.begin_untracked_candidate_confirmation(&identity);
        }
        released = released
            .saturating_add(self.process_pending_evictions(PENDING_EVICTION_CONFIRMATION_BATCH)?);
        let pending_after = self.pending_evictions.lock().len();
        if allocator_candidate_count != 0 || released != 0 || pending_before != pending_after {
            debug!(
                runtime = %self.runtime,
                clock_candidate_keys = candidate_keys.len(),
                allocator_candidates = allocator_candidate_count,
                pending_before,
                pending_after,
                released,
                force_scan,
                "bounded eviction guard reconciliation completed"
            );
        }
        self.stale_candidate_reconcile_completed_at_ms
            .store(now_ms(), std::sync::atomic::Ordering::Relaxed);
        Ok(released)
    }

    fn reclaim_stale_local_allocations_for_key(
        &self,
        key: &ObjectKey,
        current: Option<&ObjectRoute>,
    ) -> Result<usize> {
        let candidates = {
            let clock = self.hot_replicas.clock.lock();
            clock.entry_ids_for_key(key)
        };
        if candidates.is_empty() {
            return Ok(0);
        }
        for candidate in candidates {
            if current
                .is_some_and(|route| route_references_clock_entry(route, &self.runtime, &candidate))
            {
                continue;
            }
            self.begin_historical_candidate_confirmation(&candidate);
        }
        self.process_pending_evictions(PENDING_EVICTION_CONFIRMATION_BATCH)
    }

    fn evict_candidate(&self, victim: &ClockEntryId) -> Result<bool> {
        Ok(self.evict_candidate_with_outcome(victim)?.made_progress)
    }

    fn evict_candidate_with_outcome(
        &self,
        victim: &ClockEntryId,
    ) -> Result<EvictionAttempt> {
        let Some(route) = self.load_victim_route_bounded(&victim.route_key)? else {
            if self.reclaim_stale_local_allocations_for_key(&victim.route_key, None)? > 0 {
                return Ok(EvictionAttempt {
                    made_progress: true,
                    ..EvictionAttempt::default()
                });
            }
            // A route miss starts exact confirmation, but does not itself prove
            // absence. Keep the CLOCK entry (or its pending token) meanwhile.
            self.hot_replicas
                .clock
                .lock()
                .mark_hot_keys(std::slice::from_ref(&victim.route_key));
            return Ok(EvictionAttempt::default());
        };
        if route.state != RouteState::Active {
            self.hot_replicas
                .clock
                .lock()
                .mark_hot_keys(std::slice::from_ref(&victim.route_key));
            return Ok(EvictionAttempt::default());
        }
        if self.replica_index_for_victim(&route, victim).is_none() {
            if self.reclaim_stale_local_allocations_for_key(&victim.route_key, Some(&route))? > 0 {
                return Ok(EvictionAttempt {
                    made_progress: true,
                    ..EvictionAttempt::default()
                });
            }
            self.sync_route(&route);
            return Ok(EvictionAttempt::default());
        }
        let has_usable_cold_tier_device = self.has_usable_cold_tier_device()?;
        let cold_backing_not_on_disk = route.cold_backing.is_none()
            || route.cold_backing.as_ref().is_some_and(|backing| {
                backing.state == mooncake_store_core::ColdBackingState::PendingOffload
            });
        let delete_empty_route = cold_backing_not_on_disk && !has_usable_cold_tier_device;
        let route = if delete_empty_route {
            route
        } else {
            let Some(route) = self.ensure_materialized_cold_backing_for_eviction(&route)? else {
                return Ok(EvictionAttempt::default());
            };
            route
        };
        let Some(replica_index) = self.replica_index_for_victim(&route, victim) else {
            if self.reclaim_stale_local_allocations_for_key(&victim.route_key, Some(&route))? > 0 {
                return Ok(EvictionAttempt {
                    made_progress: true,
                    ..EvictionAttempt::default()
                });
            }
            self.sync_route(&route);
            return Ok(EvictionAttempt::default());
        };
        let made_progress =
            self.evict_route_replica(victim, &route, replica_index, delete_empty_route)?;
        Ok(EvictionAttempt {
            made_progress,
            dropped_without_cold: made_progress && delete_empty_route,
        })
    }

    pub(super) fn has_local_cold_tier_work(&self) -> bool {
        cold_tier::cold_tier_enabled() && self.cold_tier_devices.has_any_local_backend()
    }

    fn load_victim_route_bounded(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        Ok(self
            .route_ops
            .load_routes_bounded(std::slice::from_ref(key))?
            .into_iter()
            .next()
            .flatten())
    }

    fn has_usable_cold_tier_device(&self) -> Result<bool> {
        if !self.has_local_cold_tier_work() {
            return Ok(!self.cold_tier_devices.nof_targets.is_empty());
        }
        self.cold_tier_devices
            .has_usable_device(self.metadata.as_ref())
    }

    fn replica_index_for_victim(
        &self,
        route: &ObjectRoute,
        victim: &ClockEntryId,
    ) -> Option<usize> {
        route.replicas.iter().position(|replica| {
            replica.owner == self.runtime
                && replica.segment_name == victim.segment_name
                && replica.segment_offset == victim.segment_offset
                && replica.length == self.clock_entry_length(victim)
                && replica.allocation_id == victim.allocation_id
        })
    }

    fn clock_entry_length(&self, victim: &ClockEntryId) -> u64 {
        self.hot_replicas
            .clock
            .lock()
            .entry_length(victim)
            .unwrap_or_default()
    }

    fn evict_route_replica(
        &self,
        victim: &ClockEntryId,
        route: &ObjectRoute,
        replica_index: usize,
        delete_empty_route: bool,
    ) -> Result<bool> {
        let next =
            cold_tier::route_after_replica_eviction(route, replica_index, delete_empty_route);
        self.evict_route_replica_with_next(victim, route, replica_index, next)
    }

    fn evict_route_replica_with_next(
        &self,
        victim: &ClockEntryId,
        route: &ObjectRoute,
        replica_index: usize,
        next: Option<ObjectRoute>,
    ) -> Result<bool> {
        let Some(token) = self.begin_eviction(victim, route, replica_index, next.clone())? else {
            return Ok(false);
        };
        let cas = match next.as_ref() {
            Some(next_route) => {
                self.route_ops
                    .prune_route(&route.key, Some(route.version), next_route)
            }
            None => self.route_ops.delete_route(&route.key, Some(route.version)),
        };
        match cas {
            Ok(cas) if cas.applied => self.commit_eviction(&token),
            Ok(cas) => {
                self.abort_eviction(&token, cas.current.as_ref());
                Ok(false)
            }
            Err(error) => {
                self.defer_pending_eviction(
                    &token.identity,
                    PendingEvictionPhase::PendingConfirmation,
                );
                registry::record_eviction_guard_event("route_cas", "outcome_unknown");
                debug!(
                    runtime = %self.runtime,
                    key = %route.key.0,
                    segment = %token.identity.segment_name.0,
                    offset_bytes = token.identity.offset_bytes,
                    length_bytes = token.identity.length_bytes,
                    allocation_id = token.identity.allocation_id,
                    error = %error,
                    "route eviction CAS outcome is unknown; retaining pending token"
                );
                Ok(false)
            }
        }
    }

    fn begin_eviction(
        &self,
        victim: &ClockEntryId,
        route: &ObjectRoute,
        replica_index: usize,
        next_route: Option<ObjectRoute>,
    ) -> Result<Option<EvictionToken>> {
        let evicted_replica = route.replicas[replica_index].clone();
        let Some(allocation_id) = evicted_replica.allocation_id.filter(|id| *id != 0) else {
            return Ok(None);
        };
        if victim.allocation_id != Some(allocation_id)
            || self.read_pin_registry.is_pinned(
                &evicted_replica.segment_name,
                evicted_replica.segment_offset,
            )
        {
            return Ok(None);
        }
        let allocation = AllocationSpan {
            segment_name: evicted_replica.segment_name.clone(),
            offset_bytes: evicted_replica.segment_offset,
            length_bytes: evicted_replica.length,
        };
        let Some(identity) = self.allocator.lock().allocation_identity(&allocation) else {
            self.hot_replicas
                .clock
                .lock()
                .mark_hot_keys(std::slice::from_ref(&route.key));
            return Ok(None);
        };
        if identity.allocation_id != allocation_id {
            self.hot_replicas
                .clock
                .lock()
                .mark_hot_keys(std::slice::from_ref(&route.key));
            return Ok(None);
        }
        if self.delete_pending_contains(&identity) {
            return Ok(None);
        }
        let token = EvictionToken {
            identity: identity.clone(),
            kind: PendingEvictionKind::RouteCas,
            route_key: Some(route.key.clone()),
            expected_version: route.version,
            evicted_replica: Some(evicted_replica),
            clock_id: Some(victim.clone()),
            next_route,
        };
        let mut pending = self.pending_evictions.lock();
        if pending.contains_key(&identity) {
            return Ok(None);
        }
        if pending.len() >= PENDING_EVICTION_MAX_ENTRIES {
            debug!(
                runtime = %self.runtime,
                limit = PENDING_EVICTION_MAX_ENTRIES,
                "eviction prepare deferred because the pending token queue is full"
            );
            return Ok(None);
        }
        pending.insert(
            identity,
            PendingEviction {
                token: token.clone(),
                phase: PendingEvictionPhase::Prepared,
                confirmation_attempts: 0,
                next_attempt_at_ms: now_ms(),
                confirmation_in_progress: false,
                deferred: false,
            },
        );
        drop(pending);
        self.record_pending_eviction_metrics();
        Ok(Some(token))
    }

    fn begin_historical_candidate_confirmation(&self, victim: &ClockEntryId) {
        let Some(allocation_id) = victim.allocation_id.filter(|id| *id != 0) else {
            return;
        };
        let Some(length_bytes) = self.hot_replicas.clock.lock().entry_length(victim) else {
            return;
        };
        let allocation = AllocationSpan {
            segment_name: victim.segment_name.clone(),
            offset_bytes: victim.segment_offset,
            length_bytes,
        };
        let identity = {
            let allocator = self.allocator.lock();
            let Some(identity) = allocator.allocation_identity(&allocation) else {
                return;
            };
            if allocator.is_pending_allocation(&allocation, now_ms()) {
                return;
            }
            identity
        };
        if identity.allocation_id != allocation_id {
            return;
        }
        if self.delete_pending_contains(&identity) {
            return;
        }
        let mut pending = self.pending_evictions.lock();
        if pending.contains_key(&identity) {
            return;
        }
        if pending.len() >= PENDING_EVICTION_MAX_ENTRIES {
            return;
        }
        let token = EvictionToken {
            identity: identity.clone(),
            kind: PendingEvictionKind::RouteCas,
            route_key: Some(victim.route_key.clone()),
            expected_version: RouteVersion(0),
            evicted_replica: Some(ReplicaRoute {
                owner: self.runtime.clone(),
                segment_name: victim.segment_name.clone(),
                allocation_id: Some(allocation_id),
                offset: None,
                segment_offset: victim.segment_offset,
                length: length_bytes,
                checksum: None,
                tier: mooncake_store_core::ReplicaTier::Unknown,
                priority: u16::MAX,
            }),
            clock_id: Some(victim.clone()),
            next_route: None,
        };
        pending.insert(
            identity.clone(),
            PendingEviction {
                token,
                phase: PendingEvictionPhase::PendingConfirmation,
                confirmation_attempts: 0,
                next_attempt_at_ms: now_ms(),
                confirmation_in_progress: false,
                deferred: false,
            },
        );
        drop(pending);
        self.record_pending_eviction_metrics();
        registry::record_eviction_guard_event("clock_candidate", "pending_confirmation");
        debug!(
            runtime = %self.runtime,
            key = %victim.route_key.0,
            segment = %identity.segment_name.0,
            offset_bytes = identity.offset_bytes,
            length_bytes = identity.length_bytes,
            allocation_id = identity.allocation_id,
            "historical CLOCK candidate entered exact route confirmation"
        );
    }

    fn begin_untracked_candidate_confirmation(&self, identity: &AllocationIdentity) {
        if identity.allocation_id == 0
            || self
                .read_pin_registry
                .is_pinned(&identity.segment_name, identity.offset_bytes)
        {
            return;
        }
        let allocation = AllocationSpan {
            segment_name: identity.segment_name.clone(),
            offset_bytes: identity.offset_bytes,
            length_bytes: identity.length_bytes,
        };
        {
            let allocator = self.allocator.lock();
            if allocator.allocation_identity(&allocation).as_ref() != Some(identity)
                || allocator.is_pending_allocation(&allocation, now_ms())
            {
                return;
            }
        }
        if self.delete_pending_contains(identity) {
            return;
        }
        let mut pending = self.pending_evictions.lock();
        if pending.contains_key(identity) {
            return;
        }
        if pending.len() >= PENDING_EVICTION_MAX_ENTRIES {
            return;
        }
        pending.insert(
            identity.clone(),
            PendingEviction {
                token: EvictionToken {
                    identity: identity.clone(),
                    kind: PendingEvictionKind::AllocationCandidate,
                    route_key: None,
                    expected_version: RouteVersion(0),
                    evicted_replica: None,
                    clock_id: None,
                    next_route: None,
                },
                phase: PendingEvictionPhase::PendingAllocationConfirmation,
                confirmation_attempts: 0,
                next_attempt_at_ms: now_ms(),
                confirmation_in_progress: false,
                deferred: false,
            },
        );
        drop(pending);
        self.record_pending_eviction_metrics();
        registry::record_eviction_guard_event("allocator_candidate", "pending_confirmation");
        debug!(
            runtime = %self.runtime,
            segment = %identity.segment_name.0,
            offset_bytes = identity.offset_bytes,
            length_bytes = identity.length_bytes,
            allocation_id = identity.allocation_id,
            "allocator candidate entered exact route confirmation"
        );
    }

    fn abort_eviction(&self, token: &EvictionToken, current: Option<&ObjectRoute>) {
        let was_guarded = self
            .pending_evictions
            .lock()
            .remove(&token.identity)
            .is_some_and(|entry| entry.phase != PendingEvictionPhase::Prepared);
        if was_guarded {
            self.record_pending_eviction_metrics();
            registry::record_eviction_guard_event("pending_token", "aborted");
        }
        if let Some(current) = current {
            self.sync_route(current);
        } else if let Some(route_key) = token.route_key.as_ref() {
            self.hot_replicas
                .clock
                .lock()
                .mark_hot_keys(std::slice::from_ref(route_key));
        }
    }

    fn record_pending_eviction_metrics(&self) {
        let mut prepared = 0usize;
        let mut ready_to_commit = 0usize;
        let mut pending_confirmation = 0usize;
        let mut pending_allocation_confirmation = 0usize;
        {
            let pending = self.pending_evictions.lock();
            for entry in pending.values() {
                match entry.phase {
                    PendingEvictionPhase::Prepared => prepared = prepared.saturating_add(1),
                    PendingEvictionPhase::ReadyToCommit => {
                        ready_to_commit = ready_to_commit.saturating_add(1)
                    }
                    PendingEvictionPhase::PendingConfirmation => {
                        pending_confirmation = pending_confirmation.saturating_add(1)
                    }
                    PendingEvictionPhase::PendingAllocationConfirmation => {
                        pending_allocation_confirmation =
                            pending_allocation_confirmation.saturating_add(1)
                    }
                }
            }
        }
        registry::set_eviction_guard_pending("prepared", prepared);
        registry::set_eviction_guard_pending("ready_to_commit", ready_to_commit);
        registry::set_eviction_guard_pending("pending_confirmation", pending_confirmation);
        registry::set_eviction_guard_pending(
            "pending_allocation_confirmation",
            pending_allocation_confirmation,
        );
    }

    fn commit_eviction(&self, token: &EvictionToken) -> Result<bool> {
        if self
            .read_pin_registry
            .is_pinned(&token.identity.segment_name, token.identity.offset_bytes)
        {
            self.defer_pending_eviction(&token.identity, PendingEvictionPhase::ReadyToCommit);
            return Ok(false);
        }
        let was_guarded = self
            .pending_evictions
            .lock()
            .get(&token.identity)
            .is_some_and(|entry| entry.phase != PendingEvictionPhase::Prepared);
        let release_result = {
            let mut allocator = self.allocator.lock();
            match allocator.allocation_identity(&AllocationSpan {
                segment_name: token.identity.segment_name.clone(),
                offset_bytes: token.identity.offset_bytes,
                length_bytes: token.identity.length_bytes,
            }) {
                Some(current) if current == token.identity => allocator
                    .release(
                        &self.runtime,
                        &token.identity.segment_name,
                        token.identity.offset_bytes,
                        token.identity.length_bytes,
                        token.identity.allocation_id,
                    )
                    .map(|()| "released"),
                Some(_) | None => Ok("identity_replaced"),
            }
        };
        let release_outcome = match release_result {
            Ok(outcome) => outcome,
            Err(error) => {
                self.defer_pending_eviction(&token.identity, PendingEvictionPhase::ReadyToCommit);
                registry::record_eviction_guard_event("local_commit", "release_deferred");
                debug!(
                    runtime = %self.runtime,
                    key = ?token.route_key,
                    segment = %token.identity.segment_name.0,
                    offset_bytes = token.identity.offset_bytes,
                    length_bytes = token.identity.length_bytes,
                    allocation_id = token.identity.allocation_id,
                    error = %error,
                    "eviction local commit deferred; retaining pending token"
                );
                return Ok(false);
            }
        };
        if let Some(clock_id) = token.clock_id.as_ref() {
            self.hot_replicas.clock.lock().remove_id(clock_id);
        }
        self.pending_evictions.lock().remove(&token.identity);
        if was_guarded {
            self.record_pending_eviction_metrics();
            registry::record_eviction_guard_event("local_commit", release_outcome);
            debug!(
                runtime = %self.runtime,
                key = ?token.route_key,
                segment = %token.identity.segment_name.0,
                offset_bytes = token.identity.offset_bytes,
                length_bytes = token.identity.length_bytes,
                allocation_id = token.identity.allocation_id,
                outcome = release_outcome,
                "guarded eviction local commit completed"
            );
        }
        trace!(
            runtime = %self.runtime,
            key = ?token.route_key,
            segment = %token.identity.segment_name.0,
            offset_bytes = token.identity.offset_bytes,
            length_bytes = token.identity.length_bytes,
            expected_version = token.expected_version.0,
            route_deleted = token.kind == PendingEvictionKind::RouteCas && token.next_route.is_none(),
            "dram_eviction_complete"
        );
        Ok(true)
    }

    fn defer_pending_eviction(&self, identity: &AllocationIdentity, phase: PendingEvictionPhase) {
        let (attempts, retry_limit, became_deferred) = {
            let mut pending = self.pending_evictions.lock();
            let Some(entry) = pending.get_mut(identity) else {
                return;
            };
            entry.phase = phase;
            entry.confirmation_in_progress = false;
            entry.confirmation_attempts = entry.confirmation_attempts.saturating_add(1);
            let retry_limit = self.offload_priority.pending_eviction_retry_limit.max(1);
            let delay_ms = if entry.confirmation_attempts >= retry_limit {
                // Keep the token after transient retries are exhausted, but do not
                // turn a persistent authority disagreement into a hot RPC loop.
                PENDING_EVICTION_RETRY_MAX_MS
            } else {
                let exponent = entry.confirmation_attempts.saturating_sub(1).min(7);
                PENDING_EVICTION_RETRY_BASE_MS
                    .saturating_mul(1u64 << exponent)
                    .min(PENDING_EVICTION_RETRY_MAX_MS)
            };
            entry.next_attempt_at_ms = now_ms().saturating_add(delay_ms);
            let became_deferred = entry.confirmation_attempts >= retry_limit && !entry.deferred;
            if became_deferred {
                entry.deferred = true;
            }
            (entry.confirmation_attempts, retry_limit, became_deferred)
        };
        if became_deferred {
            warn!(
                runtime = %self.runtime,
                segment = %identity.segment_name.0,
                offset_bytes = identity.offset_bytes,
                length_bytes = identity.length_bytes,
                allocation_id = identity.allocation_id,
                attempts,
                retry_limit,
                "reclaim pending remains deferred after bounded retries"
            );
        }
        self.record_pending_eviction_metrics();
    }

    fn process_pending_evictions(&self, limit: usize) -> Result<usize> {
        let now = now_ms();
        let work = {
            let mut pending = self.pending_evictions.lock();
            pending
                .values_mut()
                .filter(|entry| {
                    entry.phase != PendingEvictionPhase::Prepared
                        && !entry.confirmation_in_progress
                        && entry.next_attempt_at_ms <= now
                })
                .take(limit.max(1))
                .map(|entry| {
                    entry.confirmation_in_progress = true;
                    (entry.token.clone(), entry.phase)
                })
                .collect::<Vec<_>>()
        };
        let mut committed = 0usize;
        for (token, phase) in work {
            match phase {
                PendingEvictionPhase::Prepared => {
                    unreachable!("prepared tokens are not worker work")
                }
                PendingEvictionPhase::ReadyToCommit => {
                    if self.commit_eviction(&token)? {
                        committed = committed.saturating_add(1);
                    }
                }
                PendingEvictionPhase::PendingConfirmation => {
                    let Some(route_key) = token.route_key.as_ref() else {
                        self.defer_pending_eviction(
                            &token.identity,
                            PendingEvictionPhase::PendingConfirmation,
                        );
                        continue;
                    };
                    let Some(evicted_replica) = token.evicted_replica.as_ref() else {
                        self.defer_pending_eviction(
                            &token.identity,
                            PendingEvictionPhase::PendingConfirmation,
                        );
                        continue;
                    };
                    match self
                        .route_ops
                        .confirm_replica_reference(route_key, evicted_replica)
                    {
                        Ok(mooncake_store_core::RouteReplicaReference::Referenced(route)) => {
                            registry::record_eviction_guard_event(
                                "route_confirmation",
                                "referenced",
                            );
                            debug!(
                                runtime = %self.runtime,
                                key = ?token.route_key,
                                segment = %token.identity.segment_name.0,
                                offset_bytes = token.identity.offset_bytes,
                                length_bytes = token.identity.length_bytes,
                                allocation_id = token.identity.allocation_id,
                                "exact route confirmation retained the allocation"
                            );
                            self.abort_eviction(&token, Some(&route));
                        }
                        Ok(mooncake_store_core::RouteReplicaReference::Absent) => {
                            registry::record_eviction_guard_event("route_confirmation", "absent");
                            debug!(
                                runtime = %self.runtime,
                                key = ?token.route_key,
                                segment = %token.identity.segment_name.0,
                                offset_bytes = token.identity.offset_bytes,
                                length_bytes = token.identity.length_bytes,
                                allocation_id = token.identity.allocation_id,
                                "exact route confirmation permits local commit"
                            );
                            {
                                let mut pending = self.pending_evictions.lock();
                                if let Some(entry) = pending.get_mut(&token.identity) {
                                    entry.phase = PendingEvictionPhase::ReadyToCommit;
                                    entry.confirmation_in_progress = false;
                                    entry.next_attempt_at_ms = now_ms();
                                }
                            }
                            if self.commit_eviction(&token)? {
                                committed = committed.saturating_add(1);
                            }
                        }
                        Err(error) => {
                            self.defer_pending_eviction(
                                &token.identity,
                                PendingEvictionPhase::PendingConfirmation,
                            );
                            registry::record_eviction_guard_event(
                                "route_confirmation",
                                "indeterminate",
                            );
                            debug!(
                                runtime = %self.runtime,
                                key = ?token.route_key,
                                segment = %token.identity.segment_name.0,
                                offset_bytes = token.identity.offset_bytes,
                                length_bytes = token.identity.length_bytes,
                                allocation_id = token.identity.allocation_id,
                                error = %error,
                                "exact eviction confirmation is indeterminate; retaining token"
                            );
                        }
                    }
                }
                PendingEvictionPhase::PendingAllocationConfirmation => {
                    let identity = mooncake_store_core::AllocationRouteIdentity {
                        owner: self.runtime.clone(),
                        segment_name: token.identity.segment_name.clone(),
                        offset_bytes: token.identity.offset_bytes,
                        length_bytes: token.identity.length_bytes,
                        allocation_id: token.identity.allocation_id,
                    };
                    match self
                        .route_ops
                        .get_allocation_route_references_consistent(&[identity])
                    {
                        Ok(references)
                            if matches!(
                                references.as_slice(),
                                [mooncake_store_core::AllocationRouteReferenceState::Absent]
                            ) =>
                        {
                            registry::record_eviction_guard_event(
                                "allocation_confirmation",
                                "absent",
                            );
                            debug!(
                                runtime = %self.runtime,
                                segment = %token.identity.segment_name.0,
                                offset_bytes = token.identity.offset_bytes,
                                length_bytes = token.identity.length_bytes,
                                allocation_id = token.identity.allocation_id,
                                "allocator candidate is absent from every authority; committing release"
                            );
                            {
                                let mut pending = self.pending_evictions.lock();
                                if let Some(entry) = pending.get_mut(&token.identity) {
                                    entry.phase = PendingEvictionPhase::ReadyToCommit;
                                    entry.confirmation_in_progress = false;
                                    entry.next_attempt_at_ms = now_ms();
                                }
                            }
                            if self.commit_eviction(&token)? {
                                committed = committed.saturating_add(1);
                            }
                        }
                        Ok(references)
                            if matches!(
                                references.as_slice(),
                                [mooncake_store_core::AllocationRouteReferenceState::Exact]
                            ) =>
                        {
                            self.defer_pending_eviction(
                                &token.identity,
                                PendingEvictionPhase::PendingAllocationConfirmation,
                            );
                            registry::record_eviction_guard_event(
                                "allocation_confirmation",
                                "referenced",
                            );
                            debug!(
                                runtime = %self.runtime,
                                segment = %token.identity.segment_name.0,
                                offset_bytes = token.identity.offset_bytes,
                                length_bytes = token.identity.length_bytes,
                                allocation_id = token.identity.allocation_id,
                                references = ?references,
                                "allocator candidate remains route-referenced; retaining token"
                            );
                        }
                        Ok(references)
                            if matches!(
                                references.as_slice(),
                                [mooncake_store_core::AllocationRouteReferenceState::Conflict]
                            ) =>
                        {
                            self.defer_pending_eviction(
                                &token.identity,
                                PendingEvictionPhase::PendingAllocationConfirmation,
                            );
                            registry::record_eviction_guard_event(
                                "allocation_confirmation",
                                "conflict",
                            );
                            debug!(
                                runtime = %self.runtime,
                                segment = %token.identity.segment_name.0,
                                offset_bytes = token.identity.offset_bytes,
                                length_bytes = token.identity.length_bytes,
                                allocation_id = token.identity.allocation_id,
                                references = ?references,
                                "allocator candidate authority results conflict; retaining token"
                            );
                        }
                        Ok(references) => {
                            self.defer_pending_eviction(
                                &token.identity,
                                PendingEvictionPhase::PendingAllocationConfirmation,
                            );
                            registry::record_eviction_guard_event(
                                "allocation_confirmation",
                                "invalid_response",
                            );
                            debug!(
                                runtime = %self.runtime,
                                segment = %token.identity.segment_name.0,
                                offset_bytes = token.identity.offset_bytes,
                                length_bytes = token.identity.length_bytes,
                                allocation_id = token.identity.allocation_id,
                                references = ?references,
                                "allocation reference response length was indeterminate; retaining token"
                            );
                        }
                        Err(error) => {
                            self.defer_pending_eviction(
                                &token.identity,
                                PendingEvictionPhase::PendingAllocationConfirmation,
                            );
                            registry::record_eviction_guard_event(
                                "allocation_confirmation",
                                "indeterminate",
                            );
                            debug!(
                                runtime = %self.runtime,
                                segment = %token.identity.segment_name.0,
                                offset_bytes = token.identity.offset_bytes,
                                length_bytes = token.identity.length_bytes,
                                allocation_id = token.identity.allocation_id,
                                error = %error,
                                "allocation reference confirmation is indeterminate; retaining token"
                            );
                        }
                    }
                }
            }
        }
        Ok(committed)
    }

    pub(super) fn current_materialized_route(
        &self,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        let Some(route) = self
            .route_ops
            .load_routes_bounded(std::slice::from_ref(key))?
            .into_iter()
            .next()
            .flatten()
        else {
            self.hot_replicas.remove_key(key);
            return Ok(None);
        };
        self.sync_route(&route);
        if route.state == RouteState::Active && cold_tier::has_materialized_cold_backing(&route) {
            Ok(Some(route))
        } else {
            Ok(None)
        }
    }

    fn rebuild_clock(&self) -> Result<()> {
        let tracker = OperationTracker::new("storage_owner_rebuild_clock");
        let result = (|| {
            let mut clock = StorageClockState::for_runtime(&self.runtime);
            let mut route_count = 0usize;
            self.route_ops
                .visit_routes_by_replica_owner_consistent(&self.runtime, &mut |route| {
                    clock.track_route(&route, &self.runtime);
                    route_count = route_count.saturating_add(1);
                    Ok(())
                })?;
            *self.hot_replicas.clock.lock() = clock;
            debug!(
                runtime = %self.runtime,
                routes = route_count,
                "storage-owner rebuilt eviction clock"
            );
            Ok(())
        })();
        tracker.finish(&result, 0);
        result
    }
}

fn route_storage_bytes(route: &ObjectRoute, runtime: &ClientRuntimeId) -> u64 {
    route
        .replicas
        .iter()
        .filter(|replica| replica.owner == *runtime)
        .map(|replica| replica.length)
        .sum()
}

fn route_references_clock_entry(
    route: &ObjectRoute,
    runtime: &ClientRuntimeId,
    entry: &ClockEntryId,
) -> bool {
    route.replicas.iter().any(|replica| {
        replica.owner == *runtime
            && replica.segment_name == entry.segment_name
            && replica.segment_offset == entry.segment_offset
            && replica.allocation_id == entry.allocation_id
    })
}

pub(in crate::client) fn nof_backing_clock_segment(target_id: &str) -> SegmentName {
    SegmentName::new(format!("__nof_backing__/{target_id}"))
}

impl StorageClockState {
    const READ_HOT_CLOCK_CREDIT: u8 = 2;

    fn for_runtime(runtime: &ClientRuntimeId) -> Self {
        Self {
            hand: runtime_clock_cursor(runtime, 0x6576_6963_7469_6f6e),
            stale_reclaim_hand: runtime_clock_cursor(runtime, 0x7374_616c_655f_6763),
            ..Self::default()
        }
    }

    fn eviction_budget(&self) -> usize {
        self.entries.len().saturating_mul(3)
    }

    fn tracked_replica_count_excluding(&self, excluded: &BTreeSet<ClockEntryId>) -> usize {
        self.by_id
            .keys()
            .filter(|id| !excluded.contains(*id))
            .count()
    }

    fn entry_length(&self, id: &ClockEntryId) -> Option<u64> {
        self.by_id
            .get(id)
            .and_then(|index| self.entries.get(*index))
            .and_then(Option::as_ref)
            .map(|entry| entry.length_bytes)
    }

    fn entry_ids_for_key(&self, key: &ObjectKey) -> Vec<ClockEntryId> {
        self.by_key
            .get(key)
            .into_iter()
            .flat_map(|indices| indices.iter())
            .filter_map(|index| self.entries.get(*index).and_then(Option::as_ref))
            .map(|entry| entry.id.clone())
            .collect()
    }

    fn reclaim_candidate_keys(
        &mut self,
        preferred_segment: Option<&SegmentName>,
        limit: usize,
    ) -> Vec<ObjectKey> {
        if self.entries.is_empty() {
            return Vec::new();
        }
        let mut keys = Vec::new();
        let mut seen = BTreeSet::new();
        let limit = limit.max(1);
        for _ in 0..limit {
            let index = self.stale_reclaim_hand % self.entries.len();
            self.stale_reclaim_hand = (self.stale_reclaim_hand + 1) % self.entries.len();
            let Some(entry) = self.entries[index].as_ref() else {
                continue;
            };
            if preferred_segment.is_some_and(|segment| entry.id.segment_name != *segment) {
                continue;
            }
            if seen.insert(entry.id.route_key.clone()) {
                keys.push(entry.id.route_key.clone());
            }
        }
        keys
    }

    #[cfg(test)]
    fn allocation_spans_for_key(&self, key: &ObjectKey) -> BTreeSet<AllocationSpan> {
        self.by_key
            .get(key)
            .into_iter()
            .flat_map(|indices| indices.iter())
            .filter_map(|index| self.entries.get(*index).and_then(Option::as_ref))
            .map(|entry| AllocationSpan {
                segment_name: entry.id.segment_name.clone(),
                offset_bytes: entry.id.segment_offset,
                length_bytes: entry.length_bytes,
            })
            .collect()
    }

    fn track_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.track_route_with_fresh_write(route, runtime, false);
    }

    fn track_fresh_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.track_route_with_fresh_write(route, runtime, true);
    }

    fn track_route_with_fresh_write(
        &mut self,
        route: &ObjectRoute,
        runtime: &ClientRuntimeId,
        fresh_write: bool,
    ) {
        let has_local_replica = route
            .replicas
            .iter()
            .any(|replica| replica.owner == *runtime);
        let pending_hot = has_local_replica && self.pending_hot_keys.remove(&route.key);
        for replica in route
            .replicas
            .iter()
            .filter(|replica| replica.owner == *runtime)
        {
            self.upsert_replica(route, replica, fresh_write, pending_hot);
        }
    }

    fn untrack_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        for replica in route
            .replicas
            .iter()
            .filter(|replica| replica.owner == *runtime)
        {
            self.remove_id(&ClockEntryId {
                route_key: route.key.clone(),
                segment_name: replica.segment_name.clone(),
                segment_offset: replica.segment_offset,
                allocation_id: replica.allocation_id,
            });
        }
    }

    fn sync_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.remove_key(&route.key);
        self.track_route(route, runtime);
    }

    fn sync_fresh_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.remove_key(&route.key);
        self.track_fresh_route(route, runtime);
    }

    fn sync_nof_route(
        &mut self,
        route: &ObjectRoute,
        runtime: &ClientRuntimeId,
        fresh_write: bool,
    ) {
        self.remove_key(&route.key);
        let Some(backing) = route
            .nof_backing
            .as_ref()
            .filter(|backing| {
                backing.state == mooncake_store_core::ColdBackingState::Materialized
            })
        else {
            return;
        };
        let local_targets = backing
            .all_targets()
            .map(|(target_id, owner, locator)| (owner, target_id, locator))
            .filter(|(owner, _, _)| *owner == runtime)
            .collect::<Vec<_>>();
        if local_targets.is_empty() {
            return;
        }
        let pending_hot = self.pending_hot_keys.remove(&route.key);
        for (owner, target_id, _) in local_targets {
            self.upsert_replica(
                route,
                &ReplicaRoute {
            allocation_id: None,
                    owner: owner.clone(),
                    segment_name: nof_backing_clock_segment(target_id),
                    offset: None,
                    segment_offset: 0,
                    length: backing.length,
                    checksum: backing.checksum,
                    tier: ReplicaTier::File,
                    priority: 0,
                },
                fresh_write,
                pending_hot,
            );
        }
        self.pending_hot_keys.remove(&route.key);
    }

    fn mark_hot_keys(&mut self, keys: &[ObjectKey]) -> RouteTrafficReport {
        self.mark_hot_keys_inner(keys, true)
    }

    /// Marks existing entries without retaining misses for a later route synchronization.
    /// NoF traffic uses this to keep unrelated hot keys out of the NoF clock.
    fn mark_tracked_hot_keys(&mut self, keys: &[ObjectKey]) -> RouteTrafficReport {
        if self.by_key.is_empty() {
            return RouteTrafficReport::new(keys.len(), 0);
        }
        self.mark_hot_keys_inner(keys, false)
    }

    fn mark_hot_keys_inner(
        &mut self,
        keys: &[ObjectKey],
        remember_untracked: bool,
    ) -> RouteTrafficReport {
        let mut slots = Vec::new();
        let mut bytes = 0u64;
        for key in keys {
            let mut tracked = false;
            if let Some(indices) = self.by_key.get(key) {
                for index in indices {
                    if let Some(entry) = self.entries.get(*index).and_then(Option::as_ref) {
                        tracked = true;
                        bytes = bytes.saturating_add(entry.length_bytes);
                        slots.push(*index);
                    }
                }
            }
            if tracked {
                self.pending_hot_keys.remove(key);
            } else if remember_untracked {
                self.pending_hot_keys.insert(key.clone());
            }
        }
        for index in slots {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::as_mut) {
                entry.hot = true;
                entry.hot_credit = Self::READ_HOT_CLOCK_CREDIT;
                entry.fresh_write = false;
            }
        }
        RouteTrafficReport::new(keys.len(), bytes)
    }

    fn pick_victim(
        &mut self,
        preferred_segment: Option<&SegmentName>,
        policy: ColdTierEvictionPriorityPolicy,
        scan_limit: usize,
    ) -> Option<ClockEntryId> {
        match policy {
            ColdTierEvictionPriorityPolicy::Clock => self.pick_victim_clock(preferred_segment),
            ColdTierEvictionPriorityPolicy::ColdestLargestFirst => {
                self.pick_victim_coldest_largest_first(preferred_segment, scan_limit)
            }
        }
    }

    fn pick_victim_clock(
        &mut self,
        preferred_segment: Option<&SegmentName>,
    ) -> Option<ClockEntryId> {
        if self.entries.is_empty() {
            return None;
        }
        let limit = self.eviction_budget().max(1);
        for _ in 0..limit {
            let index = self.hand % self.entries.len();
            self.hand = (self.hand + 1) % self.entries.len();
            let Some(entry) = self.entries[index].as_mut() else {
                continue;
            };
            if preferred_segment.is_some_and(|segment| entry.id.segment_name != *segment) {
                continue;
            }
            if entry.hot_credit > 0 {
                entry.hot_credit -= 1;
                entry.hot = entry.hot_credit > 0;
                continue;
            }
            if entry.fresh_write {
                entry.fresh_write = false;
                continue;
            }
            return Some(entry.id.clone());
        }
        None
    }

    fn upsert_replica(
        &mut self,
        route: &ObjectRoute,
        replica: &ReplicaRoute,
        fresh_write: bool,
        pending_hot: bool,
    ) {
        let id = ClockEntryId {
            route_key: route.key.clone(),
            segment_name: replica.segment_name.clone(),
            segment_offset: replica.segment_offset,
            allocation_id: replica.allocation_id,
        };
        if let Some(index) = self.by_id.get(&id).copied() {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::as_mut) {
                entry.length_bytes = replica.length;
                if pending_hot {
                    entry.hot = true;
                    entry.hot_credit = Self::READ_HOT_CLOCK_CREDIT;
                    entry.fresh_write = false;
                } else if fresh_write && entry.hot_credit == 0 {
                    entry.fresh_write = true;
                }
            }
            return;
        }
        let hot_credit = if pending_hot {
            Self::READ_HOT_CLOCK_CREDIT
        } else {
            0
        };
        let entry = ClockEntry {
            id: id.clone(),
            length_bytes: replica.length,
            hot: hot_credit > 0,
            hot_credit,
            fresh_write: fresh_write && hot_credit == 0,
        };
        // Vacancies are recorded when an entry is removed. Searching entries
        // here makes a hole-free fill quadratic in the resident replica count.
        let index = self.free_slots.pop().unwrap_or(self.entries.len());
        if index == self.entries.len() {
            self.entries.push(Some(entry));
        } else {
            self.entries[index] = Some(entry);
        }
        self.by_id.insert(id.clone(), index);
        self.by_key.entry(id.route_key).or_default().push(index);
    }

    fn remove_key(&mut self, key: &ObjectKey) {
        let indices = self.by_key.remove(key).unwrap_or_default();
        for index in indices {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::take) {
                self.by_id.remove(&entry.id);
                self.free_slots.push(index);
            }
        }
    }

    fn remove_id(&mut self, id: &ClockEntryId) {
        let Some(index) = self.by_id.remove(id) else {
            return;
        };
        self.entries[index] = None;
        self.free_slots.push(index);
        if let Some(indices) = self.by_key.get_mut(&id.route_key) {
            indices.retain(|candidate| *candidate != index);
            if indices.is_empty() {
                self.by_key.remove(&id.route_key);
            }
        }
    }

    fn tracked_allocation_spans(&self) -> BTreeSet<AllocationSpan> {
        self.entries
            .iter()
            .filter_map(Option::as_ref)
            .map(|entry| AllocationSpan {
                segment_name: entry.id.segment_name.clone(),
                offset_bytes: entry.id.segment_offset,
                length_bytes: entry.length_bytes,
            })
            .collect()
    }

    fn tracked_allocation_identities(&self) -> BTreeSet<AllocationIdentity> {
        self.entries
            .iter()
            .filter_map(Option::as_ref)
            .filter_map(|entry| {
                entry
                    .id
                    .allocation_id
                    .filter(|allocation_id| *allocation_id != 0)
                    .map(|allocation_id| AllocationIdentity {
                        segment_name: entry.id.segment_name.clone(),
                        offset_bytes: entry.id.segment_offset,
                        length_bytes: entry.length_bytes,
                        allocation_id,
                    })
            })
            .collect()
    }

    fn pick_victim_coldest_largest_first(
        &mut self,
        preferred_segment: Option<&SegmentName>,
        scan_limit: usize,
    ) -> Option<ClockEntryId> {
        if self.entries.is_empty() {
            return None;
        }
        let limit = scan_limit.max(1).min(self.eviction_budget().max(1));
        let mut selected: Option<(ClockEntryId, u64)> = None;
        for _ in 0..limit {
            let index = self.hand % self.entries.len();
            self.hand = (self.hand + 1) % self.entries.len();
            let Some(entry) = self.entries[index].as_mut() else {
                continue;
            };
            if preferred_segment.is_some_and(|segment| entry.id.segment_name != *segment) {
                continue;
            }
            if entry.hot {
                entry.hot = false;
                continue;
            }
            match selected.as_ref() {
                Some((_, length)) if *length >= entry.length_bytes => {}
                _ => selected = Some((entry.id.clone(), entry.length_bytes)),
            }
        }
        selected.map(|(id, _)| id)
    }
}

#[allow(dead_code)]
impl HotReplicaTracker {
    fn for_runtime(runtime: &ClientRuntimeId) -> Self {
        Self {
            clock: Mutex::new(StorageClockState::for_runtime(runtime)),
        }
    }

    fn eviction_budget(&self) -> usize {
        self.clock.lock().eviction_budget()
    }

    fn track_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().track_route(route, runtime);
    }

    fn untrack_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().untrack_route(route, runtime);
    }

    fn sync_route(&self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        self.clock.lock().sync_route(route, runtime);
    }

    fn sync_routes(&self, routes: &[ObjectRoute], runtime: &ClientRuntimeId) {
        let mut clock = self.clock.lock();
        for route in routes {
            clock.sync_route(route, runtime);
        }
    }

    fn sync_nof_routes(&self, routes: &[ObjectRoute], runtime: &ClientRuntimeId) {
        let mut clock = self.clock.lock();
        for route in routes {
            clock.sync_nof_route(route, runtime, false);
        }
    }

    fn mark_hot_keys(&self, keys: &[ObjectKey]) {
        self.clock.lock().mark_hot_keys(keys);
    }

    fn pick_victim(
        &self,
        preferred_segment: Option<&SegmentName>,
        policy: ColdTierEvictionPriorityPolicy,
        scan_limit: usize,
    ) -> Option<ClockEntryId> {
        self.clock
            .lock()
            .pick_victim(preferred_segment, policy, scan_limit)
    }

    fn remove_id(&self, id: &ClockEntryId) {
        self.clock.lock().remove_id(id);
    }

    fn remove_key(&self, key: &ObjectKey) {
        self.clock.lock().remove_key(key);
    }

    fn rebuild(&self, routes: &[ObjectRoute], runtime: &ClientRuntimeId) {
        let mut clock = StorageClockState::for_runtime(runtime);
        for route in routes {
            clock.track_route(route, runtime);
        }
        *self.clock.lock() = clock;
    }
}

fn runtime_clock_cursor(runtime: &ClientRuntimeId, salt: u64) -> usize {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in salt
        .to_le_bytes()
        .into_iter()
        .chain(runtime.storage_key().bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash ^= hash >> 30;
    hash = hash.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash ^= hash >> 27;
    hash = hash.wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^= hash >> 31;
    (hash as usize) & (usize::MAX >> 1)
}

#[allow(dead_code)]
impl StoreState {
    fn reclaim_queue_snapshot(&self, now_ms: u64) -> ReclaimQueueSnapshot {
        let mut snapshot = ReclaimQueueSnapshot {
            total_pending: self.pending_reclaims.len(),
            ..ReclaimQueueSnapshot::default()
        };
        for reclaim in &self.pending_reclaims {
            if reclaim.due_at_ms <= now_ms {
                snapshot.due = snapshot.due.saturating_add(1);
            }
            if reclaim.cold_backing.is_some() {
                snapshot.cold_backing_reclaims = snapshot.cold_backing_reclaims.saturating_add(1);
            } else {
                snapshot.hot_segment_reclaims = snapshot.hot_segment_reclaims.saturating_add(1);
            }
            *snapshot
                .by_qos_tier
                .entry(reclaim.qos_tier.clone())
                .or_default() += 1;
            *snapshot
                .by_policy_rank
                .entry(reclaim.policy_rank)
                .or_default() += 1;
        }
        snapshot
    }
}

fn watermark_bytes(capacity_bytes: u64, percent: u8) -> u64 {
    if percent == 0 || capacity_bytes == 0 {
        return 0;
    }
    capacity_bytes.saturating_mul(percent as u64).div_ceil(100)
}

fn remote_segment_cache_refreshable(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::Transport(_) | StoreError::NotFound(_) | StoreError::InvalidState(_)
    )
}

fn remote_segment_cache_stale(error: &StoreError) -> bool {
    match error {
        StoreError::Transport(message)
        | StoreError::NotFound(message)
        | StoreError::InvalidState(message) => {
            message.contains("segment handle")
                || message.contains("is outside segment")
                || message.contains("has no published storage target chunks")
                || message.contains("crosses an unpublished target chunk")
                || message.contains("target chunks are not contiguous")
                || message.contains("target chunk has zero length")
        }
        _ => false,
    }
}
