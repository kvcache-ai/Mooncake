include!("cold_tier_restore_state.rs");

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
        match self.registered_buffers.remove(&(buffer as usize)) {
            Some(registered) if registered == size => {
                transport.unregister_memory(buffer, size)?;
                Ok(())
            }
            Some(registered) => {
                self.registered_buffers.insert(buffer as usize, registered);
                Err(StoreError::Allocator(format!(
                    "registered buffer size mismatch: requested={size} registered={registered}"
                )))
            }
            None => Err(StoreError::NotFound(format!(
                "registered buffer {:p} not found",
                buffer
            ))),
        }
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
    use mooncake_transport::{SegmentBuffer, SegmentKind, TransferProgress};
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
            vec![(
                "10.0.0.8:12002".to_string(),
                "extra-descriptor".to_string()
            )]
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
                offset: Some(0),
                segment_offset: 0,
                length: 1024,
                checksum: None,
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: None,
        }
    }

    #[test]
    fn pending_hot_keys_accumulates_without_bound() {
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
            100,
            "pending_hot_keys grows with every unique key accessed"
        );
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
            clock.mark_hot_keys(&[ObjectKey(format!("k-{index}"))]);
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
        let victim = rebuilt.pick_victim(None, ColdTierEvictionPriorityPolicy::Clock, 0).expect("should find a cold victim");
        assert_ne!(
            victim.route_key,
            ObjectKey("k-0".to_string()),
            "genuinely hot key k-0 must survive eviction"
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
        clock.mark_hot_keys(&[hot_route.key.clone()]);

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
        clock.mark_hot_keys(&[hot_route.key.clone()]);

        let first = clock
            .pick_victim(None, ColdTierEvictionPriorityPolicy::Clock, 0)
            .expect("clock should evict a fresh route before read-hot route");
        let second = clock
            .pick_victim(None, ColdTierEvictionPriorityPolicy::Clock, 0)
            .expect("clock should still evict fresh routes before read-hot route");

        assert_ne!(first.route_key, hot_route.key);
        assert_ne!(second.route_key, hot_route.key);
    }
}

impl StorageOwnerState {
    fn report_route_hits(&self, keys: &[ObjectKey]) -> RouteTrafficReport {
        self.hot_replicas.clock.lock().mark_hot_keys(keys)
    }

    fn track_routes(&self, routes: &[ObjectRoute]) -> RouteTrafficReport {
        let bytes = routes
            .iter()
            .map(|route| route_storage_bytes(route, &self.runtime))
            .sum();
        {
            let mut allocator = self.allocator.lock();
            for route in routes {
                allocator.clear_pending_route(route, &self.runtime);
            }
        }
        let mut clock = self.hot_replicas.clock.lock();
        for route in routes {
            clock.sync_fresh_route(route, &self.runtime);
        }
        RouteTrafficReport::new(routes.len(), bytes)
    }

    fn track_route(&self, route: &ObjectRoute) {
        self.allocator
            .lock()
            .clear_pending_route(route, &self.runtime);
        self.hot_replicas.clock.lock().track_fresh_route(route, &self.runtime);
    }

    fn untrack_route(&self, route: &ObjectRoute) {
        self.hot_replicas.clock.lock().untrack_route(route, &self.runtime);
    }

    fn sync_route(&self, route: &ObjectRoute) {
        self.allocator
            .lock()
            .clear_pending_route(route, &self.runtime);
        self.hot_replicas.clock.lock().sync_route(route, &self.runtime);
    }

    fn evict_until_low_watermark(&self, high_percent: u8, low_percent: u8) -> Result<usize> {
        let tracker = OperationTracker::new("storage_owner_background_eviction");
        let started = Instant::now();
        let result = (|| {
            let (used_bytes, capacity_bytes) = self.allocator.lock().usage_bytes();
            if capacity_bytes == 0 {
                return Ok(0);
            }
            let high_watermark = watermark_bytes(capacity_bytes, high_percent);
            if used_bytes < high_watermark {
                return Ok(0);
            }
            let low_watermark = watermark_bytes(capacity_bytes, low_percent);
            let mut evicted = 0usize;
            loop {
                let (used_bytes, _) = self.allocator.lock().usage_bytes();
                if used_bytes <= low_watermark {
                    break;
                }
                if !self.evict_one_blocking(None)? {
                    break;
                }
                evicted = evicted.saturating_add(1);
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
        tracker.finish(&result, result.as_ref().copied().unwrap_or_default() as u64);
        registry::record_eviction(
            if result.is_ok() { "ok" } else { "error" },
            started.elapsed(),
        );
        result
    }

    fn evict_one(self: &Arc<Self>, preferred_segment: Option<&SegmentName>) -> Result<bool> {
        let preferred_segment = preferred_segment.cloned();
        if tokio::runtime::Handle::try_current().is_ok() {
            let storage_owner = Arc::clone(self);
            let thread = std::thread::Builder::new()
                .name(format!("mooncake-storage-owner-evict-{}", self.runtime.stable_id.0))
                .spawn(move || storage_owner.evict_one_blocking(preferred_segment.as_ref()))
                .map_err(|error| {
                    StoreError::Transport(format!(
                        "failed to spawn storage-owner eviction worker: {error}"
                    ))
                })?;
            return thread.join().map_err(|_| {
                StoreError::Transport("storage-owner eviction worker panicked".to_string())
            })?;
        }
        self.evict_one_blocking(preferred_segment.as_ref())
    }

    fn evict_one_blocking(&self, preferred_segment: Option<&SegmentName>) -> Result<bool> {
        let tracker = OperationTracker::new("storage_owner_evict_one");
        let result = (|| {
            for rebuild in 0..=1usize {
                let budget = {
                    let clock = self.hot_replicas.clock.lock();
                    clock.eviction_budget()
                };
                for _ in 0..budget.max(1) {
                    let victim = {
                        let mut clock = self.hot_replicas.clock.lock();
                        clock.pick_victim(preferred_segment, self.offload_priority.eviction_policy, self.offload_priority.eviction_scan_limit)
                    };
                    let Some(victim) = victim else {
                        break;
                    };
                    if self.evict_candidate(&victim)? {
                        return Ok(true);
                    }
                }
                if rebuild == 0 {
                    self.rebuild_clock()?;
                }
            }
            Ok(false)
        })();
        tracker.finish(&result, 0);
        result
    }

    fn evict_candidate(&self, victim: &ClockEntryId) -> Result<bool> {
        let Some(route) = self
            .route_ops
            .load_route(&victim.route_key)?
        else {
            self.hot_replicas.clock.lock().remove_id(victim);
            return Ok(false);
        };
        if route.state != RouteState::Active {
            self.hot_replicas.clock.lock().remove_id(victim);
            return Ok(false);
        }
        let Some(replica_index) = route.replicas.iter().position(|replica| {
            replica.owner == self.runtime
                && replica.segment_name == victim.segment_name
                && replica.segment_offset == victim.segment_offset
        }) else {
            self.sync_route(&route);
            return Ok(false);
        };

        let evicted_replica = route.replicas[replica_index].clone();
        let mut replicas = route.replicas.clone();
        replicas.remove(replica_index);
        replicas.sort_by_key(|replica| replica.priority);
        for (priority, replica) in replicas.iter_mut().enumerate() {
            replica.priority = priority as u16;
        }
        let keeps_materialized_cold_backing = route.cold_backing.as_ref().is_some_and(|backing| {
            backing.state == mooncake_store_core::ColdBackingState::Materialized
        });
        let next = if replicas.is_empty() && !keeps_materialized_cold_backing {
            None
        } else {
            Some(ObjectRoute {
                key: route.key.clone(),
                namespace: route.namespace.clone(),
                logical_key: route.logical_key.clone(),
                canonical_key: route.canonical_key.clone(),
                sharing_scope: route.sharing_scope.clone(),
                qos_tier: route.qos_tier.clone(),
                version: route.version.next(),
                state: route.state,
                compatibility: route.compatibility.clone(),
                replicas,
                cold_backing: route.cold_backing.clone(),
            })
        };

        let cas = match next.as_ref() {
            Some(next_route) => self
                .route_ops
                .prune_route(&route.key, Some(route.version), next_route)?,
            None => self.route_ops.delete_route(&route.key, Some(route.version))?,
        };
        if !cas.applied {
            match cas.current.as_ref() {
                Some(current) => self.sync_route(current),
                None => self.hot_replicas.clock.lock().remove_id(victim),
            }
            return Ok(false);
        }

        self.hot_replicas.clock.lock().remove_id(victim);
        self.allocator.lock().release(
            &self.runtime,
            &evicted_replica.segment_name,
            evicted_replica.segment_offset,
            evicted_replica.length,
        )?;
        if let Some(next_route) = &next {
            self.sync_route(next_route);
        }
        trace!(
            runtime = %self.runtime,
            key = %route.key.0,
            segment = %evicted_replica.segment_name.0,
            offset_bytes = evicted_replica.segment_offset,
            length_bytes = evicted_replica.length,
            "storage-owner evicted replica via route-owner cas"
        );
        if stable_debug_log_sample(&[
            "storage_owner_evicted_replica",
            &route.key.0,
            &evicted_replica.segment_name.0,
        ]) {
            debug!(
                runtime = %self.runtime,
                key = %route.key.0,
                segment = %evicted_replica.segment_name.0,
                length_bytes = evicted_replica.length,
                "sampled storage-owner replica eviction"
            );
        }
        Ok(true)
    }

    fn rebuild_clock(&self) -> Result<()> {
        let tracker = OperationTracker::new("storage_owner_rebuild_clock");
        let result = (|| {
            let routes = self.collect_routes_by_replica_owner(&self.runtime)?;
            let mut clock = StorageClockState::default();
            for route in &routes {
                clock.track_route(route, &self.runtime);
            }
            *self.hot_replicas.clock.lock() = clock;
            debug!(
                runtime = %self.runtime,
                routes = routes.len(),
                "storage-owner rebuilt eviction clock"
            );
            Ok(())
        })();
        tracker.finish(&result, 0);
        result
    }

    fn collect_routes_by_replica_owner(&self, owner: &ClientRuntimeId) -> Result<Vec<ObjectRoute>> {
        self.route_ops.list_routes_by_replica_owner(owner)
    }

    fn current_materialized_route(&self, key: &ObjectKey) -> Result<Option<ObjectRoute>> {
        let Some(route) = self.route_ops.load_route(key)? else {
            self.hot_replicas.clock.lock().remove_key(key);
            return Ok(None);
        };
        self.sync_route(&route);
        if route.state == RouteState::Active
            && route.cold_backing.as_ref().is_some_and(|cold_backing| {
                cold_backing.state == mooncake_store_core::ColdBackingState::Materialized
            })
        {
            Ok(Some(route))
        } else {
            Ok(None)
        }
    }

    fn reserve_owner_restore_space(
        &self,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        match self.allocator.lock().reserve_any(&self.runtime, length_bytes) {
            Ok(reservation) => Ok(reservation),
            Err(first_error) => {
                if self.evict_one_blocking(None)? {
                    self.allocator.lock().reserve_any(&self.runtime, length_bytes)
                } else {
                    Err(first_error)
                }
            }
        }
    }

    fn sweep_expired_staging_slots(&self, ttl: Duration) -> usize {
        let mut pending = self.pending_staging_slots.lock();
        let before = pending.len();
        pending.retain(|(segment, offset), (_, inserted_at)| {
            if inserted_at.elapsed() < ttl {
                return true;
            }
            self.read_pin_registry.force_unpin(segment, *offset);
            false
        });
        before.saturating_sub(pending.len())
    }

    pub(in super) fn evict_one_clean_and_reserve_bounded(
        &self,
        length_bytes: u64,
        preferred_segment: Option<&SegmentName>,
    ) -> Result<Option<mooncake_store_core::SegmentReservation>> {
        match self.allocator.lock().reserve_any(&self.runtime, length_bytes) {
            Ok(reservation) => Ok(Some(reservation)),
            Err(_) => {
                if self.evict_one_blocking(preferred_segment)? {
                    self.allocator
                        .lock()
                        .reserve_any(&self.runtime, length_bytes)
                        .map(Some)
                } else {
                    Ok(None)
                }
            }
        }
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

impl StorageClockState {
    const READ_HOT_CLOCK_CREDIT: u8 = 2;

    fn eviction_budget(&self) -> usize {
        self.entries.len().saturating_mul(3)
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
        for replica in route
            .replicas
            .iter()
            .filter(|replica| replica.owner == *runtime)
        {
            self.upsert_replica(route, replica, fresh_write);
        }
    }

    fn untrack_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        for replica in route.replicas.iter().filter(|replica| replica.owner == *runtime) {
            self.remove_id(&ClockEntryId {
                route_key: route.key.clone(),
                segment_name: replica.segment_name.clone(),
                segment_offset: replica.segment_offset,
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

    fn mark_hot_keys(&mut self, keys: &[ObjectKey]) -> RouteTrafficReport {
        let mut slots = Vec::new();
        let mut bytes = 0u64;
        for key in keys {
            self.pending_hot_keys.insert(key.clone());
            if let Some(indices) = self.by_key.get(key) {
                for index in indices {
                    if let Some(entry) = self.entries.get(*index).and_then(Option::as_ref) {
                        bytes = bytes.saturating_add(entry.length_bytes);
                        slots.push(*index);
                    }
                }
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

    fn pick_victim_clock(&mut self, preferred_segment: Option<&SegmentName>) -> Option<ClockEntryId> {
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
    ) {
        let id = ClockEntryId {
            route_key: route.key.clone(),
            segment_name: replica.segment_name.clone(),
            segment_offset: replica.segment_offset,
        };
        if let Some(index) = self.by_id.get(&id).copied() {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::as_mut) {
                entry.length_bytes = replica.length;
                if fresh_write && entry.hot_credit == 0 {
                    entry.fresh_write = true;
                }
            }
            return;
        }
        let hot_credit = if self.pending_hot_keys.contains(&route.key) {
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
        let index = self
            .entries
            .iter()
            .position(Option::is_none)
            .unwrap_or(self.entries.len());
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
            }
        }
    }

    fn remove_id(&mut self, id: &ClockEntryId) {
        let Some(index) = self.by_id.remove(id) else {
            return;
        };
        self.entries[index] = None;
        if let Some(indices) = self.by_key.get_mut(&id.route_key) {
            indices.retain(|candidate| *candidate != index);
            if indices.is_empty() {
                self.by_key.remove(&id.route_key);
            }
        }
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
        let mut clock = StorageClockState::default();
        for route in routes {
            clock.track_route(route, runtime);
        }
        *self.clock.lock() = clock;
    }
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
    capacity_bytes
        .saturating_mul(percent as u64)
        .div_ceil(100)
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
