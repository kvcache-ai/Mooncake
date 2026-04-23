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
        let handle = transport.open_segment(segment_name)?;
        self.remote_segments
            .insert(segment_name.to_string(), handle);
        Ok(handle)
    }

    fn invalidate_remote_segment(&mut self, segment_name: &str) {
        self.remote_segments.remove(segment_name);
        self.remote_segment_infos.remove(segment_name);
    }

    fn reopen_segment(
        &mut self,
        transport: &dyn StoreTransport,
        segment_name: &str,
    ) -> Result<u64> {
        self.remote_segment_infos.remove(segment_name);
        if let Some(handle) = self.remote_segments.remove(segment_name) {
            let _ = transport.close_segment(handle);
        }
        self.open_segment(transport, segment_name)
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
}

impl StorageOwnerState {
    fn new(
        runtime: ClientRuntimeId,
        observer: ClientLease,
        route_directory: Arc<dyn RouteDirectory>,
        allocator: Arc<Mutex<LocalAllocatorState>>,
    ) -> Self {
        Self {
            runtime,
            observer,
            route_directory,
            allocator,
            clock: Mutex::new(StorageClockState::default()),
        }
    }

    fn report_route_hits(&self, keys: &[ObjectKey]) -> usize {
        self.clock.lock().mark_hot_keys(keys);
        keys.len()
    }

    fn track_routes(&self, routes: &[ObjectRoute]) -> usize {
        {
            let mut allocator = self.allocator.lock();
            for route in routes {
                allocator.clear_pending_route(route, &self.runtime);
            }
        }
        let mut clock = self.clock.lock();
        for route in routes {
            clock.sync_route(route, &self.runtime);
        }
        routes.len()
    }

    fn track_route(&self, route: &ObjectRoute) {
        self.allocator
            .lock()
            .clear_pending_route(route, &self.runtime);
        self.clock.lock().track_route(route, &self.runtime);
    }

    fn untrack_route(&self, route: &ObjectRoute) {
        self.clock.lock().untrack_route(route, &self.runtime);
    }

    fn sync_route(&self, route: &ObjectRoute) {
        self.allocator
            .lock()
            .clear_pending_route(route, &self.runtime);
        self.clock.lock().sync_route(route, &self.runtime);
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
                debug!(
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
                    let clock = self.clock.lock();
                    clock.eviction_budget()
                };
                for _ in 0..budget.max(1) {
                    let victim = {
                        let mut clock = self.clock.lock();
                        clock.pick_victim(preferred_segment)
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
            .route_directory
            .get_object_route(&self.observer, &victim.route_key)?
        else {
            self.clock.lock().remove_id(victim);
            return Ok(false);
        };
        if route.state != RouteState::Active {
            self.clock.lock().remove_id(victim);
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
        let next = if route.replicas.len() == 1 {
            route_tombstone(&route)
        } else {
            let mut replicas = route.replicas.clone();
            replicas.remove(replica_index);
            replicas.sort_by_key(|replica| replica.priority);
            for (priority, replica) in replicas.iter_mut().enumerate() {
                replica.priority = priority as u16;
            }
            ObjectRoute {
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
            }
        };

        let cas = self.route_directory.compare_and_swap_object_route(
            &self.observer,
            &route.key,
            Some(route.version),
            Some(&next),
        )?;
        if !cas.applied {
            match cas.current.as_ref() {
                Some(current) => self.sync_route(current),
                None => self.clock.lock().remove_id(victim),
            }
            return Ok(false);
        }

        self.clock.lock().remove_id(victim);
        self.allocator.lock().release(
            &self.runtime,
            &evicted_replica.segment_name,
            evicted_replica.segment_offset,
            evicted_replica.length,
        )?;
        self.sync_route(&next);
        debug!(
            runtime = %self.runtime,
            key = %route.key.0,
            segment = %evicted_replica.segment_name.0,
            offset_bytes = evicted_replica.segment_offset,
            length_bytes = evicted_replica.length,
            "storage-owner evicted replica via route-owner cas"
        );
        Ok(true)
    }

    fn rebuild_clock(&self) -> Result<()> {
        let tracker = OperationTracker::new("storage_owner_rebuild_clock");
        let result = (|| {
            let routes = self.collect_routes_by_replica_owner(&self.runtime)?;
            let pending_hot_keys = self.clock.lock().pending_hot_keys.clone();
            let mut clock = StorageClockState {
                pending_hot_keys,
                ..StorageClockState::default()
            };
            for route in &routes {
                clock.track_route(route, &self.runtime);
            }
            *self.clock.lock() = clock;
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
        self.route_directory
            .list_routes_by_replica_owner(&self.observer, owner)
    }

}

impl StorageClockState {
    fn eviction_budget(&self) -> usize {
        self.entries.len().saturating_mul(2)
    }

    fn track_route(&mut self, route: &ObjectRoute, runtime: &ClientRuntimeId) {
        for replica in route.replicas.iter().filter(|replica| replica.owner == *runtime) {
            self.upsert_replica(route, replica);
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

    fn mark_hot_keys(&mut self, keys: &[ObjectKey]) {
        let mut slots = Vec::new();
        for key in keys {
            self.pending_hot_keys.insert(key.clone());
            if let Some(indices) = self.by_key.get(key) {
                slots.extend(indices.iter().copied());
            }
        }
        for index in slots {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::as_mut) {
                entry.hot = true;
            }
        }
    }

    fn pick_victim(&mut self, preferred_segment: Option<&SegmentName>) -> Option<ClockEntryId> {
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
            if entry.hot {
                entry.hot = false;
                continue;
            }
            return Some(entry.id.clone());
        }
        None
    }

    fn upsert_replica(&mut self, route: &ObjectRoute, replica: &ReplicaRoute) {
        let id = ClockEntryId {
            route_key: route.key.clone(),
            segment_name: replica.segment_name.clone(),
            segment_offset: replica.segment_offset,
        };
        if let Some(index) = self.by_id.get(&id).copied() {
            if let Some(entry) = self.entries.get_mut(index).and_then(Option::as_mut) {
                entry.length_bytes = replica.length;
            }
            return;
        }
        let entry = ClockEntry {
            id: id.clone(),
            length_bytes: replica.length,
            hot: self.pending_hot_keys.contains(&route.key),
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
        | StoreError::InvalidState(message) => message.contains("segment handle"),
        _ => false,
    }
}
