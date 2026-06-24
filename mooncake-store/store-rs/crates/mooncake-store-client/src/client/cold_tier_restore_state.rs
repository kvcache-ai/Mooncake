// Cold tier restore singleflight and promotion queue impls.
// Included via `include!()` at module level in state_store.rs.

impl ColdRestoreSingleflight {
    fn new(max_distinct_flights: usize) -> Self {
        Self {
            state: StdMutex::new(ColdRestoreSingleflightState::default()),
            max_distinct_flights: max_distinct_flights.max(1),
        }
    }

    fn begin(self: &Arc<Self>, key: ColdRestoreFlightKey) -> ColdRestoreFlightRegistration {
        let (registration, metric_result) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(flight) = state.flights.get(&key) {
                flight.waiters.fetch_add(1, Ordering::Relaxed);
                (
                    ColdRestoreFlightRegistration::Waiter(flight.clone()),
                    "waiter",
                )
            } else if state.flights.len() >= self.max_distinct_flights {
                (
                    ColdRestoreFlightRegistration::Rejected,
                    "rejected",
                )
            } else {
                let flight = Arc::new(ColdRestoreFlight {
                    state: StdMutex::new(ColdRestoreFlightState::default()),
                    completed: StdCondvar::new(),
                    waiters: AtomicUsize::new(0),
                });
                state.flights.insert(key.clone(), flight.clone());
                (
                    ColdRestoreFlightRegistration::Leader(ColdRestoreFlightLeader {
                        key,
                        flight,
                        singleflight: self.clone(),
                        completed: false,
                    }),
                    "leader",
                )
            }
        };
        crate::observability::registry::record_cold_restore_singleflight("begin", metric_result);
        registration
    }

    fn wait(&self, flight: &ColdRestoreFlight) -> Result<Arc<Vec<u8>>> {
        let mut state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
        while state.result.is_none() {
            state = flight
                .completed
                .wait(state)
                .unwrap_or_else(|e| e.into_inner());
        }
        let result = state
            .result
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Err(StoreError::InvalidState(
                "cold restore flight result missing after wake".to_string(),
            )));
        crate::observability::registry::record_cold_restore_singleflight(
            "wait",
            if result.is_ok() { "ok" } else { "error" },
        );
        result
    }

    fn finish(
        &self,
        key: &ColdRestoreFlightKey,
        flight: &ColdRestoreFlight,
        result: Result<Arc<Vec<u8>>>,
    ) {
        let metric_result = if result.is_ok() { "ok" } else { "error" };
        {
            let mut flight_state = flight.state.lock().unwrap_or_else(|e| e.into_inner());
            flight_state.result = Some(result);
        }
        flight.completed.notify_all();
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.flights.remove(key);
        }
        crate::observability::registry::record_cold_restore_singleflight("finish", metric_result);
    }

}

impl ColdRestoreFlightLeader {
    fn finish(mut self, result: Result<Arc<Vec<u8>>>) -> Result<Arc<Vec<u8>>> {
        self.singleflight.finish(&self.key, &self.flight, result.clone());
        self.completed = true;
        result
    }
}

impl Drop for ColdRestoreFlightLeader {
    fn drop(&mut self) {
        if !self.completed {
            self.singleflight.finish(
                &self.key,
                &self.flight,
                Err(StoreError::InvalidState(
                    "cold restore leader exited before publishing result".to_string(),
                )),
            );
        }
    }
}

impl RestorePromotionQueue {
    fn new(limit: usize, batch_limit: usize, max_in_flight: usize) -> Self {
        Self {
            state: Mutex::new(RestorePromotionQueueState::default()),
            limit: limit.max(1),
            batch_limit: batch_limit.max(1),
            max_in_flight: max_in_flight.max(1),
        }
    }

    fn from_rate_limits(rate_limits: ColdTierRateLimitConfig) -> Self {
        Self::new(
            rate_limits.restore_promotion_queue_limit,
            rate_limits.restore_promotion_batch_limit,
            rate_limits.restore_promotion_max_in_flight,
        )
    }

    fn push(&self, task: RestorePromotionTask) -> RestorePromotionPushOutcome {
        let mut state = self.state.lock();
        if state.shutdown {
            return RestorePromotionPushOutcome::Shutdown;
        }
        if state.keys.contains(&task.key) {
            return RestorePromotionPushOutcome::Duplicate;
        }
        if state.in_flight.contains(&task.key) {
            return RestorePromotionPushOutcome::InFlight;
        }
        if state.entries.len() >= self.limit {
            return RestorePromotionPushOutcome::Full;
        }
        state.keys.insert(task.key.clone());
        state.entries.push_back(task);
        RestorePromotionPushOutcome::Accepted
    }

    fn take_ready_batch(&self) -> Vec<RestorePromotionTask> {
        let mut state = self.state.lock();
        if state.worker_active {
            return Vec::new();
        }
        let tasks = self.take_ready_batch_locked(&mut state);
        if !tasks.is_empty() {
            state.worker_active = true;
        }
        tasks
    }

    fn take_next_worker_batch(&self) -> Vec<RestorePromotionTask> {
        let mut state = self.state.lock();
        if !state.worker_active || state.shutdown {
            return Vec::new();
        }
        self.take_ready_batch_locked(&mut state)
    }

    fn take_ready_batch_locked(
        &self,
        state: &mut RestorePromotionQueueState,
    ) -> Vec<RestorePromotionTask> {
        if state.entries.is_empty() {
            return Vec::new();
        }
        let available_in_flight = self.max_in_flight.saturating_sub(state.in_flight.len());
        if available_in_flight == 0 {
            return Vec::new();
        }
        let take = self.batch_limit.min(available_in_flight).min(state.entries.len());
        let tasks = state.entries.drain(..take).collect::<Vec<_>>();
        for task in &tasks {
            state.keys.remove(&task.key);
            state.in_flight.insert(task.key.clone());
        }
        tasks
    }

    fn complete(&self, key: &RestorePromotionKey) {
        self.state.lock().in_flight.remove(key);
    }

    fn abort_worker_batch(&self, tasks: Vec<RestorePromotionTask>) {
        let mut state = self.state.lock();
        for task in tasks.into_iter().rev() {
            state.in_flight.remove(&task.key);
            if !state.shutdown && !state.keys.contains(&task.key) {
                state.keys.insert(task.key.clone());
                state.entries.push_front(task);
            }
        }
        state.worker_active = false;
    }

    fn finish_worker(&self) {
        self.state.lock().worker_active = false;
    }

    fn shutdown(&self) {
        // Set shutdown flag FIRST to prevent the worker from picking up new batches.
        // Without this, wait_for_worker_idle can spin forever because the worker
        // keeps draining and refilling from the queue.
        {
            let mut state = self.state.lock();
            state.shutdown = true;
            state.entries.clear();
            state.keys.clear();
        }
        // Wait for in-flight tasks to complete, with a timeout to avoid hanging
        // if the worker is stuck in a transport write or metadata operation.
        self.wait_for_worker_idle_bounded(Duration::from_secs(5));
    }

    fn wait_for_worker_idle(&self) {
        self.wait_for_worker_idle_bounded(Duration::from_secs(30));
    }

    fn wait_for_worker_idle_bounded(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let idle = {
                let state = self.state.lock();
                !state.worker_active && state.in_flight.is_empty()
            };
            if idle {
                return;
            }
            if Instant::now() >= deadline {
                tracing::warn!(
                    timeout_ms = timeout.as_millis() as u64,
                    "restore promotion queue shutdown timed out waiting for worker idle"
                );
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

impl Default for RestorePromotionQueue {
    fn default() -> Self {
        Self::from_rate_limits(ColdTierRateLimitConfig::default())
    }
}

impl Default for ColdRestoreSingleflight {
    fn default() -> Self {
        Self::new(ColdTierRateLimitConfig::default().restore_max_distinct_flights)
    }
}

impl StoreState {
    /// Augment `SegmentInfo` with buffers derived from cached `target_chunks`.
    ///
    /// When the staging pool's memory is adopted by the transport but not
    /// reflected in the transport's segment info (as seen by remote readers),
    /// this bridges the gap: target_chunks from the cold restore gRPC reply
    /// describe the staging pool's actual memory layout.
    ///
    /// For normal DRAM reads, all target_chunks are already within the
    /// existing SegmentInfo buffers, so this is a no-op.
    #[allow(dead_code)]
    fn augment_segment_info_from_target_metadata(
        &self,
        info: &mut SegmentInfo,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
    ) {
        let Some(metadata) = self
            .segment_target_metadata
            .get(&(owner.clone(), segment_name.clone()))
        else {
            return;
        };
        for chunk in &metadata.target_chunks {
            let chunk_end = chunk.target_offset.saturating_add(chunk.length_bytes);
            let already_covered = info.buffers.iter().any(|buf| {
                let buf_end = buf.base.saturating_add(buf.length);
                chunk.target_offset >= buf.base && chunk_end <= buf_end
            });
            if !already_covered {
                info.buffers.push(SegmentBuffer {
                    base: chunk.target_offset,
                    length: chunk.length_bytes,
                    location: String::new(),
                });
            }
        }
    }
}
