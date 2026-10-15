struct LocalAuthorityAdapter {
    lifecycle_state: SharedLifecycleState,
    route_write_gate: SharedRouteWriteGate,
}

fn lifecycle_accepts_writes(lifecycle_state: &SharedLifecycleState, component: &str) -> Result<()> {
    let state = decode_lifecycle_state(lifecycle_state.load(Ordering::SeqCst));
    if state == ClientLifecycleState::Active {
        return Ok(());
    }
    Err(StoreError::InvalidState(format!(
        "{component} is {state:?}; refusing new writes"
    )))
}

impl LocalAuthorityAdapter {
    fn ensure_accepting_writes(&self) -> Result<()> {
        lifecycle_accepts_writes(&self.lifecycle_state, "route authority")
    }

    fn write_guard(&self) -> Result<RouteWritePermit<'_>> {
        let guard = self.route_write_gate.lock();
        self.ensure_accepting_writes()?;
        Ok(guard)
    }
}

impl mooncake_store_route::RouteAuthorityService for LocalAuthorityAdapter {
    fn get_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        authority_get(namespace, authority, key)
    }

    fn list_routes_by_replica_owner(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
    ) -> Result<Vec<ObjectRoute>> {
        authority_list_routes_by_replica_owner(namespace, authority, owner)
    }

    fn list_routes_by_replica_owner_page(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        owner: &ClientRuntimeId,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<mooncake_store_route::RouteOwnerPage> {
        crate::route_directory::authority_list_routes_by_replica_owner_page(
            namespace, authority, owner, cursor, limit,
        )
    }

    fn compare_and_swap_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<CasResult> {
        let _guard = self.write_guard()?;
        authority_compare_and_swap(namespace, authority, key, expected, next)
    }

    fn replace_route(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        key: &ObjectKey,
        next: Option<&ObjectRoute>,
    ) -> Result<()> {
        let _guard = self.write_guard()?;
        authority_replace(namespace, authority, key, next)
    }

    fn batch_get_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<Option<ObjectRoute>>> {
        match authority_get_many(namespace, authority, keys) {
            Ok(routes) => routes.into_iter().map(Ok).collect(),
            Err(error) => keys
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }

    fn batch_contains_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        keys: &[ObjectKey],
    ) -> Vec<Result<bool>> {
        match authority_contains_many(namespace, authority, keys) {
            Ok(results) => results.into_iter().map(Ok).collect(),
            Err(error) => keys
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }

    fn batch_compare_and_swap_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<CasResult>> {
        let _guard = match self.write_guard() {
            Ok(guard) => guard,
            Err(error) => return requests.iter().map(|_| Err(error.clone())).collect(),
        };
        match authority_compare_and_swap_many(namespace, authority, requests) {
            Ok(results) => results.into_iter().map(Ok).collect(),
            Err(error) => requests
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }

    fn batch_replace_routes(
        &self,
        namespace: &str,
        authority: &ClientStableId,
        requests: &[RouteCasRequest],
    ) -> Vec<Result<()>> {
        let _guard = match self.write_guard() {
            Ok(guard) => guard,
            Err(error) => return requests.iter().map(|_| Err(error.clone())).collect(),
        };
        match authority_replace_many(namespace, authority, requests) {
            Ok(()) => requests.iter().map(|_| Ok(())).collect(),
            Err(error) => requests
                .iter()
                .map(|_| Err(StoreError::NotFound(error.to_string())))
                .collect(),
        }
    }
}

struct LocalAllocatorAdapter {
    runtime: ClientRuntimeId,
    allocator: Arc<Mutex<LocalAllocatorState>>,
    storage_owner: Arc<StorageOwnerState>,
    lifecycle_state: SharedLifecycleState,
    transfer_stall_timeout: Duration,
    request_timeout_override: Option<Duration>,
}

impl LocalAllocatorAdapter {
    fn ensure_accepting_writes(&self) -> Result<()> {
        lifecycle_accepts_writes(&self.lifecycle_state, "storage allocator")
    }

    fn pending_publish_deadline_ms(&self, length_bytes: u64) -> u64 {
        pending_publish_deadline_ms(
            length_bytes,
            self.transfer_stall_timeout,
            self.request_timeout_override,
        )
    }

    fn reserve_any_with_eviction(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let mut last_error = None;
        for _ in 0..=32usize {
            let reservation = {
                let mut allocator = self.allocator.lock();
                allocator.reserve_any(owner, length_bytes)
            };
            match reservation {
                Ok(reservation) => {
                    let deadline_ms = self.pending_publish_deadline_ms(reservation.length_bytes);
                    self.allocator
                        .lock()
                        .mark_pending_reservation(&reservation, deadline_ms);
                    return Ok(reservation);
                }
                Err(StoreError::Allocator(message)) => {
                    last_error = Some(message);
                }
                Err(error) => return Err(error),
            }
            if self.storage_owner.has_local_cold_tier_work() {
                cold_tier::ColdTierHandle::kick_offload_for_allocator_eviction(
                    self.storage_owner.as_ref(),
                );
            }
            if !self.storage_owner.evict_one(None)? {
                break;
            }
        }
        Err(StoreError::Allocator(last_error.unwrap_or_else(|| {
            format!("no writable active segment available for {}", owner)
        })))
    }

    fn reserve_specific_with_eviction(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        let mut last_error = None;
        for _ in 0..=32usize {
            let reservation = {
                let mut allocator = self.allocator.lock();
                allocator.reserve_specific(owner, segment_name, length_bytes)
            };
            match reservation {
                Ok(reservation) => {
                    let deadline_ms = self.pending_publish_deadline_ms(reservation.length_bytes);
                    self.allocator
                        .lock()
                        .mark_pending_reservation(&reservation, deadline_ms);
                    return Ok(reservation);
                }
                Err(StoreError::Allocator(message)) => {
                    last_error = Some(message);
                }
                Err(error) => return Err(error),
            }
            if self.storage_owner.has_local_cold_tier_work() {
                cold_tier::ColdTierHandle::kick_offload_for_allocator_eviction(
                    self.storage_owner.as_ref(),
                );
            }
            if !self.storage_owner.evict_one(Some(segment_name))? {
                break;
            }
        }
        Err(StoreError::Allocator(last_error.unwrap_or_else(|| {
            format!(
                "segment capacity exhausted for {}:{} requested={}",
                owner, segment_name.0, length_bytes
            )
        })))
    }
}

impl AllocatorService for LocalAllocatorAdapter {
    fn reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.ensure_accepting_writes()?;
        self.reserve_any_with_eviction(owner, length_bytes)
    }

    fn reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        length_bytes: u64,
    ) -> Result<mooncake_store_core::SegmentReservation> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.ensure_accepting_writes()?;
        self.reserve_specific_with_eviction(owner, segment_name, length_bytes)
    }

    fn release(
        &self,
        owner: &ClientRuntimeId,
        segment_name: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        if *owner != self.runtime {
            return Err(StoreError::InvalidState(format!(
                "allocator rpc targeted runtime {} on {}",
                owner, self.runtime
            )));
        }
        self.allocator
            .lock()
            .release(owner, segment_name, offset_bytes, length_bytes)
    }

    fn batch_reserve_any(
        &self,
        owner: &ClientRuntimeId,
        length_bytes: &[u64],
    ) -> Vec<Result<mooncake_store_core::SegmentReservation>> {
        if *owner != self.runtime {
            return length_bytes
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        if let Err(error) = self.ensure_accepting_writes() {
            return length_bytes.iter().map(|_| Err(error.clone())).collect();
        }
        length_bytes
            .iter()
            .map(|length_bytes| self.reserve_any_with_eviction(owner, *length_bytes))
            .collect()
    }

    fn batch_reserve_specific(
        &self,
        owner: &ClientRuntimeId,
        requests: &[ReserveSpecificOp],
    ) -> Vec<Result<mooncake_store_core::SegmentReservation>> {
        if *owner != self.runtime {
            return requests
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        if let Err(error) = self.ensure_accepting_writes() {
            return requests.iter().map(|_| Err(error.clone())).collect();
        }
        requests
            .iter()
            .map(|request| {
                self.reserve_specific_with_eviction(owner, &request.segment_name, request.length_bytes)
            })
            .collect()
    }

    fn batch_release(&self, owner: &ClientRuntimeId, requests: &[ReleaseOp]) -> Vec<Result<()>> {
        if *owner != self.runtime {
            return requests
                .iter()
                .map(|_| {
                    Err(StoreError::InvalidState(format!(
                        "allocator rpc targeted runtime {} on {}",
                        owner, self.runtime
                    )))
                })
                .collect();
        }
        let mut allocator = self.allocator.lock();
        requests
            .iter()
            .map(|request| {
                allocator.release(
                    owner,
                    &request.segment_name,
                    request.offset_bytes,
                    request.length_bytes,
                )
            })
            .collect()
    }
}

impl EvictionService for LocalAllocatorAdapter {
    fn batch_report_route_hits(&self, keys: &[ObjectKey]) -> Result<RouteTrafficReport> {
        Ok(self.storage_owner.report_route_hits(keys))
    }

    fn batch_track_routes(&self, routes: &[ObjectRoute]) -> Result<RouteTrafficReport> {
        Ok(self.storage_owner.track_routes(routes))
    }
}

#[derive(Clone, Debug)]
struct MigrationExecutionRecord {
    state: pb::MigrationExecutionState,
    attempts: u32,
    last_error: String,
}

#[derive(Clone)]
struct MigrationWorkItem {
    execution_id: String,
    request: pb::SubmitMigrationTaskRequest,
}

#[derive(Clone)]
struct LocalMigrationExecutionContext {
    executor_stable_id: ClientStableId,
    base_lease: ClientLease,
    metadata: Arc<dyn MetadataBackend>,
    transport_factory: Option<Arc<dyn StoreTransportFactory>>,
    default_tenant: String,
    local_memory: LocalMemoryConfig,
    write_mode: WriteMode,
    route_control: RouteControlMode,
    route_topk: usize,
    transfer_stall_timeout: Duration,
    request_timeout_override: Option<Duration>,
    executions: Arc<Mutex<BTreeMap<String, MigrationExecutionRecord>>>,
}

fn panic_payload_message(payload: Box<dyn std::any::Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "migration worker panicked".to_string(),
        },
    }
}

impl LocalMigrationExecutionContext {
    fn update_execution(
        &self,
        execution_id: &str,
        state: pb::MigrationExecutionState,
        attempts: u32,
        last_error: String,
    ) {
        self.executions.lock().insert(
            execution_id.to_string(),
            MigrationExecutionRecord {
                state,
                attempts,
                last_error,
            },
        );
    }

    fn build_helper_writer(&self, execution_id: &str) -> Result<StoreClient> {
        let Some(factory) = self.transport_factory.clone() else {
            return Err(StoreError::Unsupported(
                "migration helper writer requires a transport factory".to_string(),
            ));
        };

        let helper_stable_id = format!(
            "{}-migration-helper-{}",
            self.executor_stable_id.0, execution_id
        );
        let helper_segment_name = format!(
            "{}-segment-{}",
            helper_stable_id.replace(':', "-"),
            self.base_lease.runtime.epoch.0
        );
        let helper_transport = factory.create(&helper_segment_name)?;
        let helper_expiry_ms = self
            .base_lease
            .expires_at_ms
            .max(now_ms().saturating_add(30_000));

        let mut builder = StoreClientBuilder::new(self.metadata.clone(), helper_stable_id)
            .tenant_scoped_metadata()
            .state(ClientLifecycleState::Active)
            .activate_on_local_memory_registration()
            .tenant(self.default_tenant.clone())
            .compatibility(self.base_lease.compatibility.clone())
            .local_memory(self.local_memory.clone())
            .transport(helper_transport)
            .transport_factory(factory)
            .route_control(self.route_control)
            .route_topk(self.route_topk)
            .transfer_timeout(self.transfer_stall_timeout)
            .startup_prewarm_max_delay(Duration::ZERO);
        if let Some(timeout) = self.request_timeout_override {
            builder = builder.request_timeout(timeout);
        }

        let mut labels = self.base_lease.endpoints.labels.clone();
        labels.remove(control_address_label());
        labels.insert("storage".to_string(), "false".to_string());
        labels.insert("route".to_string(), "false".to_string());
        for (key, value) in labels {
            builder = builder.label(key, value);
        }

        if let WriteMode::Routed {
            planner,
            replica_count,
        } = &self.write_mode
        {
            builder = builder.routed_writes(planner.clone(), *replica_count);
        }

        let helper = builder.build(helper_expiry_ms)?;
        helper.register_local_memory()?;
        Ok(helper)
    }

    fn build_task_plan(
        &self,
        request: &pb::SubmitMigrationTaskRequest,
    ) -> Result<(LogicalObjectId, ExplicitMigrationPlan)> {
        let mode = match pb::MigrationMode::try_from(request.mode) {
            Ok(pb::MigrationMode::Copy) => ExplicitMigrationMode::Copy,
            Ok(pb::MigrationMode::Move) => ExplicitMigrationMode::Move,
            Ok(pb::MigrationMode::Unspecified) => {
                return Err(StoreError::InvalidState(
                    "migration task is missing mode".to_string(),
                ));
            }
            Err(_) => {
                return Err(StoreError::InvalidState(format!(
                    "migration task has invalid mode value {}",
                    request.mode
                )));
            }
        };
        let object_id = LogicalObjectId::new(
            NamespaceScope::with_defaults(
                Some(request.tenant.as_str()),
                (!request.domain.is_empty()).then_some(request.domain.as_str()),
                (!request.object_set.is_empty()).then_some(request.object_set.as_str()),
            ),
            request.key.as_str(),
        );
        let plan = ExplicitMigrationPlan {
            mode,
            source: ReplicaReadSelector::Segment(SegmentName::new(request.source_segment.clone())),
            target_segments: request
                .target_segments
                .iter()
                .cloned()
                .map(SegmentName::new)
                .collect(),
            all_or_nothing: true,
        };
        Ok((object_id, plan))
    }

    fn execute_task(
        &self,
        execution_id: String,
        request: pb::SubmitMigrationTaskRequest,
    ) {
        self.update_execution(
            &execution_id,
            pb::MigrationExecutionState::Running,
            1,
            String::new(),
        );
        let result = (|| -> Result<()> {
            let helper = self.build_helper_writer(&execution_id)?;
            let (object_id, plan) = self.build_task_plan(&request)?;
            helper.execute_explicit_route_migration(&object_id, &plan)?;
            Ok(())
        })();
        match result {
            Ok(()) => self.update_execution(
                &execution_id,
                pb::MigrationExecutionState::Succeeded,
                1,
                String::new(),
            ),
            Err(error) => self.update_execution(
                &execution_id,
                pb::MigrationExecutionState::Failed,
                1,
                error.to_string(),
            ),
        }
    }
}

#[derive(Clone)]
struct LocalMigrationAdapter {
    executor_stable_id: ClientStableId,
    executions: Arc<Mutex<BTreeMap<String, MigrationExecutionRecord>>>,
    next_execution_id: Arc<AtomicU64>,
    task_sender: std::sync::mpsc::Sender<MigrationWorkItem>,
}

impl LocalMigrationAdapter {
    fn new(context: LocalMigrationExecutionContext) -> Self {
        let executions = context.executions.clone();
        let next_execution_id = Arc::new(AtomicU64::new(1));
        let (task_sender, task_receiver) = std::sync::mpsc::channel::<MigrationWorkItem>();
        let worker_context = context.clone();
        let worker_name = format!("store-migration-{}", context.executor_stable_id.0);
        if let Err(error) = std::thread::Builder::new()
            .name(worker_name)
            .spawn(move || {
                while let Ok(work_item) = task_receiver.recv() {
                    worker_context.update_execution(
                        &work_item.execution_id,
                        pb::MigrationExecutionState::Dispatching,
                        1,
                        String::new(),
                    );
                    let execution_id = work_item.execution_id.clone();
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        worker_context.execute_task(work_item.execution_id, work_item.request);
                    }));
                    if let Err(payload) = result {
                        worker_context.update_execution(
                            &execution_id,
                            pb::MigrationExecutionState::Failed,
                            1,
                            format!(
                                "migration execution panicked: {}",
                                panic_payload_message(payload)
                            ),
                        );
                    }
                }
            })
        {
            warn!(
                executor = %context.executor_stable_id,
                error = %error,
                "failed to spawn local migration worker"
            );
        }

        Self {
            executor_stable_id: context.executor_stable_id,
            executions,
            next_execution_id,
            task_sender,
        }
    }
}

impl MigrationService for LocalMigrationAdapter {
    fn submit_task(&self, request: &pb::SubmitMigrationTaskRequest) -> Result<String> {
        if request.task_executor != self.executor_stable_id.0 {
            return Err(StoreError::InvalidState(format!(
                "migration task targets executor {} but request was submitted to {}",
                request.task_executor, self.executor_stable_id
            )));
        }

        let sequence = self.next_execution_id.fetch_add(1, Ordering::SeqCst);
        let execution_id = format!("{}-{sequence}", self.executor_stable_id.0);
        self.executions.lock().insert(
            execution_id.clone(),
            MigrationExecutionRecord {
                state: pb::MigrationExecutionState::Pending,
                attempts: 0,
                last_error: String::new(),
            },
        );
        let send_result = self.task_sender
            .send(MigrationWorkItem {
                execution_id: execution_id.clone(),
                request: request.clone(),
            })
            .map_err(|_| {
                StoreError::Transport(
                    "migration task executor worker is not accepting new tasks".to_string(),
                )
            });
        if let Err(error) = send_result {
            self.executions.lock().insert(
                execution_id.clone(),
                MigrationExecutionRecord {
                    state: pb::MigrationExecutionState::Failed,
                    attempts: 0,
                    last_error: error.to_string(),
                },
            );
            return Err(error);
        }
        Ok(execution_id)
    }

    fn get_execution_status(&self, execution_id: &str) -> Result<MigrationExecutionStatus> {
        let record = self
            .executions
            .lock()
            .get(execution_id)
            .cloned()
            .ok_or_else(|| {
                StoreError::NotFound(format!(
                    "migration execution {} is not known by {}",
                    execution_id, self.executor_stable_id
                ))
            })?;
        Ok(MigrationExecutionStatus {
            state: record.state,
            attempts: record.attempts,
            last_error: record.last_error,
        })
    }
}
