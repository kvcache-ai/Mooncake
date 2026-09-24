// Cold tier admin service: types, task manager, helpers, and impl methods.
// Included via `include!()` at module level in service.rs.

#[derive(Default)]
struct ColdTierBlockerSummary {
    blocked_objects: u64,
    reclaimable_objects: u64,
    pending_delete_objects: u64,
}

const DEFAULT_COLD_TIER_OFFLOAD_BATCH_SIZE: u64 = 16;

#[derive(Clone)]
struct ColdTierOffloadTaskManager {
    state: Arc<ColdTierOffloadTaskManagerState>,
}

struct ColdTierOffloadTaskManagerState {
    backend: Arc<dyn MetadataBackend>,
    rpc: Arc<dyn MigrationRpc>,
    next_task_id: AtomicU64,
    tasks: Mutex<BTreeMap<String, ColdTierOffloadTaskRecord>>,
}

#[derive(Clone, Debug)]
struct ColdTierOffloadTaskRecord {
    task_id: String,
    stable_id: String,
    epoch: Option<u64>,
    max_tasks: u64,
    batch_size: u64,
    state: ColdTierOffloadTaskState,
    materialized: u64,
    batches: u64,
    last_error: String,
    created_at_ms: u64,
    updated_at_ms: u64,
}

impl ColdTierOffloadTaskRecord {
    fn to_response(&self) -> TriggerColdTierOffloadResponse {
        TriggerColdTierOffloadResponse {
            task_id: self.task_id.clone(),
            stable_id: self.stable_id.clone(),
            epoch: self.epoch,
            max_tasks: self.max_tasks,
            batch_size: self.batch_size,
            state: self.state,
            materialized: self.materialized,
            batches: self.batches,
            last_error: self.last_error.clone(),
            created_at_ms: self.created_at_ms,
            updated_at_ms: self.updated_at_ms,
        }
    }
}

/// Retain completed/failed tasks for at most 10 minutes before pruning.
const COLD_TIER_OFFLOAD_TASK_RETAIN_MS: u64 = 10 * 60 * 1000;

impl ColdTierOffloadTaskManager {
    fn new(backend: Arc<dyn MetadataBackend>, rpc: Arc<dyn MigrationRpc>) -> Self {
        let state = Arc::new(ColdTierOffloadTaskManagerState {
            backend,
            rpc,
            next_task_id: AtomicU64::new(1),
            tasks: Mutex::new(BTreeMap::new()),
        });
        Self { state }
    }

    fn submit(
        &self,
        stable_id: String,
        max_tasks: u64,
    ) -> AdminResult<TriggerColdTierOffloadResponse> {
        if stable_id.trim().is_empty() {
            return Err(StoreError::InvalidState(
                "cold tier offload trigger is missing stable_id".to_string(),
            ));
        }
        if max_tasks == 0 {
            return Err(StoreError::InvalidState(
                "cold tier offload trigger max_tasks must be greater than zero".to_string(),
            ));
        }
        self.prune_finished_tasks();
        let now = now_ms();
        let sequence = self.state.next_task_id.fetch_add(1, Ordering::SeqCst);
        let task_id = format!("cold-tier-offload-{sequence}");
        let record = ColdTierOffloadTaskRecord {
            task_id: task_id.clone(),
            stable_id,
            epoch: None,
            max_tasks,
            batch_size: DEFAULT_COLD_TIER_OFFLOAD_BATCH_SIZE.min(max_tasks).max(1),
            state: ColdTierOffloadTaskState::Pending,
            materialized: 0,
            batches: 0,
            last_error: String::new(),
            created_at_ms: now,
            updated_at_ms: now,
        };
        let response = record.to_response();
        self.state.tasks.lock().insert(task_id, record);
        self.state.start_worker(response.task_id.clone());
        Ok(response)
    }

    fn prune_finished_tasks(&self) {
        let now = now_ms();
        let mut tasks = self.state.tasks.lock();
        tasks.retain(|_, record| {
            match record.state {
                ColdTierOffloadTaskState::Succeeded | ColdTierOffloadTaskState::Failed => {
                    now.saturating_sub(record.updated_at_ms) < COLD_TIER_OFFLOAD_TASK_RETAIN_MS
                }
                _ => true,
            }
        });
    }

    fn get(&self, task_id: &str) -> AdminResult<TriggerColdTierOffloadResponse> {
        self.prune_finished_tasks();
        self.state
            .tasks
            .lock()
            .get(task_id)
            .cloned()
            .map(|record| record.to_response())
            .ok_or_else(|| {
                StoreError::NotFound(format!("cold tier offload task {task_id} was not found"))
            })
    }

    fn list(&self) -> ListColdTierOffloadTasksResponse {
        self.prune_finished_tasks();
        let mut tasks = self
            .state
            .tasks
            .lock()
            .values()
            .map(|record| record.to_response())
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| {
            left.created_at_ms
                .cmp(&right.created_at_ms)
                .then_with(|| left.task_id.cmp(&right.task_id))
        });
        ListColdTierOffloadTasksResponse {
            count: tasks.len(),
            tasks,
        }
    }
}

impl ColdTierOffloadTaskManagerState {
    fn start_worker(self: &Arc<Self>, task_id: String) {
        let state = Arc::clone(self);
        let thread_name = format!("mooncake-admin-cold-offload-{task_id}");
        let task_id_clone = task_id.clone();
        if let Err(error) = thread::Builder::new()
            .name(thread_name)
            .spawn(move || state.run_task(&task_id_clone))
        {
            warn!(%error, "failed to spawn cold tier offload worker thread");
            self.finish_task(
                &task_id,
                ColdTierOffloadTaskState::Failed,
                None,
                0,
                0,
                format!("failed to spawn worker thread: {error}"),
            );
        }
    }

    fn run_task(&self, task_id: &str) {
        let Some(mut record) = self.mark_running(task_id) else {
            return;
        };
        let lease = match resolve_live_lease_from_backend(self.backend.as_ref(), &record.stable_id)
        {
            Ok(lease) => lease,
            Err(error) => {
                self.finish_task(
                    task_id,
                    ColdTierOffloadTaskState::Failed,
                    None,
                    0,
                    0,
                    error.to_string(),
                );
                return;
            }
        };
        self.update_epoch(task_id, lease.runtime.epoch.0);
        record.epoch = Some(lease.runtime.epoch.0);
        while record.materialized < record.max_tasks {
            let remaining = record.max_tasks.saturating_sub(record.materialized);
            let batch = record.batch_size.min(remaining).max(1);
            match self.rpc.trigger_cold_tier_offload(&lease, batch) {
                Ok(materialized) => {
                    record.batches = record.batches.saturating_add(1);
                    record.materialized = record
                        .materialized
                        .saturating_add(materialized)
                        .min(record.max_tasks);
                    self.update_progress(task_id, record.materialized, record.batches);
                    if materialized == 0 {
                        break;
                    }
                }
                Err(error) => {
                    self.finish_task(
                        task_id,
                        ColdTierOffloadTaskState::Failed,
                        Some(lease.runtime.epoch.0),
                        record.materialized,
                        record.batches,
                        error.to_string(),
                    );
                    return;
                }
            }
        }
        self.finish_task(
            task_id,
            ColdTierOffloadTaskState::Succeeded,
            Some(lease.runtime.epoch.0),
            record.materialized,
            record.batches,
            String::new(),
        );
    }

    fn mark_running(&self, task_id: &str) -> Option<ColdTierOffloadTaskRecord> {
        let mut tasks = self.tasks.lock();
        let record = tasks.get_mut(task_id)?;
        record.state = ColdTierOffloadTaskState::Running;
        record.updated_at_ms = now_ms();
        Some(record.clone())
    }

    fn update_epoch(&self, task_id: &str, epoch: u64) {
        if let Some(record) = self.tasks.lock().get_mut(task_id) {
            record.epoch = Some(epoch);
            record.updated_at_ms = now_ms();
        }
    }

    fn update_progress(&self, task_id: &str, materialized: u64, batches: u64) {
        if let Some(record) = self.tasks.lock().get_mut(task_id) {
            record.materialized = materialized;
            record.batches = batches;
            record.updated_at_ms = now_ms();
        }
    }

    fn finish_task(
        &self,
        task_id: &str,
        state: ColdTierOffloadTaskState,
        epoch: Option<u64>,
        materialized: u64,
        batches: u64,
        last_error: String,
    ) {
        if let Some(record) = self.tasks.lock().get_mut(task_id) {
            record.state = state;
            record.epoch = epoch.or(record.epoch);
            record.materialized = materialized;
            record.batches = batches;
            record.last_error = last_error;
            record.updated_at_ms = now_ms();
        }
    }
}

fn validate_cold_tier_device_request(request: &CreateColdTierDeviceRequest) -> AdminResult<()> {
    // Admin validation is metadata-only: runtime-local filesystem checks and directory creation
    // stay in storage runtimes when they resolve or mount cold-tier targets.
    if request.stable_id.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "cold tier device is missing stable_id".to_string(),
        ));
    }
    if request.cold_tier_id.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "cold tier device is missing cold_tier_id".to_string(),
        ));
    }
    if request.kind.trim().is_empty() {
        return Err(StoreError::InvalidState(
            "cold tier device is missing kind".to_string(),
        ));
    }
    match &request.target {
        ColdTierTargetSpec::Directory { path } if path.trim().is_empty() => Err(
            StoreError::InvalidState("cold tier directory target is missing path".to_string()),
        ),
        ColdTierTargetSpec::Uuid { uuid } if uuid.trim().is_empty() => Err(
            StoreError::InvalidState("cold tier uuid target is missing uuid".to_string()),
        ),
        _ => Ok(()),
    }
}

pub(crate) fn parse_cold_tier_device_state(value: &str) -> AdminResult<ColdTierDeviceState> {
    match value {
        "unregistered" => Ok(ColdTierDeviceState::Unregistered),
        "healthy" => Ok(ColdTierDeviceState::Healthy),
        "full" => Ok(ColdTierDeviceState::Full),
        "disabled_by_admin" => Ok(ColdTierDeviceState::DisabledByAdmin),
        "draining" => Ok(ColdTierDeviceState::Draining),
        "failed" => Ok(ColdTierDeviceState::Failed),
        _ => Err(StoreError::InvalidState(
            "invalid cold tier device state query parameter".to_string(),
        )),
    }
}

fn format_cold_backing_state(state: ColdBackingState) -> &'static str {
    match state {
        ColdBackingState::PendingOffload => "pending_offload",
        ColdBackingState::Materialized => "materialized",
        ColdBackingState::PendingDelete => "pending_delete",
    }
}

fn encode_cold_tier_path_component(value: &str) -> String {
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

fn cold_tier_root_dir(device: &ColdTierDeviceRecord) -> AdminResult<&Path> {
    if let Some(root_dir) = device.root_dir.as_ref() {
        return Ok(Path::new(root_dir));
    }
    match &device.target {
        ColdTierTargetSpec::Directory { path } => Ok(Path::new(path)),
        _ => Err(StoreError::Unsupported(format!(
            "cold tier quarantine report is only supported for directory targets; device {} has target {:?}",
            device.device_id, device.target
        ))),
    }
}

fn file_modified_at_ms(metadata: &std::fs::Metadata) -> Option<u64> {
    metadata
        .modified()
        .ok()
        .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
}

fn scan_cold_tier_quarantine_dir(
    area: &str,
    directory: &Path,
) -> AdminResult<Vec<ColdTierQuarantineFile>> {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(StoreError::Transport(format!(
                "failed to scan cold tier quarantine directory {}: {error}",
                directory.display()
            )));
        }
    };
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            StoreError::Transport(format!(
                "failed to scan cold tier quarantine directory {}: {error}",
                directory.display()
            ))
        })?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|error| {
            StoreError::Transport(format!(
                "failed to stat cold tier quarantine file {}: {error}",
                path.display()
            ))
        })?;
        if !metadata.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        files.push(ColdTierQuarantineFile {
            area: area.to_string(),
            file_name: file_name.to_string(),
            encoded_path: path.to_string_lossy().into_owned(),
            size_bytes: metadata.len(),
            modified_at_ms: file_modified_at_ms(&metadata),
        });
    }
    files.sort_by(|left, right| {
        left.area
            .cmp(&right.area)
            .then_with(|| left.file_name.cmp(&right.file_name))
    });
    Ok(files)
}

fn cold_tier_device_response(device: &ColdTierDeviceRecord) -> ColdTierDeviceResponse {
    ColdTierDeviceResponse {
        device_id: device.device_id.clone(),
        stable_id: device.stable_id.clone(),
        epoch: device.epoch,
        cold_tier_id: device.cold_tier_id.clone(),
        kind: device.kind.clone(),
        target: device.target.clone(),
        root_dir: device.root_dir.clone(),
        state: device.state,
        schedulable: device.schedulable(),
        capacity_bytes: device.capacity_bytes,
        used_bytes: device.used_bytes,
        reserved_bytes: device.reserved_bytes,
        free_bytes: device.free_bytes(),
        failure_count: device.failure_count,
        last_error: device.last_error.clone(),
        tags: device.tags.clone(),
    }
}

fn require_device(
    backend: &dyn MetadataBackend,
    device_id: &str,
) -> AdminResult<ColdTierDeviceRecord> {
    backend.get_cold_tier_device(device_id)?.ok_or_else(|| {
        StoreError::NotFound(format!("cold tier device {device_id} not found"))
    })
}

impl AdminService {

pub fn trigger_cold_tier_offload(
    &self,
    request: TriggerColdTierOffloadRequest,
) -> AdminResult<TriggerColdTierOffloadResponse> {
    if request.max_tasks == Some(0) {
        return Err(StoreError::InvalidState(
            "cold tier offload trigger max_tasks must be greater than zero".to_string(),
        ));
    }
    let max_tasks = request.max_tasks.unwrap_or(64);
    let response = self
        .cold_tier_offloads
        .submit(request.stable_id, max_tasks)?;
    info!(
        task_id = %response.task_id,
        stable_id = %response.stable_id,
        max_tasks,
        batch_size = response.batch_size,
        "admin submitted cold tier pending offload materialization task"
    );
    Ok(response)
}

pub fn get_cold_tier_offload_task(
    &self,
    task_id: &str,
) -> AdminResult<TriggerColdTierOffloadResponse> {
    self.cold_tier_offloads.get(task_id)
}

pub fn list_cold_tier_offload_tasks(&self) -> ListColdTierOffloadTasksResponse {
    self.cold_tier_offloads.list()
}

pub fn create_cold_tier_device(
    &self,
    tenant: Option<&str>,
    request: CreateColdTierDeviceRequest,
) -> AdminResult<CreateColdTierDeviceResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    validate_cold_tier_device_request(&request)?;
    // Admin create currently uses cold_tier_id as the device key so persisted routes can keep
    // using their compatibility cold_tier_id field as the backend lookup key.
    let device_id = request.cold_tier_id.clone();
    let existing = backend.list_cold_tier_devices(&ColdTierDeviceFilter {
        stable_id: Some(request.stable_id.clone()),
        ..ColdTierDeviceFilter::default()
    })?;
    if existing.iter().any(|device| {
        device.cold_tier_id == request.cold_tier_id && device.device_id != device_id
    }) {
        return Err(StoreError::Conflict(format!(
            "cold tier device {} already exists with a different target definition",
            request.cold_tier_id
        )));
    }
    let record = ColdTierDeviceRecord {
        device_id: device_id.clone(),
        stable_id: request.stable_id,
        epoch: None,
        cold_tier_id: request.cold_tier_id,
        kind: request.kind,
        target: request.target.clone(),
        root_dir: None,
        state: ColdTierDeviceState::Unregistered,
        capacity_bytes: request.capacity_override_bytes,
        used_bytes: 0,
        reserved_bytes: 0,
        failure_count: 0,
        last_error: None,
        tags: request.tags,
        updated_at_ms: now_ms(),
    };
    match backend.put_cold_tier_device_if_absent(&record)? {
        ColdTierPutDeviceResult::Created(created) => Ok(CreateColdTierDeviceResponse {
            device: cold_tier_device_response(&created),
            message: "cold tier device definition created".to_string(),
        }),
        ColdTierPutDeviceResult::Existing(existing) => {
            if existing.stable_id != record.stable_id
                || existing.cold_tier_id != record.cold_tier_id
                || existing.kind != record.kind
                || existing.target != record.target
            {
                return Err(StoreError::Conflict(format!(
                    "cold tier device {} already exists with a different target definition",
                    record.cold_tier_id
                )));
            }
            Ok(CreateColdTierDeviceResponse {
                device: cold_tier_device_response(&existing),
                message: "cold tier device definition already exists".to_string(),
            })
        }
    }
}

pub fn list_cold_tier_devices(
    &self,
    tenant: Option<&str>,
    stable_id: Option<&str>,
    state: Option<ColdTierDeviceState>,
    schedulable: Option<bool>,
    kind: Option<&str>,
) -> AdminResult<ListColdTierDevicesResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let devices = backend
        .list_cold_tier_devices(&ColdTierDeviceFilter {
            stable_id: stable_id.map(ToString::to_string),
            state,
            schedulable,
            kind: kind.map(ToString::to_string),
        })?
        .into_iter()
        .map(|device| cold_tier_device_response(&device))
        .collect();
    Ok(ListColdTierDevicesResponse { devices })
}

pub fn get_cold_tier_device(
    &self,
    tenant: Option<&str>,
    device_id: &str,
) -> AdminResult<ColdTierDeviceResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let mut device = require_device(backend.as_ref(), device_id)?;
    if let Ok(lease) = resolve_live_lease_from_backend(backend.as_ref(), &device.stable_id) {
        if let Ok(probe) = self
            .migrations
            .state
            .rpc
            .probe_cold_tier_device(&lease, device_id)
        {
            device.capacity_bytes = Some(probe.capacity_bytes);
            device.used_bytes = probe.used_bytes;
            device.reserved_bytes = probe.reserved_bytes;
            device.last_error = if probe.last_error.is_empty() {
                None
            } else {
                Some(probe.last_error)
            };
        }
    }
    Ok(cold_tier_device_response(&device))
}

pub fn register_cold_tier_device(
    &self,
    tenant: Option<&str>,
    device_id: &str,
    request: ColdTierRegisterRequest,
) -> AdminResult<ColdTierRegisterResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    let epoch = current_live_epoch(backend.as_ref(), &device.stable_id)?;
    let next = if request.dry_run {
        device.clone()
    } else {
        let mut update = ColdTierDeviceUpdate::new(now_ms());
        update.expected_updated_at_ms = Some(device.updated_at_ms);
        update.epoch = Some(Some(epoch));
        update.state = Some(ColdTierDeviceState::Healthy);
        update.last_error = Some(None);
        backend.update_cold_tier_device(device_id, update)?
    };
    Ok(ColdTierRegisterResponse {
        device_id: next.device_id.clone(),
        stable_id: next.stable_id.clone(),
        epoch: Some(epoch),
        cold_tier_id: next.cold_tier_id.clone(),
        state: next.state,
        schedulable: next.schedulable(),
        dry_run: request.dry_run,
        scanned_objects: if request.scan_existing {
            Self::count_cold_tier_backing_objects(backend.as_ref(), device_id)?
        } else {
            0
        },
        used_bytes: next.used_bytes,
        message: if request.dry_run {
            "cold tier device register dry run succeeded".to_string()
        } else {
            "cold tier device registration requested; storage runtime owns local validation"
                .to_string()
        },
    })
}

pub fn unregister_cold_tier_device(
    &self,
    tenant: Option<&str>,
    device_id: &str,
    request: ColdTierUnregisterRequest,
) -> AdminResult<ColdTierUnregisterResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    let blockers = Self::cold_tier_blocker_summary(backend.as_ref(), device_id)?;
    let (blocked_objects, reclaimable_objects) =
        (blockers.blocked_objects, blockers.reclaimable_objects);
    if request.dry_run {
        return Ok(ColdTierUnregisterResponse {
            device_id: device.device_id.clone(),
            stable_id: device.stable_id.clone(),
            cold_tier_id: device.cold_tier_id.clone(),
            state: device.state,
            schedulable: device.schedulable(),
            dry_run: true,
            blocked_objects,
            inflight_operations: device.reserved_bytes,
            message: if blocked_objects == 0 {
                "cold tier device can be unregistered from Mooncake active registry".to_string()
            } else if request.force {
                "cold tier device can be force-unregistered after draining route-visible cold backing objects".to_string()
            } else {
                "cold tier device unregister requires force to drain route-visible cold backing objects".to_string()
            },
        });
    }
    if blocked_objects > 0 && !request.force {
        let mut update = ColdTierDeviceUpdate::new(now_ms());
        update.expected_updated_at_ms = Some(device.updated_at_ms);
        update.state = Some(ColdTierDeviceState::Draining);
        let next = backend.update_cold_tier_device(device_id, update)?;
        return Ok(ColdTierUnregisterResponse {
            device_id: next.device_id.clone(),
            stable_id: next.stable_id.clone(),
            cold_tier_id: next.cold_tier_id.clone(),
            state: next.state,
            schedulable: next.schedulable(),
            dry_run: false,
            blocked_objects,
            inflight_operations: next.reserved_bytes,
            message: format!(
                "cold tier device is draining; {blocked_objects} route-visible cold backing objects remain"
            ),
        });
    }
    if blocked_objects > 0 && reclaimable_objects != blocked_objects {
        return Err(StoreError::Conflict(format!(
            "cold tier device {device_id} has {blocked_objects} route-visible cold backing objects; {reclaimable_objects} are reclaimable"
        )));
    }
    if blocked_objects > 0 {
        Self::mark_cold_tier_backings_pending_delete(backend.as_ref(), device_id)?;
    }
    let latest = require_device(backend.as_ref(), device_id)?;
    if latest.reserved_bytes > 0 {
        return Err(StoreError::Conflict(format!(
            "cold tier device {device_id} has {} bytes reserved by inflight operations",
            latest.reserved_bytes
        )));
    }
    let mut update = ColdTierDeviceUpdate::new(now_ms());
    update.expected_updated_at_ms = Some(latest.updated_at_ms);
    update.state = Some(ColdTierDeviceState::Unregistered);
    update.epoch = Some(None);
    let next = backend.update_cold_tier_device(device_id, update)?;
    Ok(ColdTierUnregisterResponse {
        device_id: next.device_id.clone(),
        stable_id: next.stable_id.clone(),
        cold_tier_id: next.cold_tier_id.clone(),
        state: next.state,
        schedulable: next.schedulable(),
        dry_run: false,
        blocked_objects,
        inflight_operations: next.reserved_bytes,
        message: if blocked_objects == 0 {
            "cold tier device unregistered from Mooncake active registry".to_string()
        } else {
            format!(
                "cold tier device unregistered after marking {blocked_objects} cold backing objects pending delete"
            )
        },
    })
}

pub fn disable_cold_tier_device(
    &self,
    tenant: Option<&str>,
    device_id: &str,
    _request: ColdTierDisableRequest,
) -> AdminResult<ColdTierStateChangeResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    match device.state {
        ColdTierDeviceState::Healthy | ColdTierDeviceState::Full => {}
        ColdTierDeviceState::DisabledByAdmin => {
            return Ok(ColdTierStateChangeResponse {
                device_id: device.device_id.clone(),
                state: device.state,
                schedulable: device.schedulable(),
                message: "cold tier device already disabled".to_string(),
            });
        }
        other => {
            return Err(StoreError::InvalidState(format!(
                "cannot disable cold tier device in state {:?}",
                other
            )));
        }
    }
    let mut update = ColdTierDeviceUpdate::new(now_ms());
    update.expected_updated_at_ms = Some(device.updated_at_ms);
    update.state = Some(ColdTierDeviceState::DisabledByAdmin);
    let next = backend.update_cold_tier_device(device_id, update)?;
    Ok(ColdTierStateChangeResponse {
        device_id: next.device_id.clone(),
        state: next.state,
        schedulable: next.schedulable(),
        message: "cold tier device disabled for new offloads".to_string(),
    })
}

pub fn enable_cold_tier_device(
    &self,
    tenant: Option<&str>,
    device_id: &str,
    _request: ColdTierEnableRequest,
) -> AdminResult<ColdTierStateChangeResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    match device.state {
        ColdTierDeviceState::DisabledByAdmin | ColdTierDeviceState::Full => {}
        ColdTierDeviceState::Healthy => {
            return Ok(ColdTierStateChangeResponse {
                device_id: device.device_id.clone(),
                state: device.state,
                schedulable: device.schedulable(),
                message: "cold tier device already enabled".to_string(),
            });
        }
        other => {
            return Err(StoreError::InvalidState(format!(
                "cannot enable cold tier device in state {:?}",
                other
            )));
        }
    }
    let mut update = ColdTierDeviceUpdate::new(now_ms());
    update.expected_updated_at_ms = Some(device.updated_at_ms);
    update.state = Some(ColdTierDeviceState::Healthy);
    let next = backend.update_cold_tier_device(device_id, update)?;
    Ok(ColdTierStateChangeResponse {
        device_id: next.device_id.clone(),
        state: next.state,
        schedulable: next.schedulable(),
        message: "cold tier device enabled".to_string(),
    })
}

pub fn get_cold_tier_blockers(
    &self,
    tenant: Option<&str>,
    device_id: &str,
) -> AdminResult<ColdTierBlockersResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    let blockers = Self::cold_tier_blocker_summary(backend.as_ref(), device_id)?;
    Ok(ColdTierBlockersResponse {
        device_id: device.device_id.clone(),
        state: device.state,
        schedulable: device.schedulable(),
        blocked_objects: blockers.blocked_objects,
        reclaimable_objects: blockers.reclaimable_objects,
        pending_delete_objects: blockers.pending_delete_objects,
        inflight_operations: device.reserved_bytes,
    })
}

pub fn drain_cold_tier_device(
    &self,
    tenant: Option<&str>,
    device_id: &str,
    request: ColdTierDrainRequest,
) -> AdminResult<ColdTierDrainResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    let blockers = Self::cold_tier_blocker_summary(backend.as_ref(), device_id)?;
    if request.dry_run {
        return Ok(ColdTierDrainResponse {
            device_id: device.device_id.clone(),
            state: device.state,
            schedulable: device.schedulable(),
            dry_run: true,
            blocked_objects: blockers.blocked_objects,
            reclaimable_objects: blockers.reclaimable_objects,
            marked_pending_delete: 0,
            collected_pending_delete: 0,
            migration_tasks_submitted: 0,
            message: "cold tier drain dry run succeeded".to_string(),
        });
    }
    let migration_tasks =
        self.submit_cold_tier_drain_migrations(backend.as_ref(), device_id, &request)?;
    let blockers_after_migration =
        Self::cold_tier_blocker_summary(backend.as_ref(), device_id)?;
    let marked = if blockers_after_migration.blocked_objects == 0 {
        0
    } else if blockers_after_migration.reclaimable_objects
        == blockers_after_migration.blocked_objects
    {
        Self::mark_cold_tier_backings_pending_delete(backend.as_ref(), device_id)?
    } else if migration_tasks > 0 {
        0
    } else {
        return Err(StoreError::Conflict(format!(
            "cold tier device {device_id} has {} blocked objects; only {} are reclaimable",
            blockers_after_migration.blocked_objects,
            blockers_after_migration.reclaimable_objects
        )));
    };
    let collected_pending_delete =
        if blockers_after_migration.pending_delete_objects > 0 || marked > 0 {
            self.manual_gc_cold_tier_device(tenant, device_id)?
                .collected_objects
        } else {
            0
        };
    let final_blockers = Self::cold_tier_blocker_summary(backend.as_ref(), device_id)?;
    // Re-fetch device to get fresh updated_at_ms after intervening mutations.
    let device = require_device(backend.as_ref(), device_id)?;
    let mut update = ColdTierDeviceUpdate::new(now_ms());
    update.expected_updated_at_ms = Some(device.updated_at_ms);
    update.state = Some(ColdTierDeviceState::Draining);
    let next = backend.update_cold_tier_device(device_id, update)?;
    let message = if final_blockers.blocked_objects > 0 && migration_tasks > 0 {
        format!(
            "cold tier device drain requested; submitted {migration_tasks} cold-only migration tasks and waiting for route migration before reclaim"
        )
    } else if marked > 0 {
        format!(
            "cold tier device drain requested; marked {marked} reclaimable cold backing objects pending delete"
        )
    } else if migration_tasks > 0 {
        format!(
            "cold tier device drain requested; submitted {migration_tasks} cold-only migration tasks"
        )
    } else {
        "cold tier device drain requested".to_string()
    };
    Ok(ColdTierDrainResponse {
        device_id: next.device_id.clone(),
        state: next.state,
        schedulable: next.schedulable(),
        dry_run: false,
        blocked_objects: final_blockers.blocked_objects,
        reclaimable_objects: final_blockers.reclaimable_objects,
        marked_pending_delete: marked,
        collected_pending_delete,
        migration_tasks_submitted: migration_tasks,
        message,
    })
}

pub fn manual_gc_cold_tier_device(
    &self,
    tenant: Option<&str>,
    device_id: &str,
) -> AdminResult<ColdTierManualGcResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    let pending_delete_objects =
        Self::count_pending_delete_cold_tier_objects(backend.as_ref(), device_id)?;
    let lease = resolve_live_lease_from_backend(backend.as_ref(), &device.stable_id)?;
    let collected_objects = self.migrations.state.rpc.manual_cold_tier_gc(
        &lease,
        device_id,
        pending_delete_objects.max(1),
    )?;
    Ok(ColdTierManualGcResponse {
        device_id: device.device_id,
        pending_delete_objects,
        collected_objects,
        message: format!(
            "manual cold tier GC completed on storage runtime; collected {collected_objects} pending-delete objects"
        ),
    })
}

pub fn manual_free_cold_tier_device(
    &self,
    tenant: Option<&str>,
    device_id: &str,
) -> AdminResult<ColdTierManualFreeResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    let lease = resolve_live_lease_from_backend(backend.as_ref(), &device.stable_id)?;
    let free = self
        .migrations
        .state
        .rpc
        .manual_cold_tier_free(&lease, device_id, 64)?;
    Ok(ColdTierManualFreeResponse {
        device_id: device.device_id,
        inflight_operations: device.reserved_bytes,
        attempted_victims: free.attempted_victims,
        freed_backings: free.freed_backings,
        collected_backings: free.collected_backings,
        skipped_backings: free.skipped_backings,
        reached_low_watermark: free.reached_low_watermark,
        message: format!(
            "manual cold tier free completed on storage runtime; marked {} backings and collected {} pending-delete backings",
            free.freed_backings, free.collected_backings
        ),
    })
}

pub fn get_cold_tier_quarantine(
    &self,
    tenant: Option<&str>,
    device_id: &str,
) -> AdminResult<ColdTierQuarantineReport> {
    let backend = self.backend_for_tenant(tenant)?;
    let device = require_device(backend.as_ref(), device_id)?;
    let root_dir = cold_tier_root_dir(&device)?;
    let encoded_cold_tier_id = encode_cold_tier_path_component(&device.cold_tier_id);
    let mut files = scan_cold_tier_quarantine_dir(
        "final",
        &root_dir
            .join(&encoded_cold_tier_id)
            .join("__orphan_quarantine__"),
    )?;
    files.extend(scan_cold_tier_quarantine_dir(
        "pending",
        &root_dir
            .join("__pending__")
            .join(&encoded_cold_tier_id)
            .join("__orphan_quarantine__"),
    )?);
    files.sort_by(|left, right| {
        left.area
            .cmp(&right.area)
            .then_with(|| left.file_name.cmp(&right.file_name))
    });
    let total_bytes = files.iter().map(|file| file.size_bytes).sum();
    let root_dir = root_dir.to_string_lossy().into_owned();
    Ok(ColdTierQuarantineReport {
        device_id: device.device_id,
        cold_tier_id: device.cold_tier_id,
        root_dir,
        count: files.len(),
        total_bytes,
        files,
    })
}

pub fn get_cold_tier_object(
    &self,
    tenant: Option<&str>,
    key: &str,
) -> AdminResult<GetColdTierObjectResponse> {
    let backend = self.backend_for_tenant(tenant)?;
    let route = backend.get_object_route(&ObjectKey::new(key))?;
    let cold_backing = route
        .as_ref()
        .and_then(|route| self.debug_route_cold_backing(backend.as_ref(), route));
    Ok(GetColdTierObjectResponse {
        key: key.to_string(),
        cold_backing,
    })
}

fn debug_route_cold_backing(
    &self,
    backend: &dyn MetadataBackend,
    route: &ObjectRoute,
) -> Option<ColdTierObjectBackingResponse> {
    route
        .cold_backing
        .as_ref()
        .map(|backing| ColdTierObjectBackingResponse {
            device_id: backing.cold_tier_id.clone(),
            cold_tier_id: self.cold_tier_id_for_device(backend, &backing.cold_tier_id),
            owner: ColdTierObjectBackingOwnerResponse {
                stable_id: backing.owner.stable_id.to_string(),
                epoch: backing.owner.epoch.0,
            },
            state: format_cold_backing_state(backing.state).to_string(),
            locator: backing.object_locator.clone(),
            length: backing.length,
            checksum: backing.checksum,
        })
}

fn cold_tier_id_for_device(&self, backend: &dyn MetadataBackend, device_id: &str) -> String {
    backend
        .get_cold_tier_device(device_id)
        .ok()
        .flatten()
        .map(|device| device.cold_tier_id)
        .unwrap_or_else(|| device_id.to_string())
}

fn count_cold_tier_backing_objects(
    backend: &dyn MetadataBackend,
    device_id: &str,
) -> AdminResult<u64> {
    Ok(backend
        .list_object_routes_by_cold_backing(&mooncake_store_core::ColdBackingRouteFilter {
            device_id: Some(device_id.to_string()),
            ..mooncake_store_core::ColdBackingRouteFilter::default()
        })?
        .len() as u64)
}

fn cold_tier_blocker_summary(
    backend: &dyn MetadataBackend,
    device_id: &str,
) -> AdminResult<ColdTierBlockerSummary> {
    let mut summary = ColdTierBlockerSummary::default();
    for route in backend.list_object_routes_by_cold_backing(
        &mooncake_store_core::ColdBackingRouteFilter {
            device_id: Some(device_id.to_string()),
            ..mooncake_store_core::ColdBackingRouteFilter::default()
        },
    )? {
        let Some(backing) = route.cold_backing.as_ref() else {
            continue;
        };
        if backing.cold_tier_id != device_id {
            continue;
        }
        if backing.state == ColdBackingState::PendingDelete {
            summary.pending_delete_objects = summary.pending_delete_objects.saturating_add(1);
            continue;
        }
        summary.blocked_objects = summary.blocked_objects.saturating_add(1);
        if backing.state == ColdBackingState::Materialized && !route.replicas.is_empty() {
            summary.reclaimable_objects = summary.reclaimable_objects.saturating_add(1);
        }
    }
    Ok(summary)
}

fn count_pending_delete_cold_tier_objects(
    backend: &dyn MetadataBackend,
    device_id: &str,
) -> AdminResult<u64> {
    Ok(Self::cold_tier_blocker_summary(backend, device_id)?.pending_delete_objects)
}

fn submit_cold_tier_drain_migrations(
    &self,
    backend: &dyn MetadataBackend,
    device_id: &str,
    request: &ColdTierDrainRequest,
) -> AdminResult<u64> {
    let Some(task_executor) = request.migration_task_executor.as_ref() else {
        return Ok(0);
    };
    if task_executor.trim().is_empty() || request.migration_target_segments.is_empty() {
        return Ok(0);
    }
    let mut submitted = 0u64;
    for route in backend.list_object_routes_by_cold_backing(
        &mooncake_store_core::ColdBackingRouteFilter {
            device_id: Some(device_id.to_string()),
            ..mooncake_store_core::ColdBackingRouteFilter::default()
        },
    )? {
        let Some(backing) = route.cold_backing.as_ref() else {
            continue;
        };
        if backing.cold_tier_id != device_id
            || backing.state != ColdBackingState::Materialized
            || !route.replicas.is_empty()
        {
            continue;
        }
        let source_segment =
            mooncake_store_core::COLD_TIER_MIGRATION_SOURCE_SEGMENT.to_string();
        let namespace = route.namespace.clone().unwrap_or_default();
        self.submit_route_migration_task(
            RouteMigrationMode::Move,
            RouteMigrationTaskSubmitRequest {
                authority: route
                    .sharing_scope
                    .clone()
                    .unwrap_or_else(|| namespace.tenant.clone()),
                tenant: namespace.tenant,
                domain: Some(namespace.domain),
                object_set: Some(namespace.object_set),
                key: route
                    .logical_key
                    .clone()
                    .unwrap_or_else(|| route.key.0.clone()),
                source_segment,
                target_segments: request.migration_target_segments.clone(),
                task_executor: task_executor.clone(),
                max_retries: request.migration_max_retries,
            },
        )?;
        submitted = submitted.saturating_add(1);
    }
    Ok(submitted)
}

fn mark_cold_tier_backings_pending_delete(
    backend: &dyn MetadataBackend,
    device_id: &str,
) -> AdminResult<u64> {
    let mut marked = 0u64;
    for route in backend.list_object_routes_by_cold_backing(
        &mooncake_store_core::ColdBackingRouteFilter {
            device_id: Some(device_id.to_string()),
            ..mooncake_store_core::ColdBackingRouteFilter::default()
        },
    )? {
        let Some(backing) = route.cold_backing.as_ref() else {
            continue;
        };
        if backing.cold_tier_id != device_id || backing.state == ColdBackingState::PendingDelete
        {
            continue;
        }
        if backing.state != ColdBackingState::Materialized || route.replicas.is_empty() {
            return Err(StoreError::Conflict(format!(
                "cold tier device {device_id} has non-reclaimable backing for route {}",
                route.key.0
            )));
        }
        let mut next = route.clone();
        next.version = next.version.next();
        if let Some(next_backing) = next.cold_backing.as_mut() {
            next_backing.state = ColdBackingState::PendingDelete;
        }
        let cas = backend.compare_and_swap_object_route(
            &route.key,
            Some(route.version),
            Some(&next),
        )?;
        if !cas.applied {
            return Err(StoreError::Conflict(format!(
                "route {} changed while draining cold tier device {device_id}",
                route.key.0
            )));
        }
        marked = marked.saturating_add(1);
    }
    Ok(marked)
}

} // impl AdminService
