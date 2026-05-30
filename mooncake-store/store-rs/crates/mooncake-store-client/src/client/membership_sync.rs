struct MembershipSyncHandle {
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl MembershipSyncHandle {
    fn disabled() -> Self {
        Self {
            shutdown: None,
            thread: None,
        }
    }

    fn spawn(
        runtime: &ClientRuntimeId,
        metadata: Arc<dyn MetadataBackend>,
        live_client_cache: SharedLiveClientCache,
        interval: Duration,
    ) -> Result<Self> {
        if interval.is_zero() {
            return Ok(Self::disabled());
        }
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let stable_id = runtime.stable_id.0.clone();
        let initial_delay = membership_initial_refresh_delay(runtime, interval);
        let thread = std::thread::Builder::new()
            .name(format!("mooncake-membership-sync-{stable_id}"))
            .spawn(move || {
                let mut wait = initial_delay;
                loop {
                    match shutdown_rx.recv_timeout(wait) {
                        Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if let Err(error) = refresh_live_client_cache(
                                metadata.as_ref(),
                                &live_client_cache,
                                "live_client_snapshot_refresh",
                            ) {
                                tracing::warn!(
                                    error = %error,
                                    "background live-client refresh failed"
                                );
                            }
                            if let Err(error) = refresh_due_tenant_quota_policy_cache(
                                metadata.as_ref(),
                                &live_client_cache,
                            ) {
                                tracing::warn!(
                                    error = %error,
                                    "background tenant policy cache refresh failed"
                                );
                            }
                            wait = interval;
                        }
                    }
                }
            })
            .map_err(|error| {
                StoreError::Transport(format!("failed to spawn membership sync worker: {error}"))
            })?;
        Ok(Self {
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for MembershipSyncHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct AsyncEvictionHandle {
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AsyncEvictionHandle {
    fn disabled() -> Self {
        Self {
            shutdown: None,
            thread: None,
        }
    }

    fn spawn(
        runtime: &ClientRuntimeId,
        lease: &ClientLease,
        local_memory: &LocalMemoryConfig,
        storage_owner: Arc<StorageOwnerState>,
    ) -> Result<Self> {
        if local_memory.eviction_poll_interval.is_zero()
            || !local_memory.has_storage()
            || lease
                .endpoints
                .labels
                .get("storage")
                .is_none_or(|value| value != "true")
        {
            return Ok(Self::disabled());
        }
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let stable_id = runtime.stable_id.0.clone();
        let interval = local_memory.eviction_poll_interval;
        let high_percent = local_memory.eviction_high_watermark_percent;
        let low_percent = local_memory.eviction_low_watermark_percent;
        let thread = std::thread::Builder::new()
            .name(format!("mooncake-async-evict-{stable_id}"))
            .spawn(move || {
                while shutdown_rx.recv_timeout(interval).is_err() {
                    if let Err(error) =
                        storage_owner.evict_until_low_watermark(high_percent, low_percent)
                    {
                        tracing::warn!(error = %error, "background storage-owner eviction failed");
                    }
                }
            })
            .map_err(|error| {
                StoreError::Transport(format!("failed to spawn async eviction worker: {error}"))
            })?;
        Ok(Self {
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for AsyncEvictionHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

const ASYNC_REPLICA_TRACK_QUEUE_CAPACITY: usize = 1024;
const ASYNC_REPLICA_TRACK_DRAIN_BUDGET: usize = 4096;
const ASYNC_REPLICA_TRACK_RECV_TIMEOUT: Duration = Duration::from_millis(100);
const ASYNC_REPLICA_TRACK_COALESCE_WINDOW: Duration = Duration::from_millis(2);
const ASYNC_ROUTE_HIT_QUEUE_CAPACITY: usize = 4096;
const ASYNC_ROUTE_HIT_DRAIN_BUDGET: usize = 8192;
const ASYNC_ROUTE_HIT_RECV_TIMEOUT: Duration = Duration::from_millis(100);
const ASYNC_ROUTE_HIT_COALESCE_WINDOW: Duration = Duration::from_millis(1);

struct AsyncReplicaTrackHandle {
    sender: Option<std::sync::mpsc::SyncSender<Vec<ObjectRoute>>>,
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AsyncReplicaTrackHandle {
    fn spawn(
        runtime: &ClientRuntimeId,
        metadata: Arc<dyn MetadataBackend>,
        control_client: Arc<ControlPlaneClient>,
        live_client_cache: SharedLiveClientCache,
    ) -> Result<Self> {
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<Vec<ObjectRoute>>(ASYNC_REPLICA_TRACK_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let local_runtime = runtime.clone();
        let stable_id = runtime.stable_id.0.clone();
        let thread = std::thread::Builder::new()
            .name(format!("mooncake-replica-track-{stable_id}"))
            .spawn(move || {
                loop {
                    if shutdown_rx.try_recv().is_ok() {
                        break;
                    }
                    let mut routes = match receiver.recv_timeout(ASYNC_REPLICA_TRACK_RECV_TIMEOUT)
                    {
                        Ok(routes) => routes,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    };
                    let started = Instant::now();
                    while routes.len() < ASYNC_REPLICA_TRACK_DRAIN_BUDGET
                        && started.elapsed() < ASYNC_REPLICA_TRACK_COALESCE_WINDOW
                    {
                        match receiver.try_recv() {
                            Ok(mut more) => routes.append(&mut more),
                            Err(std::sync::mpsc::TryRecvError::Empty) => break,
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                        }
                    }
                    flush_async_replica_tracks(
                        &local_runtime,
                        metadata.as_ref(),
                        control_client.as_ref(),
                        &live_client_cache,
                        &routes,
                    );
                }
            })
            .map_err(|error| {
                StoreError::Transport(format!(
                    "failed to spawn async replica tracking worker: {error}"
                ))
            })?;
        Ok(Self {
            sender: Some(sender),
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    fn enqueue(&self, routes: &[ObjectRoute]) {
        if routes.is_empty() {
            return;
        }
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        match sender.try_send(routes.to_vec()) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full(routes)) => {
                tracing::debug!(
                    items = routes.len(),
                    "dropping best-effort replica tracking after async queue filled"
                );
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {}
        }
    }

    fn shutdown(&mut self) {
        self.sender.take();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for AsyncReplicaTrackHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct RouteHitReportBatch {
    remote_hits: BTreeMap<ClientRuntimeId, BTreeSet<ObjectKey>>,
    force_membership_refresh_on_miss: bool,
}

struct AsyncRouteHitReportHandle {
    sender: Option<std::sync::mpsc::SyncSender<RouteHitReportBatch>>,
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AsyncRouteHitReportHandle {
    fn spawn(
        runtime: &ClientRuntimeId,
        metadata: Arc<dyn MetadataBackend>,
        control_client: Arc<ControlPlaneClient>,
        live_client_cache: SharedLiveClientCache,
    ) -> Result<Self> {
        let (sender, receiver) =
            std::sync::mpsc::sync_channel::<RouteHitReportBatch>(ASYNC_ROUTE_HIT_QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let stable_id = runtime.stable_id.0.clone();
        let thread = std::thread::Builder::new()
            .name(format!("mooncake-route-hit-report-{stable_id}"))
            .spawn(move || {
                loop {
                    if shutdown_rx.try_recv().is_ok() {
                        break;
                    }
                    let mut batch = match receiver.recv_timeout(ASYNC_ROUTE_HIT_RECV_TIMEOUT) {
                        Ok(batch) => batch,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    };
                    let started = Instant::now();
                    let mut item_count = route_hit_report_item_count(&batch.remote_hits);
                    while item_count < ASYNC_ROUTE_HIT_DRAIN_BUDGET
                        && started.elapsed() < ASYNC_ROUTE_HIT_COALESCE_WINDOW
                    {
                        match receiver.try_recv() {
                            Ok(more) => {
                                item_count = item_count.saturating_add(route_hit_report_item_count(
                                    &more.remote_hits,
                                ));
                                batch.force_membership_refresh_on_miss |=
                                    more.force_membership_refresh_on_miss;
                                merge_route_hit_reports(&mut batch.remote_hits, more.remote_hits);
                            }
                            Err(std::sync::mpsc::TryRecvError::Empty) => break,
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                        }
                    }
                    flush_async_route_hit_reports(
                        metadata.as_ref(),
                        control_client.as_ref(),
                        &live_client_cache,
                        batch.remote_hits,
                        batch.force_membership_refresh_on_miss,
                    );
                }
            })
            .map_err(|error| {
                StoreError::Transport(format!(
                    "failed to spawn async route hit reporting worker: {error}"
                ))
            })?;
        Ok(Self {
            sender: Some(sender),
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    fn enqueue(
        &self,
        remote_hits: BTreeMap<ClientRuntimeId, BTreeSet<ObjectKey>>,
        force_membership_refresh_on_miss: bool,
    ) {
        if remote_hits.is_empty() {
            return;
        }
        let Some(sender) = self.sender.as_ref() else {
            return;
        };
        let item_count = route_hit_report_item_count(&remote_hits);
        match sender.try_send(RouteHitReportBatch {
            remote_hits,
            force_membership_refresh_on_miss,
        }) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full(_)) => {
                tracing::debug!(
                    items = item_count,
                    "dropping best-effort route hit reporting after async queue filled"
                );
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {}
        }
    }

    fn shutdown(&mut self) {
        self.sender.take();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for AsyncRouteHitReportHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn route_hit_report_item_count(remote_hits: &BTreeMap<ClientRuntimeId, BTreeSet<ObjectKey>>) -> usize {
    remote_hits.values().map(BTreeSet::len).sum()
}

fn merge_route_hit_reports(
    target: &mut BTreeMap<ClientRuntimeId, BTreeSet<ObjectKey>>,
    more: BTreeMap<ClientRuntimeId, BTreeSet<ObjectKey>>,
) {
    for (owner, keys) in more {
        target.entry(owner).or_default().extend(keys);
    }
}

fn flush_async_route_hit_reports(
    metadata: &dyn MetadataBackend,
    control_client: &ControlPlaneClient,
    live_client_cache: &SharedLiveClientCache,
    remote_hits: BTreeMap<ClientRuntimeId, BTreeSet<ObjectKey>>,
    force_membership_refresh_on_miss: bool,
) {
    if remote_hits.is_empty() {
        return;
    }
    let wanted = remote_hits.keys().cloned().collect::<BTreeSet<_>>();
    let leases = match route_hit_report_leases(
        metadata,
        live_client_cache,
        &wanted,
        force_membership_refresh_on_miss,
    ) {
        Ok(leases) => leases,
        Err(error) => {
            tracing::debug!(
                error = %error,
                targets = wanted.len(),
                "failed to resolve storage-owner leases for async hit reporting"
            );
            return;
        }
    };
    for (owner, keys) in remote_hits {
        let Some(lease) = leases.get(&owner) else {
            continue;
        };
        let keys = keys.into_iter().collect::<Vec<_>>();
        if let Err(error) = control_client.batch_report_route_hits(lease, &keys) {
            tracing::debug!(
                storage_owner = %owner,
                error = %error,
                items = keys.len(),
                "async storage-owner hit report failed"
            );
        }
    }
}

fn route_hit_report_leases(
    metadata: &dyn MetadataBackend,
    live_client_cache: &SharedLiveClientCache,
    wanted: &BTreeSet<ClientRuntimeId>,
    force_membership_refresh_on_miss: bool,
) -> Result<BTreeMap<ClientRuntimeId, ClientLease>> {
    let mut leases = live_client_cache
        .lock()
        .snapshot()
        .unwrap_or_default()
        .into_iter()
        .filter(|lease| wanted.contains(&lease.runtime))
        .map(|lease| (lease.runtime.clone(), lease))
        .collect::<BTreeMap<_, _>>();
    let missing = wanted
        .iter()
        .any(|runtime| !leases.contains_key(runtime));
    if missing && force_membership_refresh_on_miss {
        leases = refresh_live_client_cache(
            metadata,
            live_client_cache,
            "live_client_snapshot_hit_report",
        )?
        .into_iter()
        .filter(|lease| wanted.contains(&lease.runtime))
        .map(|lease| (lease.runtime.clone(), lease))
        .collect();
    }
    Ok(leases)
}

fn flush_async_replica_tracks(
    local_runtime: &ClientRuntimeId,
    metadata: &dyn MetadataBackend,
    control_client: &ControlPlaneClient,
    live_client_cache: &SharedLiveClientCache,
    routes: &[ObjectRoute],
) {
    let mut grouped = BTreeMap::<ClientRuntimeId, BTreeMap<String, ObjectRoute>>::new();
    for route in routes {
        let mut owners = BTreeSet::new();
        for replica in route
            .replicas
            .iter()
            .filter(|replica| replica.owner != *local_runtime)
        {
            if owners.insert(replica.owner.clone()) {
                grouped
                    .entry(replica.owner.clone())
                    .or_default()
                    .insert(route.key.0.clone(), route.clone());
            }
        }
    }
    if grouped.is_empty() {
        return;
    }
    let wanted = grouped.keys().cloned().collect::<BTreeSet<_>>();
    let leases = match replica_track_leases(metadata, live_client_cache, &wanted) {
        Ok(leases) => leases,
        Err(error) => {
            tracing::debug!(
                error = %error,
                targets = wanted.len(),
                "failed to resolve storage-owner leases for async replica tracking"
            );
            return;
        }
    };
    for (owner, routes) in grouped {
        let Some(lease) = leases.get(&owner) else {
            tracing::debug!(
                storage_owner = %owner,
                items = routes.len(),
                "dropping best-effort replica tracking for unavailable owner"
            );
            continue;
        };
        let routes = routes.into_values().collect::<Vec<_>>();
        if let Err(error) = control_client.batch_track_replica_routes(lease, &routes) {
            tracing::debug!(
                storage_owner = %owner,
                error = %error,
                items = routes.len(),
                "async storage-owner route tracking failed"
            );
        }
    }
}

fn replica_track_leases(
    metadata: &dyn MetadataBackend,
    live_client_cache: &SharedLiveClientCache,
    wanted: &BTreeSet<ClientRuntimeId>,
) -> Result<BTreeMap<ClientRuntimeId, ClientLease>> {
    let mut leases = live_client_cache
        .lock()
        .snapshot()
        .unwrap_or_default()
        .into_iter()
        .filter(|lease| wanted.contains(&lease.runtime))
        .map(|lease| (lease.runtime.clone(), lease))
        .collect::<BTreeMap<_, _>>();
    if wanted.iter().any(|runtime| !leases.contains_key(runtime)) {
        leases = refresh_live_client_cache(
            metadata,
            live_client_cache,
            "live_client_snapshot_replica_track",
        )?
        .into_iter()
        .filter(|lease| wanted.contains(&lease.runtime))
        .map(|lease| (lease.runtime.clone(), lease))
        .collect();
    }
    Ok(leases)
}

pub(crate) fn refresh_live_client_cache(
    metadata: &dyn MetadataBackend,
    live_client_cache: &SharedLiveClientCache,
    operation: &'static str,
) -> Result<Vec<ClientLease>> {
    let tracker = OperationTracker::new(operation);
    let started = Instant::now();
    let result = metadata.list_live_clients();
    tracker.finish(&result, 0);
    registry::record_membership_refresh(
        if result.is_ok() { "ok" } else { "error" },
        started.elapsed(),
    );
    if let Ok(leases) = &result {
        let live_leases = filter_live_client_leases(leases);
        registry::record_runtime_leases(&live_leases);
        live_client_cache.lock().store(live_leases.clone());
        return Ok(live_leases);
    }
    result
}

pub(crate) fn refresh_due_tenant_quota_policy_cache(
    metadata: &dyn MetadataBackend,
    live_client_cache: &SharedLiveClientCache,
) -> Result<()> {
    let now = current_time_ms();
    let tenants = live_client_cache.lock().due_tenant_quota_policy_refreshes(
        now,
        DEFAULT_TENANT_POLICY_CACHE_TTL_MS,
        DEFAULT_TENANT_POLICY_CACHE_IDLE_TTL_MS,
    );
    if tenants.is_empty() {
        return Ok(());
    }
    let scopes = tenants
        .iter()
        .map(|tenant| TenantPolicyScope::new(tenant.as_str(), None::<String>, None::<String>))
        .collect::<Vec<_>>();
    let tracker = OperationTracker::new("tenant_policy_cache_refresh");
    let result = metadata.get_tenant_policies(&scopes);
    tracker.finish(&result, 0);
    let policies = result?;
    let mut cache = live_client_cache.lock();
    for (tenant, policy) in tenants.into_iter().zip(policies) {
        let version = policy.as_ref().map(|policy| policy.version);
        let quota = policy.and_then(|policy| policy.spec.quota);
        cache.store_tenant_quota_policy(tenant, version, quota, now);
    }
    Ok(())
}

fn membership_initial_refresh_delay(runtime: &ClientRuntimeId, interval: Duration) -> Duration {
    let interval_ms = interval.as_millis().min(u128::from(u64::MAX)) as u64;
    Duration::from_millis(stable_phase_spread_ms(
        &runtime.to_string(),
        interval_ms,
        "membership_refresh",
    ))
}

#[cfg(test)]
mod membership_sync_tests {
    use super::*;

    #[test]
    fn membership_initial_refresh_delay_is_stably_spread() {
        let runtime = ClientRuntimeId::new("membership-a", ClientEpoch(7));
        let delay = membership_initial_refresh_delay(&runtime, Duration::from_millis(3_000));
        assert!((1..=3_000).contains(&delay.as_millis()));
        assert_eq!(
            delay,
            membership_initial_refresh_delay(&runtime, Duration::from_millis(3_000))
        );
        assert_eq!(
            membership_initial_refresh_delay(&runtime, Duration::ZERO),
            Duration::ZERO
        );
    }
}
