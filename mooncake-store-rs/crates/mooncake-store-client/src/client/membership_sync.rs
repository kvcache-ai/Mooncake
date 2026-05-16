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
    for (tenant, policy) in tenants.into_iter().zip(policies.into_iter()) {
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
