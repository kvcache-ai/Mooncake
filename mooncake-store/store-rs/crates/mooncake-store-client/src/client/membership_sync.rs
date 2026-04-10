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
        let thread = std::thread::Builder::new()
            .name(format!("mooncake-membership-sync-{stable_id}"))
            .spawn(move || {
                while shutdown_rx.recv_timeout(interval).is_err() {
                    if let Err(error) = refresh_live_client_cache(
                        metadata.as_ref(),
                        &live_client_cache,
                        "live_client_snapshot_refresh",
                    ) {
                        tracing::warn!(error = %error, "background live-client refresh failed");
                    }
                }
            })
            .map_err(|error| {
                StoreError::Transport(format!(
                    "failed to spawn membership sync worker: {error}"
                ))
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

pub(crate) fn refresh_live_client_cache(
    metadata: &dyn MetadataBackend,
    live_client_cache: &SharedLiveClientCache,
    operation: &'static str,
) -> Result<Vec<ClientLease>> {
    let tracker = OperationTracker::new(operation);
    let result = metadata.list_live_clients();
    tracker.finish(&result, 0);
    if let Ok(leases) = &result {
        live_client_cache.lock().store(leases.clone());
    }
    result
}
