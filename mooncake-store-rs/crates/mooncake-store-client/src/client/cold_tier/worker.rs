use super::super::{
    ClientLease, ClientRuntimeId, LocalMemoryConfig, Result, SharedRestorePromotionQueue,
    StorageOwnerState, StoreError, DEFAULT_OFFLOAD_POLL_INTERVAL,
};
use std::sync::Arc;

pub(in super::super) struct AsyncOffloadHandle {
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

#[allow(dead_code)]
impl AsyncOffloadHandle {
    pub(in super::super) fn disabled() -> Self {
        Self {
            shutdown: None,
            thread: None,
        }
    }

    pub(in super::super) fn spawn(
        runtime: &ClientRuntimeId,
        lease: &ClientLease,
        local_memory: &LocalMemoryConfig,
        storage_owner: Arc<StorageOwnerState>,
    ) -> Result<Self> {
        if !local_memory.has_storage()
            || lease
                .endpoints
                .labels
                .get("storage")
                .map(|value| value != "true")
                .unwrap_or(true)
        {
            return Ok(Self::disabled());
        }
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let stable_id = runtime.stable_id.0.clone();
        let interval = DEFAULT_OFFLOAD_POLL_INTERVAL;
        let thread = std::thread::Builder::new()
            .name(format!("mooncake-async-offload-{stable_id}"))
            .spawn(move || {
                while shutdown_rx.recv_timeout(interval).is_err() {
                    crate::client::cold_tier::ColdTierHandle::background_tick(
                        storage_owner.as_ref(),
                    );
                }
            })
            .map_err(|error| {
                StoreError::Transport(format!("failed to spawn async offload worker: {error}"))
            })?;
        Ok(Self {
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        })
    }

    pub(in super::super) fn shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for AsyncOffloadHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(in super::super) struct AsyncRestorePromotionHandle {
    queue: SharedRestorePromotionQueue,
}

impl AsyncRestorePromotionHandle {
    pub(in super::super) fn spawn(
        _runtime: &ClientRuntimeId,
        queue: SharedRestorePromotionQueue,
    ) -> Self {
        Self { queue }
    }

    pub(in super::super) fn shutdown(&mut self) {
        self.queue.shutdown();
    }
}

impl Drop for AsyncRestorePromotionHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
