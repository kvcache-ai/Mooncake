use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mooncake_store_core::{ClientLease, ClientStableId, RouteCasRequest, StoreError};
use parking_lot::Mutex;
use tracing::warn;

use crate::mesh::registry::{authority_is_local, authority_replace_many, local_authority_service};
use crate::shim::{RouteAuthorityClient, RouteMembershipProvider};

const QUEUE_CAPACITY: usize = 1024;
const DRAIN_BUDGET: usize = 4096;
const RECV_TIMEOUT: Duration = Duration::from_millis(100);
const COALESCE_WINDOW: Duration = Duration::from_millis(2);

const SUSPECT_AUTHORITY_TTL: Duration = Duration::from_secs(5);

pub(crate) struct RouteMirrorBatch {
    pub(crate) secondary: ClientLease,
    pub(crate) requests: Vec<RouteCasRequest>,
}

pub(crate) struct AsyncRouteMirrorWorker {
    state: Mutex<AsyncRouteMirrorWorkerState>,
}

struct AsyncRouteMirrorWorkerState {
    sender: Option<std::sync::mpsc::SyncSender<RouteMirrorBatch>>,
    shutdown: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AsyncRouteMirrorWorker {
    pub(crate) fn disabled() -> Self {
        Self {
            state: Mutex::new(AsyncRouteMirrorWorkerState {
                sender: None,
                shutdown: None,
                thread: None,
            }),
        }
    }

    pub(crate) fn spawn(
        namespace: String,
        local_stable_id: ClientStableId,
        authority_client: Arc<dyn RouteAuthorityClient>,
        membership: Arc<dyn RouteMembershipProvider>,
    ) -> Self {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<RouteMirrorBatch>(QUEUE_CAPACITY);
        let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
        let thread = match std::thread::Builder::new()
            .name(format!("mooncake-route-mirror-{}", local_stable_id.0))
            .spawn(move || loop {
                if shutdown_rx.try_recv().is_ok() {
                    break;
                }
                let first = match receiver.recv_timeout(RECV_TIMEOUT) {
                    Ok(batch) => batch,
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                };
                let mut groups = BTreeMap::<String, (ClientLease, Vec<RouteCasRequest>)>::new();
                let mut total_items = 0usize;
                Self::push_batch(&mut groups, first, &mut total_items);
                let started = Instant::now();
                while total_items < DRAIN_BUDGET && started.elapsed() < COALESCE_WINDOW {
                    match receiver.try_recv() {
                        Ok(batch) => Self::push_batch(&mut groups, batch, &mut total_items),
                        Err(std::sync::mpsc::TryRecvError::Empty) => break,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                    }
                }
                for (_, (secondary, requests)) in groups {
                    mirror_secondary_batch(
                        &namespace,
                        authority_client.as_ref(),
                        membership.as_ref(),
                        &secondary,
                        &requests,
                    );
                }
            }) {
            Ok(thread) => thread,
            Err(error) => {
                warn!(error = %error, "failed to spawn route mirror worker");
                return Self::disabled();
            }
        };
        Self {
            state: Mutex::new(AsyncRouteMirrorWorkerState {
                sender: Some(sender),
                shutdown: Some(shutdown_tx),
                thread: Some(thread),
            }),
        }
    }

    fn push_batch(
        groups: &mut BTreeMap<String, (ClientLease, Vec<RouteCasRequest>)>,
        batch: RouteMirrorBatch,
        total_items: &mut usize,
    ) {
        *total_items = total_items.saturating_add(batch.requests.len());
        groups
            .entry(batch.secondary.runtime.stable_id.0.clone())
            .or_insert_with(|| (batch.secondary, Vec::new()))
            .1
            .extend(batch.requests);
    }

    pub(crate) fn enqueue(&self, secondary: ClientLease, requests: Vec<RouteCasRequest>) {
        if requests.is_empty() {
            return;
        }
        let state = self.state.lock();
        let Some(sender) = state.sender.as_ref() else {
            return;
        };
        match sender.try_send(RouteMirrorBatch {
            secondary,
            requests,
        }) {
            Ok(()) => {}
            Err(std::sync::mpsc::TrySendError::Full(batch)) => {
                warn!(
                    authority = %batch.secondary.runtime,
                    items = batch.requests.len(),
                    "dropping best-effort secondary route mirror after async queue filled"
                );
            }
            Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {}
        }
    }

    pub(crate) fn shutdown(&mut self) {
        let (shutdown, thread) = {
            let mut state = self.state.lock();
            state.sender.take();
            (state.shutdown.take(), state.thread.take())
        };
        if let Some(shutdown) = shutdown {
            let _ = shutdown.send(());
        }
        if let Some(thread) = thread {
            let _ = thread.join();
        }
    }
}

impl Drop for AsyncRouteMirrorWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(crate) fn maybe_mark_authority_suspect(
    namespace: &str,
    membership: &dyn RouteMembershipProvider,
    authority: &ClientLease,
    error: &StoreError,
    context: &'static str,
) {
    if authority_is_local(namespace, &authority.runtime.stable_id)
        || !matches!(
            error,
            StoreError::Transport(_)
                | StoreError::NotFound(_)
                | StoreError::InvalidState(_)
                | StoreError::Unsupported(_)
        )
    {
        return;
    }
    membership.mark_suspect(
        authority.runtime.clone(),
        Instant::now() + SUSPECT_AUTHORITY_TTL,
        Some(authority),
    );
    warn!(
        runtime = %authority.runtime,
        context,
        quarantine_ms = SUSPECT_AUTHORITY_TTL.as_millis() as u64,
        "marked route authority as suspect after request failure"
    );
}

pub(crate) fn mirror_secondary_batch(
    namespace: &str,
    authority_client: &dyn RouteAuthorityClient,
    membership: &dyn RouteMembershipProvider,
    secondary: &ClientLease,
    requests: &[RouteCasRequest],
) {
    let result = if authority_is_local(namespace, &secondary.runtime.stable_id) {
        if let Some(service) = local_authority_service(namespace, &secondary.runtime.stable_id) {
            Ok(service.batch_replace_routes(namespace, &secondary.runtime.stable_id, requests))
        } else {
            authority_replace_many(namespace, &secondary.runtime.stable_id, requests)
                .map(|_| requests.iter().map(|_| Ok(())).collect())
        }
    } else {
        authority_client.batch_replace_routes(
            secondary,
            namespace,
            &secondary.runtime.stable_id,
            requests,
        )
    };
    match result {
        Ok(results) => {
            for (request, result) in requests.iter().zip(results) {
                if let Err(error) = result {
                    maybe_mark_authority_suspect(
                        namespace,
                        membership,
                        secondary,
                        &error,
                        "route_secondary_mirror_failed",
                    );
                    warn!(
                        authority = %secondary.runtime,
                        key = %request.key.0,
                        error = %error,
                        "secondary route mirror failed"
                    );
                }
            }
        }
        Err(error) => {
            maybe_mark_authority_suspect(
                namespace,
                membership,
                secondary,
                &error,
                "route_secondary_mirror_batch_failed",
            );
            warn!(
                authority = %secondary.runtime,
                error = %error,
                items = requests.len(),
                "secondary route mirror batch failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_core::{CasResult, ClientRuntimeId, ObjectKey, ObjectRoute, Result};

    struct RecordingAuthorityClient {
        calls: Mutex<Vec<(String, Vec<RouteCasRequest>)>>,
    }

    impl RecordingAuthorityClient {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
            })
        }

        fn take_calls(&self) -> Vec<(String, Vec<RouteCasRequest>)> {
            std::mem::take(&mut *self.calls.lock())
        }
    }

    impl RouteAuthorityClient for RecordingAuthorityClient {
        fn batch_get_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<Option<ObjectRoute>>>> {
            Ok(keys.iter().map(|_| Ok(None)).collect())
        }

        fn batch_contains_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<bool>>> {
            Ok(keys.iter().map(|_| Ok(false)).collect())
        }

        fn batch_compare_and_swap_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<CasResult>>> {
            Ok(Vec::new())
        }

        fn batch_replace_routes(
            &self,
            lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            requests: &[RouteCasRequest],
        ) -> Result<Vec<Result<()>>> {
            self.calls
                .lock()
                .push((lease.runtime.stable_id.0.clone(), requests.to_vec()));
            Ok(requests.iter().map(|_| Ok(())).collect())
        }

        fn list_routes_by_replica_owner(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _owner: &ClientRuntimeId,
        ) -> Result<Vec<ObjectRoute>> {
            Ok(Vec::new())
        }
    }

    struct NoopMembership;

    impl RouteMembershipProvider for NoopMembership {
        fn live_clients(
            &self,
            _force_refresh: bool,
            _operation: &'static str,
        ) -> Result<Vec<ClientLease>> {
            Ok(Vec::new())
        }

        fn reconcile_suspects(&self, _leases: &[ClientLease]) {}

        fn is_suspect(&self, _runtime: &ClientRuntimeId) -> bool {
            false
        }

        fn mark_suspect(
            &self,
            _runtime: ClientRuntimeId,
            _quarantine_until: Instant,
            _observed: Option<&ClientLease>,
        ) {
        }
    }

    fn test_lease(id: &str) -> ClientLease {
        use mooncake_store_core::{
            ClientEndpointSet, ClientEpoch, ClientLifecycleState, CompatibilityDescriptor,
        };
        ClientLease {
            runtime: ClientRuntimeId::new(id, ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            expires_at_ms: u64::MAX,
            compatibility: CompatibilityDescriptor::mooncake_v1(),
            endpoints: ClientEndpointSet::default(),
        }
    }

    fn test_cas_request(key: &str) -> RouteCasRequest {
        RouteCasRequest {
            key: ObjectKey::new(key.to_string()),
            expected: None,
            next: None,
        }
    }

    #[test]
    fn disabled_worker_silently_drops() {
        let worker = AsyncRouteMirrorWorker::disabled();
        worker.enqueue(test_lease("secondary-a"), vec![test_cas_request("key-1")]);
    }

    #[test]
    fn enqueue_drops_when_queue_full() {
        let client = RecordingAuthorityClient::new();
        let membership = Arc::new(NoopMembership);
        let worker = AsyncRouteMirrorWorker::spawn(
            "test-ns".to_string(),
            ClientStableId::new("local".to_string()),
            client.clone(),
            membership,
        );
        for i in 0..QUEUE_CAPACITY + 10 {
            worker.enqueue(
                test_lease("secondary"),
                vec![test_cas_request(&format!("key-{i}"))],
            );
        }
        drop(worker);
    }

    #[test]
    fn shutdown_joins_worker_thread() {
        let client = RecordingAuthorityClient::new();
        let membership = Arc::new(NoopMembership);
        let mut worker = AsyncRouteMirrorWorker::spawn(
            "test-ns".to_string(),
            ClientStableId::new("local".to_string()),
            client,
            membership,
        );
        worker.enqueue(test_lease("secondary"), vec![test_cas_request("key-1")]);
        worker.shutdown();
    }

    #[test]
    fn enqueue_coalesces_same_authority_batches() {
        let client = RecordingAuthorityClient::new();
        let membership = Arc::new(NoopMembership);
        let worker = AsyncRouteMirrorWorker::spawn(
            "test-ns".to_string(),
            ClientStableId::new("local".to_string()),
            client.clone(),
            membership,
        );
        let secondary = test_lease("secondary-x");
        worker.enqueue(secondary.clone(), vec![test_cas_request("a")]);
        worker.enqueue(secondary.clone(), vec![test_cas_request("b")]);
        worker.enqueue(secondary.clone(), vec![test_cas_request("c")]);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            let calls = client.take_calls();
            let total: usize = calls.iter().map(|(_, reqs)| reqs.len()).sum();
            if total == 3 {
                for (id, _) in &calls {
                    assert_eq!(id, "secondary-x");
                }
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("worker did not process all 3 requests within 2s (got {total})");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        drop(worker);
    }
}
