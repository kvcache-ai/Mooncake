use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::{Duration, Instant};

use mooncake_store_core::{ClientLease, ClientStableId, RouteCasRequest, StoreError};
use parking_lot::Mutex;
use tracing::warn;

use crate::mesh::registry::{authority_is_local, authority_replace_many, local_authority_service};
use crate::metrics::record_delete_marker_event;
use crate::shim::{RouteAuthorityClient, RouteMembershipProvider};

const QUEUE_CAPACITY: usize = 1024;
const DRAIN_BUDGET: usize = 4096;
const RECV_TIMEOUT: Duration = Duration::from_millis(100);
const COALESCE_WINDOW: Duration = Duration::from_millis(2);

const SUSPECT_AUTHORITY_TTL: Duration = Duration::from_secs(5);

fn mirror_queue_capacity() -> usize {
    std::env::var("MC_STORE_RS_DELETE_MARKER_PROPAGATION_QUEUE_ITEMS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(65_536))
        .unwrap_or(QUEUE_CAPACITY)
}

fn is_delete_marker_request(request: &RouteCasRequest) -> bool {
    request
        .next
        .as_ref()
        .is_none_or(|route| route.state == mooncake_store_core::RouteState::Tombstone)
}

fn mirror_retry_attempts() -> usize {
    std::env::var("MC_STORE_RS_DELETE_MARKER_RETRY_ATTEMPTS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .map(|value| value.min(8))
        .unwrap_or(3)
}

fn mirror_retry_base_delay() -> Duration {
    let millis = std::env::var("MC_STORE_RS_DELETE_MARKER_RETRY_BASE_DELAY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(|value| value.min(1_000))
        .unwrap_or(50);
    Duration::from_millis(millis)
}

pub(crate) struct RouteMirrorBatch {
    pub(crate) secondary: ClientLease,
    pub(crate) requests: Vec<RouteCasRequest>,
}

pub(crate) struct AsyncRouteMirrorWorker {
    state: Mutex<AsyncRouteMirrorWorkerState>,
}

struct AsyncRouteMirrorWorkerState {
    queue: Option<Arc<MirrorQueue>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct MirrorQueue {
    state: StdMutex<MirrorQueueState>,
    ready: Condvar,
    capacity: usize,
}

struct MirrorQueueState {
    batches: VecDeque<RouteMirrorBatch>,
    shutdown: bool,
}

impl MirrorQueue {
    fn new(capacity: usize) -> Self {
        Self {
            state: StdMutex::new(MirrorQueueState {
                batches: VecDeque::with_capacity(capacity),
                shutdown: false,
            }),
            ready: Condvar::new(),
            capacity,
        }
    }

    fn push(&self, batch: RouteMirrorBatch) -> Option<RouteMirrorBatch> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let evicted = if state.batches.len() == self.capacity {
            state.batches.pop_front()
        } else {
            None
        };
        state.batches.push_back(batch);
        self.ready.notify_one();
        evicted
    }

    fn pop_wait(&self) -> Option<RouteMirrorBatch> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        while state.batches.is_empty() && !state.shutdown {
            let waited = self
                .ready
                .wait_timeout(state, RECV_TIMEOUT)
                .unwrap_or_else(|poison| poison.into_inner());
            state = waited.0;
            if waited.1.timed_out() && state.batches.is_empty() {
                return None;
            }
        }
        state.batches.pop_front()
    }

    fn try_pop(&self) -> Option<RouteMirrorBatch> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.batches.pop_front()
    }

    fn shutdown(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.shutdown = true;
        self.ready.notify_all();
    }

    fn is_shutdown(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .shutdown
    }
}

impl AsyncRouteMirrorWorker {
    pub(crate) fn disabled() -> Self {
        Self {
            state: Mutex::new(AsyncRouteMirrorWorkerState {
                queue: None,
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
        let queue = Arc::new(MirrorQueue::new(mirror_queue_capacity()));
        let worker_queue = queue.clone();
        let thread = match std::thread::Builder::new()
            .name(format!("mooncake-route-mirror-{}", local_stable_id.0))
            .spawn(move || loop {
                if worker_queue.is_shutdown() {
                    break;
                }
                let Some(first) = worker_queue.pop_wait() else {
                    continue;
                };
                let mut groups = BTreeMap::<String, (ClientLease, Vec<RouteCasRequest>)>::new();
                let mut total_items = 0usize;
                Self::push_batch(&mut groups, first, &mut total_items);
                let started = Instant::now();
                while total_items < DRAIN_BUDGET && started.elapsed() < COALESCE_WINDOW {
                    match worker_queue.try_pop() {
                        Some(batch) => Self::push_batch(&mut groups, batch, &mut total_items),
                        None => break,
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
                queue: Some(queue),
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
        let Some(queue) = state.queue.as_ref() else {
            return;
        };
        if requests.iter().any(is_delete_marker_request) {
            record_delete_marker_event("propagation_enqueued", 1);
        }
        let evicted = queue.push(RouteMirrorBatch {
            secondary,
            requests,
        });
        if let Some(batch) = evicted {
            if batch.requests.iter().any(is_delete_marker_request) {
                record_delete_marker_event("propagation_queue_overwrite", 1);
            }
            warn!(
                authority = %batch.secondary.runtime,
                items = batch.requests.len(),
                "overwriting oldest best-effort route mirror after async queue filled"
            );
        }
    }

    pub(crate) fn shutdown(&mut self) {
        let (queue, thread) = {
            let mut state = self.state.lock();
            (state.queue.take(), state.thread.take())
        };
        if let Some(queue) = queue {
            queue.shutdown();
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
    let is_delete_marker = requests.iter().any(is_delete_marker_request);
    let attempts = if is_delete_marker {
        mirror_retry_attempts()
    } else {
        1
    };
    let base_delay = mirror_retry_base_delay();
    for attempt in 0..attempts {
        let result = mirror_secondary_batch_once(namespace, authority_client, secondary, requests);
        let succeeded = result
            .as_ref()
            .is_ok_and(|results| results.iter().all(|result| result.is_ok()));
        if succeeded {
            if is_delete_marker {
                record_delete_marker_event("propagation_ok", 1);
            }
            return;
        }
        if attempt + 1 < attempts {
            if is_delete_marker {
                record_delete_marker_event("propagation_retry", 1);
            }
            let shift = attempt.min(10) as u32;
            std::thread::sleep(
                base_delay
                    .saturating_mul(1u32 << shift)
                    .min(Duration::from_secs(2)),
            );
            continue;
        }
        if is_delete_marker {
            record_delete_marker_event("propagation_failed", 1);
        }
        log_mirror_failure(namespace, membership, secondary, requests, result);
    }
}

fn mirror_secondary_batch_once(
    namespace: &str,
    authority_client: &dyn RouteAuthorityClient,
    secondary: &ClientLease,
    requests: &[RouteCasRequest],
) -> mooncake_store_core::Result<Vec<mooncake_store_core::Result<()>>> {
    if authority_is_local(namespace, &secondary.runtime.stable_id) {
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
    }
}

fn log_mirror_failure(
    namespace: &str,
    membership: &dyn RouteMembershipProvider,
    secondary: &ClientLease,
    requests: &[RouteCasRequest],
    result: mooncake_store_core::Result<Vec<mooncake_store_core::Result<()>>>,
) {
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
    use crate::shim::{RouteAuthorityRecord, RouteOwnerPageQuery};
    use mooncake_store_core::{CasResult, ClientRuntimeId, ObjectKey, Result};

    struct RecordingAuthorityClient {
        calls: Mutex<Vec<(String, Vec<RouteCasRequest>)>>,
    }

    impl RecordingAuthorityClient {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<(String, Vec<RouteCasRequest>)> {
            self.calls.lock().clone()
        }
    }

    impl RouteAuthorityClient for RecordingAuthorityClient {
        fn batch_get_routes(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            keys: &[ObjectKey],
        ) -> Result<Vec<Result<RouteAuthorityRecord>>> {
            Ok(keys
                .iter()
                .map(|_| Ok(RouteAuthorityRecord::default()))
                .collect())
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

        fn list_routes_by_replica_owner_page(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &ClientStableId,
            _query: &RouteOwnerPageQuery,
        ) -> Result<crate::RouteOwnerPage> {
            Ok(crate::RouteOwnerPage::empty())
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
        use mooncake_store_core::{CompatibilityDescriptor, ObjectRoute, RouteState, RouteVersion};

        let key = ObjectKey::new(key.to_string());
        RouteCasRequest {
            key: key.clone(),
            expected: None,
            next: Some(ObjectRoute {
                key,
                namespace: None,
                logical_key: None,
                canonical_key: None,
                sharing_scope: None,
                qos_tier: None,
                version: RouteVersion(1),
                state: RouteState::Active,
                compatibility: CompatibilityDescriptor::mooncake_v1(),
                replicas: Vec::new(),
                cold_backing: None,
                nof_backing: None,
            }),
        }
    }

    fn test_delete_cas_request(key: &str) -> RouteCasRequest {
        RouteCasRequest {
            key: ObjectKey::new(key.to_string()),
            expected: Some(mooncake_store_core::RouteVersion(1)),
            next: None,
        }
    }

    fn test_batch(key: &str) -> RouteMirrorBatch {
        RouteMirrorBatch {
            secondary: test_lease("secondary"),
            requests: vec![test_cas_request(key)],
        }
    }

    #[test]
    fn request_classifier_distinguishes_live_routes_and_deletes() {
        assert!(!is_delete_marker_request(&test_cas_request("live")));
        assert!(is_delete_marker_request(&test_delete_cas_request(
            "deleted"
        )));
    }

    #[test]
    fn disabled_worker_silently_drops() {
        let worker = AsyncRouteMirrorWorker::disabled();
        worker.enqueue(test_lease("secondary-a"), vec![test_cas_request("key-1")]);
    }

    #[test]
    fn enqueue_overwrites_oldest_when_queue_is_full() {
        let queue = MirrorQueue::new(2);
        assert!(queue.push(test_batch("key-0")).is_none());
        assert!(queue.push(test_batch("key-1")).is_none());

        let evicted = queue
            .push(test_batch("key-2"))
            .expect("a full queue should evict its oldest batch");
        assert_eq!(evicted.requests[0].key, ObjectKey::new("key-0"));
        assert_eq!(
            queue.try_pop().expect("key-1 should remain").requests[0].key,
            ObjectKey::new("key-1")
        );
        assert_eq!(
            queue.try_pop().expect("key-2 should remain").requests[0].key,
            ObjectKey::new("key-2")
        );
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
    fn push_batch_coalesces_same_authority_batches() {
        let secondary = test_lease("secondary-x");
        let mut groups = BTreeMap::new();
        let mut total_items = 0;

        AsyncRouteMirrorWorker::push_batch(
            &mut groups,
            RouteMirrorBatch {
                secondary: secondary.clone(),
                requests: vec![test_cas_request("a")],
            },
            &mut total_items,
        );
        AsyncRouteMirrorWorker::push_batch(
            &mut groups,
            RouteMirrorBatch {
                secondary: secondary.clone(),
                requests: vec![test_cas_request("b")],
            },
            &mut total_items,
        );
        AsyncRouteMirrorWorker::push_batch(
            &mut groups,
            RouteMirrorBatch {
                secondary,
                requests: vec![test_cas_request("c")],
            },
            &mut total_items,
        );

        assert_eq!(total_items, 3);
        assert_eq!(groups.len(), 1);
        let (_, requests) = groups
            .get("secondary-x")
            .expect("same authority batches should share one group");
        assert_eq!(requests.len(), 3);
    }

    #[test]
    fn enqueue_processes_same_authority_batches() {
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
            let calls = client.calls();
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
