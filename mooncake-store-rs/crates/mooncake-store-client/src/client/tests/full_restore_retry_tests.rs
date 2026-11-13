use super::full_lifecycle_cold_probe::{cold_probe_client, cold_probe_owner, cold_probe_prepare};
use super::*;
use crate::control_plane::ColdTierControlService;

struct RestoreUnavailableDirectory {
    inner: Arc<dyn RouteDirectory>,
    unavailable: AtomicBool,
    apply_before_error: bool,
}

impl RouteDirectory for RestoreUnavailableDirectory {
    fn get_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
    ) -> Result<Option<ObjectRoute>> {
        self.inner.get_object_route(observer, key)
    }

    fn compare_and_swap_object_route(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        expected: Option<RouteVersion>,
        next: Option<&ObjectRoute>,
    ) -> Result<mooncake_store_core::CasResult> {
        if self.unavailable.load(Ordering::SeqCst) {
            if self.apply_before_error {
                self.inner
                    .compare_and_swap_object_route(observer, key, expected, next)?;
            }
            return Err(StoreError::Transport(
                "restore publication unavailable".to_string(),
            ));
        }
        self.inner
            .compare_and_swap_object_route(observer, key, expected, next)
    }

    fn confirm_replica_reference(
        &self,
        observer: &ClientLease,
        key: &ObjectKey,
        replica: &ReplicaRoute,
    ) -> Result<RouteReplicaReference> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(StoreError::Transport(
                "restore confirmation unavailable".to_string(),
            ));
        }
        self.inner.confirm_replica_reference(observer, key, replica)
    }

    fn metadata(&self) -> &dyn MetadataBackend {
        self.inner.metadata()
    }
}

fn exercise_restore_retry(apply_before_error: bool, remove_before_retry: bool) {
    let _cold_guard = enable_cold_tier_for_test();
    let mut client = cold_probe_client(if apply_before_error {
        "restore-retry-applied"
    } else if remove_before_retry {
        "restore-retry-removed"
    } else {
        "restore-retry-unapplied"
    });
    let payload = b"restore-payload!";
    let hot = client.put("retry-object", payload).expect("initial put");
    let materializer = cold_probe_owner(&client, client.route_directory.clone());
    let prepared = cold_probe_prepare(&materializer, &hot);
    let mut error = None;
    materializer
        .materialize_prepared_pending_offload_batch(&prepared, &mut error)
        .expect("offload");
    drop(prepared);
    force_cold_only_route(&client, "retry-object");
    let directory = Arc::new(RestoreUnavailableDirectory {
        inner: client.route_directory.clone(),
        unavailable: AtomicBool::new(true),
        apply_before_error,
    });
    let owner = cold_probe_owner(&client, directory.clone());
    let adapter = LocalAllocatorAdapter {
        runtime: client.runtime_id().clone(),
        allocator: client.allocator.clone(),
        storage_owner: owner.clone(),
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
        allocation_lifecycle: AllocationLifecycleConfig::default(),
    };
    let response = adapter.read_from_cold(
        &owner.metadata.route_namespace(),
        &client.runtime_id().stable_id.0,
        "default",
        "retry-object",
        "",
        "",
    );
    let reply = response.result.expect("cold read");
    response.deferred_promote.expect("deferred promote")();
    adapter
        .ack_cold_read_complete(&[(SegmentName::new(&reply.segment_name), reply.segment_offset)]);
    assert_eq!(owner.pending_restore_publications.lock().len(), 1);
    assert_eq!(owner.recover_owner_restore_publications(), 0);
    assert_eq!(
        owner.pending_restore_publications.lock().len(),
        1,
        "persistent unknown outcomes must keep one retry context"
    );
    assert_eq!(
        owner
            .recover_due_pending_publications()
            .expect("ordinary recovery"),
        0
    );
    let held = owner
        .pending_restore_publications
        .lock()
        .front()
        .expect("context")
        .reservation
        .clone();
    assert!(
        matches!(owner.allocator.lock().pending_allocations.get(&AllocationSpan {
        segment_name: held.segment_name.clone(), offset_bytes: held.offset_bytes,
        length_bytes: held.length_bytes,
    }), Some(PendingAllocation::RestorePublication { allocation_id }) if *allocation_id == held.allocation_id)
    );
    if remove_before_retry {
        client
            .remove("retry-object", false)
            .expect("remove before retry");
    }
    client.storage_owner = owner.clone();
    let other = client
        .put("other-object", b"overwrite-data!!")
        .expect("public put while held");
    assert!(
        other
            .replicas
            .iter()
            .all(|replica| replica.segment_name != held.segment_name
                || replica.segment_offset != held.offset_bytes),
        "held bytes must remain unavailable for reuse"
    );
    directory.unavailable.store(false, Ordering::SeqCst);
    owner.recover_owner_restore_publications();
    assert!(owner.pending_restore_publications.lock().is_empty());
    if remove_before_retry {
        assert!(client
            .query_route("retry-object")
            .expect("query removed")
            .is_none());
        assert_ne!(
            owner.allocator.lock().allocation_id(&AllocationSpan {
                segment_name: held.segment_name,
                offset_bytes: held.offset_bytes,
                length_bytes: held.length_bytes,
            }),
            Some(held.allocation_id),
            "definite current=None must reclaim the restore allocation"
        );
    } else {
        assert_eq!(
            client.get("retry-object").expect("public get restored"),
            payload
        );
        assert_eq!(owner.recover_owner_restore_publications(), 0);
    }
}

#[test]
fn full_lifecycle_cold_restore_unknown_applied_retries_without_releasing() {
    exercise_restore_retry(true, false);
}

#[test]
fn full_lifecycle_cold_restore_unknown_unapplied_retries_to_publish() {
    exercise_restore_retry(false, false);
}

#[test]
fn full_lifecycle_cold_restore_unknown_removed_retries_to_reclaim() {
    exercise_restore_retry(false, true);
}
