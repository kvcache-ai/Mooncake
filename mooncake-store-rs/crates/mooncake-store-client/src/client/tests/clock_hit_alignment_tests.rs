use super::*;
use crate::control_plane::EvictionService;
use crate::AllocationLifecycleConfig;
use mooncake_store_core::{ReplicaTier, RouteState};

fn clock_hit_route(runtime: &ClientRuntimeId) -> ObjectRoute {
    ObjectRoute {
        key: ObjectKey::new("clock-hit"),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: None,
        version: RouteVersion(1),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::default(),
        replicas: vec![ReplicaRoute {
            owner: runtime.clone(),
            segment_name: SegmentName::new("clock-hit-segment"),
            offset: Some(0),
            segment_offset: 0,
            allocation_id: None,
            length: 1024,
            checksum: None,
            tier: ReplicaTier::Dram,
            priority: 0,
        }],
        cold_backing: None,
        nof_backing: None,
    }
}

fn clock_hit_adapter() -> LocalAllocatorAdapter {
    let runtime = ClientRuntimeId::new("clock-hit-owner", ClientEpoch(1));
    let allocator = Arc::new(Mutex::new(LocalAllocatorState::default()));
    LocalAllocatorAdapter {
        runtime: runtime.clone(),
        storage_owner: test_storage_owner_state(&runtime, allocator.clone()),
        allocator,
        lifecycle_state: test_lifecycle_state(ClientLifecycleState::Active),
        transfer_stall_timeout: Duration::from_secs(1),
        request_timeout_override: None,
        allocation_lifecycle: AllocationLifecycleConfig::default(),
    }
}

#[test]
fn clock_hit_report_does_not_reheat_tracked_routes_on_sync() {
    let adapter = clock_hit_adapter();
    let route = clock_hit_route(&adapter.runtime);
    adapter
        .batch_track_routes(std::slice::from_ref(&route))
        .unwrap();
    adapter
        .batch_report_route_hits(std::slice::from_ref(&route.key))
        .unwrap();
    {
        let mut clock = adapter.storage_owner.hot_replicas.clock.lock();
        // Consume the read credit through the production CLOCK victim path.
        assert!(clock.pick_victim_clock(None).is_some());
    }
    adapter.storage_owner.sync_route(&route);
    let clock = adapter.storage_owner.hot_replicas.clock.lock();
    let index = clock.by_key[&route.key][0];
    let entry = clock.entries[index].as_ref().unwrap();
    assert_eq!(
        entry.hot_credit, 0,
        "synchronization must not replay an already consumed hit"
    );
    assert!(!entry.hot);
}

#[test]
fn clock_hit_report_without_nof_does_not_touch_nof_clock() {
    let adapter = clock_hit_adapter();
    let key = ObjectKey::new("untracked-hit");
    let report = adapter
        .batch_report_route_hits(std::slice::from_ref(&key))
        .unwrap();
    let _ = report;
    assert!(adapter
        .storage_owner
        .nof_backings
        .clock
        .lock()
        .pending_hot_keys
        .is_empty());
    assert!(adapter
        .storage_owner
        .hot_replicas
        .clock
        .lock()
        .pending_hot_keys
        .contains(&key));
}

#[test]
fn clock_hit_report_with_nof_does_not_remember_untracked_keys() {
    let mut adapter = clock_hit_adapter();
    Arc::get_mut(&mut adapter.storage_owner)
        .unwrap()
        .nof_backing_tracking_enabled = true;
    let key = ObjectKey::new("dram-only-hit");
    adapter
        .batch_report_route_hits(std::slice::from_ref(&key))
        .unwrap();
    assert!(adapter
        .storage_owner
        .nof_backings
        .clock
        .lock()
        .pending_hot_keys
        .is_empty());
    assert!(adapter
        .storage_owner
        .hot_replicas
        .clock
        .lock()
        .pending_hot_keys
        .contains(&key));
}

#[test]
fn clock_pending_hit_is_consumed_once_when_local_replicas_are_tracked() {
    let adapter = clock_hit_adapter();
    let mut route = clock_hit_route(&adapter.runtime);
    let mut second = route.replicas[0].clone();
    second.segment_offset = 1024;
    route.replicas.push(second);
    adapter
        .batch_report_route_hits(std::slice::from_ref(&route.key))
        .unwrap();
    adapter
        .batch_track_routes(std::slice::from_ref(&route))
        .unwrap();
    {
        let mut clock = adapter.storage_owner.hot_replicas.clock.lock();
        for index in &clock.by_key[&route.key] {
            assert_eq!(clock.entries[*index].as_ref().unwrap().hot_credit, 2);
        }
        assert!(clock.pick_victim_clock(None).is_some());
    }
    adapter.storage_owner.sync_route(&route);
    let clock = adapter.storage_owner.hot_replicas.clock.lock();
    for index in &clock.by_key[&route.key] {
        assert_eq!(
            clock.entries[*index].as_ref().unwrap().hot_credit,
            0,
            "the pending hit must be consumed once for every local replica"
        );
    }
}
