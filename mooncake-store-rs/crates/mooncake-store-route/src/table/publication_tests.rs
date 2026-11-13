use mooncake_store_core::{
    AllocationRouteIdentity, AllocationRouteReferenceState, ClientEpoch, ClientRuntimeId,
    CompatibilityDescriptor, ObjectKey, ObjectRoute, PublicationAttempt, PublicationControl,
    PublicationControlReply, PublicationReceiverRegistration, PublicationRevokeRequest,
    ReplicaRoute, ReplicaTier, RouteState, RouteVersion, SegmentName, StoreError,
};

use crate::mesh::registry::{register_local_authority, unregister_local_authority};
use crate::LocalRouteAuthority;

struct Authority {
    namespace: String,
    runtime: ClientRuntimeId,
    service: LocalRouteAuthority,
}

impl Authority {
    fn new(namespace: &str, name: &str, epoch: u64) -> Self {
        let runtime = ClientRuntimeId::new(name, ClientEpoch(epoch));
        register_local_authority(namespace, &runtime, 1024);
        Self {
            namespace: namespace.to_string(),
            service: LocalRouteAuthority::new(namespace, runtime.stable_id.clone()),
            runtime,
        }
    }

    fn control(
        &self,
        operation: PublicationControl,
    ) -> mooncake_store_core::Result<PublicationControlReply> {
        self.service.publication_control(&[operation])?.remove(0)
    }

    fn attempt(&self, publisher: &ClientRuntimeId, sequence: u64) -> PublicationAttempt {
        let PublicationControlReply::Enrolled(permit) = self
            .control(PublicationControl::Enroll {
                publisher: publisher.clone(),
            })
            .unwrap()
        else {
            panic!("enrollment reply required");
        };
        PublicationAttempt::new(mooncake_store_core::PublicationAttemptFields {
            publisher: publisher.clone(),
            sequence,
            key: ObjectKey::new("default::publication-protocol"),
            primary: self.runtime.clone(),
            receivers: vec![self.runtime.clone()],
            permits: vec![permit],
        })
    }
}

impl Drop for Authority {
    fn drop(&mut self) {
        unregister_local_authority(&self.namespace, &self.runtime.stable_id);
    }
}

fn route(attempt: &PublicationAttempt, allocation_id: u64) -> ObjectRoute {
    ObjectRoute {
        key: attempt.key.clone(),
        namespace: None,
        logical_key: None,
        canonical_key: None,
        sharing_scope: None,
        qos_tier: None,
        version: RouteVersion(1),
        state: RouteState::Active,
        compatibility: CompatibilityDescriptor::mooncake_v1(),
        replicas: vec![ReplicaRoute {
            publication: Some(attempt.clone()),
            owner: ClientRuntimeId::new("allocation-owner", ClientEpoch(1)),
            segment_name: SegmentName::new("segment"),
            allocation_id: Some(allocation_id),
            offset: None,
            segment_offset: 0,
            length: 16,
            checksum: None,
            tier: ReplicaTier::Dram,
            priority: 0,
        }],
        cold_backing: None,
        nof_backing: None,
    }
}

fn request(route: &ObjectRoute) -> PublicationRevokeRequest {
    let replica = &route.replicas[0];
    PublicationRevokeRequest {
        attempt: replica.publication.clone().unwrap(),
        allocation: AllocationRouteIdentity {
            owner: replica.owner.clone(),
            segment_name: replica.segment_name.clone(),
            offset_bytes: replica.segment_offset,
            length_bytes: replica.length,
            allocation_id: replica.allocation_id.unwrap(),
        },
        closed_receivers: Vec::new(),
        registration_generation: 0,
        receiver_permits: Vec::new(),
        superseded_before: None,
    }
}

fn register(
    primary: &Authority,
    target: &Authority,
    request: &PublicationRevokeRequest,
) -> PublicationReceiverRegistration {
    let PublicationControlReply::Enrolled(permit) = target
        .control(PublicationControl::Enroll {
            publisher: request.attempt.publisher.clone(),
        })
        .unwrap()
    else {
        panic!("enrollment required");
    };
    let PublicationControlReply::Registered(registration) = primary
        .control(PublicationControl::RegisterReceiver(
            PublicationReceiverRegistration {
                attempt: request.attempt.clone(),
                receiver: target.runtime.clone(),
                permit,
                allocations: vec![request.allocation.clone()],
                generation: 0,
            },
        ))
        .unwrap()
    else {
        panic!("registration required");
    };
    registration
}

#[test]
fn publication_revoke_only_retires_a_strictly_superseded_receiver_reference() {
    let authority = Authority::new("publication-superseded", "primary", 1);
    let attempt = authority.attempt(&ClientRuntimeId::new("publisher", ClientEpoch(1)), 1);
    let route = route(&attempt, 1);
    assert!(
        authority
            .service
            .compare_and_swap_route(&route.key, None, Some(&route))
            .unwrap()
            .applied
    );
    let mut request = request(&route);
    request.superseded_before = Some(route.version);
    let PublicationControlReply::Revoked(reply) = authority
        .control(PublicationControl::Revoke(request.clone()))
        .unwrap()
    else {
        panic!("revoke required");
    };
    assert_eq!(reply.reference.state, AllocationRouteReferenceState::Exact);
    request.superseded_before = Some(route.version.next());
    let PublicationControlReply::Revoked(reply) = authority
        .control(PublicationControl::Revoke(request))
        .unwrap()
    else {
        panic!("revoke required");
    };
    assert_eq!(reply.reference.state, AllocationRouteReferenceState::Absent);
    assert!(matches!(
        authority.service.replace_route(&route.key, Some(&route)),
        Err(StoreError::PublicationRetired(_))
    ));
}

#[test]
fn publication_revoke_rejects_late_cas_version_upgrade_and_raw_replace() {
    let authority = Authority::new("publication-late-cas", "primary", 1);
    let attempt = authority.attempt(&ClientRuntimeId::new("publisher", ClientEpoch(1)), 1);
    let mut route = route(&attempt, 1);
    let PublicationControlReply::Revoked(reply) = authority
        .control(PublicationControl::Revoke(request(&route)))
        .unwrap()
    else {
        panic!("revoke required");
    };
    assert_eq!(reply.reference.state, AllocationRouteReferenceState::Absent);
    for version in [1, 2, u64::MAX] {
        route.version = RouteVersion(version);
        assert!(matches!(
            authority
                .service
                .compare_and_swap_route(&route.key, None, Some(&route)),
            Err(StoreError::PublicationRetired(_))
        ));
        assert!(matches!(
            authority.service.replace_route(&route.key, Some(&route)),
            Err(StoreError::PublicationRetired(_))
        ));
    }
    assert!(authority.service.get_route(&route.key).unwrap().is_none());
}

#[test]
fn publication_generation_closes_late_import_and_stale_finalize() {
    let primary = Authority::new("publication-import-race", "primary", 1);
    let target = Authority::new("publication-import-race", "target", 1);
    let attempt = primary.attempt(&ClientRuntimeId::new("publisher", ClientEpoch(1)), 1);
    let route = route(&attempt, 1);
    let mut request = request(&route);
    assert!(
        primary
            .service
            .compare_and_swap_route(&route.key, None, Some(&route))
            .unwrap()
            .applied
    );
    let first = register(&primary, &target, &request);
    assert_eq!(first.generation, 1);
    let PublicationControlReply::Revoked(exact) = primary
        .control(PublicationControl::Revoke(request.clone()))
        .unwrap()
    else {
        panic!("revoke required");
    };
    assert_eq!(exact.reference.state, AllocationRouteReferenceState::Exact);
    assert_eq!(exact.registration_generation, 1);
    let second = register(&primary, &target, &request);
    assert_eq!(second.generation, 2);
    request.registration_generation = 1;
    assert!(matches!(
        primary.control(PublicationControl::Finalize {
            request: request.clone(),
            released: true
        }),
        Err(StoreError::PublicationRetired(_))
    ));
    assert!(
        primary
            .service
            .compare_and_swap_route(&route.key, Some(RouteVersion(1)), None)
            .unwrap()
            .applied
    );
    let PublicationControlReply::Revoked(absent) = primary
        .control(PublicationControl::Revoke(request.clone()))
        .unwrap()
    else {
        panic!("revoke required");
    };
    assert_eq!(
        absent.reference.state,
        AllocationRouteReferenceState::Absent
    );
    assert_eq!(absent.registration_generation, 2);
    request.registration_generation = 2;
    let PublicationControlReply::Revoked(target_absent) = target
        .control(PublicationControl::Revoke(request.clone()))
        .unwrap()
    else {
        panic!("revoke required");
    };
    assert_eq!(
        target_absent.reference.state,
        AllocationRouteReferenceState::Absent
    );
    for late in [first, second] {
        assert!(matches!(
            target.control(PublicationControl::AdmitReceiver(late)),
            Err(StoreError::PublicationRetired(_))
        ));
    }
    assert!(matches!(
        target.service.replace_route(&route.key, Some(&route)),
        Err(StoreError::PublicationRetired(_))
    ));
    primary
        .control(PublicationControl::Finalize {
            request: request.clone(),
            released: true,
        })
        .unwrap();
    target
        .control(PublicationControl::Finalize {
            request,
            released: true,
        })
        .unwrap();
    assert!(matches!(
        primary
            .service
            .compare_and_swap_route(&route.key, None, Some(&route)),
        Err(StoreError::PublicationRetired(_))
    ));
}

#[test]
fn publication_attempt_cannot_change_allocation_identity() {
    let authority = Authority::new("publication-allocation-binding", "primary", 1);
    let attempt = authority.attempt(&ClientRuntimeId::new("publisher", ClientEpoch(1)), 1);
    let initial = route(&attempt, 1);
    assert!(
        authority
            .service
            .compare_and_swap_route(&initial.key, None, Some(&initial))
            .unwrap()
            .applied
    );
    let mut replacement = route(&attempt, 2);
    replacement.version = RouteVersion(2);
    assert!(matches!(
        authority.service.compare_and_swap_route(
            &initial.key,
            Some(initial.version),
            Some(&replacement)
        ),
        Err(StoreError::PublicationRetired(_))
    ));
    assert_eq!(
        authority.service.get_route(&initial.key).unwrap(),
        Some(initial)
    );
}

#[test]
fn publication_restart_and_retired_enrollment_reject_old_tokens() {
    let old = Authority::new("publication-restart", "primary", 1);
    let publisher = ClientRuntimeId::new("publisher", ClientEpoch(1));
    let old_attempt = old.attempt(&publisher, 1);
    let old_route = route(&old_attempt, 1);
    let restarted = Authority::new("publication-restart", "primary", 2);
    assert!(matches!(
        restarted.control(PublicationControl::Revoke(request(&old_route))),
        Err(StoreError::PublicationRetired(_))
    ));
    assert!(matches!(
        restarted
            .service
            .compare_and_swap_route(&old_route.key, None, Some(&old_route)),
        Err(StoreError::PublicationRetired(_))
    ));
    let first = restarted.attempt(&publisher, 2);
    for index in 0..4096 {
        restarted
            .control(PublicationControl::Enroll {
                publisher: ClientRuntimeId::new(format!("publisher-{index}"), ClientEpoch(1)),
            })
            .unwrap();
    }
    assert!(matches!(
        restarted
            .service
            .compare_and_swap_route(&first.key, None, Some(&route(&first, 2))),
        Err(StoreError::PublicationRetired(_))
    ));
    let fresh = restarted.attempt(&publisher, 3);
    assert_ne!(fresh.permits, first.permits);
    assert!(
        restarted
            .service
            .compare_and_swap_route(&fresh.key, None, Some(&route(&fresh, 3)))
            .unwrap()
            .applied
    );
}

#[test]
fn publication_completed_history_does_not_accumulate_terminal_records() {
    let mut ledger = super::publication::PublicationLedger::default();
    let receiver = ClientRuntimeId::new("primary", ClientEpoch(1));
    let publisher = ClientRuntimeId::new("publisher", ClientEpoch(1));
    let permit = ledger.enroll(&receiver, &publisher).unwrap();
    for sequence in 1..=8192 {
        let attempt = PublicationAttempt::new(mooncake_store_core::PublicationAttemptFields {
            publisher: publisher.clone(),
            sequence,
            key: ObjectKey::new("default::retire"),
            primary: receiver.clone(),
            receivers: vec![receiver.clone()],
            permits: vec![permit.clone()],
        });
        let route = route(&attempt, sequence);
        let request = request(&route);
        ledger.admit(&receiver, &attempt, &route, false).unwrap();
        ledger
            .finalize(&receiver, &attempt, &request.allocation, false, 0, &[])
            .unwrap();
        ledger
            .revoke(&receiver, &attempt, &request.allocation, 0, &[])
            .unwrap();
        ledger
            .finalize(&receiver, &attempt, &request.allocation, true, 0, &[])
            .unwrap();
        assert_eq!(ledger.retained_attempts(), 0);
    }
}

#[test]
fn existing_holder_prune_compares_full_route_at_same_version() {
    let authority = Authority::new("publication-full-route-prune", "primary", 1);
    let publisher = ClientRuntimeId::new("publisher", ClientEpoch(1));
    let old_attempt = authority.attempt(&publisher, 1);
    let new_attempt = authority.attempt(&publisher, 2);
    let old = route(&old_attempt, 1);
    let newer = route(&new_attempt, 2);
    assert_eq!(old.version, newer.version);
    assert!(
        authority
            .service
            .compare_and_swap_route(&old.key, None, Some(&old))
            .unwrap()
            .applied
    );
    assert!(
        authority
            .service
            .compare_and_swap_route(&old.key, Some(old.version), Some(&newer))
            .unwrap()
            .applied
    );

    let PublicationControlReply::Pruned(rejected) = authority
        .control(PublicationControl::PruneExisting {
            expected: Box::new(old),
            next: None,
        })
        .unwrap()
    else {
        panic!("prune reply required");
    };
    assert!(!rejected.applied);
    assert_eq!(rejected.current, Some(newer.clone()));
    let PublicationControlReply::Pruned(applied) = authority
        .control(PublicationControl::PruneExisting {
            expected: Box::new(newer),
            next: None,
        })
        .unwrap()
    else {
        panic!("prune reply required");
    };
    assert!(applied.applied);
}
