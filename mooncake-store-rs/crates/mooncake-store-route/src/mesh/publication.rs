use std::collections::BTreeSet;

use mooncake_store_core::{
    AllocationRouteIdentity, ClientLease, PublicationControl, PublicationControlReply,
    PublicationReceiverRegistration, Result, RouteCasRequest, StoreError,
};

use crate::shim::{RouteAuthorityClient, RouteMembershipProvider};

pub(super) fn control_at(
    namespace: &str,
    client: &dyn RouteAuthorityClient,
    target: &ClientLease,
    operations: &[PublicationControl],
) -> Result<Vec<Result<PublicationControlReply>>> {
    if super::registry::authority_is_local(namespace, &target.runtime.stable_id) {
        if let Some(service) =
            super::registry::local_authority_service(namespace, &target.runtime.stable_id)
        {
            return service.publication_control(namespace, &target.runtime.stable_id, operations);
        }
        return super::registry::authority_publication_control(
            namespace,
            &target.runtime.stable_id,
            operations,
        );
    }
    client.publication_control(target, namespace, &target.runtime.stable_id, operations)
}

fn one_reply(
    namespace: &str,
    client: &dyn RouteAuthorityClient,
    target: &ClientLease,
    operation: PublicationControl,
) -> Result<PublicationControlReply> {
    let mut replies = control_at(namespace, client, target, &[operation])?;
    if replies.len() != 1 {
        return Err(StoreError::Transport(
            "publication control reply count mismatch".to_string(),
        ));
    }
    replies.remove(0)
}

/// Repairs an import rejected by the local provenance gate. Timely mirrors
/// use their original receiver permits and do not enter this handshake.
pub(super) fn register_imports(
    namespace: &str,
    client: &dyn RouteAuthorityClient,
    membership: &dyn RouteMembershipProvider,
    target: &ClientLease,
    requests: &[RouteCasRequest],
) -> Result<()> {
    let mut leases = membership.live_clients(false, "publication_import_current_authorities")?;
    leases.extend(membership.retained_live_clients("publication_import_authorities")?);
    let mut attempts = BTreeSet::new();
    for request in requests {
        let Some(route) = &request.next else {
            continue;
        };
        for replica in &route.replicas {
            let Some(attempt) = &replica.publication else {
                continue;
            };
            if !attempts.insert(attempt.clone()) {
                continue;
            }
            let primary = leases
                .iter()
                .find(|lease| lease.runtime == attempt.primary)
                .ok_or_else(|| {
                    StoreError::Transport(
                        "publication import primary incarnation is unavailable".to_string(),
                    )
                })?;
            let PublicationControlReply::Enrolled(permit) = one_reply(
                namespace,
                client,
                target,
                PublicationControl::Enroll {
                    publisher: attempt.publisher.clone(),
                },
            )?
            else {
                return Err(StoreError::Transport(
                    "publication import enrollment reply type mismatch".to_string(),
                ));
            };
            let allocation_id = replica.allocation_id.filter(|id| *id != 0).ok_or_else(|| {
                StoreError::InvalidState(
                    "publication import has no allocation generation".to_string(),
                )
            })?;
            let registration = PublicationReceiverRegistration {
                attempt: attempt.clone(),
                receiver: target.runtime.clone(),
                permit,
                allocations: vec![AllocationRouteIdentity {
                    owner: replica.owner.clone(),
                    segment_name: replica.segment_name.clone(),
                    offset_bytes: replica.segment_offset,
                    length_bytes: replica.length,
                    allocation_id,
                }],
                generation: 0,
            };
            let PublicationControlReply::Registered(registration) = one_reply(
                namespace,
                client,
                primary,
                PublicationControl::RegisterReceiver(registration),
            )?
            else {
                return Err(StoreError::Transport(
                    "publication import registration reply type mismatch".to_string(),
                ));
            };
            let PublicationControlReply::Admitted = one_reply(
                namespace,
                client,
                target,
                PublicationControl::AdmitReceiver(registration),
            )?
            else {
                return Err(StoreError::Transport(
                    "publication import permit reply type mismatch".to_string(),
                ));
            };
        }
    }
    Ok(())
}
