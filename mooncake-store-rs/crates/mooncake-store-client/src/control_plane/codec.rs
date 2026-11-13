use super::cold_tier_codec::{
    pb_cold_backing_route, pb_nof_backing_route, try_cold_backing_route, try_nof_backing_route,
};
use super::*;

pub(super) fn decode_error(error: Option<pb::ErrorDetail>) -> Result<()> {
    match error {
        Some(error) => Err(store_error_from_pb(error)),
        None => Ok(()),
    }
}

pub(super) fn ensure_batch_len(operation: &str, expected: usize, actual: usize) -> Result<()> {
    if expected == actual {
        return Ok(());
    }
    Err(StoreError::Transport(format!(
        "control plane {operation} reply length mismatch: expected={expected} actual={actual}"
    )))
}

pub(super) fn status_to_store_error(status: Status) -> StoreError {
    StoreError::Transport(format!("control plane rpc failed: {status}"))
}

pub(super) fn control_address(lease: &ClientLease) -> Result<String> {
    lease
        .endpoints
        .labels
        .get(CONTROL_ADDR_LABEL)
        .cloned()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            StoreError::Unsupported(format!(
                "client {} is missing control plane address",
                lease.runtime
            ))
        })
}

pub(super) fn normalize_control_uri(address: &str) -> String {
    if address.contains("://") {
        return address.to_string();
    }
    format!("http://{address}")
}

pub(super) fn pb_error(error: StoreError) -> pb::ErrorDetail {
    let (kind, message) = match error {
        StoreError::NotFound(message) => (pb::ErrorKind::NotFound, message),
        StoreError::Conflict(message) => (pb::ErrorKind::Conflict, message),
        StoreError::PublicationRetired(message) => (pb::ErrorKind::PublicationRetired, message),
        StoreError::QuotaExceeded { message, .. } => (pb::ErrorKind::Conflict, message),
        StoreError::InvalidState(message) => (pb::ErrorKind::InvalidState, message),
        StoreError::StaleEpoch(message) => (pb::ErrorKind::StaleEpoch, message),
        StoreError::Unsupported(message) => (pb::ErrorKind::Unsupported, message),
        StoreError::Allocator(message) => (pb::ErrorKind::Allocator, message),
        StoreError::Metadata(message) => (pb::ErrorKind::Metadata, message),
        StoreError::Transport(message) => (pb::ErrorKind::Transport, message),
        StoreError::Backpressure(message) => (pb::ErrorKind::Backpressure, message),
    };
    pb::ErrorDetail {
        kind: kind as i32,
        message,
    }
}

pub(super) fn store_error_from_pb(error: pb::ErrorDetail) -> StoreError {
    match pb::ErrorKind::try_from(error.kind).unwrap_or(pb::ErrorKind::Transport) {
        pb::ErrorKind::NotFound => StoreError::NotFound(error.message),
        pb::ErrorKind::Conflict => StoreError::Conflict(error.message),
        pb::ErrorKind::PublicationRetired => StoreError::PublicationRetired(error.message),
        pb::ErrorKind::InvalidState => StoreError::InvalidState(error.message),
        pb::ErrorKind::StaleEpoch => StoreError::StaleEpoch(error.message),
        pb::ErrorKind::Unsupported => StoreError::Unsupported(error.message),
        pb::ErrorKind::Allocator => StoreError::Allocator(error.message),
        pb::ErrorKind::Metadata => StoreError::Metadata(error.message),
        pb::ErrorKind::Backpressure => StoreError::Backpressure(error.message),
        pb::ErrorKind::Transport | pb::ErrorKind::Unspecified => {
            StoreError::Transport(error.message)
        }
    }
}

pub(super) fn pb_runtime_id(runtime: &ClientRuntimeId) -> pb::ClientRuntimeId {
    pb::ClientRuntimeId {
        stable_id: runtime.stable_id.0.clone(),
        epoch: runtime.epoch.0,
    }
}

pub(super) fn try_runtime_id(runtime: &pb::ClientRuntimeId) -> Result<ClientRuntimeId> {
    Ok(ClientRuntimeId {
        stable_id: ClientStableId::new(runtime.stable_id.clone()),
        epoch: ClientEpoch(runtime.epoch),
    })
}

pub(super) fn pb_allocation_route_identity(
    identity: &AllocationRouteIdentity,
) -> pb::AllocationRouteIdentity {
    pb::AllocationRouteIdentity {
        owner: Some(pb_runtime_id(&identity.owner)),
        segment_name: identity.segment_name.0.clone(),
        offset_bytes: identity.offset_bytes,
        length_bytes: identity.length_bytes,
        allocation_id: identity.allocation_id,
    }
}

pub(super) fn try_allocation_route_identity(
    identity: pb::AllocationRouteIdentity,
) -> Result<AllocationRouteIdentity> {
    let owner = identity
        .owner
        .as_ref()
        .map(try_runtime_id)
        .transpose()?
        .ok_or_else(|| {
            StoreError::Transport("allocation reference identity is missing owner".to_string())
        })?;
    if identity.segment_name.is_empty() || identity.length_bytes == 0 || identity.allocation_id == 0
    {
        return Err(StoreError::Transport(
            "allocation reference identity is invalid".to_string(),
        ));
    }
    Ok(AllocationRouteIdentity {
        owner,
        segment_name: SegmentName::new(identity.segment_name),
        offset_bytes: identity.offset_bytes,
        length_bytes: identity.length_bytes,
        allocation_id: identity.allocation_id,
    })
}

pub(super) fn pb_delete_pending_entry(entry: &DeletePendingEntry) -> pb::DeletePendingEntry {
    pb::DeletePendingEntry {
        route_key: entry.route_key.0.clone(),
        identity: entry.identity.as_ref().map(pb_allocation_route_identity),
        cold_backing: entry.cold_backing.as_ref().map(|target| {
            pb_cold_backing_route(&mooncake_store_core::ColdBackingRoute {
                owner: target.owner.clone(),
                cold_tier_id: target.cold_tier_id.clone(),
                object_locator: target.object_locator.clone(),
                length: target.length,
                checksum: target.checksum,
                state: mooncake_store_core::ColdBackingState::PendingDelete,
                replicas: Vec::new(),
            })
        }),
        reclaim_grace_ms: entry.reclaim_grace_ms,
        prepare_only: entry.prepare_only,
        expected_route_version: entry.expected_route_version.map(|version| version.0),
    }
}

pub(super) fn try_delete_pending_entry(
    entry: pb::DeletePendingEntry,
) -> Result<DeletePendingEntry> {
    if entry.route_key.is_empty() {
        return Err(StoreError::Transport(
            "delete pending entry is missing route key".to_string(),
        ));
    }
    let identity = entry
        .identity
        .map(try_allocation_route_identity)
        .transpose()?;
    let cold_backing = entry
        .cold_backing
        .map(try_cold_backing_route)
        .transpose()?
        .map(|route| ColdDeleteTarget {
            owner: route.owner,
            cold_tier_id: route.cold_tier_id,
            object_locator: route.object_locator,
            length: route.length,
            checksum: route.checksum,
        });
    if identity.is_none() && cold_backing.is_none() {
        return Err(StoreError::Transport(
            "delete pending entry is missing identity and cold backing".to_string(),
        ));
    }
    Ok(DeletePendingEntry {
        route_key: ObjectKey::new(entry.route_key),
        identity,
        cold_backing,
        reclaim_grace_ms: entry.reclaim_grace_ms,
        prepare_only: entry.prepare_only,
        expected_route_version: entry
            .expected_route_version
            .map(mooncake_store_core::RouteVersion),
    })
}

pub(super) fn pb_allocation_reference_state(
    state: AllocationRouteReferenceState,
) -> pb::AllocationRouteReferenceState {
    let state = match state {
        AllocationRouteReferenceState::Absent => {
            pb::allocation_route_reference_state::State::Absent(true)
        }
        AllocationRouteReferenceState::Exact => {
            pb::allocation_route_reference_state::State::Exact(true)
        }
        AllocationRouteReferenceState::Conflict => {
            pb::allocation_route_reference_state::State::Conflict(true)
        }
    };
    pb::AllocationRouteReferenceState {
        state: Some(state),
        route_key: String::new(),
    }
}

pub(super) fn pb_allocation_reference(
    reference: AllocationRouteReference,
) -> pb::AllocationRouteReferenceState {
    let mut encoded = pb_allocation_reference_state(reference.state);
    encoded.route_key = reference.route_key.map(|key| key.0).unwrap_or_default();
    encoded
}

pub(super) fn try_allocation_reference_state(
    state: pb::AllocationRouteReferenceState,
) -> Result<AllocationRouteReferenceState> {
    match state.state {
        Some(pb::allocation_route_reference_state::State::Absent(true)) => {
            Ok(AllocationRouteReferenceState::Absent)
        }
        Some(pb::allocation_route_reference_state::State::Exact(true)) => {
            Ok(AllocationRouteReferenceState::Exact)
        }
        Some(pb::allocation_route_reference_state::State::Conflict(true)) => {
            Ok(AllocationRouteReferenceState::Conflict)
        }
        _ => Err(StoreError::Transport(
            "allocation reference reply contains an invalid state".to_string(),
        )),
    }
}

pub(super) fn try_allocation_reference(
    state: pb::AllocationRouteReferenceState,
) -> Result<AllocationRouteReference> {
    let route_key = (!state.route_key.is_empty()).then(|| ObjectKey::new(state.route_key.clone()));
    let state = try_allocation_reference_state(state)?;
    match state {
        AllocationRouteReferenceState::Exact if route_key.is_none() => Err(StoreError::Transport(
            "exact allocation reference reply is missing route key".to_string(),
        )),
        AllocationRouteReferenceState::Exact => Ok(AllocationRouteReference { state, route_key }),
        _ if route_key.is_some() => Err(StoreError::Transport(
            "non-exact allocation reference reply contains a route key".to_string(),
        )),
        _ => Ok(AllocationRouteReference {
            state,
            route_key: None,
        }),
    }
}

pub(super) fn pb_compatibility(
    descriptor: &CompatibilityDescriptor,
) -> pb::CompatibilityDescriptor {
    pb::CompatibilityDescriptor {
        store_api_version: descriptor.store_api_version,
        store_api_minor_version: descriptor.store_api_minor_version,
        metadata_schema_version: descriptor.metadata_schema_version,
        transport_api_version: descriptor.transport_api_version,
        capabilities: descriptor.capabilities.iter().cloned().collect(),
    }
}

pub(super) fn try_compatibility(
    descriptor: &pb::CompatibilityDescriptor,
) -> CompatibilityDescriptor {
    CompatibilityDescriptor {
        store_api_version: descriptor.store_api_version,
        store_api_minor_version: descriptor.store_api_minor_version,
        metadata_schema_version: descriptor.metadata_schema_version,
        transport_api_version: descriptor.transport_api_version,
        capabilities: descriptor.capabilities.iter().cloned().collect(),
    }
}

pub(super) fn pb_replica_tier(tier: ReplicaTier) -> i32 {
    match tier {
        ReplicaTier::Dram => pb::ReplicaTier::Dram as i32,
        ReplicaTier::Nvme => pb::ReplicaTier::Nvme as i32,
        ReplicaTier::File => pb::ReplicaTier::File as i32,
        ReplicaTier::Unknown => pb::ReplicaTier::Unknown as i32,
    }
}

pub(super) fn try_replica_tier(tier: i32) -> Result<ReplicaTier> {
    Ok(
        match pb::ReplicaTier::try_from(tier).unwrap_or(pb::ReplicaTier::Unknown) {
            pb::ReplicaTier::Dram => ReplicaTier::Dram,
            pb::ReplicaTier::Nvme => ReplicaTier::Nvme,
            pb::ReplicaTier::File => ReplicaTier::File,
            pb::ReplicaTier::Unknown | pb::ReplicaTier::Unspecified => ReplicaTier::Unknown,
        },
    )
}

pub(super) fn pb_route_state(state: mooncake_store_core::RouteState) -> i32 {
    match state {
        mooncake_store_core::RouteState::Active => pb::RouteState::Active as i32,
        mooncake_store_core::RouteState::Deleting => pb::RouteState::Deleting as i32,
        mooncake_store_core::RouteState::Tombstone => pb::RouteState::Tombstone as i32,
    }
}

pub(super) fn try_route_state(state: i32) -> Result<mooncake_store_core::RouteState> {
    Ok(
        match pb::RouteState::try_from(state).unwrap_or(pb::RouteState::Unspecified) {
            pb::RouteState::Active | pb::RouteState::Unspecified => {
                mooncake_store_core::RouteState::Active
            }
            pb::RouteState::Deleting => mooncake_store_core::RouteState::Deleting,
            pb::RouteState::Tombstone => mooncake_store_core::RouteState::Tombstone,
        },
    )
}

pub(super) fn pb_replica_route(replica: &ReplicaRoute) -> pb::ReplicaRoute {
    pb::ReplicaRoute {
        publication: encode_publication(replica.publication.as_ref()),
        owner: Some(pb_runtime_id(&replica.owner)),
        segment_name: replica.segment_name.0.clone(),
        allocation_id: replica.allocation_id,
        offset: replica.offset,
        segment_offset: replica.segment_offset,
        length: replica.length,
        checksum: replica.checksum,
        tier: pb_replica_tier(replica.tier),
        priority: u32::from(replica.priority),
    }
}

pub(super) fn try_replica_route(replica: pb::ReplicaRoute) -> Result<ReplicaRoute> {
    let owner = replica
        .owner
        .as_ref()
        .map(try_runtime_id)
        .transpose()?
        .ok_or_else(|| {
            StoreError::Transport("control plane replica route is missing owner".to_string())
        })?;
    let priority = u16::try_from(replica.priority).map_err(|_| {
        StoreError::Transport(format!(
            "control plane replica route priority {} does not fit u16",
            replica.priority
        ))
    })?;
    Ok(ReplicaRoute {
        publication: decode_publication(&replica.publication)?,
        owner,
        segment_name: SegmentName::new(replica.segment_name),
        allocation_id: replica.allocation_id,
        offset: replica.offset,
        segment_offset: replica.segment_offset,
        length: replica.length,
        checksum: replica.checksum,
        tier: try_replica_tier(replica.tier)?,
        priority,
    })
}

pub(super) fn pb_object_route(route: &ObjectRoute) -> pb::ObjectRoute {
    pb::ObjectRoute {
        key: route.key.0.clone(),
        tenant: route
            .namespace
            .as_ref()
            .map(|scope| scope.tenant.clone())
            .unwrap_or_default(),
        domain: route
            .namespace
            .as_ref()
            .map(|scope| scope.domain.clone())
            .unwrap_or_default(),
        object_set: route
            .namespace
            .as_ref()
            .map(|scope| scope.object_set.clone())
            .unwrap_or_default(),
        logical_key: route.logical_key.clone().unwrap_or_default(),
        canonical_key: route.canonical_key.clone().unwrap_or_default(),
        sharing_scope: route.sharing_scope.clone().unwrap_or_default(),
        qos_tier: route.qos_tier.clone().unwrap_or_default(),
        version: route.version.0,
        state: pb_route_state(route.state),
        compatibility: Some(pb_compatibility(&route.compatibility)),
        replicas: route.replicas.iter().map(pb_replica_route).collect(),
        cold_backing: route.cold_backing.as_ref().map(pb_cold_backing_route),
        nof_backing: route.nof_backing.as_ref().map(pb_nof_backing_route),
    }
}

pub(super) fn try_object_route(route: pb::ObjectRoute) -> Result<ObjectRoute> {
    let compatibility = route
        .compatibility
        .as_ref()
        .map(try_compatibility)
        .ok_or_else(|| {
            StoreError::Transport("control plane object route is missing compatibility".to_string())
        })?;
    let namespace =
        if route.tenant.is_empty() && route.domain.is_empty() && route.object_set.is_empty() {
            None
        } else {
            Some(mooncake_store_core::NamespaceScope::with_defaults(
                Some(route.tenant.as_str()).filter(|value| !value.is_empty()),
                Some(route.domain.as_str()).filter(|value| !value.is_empty()),
                Some(route.object_set.as_str()).filter(|value| !value.is_empty()),
            ))
        };
    let logical_key = (!route.logical_key.is_empty()).then_some(route.logical_key.clone());
    let canonical_key = (!route.canonical_key.is_empty()).then_some(route.canonical_key.clone());
    let sharing_scope = (!route.sharing_scope.is_empty()).then_some(route.sharing_scope.clone());
    let qos_tier = (!route.qos_tier.is_empty()).then_some(route.qos_tier.clone());
    let route = ObjectRoute {
        key: ObjectKey::new(route.key),
        namespace,
        logical_key,
        canonical_key,
        sharing_scope,
        qos_tier,
        version: RouteVersion(route.version),
        state: try_route_state(route.state)?,
        compatibility,
        replicas: route
            .replicas
            .into_iter()
            .map(try_replica_route)
            .collect::<Result<Vec<_>>>()?,
        cold_backing: route.cold_backing.map(try_cold_backing_route).transpose()?,
        nof_backing: route.nof_backing.map(try_nof_backing_route).transpose()?,
    };
    Ok(route)
}

pub(super) fn try_object_route_ref(route: &pb::ObjectRoute) -> Result<ObjectRoute> {
    try_object_route(route.clone())
}

pub(super) fn pb_cas_result(result: &CasResult) -> pb::CasResult {
    pb::CasResult {
        applied: result.applied,
        current: result.current.as_ref().map(pb_object_route),
        version_floor: result.version_floor.map(|v| v.0),
        delete_version: result.delete_version.map(|v| v.0),
    }
}

pub(super) fn try_cas_result(result: pb::CasResult) -> Result<CasResult> {
    Ok(CasResult {
        applied: result.applied,
        current: result.current.map(try_object_route).transpose()?,
        delete_version: result.delete_version.map(RouteVersion),
        version_floor: result.version_floor.map(RouteVersion),
    })
}

pub(super) fn pb_segment_reservation(reservation: &SegmentReservation) -> pb::SegmentReservation {
    pb::SegmentReservation {
        publication: encode_publication(reservation.publication.as_ref()),
        owner: Some(pb_runtime_id(&reservation.owner)),
        segment_name: reservation.segment_name.0.clone(),
        offset_bytes: reservation.offset_bytes,
        length_bytes: reservation.length_bytes,
        allocation_id: reservation.allocation_id,
    }
}

pub(super) fn try_segment_reservation(
    reservation: pb::SegmentReservation,
) -> Result<SegmentReservation> {
    let owner = reservation
        .owner
        .as_ref()
        .map(try_runtime_id)
        .transpose()?
        .ok_or_else(|| {
            StoreError::Transport("control plane segment reservation is missing owner".to_string())
        })?;
    Ok(SegmentReservation {
        publication: decode_publication(&reservation.publication)?,
        owner,
        segment_name: SegmentName::new(reservation.segment_name),
        offset_bytes: reservation.offset_bytes,
        length_bytes: reservation.length_bytes,
        allocation_id: reservation.allocation_id,
    })
}

pub(super) fn encode_publication(
    attempt: Option<&mooncake_store_core::PublicationAttempt>,
) -> Vec<u8> {
    use prost::Message;
    attempt
        .map(|attempt| pb_publication_attempt(attempt).encode_to_vec())
        .unwrap_or_default()
}

pub(super) fn decode_publication(
    bytes: &[u8],
) -> Result<Option<mooncake_store_core::PublicationAttempt>> {
    use prost::Message;
    if bytes.is_empty() {
        return Ok(None);
    }
    let attempt = pb::PublicationAttemptRpc::decode(bytes)
        .map_err(|error| StoreError::Transport(format!("invalid publication identity: {error}")))?;
    take_publication_attempt(attempt).map(Some)
}

fn take_publication_attempt(
    attempt: pb::PublicationAttemptRpc,
) -> Result<mooncake_store_core::PublicationAttempt> {
    let required = |runtime: Option<pb::ClientRuntimeId>, field: &str| {
        runtime.ok_or_else(|| {
            StoreError::Transport(format!("publication identity is missing {field}"))
        })
    };
    // Consume the decoded strings rather than allocating each runtime a second time.
    let runtime = |runtime: pb::ClientRuntimeId| ClientRuntimeId {
        stable_id: ClientStableId::new(runtime.stable_id),
        epoch: ClientEpoch(runtime.epoch),
    };
    Ok(mooncake_store_core::PublicationAttempt::new(
        mooncake_store_core::PublicationAttemptFields {
            publisher: runtime(required(attempt.publisher, "publisher")?),
            sequence: attempt.sequence,
            key: ObjectKey::new(attempt.key),
            primary: runtime(required(attempt.primary, "primary")?),
            receivers: attempt.receivers.into_iter().map(runtime).collect(),
            permits: attempt
                .permits
                .into_iter()
                .map(|permit| {
                    Ok(mooncake_store_core::PublicationReceiverPermit {
                        receiver: runtime(required(permit.receiver, "permit receiver")?),
                        enrollment: permit.enrollment,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        },
    ))
}

fn pb_publication_permit(
    value: &mooncake_store_core::PublicationReceiverPermit,
) -> pb::PublicationReceiverPermitRpc {
    pb::PublicationReceiverPermitRpc {
        receiver: Some(pb_runtime_id(&value.receiver)),
        enrollment: value.enrollment,
    }
}

fn pb_publication_attempt(
    value: &mooncake_store_core::PublicationAttempt,
) -> pb::PublicationAttemptRpc {
    pb::PublicationAttemptRpc {
        publisher: Some(pb_runtime_id(&value.publisher)),
        sequence: value.sequence,
        key: value.key.0.clone(),
        primary: Some(pb_runtime_id(&value.primary)),
        receivers: value.receivers.iter().map(pb_runtime_id).collect(),
        permits: value.permits.iter().map(pb_publication_permit).collect(),
    }
}

fn pb_publication_revoke(
    value: &mooncake_store_core::PublicationRevokeRequest,
) -> pb::PublicationRevokeRequestRpc {
    pb::PublicationRevokeRequestRpc {
        attempt: Some(pb_publication_attempt(&value.attempt)),
        allocation: Some(pb_allocation_route_identity(&value.allocation)),
        closed_receivers: value.closed_receivers.iter().map(pb_runtime_id).collect(),
        registration_generation: value.registration_generation,
        receiver_permits: value
            .receiver_permits
            .iter()
            .map(pb_publication_permit)
            .collect(),
        superseded_before: value.superseded_before.map(|version| version.0),
    }
}

fn pb_publication_registration(
    value: &mooncake_store_core::PublicationReceiverRegistration,
) -> pb::PublicationRegistrationRpc {
    pb::PublicationRegistrationRpc {
        attempt: Some(pb_publication_attempt(&value.attempt)),
        receiver: Some(pb_runtime_id(&value.receiver)),
        permit: Some(pb_publication_permit(&value.permit)),
        allocations: value
            .allocations
            .iter()
            .map(pb_allocation_route_identity)
            .collect(),
        generation: value.generation,
    }
}

pub(super) fn encode_publication_control(
    value: &mooncake_store_core::PublicationControl,
) -> Vec<u8> {
    use mooncake_store_core::PublicationControl as Control;
    use pb::publication_operation_rpc::Operation;
    use prost::Message;
    pb::PublicationOperationRpc {
        operation: Some(match value {
            Control::PruneExisting { expected, next } => {
                Operation::PruneExisting(pb_publication_prune(expected, next.as_deref()))
            }
            Control::Enroll { publisher } => Operation::Enroll(pb_runtime_id(publisher)),
            Control::Revoke(value) => Operation::Revoke(pb_publication_revoke(value)),
            Control::MarkerProof { request, expected } => {
                Operation::MarkerProof(pb::PublicationMarkerRequestRpc {
                    request: Some(pb_publication_revoke(request)),
                    expected: expected.0,
                })
            }
            Control::RegisterReceiver(value) => {
                Operation::RegisterReceiver(pb_publication_registration(value))
            }
            Control::AdmitReceiver(value) => {
                Operation::AdmitReceiver(pb_publication_registration(value))
            }
            Control::Finalize { request, released } => {
                Operation::Finalize(pb::PublicationFinalizeRpc {
                    request: Some(pb_publication_revoke(request)),
                    released: *released,
                })
            }
        }),
    }
    .encode_to_vec()
}

pub(super) fn encode_publication_control_reply(
    value: &mooncake_store_core::PublicationControlReply,
) -> Vec<u8> {
    use mooncake_store_core::PublicationControlReply as Reply;
    use pb::publication_reply_rpc::Reply as WireReply;
    use prost::Message;
    pb::PublicationReplyRpc {
        reply: Some(match value {
            Reply::Pruned(result) => WireReply::Pruned(pb_publication_pruned(result)),
            Reply::Enrolled(value) => WireReply::Enrolled(pb_publication_permit(value)),
            Reply::Revoked(value) => WireReply::Revoked(pb::PublicationRevokeReplyRpc {
                reference: Some(pb::PublicationReferenceRpc {
                    state: match value.reference.state {
                        AllocationRouteReferenceState::Absent => 1,
                        AllocationRouteReferenceState::Exact => 2,
                        AllocationRouteReferenceState::Conflict => 3,
                    },
                    route_key: value.reference.route_key.as_ref().map(|key| key.0.clone()),
                }),
                registered_receivers: value
                    .registered_receivers
                    .iter()
                    .map(pb_runtime_id)
                    .collect(),
                registration_closed: value.registration_closed,
                registration_generation: value.registration_generation,
                receiver_permits: value
                    .receiver_permits
                    .iter()
                    .map(pb_publication_permit)
                    .collect(),
            }),
            Reply::MarkerProof(value) => WireReply::MarkerProof(pb::PublicationMarkerReplyRpc {
                version: value.map(|version| version.0),
            }),
            Reply::Registered(value) => WireReply::Registered(pb_publication_registration(value)),
            Reply::Admitted => WireReply::Admitted(pb::PublicationAckRpc {}),
            Reply::Finalized => WireReply::Finalized(pb::PublicationAckRpc {}),
        }),
    }
    .encode_to_vec()
}

fn publication_required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| StoreError::Transport(format!("publication control is missing {field}")))
}

// Recovery compares the entire value, including optional-field presence.
// Reuse its lossless serde representation instead of the normalized route RPC shape.
fn pb_publication_prune(
    expected: &ObjectRoute,
    next: Option<&ObjectRoute>,
) -> pb::PublicationPruneExistingRpc {
    pb::PublicationPruneExistingRpc {
        expected_json: serde_json::to_vec(expected).expect("object route serialization"),
        next_json: next.map(|route| serde_json::to_vec(route).expect("object route serialization")),
    }
}

fn pb_publication_pruned(result: &CasResult) -> pb::PublicationPrunedRpc {
    pb::PublicationPrunedRpc {
        result_json: serde_json::to_vec(result).expect("CAS result serialization"),
    }
}

fn take_publication_runtime(value: pb::ClientRuntimeId) -> ClientRuntimeId {
    ClientRuntimeId {
        stable_id: ClientStableId::new(value.stable_id),
        epoch: ClientEpoch(value.epoch),
    }
}

fn take_publication_permit(
    value: pb::PublicationReceiverPermitRpc,
) -> Result<mooncake_store_core::PublicationReceiverPermit> {
    Ok(mooncake_store_core::PublicationReceiverPermit {
        receiver: take_publication_runtime(publication_required(
            value.receiver,
            "permit receiver",
        )?),
        enrollment: value.enrollment,
    })
}

fn take_publication_allocation(
    value: pb::AllocationRouteIdentity,
) -> Result<AllocationRouteIdentity> {
    // Preserve the former JSON value domain; the authority owns semantic checks.
    Ok(AllocationRouteIdentity {
        owner: take_publication_runtime(publication_required(value.owner, "allocation owner")?),
        segment_name: SegmentName::new(value.segment_name),
        offset_bytes: value.offset_bytes,
        length_bytes: value.length_bytes,
        allocation_id: value.allocation_id,
    })
}

fn take_publication_revoke(
    value: pb::PublicationRevokeRequestRpc,
) -> Result<mooncake_store_core::PublicationRevokeRequest> {
    Ok(mooncake_store_core::PublicationRevokeRequest {
        attempt: take_publication_attempt(publication_required(value.attempt, "attempt")?)?,
        allocation: take_publication_allocation(publication_required(
            value.allocation,
            "allocation",
        )?)?,
        closed_receivers: value
            .closed_receivers
            .into_iter()
            .map(take_publication_runtime)
            .collect(),
        registration_generation: value.registration_generation,
        receiver_permits: value
            .receiver_permits
            .into_iter()
            .map(take_publication_permit)
            .collect::<Result<_>>()?,
        superseded_before: value.superseded_before.map(RouteVersion),
    })
}

fn take_publication_registration(
    value: pb::PublicationRegistrationRpc,
) -> Result<mooncake_store_core::PublicationReceiverRegistration> {
    Ok(mooncake_store_core::PublicationReceiverRegistration {
        attempt: take_publication_attempt(publication_required(value.attempt, "attempt")?)?,
        receiver: take_publication_runtime(publication_required(value.receiver, "receiver")?),
        permit: take_publication_permit(publication_required(value.permit, "permit")?)?,
        allocations: value
            .allocations
            .into_iter()
            .map(take_publication_allocation)
            .collect::<Result<_>>()?,
        generation: value.generation,
    })
}

pub(super) fn decode_publication_control(
    bytes: &[u8],
) -> Result<mooncake_store_core::PublicationControl> {
    use mooncake_store_core::PublicationControl as Control;
    use pb::publication_operation_rpc::Operation;
    use prost::Message;
    let value = pb::PublicationOperationRpc::decode(bytes)
        .map_err(|error| StoreError::Transport(format!("invalid publication control: {error}")))?;
    Ok(
        match publication_required(value.operation, "operation variant")? {
            Operation::PruneExisting(value) => Control::PruneExisting {
                expected: Box::new(serde_json::from_slice(&value.expected_json).map_err(
                    |error| StoreError::Transport(format!("invalid prune expected route: {error}")),
                )?),
                next: value
                    .next_json
                    .map(|bytes| serde_json::from_slice(&bytes).map(Box::new))
                    .transpose()
                    .map_err(|error| {
                        StoreError::Transport(format!("invalid prune next route: {error}"))
                    })?,
            },
            Operation::Enroll(value) => Control::Enroll {
                publisher: take_publication_runtime(value),
            },
            Operation::Revoke(value) => Control::Revoke(take_publication_revoke(value)?),
            Operation::MarkerProof(value) => Control::MarkerProof {
                request: take_publication_revoke(publication_required(
                    value.request,
                    "marker request",
                )?)?,
                expected: RouteVersion(value.expected),
            },
            Operation::RegisterReceiver(value) => {
                Control::RegisterReceiver(take_publication_registration(value)?)
            }
            Operation::AdmitReceiver(value) => {
                Control::AdmitReceiver(take_publication_registration(value)?)
            }
            Operation::Finalize(value) => Control::Finalize {
                request: take_publication_revoke(publication_required(
                    value.request,
                    "finalize request",
                )?)?,
                released: value.released,
            },
        },
    )
}

pub(super) fn decode_publication_control_reply(
    bytes: &[u8],
) -> Result<mooncake_store_core::PublicationControlReply> {
    use mooncake_store_core::{PublicationControlReply as Reply, PublicationRevokeReply};
    use pb::publication_reply_rpc::Reply as WireReply;
    use prost::Message;
    let value = pb::PublicationReplyRpc::decode(bytes).map_err(|error| {
        StoreError::Transport(format!("invalid publication control reply: {error}"))
    })?;
    Ok(match publication_required(value.reply, "reply variant")? {
        WireReply::Pruned(value) => Reply::Pruned(Box::new(
            serde_json::from_slice(&value.result_json)
                .map_err(|error| StoreError::Transport(format!("invalid prune result: {error}")))?,
        )),
        WireReply::Enrolled(value) => Reply::Enrolled(take_publication_permit(value)?),
        WireReply::Revoked(value) => {
            let reference = publication_required(value.reference, "reference")?;
            let state = match reference.state {
                1 => AllocationRouteReferenceState::Absent,
                2 => AllocationRouteReferenceState::Exact,
                3 => AllocationRouteReferenceState::Conflict,
                _ => {
                    return Err(StoreError::Transport(
                        "invalid publication reference state".to_string(),
                    ))
                }
            };
            Reply::Revoked(PublicationRevokeReply {
                reference: AllocationRouteReference {
                    state,
                    route_key: reference.route_key.map(ObjectKey::new),
                },
                registered_receivers: value
                    .registered_receivers
                    .into_iter()
                    .map(take_publication_runtime)
                    .collect(),
                registration_closed: value.registration_closed,
                registration_generation: value.registration_generation,
                receiver_permits: value
                    .receiver_permits
                    .into_iter()
                    .map(take_publication_permit)
                    .collect::<Result<_>>()?,
            })
        }
        WireReply::MarkerProof(value) => Reply::MarkerProof(value.version.map(RouteVersion)),
        WireReply::Registered(value) => Reply::Registered(take_publication_registration(value)?),
        WireReply::Admitted(_) => Reply::Admitted,
        WireReply::Finalized(_) => Reply::Finalized,
    })
}

#[cfg(test)]
mod publication_control_codec_tests {
    use super::*;
    use mooncake_store_core::{
        PublicationAttempt, PublicationAttemptFields, PublicationControl as Control,
        PublicationControlReply as Reply, PublicationReceiverPermit,
        PublicationReceiverRegistration, PublicationRevokeReply, PublicationRevokeRequest,
    };
    use proptest::prelude::*;
    use prost::Message;

    fn values(number: u64, text: String, count: usize, some: bool) -> (Vec<Control>, Vec<Reply>) {
        let runtime = ClientRuntimeId::new(text.clone(), ClientEpoch(number));
        let permit = PublicationReceiverPermit {
            receiver: runtime.clone(),
            enrollment: number,
        };
        let receivers = (0..count)
            .map(|index| {
                if index % 2 == 0 {
                    runtime.clone()
                } else {
                    ClientRuntimeId::new(
                        format!("{text}:{index}"),
                        ClientEpoch(number.wrapping_add(index as u64)),
                    )
                }
            })
            .collect::<Vec<_>>();
        let permits = receivers
            .iter()
            .map(|receiver| PublicationReceiverPermit {
                receiver: receiver.clone(),
                enrollment: receiver.epoch.0,
            })
            .collect::<Vec<_>>();
        let attempt = PublicationAttempt::new(PublicationAttemptFields {
            publisher: runtime.clone(),
            sequence: number,
            key: ObjectKey::new(text.clone()),
            primary: runtime.clone(),
            receivers: receivers.clone(),
            permits: permits.clone(),
        });
        let allocation = AllocationRouteIdentity {
            owner: runtime.clone(),
            segment_name: SegmentName::new(text.clone()),
            offset_bytes: number,
            length_bytes: number,
            allocation_id: number,
        };
        let request = PublicationRevokeRequest {
            attempt: attempt.clone(),
            allocation: allocation.clone(),
            closed_receivers: receivers.clone(),
            registration_generation: number,
            receiver_permits: permits.clone(),
            superseded_before: some.then_some(RouteVersion(number)),
        };
        let registration = PublicationReceiverRegistration {
            attempt,
            receiver: runtime.clone(),
            permit: permit.clone(),
            allocations: (0..count)
                .map(|index| {
                    let mut value = allocation.clone();
                    value.offset_bytes = number.wrapping_add((index % 2) as u64);
                    value
                })
                .collect(),
            generation: number,
        };
        let route = ObjectRoute {
            key: ObjectKey::new(text.clone()),
            namespace: some.then(|| mooncake_store_core::NamespaceScope {
                tenant: text.clone(),
                domain: text.clone(),
                object_set: text.clone(),
            }),
            logical_key: some.then(|| text.clone()),
            canonical_key: some.then(|| text.clone()),
            sharing_scope: some.then(|| text.clone()),
            qos_tier: some.then(|| text.clone()),
            version: RouteVersion(number),
            state: mooncake_store_core::RouteState::Active,
            compatibility: mooncake_store_core::CompatibilityDescriptor::default(),
            replicas: vec![
                ReplicaRoute {
                    owner: runtime.clone(),
                    segment_name: SegmentName::new(text.clone()),
                    allocation_id: some.then_some(number),
                    publication: Some(registration.attempt.clone()),
                    offset: some.then_some(number),
                    segment_offset: number,
                    length: number,
                    checksum: some.then_some(number),
                    tier: mooncake_store_core::ReplicaTier::Dram,
                    priority: number as u16,
                };
                count
            ],
            cold_backing: None,
            nof_backing: None,
        };
        let operations = vec![
            Control::Enroll {
                publisher: runtime.clone(),
            },
            Control::Revoke(request.clone()),
            Control::MarkerProof {
                request: request.clone(),
                expected: RouteVersion(number),
            },
            Control::RegisterReceiver(registration.clone()),
            Control::AdmitReceiver(registration.clone()),
            Control::Finalize {
                request,
                released: some,
            },
            Control::PruneExisting {
                expected: Box::new(route.clone()),
                next: some.then(|| Box::new(route.clone())),
            },
        ];
        let mut replies = vec![
            Reply::Enrolled(permit.clone()),
            Reply::MarkerProof(some.then_some(RouteVersion(number))),
            Reply::Registered(registration),
            Reply::Admitted,
            Reply::Finalized,
            Reply::Pruned(Box::new(CasResult {
                applied: some,
                current: some.then(|| route.clone()),
                delete_version: some.then_some(RouteVersion(number)),
                version_floor: some.then_some(RouteVersion(number)),
            })),
        ];
        for state in [
            AllocationRouteReferenceState::Absent,
            AllocationRouteReferenceState::Exact,
            AllocationRouteReferenceState::Conflict,
        ] {
            replies.push(Reply::Revoked(PublicationRevokeReply {
                reference: AllocationRouteReference {
                    state,
                    route_key: some.then(|| ObjectKey::new(text.clone())),
                },
                registered_receivers: receivers.clone(),
                registration_closed: some,
                registration_generation: number,
                receiver_permits: permits.clone(),
            }));
        }
        (operations, replies)
    }

    fn assert_values(number: u64, text: String, count: usize, some: bool) {
        let (operations, replies) = values(number, text, count, some);
        for operation in operations {
            let legacy: Control =
                serde_json::from_slice(&serde_json::to_vec(&operation).unwrap()).unwrap();
            let wire = encode_publication_control(&operation);
            assert_eq!(decode_publication_control(&wire).unwrap(), legacy);
            assert!(decode_publication_control(&serde_json::to_vec(&operation).unwrap()).is_err());
        }
        for reply in replies {
            let legacy: Reply =
                serde_json::from_slice(&serde_json::to_vec(&reply).unwrap()).unwrap();
            let wire = encode_publication_control_reply(&reply);
            assert_eq!(decode_publication_control_reply(&wire).unwrap(), legacy);
            assert!(
                decode_publication_control_reply(&serde_json::to_vec(&reply).unwrap()).is_err()
            );
        }
    }

    #[test]
    fn publication_control_all_variants_preserve_json_values_and_presence() {
        for number in [0, 1, 127, 128, 16383, 16384, u64::MAX] {
            for text in [String::new(), "\0租户::δ🚀".to_string(), "x".repeat(128)] {
                for some in [false, true] {
                    assert_values(number, text.clone(), 3, some);
                }
            }
        }
        assert_eq!(
            encode_publication_control_reply(&Reply::Admitted),
            vec![42, 0]
        );
        assert_eq!(
            encode_publication_control_reply(&Reply::Finalized),
            vec![50, 0]
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        #[test]
        fn publication_control_random_values_preserve_legacy_json(number in any::<u64>(), text in any::<String>(), count in 0usize..5, some in any::<bool>()) {
            assert_values(number, text, count, some);
        }
    }

    #[test]
    fn publication_control_missing_unknown_and_malformed_never_become_success() {
        use pb::publication_operation_rpc::Operation;
        use pb::publication_reply_rpc::Reply as WireReply;
        for invalid in [Vec::new(), vec![0xff], vec![0x7a, 0]] {
            assert!(decode_publication_control(&invalid).is_err());
            assert!(decode_publication_control_reply(&invalid).is_err());
        }
        let (operations, _) = values(1, "key".to_string(), 1, true);
        let Control::Revoke(request) = &operations[1] else {
            unreachable!()
        };
        let mut no_attempt = pb_publication_revoke(request);
        no_attempt.attempt = None;
        let mut no_allocation = pb_publication_revoke(request);
        no_allocation.allocation = None;
        let mut no_owner = pb_publication_revoke(request);
        no_owner.allocation.as_mut().unwrap().owner = None;
        let mut no_receiver = pb_publication_revoke(request);
        no_receiver.receiver_permits[0].receiver = None;
        let mut no_publisher = pb_publication_revoke(request);
        no_publisher.attempt.as_mut().unwrap().publisher = None;
        let mut no_primary = pb_publication_revoke(request);
        no_primary.attempt.as_mut().unwrap().primary = None;
        let mut no_attempt_permit = pb_publication_revoke(request);
        no_attempt_permit.attempt.as_mut().unwrap().permits[0].receiver = None;
        let mut invalid = vec![
            no_attempt,
            no_allocation,
            no_owner,
            no_receiver,
            no_publisher,
            no_primary,
            no_attempt_permit,
        ]
        .into_iter()
        .map(Operation::Revoke)
        .collect::<Vec<_>>();
        invalid.push(Operation::MarkerProof(
            pb::PublicationMarkerRequestRpc::default(),
        ));
        invalid.push(Operation::PruneExisting(
            pb::PublicationPruneExistingRpc::default(),
        ));
        invalid.push(Operation::Finalize(pb::PublicationFinalizeRpc::default()));
        let Control::RegisterReceiver(registered) = &operations[3] else {
            unreachable!()
        };
        let mut missing = pb_publication_registration(registered);
        missing.attempt = None;
        invalid.push(Operation::RegisterReceiver(missing));
        let mut missing = pb_publication_registration(registered);
        missing.receiver = None;
        invalid.push(Operation::AdmitReceiver(missing));
        let mut missing = pb_publication_registration(registered);
        missing.permit = None;
        invalid.push(Operation::RegisterReceiver(missing));
        for operation in invalid {
            assert!(decode_publication_control(
                &pb::PublicationOperationRpc {
                    operation: Some(operation)
                }
                .encode_to_vec()
            )
            .is_err());
        }
        for state in [0, 4, i32::MAX, -1] {
            let reply = pb::PublicationReplyRpc {
                reply: Some(WireReply::Revoked(pb::PublicationRevokeReplyRpc {
                    reference: Some(pb::PublicationReferenceRpc {
                        state,
                        route_key: None,
                    }),
                    ..Default::default()
                })),
            };
            assert!(decode_publication_control_reply(&reply.encode_to_vec()).is_err());
        }
        for reply in [
            WireReply::Enrolled(pb::PublicationReceiverPermitRpc::default()),
            WireReply::Revoked(pb::PublicationRevokeReplyRpc::default()),
            WireReply::Registered(pb::PublicationRegistrationRpc::default()),
            WireReply::Pruned(pb::PublicationPrunedRpc::default()),
        ] {
            assert!(decode_publication_control_reply(
                &pb::PublicationReplyRpc { reply: Some(reply) }.encode_to_vec()
            )
            .is_err());
        }
        let mut truncated = encode_publication_control(&operations[1]);
        truncated.pop();
        assert!(decode_publication_control(&truncated).is_err());
    }
}

#[cfg(test)]
mod publication_codec_tests {
    use super::*;
    use mooncake_store_core::{PublicationAttempt, PublicationReceiverPermit};
    use proptest::prelude::*;
    use prost::Message;

    fn assert_publication_roundtrip(attempt: Option<&PublicationAttempt>) {
        let bytes = encode_publication(attempt);
        assert_eq!(decode_publication(&bytes).unwrap().as_ref(), attempt);
    }

    #[test]
    fn publication_identity_wire_preserves_defaults_and_boundaries() {
        assert_publication_roundtrip(None);
        let mut reordered = attempt();
        assert_publication_roundtrip(Some(&reordered));
        reordered.receivers.reverse();
        reordered.permits.reverse();
        assert_publication_roundtrip(Some(&reordered));
        let empty = ClientRuntimeId::new("", ClientEpoch(0));
        let mut value = PublicationAttempt::new(mooncake_store_core::PublicationAttemptFields {
            publisher: empty.clone(),
            sequence: 0,
            key: ObjectKey::new(""),
            primary: empty.clone(),
            receivers: Vec::new(),
            permits: Vec::new(),
        });
        assert_publication_roundtrip(Some(&value));
        assert_eq!(encode_publication(Some(&value)), vec![0x0a, 0, 0x22, 0]);
        value.receivers.push(empty.clone());
        value.permits.push(PublicationReceiverPermit {
            receiver: empty,
            enrollment: 0,
        });
        assert_publication_roundtrip(Some(&value));
        assert_eq!(
            encode_publication(Some(&value)),
            vec![0x0a, 0, 0x22, 0, 0x2a, 0, 0x32, 2, 0x0a, 0]
        );
        for number in [0, 1, 127, 128, 16383, 16384, u32::MAX as u64, u64::MAX] {
            for name in [
                String::new(),
                "\0".to_owned(),
                "租户::δ🚀".to_owned(),
                "x".repeat(127),
                "x".repeat(128),
                "x".repeat(16383),
                "x".repeat(16384),
            ] {
                let runtime = ClientRuntimeId::new(name.clone(), ClientEpoch(number));
                value = PublicationAttempt::new(mooncake_store_core::PublicationAttemptFields {
                    publisher: runtime.clone(),
                    sequence: number,
                    key: ObjectKey::new(name),
                    primary: runtime.clone(),
                    receivers: vec![runtime.clone(), runtime.clone()],
                    permits: vec![
                        PublicationReceiverPermit {
                            receiver: runtime,
                            enrollment: number
                        };
                        2
                    ],
                });
                assert_publication_roundtrip(Some(&value));
                value.receivers.reverse();
                value.permits.reverse();
                assert_publication_roundtrip(Some(&value));
            }
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]
        #[test]
        fn publication_identity_wire_preserves_arbitrary_fields(
            publisher in any::<(String, u64)>(), primary in any::<(String, u64)>(),
            sequence in any::<u64>(), key in any::<String>(),
            receivers in proptest::collection::vec(any::<(String, u64)>(), 0..8),
            permits in proptest::collection::vec(any::<(String, u64, u64)>(), 0..8),
        ) {
            let runtime = |(name, epoch)| ClientRuntimeId::new(name, ClientEpoch(epoch));
            let attempt = PublicationAttempt::new(mooncake_store_core::PublicationAttemptFields {
                publisher: runtime(publisher), primary: runtime(primary), sequence,
                key: ObjectKey::new(key), receivers: receivers.into_iter().map(runtime).collect(),
                permits: permits.into_iter().map(|(name, epoch, enrollment)| PublicationReceiverPermit {
                    receiver: runtime((name, epoch)), enrollment,
                }).collect(),
            });
            prop_assert_eq!(decode_publication(&encode_publication(Some(&attempt))).unwrap(), Some(attempt));
        }
    }

    fn attempt() -> PublicationAttempt {
        let primary = ClientRuntimeId::new("primary", ClientEpoch(u64::MAX));
        let secondary = ClientRuntimeId::new("secondary", ClientEpoch(u64::MAX - 1));
        PublicationAttempt::new(mooncake_store_core::PublicationAttemptFields {
            publisher: ClientRuntimeId::new("publisher", ClientEpoch(u64::MAX - 2)),
            sequence: u64::MAX,
            key: ObjectKey::new("tenant::publication-identity"),
            primary: primary.clone(),
            receivers: vec![secondary.clone(), primary.clone()],
            permits: vec![
                PublicationReceiverPermit {
                    receiver: primary,
                    enrollment: u64::MAX,
                },
                PublicationReceiverPermit {
                    receiver: secondary,
                    enrollment: u64::MAX - 3,
                },
            ],
        })
    }

    #[test]
    fn publication_identity_protobuf_preserves_every_field_and_order() {
        let original = attempt();
        let encoded = encode_publication(Some(&original));
        assert_eq!(
            decode_publication(&encoded).unwrap(),
            Some(original.clone())
        );
        assert_eq!(decode_publication(&encode_publication(None)).unwrap(), None);
        let persisted = serde_json::to_vec(&original).unwrap();
        assert_eq!(
            serde_json::from_slice::<PublicationAttempt>(&persisted).unwrap(),
            original
        );
        assert!(encoded.len() < persisted.len());
    }

    #[test]
    fn publication_identity_protobuf_rejects_missing_nested_identity() {
        let original =
            pb::PublicationAttemptRpc::decode(encode_publication(Some(&attempt())).as_slice())
                .unwrap();
        let mut publisher = original.clone();
        publisher.publisher = None;
        let mut primary = original.clone();
        primary.primary = None;
        let mut permit = original;
        permit.permits[0].receiver = None;
        for invalid in [publisher, primary, permit] {
            assert!(matches!(
                decode_publication(&invalid.encode_to_vec()),
                Err(StoreError::Transport(_))
            ));
        }
    }

    #[test]
    fn publication_identity_nonempty_invalid_wire_never_becomes_absent() {
        for invalid in [vec![0xff], serde_json::to_vec(&attempt()).unwrap()] {
            assert!(matches!(
                decode_publication(&invalid),
                Err(StoreError::Transport(_))
            ));
        }
    }
}
