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
        StoreError::InvalidState(message) => (pb::ErrorKind::InvalidState, message),
        StoreError::StaleEpoch(message) => (pb::ErrorKind::StaleEpoch, message),
        StoreError::Unsupported(message) => (pb::ErrorKind::Unsupported, message),
        StoreError::Allocator(message) => (pb::ErrorKind::Allocator, message),
        StoreError::Metadata(message) => (pb::ErrorKind::Metadata, message),
        StoreError::Transport(message) => (pb::ErrorKind::Transport, message),
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
        pb::ErrorKind::InvalidState => StoreError::InvalidState(error.message),
        pb::ErrorKind::StaleEpoch => StoreError::StaleEpoch(error.message),
        pb::ErrorKind::Unsupported => StoreError::Unsupported(error.message),
        pb::ErrorKind::Allocator => StoreError::Allocator(error.message),
        pb::ErrorKind::Metadata => StoreError::Metadata(error.message),
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
            pb::RouteState::Active => mooncake_store_core::RouteState::Active,
            pb::RouteState::Deleting => mooncake_store_core::RouteState::Deleting,
            pb::RouteState::Tombstone | pb::RouteState::Unspecified => {
                mooncake_store_core::RouteState::Tombstone
            }
        },
    )
}

pub(super) fn pb_replica_route(replica: &ReplicaRoute) -> pb::ReplicaRoute {
    pb::ReplicaRoute {
        owner: Some(pb_runtime_id(&replica.owner)),
        segment_name: replica.segment_name.0.clone(),
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
        owner,
        segment_name: SegmentName::new(replica.segment_name),
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
    Ok(ObjectRoute {
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
    })
}

pub(super) fn try_object_route_ref(route: &pb::ObjectRoute) -> Result<ObjectRoute> {
    try_object_route(route.clone())
}

pub(super) fn pb_cas_result(result: &CasResult) -> pb::CasResult {
    pb::CasResult {
        applied: result.applied,
        current: result.current.as_ref().map(pb_object_route),
    }
}

pub(super) fn try_cas_result(result: pb::CasResult) -> Result<CasResult> {
    Ok(CasResult {
        applied: result.applied,
        current: result.current.map(try_object_route).transpose()?,
    })
}

pub(super) fn pb_segment_reservation(reservation: &SegmentReservation) -> pb::SegmentReservation {
    pb::SegmentReservation {
        owner: Some(pb_runtime_id(&reservation.owner)),
        segment_name: reservation.segment_name.0.clone(),
        offset_bytes: reservation.offset_bytes,
        length_bytes: reservation.length_bytes,
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
        owner,
        segment_name: SegmentName::new(reservation.segment_name),
        offset_bytes: reservation.offset_bytes,
        length_bytes: reservation.length_bytes,
    })
}
