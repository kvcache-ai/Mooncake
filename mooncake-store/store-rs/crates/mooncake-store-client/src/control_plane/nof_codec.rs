use super::codec::{pb_runtime_id, try_runtime_id};
use super::*;

pub(super) fn pb_nof_backing_state(state: mooncake_store_core::NofBackingState) -> i32 {
    match state {
        mooncake_store_core::NofBackingState::PendingWrite => {
            pb::NofBackingState::PendingWrite as i32
        }
        mooncake_store_core::NofBackingState::Materialized => {
            pb::NofBackingState::Materialized as i32
        }
        mooncake_store_core::NofBackingState::PendingDelete => {
            pb::NofBackingState::PendingDelete as i32
        }
    }
}

pub(super) fn try_nof_backing_state(state: i32) -> Result<mooncake_store_core::NofBackingState> {
    Ok(
        match pb::NofBackingState::try_from(state).unwrap_or(pb::NofBackingState::Unspecified) {
            pb::NofBackingState::PendingWrite | pb::NofBackingState::Unspecified => {
                mooncake_store_core::NofBackingState::PendingWrite
            }
            pb::NofBackingState::Materialized => mooncake_store_core::NofBackingState::Materialized,
            pb::NofBackingState::PendingDelete => {
                mooncake_store_core::NofBackingState::PendingDelete
            }
        },
    )
}

fn pb_nof_backing_replica(
    replica: &mooncake_store_core::NofBackingReplica,
) -> pb::NofBackingReplica {
    pb::NofBackingReplica {
        owner: Some(pb_runtime_id(&replica.owner)),
        target_id: replica.target_id.clone(),
        object_locator: replica.object_locator.clone(),
    }
}

pub(super) fn pb_nof_backing_route(
    route: &mooncake_store_core::NofBackingRoute,
) -> pb::NofBackingRoute {
    pb::NofBackingRoute {
        owner: Some(pb_runtime_id(&route.owner)),
        target_id: route.target_id.clone(),
        object_locator: route.object_locator.clone(),
        length: route.length,
        checksum: route.checksum,
        state: pb_nof_backing_state(route.state),
        replicas: route.replicas.iter().map(pb_nof_backing_replica).collect(),
    }
}

fn try_nof_backing_replica(
    replica: pb::NofBackingReplica,
) -> Result<mooncake_store_core::NofBackingReplica> {
    let owner = replica
        .owner
        .as_ref()
        .map(try_runtime_id)
        .transpose()?
        .ok_or_else(|| {
            StoreError::Transport("control plane NoF backing replica is missing owner".to_string())
        })?;
    Ok(mooncake_store_core::NofBackingReplica {
        owner,
        target_id: replica.target_id,
        object_locator: replica.object_locator,
    })
}

pub(super) fn try_nof_backing_route(
    route: pb::NofBackingRoute,
) -> Result<mooncake_store_core::NofBackingRoute> {
    let owner = route
        .owner
        .as_ref()
        .map(try_runtime_id)
        .transpose()?
        .ok_or_else(|| {
            StoreError::Transport("control plane NoF backing is missing owner".to_string())
        })?;
    Ok(mooncake_store_core::NofBackingRoute {
        owner,
        target_id: route.target_id,
        object_locator: route.object_locator,
        length: route.length,
        checksum: route.checksum,
        state: try_nof_backing_state(route.state)?,
        replicas: route
            .replicas
            .into_iter()
            .map(try_nof_backing_replica)
            .collect::<Result<Vec<_>>>()?,
    })
}
