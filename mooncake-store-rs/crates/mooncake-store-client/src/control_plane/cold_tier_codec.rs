use super::codec::{pb_runtime_id, try_runtime_id};
use super::*;

pub(super) fn pb_cold_backing_state(state: mooncake_store_core::ColdBackingState) -> i32 {
    match state {
        mooncake_store_core::ColdBackingState::PendingOffload => {
            pb::ColdBackingState::PendingOffload as i32
        }
        mooncake_store_core::ColdBackingState::Materialized => {
            pb::ColdBackingState::Materialized as i32
        }
        mooncake_store_core::ColdBackingState::PendingDelete => {
            pb::ColdBackingState::PendingDelete as i32
        }
    }
}

pub(super) fn try_cold_backing_state(state: i32) -> Result<mooncake_store_core::ColdBackingState> {
    Ok(
        match pb::ColdBackingState::try_from(state).unwrap_or(pb::ColdBackingState::Unspecified) {
            pb::ColdBackingState::PendingOffload | pb::ColdBackingState::Unspecified => {
                mooncake_store_core::ColdBackingState::PendingOffload
            }
            pb::ColdBackingState::Materialized => {
                mooncake_store_core::ColdBackingState::Materialized
            }
            pb::ColdBackingState::PendingDelete => {
                mooncake_store_core::ColdBackingState::PendingDelete
            }
        },
    )
}

pub(super) fn pb_cold_backing_route(
    route: &mooncake_store_core::ColdBackingRoute,
) -> pb::ColdBackingRoute {
    pb::ColdBackingRoute {
        owner: Some(pb_runtime_id(&route.owner)),
        cold_tier_id: route.cold_tier_id.clone(),
        object_locator: route.object_locator.clone(),
        length: route.length,
        checksum: route.checksum,
        state: pb_cold_backing_state(route.state),
    }
}

pub(super) fn try_cold_backing_route(
    route: pb::ColdBackingRoute,
) -> Result<mooncake_store_core::ColdBackingRoute> {
    let owner = route
        .owner
        .as_ref()
        .map(try_runtime_id)
        .transpose()?
        .ok_or_else(|| {
            StoreError::Transport("control plane cold backing is missing owner".to_string())
        })?;
    Ok(mooncake_store_core::ColdBackingRoute {
        owner,
        cold_tier_id: route.cold_tier_id,
        object_locator: route.object_locator,
        length: route.length,
        checksum: route.checksum,
        state: try_cold_backing_state(route.state)?,
        replicas: Vec::new(),
    })
}
