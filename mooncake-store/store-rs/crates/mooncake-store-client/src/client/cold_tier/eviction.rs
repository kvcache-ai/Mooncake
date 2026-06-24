use super::super::ObjectRoute;

pub(in super::super) fn should_delete_route_after_last_replica_eviction(
    route: &ObjectRoute,
) -> bool {
    !route
        .cold_backing
        .as_ref()
        .is_some_and(|backing| backing.state == mooncake_store_core::ColdBackingState::Materialized)
}

pub(in super::super) fn route_after_replica_eviction(
    route: &ObjectRoute,
    replica_index: usize,
    delete_empty_route: bool,
) -> Option<ObjectRoute> {
    if route.replicas.len() == 1 && delete_empty_route {
        return None;
    }

    let mut next = route.clone();
    next.version = next.version.next();

    if route.replicas.len() == 1 {
        next.replicas = Vec::new();
        return Some(next);
    }

    next.replicas.remove(replica_index);
    next.replicas.sort_by_key(|replica| replica.priority);
    for (priority, replica) in next.replicas.iter_mut().enumerate() {
        replica.priority = priority as u16;
    }
    Some(next)
}
