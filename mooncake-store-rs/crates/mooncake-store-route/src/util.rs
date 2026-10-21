use std::fmt::Write as _;

use mooncake_store_core::{ClientLease, ObjectRoute, ReplicaRoute, ReplicaTier, RouteState};

const PER_KEY_DEBUG_SAMPLE_MODULUS: u64 = 128;

pub(crate) fn compatibility_matches(left: &ClientLease, right: &ClientLease) -> bool {
    left.compatibility.is_compatible_with(&right.compatibility)
}

pub(crate) fn route_capable(lease: &ClientLease) -> bool {
    lease
        .endpoints
        .labels
        .get("route")
        .is_some_and(|value| value == "true")
}

pub(crate) fn route_weight(lease: &ClientLease) -> f64 {
    lease
        .endpoints
        .labels
        .get("route_weight")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|weight| *weight > 0.0)
        .unwrap_or(1.0)
}

pub(crate) fn route_read_source(rank: usize, route_topk: usize) -> &'static str {
    if rank == 0 {
        "primary"
    } else if rank < route_topk {
        "mirror"
    } else {
        "fallback"
    }
}

fn route_state_rank(state: RouteState) -> u8 {
    match state {
        RouteState::Active => 0,
        RouteState::Deleting => 1,
        RouteState::Tombstone => 2,
    }
}

fn replica_tier_rank(tier: ReplicaTier) -> u8 {
    match tier {
        ReplicaTier::Dram => 0,
        ReplicaTier::Nvme => 1,
        ReplicaTier::File => 2,
        ReplicaTier::Unknown => 3,
    }
}

pub(crate) fn canonical_route_key(route: &ObjectRoute) -> String {
    let mut key = String::new();
    let _ = write!(
        key,
        "{}|{}|{}|{}|{}|{}|{}|",
        route.key.0,
        route.version.0,
        route_state_rank(route.state),
        route.compatibility.store_api_version,
        route.compatibility.store_api_minor_version,
        route.compatibility.metadata_schema_version,
        route.compatibility.transport_api_version,
    );
    for capability in &route.compatibility.capabilities {
        key.push_str(capability);
        key.push(',');
    }
    key.push('|');
    for replica in &route.replicas {
        append_replica_key(&mut key, replica);
    }
    key
}

fn append_replica_key(key: &mut String, replica: &ReplicaRoute) {
    let offset = replica
        .offset
        .map(|offset| offset.to_string())
        .unwrap_or_else(|| "legacy".to_string());
    let _ = write!(
        key,
        "{}|{}|{}|{}|{}|{}|{}|{};",
        replica.owner,
        replica.segment_name.0,
        offset,
        replica.segment_offset,
        replica.length,
        replica.checksum.unwrap_or_default(),
        replica_tier_rank(replica.tier),
        replica.priority,
    );
}

pub(crate) fn weighted_rendezvous_score(
    namespace: &str,
    key: &str,
    stable_id: &str,
    weight: f64,
) -> f64 {
    let hash = stable_hash(&[namespace, key, stable_id]);
    let unit = ((hash as f64) + 1.0) / ((u64::MAX as f64) + 2.0);
    -unit.ln() / weight.max(f64::MIN_POSITIVE)
}

fn stable_hash(parts: &[&str]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash ^= u64::from(b'|');
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub(crate) fn sampled_per_key_debug_log(parts: &[&str]) -> bool {
    is_per_key_debug_sample(stable_hash(parts), PER_KEY_DEBUG_SAMPLE_MODULUS)
}

fn is_per_key_debug_sample(hash: u64, modulus: u64) -> bool {
    matches!(hash.checked_rem(modulus), Some(0))
}
