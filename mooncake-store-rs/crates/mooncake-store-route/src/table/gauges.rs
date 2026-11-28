use std::collections::BTreeMap;

use mooncake_store_core::{ClientRuntimeId, ObjectKey, ObjectRoute, ReplicaTier};

/// Aggregate counts of locally retained authority entries, including mirrors.
///
/// Distinct authority tables contribute independently, even for the same object
/// key. Counts include non-Tombstone routes and retain duplicate replica entries.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteGaugeSnapshot {
    /// Route counts by tenant, using the existing scoped-key tenant convention.
    pub objects_by_tenant: BTreeMap<String, u64>,
    /// Replica counts by owner and tier (`dram`, `nvme`, `file`, or `unknown`).
    pub replicas_by_runtime: BTreeMap<ClientRuntimeId, BTreeMap<&'static str, u64>>,
}

impl RouteGaugeSnapshot {
    pub(crate) fn update(
        &mut self,
        key: &ObjectKey,
        old: Option<&ObjectRoute>,
        next: Option<&ObjectRoute>,
    ) {
        match (old.is_some(), next.is_some()) {
            (false, true) => {
                let tenant = object_key_tenant(&key.0);
                if let Some(count) = self.objects_by_tenant.get_mut(tenant) {
                    *count += 1;
                } else {
                    self.objects_by_tenant.insert(tenant.to_owned(), 1);
                }
            }
            (true, false) => {
                let tenant = object_key_tenant(&key.0);
                let count = self
                    .objects_by_tenant
                    .get_mut(tenant)
                    .expect("missing route tenant contribution");
                *count -= 1;
                if *count == 0 {
                    self.objects_by_tenant.remove(tenant);
                }
            }
            _ => {}
        }

        // Version, placement offsets and checksums do not affect these gauges.
        if old.zip(next).is_some_and(|(old, next)| {
            old.replicas.len() == next.replicas.len()
                && old
                    .replicas
                    .iter()
                    .zip(&next.replicas)
                    .all(|(old, next)| old.owner == next.owner && old.tier == next.tier)
        }) {
            return;
        }

        if let Some(route) = old {
            for replica in &route.replicas {
                let tiers = self
                    .replicas_by_runtime
                    .get_mut(&replica.owner)
                    .expect("missing route runtime contribution");
                let tier = replica_tier_label(replica.tier);
                let count = tiers
                    .get_mut(tier)
                    .expect("missing route tier contribution");
                *count -= 1;
                if *count == 0 {
                    tiers.remove(tier);
                }
                if tiers.is_empty() {
                    self.replicas_by_runtime.remove(&replica.owner);
                }
            }
        }
        if let Some(route) = next {
            for replica in &route.replicas {
                let tier = replica_tier_label(replica.tier);
                match self.replicas_by_runtime.get_mut(&replica.owner) {
                    Some(tiers) => *tiers.entry(tier).or_default() += 1,
                    None => {
                        self.replicas_by_runtime
                            .insert(replica.owner.clone(), BTreeMap::from([(tier, 1)]));
                    }
                }
            }
        }
    }

    pub(crate) fn merge(&mut self, other: &Self) {
        for (tenant, count) in &other.objects_by_tenant {
            *self.objects_by_tenant.entry(tenant.clone()).or_default() += *count;
        }
        for (runtime, tiers) in &other.replicas_by_runtime {
            let counts = self.replicas_by_runtime.entry(runtime.clone()).or_default();
            for (tier, count) in tiers {
                *counts.entry(*tier).or_default() += *count;
            }
        }
    }
}

fn object_key_tenant(key: &str) -> &str {
    key.split_once("::")
        .or_else(|| key.split_once('/'))
        .map(|(tenant, _)| tenant)
        .unwrap_or("default")
}

fn replica_tier_label(tier: ReplicaTier) -> &'static str {
    match tier {
        ReplicaTier::Dram => "dram",
        ReplicaTier::Nvme => "nvme",
        ReplicaTier::File => "file",
        ReplicaTier::Unknown => "unknown",
    }
}
