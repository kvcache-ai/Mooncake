use std::mem::size_of;

use hashlink::LinkedHashMap;
use mooncake_store_core::{ObjectKey, RouteVersion};

pub(crate) const DEFAULT_DELETE_MARKER_BUDGET_BYTES: usize = 128 * 1024 * 1024;
const MIN_ACCOUNTED_ENTRY_BYTES: usize = 256;
const ACCOUNTING_ALIGNMENT: usize = 64;

#[derive(Clone, Debug)]
struct DeleteMarker {
    version: RouteVersion,
    accounted_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeleteMarkerSnapshot {
    pub(crate) entries: usize,
    pub(crate) accounted_bytes: usize,
    pub(crate) budget_bytes: usize,
}

pub(crate) struct DeleteMarkerStore {
    entries: LinkedHashMap<ObjectKey, DeleteMarker>,
    accounted_bytes: usize,
    budget_bytes: usize,
}

impl Default for DeleteMarkerStore {
    fn default() -> Self {
        Self::new(DEFAULT_DELETE_MARKER_BUDGET_BYTES)
    }
}

impl DeleteMarkerStore {
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self {
            entries: LinkedHashMap::new(),
            accounted_bytes: 0,
            budget_bytes: budget_bytes.max(MIN_ACCOUNTED_ENTRY_BYTES),
        }
    }

    pub(crate) fn get(&self, key: &ObjectKey) -> Option<RouteVersion> {
        self.entries.get(key).map(|marker| marker.version)
    }

    pub(crate) fn get_many(&self, keys: &[ObjectKey]) -> Vec<Option<RouteVersion>> {
        keys.iter().map(|key| self.get(key)).collect()
    }

    pub(crate) fn record(&mut self, key: &ObjectKey, version: RouteVersion) -> usize {
        if self
            .entries
            .get(key)
            .is_some_and(|current| current.version >= version)
        {
            return 0;
        }

        let accounted_bytes = accounted_entry_bytes(key);
        if let Some(previous) = self.entries.insert(
            key.clone(),
            DeleteMarker {
                version,
                accounted_bytes,
            },
        ) {
            self.accounted_bytes = self
                .accounted_bytes
                .saturating_sub(previous.accounted_bytes);
        }
        self.accounted_bytes = self.accounted_bytes.saturating_add(accounted_bytes);

        let mut evicted_count = 0;
        while self.accounted_bytes > self.budget_bytes {
            let Some((_, marker)) = self.entries.pop_front() else {
                break;
            };
            self.accounted_bytes = self.accounted_bytes.saturating_sub(marker.accounted_bytes);
            evicted_count += 1;
        }
        evicted_count
    }

    pub(crate) fn remove_if_older(&mut self, key: &ObjectKey, live_version: RouteVersion) -> bool {
        if self
            .entries
            .get(key)
            .is_none_or(|marker| marker.version >= live_version)
        {
            return false;
        }
        if let Some(marker) = self.entries.remove(key) {
            self.accounted_bytes = self.accounted_bytes.saturating_sub(marker.accounted_bytes);
            return true;
        }
        false
    }

    pub(crate) fn snapshot(&self) -> DeleteMarkerSnapshot {
        DeleteMarkerSnapshot {
            entries: self.entries.len(),
            accounted_bytes: self.accounted_bytes,
            budget_bytes: self.budget_bytes,
        }
    }
}

fn accounted_entry_bytes(key: &ObjectKey) -> usize {
    let raw = key
        .0
        .capacity()
        .saturating_add(size_of::<ObjectKey>())
        .saturating_add(size_of::<DeleteMarker>());
    align_up(raw.max(MIN_ACCOUNTED_ENTRY_BYTES), ACCOUNTING_ALIGNMENT)
}

fn align_up(value: usize, alignment: usize) -> usize {
    (value.saturating_add(alignment.saturating_sub(1)) / alignment).saturating_mul(alignment)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn higher_version_refreshes_fifo_position_and_lower_version_is_ignored() {
        let mut store = DeleteMarkerStore::new(512);
        let a = ObjectKey::new("a");
        let b = ObjectKey::new("b");
        let c = ObjectKey::new("c");

        store.record(&a, RouteVersion(1));
        store.record(&b, RouteVersion(1));
        store.record(&a, RouteVersion(3));
        store.record(&a, RouteVersion(2));
        let evicted_count = store.record(&c, RouteVersion(1));

        assert_eq!(store.get(&a), Some(RouteVersion(3)));
        assert_eq!(evicted_count, 1);
        assert_eq!(store.get(&b), None);
        assert_eq!(store.get(&c), Some(RouteVersion(1)));
    }

    #[test]
    fn storage_is_bounded_and_newer_live_route_removes_marker() {
        let mut store = DeleteMarkerStore::new(512);
        let a = ObjectKey::new("a");
        let b = ObjectKey::new("b");
        let c = ObjectKey::new("c");

        store.record(&a, RouteVersion(1));
        store.record(&b, RouteVersion(2));
        let evicted_count = store.record(&c, RouteVersion(3));

        assert_eq!(evicted_count, 1);
        assert_eq!(store.get(&a), None);
        assert_eq!(store.get(&c), Some(RouteVersion(3)));
        assert!(store.snapshot().accounted_bytes <= 512);
        assert!(!store.remove_if_older(&b, RouteVersion(2)));
        assert!(store.remove_if_older(&b, RouteVersion(3)));
        assert_eq!(store.get(&b), None);
    }

    #[test]
    fn unique_delete_churn_plateaus_at_the_byte_budget() {
        const BUDGET: usize = 256 * 64;
        let mut store = DeleteMarkerStore::new(BUDGET);

        for index in 0..10_000 {
            store.record(
                &ObjectKey::new(format!("churn-key-{index}")),
                RouteVersion(1),
            );
        }

        let snapshot = store.snapshot();
        assert!(snapshot.entries <= 64);
        assert!(snapshot.accounted_bytes <= BUDGET);
        assert_eq!(store.get(&ObjectKey::new("churn-key-0")), None);
        assert_eq!(
            store.get(&ObjectKey::new("churn-key-9999")),
            Some(RouteVersion(1))
        );
    }
}
