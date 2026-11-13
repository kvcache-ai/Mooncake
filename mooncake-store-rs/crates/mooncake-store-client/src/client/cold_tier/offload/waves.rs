use super::*;

fn materialize_groups(
    storage_owner: &StorageOwnerState,
    groups: Vec<&[PendingOffloadMaterialization]>,
    max_parallelism: usize,
) -> Vec<(Result<usize>, Option<StoreError>)> {
    let workers = groups.len().min(max_parallelism.max(1));
    if workers <= 1 {
        return groups
            .into_iter()
            .map(|group| {
                let mut first_error = None;
                let result = storage_owner
                    .materialize_prepared_pending_offload_batch(group, &mut first_error);
                (result, first_error)
            })
            .collect();
    }

    let next = std::sync::atomic::AtomicUsize::new(0);
    let outcomes = std::sync::Mutex::new(Vec::with_capacity(groups.len()));
    std::thread::scope(|scope| {
        for _ in 0..workers {
            let next = &next;
            let groups = &groups;
            let outcomes = &outcomes;
            scope.spawn(move || {
                let mut local_outcomes = Vec::new();
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(group) = groups.get(index) else {
                        break;
                    };
                    let mut first_error = None;
                    let result = storage_owner
                        .materialize_prepared_pending_offload_batch(group, &mut first_error);
                    local_outcomes.push((index, (result, first_error)));
                }
                outcomes
                    .lock()
                    .expect("offload group result lock should succeed")
                    .extend(local_outcomes);
            });
        }
    });

    let mut outcomes = outcomes
        .into_inner()
        .expect("offload group result lock should succeed");
    outcomes.sort_by_key(|(index, _)| *index);
    outcomes.into_iter().map(|(_, outcome)| outcome).collect()
}

pub(super) fn materialize_ready_pending_offload_groups(
    storage_owner: &StorageOwnerState,
    pending: &mut [PendingOffloadMaterialization],
) -> Result<usize> {
    pending.sort_by(|left, right| {
        left.cold_backing
            .cold_tier_id
            .cmp(&right.cold_backing.cold_tier_id)
    });
    let mut groups = Vec::new();
    let mut start = 0usize;
    while start < pending.len() {
        let mut end = start + 1;
        while end < pending.len()
            && pending[end].cold_backing.cold_tier_id == pending[start].cold_backing.cold_tier_id
        {
            end += 1;
        }
        groups.push(&pending[start..end]);
        start = end;
    }

    let outcomes = materialize_groups(
        storage_owner,
        groups,
        storage_owner.cold_tier_devices.offload_parallelism(),
    );
    let mut round_materialized = 0usize;
    let mut first_error = None;
    let mut first_batch_error = None;
    for (result, group_error) in outcomes {
        match result {
            Ok(materialized) => {
                round_materialized = round_materialized.saturating_add(materialized);
            }
            Err(error) => {
                first_batch_error.get_or_insert(error);
            }
        }
        if first_error.is_none() {
            first_error = group_error;
        }
    }
    if let Some(error) = first_batch_error {
        return Err(error);
    }
    if round_materialized == 0 {
        if let Some(error) = first_error {
            return Err(error);
        }
    }
    Ok(round_materialized)
}

pub(super) fn materialize_claimed_pending_offload_waves(
    storage_owner: &StorageOwnerState,
    mut entries: Vec<PendingOffloadEntry>,
) -> Result<usize> {
    let mut materialized = 0usize;
    while !entries.is_empty() {
        let prepare_tracker = OperationTracker::new("cold_offload_prepare_batch");
        let prepared = prepare_pending_offload_entries_partitioned(storage_owner, entries);
        prepare_tracker.finish(
            &prepared,
            prepared
                .as_ref()
                .map(|prepared| prepared.pending.len())
                .unwrap_or_default() as u64,
        );
        let prepared = prepared?;
        let mut pending = prepared.pending;
        let deferred = prepared.admission_deferred;
        if pending.is_empty() {
            retry_admission_deferred(storage_owner, deferred);
            break;
        }
        match materialize_ready_pending_offload_groups(storage_owner, &mut pending) {
            Ok(round_materialized) => {
                materialized = materialized.saturating_add(round_materialized);
            }
            Err(error) => {
                retry_admission_deferred(storage_owner, deferred);
                return Err(error);
            }
        }
        entries = deferred.into_iter().map(|(entry, _)| entry).collect();
    }
    Ok(materialized)
}

pub(super) fn retry_admission_deferred(
    storage_owner: &StorageOwnerState,
    deferred: Vec<(PendingOffloadEntry, Duration)>,
) {
    for (entry, delay) in deferred {
        storage_owner.pending_offloads.retry_after(entry, delay);
    }
}
