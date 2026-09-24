use super::exporter::{
    counter_family, escape, for_cold_runtime_gauge, gauge_family, histogram_family,
    render_cold_tier_io_histogram, render_phase_histogram,
};
use super::registry::{
    ColdRestoreSingleflightKey, ColdTierDeviceStateKey, ColdTierOperationKey,
    ColdTierReclaimKindKey, ColdTierReclaimPolicyRankKey, ColdTierReclaimQosKey,
    ColdTierRuntimeKey, CounterSample, GaugeSample, MetricsSnapshot, BATCH_GET_PATH_ITEMS_TOTAL,
    BATCH_GET_PHASE_DURATION, BATCH_IS_EXIST_DURATION, COLD_PREFETCH_WORKER_DURATION,
    COLD_PREFETCH_WORKER_ITEMS, COLD_READ_BATCH_WALL_DURATION, COLD_RESTORE_BATCH_DURATION,
    COLD_RESTORE_BATCH_ITEMS, COLD_RESTORE_MAX_CONCURRENT_IO_PER_OBJECT,
    COLD_RESTORE_SINGLEFLIGHT_TOTAL, COLD_TIER_DEVICE_CAPACITY_BYTES,
    COLD_TIER_DEVICE_RESERVED_BYTES, COLD_TIER_DEVICE_SCHEDULABLE_TOTAL, COLD_TIER_DEVICE_TOTAL,
    COLD_TIER_DEVICE_USED_BYTES, COLD_TIER_OPERATION_TOTAL,
    COLD_TIER_PENDING_OFFLOAD_ATTEMPTS_TOTAL, COLD_TIER_PENDING_OFFLOAD_DELAYED,
    COLD_TIER_PENDING_OFFLOAD_MAX_ATTEMPTS, COLD_TIER_PENDING_OFFLOAD_READY,
    COLD_TIER_PENDING_OFFLOAD_TOTAL, COLD_TIER_RECLAIM_BY_KIND, COLD_TIER_RECLAIM_BY_POLICY_RANK,
    COLD_TIER_RECLAIM_BY_QOS_TIER, COLD_TIER_RECLAIM_DUE_TOTAL, COLD_TIER_RECLAIM_PENDING_TOTAL,
    COLD_TIER_SSD_READ_DURATION, COLD_TIER_SSD_WRITE_DURATION, EXTENT_STORE_IO_PRIORITY_WAIT,
    EXTENT_STORE_PIPELINE_DURATION, EXTENT_STORE_QUEUE_WAIT, FACADE_PHASE_DURATION,
    IO_URING_OPS_TOTAL, IO_URING_PHASE_DURATION, RDMA_BYTES_TOTAL, RDMA_TRANSFER_DURATION,
    STAGING_POOL_EXHAUSTION_TOTAL, STAGING_POOL_WAIT,
};

pub(crate) fn render_cold_tier_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
    gauge_family(
        output,
        COLD_TIER_DEVICE_TOTAL,
        "Cold tier devices by runtime and state.",
    );
    for GaugeSample { key, value } in &snapshot.cold_tier_device_states {
        let ColdTierDeviceStateKey { runtime, state } = key;
        output.push_str(&format!(
            "{}{{runtime=\"{}\",state=\"{}\"}} {}\n",
            COLD_TIER_DEVICE_TOTAL,
            escape(runtime),
            escape(state),
            value
        ));
    }
    for GaugeSample { key, value } in &snapshot.cold_tier_device_totals {
        let ColdTierRuntimeKey { runtime } = key;
        output.push_str(&format!(
            "{}{{runtime=\"{}\",state=\"all\"}} {}\n",
            COLD_TIER_DEVICE_TOTAL,
            escape(runtime),
            value
        ));
    }

    gauge_family(
        output,
        COLD_TIER_DEVICE_SCHEDULABLE_TOTAL,
        "Schedulable cold tier devices by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_DEVICE_SCHEDULABLE_TOTAL,
        &snapshot.cold_tier_device_schedulable,
    );

    gauge_family(
        output,
        COLD_TIER_DEVICE_USED_BYTES,
        "Cold tier used bytes by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_DEVICE_USED_BYTES,
        &snapshot.cold_tier_device_used_bytes,
    );

    gauge_family(
        output,
        COLD_TIER_DEVICE_RESERVED_BYTES,
        "Cold tier reserved bytes by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_DEVICE_RESERVED_BYTES,
        &snapshot.cold_tier_device_reserved_bytes,
    );

    gauge_family(
        output,
        COLD_TIER_DEVICE_CAPACITY_BYTES,
        "Cold tier capacity bytes by runtime when all device capacities are known.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_DEVICE_CAPACITY_BYTES,
        &snapshot.cold_tier_device_capacity_bytes,
    );

    gauge_family(
        output,
        COLD_TIER_PENDING_OFFLOAD_TOTAL,
        "Pending cold tier offloads by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_PENDING_OFFLOAD_TOTAL,
        &snapshot.cold_tier_pending_offload_total,
    );

    gauge_family(
        output,
        COLD_TIER_PENDING_OFFLOAD_READY,
        "Ready pending cold tier offloads by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_PENDING_OFFLOAD_READY,
        &snapshot.cold_tier_pending_offload_ready,
    );

    gauge_family(
        output,
        COLD_TIER_PENDING_OFFLOAD_DELAYED,
        "Delayed pending cold tier offloads by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_PENDING_OFFLOAD_DELAYED,
        &snapshot.cold_tier_pending_offload_delayed,
    );

    gauge_family(
        output,
        COLD_TIER_PENDING_OFFLOAD_ATTEMPTS_TOTAL,
        "Total retry attempts across pending cold tier offloads by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_PENDING_OFFLOAD_ATTEMPTS_TOTAL,
        &snapshot.cold_tier_pending_offload_attempts_total,
    );

    gauge_family(
        output,
        COLD_TIER_PENDING_OFFLOAD_MAX_ATTEMPTS,
        "Maximum retry attempts for pending cold tier offloads by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_PENDING_OFFLOAD_MAX_ATTEMPTS,
        &snapshot.cold_tier_pending_offload_max_attempts,
    );

    gauge_family(
        output,
        COLD_TIER_RECLAIM_PENDING_TOTAL,
        "Pending cold tier reclaim entries by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_RECLAIM_PENDING_TOTAL,
        &snapshot.cold_tier_reclaim_pending_total,
    );

    gauge_family(
        output,
        COLD_TIER_RECLAIM_DUE_TOTAL,
        "Due cold tier reclaim entries by runtime.",
    );
    for_cold_runtime_gauge(
        output,
        COLD_TIER_RECLAIM_DUE_TOTAL,
        &snapshot.cold_tier_reclaim_due_total,
    );

    gauge_family(
        output,
        COLD_TIER_RECLAIM_BY_KIND,
        "Cold tier reclaim entries by runtime and reclaim kind.",
    );
    for GaugeSample { key, value } in &snapshot.cold_tier_reclaim_by_kind {
        let ColdTierReclaimKindKey { runtime, kind } = key;
        output.push_str(&format!(
            "{}{{runtime=\"{}\",kind=\"{}\"}} {}\n",
            COLD_TIER_RECLAIM_BY_KIND,
            escape(runtime),
            escape(kind),
            value
        ));
    }

    gauge_family(
        output,
        COLD_TIER_RECLAIM_BY_QOS_TIER,
        "Cold tier reclaim entries by runtime and QoS tier.",
    );
    for GaugeSample { key, value } in &snapshot.cold_tier_reclaim_by_qos_tier {
        let ColdTierReclaimQosKey { runtime, qos_tier } = key;
        output.push_str(&format!(
            "{}{{runtime=\"{}\",qos_tier=\"{}\"}} {}\n",
            COLD_TIER_RECLAIM_BY_QOS_TIER,
            escape(runtime),
            escape(qos_tier),
            value
        ));
    }

    gauge_family(
        output,
        COLD_TIER_RECLAIM_BY_POLICY_RANK,
        "Cold tier reclaim entries by runtime and policy rank.",
    );
    for GaugeSample { key, value } in &snapshot.cold_tier_reclaim_by_policy_rank {
        let ColdTierReclaimPolicyRankKey {
            runtime,
            policy_rank,
        } = key;
        output.push_str(&format!(
            "{}{{runtime=\"{}\",policy_rank=\"{}\"}} {}\n",
            COLD_TIER_RECLAIM_BY_POLICY_RANK,
            escape(runtime),
            policy_rank,
            value
        ));
    }

    counter_family(
        output,
        COLD_TIER_OPERATION_TOTAL,
        "Cold tier operation outcomes by operation, result, and error kind.",
    );
    for CounterSample { key, value } in &snapshot.cold_tier_operations {
        let ColdTierOperationKey {
            operation,
            result,
            error_kind,
        } = key;
        output.push_str(&format!(
            "{}{{operation=\"{}\",result=\"{}\",error_kind=\"{}\"}} {}\n",
            COLD_TIER_OPERATION_TOTAL,
            escape(operation),
            escape(result),
            escape(error_kind),
            value
        ));
    }

    counter_family(
        output,
        COLD_RESTORE_SINGLEFLIGHT_TOTAL,
        "Cold restore singleflight events by event and result.",
    );
    for CounterSample { key, value } in &snapshot.cold_restore_singleflight {
        let ColdRestoreSingleflightKey { event, result } = key;
        output.push_str(&format!(
            "{}{{event=\"{}\",result=\"{}\"}} {}\n",
            COLD_RESTORE_SINGLEFLIGHT_TOTAL,
            escape(event),
            escape(result),
            value
        ));
    }

    gauge_family(
        output,
        COLD_RESTORE_MAX_CONCURRENT_IO_PER_OBJECT,
        "High-water mark of concurrent SSD I/O workers for any single object. Should always be 1; >1 indicates singleflight dedup failure.",
    );
    output.push_str(&format!(
        "{} {}\n",
        COLD_RESTORE_MAX_CONCURRENT_IO_PER_OBJECT,
        snapshot.cold_restore_max_concurrent_io_per_object
    ));

    histogram_family(
        output,
        COLD_TIER_SSD_READ_DURATION,
        "Cold tier SSD read latency in seconds.",
    );
    for sample in &snapshot.cold_tier_ssd_read_duration {
        render_cold_tier_io_histogram(output, COLD_TIER_SSD_READ_DURATION, sample);
    }

    histogram_family(
        output,
        COLD_TIER_SSD_WRITE_DURATION,
        "Cold tier SSD write latency in seconds.",
    );
    for sample in &snapshot.cold_tier_ssd_write_duration {
        render_cold_tier_io_histogram(output, COLD_TIER_SSD_WRITE_DURATION, sample);
    }

    histogram_family(
        output,
        EXTENT_STORE_IO_PRIORITY_WAIT,
        "Time spent waiting for ExtentStore IO priority lock in seconds.",
    );
    for sample in &snapshot.io_priority_wait {
        render_cold_tier_io_histogram(output, EXTENT_STORE_IO_PRIORITY_WAIT, sample);
    }

    histogram_family(
        output,
        STAGING_POOL_WAIT,
        "Time spent waiting in staging pool allocate_blocking in seconds.",
    );
    for sample in &snapshot.staging_pool_wait {
        render_cold_tier_io_histogram(output, STAGING_POOL_WAIT, sample);
    }

    counter_family(
        output,
        STAGING_POOL_EXHAUSTION_TOTAL,
        "Total staging pool try_allocate failures due to pool exhaustion.",
    );
    for sample in &snapshot.staging_pool_exhaustion {
        output.push_str(&format!(
            "{}{{result=\"{}\"}} {}\n",
            STAGING_POOL_EXHAUSTION_TOTAL, sample.key.result, sample.value,
        ));
    }

    // Extent store queue wait duration histogram (per direction)
    histogram_family(
        output,
        EXTENT_STORE_QUEUE_WAIT,
        "Time requests spend waiting in the io_uring worker queue.",
    );
    for sample in &snapshot.extent_store_queue_wait {
        render_cold_tier_io_histogram(output, EXTENT_STORE_QUEUE_WAIT, sample);
    }

    // Extent store pipeline duration histogram (per direction)
    histogram_family(
        output,
        EXTENT_STORE_PIPELINE_DURATION,
        "Total time for an io_uring pipeline batch execution.",
    );
    for sample in &snapshot.extent_store_pipeline_duration {
        render_cold_tier_io_histogram(output, EXTENT_STORE_PIPELINE_DURATION, sample);
    }

    // Batch get phase duration histogram
    histogram_family(
        output,
        BATCH_GET_PHASE_DURATION,
        "Duration of each phase in batch_get_into in seconds.",
    );
    for sample in &snapshot.batch_get_phase_duration {
        render_phase_histogram(output, BATCH_GET_PHASE_DURATION, sample);
    }

    // Batch get path items counter
    counter_family(
        output,
        BATCH_GET_PATH_ITEMS_TOTAL,
        "Total items routed through each batch_get path.",
    );
    for sample in &snapshot.batch_get_path_items {
        output.push_str(&format!(
            "{}{{phase=\"{}\"}} {}\n",
            BATCH_GET_PATH_ITEMS_TOTAL, sample.key.phase, sample.value,
        ));
    }

    // Facade phase duration histogram
    histogram_family(
        output,
        FACADE_PHASE_DURATION,
        "Duration of each phase in facade batch_get_into in seconds.",
    );
    for sample in &snapshot.facade_phase_duration {
        render_phase_histogram(output, FACADE_PHASE_DURATION, sample);
    }

    // RDMA transfer duration histogram
    histogram_family(
        output,
        RDMA_TRANSFER_DURATION,
        "Duration of RDMA transfer phases in seconds.",
    );
    for sample in &snapshot.rdma_transfer_duration {
        render_phase_histogram(output, RDMA_TRANSFER_DURATION, sample);
    }

    // RDMA bytes counter
    counter_family(
        output,
        RDMA_BYTES_TOTAL,
        "Total bytes transferred via RDMA.",
    );
    for sample in &snapshot.rdma_bytes {
        output.push_str(&format!(
            "{}{{phase=\"{}\"}} {}\n",
            RDMA_BYTES_TOTAL, sample.key.phase, sample.value,
        ));
    }

    // Cold read batch wall duration histogram
    histogram_family(
        output,
        COLD_READ_BATCH_WALL_DURATION,
        "Wall clock duration of cold read batch operations in seconds.",
    );
    for sample in &snapshot.cold_read_batch_wall_duration {
        render_cold_tier_io_histogram(output, COLD_READ_BATCH_WALL_DURATION, sample);
    }

    // io_uring phase duration histogram
    histogram_family(
        output,
        IO_URING_PHASE_DURATION,
        "Duration of io_uring pipeline phases in seconds.",
    );
    for sample in &snapshot.io_uring_phase_duration {
        render_phase_histogram(output, IO_URING_PHASE_DURATION, sample);
    }

    // io_uring ops counter
    counter_family(
        output,
        IO_URING_OPS_TOTAL,
        "Total io_uring operations submitted.",
    );
    for sample in &snapshot.io_uring_ops {
        output.push_str(&format!(
            "{}{{result=\"{}\"}} {}\n",
            IO_URING_OPS_TOTAL, sample.key.result, sample.value,
        ));
    }

    // Cold restore batch duration histogram
    histogram_family(
        output,
        COLD_RESTORE_BATCH_DURATION,
        "Duration of cold restore batch phases in seconds.",
    );
    for sample in &snapshot.cold_restore_batch_duration {
        render_phase_histogram(output, COLD_RESTORE_BATCH_DURATION, sample);
    }

    // Cold restore batch items counter
    counter_family(
        output,
        COLD_RESTORE_BATCH_ITEMS,
        "Total items in cold restore batches.",
    );
    for sample in &snapshot.cold_restore_batch_items {
        output.push_str(&format!(
            "{}{{result=\"{}\"}} {}\n",
            COLD_RESTORE_BATCH_ITEMS, sample.key.result, sample.value,
        ));
    }

    // Cold prefetch worker duration histogram
    histogram_family(
        output,
        COLD_PREFETCH_WORKER_DURATION,
        "Duration of cold prefetch worker execution in seconds.",
    );
    for sample in &snapshot.cold_prefetch_worker_duration {
        render_cold_tier_io_histogram(output, COLD_PREFETCH_WORKER_DURATION, sample);
    }

    // Cold prefetch worker items counter
    counter_family(
        output,
        COLD_PREFETCH_WORKER_ITEMS,
        "Total items processed by cold prefetch worker.",
    );
    for sample in &snapshot.cold_prefetch_worker_items {
        output.push_str(&format!(
            "{}{{result=\"{}\"}} {}\n",
            COLD_PREFETCH_WORKER_ITEMS, sample.key.result, sample.value,
        ));
    }

    // Batch is_exist duration histogram
    histogram_family(
        output,
        BATCH_IS_EXIST_DURATION,
        "Duration of batch_is_exist phases in seconds.",
    );
    for sample in &snapshot.batch_is_exist_duration {
        render_phase_histogram(output, BATCH_IS_EXIST_DURATION, sample);
    }
}
