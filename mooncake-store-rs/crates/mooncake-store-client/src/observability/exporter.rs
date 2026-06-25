use super::registry::{
    ActionResultKey, ColdRestoreSingleflightKey, ColdTierDeviceStateKey, ColdTierOperationKey,
    ColdTierReclaimKindKey, ColdTierReclaimPolicyRankKey, ColdTierReclaimQosKey,
    ColdTierRuntimeKey, CounterSample, GaugeSample, HistogramSample, MetadataInflightKey,
    MetadataOperationKey, MetricsSnapshot, PhaseKey, PhaseResultKey, PreferredSegmentSkipKey,
    ReplicaDistributionKey, RequestBytesKey, RequestInflightKey, RequestKey, ResultKey, RuntimeKey,
    RuntimeStatusKey, TenantKey, TransportBytesKey, TransportOperationKey,
    BATCH_GET_PATH_ITEMS_TOTAL, BATCH_GET_PHASE_DURATION, BATCH_IS_EXIST_DURATION,
    CHECKSUM_VALIDATION_TOTAL, COLD_PREFETCH_WORKER_DURATION, COLD_PREFETCH_WORKER_ITEMS,
    COLD_READ_BATCH_WALL_DURATION, COLD_RESTORE_BATCH_DURATION, COLD_RESTORE_BATCH_ITEMS,
    COLD_RESTORE_MAX_CONCURRENT_IO_PER_OBJECT, COLD_RESTORE_SINGLEFLIGHT_TOTAL,
    COLD_TIER_DEVICE_CAPACITY_BYTES, COLD_TIER_DEVICE_RESERVED_BYTES,
    COLD_TIER_DEVICE_SCHEDULABLE_TOTAL, COLD_TIER_DEVICE_TOTAL, COLD_TIER_DEVICE_USED_BYTES,
    COLD_TIER_IO_BUCKETS, COLD_TIER_OPERATION_TOTAL, COLD_TIER_PENDING_OFFLOAD_ATTEMPTS_TOTAL,
    COLD_TIER_PENDING_OFFLOAD_DELAYED, COLD_TIER_PENDING_OFFLOAD_MAX_ATTEMPTS,
    COLD_TIER_PENDING_OFFLOAD_READY, COLD_TIER_PENDING_OFFLOAD_TOTAL, COLD_TIER_RECLAIM_BY_KIND,
    COLD_TIER_RECLAIM_BY_POLICY_RANK, COLD_TIER_RECLAIM_BY_QOS_TIER, COLD_TIER_RECLAIM_DUE_TOTAL,
    COLD_TIER_RECLAIM_PENDING_TOTAL, COLD_TIER_SSD_READ_DURATION, COLD_TIER_SSD_WRITE_DURATION,
    EVICTION_DURATION, EVICTION_TOTAL, EXTENT_STORE_IO_PRIORITY_WAIT,
    EXTENT_STORE_PIPELINE_DURATION, EXTENT_STORE_QUEUE_WAIT, FACADE_PHASE_DURATION,
    HEARTBEAT_CONSECUTIVE_FAILURES, HEARTBEAT_LAST_SUCCESS_MS, IO_URING_OPS_TOTAL,
    IO_URING_PHASE_DURATION, MEMBERSHIP_REFRESH_DURATION, MEMBERSHIP_REFRESH_TOTAL,
    METADATA_OPERATION_DURATION, METADATA_OPERATION_INFLIGHT, METADATA_OPERATION_TOTAL,
    OBJECT_ROUTES, PREFERRED_SEGMENT_SKIP_TOTAL, RDMA_BYTES_TOTAL, RDMA_TRANSFER_DURATION,
    REBALANCE_BYTES_TOTAL, REBALANCE_ROUTES_TOTAL, RECLAIM_RELEASE_TOTAL,
    REPLICATION_PUBLISH_DURATION, REPLICATION_PUBLISH_TOTAL, REPLICA_DISTRIBUTION, REQUEST_BYTES,
    REQUEST_DURATION, REQUEST_DURATION_BUCKETS, REQUEST_INFLIGHT, REQUEST_TOTAL, ROUTE_CAS_TOTAL,
    RUNTIME_LEASE_EXPIRES_AT_MS, RUNTIME_STATUS, SEGMENT_CAPACITY_BYTES, SEGMENT_LIFECYCLE_TOTAL,
    SEGMENT_USED_BYTES, STAGING_POOL_EXHAUSTION_TOTAL, STAGING_POOL_WAIT,
    TENANT_LOCAL_EVICTION_TOTAL, TENANT_QUOTA_ABORT_TOTAL, TENANT_QUOTA_FINALIZE_TOTAL,
    TENANT_QUOTA_RECONCILE_TOTAL, TENANT_QUOTA_RESERVATION_TOTAL, TRANSPORT_BYTES_TOTAL,
    TRANSPORT_OPERATION_TOTAL,
};

pub(crate) fn render_prometheus_metrics(snapshot: &MetricsSnapshot) -> String {
    let mut output = String::new();
    render_legacy_operation_metrics(&mut output, snapshot);
    render_request_metrics(&mut output, snapshot);
    render_cluster_state_metrics(&mut output, snapshot);
    render_metadata_metrics(&mut output, snapshot);
    render_cold_tier_metrics(&mut output, snapshot);
    render_consistency_metrics(&mut output, snapshot);
    render_recovery_metrics(&mut output, snapshot);
    render_process_metrics(&mut output, snapshot);
    output
}

fn render_metadata_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
    let tenant = escape(&snapshot.tenant);
    counter_family(
        output,
        METADATA_OPERATION_TOTAL,
        "Metadata backend operation outcomes by backend, operation, and result.",
    );
    for CounterSample { key, value } in &snapshot.metadata_operations {
        let MetadataOperationKey {
            backend,
            operation,
            result,
        } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",backend=\"{}\",operation=\"{}\",result=\"{}\"}} {}\n",
            METADATA_OPERATION_TOTAL,
            tenant,
            escape(backend),
            escape(operation),
            escape(result),
            value
        ));
    }

    gauge_family(
        output,
        METADATA_OPERATION_INFLIGHT,
        "Inflight metadata backend operations by backend and operation.",
    );
    for GaugeSample { key, value } in &snapshot.metadata_inflight {
        let MetadataInflightKey { backend, operation } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",backend=\"{}\",operation=\"{}\"}} {}\n",
            METADATA_OPERATION_INFLIGHT,
            tenant,
            escape(backend),
            escape(operation),
            value
        ));
    }

    histogram_family(
        output,
        METADATA_OPERATION_DURATION,
        "Metadata backend operation duration in seconds.",
    );
    for sample in &snapshot.metadata_duration {
        render_metadata_histogram(output, METADATA_OPERATION_DURATION, &tenant, sample);
    }
}

fn render_cluster_state_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
    let tenant = escape(&snapshot.tenant);
    gauge_family(
        output,
        SEGMENT_CAPACITY_BYTES,
        "Segment capacity by runtime, segment, state, and tier.",
    );
    for sample in &snapshot.segments {
        output.push_str(&format!(
            "{}{{tenant=\"{}\",runtime=\"{}\",segment=\"{}\",state=\"{}\",tier=\"{}\"}} {}\n",
            SEGMENT_CAPACITY_BYTES,
            tenant,
            escape(&sample.runtime),
            escape(&sample.segment),
            escape(sample.state),
            escape(sample.tier),
            sample.capacity_bytes
        ));
    }

    gauge_family(
        output,
        SEGMENT_USED_BYTES,
        "Segment used bytes by runtime, segment, state, and tier.",
    );
    for sample in &snapshot.segments {
        output.push_str(&format!(
            "{}{{tenant=\"{}\",runtime=\"{}\",segment=\"{}\",state=\"{}\",tier=\"{}\"}} {}\n",
            SEGMENT_USED_BYTES,
            tenant,
            escape(&sample.runtime),
            escape(&sample.segment),
            escape(sample.state),
            escape(sample.tier),
            sample.used_bytes
        ));
    }

    gauge_family(output, OBJECT_ROUTES, "Known object routes by tenant.");
    for GaugeSample { key, value } in &snapshot.object_routes {
        let TenantKey { tenant } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\"}} {}\n",
            OBJECT_ROUTES,
            escape(tenant),
            value
        ));
    }

    gauge_family(
        output,
        REPLICA_DISTRIBUTION,
        "Known replicas by runtime and tier.",
    );
    for GaugeSample { key, value } in &snapshot.replica_distribution {
        let ReplicaDistributionKey { runtime, tier } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",runtime=\"{}\",tier=\"{}\"}} {}\n",
            REPLICA_DISTRIBUTION,
            tenant,
            escape(runtime),
            escape(tier),
            value
        ));
    }

    gauge_family(output, RUNTIME_STATUS, "Runtime lifecycle status gauges.");
    for GaugeSample { key, value } in &snapshot.runtime_status {
        let RuntimeStatusKey { runtime, state } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",runtime=\"{}\",state=\"{}\"}} {}\n",
            RUNTIME_STATUS,
            tenant,
            escape(runtime),
            escape(state),
            value
        ));
    }

    gauge_family(
        output,
        RUNTIME_LEASE_EXPIRES_AT_MS,
        "Runtime lease expiration timestamp in milliseconds.",
    );
    for GaugeSample { key, value } in &snapshot.runtime_lease_expires_at_ms {
        let RuntimeKey { runtime } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",runtime=\"{}\"}} {}\n",
            RUNTIME_LEASE_EXPIRES_AT_MS,
            tenant,
            escape(runtime),
            value
        ));
    }

    gauge_family(
        output,
        HEARTBEAT_CONSECUTIVE_FAILURES,
        "Consecutive heartbeat failures by runtime.",
    );
    for GaugeSample { key, value } in &snapshot.heartbeat_consecutive_failures {
        let RuntimeKey { runtime } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",runtime=\"{}\"}} {}\n",
            HEARTBEAT_CONSECUTIVE_FAILURES,
            tenant,
            escape(runtime),
            value
        ));
    }

    gauge_family(
        output,
        HEARTBEAT_LAST_SUCCESS_MS,
        "Last successful heartbeat timestamp in milliseconds by runtime.",
    );
    for GaugeSample { key, value } in &snapshot.heartbeat_last_success_ms {
        let RuntimeKey { runtime } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",runtime=\"{}\"}} {}\n",
            HEARTBEAT_LAST_SUCCESS_MS,
            tenant,
            escape(runtime),
            value
        ));
    }

    counter_family(
        output,
        MEMBERSHIP_REFRESH_TOTAL,
        "Membership refresh outcomes.",
    );
    for_result_counter(
        output,
        MEMBERSHIP_REFRESH_TOTAL,
        &tenant,
        &snapshot.membership_refresh,
    );

    histogram_family(
        output,
        MEMBERSHIP_REFRESH_DURATION,
        "Membership refresh duration in seconds.",
    );
    for sample in &snapshot.membership_refresh_duration {
        render_result_histogram(output, MEMBERSHIP_REFRESH_DURATION, &tenant, sample);
    }
}

fn render_cold_tier_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
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

fn render_legacy_operation_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
    let tenant = escape(&snapshot.tenant);
    output.push_str("# HELP mooncake_store_operation_total Total store operations.\n");
    output.push_str("# TYPE mooncake_store_operation_total counter\n");
    for sample in &snapshot.operations {
        output.push_str(&format!(
            "mooncake_store_operation_total{{tenant=\"{}\",operation=\"{}\",status=\"{}\"}} {}\n",
            tenant,
            escape(sample.operation),
            escape(sample.status),
            sample.calls_total
        ));
    }

    output.push_str(
        "# HELP mooncake_store_operation_bytes_in_total Total input bytes by store operation.\n",
    );
    output.push_str("# TYPE mooncake_store_operation_bytes_in_total counter\n");
    for sample in &snapshot.operations {
        output.push_str(&format!(
            "mooncake_store_operation_bytes_in_total{{tenant=\"{}\",operation=\"{}\",status=\"{}\"}} {}\n",
            tenant,
            escape(sample.operation),
            escape(sample.status),
            sample.bytes_in_total
        ));
    }

    output.push_str(
        "# HELP mooncake_store_operation_bytes_out_total Total output bytes by store operation.\n",
    );
    output.push_str("# TYPE mooncake_store_operation_bytes_out_total counter\n");
    for sample in &snapshot.operations {
        output.push_str(&format!(
            "mooncake_store_operation_bytes_out_total{{tenant=\"{}\",operation=\"{}\",status=\"{}\"}} {}\n",
            tenant,
            escape(sample.operation),
            escape(sample.status),
            sample.bytes_out_total
        ));
    }

    output.push_str("# HELP mooncake_store_operation_latency_microseconds_total Total latency in microseconds by store operation.\n");
    output.push_str("# TYPE mooncake_store_operation_latency_microseconds_total counter\n");
    for sample in &snapshot.operations {
        output.push_str(&format!(
            "mooncake_store_operation_latency_microseconds_total{{tenant=\"{}\",operation=\"{}\",status=\"{}\"}} {}\n",
            tenant,
            escape(sample.operation),
            escape(sample.status),
            sample.latency_total_us
        ));
    }

    output.push_str("# HELP mooncake_store_operation_latency_microseconds_max Maximum latency in microseconds by store operation.\n");
    output.push_str("# TYPE mooncake_store_operation_latency_microseconds_max gauge\n");
    for sample in &snapshot.operations {
        output.push_str(&format!(
            "mooncake_store_operation_latency_microseconds_max{{tenant=\"{}\",operation=\"{}\",status=\"{}\"}} {}\n",
            tenant,
            escape(sample.operation),
            escape(sample.status),
            sample.latency_max_us
        ));
    }
}

fn render_request_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
    let tenant = escape(&snapshot.tenant);
    counter_family(
        output,
        REQUEST_TOTAL,
        "Total requests by operation, scope, and result.",
    );
    for sample in &snapshot.request_totals {
        let key = &sample.key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",operation=\"{}\",scope=\"{}\",result=\"{}\"}} {}\n",
            REQUEST_TOTAL,
            tenant,
            escape(key.operation),
            escape(key.scope),
            escape(key.result),
            sample.value
        ));
    }

    gauge_family(
        output,
        REQUEST_INFLIGHT,
        "Inflight requests by operation and scope.",
    );
    for GaugeSample { key, value } in &snapshot.request_inflight {
        let RequestInflightKey { operation, scope } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",operation=\"{}\",scope=\"{}\"}} {}\n",
            REQUEST_INFLIGHT,
            tenant,
            escape(operation),
            escape(scope),
            value
        ));
    }

    counter_family(
        output,
        REQUEST_BYTES,
        "Total request bytes by direction and scope.",
    );
    for CounterSample { key, value } in &snapshot.request_bytes {
        let RequestBytesKey {
            operation,
            direction,
            scope,
        } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",operation=\"{}\",direction=\"{}\",scope=\"{}\"}} {}\n",
            REQUEST_BYTES,
            tenant,
            escape(operation),
            escape(direction),
            escape(scope),
            value
        ));
    }

    histogram_family(output, REQUEST_DURATION, "Request duration in seconds.");
    for HistogramSample {
        key,
        buckets,
        count,
        sum,
    } in &snapshot.request_duration
    {
        render_request_histogram(
            output,
            REQUEST_DURATION,
            &tenant,
            key,
            buckets,
            *count,
            *sum,
        );
    }
}

fn render_consistency_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
    let tenant = escape(&snapshot.tenant);
    counter_family(output, ROUTE_CAS_TOTAL, "Route CAS outcomes.");
    for_result_counter(output, ROUTE_CAS_TOTAL, &tenant, &snapshot.route_cas);

    counter_family(output, REPLICATION_PUBLISH_TOTAL, "Route publish outcomes.");
    for_result_counter(
        output,
        REPLICATION_PUBLISH_TOTAL,
        &tenant,
        &snapshot.replication_publish,
    );

    histogram_family(
        output,
        REPLICATION_PUBLISH_DURATION,
        "Route publish duration in seconds.",
    );
    for sample in &snapshot.replication_publish_duration {
        render_result_histogram(output, REPLICATION_PUBLISH_DURATION, &tenant, sample);
    }

    counter_family(
        output,
        CHECKSUM_VALIDATION_TOTAL,
        "Checksum validation outcomes.",
    );
    for_result_counter(
        output,
        CHECKSUM_VALIDATION_TOTAL,
        &tenant,
        &snapshot.checksum_validation,
    );

    counter_family(
        output,
        TENANT_QUOTA_RESERVATION_TOTAL,
        "Tenant quota reservation outcomes.",
    );
    for_result_counter(
        output,
        TENANT_QUOTA_RESERVATION_TOTAL,
        &tenant,
        &snapshot.tenant_quota_reservation,
    );

    counter_family(
        output,
        TENANT_QUOTA_FINALIZE_TOTAL,
        "Tenant quota finalize outcomes.",
    );
    for_result_counter(
        output,
        TENANT_QUOTA_FINALIZE_TOTAL,
        &tenant,
        &snapshot.tenant_quota_finalize,
    );

    counter_family(
        output,
        TENANT_QUOTA_ABORT_TOTAL,
        "Tenant quota abort outcomes.",
    );
    for_result_counter(
        output,
        TENANT_QUOTA_ABORT_TOTAL,
        &tenant,
        &snapshot.tenant_quota_abort,
    );

    counter_family(
        output,
        TENANT_QUOTA_RECONCILE_TOTAL,
        "Tenant quota reconcile outcomes.",
    );
    for_result_counter(
        output,
        TENANT_QUOTA_RECONCILE_TOTAL,
        &tenant,
        &snapshot.tenant_quota_reconcile,
    );

    counter_family(
        output,
        TENANT_LOCAL_EVICTION_TOTAL,
        "Tenant-local quota eviction outcomes.",
    );
    for_result_counter(
        output,
        TENANT_LOCAL_EVICTION_TOTAL,
        &tenant,
        &snapshot.tenant_local_eviction,
    );

    counter_family(
        output,
        PREFERRED_SEGMENT_SKIP_TOTAL,
        "Skipped preferred-segment hints by source and reason.",
    );
    for CounterSample { key, value } in &snapshot.preferred_segment_skip {
        let PreferredSegmentSkipKey { source, reason } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",source=\"{}\",reason=\"{}\"}} {}\n",
            PREFERRED_SEGMENT_SKIP_TOTAL,
            tenant,
            escape(source),
            escape(reason),
            value
        ));
    }
}

fn render_recovery_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
    let tenant = escape(&snapshot.tenant);
    counter_family(output, REBALANCE_ROUTES_TOTAL, "Rebalance route outcomes.");
    for CounterSample { key, value } in &snapshot.rebalance_routes {
        let PhaseResultKey { phase, result } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",phase=\"{}\",result=\"{}\"}} {}\n",
            REBALANCE_ROUTES_TOTAL,
            tenant,
            escape(phase),
            escape(result),
            value
        ));
    }

    counter_family(output, REBALANCE_BYTES_TOTAL, "Rebalance bytes by phase.");
    for CounterSample { key, value } in &snapshot.rebalance_bytes {
        let PhaseKey { phase } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",phase=\"{}\"}} {}\n",
            REBALANCE_BYTES_TOTAL,
            tenant,
            escape(phase),
            value
        ));
    }

    counter_family(
        output,
        SEGMENT_LIFECYCLE_TOTAL,
        "Segment lifecycle transition outcomes.",
    );
    for CounterSample { key, value } in &snapshot.segment_lifecycle {
        let ActionResultKey { action, result } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",action=\"{}\",result=\"{}\"}} {}\n",
            SEGMENT_LIFECYCLE_TOTAL,
            tenant,
            escape(action),
            escape(result),
            value
        ));
    }

    counter_family(
        output,
        RECLAIM_RELEASE_TOTAL,
        "Reclaim release outcomes by flush action.",
    );
    for CounterSample { key, value } in &snapshot.reclaim_release {
        let ActionResultKey { action, result } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",action=\"{}\",result=\"{}\"}} {}\n",
            RECLAIM_RELEASE_TOTAL,
            tenant,
            escape(action),
            escape(result),
            value
        ));
    }

    counter_family(output, EVICTION_TOTAL, "Eviction loop outcomes.");
    for_result_counter(output, EVICTION_TOTAL, &tenant, &snapshot.eviction);

    histogram_family(
        output,
        EVICTION_DURATION,
        "Eviction loop duration in seconds.",
    );
    for sample in &snapshot.eviction_duration {
        render_result_histogram(output, EVICTION_DURATION, &tenant, sample);
    }

    counter_family(
        output,
        TRANSPORT_OPERATION_TOTAL,
        "Transport operation outcomes by peer kind.",
    );
    for CounterSample { key, value } in &snapshot.transport_operations {
        let TransportOperationKey {
            direction,
            peer_kind,
            result,
        } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",direction=\"{}\",peer_kind=\"{}\",result=\"{}\"}} {}\n",
            TRANSPORT_OPERATION_TOTAL,
            tenant,
            escape(direction),
            escape(peer_kind),
            escape(result),
            value
        ));
    }

    counter_family(
        output,
        TRANSPORT_OPERATION_TOTAL,
        "Transport operation outcomes by direction and peer kind.",
    );
    for CounterSample { key, value } in &snapshot.transport_operations {
        let TransportOperationKey {
            direction,
            peer_kind,
            result,
        } = key;
        output.push_str(&format!(
            "{}{{direction=\"{}\",peer_kind=\"{}\",result=\"{}\"}} {}\n",
            TRANSPORT_OPERATION_TOTAL,
            escape(direction),
            escape(peer_kind),
            escape(result),
            value
        ));
    }

    counter_family(
        output,
        TRANSPORT_BYTES_TOTAL,
        "Transport bytes by peer kind.",
    );
    for CounterSample { key, value } in &snapshot.transport_bytes {
        let TransportBytesKey {
            direction,
            peer_kind,
        } = key;
        output.push_str(&format!(
            "{}{{tenant=\"{}\",direction=\"{}\",peer_kind=\"{}\"}} {}\n",
            TRANSPORT_BYTES_TOTAL,
            tenant,
            escape(direction),
            escape(peer_kind),
            value
        ));
    }
}

fn render_process_metrics(output: &mut String, snapshot: &MetricsSnapshot) {
    let tenant = escape(&snapshot.tenant);
    counter_family(
        output,
        "process_cpu_seconds_total",
        "Total user and system CPU time.",
    );
    output.push_str(&format!(
        "process_cpu_seconds_total{{tenant=\"{}\"}} {}\n",
        tenant, snapshot.process.cpu_seconds_total
    ));

    gauge_family(
        output,
        "process_resident_memory_bytes",
        "Resident memory used by this process.",
    );
    output.push_str(&format!(
        "process_resident_memory_bytes{{tenant=\"{}\"}} {}\n",
        tenant, snapshot.process.resident_memory_bytes
    ));

    if let Some(open_fds) = snapshot.process.open_fds {
        gauge_family(
            output,
            "process_open_fds",
            "Open file descriptors for this process.",
        );
        output.push_str(&format!(
            "process_open_fds{{tenant=\"{}\"}} {}\n",
            tenant, open_fds
        ));
    }
}

fn render_request_histogram(
    output: &mut String,
    metric: &str,
    tenant: &str,
    key: &RequestKey,
    buckets: &[u64],
    count: u64,
    sum: f64,
) {
    for (le, value) in REQUEST_DURATION_BUCKETS.iter().zip(buckets.iter()) {
        output.push_str(&format!(
            "{metric}_bucket{{tenant=\"{}\",operation=\"{}\",scope=\"{}\",result=\"{}\",le=\"{}\"}} {}\n",
            tenant,
            escape(key.operation),
            escape(key.scope),
            escape(key.result),
            format_bucket(*le),
            value
        ));
    }
    output.push_str(&format!(
        "{metric}_bucket{{tenant=\"{}\",operation=\"{}\",scope=\"{}\",result=\"{}\",le=\"+Inf\"}} {count}\n",
        tenant,
        escape(key.operation),
        escape(key.scope),
        escape(key.result)
    ));
    output.push_str(&format!(
        "{metric}_sum{{tenant=\"{}\",operation=\"{}\",scope=\"{}\",result=\"{}\"}} {sum}\n",
        tenant,
        escape(key.operation),
        escape(key.scope),
        escape(key.result)
    ));
    output.push_str(&format!(
        "{metric}_count{{tenant=\"{}\",operation=\"{}\",scope=\"{}\",result=\"{}\"}} {count}\n",
        tenant,
        escape(key.operation),
        escape(key.scope),
        escape(key.result)
    ));
}

fn render_result_histogram(
    output: &mut String,
    metric: &str,
    tenant: &str,
    sample: &HistogramSample<ResultKey>,
) {
    for (le, value) in REQUEST_DURATION_BUCKETS.iter().zip(sample.buckets.iter()) {
        output.push_str(&format!(
            "{metric}_bucket{{tenant=\"{}\",result=\"{}\",le=\"{}\"}} {}\n",
            tenant,
            escape(sample.key.result),
            format_bucket(*le),
            value
        ));
    }
    output.push_str(&format!(
        "{metric}_bucket{{tenant=\"{}\",result=\"{}\",le=\"+Inf\"}} {}\n",
        tenant,
        escape(sample.key.result),
        sample.count
    ));
    output.push_str(&format!(
        "{metric}_sum{{tenant=\"{}\",result=\"{}\"}} {}\n",
        tenant,
        escape(sample.key.result),
        sample.sum
    ));
    output.push_str(&format!(
        "{metric}_count{{tenant=\"{}\",result=\"{}\"}} {}\n",
        tenant,
        escape(sample.key.result),
        sample.count
    ));
}

fn render_cold_tier_io_histogram(
    output: &mut String,
    metric: &str,
    sample: &HistogramSample<ResultKey>,
) {
    for (le, value) in COLD_TIER_IO_BUCKETS.iter().zip(sample.buckets.iter()) {
        output.push_str(&format!(
            "{metric}_bucket{{result=\"{}\",le=\"{}\"}} {}\n",
            escape(sample.key.result),
            format_bucket(*le),
            value
        ));
    }
    output.push_str(&format!(
        "{metric}_bucket{{result=\"{}\",le=\"+Inf\"}} {}\n",
        escape(sample.key.result),
        sample.count
    ));
    output.push_str(&format!(
        "{metric}_sum{{result=\"{}\"}} {}\n",
        escape(sample.key.result),
        sample.sum
    ));
    output.push_str(&format!(
        "{metric}_count{{result=\"{}\"}} {}\n",
        escape(sample.key.result),
        sample.count
    ));
}

fn render_phase_histogram(output: &mut String, metric: &str, sample: &HistogramSample<PhaseKey>) {
    for (le, value) in COLD_TIER_IO_BUCKETS.iter().zip(sample.buckets.iter()) {
        output.push_str(&format!(
            "{metric}_bucket{{phase=\"{}\",le=\"{}\"}} {}\n",
            escape(sample.key.phase),
            format_bucket(*le),
            value
        ));
    }
    output.push_str(&format!(
        "{metric}_bucket{{phase=\"{}\",le=\"+Inf\"}} {}\n",
        escape(sample.key.phase),
        sample.count
    ));
    output.push_str(&format!(
        "{metric}_sum{{phase=\"{}\"}} {}\n",
        escape(sample.key.phase),
        sample.sum
    ));
    output.push_str(&format!(
        "{metric}_count{{phase=\"{}\"}} {}\n",
        escape(sample.key.phase),
        sample.count
    ));
}

fn render_metadata_histogram(
    output: &mut String,
    metric: &str,
    tenant: &str,
    sample: &HistogramSample<MetadataOperationKey>,
) {
    for (le, value) in REQUEST_DURATION_BUCKETS.iter().zip(sample.buckets.iter()) {
        output.push_str(&format!(
            "{metric}_bucket{{tenant=\"{}\",backend=\"{}\",operation=\"{}\",result=\"{}\",le=\"{}\"}} {}\n",
            tenant,
            escape(sample.key.backend),
            escape(sample.key.operation),
            escape(sample.key.result),
            format_bucket(*le),
            value
        ));
    }
    output.push_str(&format!(
        "{metric}_bucket{{tenant=\"{}\",backend=\"{}\",operation=\"{}\",result=\"{}\",le=\"+Inf\"}} {}\n",
        tenant,
        escape(sample.key.backend),
        escape(sample.key.operation),
        escape(sample.key.result),
        sample.count
    ));
    output.push_str(&format!(
        "{metric}_sum{{tenant=\"{}\",backend=\"{}\",operation=\"{}\",result=\"{}\"}} {}\n",
        tenant,
        escape(sample.key.backend),
        escape(sample.key.operation),
        escape(sample.key.result),
        sample.sum
    ));
    output.push_str(&format!(
        "{metric}_count{{tenant=\"{}\",backend=\"{}\",operation=\"{}\",result=\"{}\"}} {}\n",
        tenant,
        escape(sample.key.backend),
        escape(sample.key.operation),
        escape(sample.key.result),
        sample.count
    ));
}

fn for_result_counter(
    output: &mut String,
    metric: &str,
    tenant: &str,
    samples: &[CounterSample<ResultKey>],
) {
    for CounterSample { key, value } in samples {
        output.push_str(&format!(
            "{metric}{{tenant=\"{}\",result=\"{}\"}} {}\n",
            tenant,
            escape(key.result),
            value
        ));
    }
}

fn for_cold_runtime_gauge(
    output: &mut String,
    metric: &str,
    samples: &[GaugeSample<ColdTierRuntimeKey>],
) {
    for GaugeSample { key, value } in samples {
        let ColdTierRuntimeKey { runtime } = key;
        output.push_str(&format!(
            "{metric}{{runtime=\"{}\"}} {}\n",
            escape(runtime),
            value
        ));
    }
}

fn counter_family(output: &mut String, metric: &str, help: &str) {
    output.push_str(&format!(
        "# HELP {metric} {help}\n# TYPE {metric} counter\n"
    ));
}

fn gauge_family(output: &mut String, metric: &str, help: &str) {
    output.push_str(&format!("# HELP {metric} {help}\n# TYPE {metric} gauge\n"));
}

fn histogram_family(output: &mut String, metric: &str, help: &str) {
    output.push_str(&format!(
        "# HELP {metric} {help}\n# TYPE {metric} histogram\n"
    ));
}

fn format_bucket(value: f64) -> String {
    format!("{value:.6}")
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}
