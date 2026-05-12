use super::registry::{
    ActionResultKey, CounterSample, GaugeSample, HistogramSample, MetadataInflightKey,
    MetadataOperationKey, MetricsSnapshot, PhaseKey, PhaseResultKey, PreferredSegmentSkipKey,
    ReplicaDistributionKey, RequestBytesKey, RequestInflightKey, RequestKey, ResultKey, RuntimeKey,
    RuntimeStatusKey, TenantKey, TransportBytesKey, TransportOperationKey,
    CHECKSUM_VALIDATION_TOTAL, EVICTION_DURATION, EVICTION_TOTAL, HEARTBEAT_CONSECUTIVE_FAILURES,
    HEARTBEAT_LAST_SUCCESS_MS, MEMBERSHIP_REFRESH_DURATION, MEMBERSHIP_REFRESH_TOTAL,
    METADATA_OPERATION_DURATION, METADATA_OPERATION_INFLIGHT, METADATA_OPERATION_TOTAL,
    OBJECT_ROUTES, PREFERRED_SEGMENT_SKIP_TOTAL, REBALANCE_BYTES_TOTAL, REBALANCE_ROUTES_TOTAL,
    REPLICATION_PUBLISH_DURATION, REPLICATION_PUBLISH_TOTAL, REPLICA_DISTRIBUTION, REQUEST_BYTES,
    REQUEST_DURATION, REQUEST_DURATION_BUCKETS, REQUEST_INFLIGHT, REQUEST_TOTAL, ROUTE_CAS_TOTAL,
    RUNTIME_LEASE_EXPIRES_AT_MS, RUNTIME_STATUS, SEGMENT_CAPACITY_BYTES, SEGMENT_LIFECYCLE_TOTAL,
    SEGMENT_USED_BYTES, TENANT_LOCAL_EVICTION_TOTAL, TENANT_QUOTA_ABORT_TOTAL,
    TENANT_QUOTA_FINALIZE_TOTAL, TENANT_QUOTA_RECONCILE_TOTAL, TENANT_QUOTA_RESERVATION_TOTAL,
    TRANSPORT_BYTES_TOTAL, TRANSPORT_OPERATION_TOTAL,
};

pub(crate) fn render_prometheus_metrics(snapshot: &MetricsSnapshot) -> String {
    let mut output = String::new();
    render_legacy_operation_metrics(&mut output, snapshot);
    render_request_metrics(&mut output, snapshot);
    render_cluster_state_metrics(&mut output, snapshot);
    render_metadata_metrics(&mut output, snapshot);
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
