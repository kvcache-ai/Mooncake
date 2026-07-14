use super::codec::pb_error;
use super::cold_tier_codec::try_cold_backing_route;
use super::*;

const MAX_ADMIN_OFFLOAD_TASKS: u64 = 1024;
const MAX_ADMIN_GC_BACKINGS: u64 = 1024;
const MAX_ADMIN_FREE_VICTIMS: u64 = 1024;
const MAX_ADMIN_BLOCKING_CONTROLS: usize = 8;

static ADMIN_BLOCKING_CONTROL: std::sync::LazyLock<tokio::sync::Semaphore> =
    std::sync::LazyLock::new(|| tokio::sync::Semaphore::new(MAX_ADMIN_BLOCKING_CONTROLS));

async fn run_blocking_control<T: Send + 'static>(
    operation: &'static str,
    f: impl FnOnce() -> T + Send + 'static,
) -> std::result::Result<T, Status> {
    let _permit = ADMIN_BLOCKING_CONTROL
        .acquire()
        .await
        .map_err(|_| Status::unavailable("cold tier admin control is shutting down"))?;
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|error| Status::internal(format!("{operation} worker failed: {error}")))
}

fn bounded_admin_limit(value: u64, max: u64) -> Option<usize> {
    if value > max {
        return None;
    }
    Some(value.max(1) as usize)
}

pub(super) async fn handle_trigger_cold_tier_offload(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::TriggerColdTierOffloadRequest>,
) -> std::result::Result<Response<pb::TriggerColdTierOffloadReply>, Status> {
    let request = request.into_inner();
    let Some(max_tasks) = bounded_admin_limit(request.max_tasks, MAX_ADMIN_OFFLOAD_TASKS) else {
        return Err(Status::invalid_argument(format!(
            "trigger_cold_tier_offload.max_tasks exceeds maximum {MAX_ADMIN_OFFLOAD_TASKS}"
        )));
    };
    let reply = run_blocking_control("trigger_cold_tier_offload", move || {
        match cold_tier.trigger_offload(max_tasks) {
            Ok(materialized) => pb::TriggerColdTierOffloadReply {
                materialized: materialized as u64,
                error: None,
            },
            Err(error) => pb::TriggerColdTierOffloadReply {
                materialized: 0,
                error: Some(pb_error(error)),
            },
        }
    })
    .await?;
    Ok(Response::new(reply))
}

pub(super) async fn handle_manual_cold_tier_gc(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::ManualColdTierGcRequest>,
) -> std::result::Result<Response<pb::ManualColdTierGcReply>, Status> {
    let request = request.into_inner();
    if request.device_id.is_empty() {
        return Err(Status::invalid_argument(
            "manual_cold_tier_gc requires device_id",
        ));
    }
    let Some(max_backings) = bounded_admin_limit(request.max_backings, MAX_ADMIN_GC_BACKINGS)
    else {
        return Err(Status::invalid_argument(format!(
            "manual_cold_tier_gc.max_backings exceeds maximum {MAX_ADMIN_GC_BACKINGS}"
        )));
    };
    let reply = run_blocking_control("manual_cold_tier_gc", move || {
        match cold_tier.manual_gc(&request.device_id, max_backings) {
            Ok(collected) => pb::ManualColdTierGcReply {
                collected: collected as u64,
                error: None,
            },
            Err(error) => pb::ManualColdTierGcReply {
                collected: 0,
                error: Some(pb_error(error)),
            },
        }
    })
    .await?;
    Ok(Response::new(reply))
}

pub(super) async fn handle_manual_cold_tier_free(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::ManualColdTierFreeRequest>,
) -> std::result::Result<Response<pb::ManualColdTierFreeReply>, Status> {
    let request = request.into_inner();
    if request.device_id.is_empty() {
        return Err(Status::invalid_argument(
            "manual_cold_tier_free requires device_id",
        ));
    }
    let Some(max_victims) = bounded_admin_limit(request.max_victims, MAX_ADMIN_FREE_VICTIMS) else {
        return Err(Status::invalid_argument(format!(
            "manual_cold_tier_free.max_victims exceeds maximum {MAX_ADMIN_FREE_VICTIMS}"
        )));
    };
    let reply = run_blocking_control("manual_cold_tier_free", move || {
        match cold_tier.manual_free(&request.device_id, max_victims) {
            Ok(result) => pb::ManualColdTierFreeReply {
                attempted_victims: result.attempted_victims as u64,
                freed_backings: result.freed_backings as u64,
                skipped_backings: result.skipped_backings as u64,
                reached_low_watermark: result.reached_low_watermark,
                error: None,
                collected_backings: result.collected_backings as u64,
            },
            Err(error) => pb::ManualColdTierFreeReply {
                attempted_victims: 0,
                freed_backings: 0,
                skipped_backings: 0,
                reached_low_watermark: false,
                error: Some(pb_error(error)),
                collected_backings: 0,
            },
        }
    })
    .await?;
    Ok(Response::new(reply))
}

pub(super) async fn handle_probe_cold_tier_device(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::ProbeColdTierDeviceRequest>,
) -> std::result::Result<Response<pb::ProbeColdTierDeviceReply>, Status> {
    let request = request.into_inner();
    if request.device_id.is_empty() {
        return Err(Status::invalid_argument(
            "probe_cold_tier_device requires device_id",
        ));
    }
    let reply = run_blocking_control("probe_cold_tier_device", move || {
        match cold_tier.probe_device(&request.device_id) {
            Ok(result) => pb::ProbeColdTierDeviceReply {
                device_id: result.device_id,
                capacity_bytes: result.capacity_bytes.unwrap_or_default(),
                used_bytes: result.used_bytes,
                reserved_bytes: result.reserved_bytes,
                schedulable: result.schedulable,
                state: result.state,
                last_error: result.last_error.unwrap_or_default(),
                error: None,
            },
            Err(error) => pb::ProbeColdTierDeviceReply {
                device_id: request.device_id,
                capacity_bytes: 0,
                used_bytes: 0,
                reserved_bytes: 0,
                schedulable: false,
                state: String::new(),
                last_error: String::new(),
                error: Some(pb_error(error)),
            },
        }
    })
    .await?;
    Ok(Response::new(reply))
}

pub(super) async fn handle_read_from_cold(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::ReadFromColdRequest>,
) -> std::result::Result<Response<pb::ReadFromColdReply>, Status> {
    let request = request.into_inner();
    if request.tenant.is_empty() || request.key.is_empty() {
        return Err(Status::invalid_argument(
            "read_from_cold requires non-empty tenant and key",
        ));
    }
    let (reply, deferred_promote) = run_blocking_control("read_from_cold", move || {
        cold_read_response_to_pb(cold_tier.read_from_cold(
            &request.namespace,
            &request.authority,
            &request.tenant,
            &request.key,
            &request.domain,
            &request.object_set,
        ))
    })
    .await?;
    spawn_deferred_promote(deferred_promote);
    Ok(Response::new(reply))
}

pub(super) async fn handle_batch_read_from_cold(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::BatchReadFromColdRequest>,
) -> std::result::Result<Response<pb::BatchReadFromColdReply>, Status> {
    let request = request.into_inner();
    for target in &request.targets {
        if target.tenant.is_empty() || target.key.is_empty() {
            return Err(Status::invalid_argument(
                "batch_read_from_cold requires non-empty tenant and key for every target",
            ));
        }
    }
    let namespace = request.namespace;
    let authority = request.authority;
    let targets = cold_read_targets_from_pb(request.targets);
    let responses = run_blocking_control("batch_read_from_cold", move || {
        cold_tier.batch_read_from_cold(&namespace, &authority, targets)
    })
    .await?;
    let mut results = Vec::with_capacity(responses.len());
    for response in responses {
        let (reply, deferred_promote) = cold_read_response_to_pb(response);
        results.push(reply);
        spawn_deferred_promote(deferred_promote);
    }
    Ok(Response::new(pb::BatchReadFromColdReply { results }))
}

pub(super) async fn handle_batch_reclaim_cold_backings(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::BatchReclaimColdBackingsRequest>,
) -> std::result::Result<Response<pb::BatchReclaimColdBackingsReply>, Status> {
    let request = request.into_inner();
    let mut cold_backings = Vec::with_capacity(request.cold_backings.len());
    for cold_backing in request.cold_backings {
        cold_backings.push(try_cold_backing_route(cold_backing).map_err(|error| {
            Status::invalid_argument(format!("invalid cold backing reclaim request: {error}"))
        })?);
    }
    let namespace = request.namespace;
    let authority = request.authority;
    let results = run_blocking_control("batch_reclaim_cold_backings", move || {
        cold_tier.batch_reclaim_cold_backings(&namespace, &authority, cold_backings)
    })
    .await?
    .into_iter()
    .map(cold_reclaim_result_to_pb)
    .collect();
    Ok(Response::new(pb::BatchReclaimColdBackingsReply { results }))
}

fn cold_reclaim_result_to_pb(
    result: mooncake_store_core::Result<ColdReclaimResult>,
) -> pb::ReclaimColdBackingReply {
    match result {
        Ok(result) => pb::ReclaimColdBackingReply {
            removed_cold_payload: result.removed_cold_payload,
            removed_pending_source: result.removed_pending_source,
            skipped_still_referenced: result.skipped_still_referenced,
            error: None,
        },
        Err(error) => pb::ReclaimColdBackingReply {
            removed_cold_payload: false,
            removed_pending_source: false,
            skipped_still_referenced: false,
            error: Some(pb_error(error)),
        },
    }
}

pub(super) async fn handle_ack_cold_read_complete(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::AckColdReadCompleteRequest>,
) -> std::result::Result<Response<pb::AckColdReadCompleteReply>, Status> {
    let request = request.into_inner();
    if request.segment_names.len() != request.segment_offsets.len() {
        return Err(Status::invalid_argument(
            "ack_cold_read_complete requires equal segment_names and segment_offsets lengths",
        ));
    }
    run_blocking_control("ack_cold_read_complete", move || {
        let slots = cold_read_slots_from_pb(request.segment_names, request.segment_offsets);
        cold_tier.ack_cold_read_complete(&slots);
    })
    .await?;
    Ok(Response::new(pb::AckColdReadCompleteReply {}))
}

pub(super) async fn handle_pin_for_read(
    cold_tier: Arc<dyn ColdTierControlService>,
    request: Request<pb::PinForReadRequest>,
) -> std::result::Result<Response<pb::PinForReadReply>, Status> {
    let request = request.into_inner();
    if request.segment_names.len() != request.segment_offsets.len() {
        return Err(Status::invalid_argument(
            "pin_for_read requires equal segment_names and segment_offsets lengths",
        ));
    }
    let pinned = run_blocking_control("pin_for_read", move || {
        let slots = cold_read_slots_from_pb(request.segment_names, request.segment_offsets);
        cold_tier.pin_for_read(&slots)
    })
    .await?;
    Ok(Response::new(pb::PinForReadReply { pinned }))
}

fn spawn_deferred_promote(deferred_promote: Option<Box<dyn FnOnce() + Send>>) {
    if let Some(promote) = deferred_promote {
        tokio::task::spawn_blocking(promote);
    }
}

fn cold_read_response_to_pb(
    response: ColdReadResponse,
) -> (pb::ReadFromColdReply, Option<Box<dyn FnOnce() + Send>>) {
    let reply = match response.result {
        Ok(result) => pb::ReadFromColdReply {
            segment_name: result.segment_name,
            segment_offset: result.segment_offset,
            length: result.length,
            checksum: result.checksum,
            target_chunks: result
                .target_chunks
                .into_iter()
                .map(|chunk| pb::SegmentTargetChunk {
                    logical_offset: chunk.logical_offset,
                    target_offset: chunk.target_offset,
                    length_bytes: chunk.length_bytes,
                })
                .collect(),
            transport_endpoint: result.transport_endpoint,
            transport_segment_descriptor: result.transport_segment_descriptor,
            error: None,
            admission_wait_us: result.admission_wait_us,
            ssd_read_us: result.ssd_read_us,
            memcpy_us: result.memcpy_us,
            route_update_us: result.route_update_us,
            total_promote_us: result.total_promote_us,
            backpressure: false,
        },
        Err(StoreError::Backpressure(_)) => pb::ReadFromColdReply {
            segment_name: String::new(),
            segment_offset: 0,
            length: 0,
            checksum: None,
            target_chunks: Vec::new(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            error: None,
            admission_wait_us: 0,
            ssd_read_us: 0,
            memcpy_us: 0,
            route_update_us: 0,
            total_promote_us: 0,
            backpressure: true,
        },
        Err(error) => pb::ReadFromColdReply {
            segment_name: String::new(),
            segment_offset: 0,
            length: 0,
            checksum: None,
            target_chunks: Vec::new(),
            transport_endpoint: None,
            transport_segment_descriptor: None,
            error: Some(pb_error(error)),
            admission_wait_us: 0,
            ssd_read_us: 0,
            memcpy_us: 0,
            route_update_us: 0,
            total_promote_us: 0,
            backpressure: false,
        },
    };
    (reply, response.deferred_promote)
}

pub(super) fn cold_read_targets_from_pb(targets: Vec<pb::ColdReadTarget>) -> Vec<ColdReadTarget> {
    targets
        .into_iter()
        .map(|target| ColdReadTarget {
            tenant: target.tenant,
            key: target.key,
            domain: target.domain,
            object_set: target.object_set,
        })
        .collect()
}

pub(super) fn cold_read_slots_from_pb(
    segment_names: Vec<String>,
    segment_offsets: Vec<u64>,
) -> Vec<(SegmentName, u64)> {
    segment_names
        .into_iter()
        .zip(segment_offsets)
        .map(|(name, offset)| (SegmentName::new(name), offset))
        .collect()
}
