use super::codec::pb_error;
use super::server::run_blocking_control;
use super::*;

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
