use super::codec::pb_error;
use super::*;

pub(super) fn cold_read_response_to_pb(
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
