struct ResolvedObject {
    tenant: String,
    key: String,
    route: ObjectRoute,
    replica: ReplicaRoute,
    fallback_replicas: VecDeque<ReplicaRoute>,
}

fn copy_into_region(allocation: RegionAllocation, value: &[u8]) {
    unsafe {
        ptr::copy_nonoverlapping(value.as_ptr(), allocation.addr.cast::<u8>(), value.len());
    }
}

fn record_success_metric(operation: &'static str, bytes_in: u64, bytes_out: u64) {
    let result: Result<()> = Ok(());
    OperationTracker::new(operation)
        .input_bytes(bytes_in)
        .finish(&result, bytes_out);
}

fn payload_checksum(payload: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut checksum = FNV_OFFSET;
    for byte in payload {
        checksum ^= u64::from(*byte);
        checksum = checksum.wrapping_mul(FNV_PRIME);
    }
    checksum
}

fn validate_replica_checksum(replica: &ReplicaRoute, payload: &[u8]) -> Result<()> {
    let Some(expected) = replica.checksum else {
        registry::record_checksum_validation("missing");
        return Ok(());
    };
    let actual = payload_checksum(payload);
    if actual == expected {
        registry::record_checksum_validation("ok");
        return Ok(());
    }
    registry::record_checksum_validation("mismatch");
    Err(StoreError::InvalidState(format!(
        "checksum mismatch for {}:{} expected={} actual={}",
        replica.owner, replica.segment_name.0, expected, actual
    )))
}

fn flatten_slices(buffers: &[&[u8]]) -> Vec<u8> {
    let total = buffers.iter().map(|buffer| buffer.len()).sum();
    let mut payload = Vec::with_capacity(total);
    for buffer in buffers {
        payload.extend_from_slice(buffer);
    }
    payload
}

fn scatter_into_buffers(payload: &[u8], buffers: &mut [&mut [u8]]) {
    let mut cursor = 0usize;
    for buffer in buffers {
        if cursor >= payload.len() {
            buffer.fill(0);
            continue;
        }
        let remaining = payload.len() - cursor;
        let to_copy = remaining.min(buffer.len());
        buffer[..to_copy].copy_from_slice(&payload[cursor..cursor + to_copy]);
        if to_copy < buffer.len() {
            buffer[to_copy..].fill(0);
        }
        cursor += to_copy;
    }
}

fn compatibility_matches(left: &ClientLease, right: &ClientLease) -> bool {
    left.compatibility.is_compatible_with(&right.compatibility)
}

fn should_mark_runtime_suspect_after_allocator_error(error: &StoreError) -> bool {
    matches!(error, StoreError::Transport(_) | StoreError::Unsupported(_))
}

fn control_bind_host(rpc_address: &str) -> String {
    if rpc_address.is_empty() {
        return "127.0.0.1".to_string();
    }
    rpc_address
        .rsplit_once(':')
        .map(|(host, _)| host)
        .filter(|host| !host.is_empty() && *host != "0.0.0.0" && *host != "::")
        .unwrap_or("127.0.0.1")
        .to_string()
}

fn align_up_u64(value: u64, alignment: u64) -> u64 {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time should advance")
        .as_millis() as u64
}
