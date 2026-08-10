fn recover_extent_store_segments(
    root: &ExtentStorePath,
    segment_size: u64,
) -> Result<ExtentStoreEngineInner> {
    let segments_dir = root.join("segments");
    let mut segment_ids = Vec::new();
    let entries = std::fs::read_dir(&segments_dir).map_err(|error| {
        StoreError::Transport(format!(
            "failed to list extent store segment directory {}: {error}",
            segments_dir.display()
        ))
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| {
            StoreError::Transport(format!(
                "failed to read extent store segment directory entry {}: {error}",
                segments_dir.display()
            ))
        })?;
        let path = entry.path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("seg") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Ok(segment_id) = u64::from_str_radix(stem, 16) else {
            continue;
        };
        segment_ids.push(segment_id);
    }
    segment_ids.sort_unstable();
    let mut inner = ExtentStoreEngineInner {
        segment_size,
        active_segment_id: 1,
        active_offset: 0,
        generation: 1,
        segments: BTreeMap::new(),
        free_extents: ExtentStoreFreeExtents::default(),
        delete_journal: open_delete_journal(root)?,
    };
    let mut empty_segment_ids = Vec::new();
    let max_recovered_segment_id = segment_ids.iter().copied().max().unwrap_or(0);
    for segment_id in segment_ids.iter().copied() {
        let mut segment = open_segment(root, segment_id, segment_size)?;
        let scan_len = segment.allocated_len;
        let live_bytes = recover_extent_store_segment_live_bytes(
            &segment.file,
            segment_id,
            scan_len,
            &mut segment.packed_blocks,
        )?;
        if live_bytes == 0 {
            empty_segment_ids.push(segment_id);
            continue;
        }
        segment.live_bytes = live_bytes;
        inner.active_segment_id = segment_id;
        inner.active_offset = live_bytes;
        inner.segments.insert(segment_id, segment);
    }
    recover_extent_store_delete_journal(root, &mut inner)?;
    remove_fully_dead_sealed_segments(&mut inner, max_recovered_segment_id)?;
    if inner.active_offset >= inner.segment_size {
        inner.active_segment_id = inner.active_segment_id.checked_add(1).ok_or_else(|| {
            StoreError::InvalidState("extent store segment id overflow during recovery".to_string())
        })?;
        inner.active_offset = 0;
    }
    if !inner.segments.contains_key(&inner.active_segment_id) {
        let segment = open_segment(root, inner.active_segment_id, inner.segment_size)?;
        inner.segments.insert(inner.active_segment_id, segment);
    }
    for segment_id in empty_segment_ids {
        if inner.segments.contains_key(&segment_id) {
            continue;
        }
        let path = root.join("segments").join(format!("{segment_id:016x}.seg"));
        remove_extent_store_segment_file(&path)?;
    }
    compact_delete_journal(root, &inner)?;
    inner.delete_journal = open_delete_journal(root)?;
    Ok(inner)
}

fn recover_extent_store_delete_journal(
    root: &ExtentStorePath,
    inner: &mut ExtentStoreEngineInner,
) -> Result<()> {
    let path = delete_journal_path(root);
    if !path.exists() {
        return Ok(());
    }
    let mut file = OpenOptions::new().read(true).open(&path).map_err(|error| {
        StoreError::Transport(format!(
            "failed to open extent store delete journal {}: {error}",
            path.display()
        ))
    })?;
    let mut offset = 0u64;
    let mut record = [0u8; EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN];
    loop {
        let mut read = 0usize;
        while read < record.len() {
            let bytes = file.read(&mut record[read..]).map_err(|error| {
                StoreError::Transport(format!(
                    "failed to read extent store delete journal {} at {offset}: {error}",
                    path.display()
                ))
            })?;
            if bytes == 0 {
                if read == 0 {
                    return Ok(());
                }
                return Err(StoreError::InvalidState(format!(
                    "extent store delete journal {} has partial record at offset {offset}",
                    path.display()
                )));
            }
            read += bytes;
        }
        let locator = decode_delete_journal_record(&record, offset)?;
        inner.generation =
            inner
                .generation
                .max(next_extent_store_generation_after(locator.generation)?);
        apply_recovered_delete_locator(inner, &locator)?;
        offset = offset.saturating_add(record.len() as u64);
    }
}

fn apply_recovered_delete_locator(
    inner: &mut ExtentStoreEngineInner,
    locator: &ExtentStoreLocator,
) -> Result<()> {
    let Some(segment) = inner.segments.get_mut(&locator.segment_id) else {
        return Ok(());
    };
    let append_bytes = segment.live_bytes.saturating_add(segment.dead_bytes);
    if locator.record_len == 0
        || locator.offset >= append_bytes
        || locator.offset.saturating_add(locator.record_len) > append_bytes
        || locator.value_offset.saturating_add(locator.value_len) > locator.record_len
        || (segment.packed_blocks.contains_key(&(locator.offset, locator.record_len))
            && locator.value_offset < EXTENT_STORE_HEADER_LEN as u64)
    {
        return Err(StoreError::InvalidState(format!(
            "extent store delete journal references invalid segment {} extent {}+{}",
            locator.segment_id, locator.offset, locator.record_len
        )));
    }
    let Some(delete_result) = apply_delete_to_segment(segment, locator) else {
        return Ok(());
    };
    if let Some((offset, len)) = delete_result.free_extent {
        inner.free_extents.insert_exact(locator.segment_id, offset, len);
    }
    Ok(())
}

fn recover_extent_store_segment_live_bytes(
    file: &ExtentStoreFile,
    segment_id: u64,
    segment_size: u64,
    packed_blocks: &mut BTreeMap<(u64, u64), PackedBlockLiveState>,
) -> Result<u64> {
    let mut offset = 0u64;
    let mut header = [0u8; EXTENT_STORE_HEADER_LEN];
    while offset + EXTENT_STORE_HEADER_LEN as u64 <= segment_size {
        let read = file.read_at(&mut header, offset).map_err(|error| {
            StoreError::Transport(format!(
                "failed to read extent store segment {segment_id} header at {offset}: {error}"
            ))
        })?;
        if read == 0 || header.iter().all(|byte| *byte == 0) {
            break;
        }
        if read != EXTENT_STORE_HEADER_LEN {
            return Err(StoreError::InvalidState(format!(
                "extent store segment {segment_id} has partial header at offset {offset}"
            )));
        }
        let Some(recovered) = decode_extent_store_recovery_record(&header, segment_id, offset)? else {
            break;
        };
        let record_len = recovered.record_len;
        if record_len == 0 || offset.saturating_add(record_len) > segment_size {
            return Err(StoreError::InvalidState(format!(
                "extent store segment {segment_id} has invalid recovered record length {record_len} at offset {offset}"
            )));
        }
        if recovered.record_kind == EXTENT_STORE_RECORD_KIND_PACKED && recovered.key_len > 0 {
            validate_recovered_packed_record_payload(RecoveredPackedRecordPayload {
                file,
                segment_id,
                offset,
                record_len,
                value_offset: recovered.value_offset,
                value_len: recovered.value_len,
                checksum: recovered.checksum,
                entry_count: recovered.key_len,
            })?;
        } else {
            validate_extent_store_recovery_record_payload(
                file,
                segment_id,
                offset,
                recovered.value_offset,
                recovered.value_len,
                recovered.checksum,
            )?;
        }
        if recovered.record_kind == EXTENT_STORE_RECORD_KIND_PACKED {
            register_recovered_packed_block(packed_blocks, offset, &recovered)?;
        }
        offset = offset.saturating_add(record_len);
    }
    Ok(offset)
}

struct ExtentStoreRecoveredRecordHeader {
    record_len: u64,
    key_len: u64,
    value_offset: u64,
    value_len: u64,
    checksum: Option<u64>,
    record_kind: u16,
}

fn decode_extent_store_recovery_record(
    header: &[u8; EXTENT_STORE_HEADER_LEN],
    segment_id: u64,
    offset: u64,
) -> Result<Option<ExtentStoreRecoveredRecordHeader>> {
    let magic = u32::from_le_bytes(header[0..4].try_into().expect("header magic slice length"));
    if magic == 0 {
        return Ok(None);
    }
    if magic != EXTENT_STORE_MAGIC {
        return Err(StoreError::InvalidState(format!(
            "extent store segment {segment_id} has invalid magic at offset {offset}"
        )));
    }
    let header_len = u16::from_le_bytes(
        header[4..6]
            .try_into()
            .expect("header length slice length"),
    ) as u64;
    if header_len != EXTENT_STORE_HEADER_LEN as u64 {
        return Err(StoreError::InvalidState(format!(
            "extent store segment {segment_id} has unsupported header at offset {offset}"
        )));
    }
    let record_kind = u16::from_le_bytes(
        header[6..8]
            .try_into()
            .expect("record kind slice length"),
    );
    if record_kind != EXTENT_STORE_RECORD_KIND_SINGLE
        && record_kind != EXTENT_STORE_RECORD_KIND_PACKED
    {
        return Err(StoreError::InvalidState(format!(
            "extent store segment {segment_id} has unsupported record kind {record_kind} at offset {offset}"
        )));
    }
    let key_len = u64::from_le_bytes(header[8..16].try_into().expect("key length slice length"));
    let value_len = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .expect("value length slice length"),
    );
    let checksum = Some(u64::from_le_bytes(
        header[24..32]
            .try_into()
            .expect("payload checksum slice length"),
    ));
    let value_offset = u64::from_le_bytes(
        header[32..40]
            .try_into()
            .expect("value offset slice length"),
    );
    let record_len = u64::from_le_bytes(
        header[40..48]
            .try_into()
            .expect("record length slice length"),
    );
    if value_offset < EXTENT_STORE_HEADER_LEN as u64 + key_len
        || value_offset.saturating_add(value_len) > record_len
    {
        return Err(StoreError::InvalidState(format!(
            "extent store segment {segment_id} has invalid payload bounds at offset {offset}"
        )));
    }
    Ok(Some(ExtentStoreRecoveredRecordHeader {
        record_len,
        key_len,
        value_offset,
        value_len,
        checksum,
        record_kind,
    }))
}

fn validate_single_record_header(buffer: &[u8], locator: &ExtentStoreLocator) -> Result<()> {
    if buffer.len() != locator.record_len as usize || buffer.len() < EXTENT_STORE_HEADER_LEN {
        return Err(StoreError::InvalidState(
            "extent store single record bounds mismatch".to_string(),
        ));
    }
    let magic = u32::from_le_bytes(buffer[0..4].try_into().expect("header magic slice length"));
    let record_kind = u16::from_le_bytes(
        buffer[6..8]
            .try_into()
            .expect("record kind slice length"),
    );
    let value_len = u64::from_le_bytes(
        buffer[16..24]
            .try_into()
            .expect("value length slice length"),
    );
    let value_offset = u64::from_le_bytes(
        buffer[32..40]
            .try_into()
            .expect("value offset slice length"),
    );
    let record_len = u64::from_le_bytes(
        buffer[40..48]
            .try_into()
            .expect("record length slice length"),
    );
    if magic != EXTENT_STORE_MAGIC
        || record_kind != EXTENT_STORE_RECORD_KIND_SINGLE
        || value_offset != locator.value_offset
        || value_len != locator.value_len
        || record_len != locator.record_len
        || value_offset.saturating_add(value_len) > record_len
    {
        return Err(StoreError::InvalidState(
            "extent store single record header mismatch".to_string(),
        ));
    }
    let value_start = value_offset as usize;
    let value_end = value_start
        .checked_add(value_len as usize)
        .ok_or_else(|| StoreError::InvalidState("extent store single payload bounds overflow".to_string()))?;
    if value_end > buffer.len() {
        return Err(StoreError::InvalidState(
            "extent store single payload exceeds record bounds".to_string(),
        ));
    }
    Ok(())
}

fn validate_pinned_record(locator: &ExtentStoreLocator, record: &[u8], packed: bool) -> Result<()> {
    if packed {
        validate_packed_block_buffer(record, locator)?;
        validate_packed_block_entry(record, locator)?;
        return Ok(());
    }
    validate_single_record_header(record, locator)
}

fn validate_packed_block_entry(buffer: &[u8], locator: &ExtentStoreLocator) -> Result<()> {
    let entry_count = packed_block_entry_count_from_buffer(buffer)?;
    let value_base = u64::from_le_bytes(
        buffer[32..40]
            .try_into()
            .expect("value offset slice length"),
    );
    let value_offset = locator.value_offset;
    let value_len = locator.value_len;
    let value_start = value_offset as usize;
    let value_end = value_start
        .checked_add(value_len as usize)
        .ok_or_else(|| StoreError::InvalidState("extent store packed entry bounds overflow".to_string()))?;
    if value_end > buffer.len() || value_offset < value_base {
        return Err(StoreError::InvalidState(
            "extent store packed block entry exceeds block bounds".to_string(),
        ));
    }
    let relative_offset = value_offset - value_base;
    for entry_index in 0..entry_count {
        let (entry_offset, entry_len, entry_checksum) = packed_block_index_entry(buffer, entry_index);
        if (entry_offset, entry_len) != (relative_offset, value_len) {
            continue;
        }
        let payload = &buffer[value_start..value_end];
        if entry_checksum != 0 {
            let actual = payload_checksum(payload);
            if actual != entry_checksum {
                return Err(StoreError::InvalidState(format!(
                    "extent store packed entry checksum mismatch: expected {entry_checksum} actual {actual}"
                )));
            }
        }
        return Ok(());
    }
    Err(StoreError::InvalidState(
        "extent store packed entry not found in block index".to_string(),
    ))
}

fn validate_packed_block_buffer(buffer: &[u8], locator: &ExtentStoreLocator) -> Result<()> {
    if buffer.len() < EXTENT_STORE_HEADER_LEN {
        return Err(StoreError::InvalidState(
            "extent store packed block is smaller than header".to_string(),
        ));
    }
    let magic = u32::from_le_bytes(buffer[0..4].try_into().expect("header magic slice length"));
    let record_kind = u16::from_le_bytes(
        buffer[6..8]
            .try_into()
            .expect("record kind slice length"),
    );
    let record_len = u64::from_le_bytes(
        buffer[40..48]
            .try_into()
            .expect("record length slice length"),
    );
    if magic != EXTENT_STORE_MAGIC
        || record_kind != EXTENT_STORE_RECORD_KIND_PACKED
        || record_len != locator.record_len
    {
        return Err(StoreError::InvalidState(
            "extent store packed block header mismatch".to_string(),
        ));
    }
    Ok(())
}

fn read_packed_block_entries(
    buffer: &[u8],
    reads: &mut [(usize, ReservedExtentStoreRead<'_>)],
    indices: &[usize],
) -> Result<()> {
    let entry_count = packed_block_entry_count_from_buffer(buffer)?;
    let value_base = u64::from_le_bytes(
        buffer[32..40]
            .try_into()
            .expect("value offset slice length"),
    );
    let mut cursor = 0usize;
    for index in indices {
        let read = &mut reads[*index].1;
        let value_offset = read.locator.value_offset;
        let value_len = read.locator.value_len;
        let value_start = value_offset as usize;
        let value_end = value_start
            .checked_add(value_len as usize)
            .ok_or_else(|| StoreError::InvalidState("extent store packed entry bounds overflow".to_string()))?;
        if value_end > buffer.len() || value_offset < value_base {
            return Err(StoreError::InvalidState(
                "extent store packed block entry exceeds block bounds".to_string(),
            ));
        }
        let relative_offset = value_offset - value_base;
        while cursor < entry_count {
            let (entry_offset, entry_len, _) = packed_block_index_entry(buffer, cursor);
            if (entry_offset, entry_len) >= (relative_offset, value_len) {
                break;
            }
            cursor += 1;
        }
        if cursor == entry_count {
            return Err(StoreError::InvalidState(
                "extent store packed entry not found in block index".to_string(),
            ));
        }
        let (entry_offset, entry_len, entry_checksum) = packed_block_index_entry(buffer, cursor);
        if (entry_offset, entry_len) != (relative_offset, value_len) {
            return Err(StoreError::InvalidState(
                "extent store packed entry not found in block index".to_string(),
            ));
        }
        let value_len = value_len as usize;
        let payload = &buffer[value_start..value_end];
        if entry_checksum != 0 {
            let actual = payload_checksum(payload);
            if actual != entry_checksum {
                return Err(StoreError::InvalidState(format!(
                    "extent store packed entry checksum mismatch: expected {entry_checksum} actual {actual}"
                )));
            }
        }
        read.dst[..value_len].copy_from_slice(payload);
    }
    Ok(())
}

fn packed_block_entry_count_from_buffer(buffer: &[u8]) -> Result<usize> {
    let entry_count = u64::from_le_bytes(buffer[8..16].try_into().expect("entry count slice length"));
    let entry_count = usize::try_from(entry_count).map_err(|_| {
        StoreError::InvalidState("extent store packed block entry count overflows usize".to_string())
    })?;
    let index_bytes = entry_count
        .checked_mul(EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize)
        .ok_or_else(|| StoreError::InvalidState("extent store packed index size overflow".to_string()))?;
    let index_end = EXTENT_STORE_HEADER_LEN
        .checked_add(index_bytes)
        .ok_or_else(|| StoreError::InvalidState("extent store packed index bounds overflow".to_string()))?;
    if index_end > buffer.len() {
        return Err(StoreError::InvalidState(
            "extent store packed index bounds mismatch".to_string(),
        ));
    }
    Ok(entry_count)
}

fn packed_block_index_entry(buffer: &[u8], entry_index: usize) -> (u64, u64, u64) {
    let index_offset = EXTENT_STORE_HEADER_LEN + entry_index * EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize;
    let value_offset = u64::from_le_bytes(
        buffer[index_offset..index_offset + 8]
            .try_into()
            .expect("packed entry offset slice length"),
    );
    let value_len = u64::from_le_bytes(
        buffer[index_offset + 8..index_offset + 16]
            .try_into()
            .expect("packed entry length slice length"),
    );
    let checksum = u64::from_le_bytes(
        buffer[index_offset + 16..index_offset + 24]
            .try_into()
            .expect("packed entry checksum slice length"),
    );
    (value_offset, value_len, checksum)
}

fn maybe_validate_packed_block_payload(buffer: &[u8], locator: &ExtentStoreLocator) -> Result<()> {
    if locator.record_len > EXTENT_STORE_PACKED_BLOCK_CHECKSUM_MAX_READ_BYTES {
        return Ok(());
    }
    validate_packed_block_payload(buffer, locator)
}

fn validate_packed_block_payload(buffer: &[u8], locator: &ExtentStoreLocator) -> Result<()> {
    let value_offset = u64::from_le_bytes(
        buffer[32..40]
            .try_into()
            .expect("value offset slice length"),
    );
    let value_len = u64::from_le_bytes(
        buffer[16..24]
            .try_into()
            .expect("value length slice length"),
    );
    let expected = u64::from_le_bytes(
        buffer[24..32]
            .try_into()
            .expect("payload checksum slice length"),
    );
    let value_start = value_offset as usize;
    let value_end = value_start
        .checked_add(value_len as usize)
        .ok_or_else(|| StoreError::InvalidState("extent store packed payload bounds overflow".to_string()))?;
    if value_end > buffer.len() || value_offset.saturating_add(value_len) > locator.record_len {
        return Err(StoreError::InvalidState(
            "extent store packed payload exceeds block bounds".to_string(),
        ));
    }
    let entry_count = packed_block_entry_count_from_buffer(buffer)?;
    let index_start = EXTENT_STORE_HEADER_LEN;
    let index_end = index_start + entry_count * EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize;
    if packed_entry_checksum_from_index(&buffer[index_start..index_end]) == expected {
        return Ok(());
    }
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    for entry_index in 0..entry_count {
        let (entry_offset, entry_len, entry_checksum) = packed_block_index_entry(buffer, entry_index);
        if entry_offset.saturating_add(entry_len) > value_len {
            return Err(StoreError::InvalidState(
                "extent store packed entry exceeds block payload bounds".to_string(),
            ));
        }
        let actual = payload_checksum(
            &buffer[value_start + entry_offset as usize..value_start + (entry_offset + entry_len) as usize],
        );
        if entry_checksum != 0 && actual != entry_checksum {
            return Err(StoreError::InvalidState(format!(
                "extent store packed entry checksum mismatch: expected {entry_checksum} actual {actual}"
            )));
        }
        hasher.update(&entry_offset.to_le_bytes());
        hasher.update(&entry_len.to_le_bytes());
        hasher.update(&actual.to_le_bytes());
    }
    let actual = hasher.digest();
    if expected != actual {
        let legacy_actual = payload_checksum(&buffer[value_start..value_end]);
        if expected != legacy_actual {
            return Err(StoreError::InvalidState(format!(
                "extent store packed payload checksum mismatch: expected {expected} actual {actual} legacy_actual {legacy_actual}"
            )));
        }
    }
    Ok(())
}

fn register_recovered_packed_block(
    packed_blocks: &mut BTreeMap<(u64, u64), PackedBlockLiveState>,
    offset: u64,
    recovered: &ExtentStoreRecoveredRecordHeader,
) -> Result<()> {
    let entry_count = recovered.key_len;
    let index_len = entry_count.saturating_mul(EXTENT_STORE_PACKED_ENTRY_INDEX_LEN);
    let expected_value_offset = align_up(
        EXTENT_STORE_HEADER_LEN as u64 + index_len,
        EXTENT_STORE_ALIGNMENT,
    );
    if entry_count == 0 || entry_count > u32::MAX as u64 || recovered.value_offset != expected_value_offset {
        return Err(StoreError::InvalidState(format!(
            "extent store recovered packed block at {offset} has invalid entry count {entry_count} value_offset {} expected {expected_value_offset}",
            recovered.value_offset
        )));
    }
    packed_blocks.insert(
        (offset, recovered.record_len),
        PackedBlockLiveState {
            entry_count: entry_count as u32,
            live_count: entry_count as u32,
            live_value_bytes: recovered.value_len,
            dead_value_bytes: 0,
        },
    );
    Ok(())
}

fn validate_recovered_packed_record_payload(payload: RecoveredPackedRecordPayload<'_>) -> Result<()> {
    let RecoveredPackedRecordPayload {
        file,
        segment_id,
        offset,
        record_len,
        value_offset,
        value_len,
        checksum,
        entry_count,
    } = payload;
    let Some(expected) = checksum else {
        return Ok(());
    };
    if value_offset.saturating_add(value_len) > record_len {
        return Err(StoreError::InvalidState(format!(
            "extent store segment {segment_id} packed payload bounds mismatch at offset {offset}"
        )));
    }
    let mut payload = vec![0u8; value_len as usize];
    file.read_exact_at(&mut payload, offset + value_offset)
        .map_err(|error| {
            StoreError::Transport(format!(
                "failed to read extent store segment {segment_id} packed payload at offset {offset}: {error}"
            ))
        })?;
    let entry_count = usize::try_from(entry_count).map_err(|_| {
        StoreError::InvalidState("extent store packed entry count overflows usize".to_string())
    })?;
    let mut index_buffer = vec![0u8; entry_count * EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize];
    file.read_exact_at(&mut index_buffer, offset + EXTENT_STORE_HEADER_LEN as u64)
        .map_err(|error| {
            StoreError::Transport(format!(
                "failed to read extent store segment {segment_id} packed index at offset {offset}: {error}"
            ))
        })?;
    if packed_entry_checksum_from_index(&index_buffer) == expected {
        return Ok(());
    }
    let mut actual_hasher = xxhash_rust::xxh3::Xxh3::new();
    for entry_index in 0..entry_count {
        let index_offset = entry_index * EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize;
        let entry = &index_buffer[index_offset..index_offset + EXTENT_STORE_PACKED_ENTRY_INDEX_LEN as usize];
        let entry_offset = u64::from_le_bytes(
            entry[0..8]
                .try_into()
                .expect("packed entry offset slice length"),
        );
        let entry_len = u64::from_le_bytes(
            entry[8..16]
                .try_into()
                .expect("packed entry length slice length"),
        );
        let entry_checksum = u64::from_le_bytes(
            entry[16..24]
                .try_into()
                .expect("packed entry checksum slice length"),
        );
        if entry_offset.saturating_add(entry_len) > value_len {
            return Err(StoreError::InvalidState(format!(
                "extent store segment {segment_id} packed entry bounds mismatch at offset {offset}"
            )));
        }
        let actual = payload_checksum(&payload[entry_offset as usize..(entry_offset + entry_len) as usize]);
        if entry_checksum != 0 && actual != entry_checksum {
            return Err(StoreError::InvalidState(format!(
                "extent store segment {segment_id} packed entry checksum mismatch at offset {offset}: expected {entry_checksum} actual {actual}"
            )));
        }
        actual_hasher.update(&entry_offset.to_le_bytes());
        actual_hasher.update(&entry_len.to_le_bytes());
        actual_hasher.update(&actual.to_le_bytes());
    }
    let actual = actual_hasher.digest();
    if actual != expected {
        let legacy_actual = payload_checksum(&payload);
        if legacy_actual != expected {
            return Err(StoreError::InvalidState(format!(
                "extent store segment {segment_id} packed payload checksum mismatch at offset {offset}: expected {expected} actual {actual} legacy_actual {legacy_actual}"
            )));
        }
    }
    Ok(())
}

fn packed_entry_checksum_from_index(index: &[u8]) -> u64 {
    xxhash_rust::xxh3::xxh3_64(index)
}

fn validate_extent_store_recovery_record_payload(
    file: &ExtentStoreFile,
    segment_id: u64,
    offset: u64,
    value_offset: u64,
    value_len: u64,
    checksum: Option<u64>,
) -> Result<()> {
    let Some(checksum) = checksum else {
        return Ok(());
    };
    let mut remaining = value_len;
    let mut read_offset = offset.saturating_add(value_offset);
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buffer = vec![0u8; EXTENT_STORE_ALIGNMENT as usize * 16];
    while remaining > 0 {
        let read_len = remaining.min(buffer.len() as u64) as usize;
        let read = file.read_at(&mut buffer[..read_len], read_offset).map_err(|error| {
            StoreError::Transport(format!(
                "failed to read extent store segment {segment_id} payload at {read_offset}: {error}"
            ))
        })?;
        if read != read_len {
            return Err(StoreError::InvalidState(format!(
                "extent store segment {segment_id} has partial payload at offset {read_offset}"
            )));
        }
        hasher.update(&buffer[..read]);
        read_offset = read_offset.saturating_add(read as u64);
        remaining -= read as u64;
    }
    let actual = hasher.digest();
    if actual != checksum {
        return Err(StoreError::InvalidState(format!(
            "extent store segment {segment_id} payload checksum mismatch at offset {offset}: expected {checksum} actual {actual}"
        )));
    }
    Ok(())
}

fn open_delete_journal(root: &ExtentStorePath) -> Result<ExtentStoreDeleteJournal> {
    let meta_dir = root.join("meta");
    std::fs::create_dir_all(&meta_dir).map_err(|error| {
        StoreError::Transport(format!(
            "failed to create extent store metadata directory {}: {error}",
            meta_dir.display()
        ))
    })?;
    let path = delete_journal_path(root);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .append(true)
        .read(true)
        .mode(0o600)
        .open(&path)
        .map_err(|error| {
            StoreError::Transport(format!(
                "failed to open extent store delete journal {}: {error}",
                path.display()
            ))
        })?;
    Ok(ExtentStoreDeleteJournal { file })
}

fn delete_journal_path(root: &ExtentStorePath) -> ExtentStorePathBuf {
    root.join("meta").join("delete-journal-current.log")
}

fn append_delete_journal_record(
    journal: &mut ExtentStoreDeleteJournal,
    locator: &ExtentStoreLocator,
) -> Result<()> {
    let record = encode_delete_journal_record(locator);
    journal.file.write_all(&record).map_err(|error| {
        StoreError::Transport(format!("failed to write extent store delete journal: {error}"))
    })?;
    journal.file.sync_data().map_err(|error| {
        StoreError::Transport(format!("failed to sync extent store delete journal: {error}"))
    })?;
    Ok(())
}

fn encode_delete_journal_record(locator: &ExtentStoreLocator) -> [u8; EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN] {
    let mut record = [0u8; EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN];
    record[0..4].copy_from_slice(&EXTENT_STORE_DELETE_JOURNAL_MAGIC.to_le_bytes());
    record[4..6].copy_from_slice(&(EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN as u16).to_le_bytes());
    record[6..8].copy_from_slice(&EXTENT_STORE_DELETE_JOURNAL_VERSION.to_le_bytes());
    record[8..16].copy_from_slice(&locator.segment_id.to_le_bytes());
    record[16..24].copy_from_slice(&locator.offset.to_le_bytes());
    record[24..32].copy_from_slice(&locator.record_len.to_le_bytes());
    record[32..40].copy_from_slice(&locator.value_offset.to_le_bytes());
    record[40..48].copy_from_slice(&locator.value_len.to_le_bytes());
    record[48..56].copy_from_slice(&locator.generation.to_le_bytes());
    let checksum = xxhash_rust::xxh3::xxh3_64(&record[0..56]);
    record[56..64].copy_from_slice(&checksum.to_le_bytes());
    record
}

fn decode_delete_journal_record(
    record: &[u8; EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN],
    offset: u64,
) -> Result<ExtentStoreLocator> {
    let magic = u32::from_le_bytes(record[0..4].try_into().expect("journal magic slice length"));
    if magic != EXTENT_STORE_DELETE_JOURNAL_MAGIC {
        return Err(StoreError::InvalidState(format!(
            "extent store delete journal has invalid magic at offset {offset}"
        )));
    }
    let record_len = u16::from_le_bytes(record[4..6].try_into().expect("journal length slice length"));
    let version = u16::from_le_bytes(record[6..8].try_into().expect("journal version slice length"));
    if record_len as usize != EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN
        || version != EXTENT_STORE_DELETE_JOURNAL_VERSION
    {
        return Err(StoreError::InvalidState(format!(
            "extent store delete journal has unsupported record at offset {offset}"
        )));
    }
    let expected = u64::from_le_bytes(record[56..64].try_into().expect("journal checksum slice length"));
    let actual = xxhash_rust::xxh3::xxh3_64(&record[0..56]);
    if expected != actual {
        return Err(StoreError::InvalidState(format!(
            "extent store delete journal checksum mismatch at offset {offset}: expected {expected} actual {actual}"
        )));
    }
    Ok(ExtentStoreLocator {
        segment_id: u64::from_le_bytes(record[8..16].try_into().expect("segment id slice length")),
        offset: u64::from_le_bytes(record[16..24].try_into().expect("offset slice length")),
        record_len: u64::from_le_bytes(record[24..32].try_into().expect("record len slice length")),
        value_offset: u64::from_le_bytes(record[32..40].try_into().expect("value offset slice length")),
        value_len: u64::from_le_bytes(record[40..48].try_into().expect("value len slice length")),
        generation: u64::from_le_bytes(record[48..56].try_into().expect("generation slice length")),
    })
}

fn remove_fully_dead_sealed_segments(
    inner: &mut ExtentStoreEngineInner,
    max_recovered_segment_id: u64,
) -> Result<()> {
    let remove = inner
        .segments
        .iter()
        .filter(|(segment_id, segment)| {
            segment.live_bytes == 0 && **segment_id < max_recovered_segment_id
        })
        .map(|(segment_id, segment)| (*segment_id, segment.path.clone()))
        .collect::<Vec<_>>();
    for (segment_id, path) in remove {
        inner.segments.remove(&segment_id);
        inner.free_extents.remove_segment(segment_id);
        remove_extent_store_segment_file(&path)?;
    }
    Ok(())
}

fn compact_delete_journal(root: &ExtentStorePath, inner: &ExtentStoreEngineInner) -> Result<()> {
    let path = delete_journal_path(root);
    if !path.exists() {
        return Ok(());
    }
    let mut file = OpenOptions::new().read(true).open(&path).map_err(|error| {
        StoreError::Transport(format!(
            "failed to open extent store delete journal {} for compaction: {error}",
            path.display()
        ))
    })?;
    let mut retained: Vec<Vec<u8>> = Vec::new();
    let mut offset = 0u64;
    let mut record = [0u8; EXTENT_STORE_DELETE_JOURNAL_RECORD_LEN];
    loop {
        let mut read = 0usize;
        while read < record.len() {
            let bytes = file.read(&mut record[read..]).map_err(|error| {
                StoreError::Transport(format!(
                    "failed to read extent store delete journal {} at {offset} during compaction: {error}",
                    path.display()
                ))
            })?;
            if bytes == 0 {
                if read == 0 {
                    let compacted = path.with_extension("log.compacted");
                    let mut output = OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .mode(0o600)
                        .open(&compacted)
                        .map_err(|error| {
                            StoreError::Transport(format!(
                                "failed to create compacted extent store delete journal {}: {error}",
                                compacted.display()
                            ))
                        })?;
                    for retained_record in &retained {
                        output.write_all(retained_record).map_err(|error| {
                            StoreError::Transport(format!(
                                "failed to write compacted extent store delete journal {}: {error}",
                                compacted.display()
                            ))
                        })?;
                    }
                    output.sync_data().map_err(|error| {
                        StoreError::Transport(format!(
                            "failed to sync compacted extent store delete journal {}: {error}",
                            compacted.display()
                        ))
                    })?;
                    std::fs::rename(&compacted, &path).map_err(|error| {
                        StoreError::Transport(format!(
                            "failed to replace extent store delete journal {} with compacted file {}: {error}",
                            path.display(),
                            compacted.display()
                        ))
                    })?;
                    if let Some(parent) = path.parent() {
                        let dir = OpenOptions::new().read(true).open(parent).map_err(|error| {
                            StoreError::Transport(format!(
                                "failed to open extent store delete journal parent {}: {error}",
                                parent.display()
                            ))
                        })?;
                        dir.sync_all().map_err(|error| {
                            StoreError::Transport(format!(
                                "failed to sync extent store delete journal parent {}: {error}",
                                parent.display()
                            ))
                        })?;
                    }
                    return Ok(());
                }
                return Err(StoreError::InvalidState(format!(
                    "extent store delete journal {} has partial record at offset {offset}",
                    path.display()
                )));
            }
            read += bytes;
        }
        let locator = decode_delete_journal_record(&record, offset)?;
        if inner.segments.contains_key(&locator.segment_id) {
            retained.push(record.to_vec());
        }
        offset = offset.saturating_add(record.len() as u64);
    }
}

fn remove_extent_store_segment_file(path: &ExtentStorePath) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StoreError::Transport(format!(
            "failed to remove extent store segment file {}: {error}",
            path.display()
        ))),
    }
}

fn extent_store_pinned_error_allows_owned_fallback(error: &StoreError) -> bool {
    match error {
        StoreError::QuotaExceeded { .. } | StoreError::Transport(_) => true,
        StoreError::InvalidState(message) => message.contains("pinned read not submitted")
            || message.contains("mmap cache quota exceeded")
            || message.contains("pinned payload quota exceeded")
            || message.contains("mmap slice exceeds segment bounds"),
        _ => false,
    }
}

fn open_segment(
    root: &ExtentStorePath,
    segment_id: u64,
    _segment_size: u64,
) -> Result<ExtentStoreSegment> {
    let path = root.join("segments").join(format!("{segment_id:016x}.seg"));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(&path)
        .map_err(|error| {
            StoreError::Transport(format!(
                "failed to open extent store segment {}: {error}",
                path.display()
            ))
        })?;
    let allocated_len = file
        .metadata()
        .map_err(|error| {
            StoreError::Transport(format!(
                "failed to stat extent store segment {}: {error}",
                path.display()
            ))
        })?
        .len();
    let direct_file = open_direct_segment(&path)?;
    Ok(ExtentStoreSegment {
        file: Arc::new(file),
        direct_file: direct_file.map(Arc::new),
        path,
        allocated_len,
        live_bytes: 0,
        dead_bytes: 0,
        deleted_extents: BTreeSet::new(),
        packed_blocks: BTreeMap::new(),
    })
}

fn ensure_segment_allocated(
    segment: &mut ExtentStoreSegment,
    required_len: u64,
    segment_size: u64,
    preallocate: bool,
) -> Result<()> {
    if required_len <= segment.allocated_len {
        return Ok(());
    }
    let target_len = round_up_extent_store_allocation(required_len, segment_size)?;
    extent_store_off_t(target_len, &segment.path)?;
    if !preallocate {
        segment.allocated_len = target_len;
        return Ok(());
    }
    preallocate_segment_range(
        &segment.file,
        &segment.path,
        segment.allocated_len,
        target_len,
    )?;
    segment.allocated_len = target_len;
    Ok(())
}

fn extent_store_expansion_chunk(segment_size: u64) -> u64 {
    segment_size
        .max(EXTENT_STORE_ALIGNMENT)
        .min(DEFAULT_EXTENT_STORE_EXPANSION_CHUNK_BYTES)
}

fn round_up_extent_store_allocation(required_len: u64, segment_size: u64) -> Result<u64> {
    if required_len == 0 {
        return Ok(0);
    }
    let chunk = extent_store_expansion_chunk(segment_size);
    let chunks = required_len
        .checked_add(chunk.saturating_sub(1))
        .ok_or_else(|| StoreError::InvalidState("extent store allocation overflow".to_string()))?
        / chunk;
    chunks
        .checked_mul(chunk)
        .ok_or_else(|| StoreError::InvalidState("extent store allocation overflow".to_string()))
}

fn preallocate_segment_range(
    file: &ExtentStoreFile,
    path: &ExtentStorePath,
    current_len: u64,
    target_len: u64,
) -> Result<()> {
    if target_len <= current_len {
        return Ok(());
    }
    let offset = extent_store_off_t(current_len, path)?;
    let len = extent_store_off_t(target_len - current_len, path)?;
    let result = unsafe { libc::fallocate(file.as_raw_fd(), 0, offset, len) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if preallocation_unavailable(&error) {
        file.set_len(target_len).map_err(|error| {
            StoreError::Transport(format!(
                "failed to size extent store segment {}: {error}",
                path.display()
            ))
        })?;
        return Ok(());
    }
    Err(StoreError::Transport(format!(
        "failed to preallocate extent store segment {}: {error}",
        path.display()
    )))
}

fn extent_store_off_t(value: u64, path: &ExtentStorePath) -> Result<libc::off_t> {
    if value > i64::MAX as u64 {
        return Err(StoreError::InvalidState(format!(
            "extent store segment {} offset {value} exceeds off_t",
            path.display()
        )));
    }
    Ok(value as libc::off_t)
}

fn preallocation_unavailable(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EINVAL)
    )
}

fn open_direct_segment(path: &ExtentStorePath) -> Result<Option<ExtentStoreFile>> {
    match OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(file) => Ok(Some(file)),
        Err(error) if direct_io_unavailable(&error) => {
            warn!(
                error = %error,
                path = %path.display(),
                "extent store direct I/O unavailable for segment; using buffered file handle"
            );
            Ok(None)
        }
        Err(error) => Err(StoreError::Transport(format!(
            "failed to open extent store direct segment {}: {error}",
            path.display()
        ))),
    }
}

fn direct_io_unavailable(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EINVAL) | Some(libc::EOPNOTSUPP) | Some(libc::ENOTTY)
    )
}
