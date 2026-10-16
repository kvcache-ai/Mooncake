use mooncake_store_core::{Result, StoreError};

pub(super) const EXTENT_STORE_LOCATOR_PREFIX: &str = "extent-store-v1";
pub(super) const EXTENT_STORE_MAGIC: u32 = 0x4d45_5354; // MEST
pub(super) const EXTENT_STORE_HEADER_LEN: usize = 64;
pub(super) const EXTENT_STORE_RECORD_KIND_SINGLE: u16 = 0;
pub(super) const EXTENT_STORE_RECORD_KIND_PACKED: u16 = 1;
pub(super) const EXTENT_STORE_ALIGNMENT: u64 = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ExtentStoreLocator {
    pub(super) segment_id: u64,
    pub(super) offset: u64,
    pub(super) record_len: u64,
    pub(super) value_offset: u64,
    pub(super) value_len: u64,
    pub(super) generation: u64,
}

impl ExtentStoreLocator {
    pub(super) const BINARY_LEN: usize = 6 * std::mem::size_of::<u64>();

    pub(super) fn block_key(&self) -> (u64, u64, u64) {
        (self.segment_id, self.offset, self.record_len)
    }

    pub(super) fn encode(&self) -> String {
        format!(
            "{EXTENT_STORE_LOCATOR_PREFIX}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}",
            self.segment_id,
            self.offset,
            self.record_len,
            self.value_offset,
            self.value_len,
            self.generation
        )
    }

    pub(super) fn decode(value: &str) -> Result<Self> {
        let mut parts = value.split(':');
        let prefix = parts
            .next()
            .ok_or_else(|| StoreError::InvalidState("empty extent store locator".to_string()))?;
        if prefix != EXTENT_STORE_LOCATOR_PREFIX {
            return Err(StoreError::InvalidState(format!(
                "invalid extent store locator prefix {prefix:?}"
            )));
        }
        let locator = Self {
            segment_id: parse_locator_hex(parts.next(), "segment_id")?,
            offset: parse_locator_hex(parts.next(), "offset")?,
            record_len: parse_locator_hex(parts.next(), "record_len")?,
            value_offset: parse_locator_hex(parts.next(), "value_offset")?,
            value_len: parse_locator_hex(parts.next(), "value_len")?,
            generation: parse_locator_hex(parts.next(), "generation")?,
        };
        if parts.next().is_some() {
            return Err(StoreError::InvalidState(format!(
                "extent store locator has too many fields: {value}"
            )));
        }
        Ok(locator)
    }

    pub(super) fn encode_binary(self) -> Vec<u8> {
        [
            self.segment_id,
            self.offset,
            self.record_len,
            self.value_offset,
            self.value_len,
            self.generation,
        ]
        .into_iter()
        .flat_map(u64::to_le_bytes)
        .collect()
    }

    pub(super) fn decode_binary(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::BINARY_LEN {
            return Err(StoreError::InvalidState(
                "invalid binary ExtentStore locator".to_string(),
            ));
        }
        let mut fields = bytes
            .chunks_exact(std::mem::size_of::<u64>())
            .map(|field| u64::from_le_bytes(field.try_into().unwrap()));
        Ok(Self {
            segment_id: fields.next().unwrap(),
            offset: fields.next().unwrap(),
            record_len: fields.next().unwrap(),
            value_offset: fields.next().unwrap(),
            value_len: fields.next().unwrap(),
            generation: fields.next().unwrap(),
        })
    }
}

pub(super) fn parse_locator_hex(value: Option<&str>, field: &str) -> Result<u64> {
    let value = value
        .ok_or_else(|| StoreError::InvalidState(format!("extent store locator missing {field}")))?;
    u64::from_str_radix(value, 16).map_err(|error| {
        StoreError::InvalidState(format!(
            "extent store locator field {field}={value:?} is not hex: {error}"
        ))
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ExtentStoreRecordHeader {
    pub(super) record_kind: u16,
    pub(super) key_len: u64,
    pub(super) value_len: u64,
    pub(super) checksum: u64,
    pub(super) value_offset: u64,
    pub(super) record_len: u64,
}

pub(super) fn scan_extent_store_headers(
    start_offset: u64,
    end_offset: u64,
    hole_alignment: Option<u64>,
    mut read_header: impl FnMut(u64) -> Result<Option<[u8; EXTENT_STORE_HEADER_LEN]>>,
    mut visit: impl FnMut(u64, ExtentStoreRecordHeader) -> Result<()>,
) -> Result<u64> {
    let mut offset = start_offset;
    while offset.saturating_add(EXTENT_STORE_HEADER_LEN as u64) <= end_offset {
        let decoded = match read_header(offset)? {
            Some(header) => ExtentStoreRecordHeader::decode(&header),
            None => Ok(None),
        };
        let header = match decoded {
            Ok(Some(header)) => header,
            Ok(None) => match hole_alignment {
                Some(alignment) => {
                    offset = offset.saturating_add(alignment);
                    continue;
                }
                None => break,
            },
            Err(error) => match hole_alignment {
                Some(alignment) => {
                    offset = offset.saturating_add(alignment);
                    continue;
                }
                None => return Err(error),
            },
        };
        let record_end = offset.checked_add(header.record_len).ok_or_else(|| {
            StoreError::InvalidState("extent store record offset overflow".to_string())
        })?;
        if header.record_len == 0
            || record_end > end_offset
            || hole_alignment.is_some_and(|alignment| !header.record_len.is_multiple_of(alignment))
        {
            if let Some(alignment) = hole_alignment {
                offset = offset.saturating_add(alignment);
                continue;
            }
            return Err(StoreError::InvalidState(format!(
                "extent store record at {offset} has invalid length {}",
                header.record_len
            )));
        }
        visit(offset, header)?;
        offset = record_end;
    }
    Ok(offset)
}

impl ExtentStoreRecordHeader {
    pub(super) fn single(
        key_len: usize,
        value_len: u64,
        checksum: u64,
        alignment: u64,
    ) -> Result<Self> {
        if alignment == 0 || !alignment.is_power_of_two() {
            return Err(StoreError::InvalidState(
                "extent store alignment must be a non-zero power of two".to_string(),
            ));
        }
        let key_len = u64::try_from(key_len).map_err(|_| {
            StoreError::InvalidState("extent store record key is too large".to_string())
        })?;
        let minimum_value_offset = (EXTENT_STORE_HEADER_LEN as u64)
            .checked_add(key_len)
            .ok_or_else(|| StoreError::InvalidState("extent store record overflow".to_string()))?;
        let value_offset = align_up_checked(minimum_value_offset, alignment)?;
        let record_len = align_up_checked(
            value_offset.checked_add(value_len).ok_or_else(|| {
                StoreError::InvalidState("extent store record overflow".to_string())
            })?,
            alignment,
        )?;
        Ok(Self {
            record_kind: EXTENT_STORE_RECORD_KIND_SINGLE,
            key_len,
            value_len,
            checksum,
            value_offset,
            record_len,
        })
    }

    pub(super) fn encode_into(self, header: &mut [u8]) {
        debug_assert!(header.len() >= EXTENT_STORE_HEADER_LEN);
        header[..EXTENT_STORE_HEADER_LEN].fill(0);
        header[0..4].copy_from_slice(&EXTENT_STORE_MAGIC.to_le_bytes());
        header[4..6].copy_from_slice(&(EXTENT_STORE_HEADER_LEN as u16).to_le_bytes());
        header[6..8].copy_from_slice(&self.record_kind.to_le_bytes());
        header[8..16].copy_from_slice(&self.key_len.to_le_bytes());
        header[16..24].copy_from_slice(&self.value_len.to_le_bytes());
        header[24..32].copy_from_slice(&self.checksum.to_le_bytes());
        header[32..40].copy_from_slice(&self.value_offset.to_le_bytes());
        header[40..48].copy_from_slice(&self.record_len.to_le_bytes());
    }

    pub(super) fn decode(header: &[u8]) -> Result<Option<Self>> {
        if header.len() < EXTENT_STORE_HEADER_LEN {
            return Err(StoreError::InvalidState(
                "extent store record header is truncated".to_string(),
            ));
        }
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic == 0 {
            return Ok(None);
        }
        if magic != EXTENT_STORE_MAGIC {
            return Err(StoreError::InvalidState(
                "extent store record has invalid magic".to_string(),
            ));
        }
        let header_len = u16::from_le_bytes(header[4..6].try_into().unwrap()) as usize;
        if header_len != EXTENT_STORE_HEADER_LEN {
            return Err(StoreError::InvalidState(
                "extent store record has an unsupported header".to_string(),
            ));
        }
        let record_kind = u16::from_le_bytes(header[6..8].try_into().unwrap());
        if record_kind != EXTENT_STORE_RECORD_KIND_SINGLE
            && record_kind != EXTENT_STORE_RECORD_KIND_PACKED
        {
            return Err(StoreError::InvalidState(format!(
                "extent store record has unsupported kind {record_kind}"
            )));
        }
        let decoded = Self {
            record_kind,
            key_len: u64::from_le_bytes(header[8..16].try_into().unwrap()),
            value_len: u64::from_le_bytes(header[16..24].try_into().unwrap()),
            checksum: u64::from_le_bytes(header[24..32].try_into().unwrap()),
            value_offset: u64::from_le_bytes(header[32..40].try_into().unwrap()),
            record_len: u64::from_le_bytes(header[40..48].try_into().unwrap()),
        };
        let minimum_value_offset = (EXTENT_STORE_HEADER_LEN as u64)
            .checked_add(decoded.key_len)
            .ok_or_else(|| StoreError::InvalidState("extent store record overflow".to_string()))?;
        let value_end = decoded
            .value_offset
            .checked_add(decoded.value_len)
            .ok_or_else(|| StoreError::InvalidState("extent store record overflow".to_string()))?;
        if decoded.value_offset < minimum_value_offset || value_end > decoded.record_len {
            return Err(StoreError::InvalidState(
                "extent store record has invalid payload bounds".to_string(),
            ));
        }
        Ok(Some(decoded))
    }
}

pub(super) fn align_up_checked(value: u64, alignment: u64) -> Result<u64> {
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| StoreError::InvalidState("extent store alignment overflow".to_string()))
}
