//! KVCS low-level physical layout.
//!
//! The shared NoF adapter passes one complete object to the executor. KVCS alone translates that
//! object into SDK-sized records and persists the layout descriptor as an opaque locator.

use mooncake_store_core::{Result, StoreError};

use crate::client::cold_tier::layout::{
    OpaquePhysicalKey, PhysicalKeyCodec, PhysicalKeyInput, Sha256PhysicalKeyCodec, ValueChunkPlan,
};
use crate::client::cold_tier::nof::NofPhysicalLocator;

const LOCATOR_MAGIC: &[u8; 8] = b"MCKKV001";
const LOCATOR_LEN: usize = 25;
const INLINE: u8 = 0;
const CHUNKED: u8 = 1;
const CHUNK_KEY_DOMAIN: &[u8] = b"mooncake:nof:kvcs:chunk:v1";

pub(super) struct KvcsWriteLayout<'a> {
    pub(super) locator: NofPhysicalLocator,
    pub(super) records: Vec<(OpaquePhysicalKey, &'a [u8])>,
}

pub(super) struct KvcsReadRecord {
    pub(super) key: OpaquePhysicalKey,
    pub(super) expected_value_size: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KvcsLayout {
    Inline,
    Chunked { chunk_size: u64, chunk_count: u64 },
}

impl KvcsLayout {
    fn encode(self) -> Result<NofPhysicalLocator> {
        let mut bytes = vec![0; LOCATOR_LEN];
        bytes[..8].copy_from_slice(LOCATOR_MAGIC);
        match self {
            Self::Inline => bytes[8] = INLINE,
            Self::Chunked {
                chunk_size,
                chunk_count,
            } => {
                bytes[8] = CHUNKED;
                bytes[9..17].copy_from_slice(&chunk_size.to_le_bytes());
                bytes[17..25].copy_from_slice(&chunk_count.to_le_bytes());
            }
        }
        NofPhysicalLocator::new(bytes)
    }

    fn decode(locator: &NofPhysicalLocator) -> Result<Self> {
        let bytes = locator.as_bytes();
        if bytes.len() != LOCATOR_LEN || &bytes[..8] != LOCATOR_MAGIC {
            return Err(StoreError::InvalidState(
                "KVCS physical locator has invalid length or magic".to_string(),
            ));
        }
        let mut encoded_chunk_size = [0; 8];
        encoded_chunk_size.copy_from_slice(&bytes[9..17]);
        let chunk_size = u64::from_le_bytes(encoded_chunk_size);
        let mut encoded_chunk_count = [0; 8];
        encoded_chunk_count.copy_from_slice(&bytes[17..25]);
        let chunk_count = u64::from_le_bytes(encoded_chunk_count);
        match (bytes[8], chunk_size, chunk_count) {
            (INLINE, 0, 0) => Ok(Self::Inline),
            (CHUNKED, 1.., 1..) => Ok(Self::Chunked {
                chunk_size,
                chunk_count,
            }),
            _ => Err(StoreError::InvalidState(
                "KVCS physical locator has an invalid layout".to_string(),
            )),
        }
    }

    fn chunk_plan(self, value_len: u64) -> Result<ValueChunkPlan> {
        let Self::Chunked {
            chunk_size,
            chunk_count,
        } = self
        else {
            return Err(StoreError::InvalidState(
                "KVCS inline layout does not have a chunk plan".to_string(),
            ));
        };
        let plan = ValueChunkPlan::new(value_len, chunk_size)?;
        if plan.chunk_count() != chunk_count {
            return Err(StoreError::InvalidState(
                "KVCS physical locator chunk count does not match the object length".to_string(),
            ));
        }
        Ok(plan)
    }
}

pub(super) fn build_write_layout<'a>(
    root_key: &OpaquePhysicalKey,
    value: &'a [u8],
    max_value_size: u64,
) -> Result<KvcsWriteLayout<'a>> {
    let value_len = u64::try_from(value.len())
        .map_err(|_| StoreError::InvalidState("KVCS value length does not fit u64".to_string()))?;
    if value_len <= max_value_size {
        return Ok(KvcsWriteLayout {
            locator: KvcsLayout::Inline.encode()?,
            records: vec![(root_key.clone(), value)],
        });
    }
    let plan = ValueChunkPlan::new(value_len, max_value_size)?;
    let layout = KvcsLayout::Chunked {
        chunk_size: max_value_size,
        chunk_count: plan.chunk_count(),
    };
    let mut records = Vec::with_capacity(usize::try_from(plan.chunk_count()).map_err(|_| {
        StoreError::InvalidState("KVCS physical chunk count does not fit usize".to_string())
    })?);
    for index in 0..plan.chunk_count() {
        let range = plan.range(index)?;
        let start = usize::try_from(range.start).map_err(|_| {
            StoreError::InvalidState("KVCS physical chunk start does not fit usize".to_string())
        })?;
        let end = usize::try_from(range.end).map_err(|_| {
            StoreError::InvalidState("KVCS physical chunk end does not fit usize".to_string())
        })?;
        records.push((chunk_key(root_key, index)?, &value[start..end]));
    }
    Ok(KvcsWriteLayout {
        locator: layout.encode()?,
        records,
    })
}

pub(super) fn read_records(
    root_key: &OpaquePhysicalKey,
    locator: &NofPhysicalLocator,
    expected_value_size: usize,
) -> Result<Vec<KvcsReadRecord>> {
    let layout = KvcsLayout::decode(locator)?;
    let value_len = u64::try_from(expected_value_size).map_err(|_| {
        StoreError::InvalidState("KVCS physical read length does not fit u64".to_string())
    })?;
    let plan = match layout {
        KvcsLayout::Inline => {
            return Ok(vec![KvcsReadRecord {
                key: root_key.clone(),
                expected_value_size,
            }])
        }
        KvcsLayout::Chunked { .. } => layout.chunk_plan(value_len)?,
    };
    let mut records = Vec::with_capacity(usize::try_from(plan.chunk_count()).map_err(|_| {
        StoreError::InvalidState("KVCS physical chunk count does not fit usize".to_string())
    })?);
    for index in 0..plan.chunk_count() {
        let range = plan.range(index)?;
        let expected_value_size = usize::try_from(range.end - range.start).map_err(|_| {
            StoreError::InvalidState("KVCS physical chunk length does not fit usize".to_string())
        })?;
        records.push(KvcsReadRecord {
            key: chunk_key(root_key, index)?,
            expected_value_size,
        });
    }
    Ok(records)
}

pub(super) fn object_keys(
    root_key: &OpaquePhysicalKey,
    locator: &NofPhysicalLocator,
    expected_value_size: usize,
) -> Result<Vec<OpaquePhysicalKey>> {
    Ok(read_records(root_key, locator, expected_value_size)?
        .into_iter()
        .map(|record| record.key)
        .collect())
}

pub(super) fn assemble_read(
    expected_value_size: usize,
    records: Vec<Result<Option<Vec<u8>>>>,
) -> Result<Option<Vec<u8>>> {
    let mut value = Vec::with_capacity(expected_value_size);
    let mut found = 0usize;
    for record in records {
        if let Some(record) = record? {
            found += 1;
            value.extend_from_slice(&record);
        }
    }
    if found == 0 {
        return Ok(None);
    }
    if value.len() != expected_value_size {
        return Err(StoreError::InvalidState(format!(
            "KVCS physical layout reconstructed {} bytes, expected {expected_value_size}",
            value.len()
        )));
    }
    Ok(Some(value))
}

fn chunk_key(root_key: &OpaquePhysicalKey, index: u64) -> Result<OpaquePhysicalKey> {
    Sha256PhysicalKeyCodec.encode(PhysicalKeyInput {
        domain: CHUNK_KEY_DOMAIN,
        fields: &[root_key.as_bytes()],
        chunk_index: Some(index),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_layout_keeps_one_complete_record() {
        let root = OpaquePhysicalKey::new(b"root".to_vec());
        let layout = build_write_layout(&root, b"value", 64).unwrap();
        assert_eq!(layout.records.len(), 1);
        assert_eq!(layout.records[0].0, root);
        assert_eq!(layout.records[0].1, b"value");
    }

    #[test]
    fn chunk_layout_is_private_to_kvcs() {
        let root = OpaquePhysicalKey::new(b"root".to_vec());
        let value = vec![7; 150];
        let layout = build_write_layout(&root, &value, 64).unwrap();
        assert_eq!(layout.records.len(), 3);
        assert!(layout.records.iter().all(|(key, _)| key != &root));
        let reads = read_records(&root, &layout.locator, value.len()).unwrap();
        assert_eq!(reads.len(), 3);
        assert_eq!(reads[2].expected_value_size, 22);
    }

    #[test]
    fn rejects_locator_chunk_count_that_disagrees_with_route_length() {
        let root = OpaquePhysicalKey::new(b"root".to_vec());
        let value = vec![7; 150];
        let layout = build_write_layout(&root, &value, 64).unwrap();
        let mut corrupted = layout.locator.as_bytes().to_vec();
        corrupted[17..25].copy_from_slice(&u64::MAX.to_le_bytes());
        let corrupted = NofPhysicalLocator::new(corrupted).unwrap();

        assert!(matches!(
            read_records(&root, &corrupted, value.len()),
            Err(StoreError::InvalidState(_))
        ));
    }
}
