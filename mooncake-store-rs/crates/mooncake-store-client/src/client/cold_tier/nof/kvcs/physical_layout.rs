//! KVCS low-level physical layout.
//!
//! An object that fits the provider limit is stored directly at its root key. Only oversized
//! objects use executor-private chunk keys and a sidecar manifest. The sidecar is written first so
//! a retry can reconstruct the same chunk set; reads reject the object until every chunk exists.
//! KVCS remains responsible for reclaiming unreachable provider records.

use mooncake_store_core::{Result, StoreError};

use crate::client::cold_tier::layout::{
    derive_physical_key, OpaquePhysicalKey, PhysicalKeyInput, ValueChunkPlan,
};

const CHUNK_KEY_DOMAIN: &[u8] = b"mooncake:nof:kvcs:chunk:v1";
const MANIFEST_KEY_DOMAIN: &[u8] = b"mooncake:nof:kvcs:manifest:v1";
const MANIFEST_MAGIC: &[u8; 8] = b"MKVCLL01";
pub(super) const MANIFEST_SIZE: usize = 32;

pub(super) struct KvcsWriteLayout<'a> {
    pub(super) data_records: Vec<(OpaquePhysicalKey, &'a [u8])>,
    pub(super) manifest: Option<(OpaquePhysicalKey, [u8; MANIFEST_SIZE])>,
}

pub(super) struct KvcsReadRecord {
    pub(super) key: OpaquePhysicalKey,
    pub(super) expected_value_size: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ChunkManifest {
    value_size: u64,
    chunk_size: u64,
    chunk_count: u64,
}

impl ChunkManifest {
    fn encode(self) -> [u8; MANIFEST_SIZE] {
        let mut bytes = [0; MANIFEST_SIZE];
        bytes[..8].copy_from_slice(MANIFEST_MAGIC);
        bytes[8..16].copy_from_slice(&self.value_size.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.chunk_size.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.chunk_count.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != MANIFEST_SIZE || &bytes[..8] != MANIFEST_MAGIC {
            return Err(StoreError::InvalidState(
                "KVCS low-level chunk manifest is malformed".to_string(),
            ));
        }
        let read_u64 = |range: std::ops::Range<usize>| {
            u64::from_le_bytes(bytes[range].try_into().expect("eight-byte manifest field"))
        };
        let manifest = Self {
            value_size: read_u64(8..16),
            chunk_size: read_u64(16..24),
            chunk_count: read_u64(24..32),
        };
        let plan = ValueChunkPlan::new(manifest.value_size, manifest.chunk_size)?;
        if plan.chunk_count() != manifest.chunk_count || manifest.chunk_count < 2 {
            return Err(StoreError::InvalidState(
                "KVCS low-level chunk manifest has an invalid chunk count".to_string(),
            ));
        }
        Ok(manifest)
    }
}

pub(super) fn build_write_layout<'a>(
    root_key: &OpaquePhysicalKey,
    value: &'a [u8],
    max_value_size: u64,
) -> Result<KvcsWriteLayout<'a>> {
    let value_size = u64::try_from(value.len())
        .map_err(|_| StoreError::InvalidState("KVCS value length does not fit u64".to_string()))?;
    if value_size == 0 {
        return Err(StoreError::InvalidState(
            "KVCS physical value must not be empty".to_string(),
        ));
    }
    if max_value_size != 0 && value_size <= max_value_size {
        return Ok(KvcsWriteLayout {
            data_records: vec![(root_key.clone(), value)],
            manifest: None,
        });
    }

    let plan = ValueChunkPlan::new(value_size, max_value_size)?;
    let data_records = plan
        .slices(value)?
        .map(|(index, value)| Ok((chunk_key(root_key, index)?, value)))
        .collect::<Result<Vec<_>>>()?;
    let manifest = ChunkManifest {
        value_size,
        chunk_size: max_value_size,
        chunk_count: plan.chunk_count(),
    }
    .encode();
    Ok(KvcsWriteLayout {
        data_records,
        manifest: Some((manifest_key(root_key)?, manifest)),
    })
}

pub(super) fn manifest_key(root_key: &OpaquePhysicalKey) -> Result<OpaquePhysicalKey> {
    derive_physical_key(PhysicalKeyInput {
        domain: MANIFEST_KEY_DOMAIN,
        fields: &[root_key.as_bytes()],
        chunk_index: None,
    })
}

pub(super) fn read_records(
    root_key: &OpaquePhysicalKey,
    manifest_bytes: &[u8],
) -> Result<(Vec<KvcsReadRecord>, usize)> {
    let manifest = ChunkManifest::decode(manifest_bytes)?;
    let plan = ValueChunkPlan::new(manifest.value_size, manifest.chunk_size)?;
    let mut records = Vec::with_capacity(usize::try_from(manifest.chunk_count).map_err(|_| {
        StoreError::InvalidState("KVCS physical chunk count does not fit usize".to_string())
    })?);
    for index in 0..manifest.chunk_count {
        let range = plan.range(index)?;
        let expected_value_size = usize::try_from(range.end - range.start).map_err(|_| {
            StoreError::InvalidState("KVCS physical chunk length does not fit usize".to_string())
        })?;
        records.push(KvcsReadRecord {
            key: chunk_key(root_key, index)?,
            expected_value_size,
        });
    }
    let value_size = usize::try_from(manifest.value_size).map_err(|_| {
        StoreError::InvalidState("KVCS physical value length does not fit usize".to_string())
    })?;
    Ok((records, value_size))
}

fn chunk_key(root_key: &OpaquePhysicalKey, index: u64) -> Result<OpaquePhysicalKey> {
    derive_physical_key(PhysicalKeyInput {
        domain: CHUNK_KEY_DOMAIN,
        fields: &[root_key.as_bytes()],
        chunk_index: Some(index),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_layout_is_a_single_direct_root_record() {
        let root = OpaquePhysicalKey::new(b"root".to_vec());
        let layout = build_write_layout(&root, b"value", 64).unwrap();
        assert_eq!(layout.data_records.len(), 1);
        assert_eq!(layout.data_records[0].0, root);
        assert_eq!(layout.data_records[0].1, b"value");
        assert!(layout.manifest.is_none());
    }

    #[test]
    fn empty_value_is_rejected_before_the_inline_path() {
        let root = OpaquePhysicalKey::new(b"root".to_vec());
        assert!(build_write_layout(&root, b"", 64).is_err());
    }

    #[test]
    fn chunk_manifest_preserves_the_original_layout() {
        let root = OpaquePhysicalKey::new(b"root".to_vec());
        let value = vec![7; 150];
        let layout = build_write_layout(&root, &value, 64).unwrap();
        assert_eq!(layout.data_records.len(), 3);
        assert!(layout.data_records.iter().all(|(key, _)| key != &root));
        let (sidecar_key, manifest) = layout.manifest.unwrap();
        assert_eq!(sidecar_key, manifest_key(&root).unwrap());

        let (reads, value_size) = read_records(&root, &manifest).unwrap();
        assert_eq!(value_size, value.len());
        assert_eq!(reads.len(), 3);
        assert_eq!(reads[2].expected_value_size, 22);
    }
}
