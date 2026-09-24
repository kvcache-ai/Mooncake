//! Cold Tier opaque physical keys and codec boundary.

use mooncake_store_core::{Result, StoreError};
use sha2::{Digest, Sha256};

/// Opaque bytes consumed by a physical executor.
///
/// Width and string encoding are deliberately not part of this contract. A concrete executor
/// validates its own representation after any encoding it applies.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OpaquePhysicalKey(Vec<u8>);

impl OpaquePhysicalKey {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        encode_hex(&self.0)
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

/// Borrowed, length-delimited identity fields for one physical key.
///
/// The codec must preserve field boundaries. Callers provide the complete logical identity,
/// including any route generation/version required by their fencing semantics.
pub struct PhysicalKeyInput<'a> {
    pub domain: &'a [u8],
    pub fields: &'a [&'a [u8]],
    pub chunk_index: Option<u64>,
}

/// Derives a SHA-256 key from domain-separated, length-prefixed identity fields.
///
/// The 32-byte output is an internal Mooncake identity; executors still receive opaque bytes and
/// remain free to encode them for their own key API.
pub fn derive_physical_key(input: PhysicalKeyInput<'_>) -> Result<OpaquePhysicalKey> {
    let mut hasher = Sha256::new();
    hasher.update(b"mooncake:physical-key:v1\0");
    hash_field(&mut hasher, input.domain)?;
    for field in input.fields {
        hash_field(&mut hasher, field)?;
    }
    match input.chunk_index {
        Some(index) => {
            hasher.update([1]);
            hasher.update(index.to_le_bytes());
        }
        None => hasher.update([0]),
    }
    Ok(OpaquePhysicalKey::new(hasher.finalize().to_vec()))
}

fn hash_field(hasher: &mut Sha256, field: &[u8]) -> Result<()> {
    let len = u64::try_from(field.len()).map_err(|_| {
        StoreError::InvalidState("physical key field length does not fit u64".to_string())
    })?;
    hasher.update(len.to_le_bytes());
    hasher.update(field);
    Ok(())
}
