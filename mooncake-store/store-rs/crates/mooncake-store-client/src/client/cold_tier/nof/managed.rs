//! Optional capabilities for Mooncake-managed NoF storage.
//!
//! A managed executor returns opaque locators. Mooncake owns the route that binds a locator to a
//! target, while the executor owns its physical layout and allocator internals.

use mooncake_store_core::{Result, StoreError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NofManagedLimits {
    pub max_batch_items: usize,
    pub max_batch_bytes: u64,
}

impl NofManagedLimits {
    pub(crate) fn validate(self) -> Result<Self> {
        if self.max_batch_items == 0 || self.max_batch_bytes == 0 {
            return Err(StoreError::InvalidState(
                "managed NoF limits must be non-zero".to_string(),
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct NofManagedLocator(Vec<u8>);

impl NofManagedLocator {
    pub fn new(bytes: Vec<u8>) -> Result<Self> {
        if bytes.is_empty() {
            return Err(StoreError::InvalidState(
                "managed NoF locator must not be empty".to_string(),
            ));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        let mut encoded = String::with_capacity(self.0.len() * 2);
        for byte in &self.0 {
            use std::fmt::Write;
            let _ = write!(encoded, "{byte:02x}");
        }
        encoded
    }

    pub fn from_hex(encoded: &str) -> Result<Self> {
        if encoded.is_empty() || !encoded.len().is_multiple_of(2) {
            return Err(StoreError::InvalidState(
                "managed NoF locator hex must contain a non-empty even number of digits"
                    .to_string(),
            ));
        }
        let mut bytes = Vec::with_capacity(encoded.len() / 2);
        for pair in encoded.as_bytes().chunks_exact(2) {
            let high = hex_digit(pair[0])?;
            let low = hex_digit(pair[1])?;
            bytes.push((high << 4) | low);
        }
        Self::new(bytes)
    }
}

fn hex_digit(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(StoreError::InvalidState(
            "managed NoF locator contains a non-hex digit".to_string(),
        )),
    }
}

pub struct NofManagedAllocationRequest {
    pub key: String,
    pub length: u64,
    pub checksum: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct NofManagedReadRequest {
    pub locator: NofManagedLocator,
    pub length: u64,
    pub checksum: Option<u64>,
}

pub struct NofManagedWriteRequest<'a> {
    pub locator: NofManagedLocator,
    pub value: &'a [u8],
    pub checksum: Option<u64>,
}

/// Allocator and recovery operations owned by the managed executor.
pub trait NofManagedAllocator: Send + Sync {
    fn recover(&self, records: &[NofManagedReadRequest]) -> Result<()>;

    fn reserve_batch(
        &self,
        requests: &[NofManagedAllocationRequest],
    ) -> Vec<Result<NofManagedLocator>>;

    fn release_batch(&self, requests: &[NofManagedReadRequest]) -> Vec<Result<()>>;
}

/// Complete-object writes using opaque executor locators.
pub trait NofManagedWrite: Send + Sync {
    fn put_batch(&self, requests: &[NofManagedWriteRequest<'_>]) -> Vec<Result<()>>;

    fn flush(&self) -> Result<()>;
}

/// Complete-object reads using opaque executor locators.
pub trait NofManagedRead: Send + Sync {
    fn get_batch(&self, requests: &[NofManagedReadRequest]) -> Vec<Result<Option<Vec<u8>>>>;
}
