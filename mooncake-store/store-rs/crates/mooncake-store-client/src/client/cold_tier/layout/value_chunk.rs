//! Cold Tier planning for values that exceed a physical executor limit.

use std::ops::Range;

use mooncake_store_core::{Result, StoreError};

/// Ordered logical byte ranges for one value.
///
/// This type intentionally knows nothing about providers, keys, record headers, or locators.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ValueChunkPlan {
    total_len: u64,
    max_chunk_len: u64,
    chunk_count: u64,
}

impl ValueChunkPlan {
    pub(crate) fn new(total_len: u64, max_chunk_len: u64) -> Result<Self> {
        if max_chunk_len == 0 {
            return Err(StoreError::InvalidState(
                "value chunk limit must be non-zero".to_string(),
            ));
        }
        let chunk_count = if total_len == 0 {
            0
        } else {
            let full_chunks = total_len / max_chunk_len;
            full_chunks
                .checked_add(u64::from(!total_len.is_multiple_of(max_chunk_len)))
                .ok_or_else(|| StoreError::InvalidState("value chunk count overflow".to_string()))?
        };
        Ok(Self {
            total_len,
            max_chunk_len,
            chunk_count,
        })
    }

    pub(crate) fn total_len(self) -> u64 {
        self.total_len
    }

    pub(crate) fn max_chunk_len(self) -> u64 {
        self.max_chunk_len
    }

    pub(crate) fn chunk_count(self) -> u64 {
        self.chunk_count
    }

    pub(crate) fn range(self, chunk_index: u64) -> Result<Range<u64>> {
        if chunk_index >= self.chunk_count {
            return Err(StoreError::InvalidState(format!(
                "value chunk index {chunk_index} is outside {} chunks",
                self.chunk_count
            )));
        }
        let start = chunk_index
            .checked_mul(self.max_chunk_len)
            .ok_or_else(|| StoreError::InvalidState("value chunk offset overflow".to_string()))?;
        let remaining = self.total_len.checked_sub(start).ok_or_else(|| {
            StoreError::InvalidState("value chunk start exceeds total length".to_string())
        })?;
        let chunk_len = remaining.min(self.max_chunk_len);
        let end = start
            .checked_add(chunk_len)
            .ok_or_else(|| StoreError::InvalidState("value chunk end overflow".to_string()))?;
        if start >= end {
            return Err(StoreError::InvalidState(
                "value chunk range must not be empty".to_string(),
            ));
        }
        Ok(start..end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_ordered_ranges() {
        let plan = ValueChunkPlan::new(10, 4).unwrap();
        assert_eq!(plan.chunk_count(), 3);
        assert_eq!(plan.range(0).unwrap(), 0..4);
        assert_eq!(plan.range(1).unwrap(), 4..8);
        assert_eq!(plan.range(2).unwrap(), 8..10);
    }

    #[test]
    fn empty_value_has_no_empty_chunk() {
        let plan = ValueChunkPlan::new(0, 4).unwrap();
        assert_eq!(plan.chunk_count(), 0);
        assert!(plan.range(0).is_err());
    }

    #[test]
    fn rejects_zero_limit_and_out_of_range_index() {
        assert!(ValueChunkPlan::new(1, 0).is_err());
        assert!(ValueChunkPlan::new(1, 1).unwrap().range(1).is_err());
    }

    #[test]
    fn plans_u64_boundary_without_saturating_arithmetic() {
        let plan = ValueChunkPlan::new(u64::MAX, u64::MAX - 1).unwrap();
        assert_eq!(plan.chunk_count(), 2);
        assert_eq!(plan.range(0).unwrap(), 0..u64::MAX - 1);
        assert_eq!(plan.range(1).unwrap(), u64::MAX - 1..u64::MAX);
    }
}
