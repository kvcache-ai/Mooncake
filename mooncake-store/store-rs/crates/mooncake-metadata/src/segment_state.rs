use mooncake_store_core::{
    ClientRuntimeId, Result, SegmentAnnouncement, SegmentLifecycleState, SegmentName,
    SegmentReservation, StoreError,
};
use serde::de::{Deserializer, Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct FreeSpan {
    pub offset_bytes: u64,
    pub length_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct StoredSegmentState {
    pub announcement: SegmentAnnouncement,
    pub cursor_bytes: u64,
    #[serde(default, deserialize_with = "deserialize_free_spans")]
    pub free_spans: Vec<FreeSpan>,
}

impl StoredSegmentState {
    pub(crate) fn new(announcement: SegmentAnnouncement) -> Self {
        Self {
            cursor_bytes: announcement.used_bytes,
            announcement,
            free_spans: Vec::new(),
        }
    }

    pub(crate) fn merge_announcement(&mut self, next: &SegmentAnnouncement) {
        self.announcement.owner = next.owner.clone();
        self.announcement.segment_name = next.segment_name.clone();
        self.announcement.capacity_bytes = next.capacity_bytes;
        self.announcement.tags = next.tags.clone();
        self.announcement.state = next.state;
        self.announcement.alignment_bytes = next.alignment_bytes.max(1);
        self.announcement.used_bytes = self.announcement.used_bytes.max(next.used_bytes);
        self.cursor_bytes = self.cursor_bytes.max(next.used_bytes);
    }

    pub(crate) fn reserve(
        &mut self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        length_bytes: u64,
    ) -> Result<SegmentReservation> {
        if self.announcement.state != SegmentLifecycleState::Active {
            return Err(StoreError::InvalidState(format!(
                "segment {}:{} is not active",
                owner, segment.0
            )));
        }
        if length_bytes == 0 {
            return Err(StoreError::Allocator(
                "zero-length segment reservation is not supported".to_string(),
            ));
        }
        let alignment = self.announcement.alignment_bytes.max(1);
        let reserved_len = align_up_u64(length_bytes, alignment);

        if let Some(index) = self
            .free_spans
            .iter()
            .position(|span| span.length_bytes >= reserved_len)
        {
            let span = self.free_spans.remove(index);
            if span.length_bytes > reserved_len {
                self.free_spans.push(FreeSpan {
                    offset_bytes: span.offset_bytes + reserved_len,
                    length_bytes: span.length_bytes - reserved_len,
                });
                self.free_spans.sort_by_key(|entry| entry.offset_bytes);
            }
            self.announcement.used_bytes = self
                .announcement
                .used_bytes
                .checked_add(reserved_len)
                .ok_or_else(|| {
                StoreError::Allocator("segment reservation overflow".to_string())
            })?;
            return Ok(SegmentReservation {
                owner: owner.clone(),
                segment_name: segment.clone(),
                offset_bytes: span.offset_bytes,
                length_bytes,
            });
        }

        let offset = align_up_u64(self.cursor_bytes, alignment);
        let next_cursor = offset
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment reservation overflow".to_string()))?;
        if next_cursor > self.announcement.capacity_bytes {
            return Err(StoreError::Allocator(format!(
                "segment capacity exhausted for {}:{} requested={} remaining={}",
                owner,
                segment.0,
                length_bytes,
                self.announcement.capacity_bytes.saturating_sub(offset)
            )));
        }
        self.cursor_bytes = next_cursor;
        self.announcement.used_bytes = self
            .announcement
            .used_bytes
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment reservation overflow".to_string()))?;
        Ok(SegmentReservation {
            owner: owner.clone(),
            segment_name: segment.clone(),
            offset_bytes: offset,
            length_bytes,
        })
    }

    pub(crate) fn release(
        &mut self,
        owner: &ClientRuntimeId,
        segment: &SegmentName,
        offset_bytes: u64,
        length_bytes: u64,
    ) -> Result<()> {
        let alignment = self.announcement.alignment_bytes.max(1);
        let reserved_len = align_up_u64(length_bytes, alignment);
        let end = offset_bytes
            .checked_add(reserved_len)
            .ok_or_else(|| StoreError::Allocator("segment release overflow".to_string()))?;
        if end > self.announcement.capacity_bytes {
            return Err(StoreError::Allocator(format!(
                "segment release exceeds capacity for {}:{} offset={} len={}",
                owner, segment.0, offset_bytes, length_bytes
            )));
        }
        self.insert_free_span(offset_bytes, reserved_len);
        self.announcement.used_bytes = self.announcement.used_bytes.saturating_sub(reserved_len);
        self.trim_tail();
        if self.announcement.used_bytes == 0 {
            self.cursor_bytes = 0;
            self.free_spans.clear();
        }
        Ok(())
    }

    fn trim_tail(&mut self) {
        loop {
            let Some(last) = self.free_spans.last().cloned() else {
                return;
            };
            if last.offset_bytes + last.length_bytes != self.cursor_bytes {
                return;
            }
            self.cursor_bytes = last.offset_bytes;
            self.free_spans.pop();
        }
    }

    fn insert_free_span(&mut self, offset_bytes: u64, length_bytes: u64) {
        self.free_spans.push(FreeSpan {
            offset_bytes,
            length_bytes,
        });
        self.free_spans.sort_by_key(|entry| entry.offset_bytes);
        let mut merged: Vec<FreeSpan> = Vec::with_capacity(self.free_spans.len());
        for span in self.free_spans.drain(..) {
            if let Some(previous) = merged.last_mut() {
                let prev_end = previous.offset_bytes + previous.length_bytes;
                if prev_end >= span.offset_bytes {
                    let merged_end = prev_end.max(span.offset_bytes + span.length_bytes);
                    previous.length_bytes = merged_end - previous.offset_bytes;
                    continue;
                }
            }
            merged.push(span);
        }
        self.free_spans = merged;
    }
}

fn align_up_u64(value: u64, alignment: u64) -> u64 {
    let mask = alignment.saturating_sub(1);
    value.saturating_add(mask) & !mask
}

fn deserialize_free_spans<'de, D>(deserializer: D) -> std::result::Result<Vec<FreeSpan>, D::Error>
where
    D: Deserializer<'de>,
{
    struct FreeSpanVisitor;

    impl<'de> Visitor<'de> for FreeSpanVisitor {
        type Value = Vec<FreeSpan>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a free-span array or an empty object")
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut spans = Vec::new();
            while let Some(span) = sequence.next_element()? {
                spans.push(span);
            }
            Ok(spans)
        }

        fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            if map.next_key::<String>()?.is_some() {
                return Err(A::Error::custom(
                    "free_spans object payload is only valid when empty",
                ));
            }
            Ok(Vec::new())
        }
    }

    deserializer.deserialize_any(FreeSpanVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_store_core::{ClientEpoch, SegmentName};

    fn announcement() -> SegmentAnnouncement {
        SegmentAnnouncement {
            owner: ClientRuntimeId::new("stable".to_string(), ClientEpoch(1)),
            segment_name: SegmentName::new("segment"),
            capacity_bytes: 1024,
            used_bytes: 0,
            state: SegmentLifecycleState::Active,
            alignment_bytes: 64,
            tags: vec!["dram".to_string()],
        }
    }

    #[test]
    fn deserialize_empty_object_free_spans_for_redis_compat() {
        let payload = r#"{
            "announcement": {
                "owner": {"stable_id":"stable","epoch":1},
                "segment_name": "segment",
                "capacity_bytes": 1024,
                "used_bytes": 0,
                "state": "Active",
                "alignment_bytes": 64,
                "tags": ["dram"]
            },
            "cursor_bytes": 0,
            "free_spans": {}
        }"#;
        let state: StoredSegmentState = serde_json::from_str(payload).expect("payload must parse");
        assert!(state.free_spans.is_empty());
    }

    #[test]
    fn serialize_round_trip_keeps_free_spans_sequence() {
        let state = StoredSegmentState::new(announcement());
        let payload = serde_json::to_string(&state).expect("state must serialize");
        assert!(payload.contains(r#""free_spans":[]"#));
    }
}
