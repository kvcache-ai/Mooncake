use mooncake_store_core::{ClientRuntimeId, ClientStableId, ObjectKey, SegmentName};

#[derive(Clone, Debug)]
pub struct MetadataKeyspace {
    prefix: String,
}

impl MetadataKeyspace {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    pub fn client(&self, runtime: &ClientRuntimeId) -> String {
        format!("{}/clients/{}", self.prefix, runtime.storage_key())
    }

    pub fn client_pattern(&self) -> String {
        format!("{}/clients/*", self.prefix)
    }

    pub fn segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> String {
        format!("{}/segments/{}:{}", self.prefix, owner.storage_key(), segment.0)
    }

    pub fn segment_prefix(&self, owner: Option<&ClientRuntimeId>) -> String {
        match owner {
            Some(owner) => format!("{}/segments/{}:", self.prefix, owner.storage_key()),
            None => format!("{}/segments/", self.prefix),
        }
    }

    pub fn segment_pattern(&self, owner: Option<&ClientRuntimeId>) -> String {
        format!("{}*", self.segment_prefix(owner))
    }

    pub fn object(&self, key: &ObjectKey) -> String {
        format!("{}/objects/{}", self.prefix, key.0)
    }

    pub fn handoff(&self, stable_id: &ClientStableId) -> String {
        format!("{}/handoffs/{}", self.prefix, stable_id.0)
    }
}

impl Default for MetadataKeyspace {
    fn default() -> Self {
        Self::new("mc/store-rs/v1")
    }
}
