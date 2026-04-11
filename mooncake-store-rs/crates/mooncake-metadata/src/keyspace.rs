use mooncake_store_core::{
    ClientRuntimeId, ClientStableId, ObjectKey, RoutePolicyDomain, SegmentName,
};

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
        format!(
            "{}/segments/{}:{}",
            self.prefix,
            owner.storage_key(),
            segment.0
        )
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

    pub fn object_prefix(&self) -> String {
        format!("{}/objects/", self.prefix)
    }

    pub fn object_pattern(&self) -> String {
        format!("{}*", self.object_prefix())
    }

    pub fn handoff(&self, stable_id: &ClientStableId) -> String {
        format!("{}/handoffs/{}", self.prefix, stable_id.0)
    }

    pub fn route_policy(&self, domain: &RoutePolicyDomain) -> String {
        match domain {
            RoutePolicyDomain::Default => format!("{}/system/route-policy/default", self.prefix),
            RoutePolicyDomain::Tenant(tenant) => format!(
                "{}/system/route-policy/tenants/{}",
                self.prefix,
                encode_key_component(tenant)
            ),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

fn encode_key_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        match byte {
            b'0'..=b'9' | b'A'..=b'Z' | b'a'..=b'z' | b'-' | b'_' | b'.' => {
                encoded.push(*byte as char);
            }
            _ => {
                use std::fmt::Write as _;
                let _ = write!(&mut encoded, "%{byte:02X}");
            }
        }
    }
    encoded
}

impl Default for MetadataKeyspace {
    fn default() -> Self {
        Self::new("mc/store-rs/v1")
    }
}

#[cfg(test)]
mod tests {
    use mooncake_store_core::{
        ClientEpoch, ClientRuntimeId, ClientStableId, ObjectKey, RoutePolicyDomain, SegmentName,
    };

    use super::MetadataKeyspace;

    #[test]
    fn keyspace_builds_scoped_keys_and_patterns() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        let runtime = ClientRuntimeId::new("writer", ClientEpoch(9));
        let stable = ClientStableId::new("writer");
        let object = ObjectKey::new("alpha");
        let segment = SegmentName::new("seg-1");

        assert_eq!(keyspace.client(&runtime), "tenant-a/clients/writer:9");
        assert_eq!(keyspace.client_pattern(), "tenant-a/clients/*");
        assert_eq!(
            keyspace.segment(&runtime, &segment),
            "tenant-a/segments/writer:9:seg-1"
        );
        assert_eq!(
            keyspace.segment_prefix(Some(&runtime)),
            "tenant-a/segments/writer:9:"
        );
        assert_eq!(keyspace.segment_prefix(None), "tenant-a/segments/");
        assert_eq!(
            keyspace.segment_pattern(Some(&runtime)),
            "tenant-a/segments/writer:9:*"
        );
        assert_eq!(keyspace.object(&object), "tenant-a/objects/alpha");
        assert_eq!(keyspace.object_prefix(), "tenant-a/objects/");
        assert_eq!(keyspace.object_pattern(), "tenant-a/objects/*");
        assert_eq!(keyspace.handoff(&stable), "tenant-a/handoffs/writer");
        assert_eq!(
            keyspace.route_policy(&RoutePolicyDomain::Default),
            "tenant-a/system/route-policy/default"
        );
        assert_eq!(
            keyspace.route_policy(&RoutePolicyDomain::Tenant("tenant/a".to_string())),
            "tenant-a/system/route-policy/tenants/tenant%2Fa"
        );
        assert_eq!(keyspace.prefix(), "tenant-a");
    }

    #[test]
    fn default_keyspace_uses_store_rs_namespace() {
        assert_eq!(MetadataKeyspace::default().prefix(), "mc/store-rs/v1");
    }
}
