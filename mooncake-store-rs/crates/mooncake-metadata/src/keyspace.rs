use mooncake_store_core::{
    ClientRuntimeId, ClientStableId, ObjectKey, RoutePolicyDomain, SegmentName, TenantPolicyScope,
};
use std::borrow::Cow;

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

    pub fn client_index(&self) -> String {
        format!("{}/indexes/clients", self.prefix)
    }

    pub fn client_by_stable_index(&self, stable_id: &ClientStableId) -> String {
        format!("{}/indexes/clients/by-stable/{}", self.prefix, stable_id.0)
    }

    pub fn client_by_stable_index_prefix(&self) -> String {
        format!("{}/indexes/clients/by-stable/", self.prefix)
    }

    pub fn client_epoch_hwm(&self, stable_id: &ClientStableId) -> String {
        format!("{}/state/client-epoch-hwm/{}", self.prefix, stable_id.0)
    }

    pub fn parse_client_key(&self, key: &str) -> Option<(String, u64)> {
        let prefix = format!("{}/clients/", self.prefix);
        let rest = key.strip_prefix(&prefix)?;
        let (stable_id, epoch_str) = rest.rsplit_once(':')?;
        let epoch = epoch_str.parse::<u64>().ok()?;
        Some((stable_id.to_string(), epoch))
    }

    pub fn client_by_stable_marker(&self, stable_id: &ClientStableId, epoch: u64) -> String {
        format!(
            "{}/indexes/clients/by-stable/{}/{}",
            self.prefix, stable_id.0, epoch
        )
    }

    pub fn client_by_stable_marker_prefix(&self, stable_id: &ClientStableId) -> String {
        format!("{}/indexes/clients/by-stable/{}/", self.prefix, stable_id.0)
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

    pub fn segment_index(&self, owner: Option<&ClientRuntimeId>) -> String {
        match owner {
            Some(owner) => format!("{}/indexes/segments/{}", self.prefix, owner.storage_key()),
            None => format!("{}/indexes/segments", self.prefix),
        }
    }

    pub fn segment_index_for_owner_key(&self, owner_storage_key: &str) -> String {
        format!("{}/indexes/segments/{}", self.prefix, owner_storage_key)
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

    pub fn object_index(&self) -> String {
        format!("{}/indexes/objects", self.prefix)
    }

    pub fn handoff(&self, stable_id: &ClientStableId) -> String {
        format!("{}/handoffs/{}", self.prefix, stable_id.0)
    }

    pub fn route_policy(&self, domain: &RoutePolicyDomain) -> String {
        match domain {
            RoutePolicyDomain::Default => format!("{}default", self.route_policy_prefix()),
            RoutePolicyDomain::Tenant(tenant) => format!(
                "{}tenants/{}",
                self.route_policy_prefix(),
                encode_key_component(tenant)
            ),
        }
    }

    pub fn route_policy_prefix(&self) -> String {
        format!("{}/system/route-policy/", self.prefix)
    }

    pub fn tenant_policy(&self, scope: &TenantPolicyScope) -> String {
        let mut key = format!(
            "{}/system/tenant-policy/tenants/{}",
            self.prefix,
            encode_key_component(&scope.tenant)
        );
        if let Some(domain) = scope.domain.as_deref() {
            key.push_str("/domains/");
            key.push_str(&encode_key_component(domain));
        }
        if let Some(object_set) = scope.object_set.as_deref() {
            key.push_str("/object-sets/");
            key.push_str(&encode_key_component(object_set));
        }
        key
    }

    pub fn tenant_policy_prefix(&self, tenant: Option<&str>) -> String {
        match tenant {
            Some(tenant) => format!(
                "{}/system/tenant-policy/tenants/{}/",
                self.prefix,
                encode_key_component(tenant)
            ),
            None => format!("{}/system/tenant-policy/tenants/", self.prefix),
        }
    }

    pub fn tenant_quota_state(&self, scope: &TenantPolicyScope) -> String {
        format!(
            "{}/system/tenant-quota/tenants/{}",
            self.prefix,
            encode_key_component(&scope.tenant)
        )
    }

    pub fn tenant_object_accounting(&self, key: &ObjectKey) -> String {
        format!(
            "{}/system/tenant-object-accounting/objects/{}",
            self.prefix, key.0
        )
    }

    pub fn tenant_object_accounting_prefix(&self) -> String {
        format!("{}/system/tenant-object-accounting/objects/", self.prefix)
    }

    pub fn tenant_quota_reservation(&self, reservation_id: &str) -> String {
        format!(
            "{}/system/tenant-quota-reservations/{}",
            self.prefix,
            encode_key_component(reservation_id)
        )
    }

    pub fn tenant_quota_reservation_prefix(&self, tenant: Option<&str>) -> String {
        match tenant {
            Some(tenant) => format!(
                "{}/system/tenant-quota-reservations/by-tenant/{}/",
                self.prefix,
                encode_key_component(tenant)
            ),
            None => format!(
                "{}/system/tenant-quota-reservations/by-tenant/",
                self.prefix
            ),
        }
    }

    pub fn tenant_quota_reservation_index(
        &self,
        scope: &TenantPolicyScope,
        reservation_id: &str,
    ) -> String {
        format!(
            "{}{}/{}",
            self.tenant_quota_reservation_prefix(Some(&scope.tenant)),
            "reservations",
            encode_key_component(reservation_id)
        )
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }
}

pub fn parse_route_policy_domain(
    keyspace: &MetadataKeyspace,
    key: &str,
) -> Option<RoutePolicyDomain> {
    let default = keyspace.route_policy(&RoutePolicyDomain::Default);
    if key == default {
        return Some(RoutePolicyDomain::Default);
    }
    let tenant_prefix = format!("{}tenants/", keyspace.route_policy_prefix());
    let encoded = key.strip_prefix(&tenant_prefix)?;
    Some(RoutePolicyDomain::Tenant(
        decode_key_component_checked(encoded)?.into_owned(),
    ))
}

pub fn parse_tenant_policy_scope(
    keyspace: &MetadataKeyspace,
    key: &str,
) -> Option<TenantPolicyScope> {
    let prefix = format!("{}/system/tenant-policy/tenants/", keyspace.prefix());
    let rest = key.strip_prefix(&prefix)?;
    let mut parts = rest.split('/');
    let tenant = decode_key_component_checked(parts.next()?)?.into_owned();
    let mut domain = None;
    let mut object_set = None;
    while let Some(part) = parts.next() {
        match part {
            "domains" => {
                domain = Some(decode_key_component_checked(parts.next()?)?.into_owned());
            }
            "object-sets" => {
                object_set = Some(decode_key_component_checked(parts.next()?)?.into_owned());
            }
            _ => return None,
        }
    }
    let scope = TenantPolicyScope {
        tenant,
        domain,
        object_set,
    };
    scope.validate().ok()?;
    Some(scope)
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

fn decode_key_component_checked(value: &str) -> Option<Cow<'_, str>> {
    if !value.contains('%') {
        return Some(Cow::Borrowed(value));
    }
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return None;
            }
            let hex = &value[index + 1..index + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                decoded.push(byte);
                index += 3;
                continue;
            }
            return None;
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(decoded).ok().map(Cow::Owned)
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

    use super::{parse_route_policy_domain, parse_tenant_policy_scope, MetadataKeyspace};

    #[test]
    fn keyspace_builds_scoped_keys_and_patterns() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        let runtime = ClientRuntimeId::new("writer", ClientEpoch(9));
        let stable = ClientStableId::new("writer");
        let object = ObjectKey::new("alpha");
        let segment = SegmentName::new("seg-1");

        assert_eq!(keyspace.client(&runtime), "tenant-a/clients/writer:9");
        assert_eq!(keyspace.client_pattern(), "tenant-a/clients/*");
        assert_eq!(keyspace.client_index(), "tenant-a/indexes/clients");
        assert_eq!(
            keyspace.client_by_stable_index(&stable),
            "tenant-a/indexes/clients/by-stable/writer"
        );
        assert_eq!(
            keyspace.client_by_stable_index_prefix(),
            "tenant-a/indexes/clients/by-stable/"
        );
        assert_eq!(
            keyspace.client_epoch_hwm(&stable),
            "tenant-a/state/client-epoch-hwm/writer"
        );
        assert_eq!(
            keyspace.parse_client_key("tenant-a/clients/writer:9"),
            Some(("writer".to_string(), 9))
        );
        assert_eq!(
            keyspace.parse_client_key("tenant-a/clients/host:b:42"),
            Some(("host:b".to_string(), 42))
        );
        assert_eq!(
            keyspace.parse_client_key("tenant-a/clients/malformed"),
            None
        );
        assert_eq!(keyspace.parse_client_key("other/clients/writer:9"), None);
        assert_eq!(
            keyspace.client_by_stable_marker(&stable, 9),
            "tenant-a/indexes/clients/by-stable/writer/9"
        );
        assert_eq!(
            keyspace.client_by_stable_marker_prefix(&stable),
            "tenant-a/indexes/clients/by-stable/writer/"
        );
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
        assert_eq!(
            keyspace.segment_index(Some(&runtime)),
            "tenant-a/indexes/segments/writer:9"
        );
        assert_eq!(keyspace.segment_index(None), "tenant-a/indexes/segments");
        assert_eq!(keyspace.object(&object), "tenant-a/objects/alpha");
        assert_eq!(keyspace.object_prefix(), "tenant-a/objects/");
        assert_eq!(keyspace.object_pattern(), "tenant-a/objects/*");
        assert_eq!(keyspace.object_index(), "tenant-a/indexes/objects");
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

    #[test]
    fn route_policy_domain_parser_round_trips_default_and_tenant_keys() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        let default_key = keyspace.route_policy(&RoutePolicyDomain::Default);
        let tenant_key = keyspace.route_policy(&RoutePolicyDomain::Tenant("tenant/a".to_string()));

        assert_eq!(
            parse_route_policy_domain(&keyspace, &default_key),
            Some(RoutePolicyDomain::Default)
        );
        assert_eq!(
            parse_route_policy_domain(&keyspace, &tenant_key),
            Some(RoutePolicyDomain::Tenant("tenant/a".to_string()))
        );
    }

    #[test]
    fn route_policy_domain_parser_rejects_keys_from_different_namespace() {
        let keyspace_a = MetadataKeyspace::new("namespace-a");
        let keyspace_b = MetadataKeyspace::new("namespace-b");
        let key_from_a =
            keyspace_a.route_policy(&RoutePolicyDomain::Tenant("tenant/x".to_string()));

        assert_eq!(parse_route_policy_domain(&keyspace_b, &key_from_a), None);
    }

    #[test]
    fn keyspace_prefix_helpers_cover_route_and_tenant_policy_namespaces() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        assert_eq!(
            keyspace.route_policy_prefix(),
            "tenant-a/system/route-policy/"
        );
        assert_eq!(
            keyspace.tenant_policy_prefix(Some("tenant/a")),
            "tenant-a/system/tenant-policy/tenants/tenant%2Fa/"
        );
    }

    #[test]
    fn tenant_policy_scope_parser_round_trips_encoded_components() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        let scope = mooncake_store_core::TenantPolicyScope::new(
            "tenant/a",
            Some("domain/b"),
            Some("object-set/c"),
        );
        let key = keyspace.tenant_policy(&scope);

        assert_eq!(parse_tenant_policy_scope(&keyspace, &key), Some(scope));
    }

    #[test]
    fn tenant_policy_scope_parser_rejects_invalid_utf8_after_decoding() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        let key = "tenant-a/system/tenant-policy/tenants/%FF";
        assert_eq!(parse_tenant_policy_scope(&keyspace, key), None);
    }

    #[test]
    fn tenant_policy_scope_parser_rejects_incomplete_percent_encoding() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "tenant-a/system/tenant-policy/tenants/%"),
            None
        );
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "tenant-a/system/tenant-policy/tenants/%A"),
            None
        );
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "tenant-a/system/tenant-policy/tenants/%1"),
            None
        );
    }

    #[test]
    fn tenant_policy_scope_parser_rejects_invalid_hex_chars() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "tenant-a/system/tenant-policy/tenants/%GG"),
            None
        );
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "tenant-a/system/tenant-policy/tenants/%ZZ"),
            None
        );
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "tenant-a/system/tenant-policy/tenants/%1G"),
            None
        );
    }
}
