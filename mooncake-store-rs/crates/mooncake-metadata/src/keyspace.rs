use mooncake_store_core::{
    ClientRuntimeId, ClientStableId, ObjectKey, RoutePolicyDomain, SegmentName, TenantPolicyScope,
};
use std::borrow::Cow;

// ── MetadataKeyspace ──────────────────────────────────────────────────
//
// All generated Redis keys embed a hash-tag `{prefix}` so that every
// key in the same keyspace maps to the same Redis Cluster hash slot.
// This is required because Lua scripts (lease allocation, tenant quota,
// etc.) touch multiple keys atomically and Redis Cluster demands all
// KEYS in a single EVALSHA land in the same slot.

#[derive(Clone, Debug)]
pub struct MetadataKeyspace {
    raw_prefix: String,
    slot_tag: String,
}

impl MetadataKeyspace {
    pub fn new(prefix: impl Into<String>) -> Self {
        let raw_prefix = prefix.into();
        let slot_tag = format!("{{{raw_prefix}}}");
        Self {
            raw_prefix,
            slot_tag,
        }
    }

    pub fn client(&self, runtime: &ClientRuntimeId) -> String {
        format!("{}/clients/{}", self.slot_tag, runtime.storage_key())
    }

    pub fn client_lease_field(&self) -> &'static str {
        "lease"
    }

    pub fn client_prefix_for_stable(&self, stable_id: &ClientStableId) -> String {
        format!("{}/clients/{}:", self.slot_tag, stable_id.0)
    }

    pub fn client_pattern(&self) -> String {
        format!("{}/clients/*", self.slot_tag)
    }

    pub fn client_index(&self) -> String {
        format!("{}/indexes/clients", self.slot_tag)
    }

    pub fn client_by_stable_index(&self, stable_id: &ClientStableId) -> String {
        format!(
            "{}/indexes/clients/by-stable/{}",
            self.slot_tag, stable_id.0
        )
    }

    pub fn client_by_stable_index_prefix(&self) -> String {
        format!("{}/indexes/clients/by-stable/", self.slot_tag)
    }

    pub fn client_epoch_hwm(&self, stable_id: &ClientStableId) -> String {
        format!("{}/state/client-epoch-hwm/{}", self.slot_tag, stable_id.0)
    }

    pub fn client_lease_expiry_index(&self) -> String {
        format!("{}/system/client-lease-expiry", self.slot_tag)
    }

    pub fn client_lease_expiry_runtime(&self, runtime: &ClientRuntimeId) -> String {
        format!(
            "{}/system/client-lease-expiry/by-runtime/{}",
            self.slot_tag,
            runtime.storage_key()
        )
    }

    pub fn client_lease_expiry_runtime_prefix(&self) -> String {
        format!("{}/system/client-lease-expiry/by-runtime/", self.slot_tag)
    }

    pub fn client_lease_expiry_time_prefix(&self) -> String {
        format!("{}/system/client-lease-expiry/by-time/", self.slot_tag)
    }

    pub fn client_lease_expiry_time(
        &self,
        expires_at_ms: u64,
        runtime: &ClientRuntimeId,
    ) -> String {
        format!(
            "{}{:020}/{}",
            self.client_lease_expiry_time_prefix(),
            expires_at_ms,
            runtime.storage_key()
        )
    }

    pub fn client_lease_expiry_time_range_end(&self, expires_before_exclusive_ms: u64) -> String {
        format!(
            "{}{:020}/",
            self.client_lease_expiry_time_prefix(),
            expires_before_exclusive_ms
        )
    }

    pub fn parse_client_key(&self, key: &str) -> Option<(String, u64)> {
        let prefix = format!("{}/clients/", self.slot_tag);
        let rest = key.strip_prefix(&prefix)?;
        parse_runtime_storage_key(rest)
    }

    pub fn parse_client_lease_expiry_time_key(&self, key: &str) -> Option<(u64, String, u64)> {
        let prefix = self.client_lease_expiry_time_prefix();
        let rest = key.strip_prefix(&prefix)?;
        let (expires_at_ms, runtime_storage_key) = rest.split_once('/')?;
        let expires_at_ms = expires_at_ms.parse::<u64>().ok()?;
        let (stable_id, epoch) = parse_runtime_storage_key(runtime_storage_key)?;
        Some((expires_at_ms, stable_id, epoch))
    }

    pub fn parse_client_lease_expiry_runtime_key(&self, key: &str) -> Option<(String, u64)> {
        let prefix = self.client_lease_expiry_runtime_prefix();
        let rest = key.strip_prefix(&prefix)?;
        let (stable_id, epoch_str) = rest.rsplit_once(':')?;
        let epoch = epoch_str.parse::<u64>().ok()?;
        Some((stable_id.to_string(), epoch))
    }

    pub fn client_by_stable_marker(&self, stable_id: &ClientStableId, epoch: u64) -> String {
        format!(
            "{}/indexes/clients/by-stable/{}/{}",
            self.slot_tag, stable_id.0, epoch
        )
    }

    pub fn client_by_stable_marker_prefix(&self, stable_id: &ClientStableId) -> String {
        format!(
            "{}/indexes/clients/by-stable/{}/",
            self.slot_tag, stable_id.0
        )
    }

    pub fn stable_runtime(&self, stable_id: &ClientStableId) -> String {
        format!(
            "{}/indexes/stable-runtimes/{}",
            self.slot_tag,
            encode_key_component(&stable_id.0)
        )
    }

    pub fn segment(&self, owner: &ClientRuntimeId, segment: &SegmentName) -> String {
        format!(
            "{}/client-resources/{}/segments/{}",
            self.slot_tag,
            owner.storage_key(),
            encode_key_component(&segment.0)
        )
    }

    pub fn client_segment_field(&self, segment: &SegmentName) -> String {
        format!("segment:{}", encode_key_component(&segment.0))
    }

    pub fn client_segment_field_prefix(&self) -> &'static str {
        "segment:"
    }

    pub fn segment_prefix(&self, owner: Option<&ClientRuntimeId>) -> String {
        match owner {
            Some(owner) => format!(
                "{}/client-resources/{}/segments/",
                self.slot_tag,
                owner.storage_key()
            ),
            None => format!("{}/client-resources/", self.slot_tag),
        }
    }

    pub fn segment_pattern(&self, owner: Option<&ClientRuntimeId>) -> String {
        format!("{}*", self.segment_prefix(owner))
    }

    pub fn segment_index(&self, owner: Option<&ClientRuntimeId>) -> String {
        match owner {
            Some(owner) => format!(
                "{}/client-resources/{}/indexes/segments",
                self.slot_tag,
                owner.storage_key()
            ),
            None => format!("{}/client-resources/indexes/segments", self.slot_tag),
        }
    }

    pub fn segment_index_for_owner_key(&self, owner_storage_key: &str) -> String {
        format!(
            "{}/client-resources/{}/indexes/segments",
            self.slot_tag, owner_storage_key
        )
    }

    pub fn object(&self, key: &ObjectKey) -> String {
        format!("{}/objects/{}", self.slot_tag, key.0)
    }

    pub fn object_prefix(&self) -> String {
        format!("{}/objects/", self.slot_tag)
    }

    pub fn object_pattern(&self) -> String {
        format!("{}*", self.object_prefix())
    }

    pub fn object_index(&self) -> String {
        format!("{}/indexes/objects", self.slot_tag)
    }

    pub fn handoff(&self, stable_id: &ClientStableId) -> String {
        format!("{}/handoffs/{}", self.slot_tag, stable_id.0)
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
        format!("{}/system/route-policy/", self.slot_tag)
    }

    pub fn tenant_policy(&self, scope: &TenantPolicyScope) -> String {
        let mut key = format!(
            "{}/system/tenant-policy/tenants/{}",
            self.slot_tag,
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
                self.slot_tag,
                encode_key_component(tenant)
            ),
            None => format!("{}/system/tenant-policy/tenants/", self.slot_tag),
        }
    }

    pub fn tenant_policy_index(&self, tenant: &str) -> String {
        format!(
            "{}/system/tenant-policy-index/by-tenant/{}",
            self.slot_tag,
            encode_key_component(tenant)
        )
    }

    pub fn tenant_policy_index_ready(&self, tenant: &str) -> String {
        format!(
            "{}/system/tenant-policy-index-ready/by-tenant/{}",
            self.slot_tag,
            encode_key_component(tenant)
        )
    }

    pub fn tenant_quota_state(&self, scope: &TenantPolicyScope) -> String {
        format!(
            "{}/system/tenant-quota/tenants/{}",
            self.slot_tag,
            encode_key_component(&scope.tenant)
        )
    }

    pub fn tenant_object_accounting(&self, key: &ObjectKey) -> String {
        format!(
            "{}/system/tenant-object-accounting/objects/{}",
            self.slot_tag, key.0
        )
    }

    pub fn tenant_object_accounting_prefix(&self) -> String {
        format!("{}/system/tenant-object-accounting/objects/", self.slot_tag)
    }

    pub fn tenant_quota_reservation(&self, reservation_id: &str) -> String {
        format!(
            "{}/system/tenant-quota-reservations/{}",
            self.slot_tag,
            encode_key_component(reservation_id)
        )
    }

    pub fn tenant_quota_reservation_prefix(&self, tenant: Option<&str>) -> String {
        match tenant {
            Some(tenant) => format!(
                "{}/system/tenant-quota-reservations/by-tenant/{}/",
                self.slot_tag,
                encode_key_component(tenant)
            ),
            None => format!(
                "{}/system/tenant-quota-reservations/by-tenant/",
                self.slot_tag
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

    pub fn tenant_eviction_frontier(&self, tenant: &str) -> String {
        format!(
            "{}/indexes/tenant-eviction/tenants/{}",
            self.slot_tag,
            encode_key_component(tenant)
        )
    }

    pub fn tenant_eviction_frontier_prefix(&self, tenant: &str) -> String {
        format!("{}/", self.tenant_eviction_frontier(tenant))
    }

    pub fn tenant_eviction_candidate(
        &self,
        tenant: &str,
        updated_at_ms: u64,
        committed_length: u64,
        key: &ObjectKey,
    ) -> String {
        format!(
            "{}{updated_at_ms:020}/{:020}/{}",
            self.tenant_eviction_frontier_prefix(tenant),
            u64::MAX - committed_length,
            encode_key_component(&key.0)
        )
    }

    pub fn prefix(&self) -> &str {
        &self.raw_prefix
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

pub fn parse_tenant_eviction_candidate_key(
    keyspace: &MetadataKeyspace,
    tenant: &str,
    key: &str,
) -> Option<ObjectKey> {
    let prefix = keyspace.tenant_eviction_frontier_prefix(tenant);
    let rest = key.strip_prefix(&prefix)?;
    let mut parts = rest.split('/');
    parts.next()?;
    parts.next()?;
    let encoded_key = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    Some(ObjectKey::new(
        decode_key_component_checked(encoded_key)?.into_owned(),
    ))
}

pub fn parse_tenant_policy_scope(
    keyspace: &MetadataKeyspace,
    key: &str,
) -> Option<TenantPolicyScope> {
    let prefix = keyspace.tenant_policy_prefix(None);
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
        Self::new("mc/store-rs/v2")
    }
}

fn parse_runtime_storage_key(storage_key: &str) -> Option<(String, u64)> {
    let (stable_id, epoch_str) = storage_key.rsplit_once(':')?;
    let epoch = epoch_str.parse::<u64>().ok()?;
    Some((stable_id.to_string(), epoch))
}

#[cfg(test)]
mod tests {
    use mooncake_store_core::{
        ClientEpoch, ClientRuntimeId, ClientStableId, ObjectKey, RoutePolicyDomain, SegmentName,
        TenantPolicyScope,
    };

    use super::{
        decode_key_component_checked, encode_key_component, parse_route_policy_domain,
        parse_tenant_eviction_candidate_key, parse_tenant_policy_scope, MetadataKeyspace,
    };

    #[test]
    fn keyspace_builds_scoped_keys_and_patterns() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        let runtime = ClientRuntimeId::new("writer", ClientEpoch(9));
        let stable = ClientStableId::new("writer");
        let object = ObjectKey::new("alpha");
        let segment = SegmentName::new("seg-1");

        assert_eq!(keyspace.client(&runtime), "{tenant-a}/clients/writer:9");
        assert_eq!(
            keyspace.client_prefix_for_stable(&stable),
            "{tenant-a}/clients/writer:"
        );
        assert_eq!(keyspace.client_pattern(), "{tenant-a}/clients/*");
        assert_eq!(keyspace.client_index(), "{tenant-a}/indexes/clients");
        assert_eq!(
            keyspace.client_by_stable_index(&stable),
            "{tenant-a}/indexes/clients/by-stable/writer"
        );
        assert_eq!(
            keyspace.client_by_stable_index_prefix(),
            "{tenant-a}/indexes/clients/by-stable/"
        );
        assert_eq!(
            keyspace.client_epoch_hwm(&stable),
            "{tenant-a}/state/client-epoch-hwm/writer"
        );
        assert_eq!(
            keyspace.client_lease_expiry_index(),
            "{tenant-a}/system/client-lease-expiry"
        );
        assert_eq!(
            keyspace.client_lease_expiry_runtime(&runtime),
            "{tenant-a}/system/client-lease-expiry/by-runtime/writer:9"
        );
        assert_eq!(
            keyspace.client_lease_expiry_runtime_prefix(),
            "{tenant-a}/system/client-lease-expiry/by-runtime/"
        );
        assert_eq!(
            keyspace.client_lease_expiry_time_prefix(),
            "{tenant-a}/system/client-lease-expiry/by-time/"
        );
        assert_eq!(
            keyspace.client_lease_expiry_time(123, &runtime),
            "{tenant-a}/system/client-lease-expiry/by-time/00000000000000000123/writer:9"
        );
        assert_eq!(
            keyspace.client_lease_expiry_time_range_end(124),
            "{tenant-a}/system/client-lease-expiry/by-time/00000000000000000124/"
        );
        assert_eq!(
            keyspace.parse_client_key("{tenant-a}/clients/writer:9"),
            Some(("writer".to_string(), 9))
        );
        assert_eq!(
            keyspace.parse_client_key("{tenant-a}/clients/host:b:42"),
            Some(("host:b".to_string(), 42))
        );
        assert_eq!(
            keyspace.parse_client_key("{tenant-a}/clients/malformed"),
            None
        );
        assert_eq!(keyspace.parse_client_key("other/clients/writer:9"), None);
        assert_eq!(
            keyspace.parse_client_lease_expiry_time_key(
                "{tenant-a}/system/client-lease-expiry/by-time/00000000000000000123/writer:9"
            ),
            Some((123, "writer".to_string(), 9))
        );
        assert_eq!(
            keyspace.parse_client_lease_expiry_runtime_key(
                "{tenant-a}/system/client-lease-expiry/by-runtime/writer:9"
            ),
            Some(("writer".to_string(), 9))
        );
        assert_eq!(
            keyspace.client_by_stable_marker(&stable, 9),
            "{tenant-a}/indexes/clients/by-stable/writer/9"
        );
        assert_eq!(
            keyspace.client_by_stable_marker_prefix(&stable),
            "{tenant-a}/indexes/clients/by-stable/writer/"
        );
        assert_eq!(
            keyspace.segment(&runtime, &segment),
            "{tenant-a}/client-resources/writer:9/segments/seg-1"
        );
        assert_eq!(keyspace.client_lease_field(), "lease");
        assert_eq!(keyspace.client_segment_field(&segment), "segment:seg-1");
        assert_eq!(keyspace.client_segment_field_prefix(), "segment:");
        assert_eq!(
            keyspace.segment_prefix(Some(&runtime)),
            "{tenant-a}/client-resources/writer:9/segments/"
        );
        assert_eq!(
            keyspace.segment_prefix(None),
            "{tenant-a}/client-resources/"
        );
        assert_eq!(
            keyspace.segment_pattern(Some(&runtime)),
            "{tenant-a}/client-resources/writer:9/segments/*"
        );
        assert_eq!(
            keyspace.segment_index(Some(&runtime)),
            "{tenant-a}/client-resources/writer:9/indexes/segments"
        );
        assert_eq!(
            keyspace.segment_index(None),
            "{tenant-a}/client-resources/indexes/segments"
        );
        assert_eq!(keyspace.object(&object), "{tenant-a}/objects/alpha");
        assert_eq!(keyspace.object_prefix(), "{tenant-a}/objects/");
        assert_eq!(keyspace.object_pattern(), "{tenant-a}/objects/*");
        assert_eq!(keyspace.object_index(), "{tenant-a}/indexes/objects");
        assert_eq!(
            keyspace.tenant_eviction_frontier("tenant/a"),
            "{tenant-a}/indexes/tenant-eviction/tenants/tenant%2Fa"
        );
        assert_eq!(
            keyspace.tenant_eviction_frontier_prefix("tenant/a"),
            "{tenant-a}/indexes/tenant-eviction/tenants/tenant%2Fa/"
        );
        assert_eq!(
            keyspace.tenant_eviction_candidate("tenant/a", 7, 11, &object),
            "{tenant-a}/indexes/tenant-eviction/tenants/tenant%2Fa/00000000000000000007/18446744073709551604/alpha"
        );
        assert_eq!(keyspace.handoff(&stable), "{tenant-a}/handoffs/writer");
        assert_eq!(
            keyspace.route_policy(&RoutePolicyDomain::Default),
            "{tenant-a}/system/route-policy/default"
        );
        assert_eq!(
            keyspace.route_policy(&RoutePolicyDomain::Tenant("tenant/a".to_string())),
            "{tenant-a}/system/route-policy/tenants/tenant%2Fa"
        );
        assert_eq!(keyspace.prefix(), "tenant-a");
    }

    #[test]
    fn default_keyspace_uses_store_rs_namespace() {
        assert_eq!(MetadataKeyspace::default().prefix(), "mc/store-rs/v2");
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
            "{tenant-a}/system/route-policy/"
        );
        assert_eq!(
            keyspace.tenant_policy_prefix(Some("tenant/a")),
            "{tenant-a}/system/tenant-policy/tenants/tenant%2Fa/"
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
    fn tenant_eviction_candidate_key_parser_round_trips_encoded_object_key() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        let object = ObjectKey::new("tenant/a::path/to/object");
        let key = keyspace.tenant_eviction_candidate("tenant/a", 7, 11, &object);

        assert_eq!(
            parse_tenant_eviction_candidate_key(&keyspace, "tenant/a", &key),
            Some(object)
        );
    }

    #[test]
    fn tenant_policy_scope_parser_rejects_invalid_utf8_after_decoding() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        let key = "{tenant-a}/system/tenant-policy/tenants/%FF";
        assert_eq!(parse_tenant_policy_scope(&keyspace, key), None);
    }

    #[test]
    fn tenant_policy_scope_parser_rejects_incomplete_percent_encoding() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "{tenant-a}/system/tenant-policy/tenants/%"),
            None
        );
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "{tenant-a}/system/tenant-policy/tenants/%A"),
            None
        );
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "{tenant-a}/system/tenant-policy/tenants/%1"),
            None
        );
    }

    #[test]
    fn tenant_policy_scope_parser_rejects_invalid_hex_chars() {
        let keyspace = MetadataKeyspace::new("tenant-a");
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "{tenant-a}/system/tenant-policy/tenants/%GG"),
            None
        );
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "{tenant-a}/system/tenant-policy/tenants/%ZZ"),
            None
        );
        assert_eq!(
            parse_tenant_policy_scope(&keyspace, "{tenant-a}/system/tenant-policy/tenants/%1G"),
            None
        );
    }

    #[test]
    fn all_keys_in_same_redis_cluster_slot() {
        let keyspace = MetadataKeyspace::new("mc/store-rs/v2");
        let stable = ClientStableId::new("writer");
        let runtime = ClientRuntimeId::new("writer", ClientEpoch(1));

        let keys = [
            keyspace.client(&runtime),
            keyspace.client_prefix_for_stable(&stable),
            keyspace.client_index(),
            keyspace.client_by_stable_index(&stable),
            keyspace.client_epoch_hwm(&stable),
            keyspace.client_lease_expiry_index(),
            keyspace.client_lease_expiry_runtime(&runtime),
            keyspace.client_lease_expiry_time(42, &runtime),
            keyspace.object(&ObjectKey::new("key-1")),
            keyspace.object_index(),
            keyspace.tenant_eviction_frontier("tenant-a"),
        ];

        fn extract_hash_tag(key: &str) -> Option<&str> {
            let start = key.find('{')?;
            let end = key[start..].find('}')? + start;
            Some(&key[start + 1..end])
        }

        let first_tag = extract_hash_tag(&keys[0]).expect("key should have hash tag");
        for key in &keys[1..] {
            assert_eq!(
                extract_hash_tag(key).expect("key should have hash tag"),
                first_tag,
                "all keys must share the same hash tag: {key}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Adversarial: percent-encoding roundtrip + rejection + key-shape
    // -----------------------------------------------------------------------

    #[test]
    fn encode_decode_round_trip_ascii_safe_chars() {
        let input = "abcABC012-_";
        let encoded = encode_key_component(input);
        assert_eq!(encoded, input, "ASCII-safe chars must not be escaped");
        let decoded = decode_key_component_checked(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn encode_decode_round_trip_special_chars() {
        let input = "has/slash has:colon%percent?question#hash";
        let encoded = encode_key_component(input);
        assert_ne!(encoded, input, "special chars must be percent-escaped");
        let decoded = decode_key_component_checked(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn encode_decode_round_trip_unicode() {
        let input = "α-tenant/β-domain/γ-set naïve/path";
        let encoded = encode_key_component(input);
        let decoded = decode_key_component_checked(&encoded).unwrap();
        assert_eq!(decoded, input);
    }

    #[test]
    fn encode_decode_round_trip_empty_string() {
        let encoded = encode_key_component("");
        assert_eq!(encoded, "");
        let decoded = decode_key_component_checked(&encoded).unwrap();
        assert_eq!(decoded, "");
    }

    #[test]
    fn decode_rejects_truncated_percent_at_end() {
        assert!(decode_key_component_checked("hello%").is_none());
        assert!(decode_key_component_checked("hello%2").is_none());
    }

    #[test]
    fn decode_rejects_non_hex_after_percent() {
        assert!(decode_key_component_checked("%XY").is_none());
        assert!(decode_key_component_checked("%0G").is_none());
    }

    #[test]
    fn keyspace_object_key_preserves_embedded_slashes() {
        let keyspace = MetadataKeyspace::new("ns");
        let key = ObjectKey::new("path/to/model/weights");
        let full = keyspace.object(&key);
        assert!(
            full.contains("path/to/model/weights"),
            "ObjectKey slashes must survive into the key path: got {full}"
        );
    }

    #[test]
    fn keyspace_segment_key_preserves_special_chars_in_name() {
        let keyspace = MetadataKeyspace::new("ns");
        let runtime = ClientRuntimeId::new("node-1", ClientEpoch(42));
        let segment = SegmentName::new("seg:special");
        let key = keyspace.segment(&runtime, &segment);
        assert_eq!(
            key,
            "{ns}/client-resources/node-1:42/segments/seg%3Aspecial"
        );
    }

    #[test]
    fn route_policy_domain_tenant_with_special_chars_round_trips_via_percent_encoding() {
        let keyspace = MetadataKeyspace::new("ns");
        let domain = RoutePolicyDomain::Tenant("tenant/with:special".to_string());
        let key = keyspace.route_policy(&domain);
        let parsed = parse_route_policy_domain(&keyspace, &key);
        assert_eq!(parsed, Some(domain));
    }

    #[test]
    fn route_policy_domain_parse_rejects_unrelated_key() {
        let keyspace = MetadataKeyspace::new("ns");
        assert_eq!(parse_route_policy_domain(&keyspace, "unrelated/key"), None);
    }

    #[test]
    fn tenant_policy_scope_with_unicode_round_trips() {
        let keyspace = MetadataKeyspace::new("ns");
        let scope = TenantPolicyScope::new("α-tenant", Some("β-domain"), Some("γ-set"));
        let key = keyspace.tenant_policy(&scope);
        let parsed = parse_tenant_policy_scope(&keyspace, &key);
        assert_eq!(parsed, Some(scope));
    }
}
