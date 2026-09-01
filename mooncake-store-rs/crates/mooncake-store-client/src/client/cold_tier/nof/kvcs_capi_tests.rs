use super::*;

#[test]
fn physical_key_limit_is_checked_after_hex_encoding() {
    let key = OpaquePhysicalKey::new(vec![0xab; 128]);
    assert!(encode_physical_key(&key, 256).is_ok());
    let oversized = OpaquePhysicalKey::new(vec![0xab; 129]);
    assert!(encode_physical_key(&oversized, 256).is_err());
}

#[test]
fn incomplete_is_not_mapped_to_not_found() {
    assert!(matches!(
        map_call_status(-2, "query"),
        StoreError::Backpressure(_)
    ));
}

#[test]
fn malformed_numeric_configuration_is_rejected() {
    assert!(matches!(
        parse_env_number::<u32>("MOONCAKE_KVCS_GET_WORKERS", Some("four".to_string()), 0),
        Err(StoreError::InvalidState(_))
    ));
}

#[test]
fn capi_config_layout_matches_the_extended_sdk_abi() {
    assert_eq!(std::mem::size_of::<KvcsClientConfig>(), 96);
    assert_eq!(std::mem::size_of::<KvcsLlConfig>(), 72);
}
