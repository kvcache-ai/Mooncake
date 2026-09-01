use super::*;
use std::sync::Mutex;

#[test]
fn namespace_fields_do_not_collide() {
    let left = NamespaceScope::new("tenant", "a/b", "c");
    let right = NamespaceScope::new("tenant", "a", "b/c");
    assert_ne!(
        encode_namespace(&left).unwrap(),
        encode_namespace(&right).unwrap()
    );
}

#[test]
fn object_state_map_preserves_provider_state() {
    assert_eq!(
        NofObjectState::Found(2).map(|value| value * 3),
        NofObjectState::Found(6)
    );
    assert_eq!(
        NofObjectState::<u8>::Missing.map(u16::from),
        NofObjectState::Missing
    );
    assert_eq!(
        NofObjectState::<u8>::Incomplete.map(u16::from),
        NofObjectState::Incomplete
    );
}

struct RecordingObjectBacking {
    shards: Mutex<Vec<(u32, u32, usize)>>,
    expose_query: bool,
}

struct ObjectReadWithoutLimits;

impl NofBacking for ObjectReadWithoutLimits {
    fn object_read(&self) -> Option<&dyn NofObjectRead> {
        Some(self)
    }
}

impl NofObjectRead for ObjectReadWithoutLimits {
    fn get_object(
        &self,
        _namespace: &NamespaceScope,
        _key: &str,
    ) -> Result<NofObjectState<NofObject>> {
        Ok(NofObjectState::Missing)
    }
}

impl NofBacking for RecordingObjectBacking {
    fn object_limits(&self) -> Option<NofObjectLimits> {
        Some(NofObjectLimits {
            max_value_size: 4,
            max_key_size: 256,
        })
    }

    fn object_write(&self) -> Option<&dyn NofObjectWrite> {
        Some(self)
    }

    fn object_read(&self) -> Option<&dyn NofObjectRead> {
        Some(self)
    }

    fn object_query(&self) -> Option<&dyn NofObjectQuery> {
        self.expose_query.then_some(self)
    }

    fn object_delete(&self) -> Option<&dyn NofObjectDelete> {
        Some(self)
    }
}

impl NofObjectWrite for RecordingObjectBacking {
    fn init_namespace(&self, _namespace: &NamespaceScope) -> Result<()> {
        Ok(())
    }

    fn put_shards(&self, requests: &[NofObjectShardWrite<'_>]) -> Vec<Result<()>> {
        self.shards.lock().unwrap().extend(
            requests
                .iter()
                .map(|request| (request.shard_id, request.total_shards, request.value.len())),
        );
        requests.iter().map(|_| Ok(())).collect()
    }
}

impl NofObjectQuery for RecordingObjectBacking {
    fn query_object(
        &self,
        _namespace: &NamespaceScope,
        _key: &str,
    ) -> Result<NofObjectState<NofObjectMetadata>> {
        Ok(NofObjectState::Found(NofObjectMetadata {
            length: 10,
            total_shards: 3,
        }))
    }
}

impl NofObjectRead for RecordingObjectBacking {
    fn get_object(
        &self,
        _namespace: &NamespaceScope,
        _key: &str,
    ) -> Result<NofObjectState<NofObject>> {
        Ok(NofObjectState::Missing)
    }
}

impl NofObjectDelete for RecordingObjectBacking {
    fn delete_object(&self, _namespace: &NamespaceScope, _key: &str) -> Result<NofObjectState<()>> {
        Ok(NofObjectState::Missing)
    }
}

fn recording_backend(expose_query: bool) -> (Arc<RecordingObjectBacking>, NofBackend) {
    let backing = Arc::new(RecordingObjectBacking {
        shards: Mutex::new(Vec::new()),
        expose_query,
    });
    let backend = NofBackend::new(backing.clone()).unwrap();
    (backing, backend)
}

#[test]
fn object_capability_requires_matching_limits() {
    assert!(matches!(
        NofBackend::new(Arc::new(ObjectReadWithoutLimits)),
        Err(StoreError::InvalidState(message)) if message.contains("object limits")
    ));
}

#[test]
fn object_backing_uses_native_zero_based_shards() {
    let (backing, backend) = recording_backend(true);
    let scope = NamespaceScope::new("tenant", "domain", "set");
    backend.put_object(&scope, "key", &[0; 10]).unwrap();
    assert_eq!(
        *backing.shards.lock().unwrap(),
        vec![(0, 3, 4), (1, 3, 4), (2, 3, 2)]
    );
}

#[test]
fn object_query_is_an_optional_post_write_capability() {
    let (_, backend) = recording_backend(false);
    let scope = NamespaceScope::new("tenant", "domain", "set");
    assert_eq!(
        backend.put_object(&scope, "key", &[0; 10]).unwrap(),
        NofObjectMetadata {
            length: 10,
            total_shards: 3,
        }
    );
    assert!(matches!(
        backend.query_object(&scope, "key"),
        Err(StoreError::Unsupported(message)) if message.contains("query")
    ));
}

#[test]
fn external_metadata_is_composed_explicitly() {
    let (_, backend) = recording_backend(false);
    assert!(NofBacking::metadata(&backend).is_none());

    let metadata = Arc::new(mooncake_metadata::InMemoryMetadataBackend::new());
    let backend = backend.external_metadata(metadata);
    assert!(NofBacking::metadata(&backend).is_some());
}

#[test]
fn object_backing_rejects_empty_shards_before_provider_io() {
    let (backing, backend) = recording_backend(true);
    let scope = NamespaceScope::new("tenant", "domain", "set");
    assert!(matches!(
        backend.put_object(&scope, "key", &[]),
        Err(StoreError::InvalidState(_))
    ));
    assert!(backing.shards.lock().unwrap().is_empty());
}

#[test]
fn object_backing_rejects_oversized_keys_before_provider_io() {
    let (backing, backend) = recording_backend(true);
    let scope = NamespaceScope::new("tenant", "domain", "set");
    assert!(matches!(
        backend.put_object(&scope, &"k".repeat(257), b"value"),
        Err(StoreError::InvalidState(_))
    ));
    assert!(backing.shards.lock().unwrap().is_empty());
}
