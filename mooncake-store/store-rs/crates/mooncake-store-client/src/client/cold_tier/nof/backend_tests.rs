use super::*;
use crate::{NofObjectDelete, NofObjectQuery, NofObjectRead, NofObjectWrite};
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

struct RecordingObjectBacking {
    shards: Mutex<Vec<(u32, u32, usize)>>,
    batches: Mutex<Vec<usize>>,
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
    ) -> Result<NofObjectState<Vec<u8>>> {
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
        self.batches.lock().unwrap().push(requests.len());
        self.shards.lock().unwrap().extend(
            requests
                .iter()
                .map(|request| (request.shard_id, request.total_shards, request.value.len())),
        );
        requests.iter().map(|_| Ok(())).collect()
    }
}

impl NofObjectQuery for RecordingObjectBacking {
    fn query_object(&self, _namespace: &NamespaceScope, _key: &str) -> Result<NofObjectState<u64>> {
        Ok(NofObjectState::Found(10))
    }
}

impl NofObjectRead for RecordingObjectBacking {
    fn get_object(
        &self,
        _namespace: &NamespaceScope,
        _key: &str,
    ) -> Result<NofObjectState<Vec<u8>>> {
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
        batches: Mutex::new(Vec::new()),
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
fn object_chunking_does_not_duplicate_provider_batch_splitting() {
    let (backing, backend) = recording_backend(false);
    let scope = NamespaceScope::new("tenant", "domain", "set");
    backend.put_object(&scope, "key", &[0; 1028]).unwrap();
    assert_eq!(*backing.batches.lock().unwrap(), vec![257]);
    assert_eq!(backing.shards.lock().unwrap().len(), 257);
}

#[test]
fn object_put_does_not_require_a_post_write_query() {
    let (_, backend) = recording_backend(false);
    let scope = NamespaceScope::new("tenant", "domain", "set");
    backend.put_object(&scope, "key", &[0; 10]).unwrap();
    assert!(matches!(
        backend.query_object(&scope, "key"),
        Err(StoreError::Unsupported(message)) if message.contains("query")
    ));
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
