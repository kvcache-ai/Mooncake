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
fn high_level_read_map_preserves_provider_state() {
    assert_eq!(
        NofHighLevelRead::Found(2).map(|value| value * 3),
        NofHighLevelRead::Found(6)
    );
    assert_eq!(
        NofHighLevelRead::<u8>::Missing.map(u16::from),
        NofHighLevelRead::Missing
    );
    assert_eq!(
        NofHighLevelRead::<u8>::Incomplete.map(u16::from),
        NofHighLevelRead::Incomplete
    );
}

struct RecordingHighLevel {
    shards: Mutex<Vec<(u32, u32, usize)>>,
}

impl NofHighLevelExecutor for RecordingHighLevel {
    fn capabilities(&self) -> NofHighLevelCapabilities {
        NofHighLevelCapabilities {
            max_value_size: 4,
            max_key_size: 256,
        }
    }

    fn init_namespace(&self, _namespace: &NamespaceScope) -> Result<()> {
        Ok(())
    }

    fn put_shards(&self, requests: &[NofHighLevelShardPut<'_>]) -> Vec<Result<()>> {
        self.shards.lock().unwrap().extend(
            requests
                .iter()
                .map(|request| (request.shard_id, request.total_shards, request.value.len())),
        );
        requests.iter().map(|_| Ok(())).collect()
    }

    fn query_object(
        &self,
        _namespace: &NamespaceScope,
        _key: &str,
    ) -> Result<NofHighLevelRead<NofHighLevelObjectMetadata>> {
        Ok(NofHighLevelRead::Found(NofHighLevelObjectMetadata {
            length: 10,
            total_shards: 3,
        }))
    }

    fn get_object(
        &self,
        _namespace: &NamespaceScope,
        _key: &str,
    ) -> Result<NofHighLevelRead<NofHighLevelObject>> {
        Ok(NofHighLevelRead::Missing)
    }

    fn delete_object(
        &self,
        _namespace: &NamespaceScope,
        _key: &str,
    ) -> Result<NofHighLevelRead<()>> {
        Ok(NofHighLevelRead::Missing)
    }
}

#[test]
fn high_level_uses_native_zero_based_shards() {
    let executor = Arc::new(RecordingHighLevel {
        shards: Mutex::new(Vec::new()),
    });
    let backend = NofHighLevelBackend::new(executor.clone()).unwrap();
    let scope = NamespaceScope::new("tenant", "domain", "set");
    backend.put_object(&scope, "key", &[0; 10]).unwrap();
    assert_eq!(
        *executor.shards.lock().unwrap(),
        vec![(0, 3, 4), (1, 3, 4), (2, 3, 2)]
    );
}

#[test]
fn high_level_rejects_empty_shards_before_provider_io() {
    let executor = Arc::new(RecordingHighLevel {
        shards: Mutex::new(Vec::new()),
    });
    let backend = NofHighLevelBackend::new(executor.clone()).unwrap();
    let scope = NamespaceScope::new("tenant", "domain", "set");
    assert!(matches!(
        backend.put_object(&scope, "key", &[]),
        Err(StoreError::InvalidState(_))
    ));
    assert!(executor.shards.lock().unwrap().is_empty());
}

#[test]
fn high_level_rejects_oversized_keys_before_provider_io() {
    let executor = Arc::new(RecordingHighLevel {
        shards: Mutex::new(Vec::new()),
    });
    let backend = NofHighLevelBackend::new(executor.clone()).unwrap();
    let scope = NamespaceScope::new("tenant", "domain", "set");
    assert!(matches!(
        backend.put_object(&scope, &"k".repeat(257), b"value"),
        Err(StoreError::InvalidState(_))
    ));
    assert!(executor.shards.lock().unwrap().is_empty());
}
