use thiserror::Error;

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuotaKind {
    Bytes,
    Objects,
}

#[derive(Clone, Debug, Error)]
pub enum StoreError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("quota exceeded ({kind:?}): {message}")]
    QuotaExceeded { kind: QuotaKind, message: String },

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("stale epoch: {0}")]
    StaleEpoch(String),

    #[error("unsupported operation: {0}")]
    Unsupported(String),

    #[error("allocator error: {0}")]
    Allocator(String),

    #[error("metadata error: {0}")]
    Metadata(String),

    #[error("transport error: {0}")]
    Transport(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_error_displays_message() {
        let err = StoreError::NotFound("key-abc".to_string());
        assert_eq!(err.to_string(), "object not found: key-abc");
    }

    #[test]
    fn conflict_error_displays_message() {
        let err = StoreError::Conflict("version mismatch".to_string());
        assert_eq!(err.to_string(), "conflict: version mismatch");
    }

    #[test]
    fn invalid_state_error_displays_message() {
        let err = StoreError::InvalidState("not initialized".to_string());
        assert_eq!(err.to_string(), "invalid state: not initialized");
    }

    #[test]
    fn stale_epoch_error_displays_message() {
        let err = StoreError::StaleEpoch("epoch 3 is old".to_string());
        assert_eq!(err.to_string(), "stale epoch: epoch 3 is old");
    }

    #[test]
    fn unsupported_error_displays_message() {
        let err = StoreError::Unsupported("rdma not available".to_string());
        assert_eq!(err.to_string(), "unsupported operation: rdma not available");
    }

    #[test]
    fn allocator_error_displays_message() {
        let err = StoreError::Allocator("out of memory".to_string());
        assert_eq!(err.to_string(), "allocator error: out of memory");
    }

    #[test]
    fn metadata_error_displays_message() {
        let err = StoreError::Metadata("redis connection lost".to_string());
        assert_eq!(err.to_string(), "metadata error: redis connection lost");
    }

    #[test]
    fn transport_error_displays_message() {
        let err = StoreError::Transport("tcp timeout".to_string());
        assert_eq!(err.to_string(), "transport error: tcp timeout");
    }

    #[test]
    fn error_is_debug_printable() {
        let err = StoreError::NotFound("missing".to_string());
        let output = format!("{:?}", err);
        assert!(output.contains("NotFound"));
        assert!(output.contains("missing"));
    }

    #[test]
    fn result_type_alias_works_with_ok() {
        let r: Result<i32> = Ok(42);
        assert_eq!(r.unwrap(), 42);
    }

    #[test]
    fn result_type_alias_works_with_err() {
        let r: Result<i32> = Err(StoreError::NotFound("x".to_string()));
        assert!(r.is_err());
    }

    #[test]
    fn error_variants_are_distinct() {
        let nf = StoreError::NotFound("a".to_string()).to_string();
        let cf = StoreError::Conflict("a".to_string()).to_string();
        assert_ne!(nf, cf);
    }

    #[test]
    fn error_with_empty_message() {
        let err = StoreError::NotFound(String::new());
        assert_eq!(err.to_string(), "object not found: ");
    }

    #[test]
    fn error_with_unicode_message() {
        let err = StoreError::Metadata("café-naïve-α".to_string());
        assert!(err.to_string().contains("café-naïve-α"));
    }

    #[test]
    fn error_with_very_long_message_is_not_truncated() {
        let long = "x".repeat(10_000);
        let err = StoreError::Transport(long.clone());
        assert!(err.to_string().contains(&long));
    }

    #[test]
    fn error_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<StoreError>();
    }

    #[test]
    fn error_clone_preserves_payload() {
        let err = StoreError::StaleEpoch("epoch=5".to_string());
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }
}
