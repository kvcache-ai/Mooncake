use thiserror::Error;

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("object not found: {0}")]
    NotFound(String),

    #[error("conflict: {0}")]
    Conflict(String),

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
