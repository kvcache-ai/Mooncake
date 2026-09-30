use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after Unix epoch")
        .as_millis() as u64
}

/// Lease expiry 30 seconds into the future.
pub fn test_future_expiry_ms() -> u64 {
    now_ms() + 30_000
}
