#[cfg(test)]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
pub(crate) fn env_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
