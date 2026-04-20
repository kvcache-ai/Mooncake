#[cfg(test)]
use std::sync::OnceLock;

#[cfg(test)]
use parking_lot::Mutex;

#[cfg(test)]
pub(crate) fn env_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
