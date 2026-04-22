use std::time::{Duration, Instant};

/// Polls `predicate` every `interval` until it returns `true` or `timeout` elapses.
/// Panics on timeout.
pub fn wait_for<F>(timeout: Duration, interval: Duration, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if predicate() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "wait_for timed out after {timeout:?}"
        );
        std::thread::sleep(interval);
    }
}

pub fn wait_for_5s<F>(predicate: F)
where
    F: FnMut() -> bool,
{
    wait_for(Duration::from_secs(5), Duration::from_millis(50), predicate);
}

/// Repeatedly evaluates `$expr` until it returns `true`, waiting up to 5 seconds.
#[macro_export]
macro_rules! assert_eventually {
    ($expr:expr) => {
        $crate::assertions::wait_for_5s(|| $expr)
    };
    ($expr:expr, timeout = $t:expr) => {
        $crate::assertions::wait_for($t, ::std::time::Duration::from_millis(50), || $expr)
    };
}
