use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::cli::FaultSpec;

/// Thread-safe fault injector used by benchmark workers.
/// Workers call `pre_op()` before each store operation to apply configured faults.
pub struct FaultInjector {
    specs: Vec<FaultSpec>,
    op_counter: Arc<AtomicU64>,
}

impl FaultInjector {
    pub fn new(specs: Vec<FaultSpec>) -> Self {
        Self {
            specs,
            op_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// Call before each store operation. Returns an error string if the operation
    /// should be treated as failed (e.g. metadata-drop or transport-error).
    pub fn pre_op(&self) -> Option<String> {
        if self.specs.is_empty() {
            return None;
        }
        let op_n = self.op_counter.fetch_add(1, Ordering::Relaxed);

        for spec in &self.specs {
            match spec {
                FaultSpec::RedisJitter { min_ms, max_ms } => {
                    let delay = jitter_ms(*min_ms, *max_ms, op_n);
                    if delay > 0 {
                        thread::sleep(Duration::from_millis(delay));
                    }
                }
                FaultSpec::MetadataDrop { percent } => {
                    if should_trigger(*percent, op_n) {
                        return Some(format!("fault-injected: metadata-drop ({percent}%)"));
                    }
                }
                FaultSpec::TransportDelay { min_ms, max_ms } => {
                    let delay = jitter_ms(*min_ms, *max_ms, op_n);
                    if delay > 0 {
                        thread::sleep(Duration::from_millis(delay));
                    }
                }
                FaultSpec::TransportError { percent } => {
                    if should_trigger(*percent, op_n) {
                        return Some(format!("fault-injected: transport-error ({percent}%)"));
                    }
                }
            }
        }
        None
    }
}

/// Deterministic jitter in [min_ms, max_ms] range, seeded by op counter.
fn jitter_ms(min_ms: u64, max_ms: u64, seed: u64) -> u64 {
    if min_ms >= max_ms {
        return min_ms;
    }
    let range = max_ms - min_ms;
    // LCG-based cheap random from op counter
    let r = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    min_ms + (r >> 32) % range
}

/// Returns true for `percent`% of operations, based on op counter.
fn should_trigger(percent: u8, seed: u64) -> bool {
    if percent == 0 {
        return false;
    }
    if percent >= 100 {
        return true;
    }
    let r = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (r % 100) < percent as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::FaultSpec;

    #[test]
    fn no_faults() {
        let fi = FaultInjector::new(vec![]);
        assert!(fi.is_empty());
        assert!(fi.pre_op().is_none());
    }

    #[test]
    fn metadata_drop_100_percent() {
        let fi = FaultInjector::new(vec![FaultSpec::MetadataDrop { percent: 100 }]);
        assert!(fi.pre_op().is_some());
    }

    #[test]
    fn metadata_drop_0_percent() {
        let fi = FaultInjector::new(vec![FaultSpec::MetadataDrop { percent: 0 }]);
        assert!(fi.pre_op().is_none());
    }

    #[test]
    fn transport_error_100_percent() {
        let fi = FaultInjector::new(vec![FaultSpec::TransportError { percent: 100 }]);
        assert!(fi.pre_op().is_some());
    }

    #[test]
    fn jitter_bounds() {
        for seed in 0..1000 {
            let v = jitter_ms(10, 50, seed);
            assert!(v >= 10 && v < 50, "jitter {v} out of range for seed {seed}");
        }
    }

    #[test]
    fn trigger_rate_approximate() {
        // Over 1000 ops with 50% trigger rate, expect 400-600 triggers
        let count: u64 = (0..1000).filter(|&i| should_trigger(50, i)).count() as u64;
        assert!(
            count > 400 && count < 600,
            "trigger rate far from 50%: {count}/1000"
        );
    }
}
