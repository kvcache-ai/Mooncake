use std::time::{Duration, Instant};

pub struct LatencyRecorder {
    samples: Vec<u64>,
    total_bytes: u64,
    total_ops: u64,
    error_count: u64,
    start: Instant,
    end: Option<Instant>,
    sorted: bool,
}

impl LatencyRecorder {
    pub fn new() -> Self {
        Self {
            samples: Vec::new(),
            total_bytes: 0,
            total_ops: 0,
            error_count: 0,
            start: Instant::now(),
            end: None,
            sorted: false,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            samples: Vec::with_capacity(capacity),
            total_bytes: 0,
            total_ops: 0,
            error_count: 0,
            start: Instant::now(),
            end: None,
            sorted: false,
        }
    }

    pub fn reset_start(&mut self) {
        self.start = Instant::now();
        self.end = None;
    }

    pub fn finish(&mut self) {
        self.end = Some(Instant::now());
    }

    pub fn record(&mut self, latency: Duration, bytes: u64) {
        self.samples.push(latency.as_micros() as u64);
        self.total_bytes += bytes;
        self.total_ops += 1;
        self.sorted = false;
    }

    pub fn record_error(&mut self) {
        self.error_count += 1;
    }

    pub fn elapsed(&self) -> Duration {
        self.end
            .unwrap_or_else(Instant::now)
            .duration_since(self.start)
    }

    pub fn total_ops(&self) -> u64 {
        self.total_ops
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn error_count(&self) -> u64 {
        self.error_count
    }

    #[cfg(test)]
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    fn ensure_sorted(&mut self) {
        if !self.sorted {
            self.samples.sort_unstable();
            self.sorted = true;
        }
    }

    pub fn percentile(&mut self, p: f64) -> Duration {
        self.ensure_sorted();
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let index = ((p / 100.0) * (self.samples.len() - 1) as f64).round() as usize;
        let index = index.min(self.samples.len() - 1);
        Duration::from_micros(self.samples[index])
    }

    pub fn max_latency(&mut self) -> Duration {
        self.ensure_sorted();
        self.samples
            .last()
            .map(|us| Duration::from_micros(*us))
            .unwrap_or(Duration::ZERO)
    }

    pub fn mean_latency(&self) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let sum: u64 = self.samples.iter().sum();
        Duration::from_micros(sum / self.samples.len() as u64)
    }

    pub fn qps(&self) -> f64 {
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            return 0.0;
        }
        self.total_ops as f64 / elapsed
    }

    pub fn throughput_mib_s(&self) -> f64 {
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            return 0.0;
        }
        self.total_bytes as f64 / elapsed / (1024.0 * 1024.0)
    }

    pub fn merge(&mut self, other: &LatencyRecorder) {
        let was_empty = self.samples.is_empty() && self.total_ops == 0 && self.error_count == 0;
        self.samples.extend_from_slice(&other.samples);
        self.total_bytes += other.total_bytes;
        self.total_ops += other.total_ops;
        self.error_count += other.error_count;
        if was_empty {
            self.start = other.start;
            self.end = other.end;
        } else {
            if other.start < self.start {
                self.start = other.start;
            }
            self.end = match (self.end, other.end) {
                (Some(left), Some(right)) => Some(left.max(right)),
                _ => None,
            };
        }
        self.sorted = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_recorder() {
        let mut r = LatencyRecorder::new();
        assert_eq!(r.total_ops(), 0);
        assert_eq!(r.percentile(50.0), Duration::ZERO);
        assert_eq!(r.max_latency(), Duration::ZERO);
    }

    #[test]
    fn single_sample() {
        let mut r = LatencyRecorder::new();
        r.record(Duration::from_micros(100), 4096);
        assert_eq!(r.total_ops(), 1);
        assert_eq!(r.total_bytes(), 4096);
        assert_eq!(r.percentile(50.0), Duration::from_micros(100));
        assert_eq!(r.percentile(99.0), Duration::from_micros(100));
    }

    #[test]
    fn percentile_ordering() {
        let mut r = LatencyRecorder::new();
        for i in 0..100 {
            r.record(Duration::from_micros(i * 10), 0);
        }
        let p50 = r.percentile(50.0);
        let p90 = r.percentile(90.0);
        let p99 = r.percentile(99.0);
        assert!(p50 <= p90);
        assert!(p90 <= p99);
    }

    #[test]
    fn merge_recorders() {
        let mut a = LatencyRecorder::new();
        a.record(Duration::from_micros(100), 1024);
        a.record_error();

        let mut b = LatencyRecorder::new();
        b.record(Duration::from_micros(200), 2048);
        b.record(Duration::from_micros(300), 4096);

        a.merge(&b);
        assert_eq!(a.total_ops(), 3);
        assert_eq!(a.total_bytes(), 1024 + 2048 + 4096);
        assert_eq!(a.error_count(), 1);
        assert_eq!(a.sample_count(), 3);
    }

    #[test]
    fn mean_latency() {
        let mut r = LatencyRecorder::new();
        r.record(Duration::from_micros(100), 0);
        r.record(Duration::from_micros(200), 0);
        r.record(Duration::from_micros(300), 0);
        assert_eq!(r.mean_latency(), Duration::from_micros(200));
    }

    #[test]
    fn finish_freezes_elapsed_window() {
        let mut r = LatencyRecorder::new();
        std::thread::sleep(Duration::from_millis(2));
        r.finish();
        let elapsed = r.elapsed();
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(r.elapsed(), elapsed);
    }
}
