use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use mooncake_store_core::{Result, StoreError};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{EnvFilter, fmt};

type MetricKey = (&'static str, &'static str);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OperationMetricSnapshot {
    pub operation: &'static str,
    pub status: &'static str,
    pub calls_total: u64,
    pub bytes_in_total: u64,
    pub bytes_out_total: u64,
    pub latency_total_us: u64,
    pub latency_max_us: u64,
}

#[derive(Default)]
struct OperationMetricState {
    calls_total: u64,
    bytes_in_total: u64,
    bytes_out_total: u64,
    latency_total_us: u64,
    latency_max_us: u64,
}

#[derive(Default)]
struct MetricsRegistry {
    operations: BTreeMap<MetricKey, OperationMetricState>,
}

impl MetricsRegistry {
    fn record(
        &mut self,
        operation: &'static str,
        status: &'static str,
        bytes_in: u64,
        bytes_out: u64,
        latency_us: u64,
    ) {
        let state = self.operations.entry((operation, status)).or_default();
        state.calls_total = state.calls_total.saturating_add(1);
        state.bytes_in_total = state.bytes_in_total.saturating_add(bytes_in);
        state.bytes_out_total = state.bytes_out_total.saturating_add(bytes_out);
        state.latency_total_us = state.latency_total_us.saturating_add(latency_us);
        state.latency_max_us = state.latency_max_us.max(latency_us);
    }

    fn snapshot(&self) -> Vec<OperationMetricSnapshot> {
        self.operations
            .iter()
            .map(|((operation, status), state)| OperationMetricSnapshot {
                operation,
                status,
                calls_total: state.calls_total,
                bytes_in_total: state.bytes_in_total,
                bytes_out_total: state.bytes_out_total,
                latency_total_us: state.latency_total_us,
                latency_max_us: state.latency_max_us,
            })
            .collect()
    }
}

static METRICS: OnceLock<Mutex<MetricsRegistry>> = OnceLock::new();
static TRACING_STATE: OnceLock<()> = OnceLock::new();

pub struct OperationTracker {
    operation: &'static str,
    start: Instant,
    bytes_in: u64,
}

impl OperationTracker {
    pub fn new(operation: &'static str) -> Self {
        Self {
            operation,
            start: Instant::now(),
            bytes_in: 0,
        }
    }

    pub fn input_bytes(mut self, bytes_in: u64) -> Self {
        self.bytes_in = bytes_in;
        self
    }

    pub fn finish<T>(&self, result: &Result<T>, bytes_out: u64) {
        let status = if result.is_ok() { "ok" } else { "error" };
        let latency_us = self.start.elapsed().as_micros() as u64;
        metrics_registry().lock().expect("metrics lock poisoned").record(
            self.operation,
            status,
            self.bytes_in,
            bytes_out,
            latency_us,
        );
    }
}

pub fn init_tracing(filter: Option<&str>) -> Result<()> {
    if TRACING_STATE.get().is_some() {
        return Ok(());
    }

    let env_filter = match filter {
        Some(filter) => EnvFilter::try_new(filter)
            .map_err(|error| StoreError::InvalidState(format!("invalid tracing filter: {error}")))?,
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };

    let subscriber = fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::CLOSE);

    match subscriber.try_init() {
        Ok(()) => {
            let _ = TRACING_STATE.set(());
            Ok(())
        }
        Err(error) => {
            let _ = TRACING_STATE.set(());
            if error
                .to_string()
                .contains("global default trace dispatcher has already been set")
            {
                return Ok(());
            }
            Err(StoreError::InvalidState(format!("tracing init failed: {error}")))
        }
    }
}

pub fn init_tracing_from_env(toggle_env: &str, filter_env: &str) -> Result<bool> {
    let enabled = std::env::var(toggle_env)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    if !enabled {
        return Ok(false);
    }

    let filter = std::env::var(filter_env).ok();
    init_tracing(filter.as_deref())?;
    Ok(true)
}

pub fn render_prometheus_metrics() -> String {
    let snapshots = snapshot_metrics();
    let mut output = String::new();
    output.push_str("# HELP mooncake_store_client_operation_total Total StoreClient operations.\n");
    output.push_str("# TYPE mooncake_store_client_operation_total counter\n");
    for snapshot in &snapshots {
        output.push_str(&format!(
            "mooncake_store_client_operation_total{{operation=\"{}\",status=\"{}\"}} {}\n",
            snapshot.operation, snapshot.status, snapshot.calls_total
        ));
    }

    output.push_str("# HELP mooncake_store_client_operation_bytes_in_total Total input bytes by StoreClient operation.\n");
    output.push_str("# TYPE mooncake_store_client_operation_bytes_in_total counter\n");
    for snapshot in &snapshots {
        output.push_str(&format!(
            "mooncake_store_client_operation_bytes_in_total{{operation=\"{}\",status=\"{}\"}} {}\n",
            snapshot.operation, snapshot.status, snapshot.bytes_in_total
        ));
    }

    output.push_str("# HELP mooncake_store_client_operation_bytes_out_total Total output bytes by StoreClient operation.\n");
    output.push_str("# TYPE mooncake_store_client_operation_bytes_out_total counter\n");
    for snapshot in &snapshots {
        output.push_str(&format!(
            "mooncake_store_client_operation_bytes_out_total{{operation=\"{}\",status=\"{}\"}} {}\n",
            snapshot.operation, snapshot.status, snapshot.bytes_out_total
        ));
    }

    output.push_str("# HELP mooncake_store_client_operation_latency_microseconds_total Total latency in microseconds by StoreClient operation.\n");
    output.push_str("# TYPE mooncake_store_client_operation_latency_microseconds_total counter\n");
    for snapshot in &snapshots {
        output.push_str(&format!(
            "mooncake_store_client_operation_latency_microseconds_total{{operation=\"{}\",status=\"{}\"}} {}\n",
            snapshot.operation, snapshot.status, snapshot.latency_total_us
        ));
    }

    output.push_str("# HELP mooncake_store_client_operation_latency_microseconds_max Maximum latency in microseconds by StoreClient operation.\n");
    output.push_str("# TYPE mooncake_store_client_operation_latency_microseconds_max gauge\n");
    for snapshot in &snapshots {
        output.push_str(&format!(
            "mooncake_store_client_operation_latency_microseconds_max{{operation=\"{}\",status=\"{}\"}} {}\n",
            snapshot.operation, snapshot.status, snapshot.latency_max_us
        ));
    }

    output
}

pub fn snapshot_metrics() -> Vec<OperationMetricSnapshot> {
    metrics_registry()
        .lock()
        .expect("metrics lock poisoned")
        .snapshot()
}

#[cfg(test)]
pub fn reset_metrics() {
    *metrics_registry().lock().expect("metrics lock poisoned") = MetricsRegistry::default();
}

fn metrics_registry() -> &'static Mutex<MetricsRegistry> {
    METRICS.get_or_init(|| Mutex::new(MetricsRegistry::default()))
}
