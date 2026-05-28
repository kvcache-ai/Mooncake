mod exporter;
mod metadata;
mod process;
mod profiling;
pub(crate) mod registry;

use is_terminal::IsTerminal;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mooncake_store_core::{Result, StoreError};
#[cfg(test)]
use parking_lot::ReentrantMutex;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::fmt::writer::MakeWriter;
use tracing_subscriber::{fmt, EnvFilter};

pub(crate) use metadata::observe_metadata_backend;
pub(crate) use profiling::{record_api_items, ApiItemTrace, ApiItemsTraceRecord, ProfilingSpan};
pub use registry::{MetricsSnapshot, OperationMetricSnapshot};

static TRACING_STATE: OnceLock<()> = OnceLock::new();
static METRICS_HTTP_SERVER: OnceLock<Mutex<Option<MetricsHttpServer>>> = OnceLock::new();
#[cfg(test)]
static TEST_PROCESS_LOCK: OnceLock<ReentrantMutex<()>> = OnceLock::new();

const HTTP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HTTP_READ_TIMEOUT: Duration = Duration::from_millis(250);
const TRACE_FILE_ENV: &str = "MC_STORE_RS_TRACE_FILE";
const TRACE_SPAN_EVENTS_ENV: &str = "MC_STORE_RS_TRACE_SPAN_EVENTS";

struct MetricsHttpServer {
    address: String,
    shutdown: Sender<()>,
    thread: JoinHandle<()>,
}

impl MetricsHttpServer {
    fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(());
        self.thread
            .join()
            .map_err(|_| StoreError::InvalidState("metrics http server panicked".to_string()))?;
        Ok(())
    }
}

pub struct OperationTracker {
    operation: &'static str,
    scope: &'static str,
    start: Instant,
    bytes_in: u64,
    registry: registry::SharedMetricsRegistry,
    profiling_span: ProfilingSpan,
    _inflight: InflightGuard,
}

impl OperationTracker {
    pub fn new(operation: &'static str) -> Self {
        let scope = default_scope(operation);
        Self {
            operation,
            scope,
            start: Instant::now(),
            bytes_in: 0,
            registry: registry::global_metrics_registry().clone(),
            profiling_span: {
                let mut span = ProfilingSpan::start(operation);
                span.set_str("mooncake.scope", scope);
                span
            },
            _inflight: InflightGuard::enter(operation, scope),
        }
    }

    #[cfg(test)]
    fn with_registry(operation: &'static str, registry: registry::SharedMetricsRegistry) -> Self {
        let scope = default_scope(operation);
        Self {
            operation,
            scope,
            start: Instant::now(),
            bytes_in: 0,
            registry: registry.clone(),
            profiling_span: {
                let mut span = ProfilingSpan::start(operation);
                span.set_str("mooncake.scope", scope);
                span
            },
            _inflight: InflightGuard::enter_with_registry(operation, scope, registry),
        }
    }

    pub fn scope(mut self, scope: &'static str) -> Self {
        if self.scope != scope {
            self._inflight =
                InflightGuard::enter_with_registry(self.operation, scope, self.registry.clone());
            self.profiling_span.set_str("mooncake.scope", scope);
        }
        self.scope = scope;
        self
    }

    pub fn input_bytes(mut self, bytes_in: u64) -> Self {
        self.bytes_in = bytes_in;
        self.profiling_span.set_u64("mooncake.bytes_in", bytes_in);
        self
    }

    pub fn attribute_str(mut self, key: &'static str, value: &str) -> Self {
        self.profiling_span.set_str(key, value);
        self
    }

    pub fn attribute_u64(mut self, key: &'static str, value: u64) -> Self {
        self.profiling_span.set_u64(key, value);
        self
    }

    pub(crate) fn trace_request_id(&self) -> Option<u64> {
        self.profiling_span.request_id()
    }

    pub fn finish<T>(self, result: &Result<T>, bytes_out: u64) {
        let result_label = result_label(result);
        self.finish_with_result(result_label, bytes_out);
    }

    pub(crate) fn finish_with_result(self, result: &'static str, bytes_out: u64) {
        registry::record_request_with_registry(
            &self.registry,
            self.operation,
            self.scope,
            result,
            self.bytes_in,
            bytes_out,
            self.start.elapsed(),
        );
        self.profiling_span.finish(result, bytes_out);
    }
}

pub struct InflightGuard {
    operation: &'static str,
    scope: &'static str,
    registry: registry::SharedMetricsRegistry,
}

#[derive(Clone)]
struct TraceFileMakeWriter {
    file: Arc<Mutex<File>>,
}

struct TraceFileWriter {
    file: Arc<Mutex<File>>,
}

impl<'a> MakeWriter<'a> for TraceFileMakeWriter {
    type Writer = TraceFileWriter;

    fn make_writer(&'a self) -> Self::Writer {
        TraceFileWriter {
            file: self.file.clone(),
        }
    }
}

impl Write for TraceFileWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.file
            .lock()
            .expect("trace file lock poisoned")
            .write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.lock().expect("trace file lock poisoned").flush()
    }
}

impl InflightGuard {
    pub fn enter(operation: &'static str, scope: &'static str) -> Self {
        Self::enter_with_registry(
            operation,
            scope,
            registry::global_metrics_registry().clone(),
        )
    }

    fn enter_with_registry(
        operation: &'static str,
        scope: &'static str,
        registry: registry::SharedMetricsRegistry,
    ) -> Self {
        registry::increment_inflight_with_registry(&registry, operation, scope);
        Self {
            operation,
            scope,
            registry,
        }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        registry::decrement_inflight_with_registry(&self.registry, self.operation, self.scope);
    }
}

pub fn init_tracing(filter: Option<&str>) -> Result<()> {
    if TRACING_STATE.get().is_some() {
        return Ok(());
    }

    let env_filter = match filter {
        Some(filter) => EnvFilter::try_new(filter).map_err(|error| {
            StoreError::InvalidState(format!("invalid tracing filter: {error}"))
        })?,
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
    };

    let trace_file = trace_file_from_env(TRACE_FILE_ENV)?;
    let span_events = trace_span_events_from_env(TRACE_SPAN_EVENTS_ENV)?;
    let use_ansi = trace_file.is_none() && std::io::stdout().is_terminal();

    let init_result = match trace_file {
        Some(trace_file) => {
            let writer = TraceFileMakeWriter {
                file: Arc::new(Mutex::new(open_trace_file(&trace_file)?)),
            };
            fmt()
                .with_env_filter(env_filter)
                .with_target(true)
                .with_thread_ids(true)
                .with_ansi(false)
                .with_span_events(span_events)
                .with_writer(writer)
                .try_init()
        }
        None => fmt()
            .with_env_filter(env_filter)
            .with_target(true)
            .with_thread_ids(true)
            .with_ansi(use_ansi)
            .with_span_events(span_events)
            .try_init(),
    };

    match init_result {
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
            Err(StoreError::InvalidState(format!(
                "tracing init failed: {error}"
            )))
        }
    }
}

pub fn init_tracing_from_env(toggle_env: &str, filter_env: &str) -> Result<bool> {
    let enabled = std::env::var(toggle_env)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    let has_trace_file = trace_file_from_env(TRACE_FILE_ENV)?.is_some();
    if !enabled && !has_trace_file {
        return Ok(false);
    }

    let filter = std::env::var(filter_env).ok();
    init_tracing(filter.as_deref())?;
    Ok(true)
}

fn trace_file_from_env(name: &str) -> Result<Option<PathBuf>> {
    let Some(path) = std::env::var_os(name) else {
        return Ok(None);
    };
    let path = PathBuf::from(path);
    if path.as_os_str().is_empty() {
        return Ok(None);
    }
    Ok(Some(path))
}

fn trace_span_events_from_env(name: &str) -> Result<FmtSpan> {
    let Some(value) = std::env::var_os(name) else {
        return Ok(FmtSpan::NONE);
    };
    let value = value.to_string_lossy();
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "none" | "off" | "0" | "false" | "no" => Ok(FmtSpan::NONE),
        "close" | "1" | "true" | "yes" | "on" => Ok(FmtSpan::CLOSE),
        other => Err(StoreError::InvalidState(format!(
            "invalid tracing span events value {other:?}; expected close or none"
        ))),
    }
}

fn open_trace_file(path: &Path) -> Result<File> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|error| {
            StoreError::InvalidState(format!(
                "failed to create trace log directory {}: {error}",
                parent.display()
            ))
        })?;
    }
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| {
            StoreError::InvalidState(format!(
                "failed to open trace log file {}: {error}",
                path.display()
            ))
        })
}

pub fn start_metrics_http_server(bind_addr: &str) -> Result<String> {
    let mut server = metrics_http_server()
        .lock()
        .expect("metrics http server lock poisoned");
    if let Some(server) = server.as_ref() {
        return Ok(server.address.clone());
    }

    let server_instance = spawn_metrics_http_server_for_registry(
        bind_addr,
        registry::global_metrics_registry().clone(),
    )?;
    let address = server_instance.address.clone();
    *server = Some(server_instance);
    Ok(address)
}

fn spawn_metrics_http_server_for_registry(
    bind_addr: &str,
    registry: registry::SharedMetricsRegistry,
) -> Result<MetricsHttpServer> {
    let listener = TcpListener::bind(bind_addr).map_err(|error| {
        StoreError::InvalidState(format!(
            "metrics http server failed to bind {bind_addr}: {error}"
        ))
    })?;
    listener.set_nonblocking(true).map_err(|error| {
        StoreError::InvalidState(format!(
            "metrics http server failed to enable nonblocking mode: {error}"
        ))
    })?;
    let address = listener
        .local_addr()
        .map_err(|error| {
            StoreError::InvalidState(format!(
                "metrics http server failed to read local address: {error}"
            ))
        })?
        .to_string();
    let (shutdown, shutdown_rx) = mpsc::channel();
    let thread = thread::Builder::new()
        .name("mooncake-store-metrics".to_string())
        .spawn(move || run_metrics_http_server(listener, shutdown_rx, registry))
        .map_err(|error| {
            StoreError::InvalidState(format!(
                "metrics http server failed to spawn worker thread: {error}"
            ))
        })?;
    Ok(MetricsHttpServer {
        address: address.clone(),
        shutdown,
        thread,
    })
}

pub fn start_metrics_http_server_from_env(addr_env: &str) -> Result<Option<String>> {
    let Ok(address) = std::env::var(addr_env) else {
        return Ok(None);
    };
    if address.trim().is_empty() {
        return Ok(None);
    }
    start_metrics_http_server(&address).map(Some)
}

pub fn metrics_http_server_addr() -> Option<String> {
    metrics_http_server()
        .lock()
        .expect("metrics http server lock poisoned")
        .as_ref()
        .map(|server| server.address.clone())
}

pub fn stop_metrics_http_server() -> Result<()> {
    let server = metrics_http_server()
        .lock()
        .expect("metrics http server lock poisoned")
        .take();
    if let Some(server) = server {
        server.shutdown()?;
    }
    Ok(())
}

pub fn render_prometheus_metrics() -> String {
    render_prometheus_metrics_with_registry(registry::global_metrics_registry())
}

pub fn render_stats_json() -> String {
    render_stats_json_with_registry(registry::global_metrics_registry())
}

pub fn render_breakdown_json() -> String {
    render_breakdown_json_with_registry(registry::global_metrics_registry())
}

fn render_prometheus_metrics_with_registry(registry: &registry::SharedMetricsRegistry) -> String {
    exporter::render_prometheus_metrics(&snapshot_metrics_with_registry(registry))
}

fn render_stats_json_with_registry(registry: &registry::SharedMetricsRegistry) -> String {
    let snapshot = snapshot_metrics_with_registry(registry);
    serde_json::to_string(&StatsSnapshot::from_metrics(snapshot))
        .expect("stats snapshot serialization should succeed")
}

fn render_breakdown_json_with_registry(registry: &registry::SharedMetricsRegistry) -> String {
    let snapshot = snapshot_metrics_with_registry(registry);
    serde_json::to_string(&BreakdownSnapshot::from_metrics(snapshot))
        .expect("breakdown snapshot serialization should succeed")
}

pub fn snapshot_metrics() -> MetricsSnapshot {
    snapshot_metrics_with_registry(registry::global_metrics_registry())
}

fn snapshot_metrics_with_registry(registry: &registry::SharedMetricsRegistry) -> MetricsSnapshot {
    registry::snapshot_metrics_with_registry(registry, process::snapshot_process())
}

pub fn record_heartbeat_health(runtime: &str, consecutive_failures: u64, last_success_ms: u64) {
    registry::record_heartbeat_health(runtime, consecutive_failures, last_success_ms);
}

pub fn record_tenant_quota_reconcile(result: &'static str) {
    registry::record_tenant_quota_reconcile(result);
}

pub fn record_tenant_local_eviction(result: &'static str) {
    registry::record_tenant_local_eviction(result);
}

#[cfg(test)]
pub fn reset_metrics() {
    registry::reset_metrics();
}

#[cfg(test)]
pub(crate) fn test_process_lock() -> &'static ReentrantMutex<()> {
    TEST_PROCESS_LOCK.get_or_init(|| ReentrantMutex::new(()))
}

#[cfg(test)]
pub fn metrics_test_lock() -> &'static ReentrantMutex<()> {
    test_process_lock()
}

fn metrics_http_server() -> &'static Mutex<Option<MetricsHttpServer>> {
    METRICS_HTTP_SERVER.get_or_init(|| Mutex::new(None))
}

fn run_metrics_http_server(
    listener: TcpListener,
    shutdown_rx: Receiver<()>,
    registry: registry::SharedMetricsRegistry,
) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => handle_metrics_http_connection(stream, &registry),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if shutdown_rx.recv_timeout(HTTP_POLL_INTERVAL).is_ok() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

fn handle_metrics_http_connection(
    mut stream: TcpStream,
    registry: &registry::SharedMetricsRegistry,
) {
    if stream.set_read_timeout(Some(HTTP_READ_TIMEOUT)).is_err() {
        return;
    }
    let path = match read_http_path(&mut stream) {
        Ok(path) => path,
        Err(_) => return,
    };
    let (status, content_type, body) = match path.as_str() {
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            render_prometheus_metrics_with_registry(registry),
        ),
        "/stats" => (
            "200 OK",
            "application/json; charset=utf-8",
            render_stats_json_with_registry(registry),
        ),
        "/breakdown" => (
            "200 OK",
            "application/json; charset=utf-8",
            render_breakdown_json_with_registry(registry),
        ),
        "/healthz" | "/livez" => ("200 OK", "text/plain; charset=utf-8", "ok\n".to_string()),
        path if path.starts_with("/tracing") || path.starts_with("/trace") => {
            match profiling::handle_tracing_http_path(path) {
                Ok(body) => ("200 OK", "application/json; charset=utf-8", body),
                Err(error) => (
                    "400 Bad Request",
                    "text/plain; charset=utf-8",
                    format!("{error}\n"),
                ),
            }
        }
        _ => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        ),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

fn read_http_path(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut request = Vec::with_capacity(1024);
    let mut buffer = [0_u8; 512];
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") || request.len() >= 8192 {
            break;
        }
    }
    let request = String::from_utf8_lossy(&request);
    let first_line = request.lines().next().unwrap_or_default();
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or("/");
    if method != "GET" {
        return Ok("/".to_string());
    }
    Ok(path.to_string())
}

fn default_scope(operation: &'static str) -> &'static str {
    if operation.starts_with("control_") {
        return "control_plane";
    }
    if operation.contains("background")
        || operation.contains("evict")
        || operation.contains("rebuild")
    {
        return "background";
    }
    "foreground"
}

fn result_label<T>(result: &Result<T>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(StoreError::Conflict(_) | StoreError::QuotaExceeded { .. }) => "conflict",
        Err(_) => "error",
    }
}

#[derive(Serialize)]
struct StatsSnapshot {
    process: StatsProcessSnapshot,
    operations: Vec<StatsOperationSnapshot>,
    runtimes: Vec<StatsRuntimeSnapshot>,
    segments: Vec<StatsSegmentSnapshot>,
}

impl StatsSnapshot {
    fn from_metrics(snapshot: MetricsSnapshot) -> Self {
        let mut runtimes = std::collections::BTreeMap::<String, StatsRuntimeSnapshot>::new();
        for sample in snapshot.runtime_status {
            let runtime = sample.key.runtime;
            let entry = runtimes
                .entry(runtime.clone())
                .or_insert_with(|| StatsRuntimeSnapshot::new(runtime));
            if sample.value >= 0.5 {
                entry.state = Some(sample.key.state.to_string());
            }
        }
        for sample in snapshot.runtime_lease_expires_at_ms {
            let runtime = sample.key.runtime;
            let entry = runtimes
                .entry(runtime.clone())
                .or_insert_with(|| StatsRuntimeSnapshot::new(runtime));
            entry.lease_expires_at_ms = Some(sample.value.max(0.0) as u64);
        }
        for sample in snapshot.heartbeat_consecutive_failures {
            let runtime = sample.key.runtime;
            let entry = runtimes
                .entry(runtime.clone())
                .or_insert_with(|| StatsRuntimeSnapshot::new(runtime));
            entry.heartbeat_consecutive_failures = Some(sample.value.max(0.0) as u64);
        }
        for sample in snapshot.heartbeat_last_success_ms {
            let runtime = sample.key.runtime;
            let entry = runtimes
                .entry(runtime.clone())
                .or_insert_with(|| StatsRuntimeSnapshot::new(runtime));
            entry.heartbeat_last_success_ms = Some(sample.value.max(0.0) as u64);
        }

        Self {
            process: StatsProcessSnapshot {
                cpu_seconds_total: snapshot.process.cpu_seconds_total,
                resident_memory_bytes: snapshot.process.resident_memory_bytes,
                open_fds: snapshot.process.open_fds,
            },
            operations: snapshot
                .operations
                .into_iter()
                .map(|operation| StatsOperationSnapshot {
                    operation: operation.operation,
                    status: operation.status,
                    calls_total: operation.calls_total,
                    bytes_in_total: operation.bytes_in_total,
                    bytes_out_total: operation.bytes_out_total,
                    latency_total_us: operation.latency_total_us,
                    latency_max_us: operation.latency_max_us,
                })
                .collect(),
            runtimes: runtimes.into_values().collect(),
            segments: snapshot
                .segments
                .into_iter()
                .map(|segment| StatsSegmentSnapshot {
                    runtime: segment.runtime,
                    segment: segment.segment,
                    state: segment.state,
                    tier: segment.tier,
                    capacity_bytes: segment.capacity_bytes,
                    used_bytes: segment.used_bytes,
                })
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct StatsProcessSnapshot {
    cpu_seconds_total: f64,
    resident_memory_bytes: u64,
    open_fds: Option<u64>,
}

#[derive(Serialize)]
struct StatsOperationSnapshot {
    operation: &'static str,
    status: &'static str,
    calls_total: u64,
    bytes_in_total: u64,
    bytes_out_total: u64,
    latency_total_us: u64,
    latency_max_us: u64,
}

#[derive(Serialize)]
struct StatsRuntimeSnapshot {
    runtime: String,
    state: Option<String>,
    lease_expires_at_ms: Option<u64>,
    heartbeat_consecutive_failures: Option<u64>,
    heartbeat_last_success_ms: Option<u64>,
}

impl StatsRuntimeSnapshot {
    fn new(runtime: String) -> Self {
        Self {
            runtime,
            state: None,
            lease_expires_at_ms: None,
            heartbeat_consecutive_failures: None,
            heartbeat_last_success_ms: None,
        }
    }
}

#[derive(Serialize)]
struct StatsSegmentSnapshot {
    runtime: String,
    segment: String,
    state: &'static str,
    tier: &'static str,
    capacity_bytes: u64,
    used_bytes: u64,
}

#[derive(Serialize)]
struct BreakdownSnapshot {
    tenant: String,
    process: StatsProcessSnapshot,
    operations: Vec<BreakdownOperationSnapshot>,
    metadata_operations: Vec<BreakdownMetadataSnapshot>,
    transport: Vec<BreakdownTransportSnapshot>,
    runtimes: Vec<StatsRuntimeSnapshot>,
    segments: BreakdownSegmentSummary,
    bottlenecks: Vec<BreakdownBottleneckSnapshot>,
    note: &'static str,
}

#[derive(Serialize)]
struct BreakdownOperationSnapshot {
    operation: String,
    kind: &'static str,
    scope: &'static str,
    result: &'static str,
    calls_total: u64,
    bytes_in_total: u64,
    bytes_out_total: u64,
    latency_total_us: u64,
    latency_avg_us: u64,
    latency_max_us: u64,
    latency_p50_us: u64,
    latency_p90_us: u64,
    latency_p99_us: u64,
    inflight: u64,
}

#[derive(Serialize)]
struct BreakdownMetadataSnapshot {
    backend: &'static str,
    operation: &'static str,
    result: &'static str,
    calls_total: u64,
    latency_total_us: u64,
    latency_avg_us: u64,
    latency_p50_us: u64,
    latency_p90_us: u64,
    latency_p99_us: u64,
    inflight: u64,
}

#[derive(Serialize)]
struct BreakdownTransportSnapshot {
    direction: &'static str,
    peer_kind: &'static str,
    result: Option<&'static str>,
    operations_total: u64,
    bytes_total: u64,
}

#[derive(Serialize)]
struct BreakdownSegmentSummary {
    count: usize,
    capacity_bytes: u64,
    used_bytes: u64,
    by_runtime: Vec<StatsSegmentSnapshot>,
}

#[derive(Serialize)]
struct BreakdownBottleneckSnapshot {
    source: &'static str,
    name: String,
    result: &'static str,
    calls_total: u64,
    latency_total_us: u64,
    latency_p99_us: u64,
    reason: &'static str,
}

impl BreakdownSnapshot {
    fn from_metrics(snapshot: MetricsSnapshot) -> Self {
        let process = StatsProcessSnapshot {
            cpu_seconds_total: snapshot.process.cpu_seconds_total,
            resident_memory_bytes: snapshot.process.resident_memory_bytes,
            open_fds: snapshot.process.open_fds,
        };
        let runtimes = stats_runtimes(
            snapshot.runtime_status,
            snapshot.runtime_lease_expires_at_ms,
            snapshot.heartbeat_consecutive_failures,
            snapshot.heartbeat_last_success_ms,
        );
        let segment_rows = snapshot
            .segments
            .into_iter()
            .map(|segment| StatsSegmentSnapshot {
                runtime: segment.runtime,
                segment: segment.segment,
                state: segment.state,
                tier: segment.tier,
                capacity_bytes: segment.capacity_bytes,
                used_bytes: segment.used_bytes,
            })
            .collect::<Vec<_>>();
        let segment_count = segment_rows.len();
        let segment_capacity = segment_rows
            .iter()
            .map(|segment| segment.capacity_bytes)
            .sum();
        let segment_used = segment_rows.iter().map(|segment| segment.used_bytes).sum();
        let operations = build_breakdown_operations(
            snapshot.operations,
            snapshot.request_duration,
            snapshot.request_bytes,
            snapshot.request_inflight,
        );
        let metadata_operations = build_breakdown_metadata(
            snapshot.metadata_operations,
            snapshot.metadata_duration,
            snapshot.metadata_inflight,
        );
        let transport =
            build_breakdown_transport(snapshot.transport_operations, snapshot.transport_bytes);
        let bottlenecks = build_bottlenecks(&operations, &metadata_operations);
        Self {
            tenant: snapshot.tenant,
            process,
            operations,
            metadata_operations,
            transport,
            runtimes,
            segments: BreakdownSegmentSummary {
                count: segment_count,
                capacity_bytes: segment_capacity,
                used_bytes: segment_used,
                by_runtime: segment_rows,
            },
            bottlenecks,
            note: "Bottleneck candidates are ranked observations from Store-RS metrics; they are not root-cause proof.",
        }
    }
}

fn stats_runtimes(
    runtime_status: Vec<registry::GaugeSample<registry::RuntimeStatusKey>>,
    runtime_lease_expires_at_ms: Vec<registry::GaugeSample<registry::RuntimeKey>>,
    heartbeat_consecutive_failures: Vec<registry::GaugeSample<registry::RuntimeKey>>,
    heartbeat_last_success_ms: Vec<registry::GaugeSample<registry::RuntimeKey>>,
) -> Vec<StatsRuntimeSnapshot> {
    let mut runtimes = std::collections::BTreeMap::<String, StatsRuntimeSnapshot>::new();
    for sample in runtime_status {
        let runtime = sample.key.runtime;
        let entry = runtimes
            .entry(runtime.clone())
            .or_insert_with(|| StatsRuntimeSnapshot::new(runtime));
        if sample.value >= 0.5 {
            entry.state = Some(sample.key.state.to_string());
        }
    }
    for sample in runtime_lease_expires_at_ms {
        let runtime = sample.key.runtime;
        let entry = runtimes
            .entry(runtime.clone())
            .or_insert_with(|| StatsRuntimeSnapshot::new(runtime));
        entry.lease_expires_at_ms = Some(sample.value.max(0.0) as u64);
    }
    for sample in heartbeat_consecutive_failures {
        let runtime = sample.key.runtime;
        let entry = runtimes
            .entry(runtime.clone())
            .or_insert_with(|| StatsRuntimeSnapshot::new(runtime));
        entry.heartbeat_consecutive_failures = Some(sample.value.max(0.0) as u64);
    }
    for sample in heartbeat_last_success_ms {
        let runtime = sample.key.runtime;
        let entry = runtimes
            .entry(runtime.clone())
            .or_insert_with(|| StatsRuntimeSnapshot::new(runtime));
        entry.heartbeat_last_success_ms = Some(sample.value.max(0.0) as u64);
    }
    runtimes.into_values().collect()
}

fn build_breakdown_operations(
    operations: Vec<OperationMetricSnapshot>,
    durations: Vec<registry::HistogramSample<registry::RequestKey>>,
    bytes: Vec<registry::CounterSample<registry::RequestBytesKey>>,
    inflight: Vec<registry::GaugeSample<registry::RequestInflightKey>>,
) -> Vec<BreakdownOperationSnapshot> {
    let mut by_key = std::collections::BTreeMap::<
        (&'static str, &'static str, &'static str),
        BreakdownOperationSnapshot,
    >::new();
    let mut bytes_by_key =
        std::collections::BTreeMap::<(&'static str, &'static str, &'static str), (u64, u64)>::new();
    for sample in bytes {
        let entry = bytes_by_key
            .entry((sample.key.operation, sample.key.scope, "ok"))
            .or_default();
        match sample.key.direction {
            "in" => entry.0 = entry.0.saturating_add(sample.value),
            "out" => entry.1 = entry.1.saturating_add(sample.value),
            _ => {}
        }
    }
    let mut inflight_by_key =
        std::collections::BTreeMap::<(&'static str, &'static str), u64>::new();
    for sample in inflight {
        inflight_by_key.insert(
            (sample.key.operation, sample.key.scope),
            sample.value.max(0.0) as u64,
        );
    }
    for sample in durations {
        let (bytes_in, bytes_out) = bytes_by_key
            .get(&(sample.key.operation, sample.key.scope, sample.key.result))
            .copied()
            .unwrap_or_default();
        let calls_total = sample.count;
        let total_us = seconds_to_us(sample.sum);
        by_key.insert(
            (sample.key.operation, sample.key.scope, sample.key.result),
            BreakdownOperationSnapshot {
                operation: sample.key.operation.to_string(),
                kind: operation_kind(sample.key.operation),
                scope: sample.key.scope,
                result: sample.key.result,
                calls_total,
                bytes_in_total: bytes_in,
                bytes_out_total: bytes_out,
                latency_total_us: total_us,
                latency_avg_us: avg_us(total_us, calls_total),
                latency_max_us: 0,
                latency_p50_us: histogram_quantile_us(&sample.buckets, sample.count, 0.50),
                latency_p90_us: histogram_quantile_us(&sample.buckets, sample.count, 0.90),
                latency_p99_us: histogram_quantile_us(&sample.buckets, sample.count, 0.99),
                inflight: inflight_by_key
                    .get(&(sample.key.operation, sample.key.scope))
                    .copied()
                    .unwrap_or_default(),
            },
        );
    }
    for operation in operations {
        for ((name, _, result), row) in &mut by_key {
            if *name == operation.operation && *result == operation.status {
                row.bytes_in_total = row.bytes_in_total.max(operation.bytes_in_total);
                row.bytes_out_total = row.bytes_out_total.max(operation.bytes_out_total);
                row.latency_max_us = row.latency_max_us.max(operation.latency_max_us);
            }
        }
    }
    by_key.into_values().collect()
}

fn build_breakdown_metadata(
    operations: Vec<registry::CounterSample<registry::MetadataOperationKey>>,
    durations: Vec<registry::HistogramSample<registry::MetadataOperationKey>>,
    inflight: Vec<registry::GaugeSample<registry::MetadataInflightKey>>,
) -> Vec<BreakdownMetadataSnapshot> {
    let mut calls =
        std::collections::BTreeMap::<(&'static str, &'static str, &'static str), u64>::new();
    for sample in operations {
        calls.insert(
            (sample.key.backend, sample.key.operation, sample.key.result),
            sample.value,
        );
    }
    let mut inflight_by_key =
        std::collections::BTreeMap::<(&'static str, &'static str), u64>::new();
    for sample in inflight {
        inflight_by_key.insert(
            (sample.key.backend, sample.key.operation),
            sample.value.max(0.0) as u64,
        );
    }
    durations
        .into_iter()
        .map(|sample| {
            let total_us = seconds_to_us(sample.sum);
            let calls_total = calls
                .get(&(sample.key.backend, sample.key.operation, sample.key.result))
                .copied()
                .unwrap_or(sample.count);
            BreakdownMetadataSnapshot {
                backend: sample.key.backend,
                operation: sample.key.operation,
                result: sample.key.result,
                calls_total,
                latency_total_us: total_us,
                latency_avg_us: avg_us(total_us, calls_total),
                latency_p50_us: histogram_quantile_us(&sample.buckets, sample.count, 0.50),
                latency_p90_us: histogram_quantile_us(&sample.buckets, sample.count, 0.90),
                latency_p99_us: histogram_quantile_us(&sample.buckets, sample.count, 0.99),
                inflight: inflight_by_key
                    .get(&(sample.key.backend, sample.key.operation))
                    .copied()
                    .unwrap_or_default(),
            }
        })
        .collect()
}

fn build_breakdown_transport(
    operations: Vec<registry::CounterSample<registry::TransportOperationKey>>,
    bytes: Vec<registry::CounterSample<registry::TransportBytesKey>>,
) -> Vec<BreakdownTransportSnapshot> {
    let mut rows = Vec::new();
    for sample in operations {
        rows.push(BreakdownTransportSnapshot {
            direction: sample.key.direction,
            peer_kind: sample.key.peer_kind,
            result: Some(sample.key.result),
            operations_total: sample.value,
            bytes_total: 0,
        });
    }
    for sample in bytes {
        rows.push(BreakdownTransportSnapshot {
            direction: sample.key.direction,
            peer_kind: sample.key.peer_kind,
            result: None,
            operations_total: 0,
            bytes_total: sample.value,
        });
    }
    rows
}

fn build_bottlenecks(
    operations: &[BreakdownOperationSnapshot],
    metadata: &[BreakdownMetadataSnapshot],
) -> Vec<BreakdownBottleneckSnapshot> {
    let mut rows = operations
        .iter()
        .filter(|row| row.calls_total > 0)
        .map(|row| BreakdownBottleneckSnapshot {
            source: row.kind,
            name: row.operation.clone(),
            result: row.result,
            calls_total: row.calls_total,
            latency_total_us: row.latency_total_us,
            latency_p99_us: row.latency_p99_us,
            reason: "ranked by observed total latency and p99 latency; correlation only",
        })
        .chain(
            metadata
                .iter()
                .filter(|row| row.calls_total > 0)
                .map(|row| BreakdownBottleneckSnapshot {
                    source: "metadata",
                    name: format!("{}:{}", row.backend, row.operation),
                    result: row.result,
                    calls_total: row.calls_total,
                    latency_total_us: row.latency_total_us,
                    latency_p99_us: row.latency_p99_us,
                    reason: "ranked by observed total latency and p99 latency; correlation only",
                }),
        )
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .latency_total_us
            .cmp(&left.latency_total_us)
            .then_with(|| right.latency_p99_us.cmp(&left.latency_p99_us))
    });
    rows.truncate(10);
    rows
}

fn operation_kind(operation: &str) -> &'static str {
    match operation {
        "batch_put_from" | "batch_get_into" | "batch_is_exist" | "batch_is_readable"
        | "get_size" | "query_route" | "register_buffer" | "unregister_buffer" | "put"
        | "put_from" | "batch_put" | "batch_get" | "get" | "get_into" => "api",
        op if op.contains("stage")
            || op.starts_with("route_lookup")
            || op.starts_with("readable_replica_select")
            || op.starts_with("get_remote")
            || op.starts_with("put_remote")
            || op.contains("local_copy")
            || op.contains("checksum")
            || op.starts_with("compat_dispatcher")
            || op.starts_with("py_") =>
        {
            "phase"
        }
        op if op.starts_with("control_") => "control",
        _ => "internal",
    }
}

fn histogram_quantile_us(buckets: &[u64], count: u64, quantile: f64) -> u64 {
    if count == 0 {
        return 0;
    }
    let target = ((count as f64) * quantile).ceil().max(1.0) as u64;
    let mut fallback = 0;
    for (bucket, upper_bound) in buckets.iter().zip(registry::REQUEST_DURATION_BUCKETS) {
        fallback = seconds_to_us(*upper_bound);
        if *bucket >= target {
            return seconds_to_us(*upper_bound);
        }
    }
    fallback
}

fn seconds_to_us(value: f64) -> u64 {
    (value.max(0.0) * 1_000_000.0).round() as u64
}

fn avg_us(total_us: u64, count: u64) -> u64 {
    if count == 0 {
        0
    } else {
        total_us / count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mooncake_metadata::InMemoryMetadataBackend;
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        CompatibilityDescriptor, MetadataBackend, StoreError,
    };
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    #[test]
    fn metrics_http_server_serves_prometheus_text() {
        let _guard = metrics_test_lock().lock();
        let registry = registry::new_metrics_registry();

        let result = Ok(());
        OperationTracker::with_registry("put", registry.clone())
            .input_bytes(16)
            .finish(&result, 16);

        let server = spawn_metrics_http_server_for_registry("127.0.0.1:0", registry)
            .expect("metrics server should start on an ephemeral port");
        let response = http_get(&server.address, "/metrics");

        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("mooncake_store_operation_total"));
        assert!(response.contains("operation=\"put\",status=\"ok\""));
        server
            .shutdown()
            .expect("metrics server should stop cleanly");
    }

    #[test]
    fn request_metrics_include_histogram_bytes_and_inflight() {
        let _guard = metrics_test_lock().lock();
        let registry = registry::new_metrics_registry();

        let result: Result<()> = Ok(());
        {
            let tracker = OperationTracker::with_registry("put", registry.clone())
                .scope("foreground")
                .input_bytes(16);
            let active = render_prometheus_metrics_with_registry(&registry);
            assert!(active.contains(
                "mooncake_store_request_inflight{tenant=\"default\",operation=\"put\",scope=\"foreground\"} 1"
            ));
            tracker.finish(&result, 8);

            let finished = render_prometheus_metrics_with_registry(&registry);
            assert!(finished.contains(
                "mooncake_store_request_total{tenant=\"default\",operation=\"put\",scope=\"foreground\",result=\"ok\"} 1"
            ));
            assert!(finished.contains(
                "mooncake_store_request_duration_seconds_bucket{tenant=\"default\",operation=\"put\",scope=\"foreground\",result=\"ok\",le=\"+Inf\"} 1"
            ));
            assert!(finished.contains(
                "mooncake_store_request_bytes_total{tenant=\"default\",operation=\"put\",direction=\"in\",scope=\"foreground\"} 16"
            ));
            assert!(finished.contains(
                "mooncake_store_request_bytes_total{tenant=\"default\",operation=\"put\",direction=\"out\",scope=\"foreground\"} 8"
            ));
        }

        let idle = render_prometheus_metrics_with_registry(&registry);
        assert!(idle.contains(
            "mooncake_store_request_inflight{tenant=\"default\",operation=\"put\",scope=\"foreground\"} 0"
        ));
    }

    #[test]
    fn prometheus_metrics_include_process_tenant_label() {
        let _guard = metrics_test_lock().lock();
        let registry = registry::new_metrics_registry();
        registry::set_process_tenant_with_registry(&registry, "tenant-a");

        let result: Result<()> = Ok(());
        OperationTracker::with_registry("put", registry.clone()).finish(&result, 0);

        let metrics = render_prometheus_metrics_with_registry(&registry);
        assert!(metrics.contains(
            "mooncake_store_operation_total{tenant=\"tenant-a\",operation=\"put\",status=\"ok\"} 1"
        ));
        assert!(metrics.contains(
            "mooncake_store_request_total{tenant=\"tenant-a\",operation=\"put\",scope=\"foreground\",result=\"ok\"} 1"
        ));
        assert!(metrics.contains("process_cpu_seconds_total{tenant=\"tenant-a\"}"));
    }

    #[test]
    fn reset_metrics_restores_default_process_tenant_label() {
        let _guard = metrics_test_lock().lock();
        reset_metrics();
        registry::set_process_tenant("tenant-a");

        let result: Result<()> = Ok(());
        OperationTracker::new("put").finish(&result, 0);
        let metrics = render_prometheus_metrics();
        assert!(metrics.contains(
            "mooncake_store_request_total{tenant=\"tenant-a\",operation=\"put\",scope=\"foreground\",result=\"ok\"} 1"
        ));

        reset_metrics();
        OperationTracker::new("put").finish(&result, 0);
        let metrics = render_prometheus_metrics();
        assert!(metrics.contains(
            "mooncake_store_request_total{tenant=\"default\",operation=\"put\",scope=\"foreground\",result=\"ok\"} 1"
        ));
        assert!(!metrics.contains(
            "mooncake_store_request_total{tenant=\"tenant-a\",operation=\"put\",scope=\"foreground\",result=\"ok\"}"
        ));
        assert!(!metrics.contains("process_cpu_seconds_total{tenant=\"tenant-a\"}"));
    }

    #[test]
    fn metadata_backend_metrics_include_backend_operation_result_and_inflight() {
        let _guard = metrics_test_lock().lock();
        let registry = registry::new_metrics_registry();
        let metadata: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        let metadata = metadata::observe_metadata_backend_with_registry(metadata, registry.clone());
        let metadata = metadata::observe_metadata_backend_with_registry(metadata, registry.clone());
        let lease = ClientLease {
            runtime: ClientRuntimeId::new("metadata-observed", ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: u64::MAX,
        };

        metadata
            .upsert_client_lease(&lease)
            .expect("lease publish should succeed");
        let fetched = metadata
            .get_client_lease(&lease.runtime)
            .expect("lease lookup should succeed");

        assert!(fetched.is_some());
        let metrics = render_prometheus_metrics_with_registry(&registry);
        assert!(metrics.contains(
            "mooncake_store_metadata_operation_total{tenant=\"default\",backend=\"in_memory\",operation=\"upsert_client_lease\",result=\"ok\"} 1"
        ));
        assert!(metrics.contains(
            "mooncake_store_metadata_operation_duration_seconds_bucket{tenant=\"default\",backend=\"in_memory\",operation=\"upsert_client_lease\",result=\"ok\",le=\"+Inf\"} 1"
        ));
        assert!(metrics.contains(
            "mooncake_store_metadata_operation_inflight{tenant=\"default\",backend=\"in_memory\",operation=\"upsert_client_lease\"} 0"
        ));
    }

    #[test]
    fn metadata_result_label_keeps_backend_errors_actionable() {
        let metadata_error: Result<()> =
            Err(StoreError::Metadata("redis connection lost".to_string()));
        let transport_error: Result<()> =
            Err(StoreError::Transport("tcp connect timeout".to_string()));

        assert_eq!(
            metadata::metadata_result_label(&metadata_error),
            "metadata_error"
        );
        assert_eq!(
            metadata::metadata_result_label(&transport_error),
            "transport_error"
        );
    }

    #[test]
    fn consistency_and_transport_counters_are_rendered() {
        let _guard = metrics_test_lock().lock();
        registry::reset_metrics();

        registry::record_replication_publish("error", std::time::Duration::from_millis(7));
        registry::record_transport_operation("write", "storage", "error");
        registry::record_reclaim_release("flush_due_reclaims", "skipped_unavailable_runtime");

        let metrics = render_prometheus_metrics();
        assert!(metrics.contains(
            "mooncake_store_replication_publish_total{tenant=\"default\",result=\"error\"} 1"
        ));
        assert!(metrics.contains(
            "mooncake_store_transport_operation_total{tenant=\"default\",direction=\"write\",peer_kind=\"storage\",result=\"error\"} 1"
        ));
        assert!(metrics.contains(
            "mooncake_store_reclaim_release_total{tenant=\"default\",action=\"flush_due_reclaims\",result=\"skipped_unavailable_runtime\"} 1"
        ));
    }

    #[test]
    fn sparse_operational_counters_render_zero_baselines() {
        let _guard = metrics_test_lock().lock();
        registry::reset_metrics();

        let metrics = render_prometheus_metrics();
        assert!(metrics.contains(
            "mooncake_store_tenant_quota_reservation_total{tenant=\"default\",result=\"ok\"} 0"
        ));
        assert!(metrics.contains(
            "mooncake_store_tenant_quota_finalize_total{tenant=\"default\",result=\"conflict\"} 0"
        ));
        assert!(metrics.contains(
            "mooncake_store_tenant_quota_abort_total{tenant=\"default\",result=\"error\"} 0"
        ));
        assert!(metrics.contains(
            "mooncake_store_tenant_quota_reconcile_total{tenant=\"default\",result=\"aborted\"} 0"
        ));
        assert!(metrics.contains(
            "mooncake_store_tenant_local_eviction_total{tenant=\"default\",result=\"miss\"} 0"
        ));
        assert!(metrics.contains(
            "mooncake_store_preferred_segment_skip_total{tenant=\"default\",source=\"tenant_policy\",reason=\"not_found\"} 0"
        ));
        assert!(metrics.contains(
            "mooncake_store_rebalance_routes_total{tenant=\"default\",phase=\"migrate\",result=\"ok\"} 0"
        ));
        assert!(metrics.contains(
            "mooncake_store_rebalance_bytes_total{tenant=\"default\",phase=\"migrate\"} 0"
        ));
        assert!(metrics.contains(
            "mooncake_store_segment_lifecycle_total{tenant=\"default\",action=\"mount_segment\",result=\"ok\"} 0"
        ));
    }

    #[test]
    fn process_metrics_are_rendered_with_request_snapshot() {
        let _guard = metrics_test_lock().lock();
        let registry = registry::new_metrics_registry();

        let metrics = render_prometheus_metrics_with_registry(&registry);

        assert!(metrics.contains("# HELP process_cpu_seconds_total"));
        assert!(metrics.contains("# TYPE process_resident_memory_bytes gauge"));
    }

    #[test]
    fn metrics_http_server_serves_health_probe() {
        let _guard = metrics_test_lock().lock();
        let server =
            spawn_metrics_http_server_for_registry("127.0.0.1:0", registry::new_metrics_registry())
                .expect("metrics server should start on an ephemeral port");
        let response = http_get(&server.address, "/healthz");
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.ends_with("ok\n"));
        server
            .shutdown()
            .expect("metrics server should stop cleanly");
    }

    #[test]
    fn metrics_http_server_serves_stats_json() {
        let _guard = metrics_test_lock().lock();
        let registry = registry::new_metrics_registry();

        let result = Ok(());
        OperationTracker::with_registry("storage_owner_background_eviction", registry.clone())
            .input_bytes(32)
            .finish(&result, 64);
        let runtime = sample_runtime_lease("runtime-stats");
        registry::record_runtime_leases_with_registry(&registry, std::slice::from_ref(&runtime));
        registry::record_heartbeat_health_with_registry(
            &registry,
            &runtime.runtime.to_string(),
            2,
            456_789,
        );

        let server = spawn_metrics_http_server_for_registry("127.0.0.1:0", registry)
            .expect("metrics server should start on an ephemeral port");
        let response = http_get(&server.address, "/stats");

        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("Content-Type: application/json; charset=utf-8"));
        let body = response
            .split("\r\n\r\n")
            .nth(1)
            .expect("http response should contain a body");
        let value: serde_json::Value =
            serde_json::from_str(body).expect("stats endpoint should return valid json");
        let operations = value["operations"]
            .as_array()
            .expect("stats json should expose operations as an array");
        assert!(
            operations.iter().any(|operation| {
                operation["operation"]
                    == serde_json::Value::String("storage_owner_background_eviction".to_string())
                    && operation["status"] == serde_json::Value::String("ok".to_string())
                    && operation["bytes_in_total"] == 32_u64
                    && operation["bytes_out_total"] == 64_u64
            }),
            "stats json should include the recorded eviction operation"
        );

        let runtimes = value["runtimes"]
            .as_array()
            .expect("stats json should expose runtimes as an array");
        assert!(
            runtimes.iter().any(|entry| {
                entry["runtime"] == serde_json::Value::String(runtime.runtime.to_string())
                    && entry["heartbeat_consecutive_failures"] == 2_u64
                    && entry["heartbeat_last_success_ms"] == 456_789_u64
            }),
            "stats json should include heartbeat health for the recorded runtime"
        );

        server
            .shutdown()
            .expect("metrics server should stop cleanly");
    }

    #[test]
    fn breakdown_json_exposes_api_phase_metadata_and_transport() {
        let _guard = metrics_test_lock().lock();
        reset_metrics();
        registry::set_process_tenant("tenant-breakdown");

        let result = Ok(());
        OperationTracker::new("batch_put_from")
            .input_bytes(1024)
            .finish(&result, 512);
        OperationTracker::new("batch_is_readable").finish(&result, 1);
        OperationTracker::new("route_lookup_many")
            .scope("route_lookup")
            .finish(&result, 0);
        registry::record_metadata_operation_with_registry(
            registry::global_metrics_registry(),
            "redis",
            "get_object_route",
            "ok",
            std::time::Duration::from_millis(4),
        );
        registry::record_transport_operation("write", "storage", "ok");
        registry::record_transport_bytes("write", "storage", 512);

        let body = render_breakdown_json();
        let value: serde_json::Value =
            serde_json::from_str(&body).expect("breakdown endpoint should return valid json");
        assert_eq!(value["tenant"], "tenant-breakdown");
        assert!(value["note"]
            .as_str()
            .expect("breakdown should explain ranking semantics")
            .contains("not root-cause proof"));
        assert!(value["operations"]
            .as_array()
            .expect("operations should be an array")
            .iter()
            .any(|entry| entry["operation"] == "batch_put_from"
                && entry["kind"] == "api"
                && entry["bytes_in_total"] == 1024_u64));
        assert!(value["operations"]
            .as_array()
            .expect("operations should be an array")
            .iter()
            .any(|entry| entry["operation"] == "batch_is_readable"
                && entry["kind"] == "api"
                && entry["calls_total"] == 1_u64));
        assert!(value["operations"]
            .as_array()
            .expect("operations should be an array")
            .iter()
            .any(|entry| entry["operation"] == "route_lookup_many" && entry["kind"] == "phase"));
        assert!(value["metadata_operations"]
            .as_array()
            .expect("metadata operations should be an array")
            .iter()
            .any(|entry| entry["backend"] == "redis"
                && entry["operation"] == "get_object_route"
                && entry["calls_total"] == 1_u64));
        assert!(value["transport"]
            .as_array()
            .expect("transport rows should be an array")
            .iter()
            .any(|entry| entry["direction"] == "write"
                && entry["peer_kind"] == "storage"
                && entry["bytes_total"] == 512_u64));
        assert!(!value["bottlenecks"]
            .as_array()
            .expect("bottlenecks should be an array")
            .is_empty());

        reset_metrics();
    }

    #[test]
    fn tracing_init_from_env_covers_invalid_disabled_and_repeated_paths() {
        let _guard = metrics_test_lock().lock();
        let toggle_env = "MOONCAKE_TEST_TRACING_ENABLED";
        let filter_env = "MOONCAKE_TEST_TRACING_FILTER";

        let error = init_tracing(Some("["))
            .expect_err("invalid tracing filter must surface configuration errors");
        assert!(matches!(error, StoreError::InvalidState(_)));

        with_env_var(toggle_env, None, || {
            with_env_var(filter_env, None, || {
                assert!(!init_tracing_from_env(toggle_env, filter_env)
                    .expect("missing toggle should keep tracing disabled"));
            })
        });

        with_env_var(toggle_env, Some("YES"), || {
            with_env_var(filter_env, Some("info"), || {
                assert!(init_tracing_from_env(toggle_env, filter_env)
                    .expect("enabled env toggle should initialize tracing"));
            })
        });
        std::env::set_var(toggle_env, "0");
        with_env_var(toggle_env, Some("YES"), || ());
        assert_eq!(std::env::var(toggle_env).as_deref(), Ok("0"));
        std::env::remove_var(toggle_env);

        init_tracing(Some("[")).expect("repeated tracing init should short-circuit cleanly");
    }

    #[test]
    fn trace_file_helpers_ignore_blank_and_open_append_file() {
        let _guard = metrics_test_lock().lock();
        let path_env = "MOONCAKE_TEST_TRACE_FILE";
        let temp_dir =
            std::env::temp_dir().join(format!("mooncake-store-trace-{}", std::process::id()));
        let trace_path = temp_dir.join("nested").join("real-client.log");

        with_env_var(path_env, None, || {
            assert_eq!(
                trace_file_from_env(path_env).expect("missing trace file env should parse"),
                None
            );
        });

        with_env_var(path_env, Some(""), || {
            assert_eq!(
                trace_file_from_env(path_env).expect("blank trace file env should parse"),
                None
            );
        });

        let mut file = open_trace_file(&trace_path).expect("trace file should open");
        writeln!(file, "first line").expect("trace file write should succeed");
        drop(file);

        let contents = std::fs::read_to_string(&trace_path).expect("trace file should be readable");
        assert!(contents.contains("first line"));

        let _ = std::fs::remove_file(&trace_path);
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[test]
    fn trace_span_events_from_env_defaults_to_none_and_accepts_close() {
        let _guard = metrics_test_lock().lock();
        let env = "MOONCAKE_TEST_TRACE_SPAN_EVENTS";

        with_env_var(env, None, || {
            assert_eq!(
                trace_span_events_from_env(env).expect("missing span events env should parse"),
                FmtSpan::NONE
            );
        });

        with_env_var(env, Some("none"), || {
            assert_eq!(
                trace_span_events_from_env(env).expect("none span events env should parse"),
                FmtSpan::NONE
            );
        });

        with_env_var(env, Some("close"), || {
            assert_eq!(
                trace_span_events_from_env(env).expect("close span events env should parse"),
                FmtSpan::CLOSE
            );
        });

        with_env_var(env, Some("bad"), || {
            assert!(matches!(
                trace_span_events_from_env(env),
                Err(StoreError::InvalidState(_))
            ));
        });
    }

    #[test]
    fn metrics_http_server_env_helpers_reuse_server_and_cover_routes() {
        let _guard = metrics_test_lock().lock();
        let addr_env = "MOONCAKE_TEST_METRICS_ADDR";
        stop_metrics_http_server().expect("metrics server cleanup should succeed");

        with_env_var(addr_env, None, || {
            assert_eq!(
                start_metrics_http_server_from_env(addr_env)
                    .expect("missing metrics env should be ignored"),
                None
            );
        });
        with_env_var(addr_env, Some("   "), || {
            assert_eq!(
                start_metrics_http_server_from_env(addr_env)
                    .expect("blank metrics env should be ignored"),
                None
            );
        });

        let address = with_env_var(addr_env, Some("127.0.0.1:0"), || {
            start_metrics_http_server_from_env(addr_env)
                .expect("metrics env startup should succeed")
                .expect("metrics env should produce a bound address")
        });
        assert_eq!(
            metrics_http_server_addr().as_deref(),
            Some(address.as_str())
        );
        assert_eq!(
            start_metrics_http_server("127.0.0.1:1")
                .expect("reusing an existing metrics server should succeed"),
            address
        );

        let livez = http_get(&address, "/livez");
        assert!(livez.contains("HTTP/1.1 200 OK"));
        assert!(livez.ends_with("ok\n"));

        let tracing_status = http_get(&address, "/tracing");
        assert!(tracing_status.contains("HTTP/1.1 200 OK"));
        assert!(tracing_status.contains("\"enabled\":"));

        let tracing_off = http_get(&address, "/tracing/off");
        assert!(tracing_off.contains("HTTP/1.1 200 OK"));
        assert!(tracing_off.contains("\"enabled\":false"));

        let tracing_bad = http_get(&address, "/tracing?enabled=maybe");
        assert!(tracing_bad.contains("HTTP/1.1 400 Bad Request"));

        let missing = http_get(&address, "/missing");
        assert!(missing.contains("HTTP/1.1 404 Not Found"));
        assert!(missing.ends_with("not found\n"));

        let post = http_request(
            &address,
            &format!("POST /metrics HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"),
        );
        assert!(post.contains("HTTP/1.1 404 Not Found"));

        stop_metrics_http_server().expect("metrics server should stop");
    }

    #[test]
    fn metrics_http_connection_tolerates_empty_clients() {
        let _guard = metrics_test_lock().lock();
        let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should report local address");
        let registry = registry::new_metrics_registry();
        let worker = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("test listener should accept");
            handle_metrics_http_connection(stream, &registry);
        });

        TcpStream::connect(address).expect("probe client should connect and drop immediately");
        worker
            .join()
            .expect("http handler worker should exit cleanly");
    }

    #[test]
    fn runtime_lease_metrics_reconcile_to_latest_live_snapshot() {
        let leases_ab = [
            sample_runtime_lease("runtime-a"),
            sample_runtime_lease("runtime-b"),
        ];
        let leases_a = [sample_runtime_lease("runtime-a")];
        let metrics = registry::snapshot_runtime_leases_after_updates(&[&leases_ab, &leases_a]);
        let runtime_statuses = metrics
            .runtime_status
            .into_iter()
            .map(|sample| sample.key.runtime)
            .collect::<std::collections::BTreeSet<_>>();
        let runtime_leases = metrics
            .runtime_lease_expires_at_ms
            .into_iter()
            .map(|sample| sample.key.runtime)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(
            runtime_statuses,
            std::collections::BTreeSet::from([
                ClientRuntimeId::new("runtime-a", ClientEpoch(1)).to_string()
            ]),
        );
        assert_eq!(
            runtime_leases,
            std::collections::BTreeSet::from([
                ClientRuntimeId::new("runtime-a", ClientEpoch(1)).to_string()
            ]),
        );
    }

    #[test]
    fn heartbeat_health_metrics_are_rendered() {
        let _guard = metrics_test_lock().lock();
        let registry = registry::new_metrics_registry();

        let runtime = ClientRuntimeId::new("runtime-heartbeat", ClientEpoch(3)).to_string();
        registry::record_heartbeat_health_with_registry(&registry, &runtime, 2, 123_456);

        let metrics = render_prometheus_metrics_with_registry(&registry);
        assert!(metrics.contains(
            "mooncake_store_heartbeat_consecutive_failures{tenant=\"default\",runtime=\"runtime-heartbeat:3\"} 2"
        ));
        assert!(metrics.contains(
            "mooncake_store_heartbeat_last_success_ms{tenant=\"default\",runtime=\"runtime-heartbeat:3\"} 123456"
        ));
    }

    #[test]
    fn metrics_http_server_exposes_heartbeat_health_metrics() {
        let _guard = metrics_test_lock().lock();
        let registry = registry::new_metrics_registry();

        let runtime = ClientRuntimeId::new("runtime-heartbeat-http", ClientEpoch(5)).to_string();
        registry::record_heartbeat_health_with_registry(&registry, &runtime, 4, 456_789);

        let server = spawn_metrics_http_server_for_registry("127.0.0.1:0", registry)
            .expect("metrics server should start on an ephemeral port");
        let response = http_get(&server.address, "/metrics");

        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains(
            "mooncake_store_heartbeat_consecutive_failures{tenant=\"default\",runtime=\"runtime-heartbeat-http:5\"} 4"
        ));
        assert!(response.contains(
            "mooncake_store_heartbeat_last_success_ms{tenant=\"default\",runtime=\"runtime-heartbeat-http:5\"} 456789"
        ));

        server
            .shutdown()
            .expect("metrics server should stop cleanly");
    }

    fn http_get(address: &str, path: &str) -> String {
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
        http_request(address, &request)
    }

    fn http_request(address: &str, request: &str) -> String {
        let mut stream = TcpStream::connect(address).expect("http client should connect");
        stream
            .write_all(request.as_bytes())
            .expect("http client should write request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("http client should read response");
        response
    }

    fn with_env_var<T>(key: &str, value: Option<&str>, f: impl FnOnce() -> T) -> T {
        let _guard = test_process_lock().lock();
        let previous = std::env::var(key).ok();
        match value {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
        let result = f();
        match previous {
            Some(value) => std::env::set_var(key, value),
            None => std::env::remove_var(key),
        }
        result
    }

    fn sample_runtime_lease(stable_id: &str) -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(1)),
            state: ClientLifecycleState::Active,
            compatibility: CompatibilityDescriptor::default(),
            endpoints: ClientEndpointSet::default(),
            expires_at_ms: 4_102_444_800_000,
        }
    }
}
