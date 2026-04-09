use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;
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
static METRICS_HTTP_SERVER: OnceLock<Mutex<Option<MetricsHttpServer>>> = OnceLock::new();

const HTTP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HTTP_READ_TIMEOUT: Duration = Duration::from_millis(250);

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

pub fn start_metrics_http_server(bind_addr: &str) -> Result<String> {
    let mut server = metrics_http_server()
        .lock()
        .expect("metrics http server lock poisoned");
    if let Some(server) = server.as_ref() {
        return Ok(server.address.clone());
    }

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
        .spawn(move || run_metrics_http_server(listener, shutdown_rx))
        .map_err(|error| {
            StoreError::InvalidState(format!(
                "metrics http server failed to spawn worker thread: {error}"
            ))
        })?;
    *server = Some(MetricsHttpServer {
        address: address.clone(),
        shutdown,
        thread,
    });
    Ok(address)
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

fn metrics_http_server() -> &'static Mutex<Option<MetricsHttpServer>> {
    METRICS_HTTP_SERVER.get_or_init(|| Mutex::new(None))
}

fn run_metrics_http_server(listener: TcpListener, shutdown_rx: Receiver<()>) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => handle_metrics_http_connection(stream),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if shutdown_rx.recv_timeout(HTTP_POLL_INTERVAL).is_ok() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

fn handle_metrics_http_connection(mut stream: TcpStream) {
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
            render_prometheus_metrics(),
        ),
        "/healthz" | "/livez" => ("200 OK", "text/plain; charset=utf-8", "ok\n".to_string()),
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

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::{Mutex, OnceLock};

    use super::*;

    static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    #[test]
    fn metrics_http_server_serves_prometheus_text() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        reset_metrics();
        stop_metrics_http_server().expect("metrics server cleanup should succeed");

        let result = Ok(());
        OperationTracker::new("put")
            .input_bytes(16)
            .finish(&result, 16);

        let Ok(address) = start_metrics_http_server("127.0.0.1:0") else {
            return;
        };
        let response = http_get(&address, "/metrics");

        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("mooncake_store_client_operation_total"));
        assert!(response.contains("operation=\"put\",status=\"ok\""));

        stop_metrics_http_server().expect("metrics server should stop");
    }

    #[test]
    fn metrics_http_server_serves_health_probe() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        stop_metrics_http_server().expect("metrics server cleanup should succeed");
        let Ok(address) = start_metrics_http_server("127.0.0.1:0") else {
            return;
        };
        let response = http_get(&address, "/healthz");
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.ends_with("ok\n"));
        stop_metrics_http_server().expect("metrics server should stop");
    }

    fn http_get(address: &str, path: &str) -> String {
        let mut stream = TcpStream::connect(address).expect("http client should connect");
        let request = format!("GET {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .expect("http client should write request");
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .expect("http client should read response");
        response
    }

    fn test_lock() -> &'static Mutex<()> {
        TEST_LOCK.get_or_init(|| Mutex::new(()))
    }
}
