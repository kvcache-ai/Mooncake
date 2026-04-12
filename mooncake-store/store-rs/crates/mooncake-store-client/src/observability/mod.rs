mod exporter;
mod process;
pub(crate) mod registry;

use is_terminal::IsTerminal;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use mooncake_store_core::{Result, StoreError};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{fmt, EnvFilter};

pub use registry::{MetricsSnapshot, OperationMetricSnapshot};

static TRACING_STATE: OnceLock<()> = OnceLock::new();
static METRICS_HTTP_SERVER: OnceLock<Mutex<Option<MetricsHttpServer>>> = OnceLock::new();
#[cfg(test)]
static METRICS_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

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
    scope: &'static str,
    start: Instant,
    bytes_in: u64,
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
            _inflight: InflightGuard::enter(operation, scope),
        }
    }

    pub fn scope(mut self, scope: &'static str) -> Self {
        if self.scope != scope {
            self._inflight = InflightGuard::enter(self.operation, scope);
        }
        self.scope = scope;
        self
    }

    pub fn input_bytes(mut self, bytes_in: u64) -> Self {
        self.bytes_in = bytes_in;
        self
    }

    pub fn finish<T>(&self, result: &Result<T>, bytes_out: u64) {
        let result_label = result_label(result);
        self.finish_with_result(result_label, bytes_out);
    }

    pub(crate) fn finish_with_result(&self, result: &'static str, bytes_out: u64) {
        registry::record_request(
            self.operation,
            self.scope,
            result,
            self.bytes_in,
            bytes_out,
            self.start.elapsed(),
        );
    }
}

pub struct InflightGuard {
    operation: &'static str,
    scope: &'static str,
}

impl InflightGuard {
    pub fn enter(operation: &'static str, scope: &'static str) -> Self {
        registry::increment_inflight(operation, scope);
        Self { operation, scope }
    }
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        registry::decrement_inflight(self.operation, self.scope);
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

    let use_ansi = std::io::stdout().is_terminal();

    let subscriber = fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .with_thread_ids(true)
        .with_ansi(use_ansi)
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
    exporter::render_prometheus_metrics(&snapshot_metrics())
}

pub fn snapshot_metrics() -> MetricsSnapshot {
    registry::snapshot_metrics(process::snapshot_process())
}

#[cfg(test)]
pub fn reset_metrics() {
    registry::reset_metrics();
}

#[cfg(test)]
pub fn metrics_test_lock() -> &'static Mutex<()> {
    METRICS_TEST_LOCK.get_or_init(|| Mutex::new(()))
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
        Err(StoreError::Conflict(_)) => "conflict",
        Err(_) => "error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};

    #[test]
    fn metrics_http_server_serves_prometheus_text() {
        let _guard = metrics_test_lock().lock().expect("test lock poisoned");
        reset_metrics();
        stop_metrics_http_server().expect("metrics server cleanup should succeed");

        let result = Ok(());
        OperationTracker::new("put")
            .input_bytes(16)
            .finish(&result, 16);

        let address = start_metrics_http_server("127.0.0.1:0")
            .expect("metrics server should start on an ephemeral port");
        let response = http_get(&address, "/metrics");

        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("mooncake_store_client_operation_total"));
        assert!(response.contains("operation=\"put\",status=\"ok\""));

        stop_metrics_http_server().expect("metrics server should stop");
    }

    #[test]
    fn request_metrics_include_histogram_bytes_and_inflight() {
        let _guard = metrics_test_lock().lock().expect("test lock poisoned");
        reset_metrics();

        let result: Result<()> = Ok(());
        {
            let tracker = OperationTracker::new("put")
                .scope("foreground")
                .input_bytes(16);
            tracker.finish(&result, 8);

            let active = render_prometheus_metrics();
            assert!(active.contains(
                "mooncake_store_request_inflight{operation=\"put\",scope=\"foreground\"} 1"
            ));
            assert!(active.contains(
                "mooncake_store_request_total{operation=\"put\",scope=\"foreground\",result=\"ok\"} 1"
            ));
            assert!(active.contains(
                "mooncake_store_request_duration_seconds_bucket{operation=\"put\",scope=\"foreground\",result=\"ok\",le=\"+Inf\"} 1"
            ));
            assert!(active.contains(
                "mooncake_store_request_bytes_total{operation=\"put\",direction=\"in\",scope=\"foreground\"} 16"
            ));
            assert!(active.contains(
                "mooncake_store_request_bytes_total{operation=\"put\",direction=\"out\",scope=\"foreground\"} 8"
            ));
        }

        let idle = render_prometheus_metrics();
        assert!(idle
            .contains("mooncake_store_request_inflight{operation=\"put\",scope=\"foreground\"} 0"));
    }

    #[test]
    fn process_metrics_are_rendered_with_request_snapshot() {
        let _guard = metrics_test_lock().lock().expect("test lock poisoned");
        reset_metrics();

        let metrics = render_prometheus_metrics();

        assert!(metrics.contains("# HELP process_cpu_seconds_total"));
        assert!(metrics.contains("# TYPE process_resident_memory_bytes gauge"));
    }

    #[test]
    fn metrics_http_server_serves_health_probe() {
        let _guard = metrics_test_lock().lock().expect("test lock poisoned");
        stop_metrics_http_server().expect("metrics server cleanup should succeed");
        let address = start_metrics_http_server("127.0.0.1:0")
            .expect("metrics server should start on an ephemeral port");
        let response = http_get(&address, "/healthz");
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.ends_with("ok\n"));
        stop_metrics_http_server().expect("metrics server should stop");
    }

    #[test]
    fn tracing_init_from_env_covers_invalid_disabled_and_repeated_paths() {
        let _guard = metrics_test_lock().lock().expect("test lock poisoned");
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
    fn metrics_http_server_env_helpers_reuse_server_and_cover_routes() {
        let _guard = metrics_test_lock().lock().expect("test lock poisoned");
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
        let _guard = metrics_test_lock().lock().expect("test lock poisoned");
        let listener = TcpListener::bind("127.0.0.1:0").expect("ephemeral listener should bind");
        let address = listener
            .local_addr()
            .expect("listener should report local address");
        let worker = std::thread::spawn(move || {
            let (stream, _) = listener.accept().expect("test listener should accept");
            handle_metrics_http_connection(stream);
        });

        TcpStream::connect(address).expect("probe client should connect and drop immediately");
        worker
            .join()
            .expect("http handler worker should exit cleanly");
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
}
