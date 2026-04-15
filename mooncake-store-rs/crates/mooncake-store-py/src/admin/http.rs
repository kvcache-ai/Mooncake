use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mooncake_store_core::StoreError;
use serde::Serialize;

use super::models::{ErrorResponse, PutTenantPolicyRequest};
use super::service::AdminService;

const HTTP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HTTP_READ_TIMEOUT: Duration = Duration::from_millis(250);

pub struct AdminHttpServerHandle {
    address: String,
    shutdown: Sender<()>,
    thread: Option<JoinHandle<()>>,
}

impl AdminHttpServerHandle {
    pub fn start(bind_addr: &str, service: AdminService) -> mooncake_store_core::Result<Self> {
        let listener = TcpListener::bind(bind_addr).map_err(|error| {
            StoreError::Transport(format!("admin http server failed to bind {bind_addr}: {error}"))
        })?;
        listener.set_nonblocking(true).map_err(|error| {
            StoreError::Transport(format!(
                "admin http server failed to enable nonblocking mode: {error}"
            ))
        })?;
        let address = listener
            .local_addr()
            .map_err(|error| {
                StoreError::Transport(format!(
                    "admin http server failed to read local address: {error}"
                ))
            })?
            .to_string();
        let (shutdown, shutdown_rx) = mpsc::channel();
        let thread = thread::Builder::new()
            .name(format!("mooncake-store-admin-{address}"))
            .spawn(move || run_admin_http_server(listener, shutdown_rx, service))
            .map_err(|error| {
                StoreError::Transport(format!(
                    "admin http server failed to spawn worker thread: {error}"
                ))
            })?;
        Ok(Self {
            address,
            shutdown,
            thread: Some(thread),
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn shutdown(&mut self) -> mooncake_store_core::Result<()> {
        let _ = self.shutdown.send(());
        if let Some(thread) = self.thread.take() {
            thread.join().map_err(|_| {
                StoreError::InvalidState("admin http server panicked".to_string())
            })?;
        }
        Ok(())
    }
}

impl Drop for AdminHttpServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn run_admin_http_server(listener: TcpListener, shutdown_rx: Receiver<()>, service: AdminService) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => handle_admin_http_connection(stream, &service),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if shutdown_rx.recv_timeout(HTTP_POLL_INTERVAL).is_ok() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

fn handle_admin_http_connection(mut stream: TcpStream, service: &AdminService) {
    if stream.set_read_timeout(Some(HTTP_READ_TIMEOUT)).is_err() {
        return;
    }
    let request = match read_http_request(&mut stream) {
        Ok(request) => request,
        Err(_) => return,
    };
    let response = route_request(service, request);
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

fn route_request(service: &AdminService, request: HttpRequest) -> String {
    let path_only = request
        .path
        .split('?')
        .next()
        .unwrap_or("/")
        .to_string();
    match (request.method.as_str(), path_only.as_str()) {
        ("GET", "/healthz") | ("GET", "/livez") => http_text_response("200 OK", "ok\n"),
        ("GET", "/v1/tenant-policies") => {
            let tenant_filter = query_param(&request.path, "tenant");
            let effective = query_flag(&request.path, "effective");
            if effective {
                return http_error_response(
                    "400 Bad Request",
                    "effective=true requires a tenant policy scope path",
                );
            }
            match service.list_tenant_policies(tenant_filter.as_deref()) {
                Ok(policies) => http_json_response("200 OK", &policies),
                Err(error) => http_store_error(error),
            }
        }
        ("POST", "/v1/maintenance/cleanup-stale-segments") => match service.cleanup_stale_segments()
        {
            Ok(report) => http_json_response("200 OK", &report),
            Err(error) => http_store_error(error),
        },
        _ => route_policy_scope_request(service, request, path_only.as_str()),
    }
}

fn route_policy_scope_request(service: &AdminService, request: HttpRequest, path_only: &str) -> String {
    let Some(scope) = parse_policy_scope_path(path_only) else {
        return http_error_response("404 Not Found", "not found");
    };
    match request.method.as_str() {
        "GET" => {
            let effective = query_flag(&request.path, "effective");
            match service.get_tenant_policy(
                &scope.tenant,
                scope.domain.as_deref(),
                scope.object_set.as_deref(),
                effective,
            ) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        "PUT" => {
            let payload = match serde_json::from_slice::<PutTenantPolicyRequest>(&request.body) {
                Ok(payload) => payload,
                Err(error) => {
                    return http_error_response(
                        "400 Bad Request",
                        &format!("invalid JSON body: {error}"),
                    )
                }
            };
            match service.set_tenant_policy(
                &scope.tenant,
                scope.domain.as_deref(),
                scope.object_set.as_deref(),
                payload.patch,
                payload.expected_version,
                &payload.updated_by,
            ) {
                Ok(policy) => http_json_response("200 OK", &policy),
                Err(error) => http_store_error(error),
            }
        }
        "DELETE" => {
            let expected_version = query_param(&request.path, "expected_version")
                .map(|value| {
                    value.parse::<u64>().map_err(|error| {
                        StoreError::InvalidState(format!(
                            "invalid expected_version query parameter: {error}"
                        ))
                    })
                })
                .transpose();
            let expected_version = match expected_version {
                Ok(version) => version,
                Err(error) => return http_store_error(error),
            };
            match service.delete_tenant_policy(
                &scope.tenant,
                scope.domain.as_deref(),
                scope.object_set.as_deref(),
                expected_version,
            ) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        _ => http_error_response("405 Method Not Allowed", "method not allowed"),
    }
}

#[derive(Debug)]
struct PolicyScopePath {
    tenant: String,
    domain: Option<String>,
    object_set: Option<String>,
}

fn parse_policy_scope_path(path: &str) -> Option<PolicyScopePath> {
    let prefix = "/v1/tenant-policies/";
    let rest = path.strip_prefix(prefix)?;
    let parts = rest
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [tenant] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: None,
            object_set: None,
        }),
        [tenant, domain] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: Some((*domain).to_string()),
            object_set: None,
        }),
        [tenant, domain, object_set] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: Some((*domain).to_string()),
            object_set: Some((*object_set).to_string()),
        }),
        _ => None,
    }
}

fn read_http_request(stream: &mut TcpStream) -> std::io::Result<HttpRequest> {
    let mut request = Vec::with_capacity(2048);
    let mut buffer = [0_u8; 1024];
    let mut header_end = None;
    let mut content_length = 0usize;
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
        if header_end.is_none() {
            header_end = request.windows(4).position(|window| window == b"\r\n\r\n");
            if let Some(index) = header_end {
                content_length = parse_content_length(&request[..index + 4]);
                let body_len = request.len().saturating_sub(index + 4);
                if body_len >= content_length {
                    break;
                }
            }
        } else if let Some(index) = header_end {
            let body_len = request.len().saturating_sub(index + 4);
            if body_len >= content_length {
                break;
            }
        }
        if request.len() >= 1024 * 1024 {
            break;
        }
    }

    let header_end = header_end.unwrap_or(request.len());
    let header_bytes = &request[..header_end];
    let header_text = String::from_utf8_lossy(header_bytes);
    let first_line = header_text.lines().next().unwrap_or_default();
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let body_start = usize::min(request.len(), header_end.saturating_add(4));
    let body_end = usize::min(request.len(), body_start.saturating_add(content_length));
    let body = request[body_start..body_end].to_vec();
    Ok(HttpRequest { method, path, body })
}

fn parse_content_length(headers: &[u8]) -> usize {
    let text = String::from_utf8_lossy(headers);
    text.lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0)
}

fn query_param(path: &str, key: &str) -> Option<String> {
    let query = path.split_once('?')?.1;
    for pair in query.split('&') {
        let (name, value) = pair.split_once('=')?;
        if name == key {
            return Some(value.to_string());
        }
    }
    None
}

fn query_flag(path: &str, key: &str) -> bool {
    matches!(query_param(path, key).as_deref(), Some("1" | "true" | "TRUE" | "yes" | "YES"))
}

fn http_store_error(error: StoreError) -> String {
    match error {
        StoreError::Conflict(message) => http_error_response("409 Conflict", &message),
        StoreError::Unsupported(message) => http_error_response("501 Not Implemented", &message),
        StoreError::InvalidState(message)
        | StoreError::Metadata(message)
        | StoreError::Transport(message)
        | StoreError::Allocator(message)
        | StoreError::StaleEpoch(message)
        | StoreError::NotFound(message) => http_error_response("400 Bad Request", &message),
    }
}

fn http_text_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
}

fn http_json_response<T: Serialize>(status: &str, body: &T) -> String {
    match serde_json::to_vec(body) {
        Ok(encoded) => format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            encoded.len(),
            String::from_utf8_lossy(&encoded)
        ),
        Err(error) => http_error_response(
            "500 Internal Server Error",
            &format!("failed to serialize response: {error}"),
        ),
    }
}

fn http_error_response(status: &str, message: &str) -> String {
    http_json_response(
        status,
        &ErrorResponse {
            error: message.to_string(),
        },
    )
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;

    use mooncake_metadata::{InMemoryMetadataBackend, MetadataKeyspace};
    use mooncake_store_client::RouteControlMode;
    use mooncake_store_core::{MetadataBackend, TenantPolicySpec, TenantRoutePolicy};

    use crate::admin::models::PolicyPatchInput;
    use crate::admin::service::AdminService;

    use super::AdminHttpServerHandle;

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

    fn test_service() -> AdminService {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        AdminService::new(backend, "memory://test", MetadataKeyspace::default())
    }

    #[test]
    fn admin_http_server_serves_health_and_policy_crud() {
        let service = test_service();
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service.clone()).expect("server start");
        let address = server.address().to_string();

        let health = http_request(
            &address,
            &format!("GET /healthz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"),
        );
        assert!(health.contains("HTTP/1.1 200 OK"));
        assert!(health.ends_with("ok\n"));

        let put_body = serde_json::json!({
            "route_topk": 3,
            "route_control": "EmbeddedWrh",
            "updated_by": "tester"
        })
        .to_string();
        let put = http_request(
            &address,
            &format!(
                "PUT /v1/tenant-policies/tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                put_body.len(),
                put_body
            ),
        );
        assert!(put.contains("HTTP/1.1 200 OK"));
        assert!(put.contains("\"tenant\":\"tenant-a\""));

        let get = http_request(
            &address,
            &format!(
                "GET /v1/tenant-policies/tenant-a?effective=true HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(get.contains("HTTP/1.1 200 OK"));
        assert!(get.contains("\"effective\":true"));
        assert!(get.contains("\"route_topk\":3"));

        let delete = http_request(
            &address,
            &format!(
                "DELETE /v1/tenant-policies/tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(delete.contains("HTTP/1.1 200 OK"));
        assert!(delete.contains("\"removed\":true"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_reports_cleanup_capability_errors() {
        let service = test_service();
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "POST /v1/maintenance/cleanup-stale-segments HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 501 Not Implemented"));
        assert!(response.contains("redis:// metadata only"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_bad_payloads() {
        let service = test_service();
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let bad_body = "bad{";
        let response = http_request(
            &address,
            &format!(
                "PUT /v1/tenant-policies/tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                bad_body.len(),
                bad_body
            ),
        );
        assert!(response.contains("HTTP/1.1 400 Bad Request"));
        assert!(response.contains("invalid JSON body"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn query_helpers_and_scope_parser_cover_paths() {
        assert!(super::query_flag("/v1/tenant-policies/tenant-a?effective=true", "effective"));
        assert_eq!(
            super::query_param("/v1/tenant-policies?tenant=tenant-a", "tenant").as_deref(),
            Some("tenant-a")
        );
        let parsed = super::parse_policy_scope_path("/v1/tenant-policies/tenant-a/domain-a/set-a")
            .expect("policy path should parse");
        assert_eq!(parsed.tenant, "tenant-a");
        assert_eq!(parsed.domain.as_deref(), Some("domain-a"));
        assert_eq!(parsed.object_set.as_deref(), Some("set-a"));
    }

    #[test]
    fn admin_http_service_fixture_matches_direct_service_behavior() {
        let service = test_service();
        let stored = service
            .set_tenant_policy(
                "tenant-a",
                None,
                None,
                PolicyPatchInput {
                    route_topk: Some(4),
                    route_control: Some(RouteControlMode::MetadataOnly),
                    ..PolicyPatchInput::default()
                },
                None,
                "tester",
            )
            .expect("tenant policy should store");
        assert_eq!(stored.spec.routing, Some(TenantRoutePolicy {
            route_topk: Some(4),
            route_control: Some(RouteControlMode::MetadataOnly),
        }));

        let fetched = service
            .get_tenant_policy("tenant-a", None, None, true)
            .expect("tenant policy should resolve");
        assert_eq!(
            fetched.effective_spec,
            Some(TenantPolicySpec {
                routing: Some(TenantRoutePolicy {
                    route_topk: Some(4),
                    route_control: Some(RouteControlMode::MetadataOnly),
                }),
                ..TenantPolicySpec::default()
            })
        );
    }
}
