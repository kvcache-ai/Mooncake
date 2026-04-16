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
            StoreError::Transport(format!(
                "admin http server failed to bind {bind_addr}: {error}"
            ))
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
            thread
                .join()
                .map_err(|_| StoreError::InvalidState("admin http server panicked".to_string()))?;
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
    let path_only = request.path.split('?').next().unwrap_or("/").to_string();
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
        ("POST", "/v1/maintenance/cleanup-stale-segments") => {
            match service.cleanup_stale_segments() {
                Ok(report) => http_json_response("200 OK", &report),
                Err(error) => http_store_error(error),
            }
        }
        _ => route_scoped_request(service, request, path_only.as_str()),
    }
}

fn route_scoped_request(service: &AdminService, request: HttpRequest, path_only: &str) -> String {
    if path_only.starts_with("/v1/tenant-quotas/") {
        return route_tenant_quota_request(service, request, path_only);
    }
    if path_only.starts_with("/v1/tenant-object-accounting/") {
        return route_tenant_object_accounting_request(service, request, path_only);
    }
    route_policy_scope_request(service, request, path_only)
}

fn route_policy_scope_request(
    service: &AdminService,
    request: HttpRequest,
    path_only: &str,
) -> String {
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

fn route_tenant_quota_request(
    service: &AdminService,
    request: HttpRequest,
    path_only: &str,
) -> String {
    if request.method != "GET" {
        return http_error_response("405 Method Not Allowed", "method not allowed");
    }
    let Some(scope) = parse_quota_scope_path(path_only) else {
        return http_error_response("404 Not Found", "not found");
    };
    if path_only.ends_with("/reservations") {
        let state = match query_param(&request.path, "state") {
            Some(value) => match parse_reservation_state_query(&value) {
                Ok(state) => Some(state),
                Err(error) => return http_store_error(error),
            },
            None => None,
        };
        return match service.list_tenant_quota_reservations(
            &scope.tenant,
            scope.domain.as_deref(),
            scope.object_set.as_deref(),
            state,
        ) {
            Ok(response) => http_json_response("200 OK", &response),
            Err(error) => http_store_error(error),
        };
    }
    match service.get_tenant_quota_state(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
    ) {
        Ok(response) => http_json_response("200 OK", &response),
        Err(error) => http_store_error(error),
    }
}

fn route_tenant_object_accounting_request(
    service: &AdminService,
    request: HttpRequest,
    path_only: &str,
) -> String {
    if request.method != "GET" {
        return http_error_response("405 Method Not Allowed", "method not allowed");
    }
    let Some((scope, key)) = parse_object_accounting_path(path_only) else {
        return http_error_response("404 Not Found", "not found");
    };
    match service.get_tenant_object_accounting(
        &scope.tenant,
        scope.domain.as_deref(),
        scope.object_set.as_deref(),
        &key,
    ) {
        Ok(response) => http_json_response("200 OK", &response),
        Err(error) => http_store_error(error),
    }
}

#[derive(Debug)]
struct PolicyScopePath {
    tenant: String,
    domain: Option<String>,
    object_set: Option<String>,
}

fn parse_policy_scope_path(path: &str) -> Option<PolicyScopePath> {
    parse_policy_scope_with_prefix(path, "/v1/tenant-policies/")
}

fn parse_quota_scope_path(path: &str) -> Option<PolicyScopePath> {
    let scope = parse_policy_scope_with_prefix(path, "/v1/tenant-quotas/")?;
    let suffix = format!("/v1/tenant-quotas/{}", format_scope_suffix(&scope));
    let reservations_suffix = format!("{suffix}/reservations");
    (path == suffix || path == reservations_suffix).then_some(scope)
}

fn parse_object_accounting_path(path: &str) -> Option<(PolicyScopePath, String)> {
    let prefix = "/v1/tenant-object-accounting/";
    let rest = path.strip_prefix(prefix)?;
    let parts = rest
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [tenant, "objects", key] => Some((
            PolicyScopePath {
                tenant: (*tenant).to_string(),
                domain: None,
                object_set: None,
            },
            percent_decode_component(key)?,
        )),
        [tenant, domain, "objects", key] => Some((
            PolicyScopePath {
                tenant: (*tenant).to_string(),
                domain: Some((*domain).to_string()),
                object_set: None,
            },
            percent_decode_component(key)?,
        )),
        [tenant, domain, object_set, "objects", key] => Some((
            PolicyScopePath {
                tenant: (*tenant).to_string(),
                domain: Some((*domain).to_string()),
                object_set: Some((*object_set).to_string()),
            },
            percent_decode_component(key)?,
        )),
        _ => None,
    }
}

fn parse_policy_scope_with_prefix(path: &str, prefix: &str) -> Option<PolicyScopePath> {
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

fn format_scope_suffix(scope: &PolicyScopePath) -> String {
    let mut suffix = scope.tenant.clone();
    if let Some(domain) = scope.domain.as_deref() {
        suffix.push('/');
        suffix.push_str(domain);
    }
    if let Some(object_set) = scope.object_set.as_deref() {
        suffix.push('/');
        suffix.push_str(object_set);
    }
    suffix
}

fn parse_reservation_state_query(
    value: &str,
) -> Result<mooncake_store_core::TenantQuotaReservationState, StoreError> {
    match value {
        "pending" => Ok(mooncake_store_core::TenantQuotaReservationState::Pending),
        "finalized" => Ok(mooncake_store_core::TenantQuotaReservationState::Finalized),
        "aborted" => Ok(mooncake_store_core::TenantQuotaReservationState::Aborted),
        _ => Err(StoreError::InvalidState(
            "invalid state query parameter: expected pending, finalized, or aborted".to_string(),
        )),
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
            return percent_decode_component(value);
        }
    }
    None
}

fn percent_decode_component(value: &str) -> Option<String> {
    if !value.contains('%') && !value.contains('+') {
        return Some(value.to_string());
    }

    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= bytes.len() {
                    return None;
                }
                let hex = &value[index + 1..index + 3];
                let byte = u8::from_str_radix(hex, 16).ok()?;
                decoded.push(byte);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded).ok()
}

fn query_flag(path: &str, key: &str) -> bool {
    matches!(
        query_param(path, key).as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "YES")
    )
}

fn http_store_error(error: StoreError) -> String {
    match error {
        StoreError::Conflict(message) | StoreError::StaleEpoch(message) => {
            http_error_response("409 Conflict", &message)
        }
        StoreError::NotFound(message) => http_error_response("404 Not Found", &message),
        StoreError::Unsupported(message) => http_error_response("501 Not Implemented", &message),
        StoreError::InvalidState(message) => http_error_response("400 Bad Request", &message),
        StoreError::Metadata(message)
        | StoreError::Transport(message)
        | StoreError::Allocator(message) => {
            http_error_response("500 Internal Server Error", &message)
        }
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
    use mooncake_store_core::{
        ClientEpoch, ClientRuntimeId, MetadataBackend, StoreError, TenantObjectAccountingState,
        TenantPolicySpec, TenantQuotaPolicy, TenantQuotaReservationRequest, TenantRoutePolicy,
    };

    use crate::admin::models::PolicyPatchInput;
    use crate::admin::service::AdminService;

    use super::{
        http_store_error, parse_policy_scope_path, percent_decode_component, query_flag,
        query_param, AdminHttpServerHandle,
    };

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
        assert!(response.contains("supports redis:// and rediss:// metadata only"));

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
        assert!(query_flag(
            "/v1/tenant-policies/tenant-a?effective=true",
            "effective"
        ));
        assert_eq!(
            query_param("/v1/tenant-policies?tenant=tenant-a", "tenant").as_deref(),
            Some("tenant-a")
        );
        assert_eq!(
            query_param("/v1/tenant-policies?tenant=tenant+a", "tenant").as_deref(),
            Some("tenant a")
        );
        assert_eq!(
            query_param("/v1/tenant-policies?tenant=tenant%2Fa", "tenant").as_deref(),
            Some("tenant/a")
        );
        assert_eq!(percent_decode_component("bad%"), None);
        assert_eq!(percent_decode_component("bad%ZZ"), None);
        let parsed = parse_policy_scope_path("/v1/tenant-policies/tenant-a/domain-a/set-a")
            .expect("policy path should parse");
        assert_eq!(parsed.tenant, "tenant-a");
        assert_eq!(parsed.domain.as_deref(), Some("domain-a"));
        assert_eq!(parsed.object_set.as_deref(), Some("set-a"));
    }

    #[test]
    fn admin_http_error_mapping_uses_specific_status_codes() {
        assert!(
            http_store_error(StoreError::NotFound("missing".to_string()))
                .contains("HTTP/1.1 404 Not Found")
        );
        assert!(
            http_store_error(StoreError::Conflict("conflict".to_string()))
                .contains("HTTP/1.1 409 Conflict")
        );
        assert!(
            http_store_error(StoreError::StaleEpoch("stale".to_string()))
                .contains("HTTP/1.1 409 Conflict")
        );
        assert!(http_store_error(StoreError::Metadata("boom".to_string()))
            .contains("HTTP/1.1 500 Internal Server Error"));
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
        assert_eq!(
            stored.spec.routing,
            Some(TenantRoutePolicy {
                route_topk: Some(4),
                route_control: Some(RouteControlMode::MetadataOnly),
            })
        );

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

    #[test]
    fn admin_http_server_serves_quota_observation_endpoints() {
        let service = test_service();
        service
            .backend()
            .put_tenant_policy(
                &mooncake_store_core::TenantPolicy {
                    scope: mooncake_store_core::TenantPolicyScope::new(
                        "tenant-a",
                        None::<String>,
                        None::<String>,
                    ),
                    spec: TenantPolicySpec {
                        quota: Some(TenantQuotaPolicy {
                            max_bytes: Some(1024),
                            max_objects: Some(16),
                        }),
                        ..TenantPolicySpec::default()
                    },
                    version: 1,
                    updated_at_ms: 1,
                    updated_by: "tester".to_string(),
                },
                None,
            )
            .expect("tenant policy should store");
        service
            .backend()
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "res-a".to_string(),
                scope: mooncake_store_core::TenantPolicyScope::new(
                    "tenant-a",
                    None::<String>,
                    None::<String>,
                ),
                key: mooncake_store_core::ObjectKey::new("tenant-a::object-a"),
                expected_object_version: None,
                delta_bytes: 12,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(1024),
                    max_objects: Some(16),
                },
                expires_at_ms: 10,
                created_at_ms: 5,
                writer_runtime: ClientRuntimeId::new("writer-a", ClientEpoch(1)),
            })
            .expect("quota reservation should store");
        service
            .backend()
            .finalize_tenant_quota(&mooncake_store_core::TenantQuotaFinalizeRequest {
                reservation_id: "res-a".to_string(),
                expected_object_version: None,
                committed_length: Some(12),
                route_version: Some(mooncake_store_core::RouteVersion(7)),
                state: TenantObjectAccountingState::Active,
                updated_at_ms: 6,
                updated_by: "writer-a".to_string(),
            })
            .expect("quota finalize should succeed");
        service
            .backend()
            .reserve_tenant_quota(&TenantQuotaReservationRequest {
                reservation_id: "res-b".to_string(),
                scope: mooncake_store_core::TenantPolicyScope::new(
                    "tenant-a",
                    None::<String>,
                    None::<String>,
                ),
                key: mooncake_store_core::ObjectKey::new("tenant-a::object-b"),
                expected_object_version: None,
                delta_bytes: 3,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(1024),
                    max_objects: Some(16),
                },
                expires_at_ms: 12,
                created_at_ms: 8,
                writer_runtime: ClientRuntimeId::new("writer-b", ClientEpoch(1)),
            })
            .expect("pending reservation should store");

        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let quota_state = http_request(
            &address,
            &format!(
                "GET /v1/tenant-quotas/tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(quota_state.contains("HTTP/1.1 200 OK"));
        assert!(quota_state.contains("\"used_bytes\":12"));
        assert!(quota_state.contains("\"pending_reserved_bytes\":3"));

        let object = http_request(
            &address,
            &format!(
                "GET /v1/tenant-object-accounting/tenant-a/objects/object-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(object.contains("HTTP/1.1 200 OK"));
        assert!(object.contains("\"committed_length\":12"));
        assert!(object.contains("\"state\":\"Active\""));

        let reservations = http_request(
            &address,
            &format!(
                "GET /v1/tenant-quotas/tenant-a/reservations?state=pending HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(reservations.contains("HTTP/1.1 200 OK"));
        assert!(reservations.contains("\"reservation_id\":\"res-b\""));
        assert!(!reservations.contains("\"reservation_id\":\"res-a\""));

        server.shutdown().expect("server shutdown");
    }
}
