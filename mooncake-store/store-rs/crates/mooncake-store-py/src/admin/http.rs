use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mooncake_store_core::StoreError;
use serde::Serialize;

use super::models::{
    ErrorResponse, PutTenantPolicyRequest, RouteMigrationMode, RouteMigrationTaskSubmitRequest,
    TenantQuotaAbortRequest, TenantQuotaReconcileRequest,
};
use super::service::AdminService;

const HTTP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HTTP_READ_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_HTTP_BODY_BYTES: usize = 1024 * 1024;

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
    let response = match read_http_request(&mut stream) {
        Ok(request) => route_request(service, request),
        Err(HttpRequestReadError::ContentTooLarge) => {
            drain_remaining_input(&mut stream);
            http_error_response("413 Payload Too Large", "request content is too large")
        }
        Err(HttpRequestReadError::Io) => return,
    };
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

#[derive(Debug)]
enum HttpRequestReadError {
    Io,
    ContentTooLarge,
}

impl From<std::io::Error> for HttpRequestReadError {
    fn from(_: std::io::Error) -> Self {
        Self::Io
    }
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
        ("GET", "/v1/route-migrations") => {
            http_json_response("200 OK", &service.list_route_migration_tasks())
        }
        ("POST", "/v1/route-migrations/copy") => {
            submit_route_migration_request(service, &request.body, RouteMigrationMode::Copy)
        }
        ("POST", "/v1/route-migrations/move") => {
            submit_route_migration_request(service, &request.body, RouteMigrationMode::Move)
        }
        _ => route_scoped_request(service, request, path_only.as_str()),
    }
}

fn submit_route_migration_request(
    service: &AdminService,
    body: &[u8],
    expected_mode: RouteMigrationMode,
) -> String {
    let payload = match serde_json::from_slice::<RouteMigrationTaskSubmitRequest>(body) {
        Ok(payload) => payload,
        Err(error) => {
            return http_error_response("400 Bad Request", &format!("invalid JSON body: {error}"))
        }
    };
    match service.submit_route_migration_task(expected_mode, payload) {
        Ok(response) => http_json_response("200 OK", &response),
        Err(error) => http_store_error(error),
    }
}

fn route_scoped_request(service: &AdminService, request: HttpRequest, path_only: &str) -> String {
    if path_only.starts_with("/v1/route-migrations/") {
        return route_migration_request(service, request, path_only);
    }
    if path_only.starts_with("/v1/tenant-quotas/") {
        return route_tenant_quota_request(service, request, path_only);
    }
    if path_only.starts_with("/v1/tenant-object-accounting/") {
        return route_tenant_object_accounting_request(service, request, path_only);
    }
    route_policy_scope_request(service, request, path_only)
}

fn route_migration_request(
    service: &AdminService,
    request: HttpRequest,
    path_only: &str,
) -> String {
    let Some(task_id) = path_only.strip_prefix("/v1/route-migrations/") else {
        return http_error_response("404 Not Found", "not found");
    };
    if request.method != "GET" {
        return http_error_response("405 Method Not Allowed", "method not allowed");
    }
    match service.get_route_migration_task(task_id) {
        Ok(response) => http_json_response("200 OK", &response),
        Err(error) => http_store_error(error),
    }
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
    let Some(scope) = parse_quota_scope_path(path_only) else {
        return http_error_response("404 Not Found", "not found");
    };
    if path_only.ends_with("/reservations") {
        if request.method != "GET" {
            return http_error_response("405 Method Not Allowed", "method not allowed");
        }
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
    if let Some(reservation_id) = parse_quota_abort_path(path_only, &scope) {
        if request.method != "POST" {
            return http_error_response("405 Method Not Allowed", "method not allowed");
        }
        let payload =
            serde_json::from_slice::<TenantQuotaAbortRequest>(&request.body).unwrap_or_default();
        return match service.abort_tenant_quota_reservation(
            &scope.tenant,
            scope.domain.as_deref(),
            scope.object_set.as_deref(),
            &reservation_id,
            payload.dry_run,
        ) {
            Ok(response) => http_json_response("200 OK", &response),
            Err(error) => http_store_error(error),
        };
    }
    if path_only.ends_with("/reconcile") {
        if request.method != "POST" {
            return http_error_response("405 Method Not Allowed", "method not allowed");
        }
        let payload = serde_json::from_slice::<TenantQuotaReconcileRequest>(&request.body)
            .unwrap_or_default();
        return match service.reconcile_tenant_quota_reservations(
            &scope.tenant,
            scope.domain.as_deref(),
            scope.object_set.as_deref(),
            payload.dry_run,
        ) {
            Ok(response) => http_json_response("200 OK", &response),
            Err(error) => http_store_error(error),
        };
    }
    if request.method != "GET" {
        return http_error_response("405 Method Not Allowed", "method not allowed");
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
    let prefix = "/v1/tenant-quotas/";
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
        [tenant, "reservations"] | [tenant, "reconcile"] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: None,
            object_set: None,
        }),
        [tenant, "reservations", _reservation_id, "abort"] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: None,
            object_set: None,
        }),
        [tenant, domain] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: Some((*domain).to_string()),
            object_set: None,
        }),
        [tenant, domain, "reservations"] | [tenant, domain, "reconcile"] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: Some((*domain).to_string()),
            object_set: None,
        }),
        [tenant, domain, "reservations", _reservation_id, "abort"] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: Some((*domain).to_string()),
            object_set: None,
        }),
        [tenant, domain, object_set] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: Some((*domain).to_string()),
            object_set: Some((*object_set).to_string()),
        }),
        [tenant, domain, object_set, "reservations"]
        | [tenant, domain, object_set, "reconcile"] => Some(PolicyScopePath {
            tenant: (*tenant).to_string(),
            domain: Some((*domain).to_string()),
            object_set: Some((*object_set).to_string()),
        }),
        [tenant, domain, object_set, "reservations", _reservation_id, "abort"] => {
            Some(PolicyScopePath {
                tenant: (*tenant).to_string(),
                domain: Some((*domain).to_string()),
                object_set: Some((*object_set).to_string()),
            })
        }
        _ => None,
    }
}

fn parse_quota_abort_path(path: &str, scope: &PolicyScopePath) -> Option<String> {
    let prefix = format!(
        "/v1/tenant-quotas/{}/reservations/",
        format_scope_suffix(scope)
    );
    let rest = path.strip_prefix(&prefix)?;
    let reservation_id = rest.strip_suffix("/abort")?;
    percent_decode_component(reservation_id)
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

fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest, HttpRequestReadError> {
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
                if content_length > MAX_HTTP_BODY_BYTES {
                    return Err(HttpRequestReadError::ContentTooLarge);
                }
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
        if request.len() >= MAX_HTTP_BODY_BYTES {
            while stream.read(&mut buffer)? > 0 {}
            return Err(HttpRequestReadError::ContentTooLarge);
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

fn drain_remaining_input(stream: &mut TcpStream) {
    let mut buffer = [0_u8; 1024];
    while let Ok(read) = stream.read(&mut buffer) {
        if read == 0 {
            break;
        }
    }
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
        StoreError::Conflict(message)
        | StoreError::QuotaExceeded { message, .. }
        | StoreError::StaleEpoch(message) => http_error_response("409 Conflict", &message),
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
    use std::collections::VecDeque;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use mooncake_metadata::{InMemoryMetadataBackend, MetadataKeyspace};
    use mooncake_store_client::{control_plane_pb, RouteControlMode};
    use mooncake_store_core::{
        ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState, ClientRuntimeId,
        CompatibilityDescriptor, MetadataBackend, ObjectKey, ObjectRoute, ReplicaRoute,
        ReplicaTier, RouteState, RouteVersion, SegmentName, StoreError,
        TenantObjectAccountingState, TenantPolicySpec, TenantQuotaPolicy,
        TenantQuotaReservationRequest, TenantRoutePolicy,
    };
    use parking_lot::Mutex;

    use crate::admin::models::PolicyPatchInput;
    use crate::admin::service::{
        AdminService, MigrationExecutionProbe, MigrationQueueConfig, MigrationRpc,
    };

    use super::{
        http_store_error, parse_policy_scope_path, parse_quota_abort_path, parse_quota_scope_path,
        percent_decode_component, query_flag, query_param, AdminHttpServerHandle, PolicyScopePath,
        MAX_HTTP_BODY_BYTES,
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

    #[derive(Clone, Default)]
    struct FakeMigrationRpc {
        submit_results: Arc<Mutex<VecDeque<mooncake_store_core::Result<String>>>>,
        status_results: Arc<Mutex<VecDeque<mooncake_store_core::Result<MigrationExecutionProbe>>>>,
        route_results: Arc<Mutex<VecDeque<mooncake_store_core::Result<Option<ObjectRoute>>>>>,
    }

    impl MigrationRpc for FakeMigrationRpc {
        fn submit_migration_task(
            &self,
            _lease: &ClientLease,
            _request: control_plane_pb::SubmitMigrationTaskRequest,
        ) -> mooncake_store_core::Result<String> {
            self.submit_results
                .lock()
                .pop_front()
                .unwrap_or_else(|| Ok("execution-1".to_string()))
        }

        fn get_migration_execution_status(
            &self,
            _lease: &ClientLease,
            _request: control_plane_pb::GetMigrationExecutionStatusRequest,
        ) -> mooncake_store_core::Result<MigrationExecutionProbe> {
            self.status_results.lock().pop_front().unwrap_or_else(|| {
                Ok(MigrationExecutionProbe {
                    state: control_plane_pb::MigrationExecutionState::Succeeded,
                    attempts: 1,
                    last_error: String::new(),
                })
            })
        }

        fn get_route(
            &self,
            _lease: &ClientLease,
            _namespace: &str,
            _authority: &mooncake_store_core::ClientStableId,
            _key: &ObjectKey,
        ) -> mooncake_store_core::Result<Option<ObjectRoute>> {
            self.route_results.lock().pop_front().unwrap_or(Ok(None))
        }
    }

    fn test_migration_service(rpc: Arc<dyn MigrationRpc>) -> AdminService {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease("executor-a", 1))
            .expect("executor lease should store");
        AdminService::new_with_migration_support(
            backend,
            "memory://test",
            MetadataKeyspace::default(),
            MigrationQueueConfig {
                default_max_retries: 2,
                retry_base_delay: Duration::from_millis(5),
                retry_max_delay: Duration::from_millis(5),
                poll_interval: Duration::from_millis(5),
            },
            rpc,
        )
    }

    fn live_lease(stable_id: &str, epoch: u64) -> ClientLease {
        ClientLease {
            runtime: ClientRuntimeId::new(stable_id, ClientEpoch(epoch)),
            compatibility: CompatibilityDescriptor::default(),
            state: ClientLifecycleState::Active,
            endpoints: ClientEndpointSet {
                rpc_address: format!("127.0.0.1:{}", 28_000 + epoch),
                segment_name: Some(SegmentName::new(format!("{stable_id}-segment"))),
                labels: [
                    (
                        "control_addr".to_string(),
                        format!("http://127.0.0.1:{}", 29_000 + epoch),
                    ),
                    ("route".to_string(), "true".to_string()),
                    ("storage".to_string(), "true".to_string()),
                ]
                .into_iter()
                .collect(),
            },
            expires_at_ms: u64::MAX,
        }
    }

    fn wait_for_http_task_state(address: &str, task_id: &str, expected: &str) -> String {
        for _ in 0..200 {
            let response = http_request(
                address,
                &format!(
                    "GET /v1/route-migrations/{task_id} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
                ),
            );
            if response.contains(expected) {
                return response;
            }
            sleep(Duration::from_millis(10));
        }
        panic!("task {task_id} did not reach {expected}");
    }

    fn sample_completed_move_route() -> ObjectRoute {
        ObjectRoute {
            key: ObjectKey::new("tenant-a::object-a"),
            namespace: Some(mooncake_store_core::NamespaceScope::with_defaults(
                Some("tenant-a"),
                None::<&str>,
                None::<&str>,
            )),
            logical_key: Some("object-a".to_string()),
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new("storage-b", ClientEpoch(1)),
                segment_name: SegmentName::new("segment-b"),
                offset: 64,
                segment_offset: 64,
                length: 12,
                checksum: None,
                tier: ReplicaTier::Nvme,
                priority: 1,
            }],
        }
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
        assert!(response.contains("supports redis://, rediss://, and etcd:// metadata only"));

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
    fn admin_http_server_returns_413_for_oversized_payloads() {
        let service = test_service();
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();
        let body = "x".repeat(16);
        let oversized_length = MAX_HTTP_BODY_BYTES + 1;
        let response = http_request(
            &address,
            &format!(
                "PUT /v1/tenant-policies/tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {oversized_length}\r\nConnection: close\r\n\r\n{body}",
            ),
        );
        assert!(response.contains("HTTP/1.1 413 Payload Too Large"));
        assert!(response.contains("request content is too large"));

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

    #[test]
    fn quota_scope_parser_accepts_reconcile_and_abort_paths() {
        let root = parse_quota_scope_path("/v1/tenant-quotas/tenant-a/reconcile")
            .expect("reconcile path should parse");
        assert_eq!(root.tenant, "tenant-a");
        assert!(root.domain.is_none());
        assert!(root.object_set.is_none());

        let nested = parse_quota_scope_path(
            "/v1/tenant-quotas/tenant-a/domain-a/set-a/reservations/res-1/abort",
        )
        .expect("abort path should parse");
        assert_eq!(nested.tenant, "tenant-a");
        assert_eq!(nested.domain.as_deref(), Some("domain-a"));
        assert_eq!(nested.object_set.as_deref(), Some("set-a"));

        let reservation_id = parse_quota_abort_path(
            "/v1/tenant-quotas/tenant-a/domain-a/set-a/reservations/res%2F1/abort",
            &PolicyScopePath {
                tenant: "tenant-a".to_string(),
                domain: Some("domain-a".to_string()),
                object_set: Some("set-a".to_string()),
            },
        )
        .expect("reservation id should decode");
        assert_eq!(reservation_id, "res/1");
    }

    #[test]
    fn admin_http_server_serves_quota_abort_endpoint() {
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
                reservation_id: "res-abort".to_string(),
                scope: mooncake_store_core::TenantPolicyScope::new(
                    "tenant-a",
                    None::<String>,
                    None::<String>,
                ),
                key: mooncake_store_core::ObjectKey::new("tenant-a::object-abort"),
                expected_object_version: None,
                delta_bytes: 3,
                delta_objects: 1,
                limit: TenantQuotaPolicy {
                    max_bytes: Some(1024),
                    max_objects: Some(16),
                },
                expires_at_ms: u64::MAX,
                created_at_ms: 8,
                writer_runtime: ClientRuntimeId::new("writer-b", ClientEpoch(1)),
            })
            .expect("pending reservation should store");

        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service.clone()).expect("server start");
        let address = server.address().to_string();
        let body = serde_json::json!({"dry_run": false}).to_string();
        let response = http_request(
            &address,
            &format!(
                "POST /v1/tenant-quotas/tenant-a/reservations/res-abort/abort HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"aborted\":true"));

        let reservations = service
            .list_tenant_quota_reservations("tenant-a", None, None, None)
            .expect("reservations should read");
        assert_eq!(
            reservations.reservations[0].state,
            mooncake_store_core::TenantQuotaReservationState::Aborted
        );

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_accepts_route_migration_tasks_and_reports_status() {
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![
                    Ok(MigrationExecutionProbe {
                        state: control_plane_pb::MigrationExecutionState::Running,
                        attempts: 1,
                        last_error: String::new(),
                    }),
                    Ok(MigrationExecutionProbe {
                        state: control_plane_pb::MigrationExecutionState::Succeeded,
                        attempts: 1,
                        last_error: String::new(),
                    }),
                ]
                .into(),
            )),
            route_results: Arc::new(Mutex::new(VecDeque::new())),
        });
        let service = test_migration_service(rpc);
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "source_segment": "segment-a",
            "target_segments": ["segment-b"],
            "task_executor": "executor-a"
        })
        .to_string();
        let submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/move HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(submit.contains("HTTP/1.1 200 OK"));
        assert!(submit.contains("\"task_id\":\"route-migration-1\""));

        let listed = http_request(
            &address,
            &format!(
                "GET /v1/route-migrations HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(listed.contains("\"count\":1"));
        let status =
            wait_for_http_task_state(&address, "route-migration-1", "\"state\":\"succeeded\"");
        assert!(status.contains("\"execution_id\":\"execution-1\""));
        assert!(status.contains("\"attempts\":1"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_route_migration_status_reflects_route_based_success() {
        let rpc = Arc::new(FakeMigrationRpc {
            submit_results: Arc::new(Mutex::new(vec![Ok("execution-1".to_string())].into())),
            status_results: Arc::new(Mutex::new(
                vec![Err(StoreError::Transport("executor lost".to_string()))].into(),
            )),
            route_results: Arc::new(Mutex::new(
                vec![Ok(Some(sample_completed_move_route()))].into(),
            )),
        });
        let service = test_migration_service(rpc);
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "source_segment": "segment-a",
            "target_segments": ["segment-b"],
            "task_executor": "executor-a"
        })
        .to_string();
        let submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/move HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(submit.contains("HTTP/1.1 200 OK"));

        let status =
            wait_for_http_task_state(&address, "route-migration-1", "\"state\":\"succeeded\"");
        assert!(!status.contains("\"state\":\"failed\""));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_route_migration_with_zero_max_retries() {
        let service = test_migration_service(Arc::new(FakeMigrationRpc::default()));
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "source_segment": "segment-a",
            "target_segments": ["segment-b"],
            "task_executor": "executor-a",
            "max_retries": 0
        })
        .to_string();
        let submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/move HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(submit.contains("HTTP/1.1 400 Bad Request"));
        assert!(submit.contains("max_retries"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_invalid_route_migration_target_shapes() {
        let service = test_migration_service(Arc::new(FakeMigrationRpc::default()));
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let copy_body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "source_segment": "segment-a",
            "target_segments": [],
            "task_executor": "executor-a"
        })
        .to_string();
        let copy_submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/copy HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                copy_body.len(),
                copy_body
            ),
        );
        assert!(copy_submit.contains("HTTP/1.1 400 Bad Request"));
        assert!(copy_submit.contains("target_segment"));

        let move_body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "source_segment": "segment-a",
            "target_segments": ["segment-b", "segment-c"],
            "task_executor": "executor-a"
        })
        .to_string();
        let move_submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/move HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                move_body.len(),
                move_body
            ),
        );
        assert!(move_submit.contains("HTTP/1.1 400 Bad Request"));
        assert!(move_submit.contains("exactly one target_segment"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_legacy_mode_field_in_route_migration_submit_body() {
        let service = test_migration_service(Arc::new(FakeMigrationRpc::default()));
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let copy_body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "mode": "copy",
            "source_segment": "segment-a",
            "target_segments": ["segment-b"],
            "task_executor": "executor-a"
        })
        .to_string();
        let copy_submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/copy HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                copy_body.len(),
                copy_body
            ),
        );
        assert!(copy_submit.contains("HTTP/1.1 400 Bad Request"));
        assert!(copy_submit.contains("unknown field `mode`"));

        let move_body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "mode": "move",
            "source_segment": "segment-a",
            "target_segments": ["segment-b"],
            "task_executor": "executor-a"
        })
        .to_string();
        let move_submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/move HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                move_body.len(),
                move_body
            ),
        );
        assert!(move_submit.contains("HTTP/1.1 400 Bad Request"));
        assert!(move_submit.contains("unknown field `mode`"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_unknown_route_migration_submit_fields() {
        let service = test_migration_service(Arc::new(FakeMigrationRpc::default()));
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "source_segment": "segment-a",
            "target_segments": ["segment-b"],
            "task_executor": "executor-a",
            "unexpected": "surprise"
        })
        .to_string();
        let submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/move HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(submit.contains("HTTP/1.1 400 Bad Request"));
        assert!(submit.contains("unknown field `unexpected`"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_legacy_unified_route_migration_submit_endpoint() {
        let service = test_migration_service(Arc::new(FakeMigrationRpc::default()));
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let body = serde_json::json!({
            "authority": "authority-a",
            "tenant": "tenant-a",
            "key": "object-a",
            "source_segment": "segment-a",
            "target_segments": ["segment-b"],
            "task_executor": "executor-a"
        })
        .to_string();
        let submit = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(submit.contains("HTTP/1.1 404 Not Found"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_unknown_route_migration_task_id() {
        let service = test_migration_service(Arc::new(FakeMigrationRpc::default()));
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "GET /v1/route-migrations/missing-task HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 404 Not Found"));
        assert!(response.contains("missing-task"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_non_get_route_migration_detail_requests() {
        let service = test_migration_service(Arc::new(FakeMigrationRpc::default()));
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "POST /v1/route-migrations/route-migration-1 HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 405 Method Not Allowed"));
        assert!(response.contains("method not allowed"));

        server.shutdown().expect("server shutdown");
    }
}
