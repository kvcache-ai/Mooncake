use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use mooncake_store_core::StoreError;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::models::{
    ColdTierDisableRequest, ColdTierDrainRequest, ColdTierEnableRequest, ColdTierRegisterRequest,
    ColdTierUnregisterRequest, CreateColdTierDeviceRequest, ErrorResponse, PutTenantPolicyRequest,
    RouteMigrationMode, RouteMigrationTaskSubmitRequest, TenantQuotaAbortRequest,
    TenantQuotaReconcileRequest, TracingAction, TracingUpdateRequest,
    TriggerColdTierOffloadRequest,
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
        Self::start_with_auth(bind_addr, service, None)
    }

    pub fn start_with_auth(
        bind_addr: &str,
        service: AdminService,
        auth_token: Option<String>,
    ) -> mooncake_store_core::Result<Self> {
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
        let auth_token = auth_token.map(Arc::new);
        let (shutdown, shutdown_rx) = mpsc::channel();
        let thread = thread::Builder::new()
            .name(format!("mooncake-store-admin-{address}"))
            .spawn(move || run_admin_http_server(listener, shutdown_rx, service, auth_token))
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

fn run_admin_http_server(
    listener: TcpListener,
    shutdown_rx: Receiver<()>,
    service: AdminService,
    auth_token: Option<Arc<String>>,
) {
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                handle_admin_http_connection(stream, &service, auth_token.as_deref())
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if shutdown_rx.recv_timeout(HTTP_POLL_INTERVAL).is_ok() {
                    return;
                }
            }
            Err(_) => return,
        }
    }
}

fn handle_admin_http_connection(
    mut stream: TcpStream,
    service: &AdminService,
    auth_token: Option<&String>,
) {
    if stream.set_read_timeout(Some(HTTP_READ_TIMEOUT)).is_err() {
        return;
    }
    let response = match read_http_request(&mut stream) {
        Ok(request) => route_request(service, request, auth_token),
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
    headers: Vec<(String, String)>,
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

fn route_request(
    service: &AdminService,
    request: HttpRequest,
    auth_token: Option<&String>,
) -> String {
    let path_only = request.path.split('?').next().unwrap_or("/").to_string();
    if matches!(
        (request.method.as_str(), path_only.as_str()),
        ("GET", "/healthz") | ("GET", "/livez")
    ) {
        return http_text_response("200 OK", "ok\n");
    }
    if let Some(token) = auth_token {
        if !request_has_bearer_token(&request, token) {
            return http_error_response(
                "401 Unauthorized",
                "missing or invalid admin authorization token",
            );
        }
    }
    match (request.method.as_str(), path_only.as_str()) {
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
        ("GET", "/v1/tracing") => {
            match service.update_tracing(TracingAction::Status, TracingUpdateRequest::default()) {
                Ok(report) => http_json_response("200 OK", &report),
                Err(error) => http_store_error(error),
            }
        }
        ("POST", "/v1/tracing/on") => {
            update_tracing_request(service, &request.body, TracingAction::On)
        }
        ("POST", "/v1/tracing/off") => {
            update_tracing_request(service, &request.body, TracingAction::Off)
        }
        ("POST", "/v1/tracing/flush") => {
            update_tracing_request(service, &request.body, TracingAction::Flush)
        }
        ("GET", "/v1/route-migrations") => {
            http_json_response("200 OK", &service.list_route_migration_tasks())
        }
        ("POST", "/v1/cold-tier/devices") => {
            create_cold_tier_device(service, &request.path, &request.body)
        }
        ("GET", "/v1/cold-tier/devices") => list_cold_tier_devices(service, &request.path),
        ("POST", "/v1/cold-tier/offloads/trigger") => {
            trigger_cold_tier_offload(service, &request.body)
        }
        ("GET", "/v1/cold-tier/offloads") => {
            http_json_response("200 OK", &service.list_cold_tier_offload_tasks())
        }
        ("GET", "/v1/cold-tier/offloads/trigger") => {
            http_error_response("405 Method Not Allowed", "method not allowed")
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

fn update_tracing_request(service: &AdminService, body: &[u8], action: TracingAction) -> String {
    let request = if body.is_empty() {
        TracingUpdateRequest::default()
    } else {
        match serde_json::from_slice::<TracingUpdateRequest>(body) {
            Ok(request) => request,
            Err(error) => {
                return http_error_response(
                    "400 Bad Request",
                    &format!("invalid JSON body: {error}"),
                )
            }
        }
    };
    match service.update_tracing(action, request) {
        Ok(report) => http_json_response("200 OK", &report),
        Err(error) => http_store_error(error),
    }
}

include!("cold_tier_http.rs");

fn submit_route_migration_request(
    service: &AdminService,
    body: &[u8],
    expected_mode: RouteMigrationMode,
) -> String {
    let payload = match serde_json::from_slice::<RouteMigrationTaskSubmitRequest>(body) {
        Ok(payload) => payload,
        Err(error) => {
            return http_error_response("400 Bad Request", &format!("invalid JSON body: {error}"));
        }
    };
    match service.submit_route_migration_task(expected_mode, payload) {
        Ok(response) => http_json_response("200 OK", &response),
        Err(error) => http_store_error(error),
    }
}

fn request_has_bearer_token(request: &HttpRequest, expected: &str) -> bool {
    let Some(value) = request
        .headers
        .iter()
        .find_map(|(name, value)| name.eq_ignore_ascii_case("authorization").then_some(value))
    else {
        return false;
    };
    let Some(token) = value.trim().strip_prefix("Bearer ") else {
        return false;
    };
    token == expected
}

fn route_scoped_request(service: &AdminService, request: HttpRequest, path_only: &str) -> String {
    if path_only.starts_with("/v1/cold-tier/devices/") {
        return route_cold_tier_device_request(service, request, path_only);
    }
    if path_only.starts_with("/v1/cold-tier/objects/") {
        return route_cold_tier_object_request(service, request, path_only);
    }
    if path_only == "/v1/debug/routes" || path_only.starts_with("/v1/debug/routes/") {
        return route_debug_route_request(service, request, path_only);
    }
    if path_only.starts_with("/v1/cold-tier/offloads/") {
        return route_cold_tier_offload_request(service, request, path_only);
    }
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

fn route_debug_route_request(
    service: &AdminService,
    request: HttpRequest,
    path_only: &str,
) -> String {
    if request.method != "GET" {
        return http_error_response("405 Method Not Allowed", "method not allowed");
    }
    if path_only == "/v1/debug/routes" {
        return match service.list_debug_routes(query_param(&request.path, "tenant").as_deref()) {
            Ok(response) => http_json_response("200 OK", &response),
            Err(error) => http_store_error(error),
        };
    }
    let Some(encoded_key) = path_only.strip_prefix("/v1/debug/routes/") else {
        return http_error_response("404 Not Found", "not found");
    };
    let Some(key) = percent_decode_component(encoded_key) else {
        return http_error_response("400 Bad Request", "invalid object key encoding");
    };
    match service.get_debug_route(query_param(&request.path, "tenant").as_deref(), &key) {
        Ok(Some(response)) => http_json_response("200 OK", &response),
        Ok(None) => http_error_response("404 Not Found", &format!("route {key} not found")),
        Err(error) => http_store_error(error),
    }
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
                    );
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
    let mut header_lines = header_text.lines();
    let first_line = header_lines.next().unwrap_or_default();
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or("/").to_string();
    let headers = header_lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_string(), value.trim().to_string()))
        })
        .collect();
    let body_start = usize::min(request.len(), header_end.saturating_add(4));
    let body_end = usize::min(request.len(), body_start.saturating_add(content_length));
    let body = request[body_start..body_end].to_vec();
    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
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

fn parse_json_body<T: Default + DeserializeOwned>(body: &[u8]) -> Result<T, String> {
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice::<T>(body).map_err(|error| {
        http_error_response("400 Bad Request", &format!("invalid JSON body: {error}"))
    })
}

fn http_store_error(error: StoreError) -> String {
    match error {
        StoreError::Conflict(message)
        | StoreError::QuotaExceeded { message, .. }
        | StoreError::StaleEpoch(message) => http_error_response("409 Conflict", &message),
        StoreError::NotFound(message) => http_error_response("404 Not Found", &message),
        StoreError::Unsupported(message) => http_error_response("501 Not Implemented", &message),
        StoreError::InvalidState(message) => http_error_response("400 Bad Request", &message),
        StoreError::Backpressure(message) => http_error_response("429 Too Many Requests", &message),
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
    use std::fs;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use mooncake_metadata::{InMemoryMetadataBackend, MetadataKeyspace};
    use mooncake_store_client::control_plane_pb;
    use mooncake_store_core::{
        scoped_object_key, ClientEndpointSet, ClientEpoch, ClientLease, ClientLifecycleState,
        ClientRuntimeId, ColdBackingRoute, ColdBackingState, ColdTierDeviceRecord,
        ColdTierDeviceState, ColdTierTargetSpec, CompatibilityDescriptor, MetadataBackend,
        ObjectKey, ObjectRoute, ReplicaRoute, ReplicaTier, RouteState, RouteVersion, SegmentName,
        StoreError, TenantObjectAccountingState, TenantPolicySpec, TenantQuotaPolicy,
        TenantQuotaReservationRequest, TenantRoutePolicy,
    };
    use parking_lot::Mutex;

    use crate::admin::models::PolicyPatchInput;
    use crate::admin::models::TriggerColdTierOffloadRequest;
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

    fn test_service_with_backend(backend: Arc<dyn MetadataBackend>) -> AdminService {
        AdminService::new(backend, "memory://test", MetadataKeyspace::default())
    }

    fn bearer(token: &str) -> String {
        format!("Authorization: Bearer {token}\r\n")
    }

    fn encode_test_cold_tier_path_component(value: &str) -> String {
        if value.is_empty() {
            return "~".to_string();
        }
        let mut component = String::with_capacity(value.len());
        for byte in value.as_bytes() {
            if byte.is_ascii_alphanumeric() || matches!(*byte, b'.' | b'-') {
                component.push(*byte as char);
            } else {
                component.push('~');
                component.push_str(&format!("{byte:02X}"));
            }
        }
        component
    }

    fn sample_cold_tier_device(device_id: &str) -> ColdTierDeviceRecord {
        ColdTierDeviceRecord {
            device_id: device_id.to_string(),
            stable_id: "storage-a".to_string(),
            epoch: Some(1),
            cold_tier_id: device_id.to_string(),
            kind: "ssd".to_string(),
            target: ColdTierTargetSpec::Directory {
                path: format!("/tmp/{device_id}"),
            },
            root_dir: Some(format!("/tmp/{device_id}")),
            state: ColdTierDeviceState::Healthy,
            capacity_bytes: Some(1024),
            used_bytes: 0,
            reserved_bytes: 0,
            failure_count: 0,
            last_error: None,
            tags: Vec::new(),
            updated_at_ms: 1,
        }
    }

    #[derive(Clone, Default)]
    struct FakeMigrationRpc {
        submit_results: Arc<Mutex<VecDeque<mooncake_store_core::Result<String>>>>,
        status_results: Arc<Mutex<VecDeque<mooncake_store_core::Result<MigrationExecutionProbe>>>>,
        route_results: Arc<Mutex<VecDeque<mooncake_store_core::Result<Option<ObjectRoute>>>>>,
        offload_results: Arc<Mutex<VecDeque<mooncake_store_core::Result<u64>>>>,
        offload_calls: Arc<AtomicUsize>,
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

        fn trigger_cold_tier_offload(
            &self,
            _lease: &ClientLease,
            _max_tasks: u64,
        ) -> mooncake_store_core::Result<u64> {
            self.offload_calls.fetch_add(1, Ordering::SeqCst);
            self.offload_results.lock().pop_front().unwrap_or(Ok(0))
        }

        fn manual_cold_tier_gc(
            &self,
            _lease: &ClientLease,
            _device_id: &str,
            _max_backings: u64,
        ) -> mooncake_store_core::Result<u64> {
            Ok(0)
        }

        fn manual_cold_tier_free(
            &self,
            _lease: &ClientLease,
            _device_id: &str,
            _max_victims: u64,
        ) -> mooncake_store_core::Result<control_plane_pb::ManualColdTierFreeReply> {
            Ok(control_plane_pb::ManualColdTierFreeReply {
                attempted_victims: 0,
                freed_backings: 0,
                skipped_backings: 0,
                reached_low_watermark: true,
                error: None,
                collected_backings: 0,
            })
        }

        fn probe_cold_tier_device(
            &self,
            _lease: &ClientLease,
            device_id: &str,
        ) -> mooncake_store_core::Result<control_plane_pb::ProbeColdTierDeviceReply> {
            Ok(control_plane_pb::ProbeColdTierDeviceReply {
                device_id: device_id.to_string(),
                capacity_bytes: 0,
                used_bytes: 0,
                reserved_bytes: 0,
                schedulable: true,
                state: "Healthy".to_string(),
                last_error: String::new(),
                error: None,
            })
        }
    }

    fn test_migration_service(rpc: Arc<dyn MigrationRpc>) -> AdminService {
        test_migration_service_with_stable_id(rpc, "executor-a")
    }

    fn test_migration_service_with_stable_id(
        rpc: Arc<dyn MigrationRpc>,
        stable_id: &str,
    ) -> AdminService {
        let backend: Arc<dyn MetadataBackend> = Arc::new(InMemoryMetadataBackend::new());
        backend
            .upsert_client_lease(&live_lease("authority-a", 1))
            .expect("authority lease should store");
        backend
            .upsert_client_lease(&live_lease(stable_id, 1))
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
                offset: Some(64),
                segment_offset: 64,
                length: 12,
                checksum: None,
                tier: ReplicaTier::Nvme,
                priority: 1,
            }],
            cold_backing: None,
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
    fn admin_http_auth_token_protects_non_health_requests() {
        let service = test_service();
        let mut server = AdminHttpServerHandle::start_with_auth(
            "127.0.0.1:0",
            service.clone(),
            Some("secret-token".to_string()),
        )
        .expect("server start");
        let address = server.address().to_string();

        let health = http_request(
            &address,
            &format!("GET /healthz HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"),
        );
        assert!(health.contains("HTTP/1.1 200 OK"));

        let unauthorized = http_request(
            &address,
            &format!(
                "GET /v1/tenant-policies HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(unauthorized.contains("HTTP/1.1 401 Unauthorized"));

        let authorized = http_request(
            &address,
            &format!(
                "GET /v1/tenant-policies HTTP/1.1\r\nHost: {address}\r\n{}Connection: close\r\n\r\n",
                bearer("secret-token")
            ),
        );
        assert!(authorized.contains("HTTP/1.1 200 OK"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_auth_token_rejects_wrong_token() {
        let service = test_service();
        let mut server = AdminHttpServerHandle::start_with_auth(
            "127.0.0.1:0",
            service,
            Some("secret-token".to_string()),
        )
        .expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "GET /v1/tenant-policies HTTP/1.1\r\nHost: {address}\r\n{}Connection: close\r\n\r\n",
                bearer("wrong-token")
            ),
        );
        assert!(response.contains("HTTP/1.1 401 Unauthorized"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_serves_cold_tier_device_lifecycle() {
        let service = test_service();
        service
            .backend()
            .upsert_client_lease(&live_lease("store-a", 7))
            .expect("store lease should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();
        let root = std::env::temp_dir().join(format!(
            "mooncake-cold-tier-http-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);

        let body = serde_json::json!({
            "stable_id": "store-a",
            "cold_tier_id": "ssd-0",
            "kind": "ssd",
            "target": {"type": "directory", "path": root.to_string_lossy()},
            "capacity_override_bytes": 1024,
            "tags": ["local", "ssd"]
        })
        .to_string();
        let create = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(create.contains("HTTP/1.1 201 Created"));
        assert!(create.contains("\"cold_tier_id\":\"ssd-0\""));
        assert!(create.contains("\"state\":\"unregistered\""));
        let device_id = create
            .split("\"device_id\":\"")
            .nth(1)
            .and_then(|rest| rest.split('"').next())
            .expect("device_id should be present")
            .to_string();

        let register = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/{device_id}/register HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                "{}"
            ),
        );
        assert!(register.contains("HTTP/1.1 201 Created"));
        assert!(register.contains("\"epoch\":7"));
        assert!(register.contains("\"state\":\"healthy\""));
        assert!(register.contains("\"schedulable\":true"));

        let list = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices?stable_id=store-a&state=healthy&schedulable=true HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(list.contains("HTTP/1.1 200 OK"));
        assert!(list.contains("\"devices\":["));
        assert!(list.contains(&device_id));

        let bad_state = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices?state=bad HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(bad_state.contains("HTTP/1.1 400 Bad Request"));

        let bad_schedulable = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices?schedulable=maybe HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(bad_schedulable.contains("HTTP/1.1 400 Bad Request"));
        assert!(bad_schedulable.contains("invalid schedulable filter"));

        let disable = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/{device_id}/disable HTTP/1.1\r\nHost: {address}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                "{}"
            ),
        );
        assert!(disable.contains("\"state\":\"disabled_by_admin\""));
        assert!(disable.contains("\"schedulable\":false"));

        let enable = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/{device_id}/enable HTTP/1.1\r\nHost: {address}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                "{}"
            ),
        );
        assert!(enable.contains("\"state\":\"healthy\""));

        let unregister = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/{device_id}/unregister HTTP/1.1\r\nHost: {address}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                "{}"
            ),
        );
        assert!(unregister.contains("HTTP/1.1 200 OK"));
        assert!(unregister.contains("\"state\":\"unregistered\""));

        server.shutdown().expect("server shutdown");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn admin_http_server_reports_cold_tier_orphan_quarantine() {
        let service = test_service();
        let root = std::env::temp_dir().join(format!(
            "mooncake-cold-tier-http-quarantine-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let encoded = encode_test_cold_tier_path_component("ssd/quarantine");
        let final_quarantine = root.join(&encoded).join("__orphan_quarantine__");
        let pending_quarantine = root
            .join("__pending__")
            .join(&encoded)
            .join("__orphan_quarantine__");
        fs::create_dir_all(&final_quarantine).expect("final quarantine should exist");
        fs::create_dir_all(&pending_quarantine).expect("pending quarantine should exist");
        fs::write(final_quarantine.join("final.bin"), b"final").expect("final file should write");
        fs::write(pending_quarantine.join("pending.bin"), b"pending")
            .expect("pending file should write");
        fs::create_dir_all(final_quarantine.join("nested.bin"))
            .expect("nested directory should write");
        service
            .backend()
            .put_cold_tier_device_if_absent(&ColdTierDeviceRecord {
                device_id: "ssd/quarantine".to_string(),
                stable_id: "store-a".to_string(),
                epoch: None,
                cold_tier_id: "ssd/quarantine".to_string(),
                kind: "ssd".to_string(),
                target: ColdTierTargetSpec::Directory {
                    path: root.to_string_lossy().into_owned(),
                },
                root_dir: None,
                state: ColdTierDeviceState::Healthy,
                capacity_bytes: None,
                used_bytes: 0,
                reserved_bytes: 0,
                failure_count: 0,
                last_error: None,
                tags: Vec::new(),
                updated_at_ms: 1,
            })
            .expect("device should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd%2Fquarantine/orphan-quarantine HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"device_id\":\"ssd/quarantine\""));
        assert!(response.contains("\"cold_tier_id\":\"ssd/quarantine\""));
        assert!(response.contains("\"count\":2"));
        assert!(response.contains("\"total_bytes\":12"));
        assert!(response.contains("\"area\":\"final\""));
        assert!(response.contains("\"file_name\":\"final.bin\""));
        assert!(response.contains("\"area\":\"pending\""));
        assert!(response.contains("\"file_name\":\"pending.bin\""));
        assert!(!response.contains("nested.bin"));

        let wrong_method = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd%2Fquarantine/orphan-quarantine HTTP/1.1\r\nHost: {address}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
            ),
        );
        assert!(wrong_method.contains("HTTP/1.1 405 Method Not Allowed"));

        server.shutdown().expect("server shutdown");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn admin_http_server_uses_dot_segment_safe_cold_tier_quarantine_paths() {
        let service = test_service();
        let root = std::env::temp_dir().join(format!(
            "mooncake-cold-tier-http-dot-quarantine-test-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        let safe_quarantine = root.join("~2E").join("__orphan_quarantine__");
        let unsafe_quarantine = root.join("__orphan_quarantine__");
        fs::create_dir_all(&safe_quarantine).expect("safe quarantine should exist");
        fs::create_dir_all(&unsafe_quarantine).expect("unsafe quarantine should exist");
        fs::write(safe_quarantine.join("safe.bin"), b"safe").expect("safe file should write");
        fs::write(unsafe_quarantine.join("unsafe.bin"), b"unsafe")
            .expect("unsafe file should write");
        service
            .backend()
            .put_cold_tier_device_if_absent(&ColdTierDeviceRecord {
                device_id: ".".to_string(),
                stable_id: "store-a".to_string(),
                epoch: None,
                cold_tier_id: ".".to_string(),
                kind: "ssd".to_string(),
                target: ColdTierTargetSpec::Directory {
                    path: root.to_string_lossy().into_owned(),
                },
                root_dir: None,
                state: ColdTierDeviceState::Healthy,
                capacity_bytes: None,
                used_bytes: 0,
                reserved_bytes: 0,
                failure_count: 0,
                last_error: None,
                tags: Vec::new(),
                updated_at_ms: 1,
            })
            .expect("device should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/%2E/orphan-quarantine HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"device_id\":\".\""));
        assert!(response.contains("\"count\":1"));
        assert!(response.contains("\"file_name\":\"safe.bin\""));
        assert!(response.contains("/~2E/__orphan_quarantine__/safe.bin"));
        assert!(!response.contains("unsafe.bin"));

        server.shutdown().expect("server shutdown");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn admin_http_server_handles_multiple_cold_tier_devices_for_one_runtime() {
        let backend: Arc<dyn MetadataBackend> =
            Arc::new(InMemoryMetadataBackend::new_hard_isolated());
        let tenant_backend = backend.for_tenant("tenant-a").expect("tenant backend");
        let service = test_service_with_backend(backend);
        tenant_backend
            .upsert_client_lease(&live_lease("store-a", 7))
            .expect("store lease should store");
        let mut route = ObjectRoute {
            key: ObjectKey::new("cold-object-a"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new("store-a", ClientEpoch(7)),
                segment_name: SegmentName::new("hot-segment-a"),
                offset: None,
                segment_offset: 0,
                length: 12,
                checksum: Some(99),
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: Some(ColdBackingRoute {
                owner: ClientRuntimeId::new("store-a", ClientEpoch(7)),
                cold_tier_id: "ssd-a".to_string(),
                object_locator: "objects/a.bin".to_string(),
                length: 12,
                checksum: Some(99),
                state: ColdBackingState::Materialized,
                replicas: Vec::new(),
            }),
        };
        tenant_backend
            .compare_and_swap_object_route(&route.key, None, Some(&route))
            .expect("route should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service.clone()).expect("server start");
        let address = server.address().to_string();
        let first_root = std::env::temp_dir().join(format!(
            "mooncake-cold-tier-http-multi-a-{}",
            std::process::id()
        ));
        let second_root = std::env::temp_dir().join(format!(
            "mooncake-cold-tier-http-multi-b-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&first_root);
        let _ = fs::remove_dir_all(&second_root);
        fs::create_dir_all(&first_root).expect("first cold root should exist");
        fs::create_dir_all(&second_root).expect("second cold root should exist");

        for (cold_tier_id, root) in [("ssd-a", &first_root), ("ssd-b", &second_root)] {
            let body = serde_json::json!({
                "stable_id": "store-a",
                "cold_tier_id": cold_tier_id,
                "kind": "ssd",
                "target": {"type": "directory", "path": root.to_string_lossy()}
            })
            .to_string();
            let create = http_request(
                &address,
                &format!(
                    "POST /v1/cold-tier/devices?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                ),
            );
            assert!(create.contains("HTTP/1.1 201 Created"));
            assert!(create.contains(&format!("\"cold_tier_id\":\"{cold_tier_id}\"")));
            let register = http_request(
                &address,
                &format!(
                    "POST /v1/cold-tier/devices/{cold_tier_id}/register?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
                ),
            );
            assert!(register.contains("HTTP/1.1 201 Created"));
            assert!(register.contains("\"state\":\"healthy\""));
        }

        let list = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices?tenant=tenant-a&stable_id=store-a&state=healthy&schedulable=true HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(list.contains("HTTP/1.1 200 OK"));
        assert!(list.contains("\"cold_tier_id\":\"ssd-a\""));
        assert!(list.contains("\"cold_tier_id\":\"ssd-b\""));

        let root_list = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices?stable_id=store-a&state=healthy&schedulable=true HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(root_list.contains("HTTP/1.1 200 OK"));
        assert!(root_list.contains("\"devices\":[]"));

        let dry_run = serde_json::json!({"dry_run": true}).to_string();
        let first_dry_run = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-a/unregister?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                dry_run.len(),
                dry_run
            ),
        );
        assert!(first_dry_run.contains("HTTP/1.1 200 OK"));
        assert!(first_dry_run.contains("\"dry_run\":true"));
        assert!(first_dry_run.contains("\"state\":\"healthy\""));
        assert!(first_dry_run.contains("\"blocked_objects\":1"));
        let first_after_dry_run = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-a?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(first_after_dry_run.contains("\"state\":\"healthy\""));

        let first_drain = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-a/unregister?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}"
            ),
        );
        assert!(first_drain.contains("HTTP/1.1 200 OK"));
        assert!(first_drain.contains("\"dry_run\":false"));
        assert!(first_drain.contains("\"state\":\"draining\""));
        assert!(first_drain.contains("\"blocked_objects\":1"));
        let second_after_drain = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-b?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(second_after_drain.contains("HTTP/1.1 200 OK"));
        assert!(second_after_drain.contains("\"state\":\"healthy\""));
        assert!(second_after_drain.contains("\"schedulable\":true"));

        let force_drain = serde_json::json!({"force": true}).to_string();
        let first_force = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-a/unregister?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                force_drain.len(),
                force_drain
            ),
        );
        assert!(first_force.contains("HTTP/1.1 200 OK"));
        assert!(first_force.contains("\"state\":\"unregistered\""));
        let second_after_force = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-b?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(second_after_force.contains("HTTP/1.1 200 OK"));
        assert!(second_after_force.contains("\"state\":\"healthy\""));
        assert!(second_after_force.contains("\"schedulable\":true"));
        route.version = route.version.next();
        let observed = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/cold-object-a?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(observed.contains("\"state\":\"pending_delete\""));

        let cold_only_route = ObjectRoute {
            key: ObjectKey::new("cold-only-object"),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: Some(ColdBackingRoute {
                owner: ClientRuntimeId::new("store-a", ClientEpoch(7)),
                cold_tier_id: "ssd-b".to_string(),
                object_locator: "objects/cold-only.bin".to_string(),
                length: 12,
                checksum: None,
                state: ColdBackingState::Materialized,
                replicas: Vec::new(),
            }),
        };
        tenant_backend
            .compare_and_swap_object_route(&cold_only_route.key, None, Some(&cold_only_route))
            .expect("cold-only route should store");
        let force_cold_only = serde_json::json!({"force": true}).to_string();
        let cold_only_force = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-b/unregister?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                force_cold_only.len(),
                force_cold_only
            ),
        );
        assert!(cold_only_force.contains("HTTP/1.1 409 Conflict"));
        assert!(cold_only_force.contains("reclaimable"));
        let second_after_rejected_force = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-b?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(second_after_rejected_force.contains("\"state\":\"healthy\""));

        server.shutdown().expect("server shutdown");
        let _ = fs::remove_dir_all(first_root);
        let _ = fs::remove_dir_all(second_root);
    }

    #[test]
    fn admin_http_server_uses_shared_in_memory_backend_for_cold_tier_queries() {
        let shared = Arc::new(InMemoryMetadataBackend::new());
        let runtime_backend = shared
            .for_tenant("tenant-a")
            .expect("tenant view should exist");
        runtime_backend
            .upsert_client_lease(&live_lease("store-a", 7))
            .expect("store lease should store");
        runtime_backend
            .put_cold_tier_device_if_absent(&ColdTierDeviceRecord {
                device_id: "ssd-shared".to_string(),
                stable_id: "store-a".to_string(),
                epoch: Some(7),
                cold_tier_id: "ssd-shared".to_string(),
                kind: "ssd".to_string(),
                target: ColdTierTargetSpec::Directory {
                    path: "/tmp/shared-cold-root".to_string(),
                },
                root_dir: Some("/tmp/shared-cold-root".to_string()),
                state: ColdTierDeviceState::Healthy,
                capacity_bytes: Some(4096),
                used_bytes: 64,
                reserved_bytes: 0,
                failure_count: 0,
                last_error: None,
                tags: vec!["shared".to_string()],
                updated_at_ms: 1,
            })
            .expect("device should store in tenant view");

        let service = test_service_with_backend(runtime_backend.clone());
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();
        let response = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-shared?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"device_id\":\"ssd-shared\""));
        assert!(response.contains("\"stable_id\":\"store-a\""));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_triggers_cold_tier_offload() {
        let rpc = Arc::new(FakeMigrationRpc {
            offload_results: Arc::new(Mutex::new(vec![Ok(3), Ok(0)].into())),
            ..FakeMigrationRpc::default()
        });
        let service = test_migration_service_with_stable_id(rpc.clone(), "storage-a");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();
        let body = serde_json::to_string(&TriggerColdTierOffloadRequest {
            stable_id: "storage-a".to_string(),
            max_tasks: Some(7),
        })
        .expect("request should serialize");

        let response = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/offloads/trigger HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(response.contains("HTTP/1.1 202 Accepted"));
        assert!(response.contains("\"task_id\":\"cold-tier-offload-1\""));
        assert!(response.contains("\"stable_id\":\"storage-a\""));
        assert!(response.contains("\"max_tasks\":7"));

        let started = std::time::Instant::now();
        let mut status = String::new();
        while started.elapsed() < Duration::from_secs(5) {
            status = http_request(
                &address,
                &format!(
                    "GET /v1/cold-tier/offloads/cold-tier-offload-1 HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
                ),
            );
            if status.contains("\"state\":\"succeeded\"") {
                break;
            }
            sleep(Duration::from_millis(10));
        }
        assert!(status.contains("HTTP/1.1 200 OK"));
        assert!(status.contains("\"state\":\"succeeded\""));
        assert!(status.contains("\"materialized\":3"));
        assert!(rpc.offload_calls.load(Ordering::SeqCst) >= 1);

        let list = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/offloads HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(list.contains("HTTP/1.1 200 OK"));
        assert!(list.contains("\"count\":1"));
        assert!(list.contains("cold-tier-offload-1"));

        let wrong_method = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/offloads/trigger HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(wrong_method.contains("HTTP/1.1 405 Method Not Allowed"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_keeps_async_cold_tier_drain_in_draining_state() {
        let rpc = Arc::new(FakeMigrationRpc::default());
        let service = test_migration_service(rpc);
        service
            .backend()
            .put_cold_tier_device_if_absent(&sample_cold_tier_device("ssd-async-drain"))
            .expect("device should seed");
        service
            .backend()
            .compare_and_swap_object_route(
                &ObjectKey::new("drain-object"),
                None,
                Some(&ObjectRoute {
                    key: ObjectKey::new("drain-object"),
                    namespace: None,
                    logical_key: None,
                    canonical_key: None,
                    sharing_scope: None,
                    qos_tier: None,
                    version: RouteVersion(1),
                    state: RouteState::Active,
                    compatibility: CompatibilityDescriptor::default(),
                    replicas: Vec::new(),
                    cold_backing: Some(ColdBackingRoute {
                        owner: ClientRuntimeId::new("storage-a", ClientEpoch(1)),
                        cold_tier_id: "ssd-async-drain".to_string(),
                        object_locator: "objects/drain-object.bin".to_string(),
                        length: 12,
                        checksum: None,
                        state: ColdBackingState::Materialized,
                        replicas: Vec::new(),
                    }),
                }),
            )
            .expect("route should seed");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();
        let body = serde_json::json!({
            "migration_task_executor": "executor-a",
            "migration_target_segments": ["segment-b"],
            "migration_max_retries": 3
        })
        .to_string();

        let drain = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-async-drain/drain HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            ),
        );
        assert!(drain.contains("HTTP/1.1 200 OK"));
        assert!(drain.contains("\"state\":\"draining\""));
        assert!(drain.contains("\"blocked_objects\":1"));
        assert!(drain.contains("\"reclaimable_objects\":0"));
        assert!(drain.contains("\"marked_pending_delete\":0"));
        assert!(drain.contains("\"collected_pending_delete\":0"));
        assert!(drain.contains("\"migration_tasks_submitted\":1"));

        let device = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-async-drain HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(device.contains("HTTP/1.1 200 OK"));
        assert!(device.contains("\"state\":\"draining\""));

        let object = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/drain-object HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(object.contains("HTTP/1.1 200 OK"));
        assert!(object.contains("\"state\":\"materialized\""));
        assert!(object.contains("\"locator\":\"objects/drain-object.bin\""));

        let tasks = http_request(
            &address,
            &format!(
                "GET /v1/route-migrations HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(tasks.contains("HTTP/1.1 200 OK"));
        assert!(tasks.contains("\"count\":1"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_serves_debug_route_dump() {
        let service = test_service();
        let key = ObjectKey::new("tenant-a/default/default/object-a");
        let route = ObjectRoute {
            key: key.clone(),
            namespace: Some(mooncake_store_core::NamespaceScope::with_defaults(
                Some("tenant-a"),
                Some("default"),
                Some("default"),
            )),
            logical_key: Some("object-a".to_string()),
            canonical_key: Some("tenant-a/default/default/object-a".to_string()),
            sharing_scope: Some("tenant-a".to_string()),
            qos_tier: Some("default".to_string()),
            version: RouteVersion(7),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: vec![ReplicaRoute {
                owner: ClientRuntimeId::new("store-a", ClientEpoch(7)),
                segment_name: SegmentName::new("segment-a"),
                offset: None,
                segment_offset: 12,
                length: 34,
                checksum: Some(56),
                tier: ReplicaTier::Dram,
                priority: 0,
            }],
            cold_backing: Some(ColdBackingRoute {
                owner: ClientRuntimeId::new("store-a", ClientEpoch(7)),
                cold_tier_id: "device-a".to_string(),
                object_locator: "objects/ab/object-a.bin".to_string(),
                length: 34,
                checksum: Some(56),
                state: ColdBackingState::Materialized,
                replicas: Vec::new(),
            }),
        };
        service
            .backend()
            .put_cold_tier_device_if_absent(&sample_cold_tier_device("device-a"))
            .expect("device should seed");
        service
            .backend()
            .compare_and_swap_object_route(&key, None, Some(&route))
            .expect("route should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let list = http_request(
            &address,
            &format!(
                "GET /v1/debug/routes?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(list.contains("HTTP/1.1 200 OK"));
        assert!(list.contains("\"count\":1"));
        assert!(list.contains("\"key\":\"tenant-a/default/default/object-a\""));
        assert!(list.contains("\"logical_key\":\"object-a\""));
        assert!(list.contains("\"tenant\":\"tenant-a\""));
        assert!(list.contains("\"domain\":\"default\""));
        assert!(list.contains("\"object_set\":\"default\""));
        assert!(list.contains("\"owner_stable_id\":\"store-a\""));
        assert!(list.contains("\"cold_tier_id\":\"device-a\""));

        let detail = http_request(
            &address,
            &format!(
                "GET /v1/debug/routes/tenant-a%2Fdefault%2Fdefault%2Fobject-a?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(detail.contains("HTTP/1.1 200 OK"));
        assert!(detail.contains("\"version\":7"));
        assert!(detail.contains("\"segment_name\":\"segment-a\""));
        assert!(detail.contains("\"locator\":\"objects/ab/object-a.bin\""));

        let missing = http_request(
            &address,
            &format!(
                "GET /v1/debug/routes/missing-object HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(missing.contains("HTTP/1.1 404 Not Found"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_serves_cold_tier_object_lookup() {
        let service = test_service();
        service
            .backend()
            .put_cold_tier_device_if_absent(&sample_cold_tier_device("device-a"))
            .expect("device should seed");
        let key = ObjectKey::new("tenant-a/ns/key-1");
        let route = ObjectRoute {
            key: key.clone(),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: Some(ColdBackingRoute {
                owner: ClientRuntimeId::new("store-a", ClientEpoch(7)),
                cold_tier_id: "device-a".to_string(),
                object_locator: "objects/ab/key-1.bin".to_string(),
                length: 12,
                checksum: Some(99),
                state: ColdBackingState::Materialized,
                replicas: Vec::new(),
            }),
        };
        service
            .backend()
            .compare_and_swap_object_route(&key, None, Some(&route))
            .expect("route should store");
        let pending_key = ObjectKey::new("tenant-a/ns/key-2");
        let pending_route = ObjectRoute {
            key: pending_key.clone(),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: Some(ColdBackingRoute {
                owner: ClientRuntimeId::new("store-a", ClientEpoch(7)),
                cold_tier_id: "device-a".to_string(),
                object_locator: "objects/ab/key-2.bin".to_string(),
                length: 21,
                checksum: None,
                state: ColdBackingState::PendingOffload,
                replicas: Vec::new(),
            }),
        };
        service
            .backend()
            .compare_and_swap_object_route(&pending_key, None, Some(&pending_route))
            .expect("pending route should store");
        let hot_key = ObjectKey::new("tenant-a/ns/hot-key");
        let hot_route = ObjectRoute {
            key: hot_key.clone(),
            namespace: None,
            logical_key: None,
            canonical_key: None,
            sharing_scope: None,
            qos_tier: None,
            version: RouteVersion(1),
            state: RouteState::Active,
            compatibility: CompatibilityDescriptor::default(),
            replicas: Vec::new(),
            cold_backing: None,
        };
        service
            .backend()
            .compare_and_swap_object_route(&hot_key, None, Some(&hot_route))
            .expect("hot route should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/tenant-a%2Fns%2Fkey-1 HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"device_id\":\"device-a\""));
        assert!(response.contains("\"cold_tier_id\":\"device-a\""));
        assert!(response.contains("\"state\":\"materialized\""));
        assert!(response.contains("\"locator\":\"objects/ab/key-1.bin\""));
        assert!(response.contains("\"length\":12"));
        assert!(response.contains("\"checksum\":99"));

        let pending = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/tenant-a%2Fns%2Fkey-2 HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(pending.contains("HTTP/1.1 200 OK"));
        assert!(pending.contains("\"state\":\"pending_offload\""));
        assert!(pending.contains("\"checksum\":null"));

        let hot = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/tenant-a%2Fns%2Fhot-key HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(hot.contains("HTTP/1.1 200 OK"));
        assert!(hot.contains("\"key\":\"tenant-a/ns/hot-key\""));
        assert!(hot.contains("\"cold_backing\":null"));

        let missing = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/missing-object HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(missing.contains("HTTP/1.1 200 OK"));
        assert!(missing.contains("\"key\":\"missing-object\""));
        assert!(missing.contains("\"cold_backing\":null"));

        let bad_encoding = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/bad%ZZ HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(bad_encoding.contains("HTTP/1.1 400 Bad Request"));
        assert!(bad_encoding.contains("invalid object key encoding"));

        let wrong_method = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/objects/tenant-a%2Fns%2Fkey-1 HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(wrong_method.contains("HTTP/1.1 405 Method Not Allowed"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_scopes_cold_tier_object_lookup_by_tenant() {
        let shared: Arc<dyn MetadataBackend> =
            Arc::new(InMemoryMetadataBackend::new_hard_isolated());
        let tenant_a = shared
            .for_tenant("tenant-a")
            .expect("tenant-a view should exist");
        let tenant_b = shared
            .for_tenant("tenant-b")
            .expect("tenant-b view should exist");
        tenant_a
            .put_cold_tier_device_if_absent(&sample_cold_tier_device("device-a"))
            .expect("tenant-a device should seed");
        tenant_b
            .put_cold_tier_device_if_absent(&sample_cold_tier_device("device-b"))
            .expect("tenant-b device should seed");
        tenant_a
            .compare_and_swap_object_route(
                &ObjectKey::new("shared-key"),
                None,
                Some(&ObjectRoute {
                    key: ObjectKey::new("shared-key"),
                    namespace: None,
                    logical_key: None,
                    canonical_key: None,
                    sharing_scope: None,
                    qos_tier: None,
                    version: RouteVersion(1),
                    state: RouteState::Active,
                    compatibility: CompatibilityDescriptor::default(),
                    replicas: Vec::new(),
                    cold_backing: Some(ColdBackingRoute {
                        owner: ClientRuntimeId::new("store-a", ClientEpoch(7)),
                        cold_tier_id: "device-a".to_string(),
                        object_locator: "objects/a.bin".to_string(),
                        length: 12,
                        checksum: None,
                        state: ColdBackingState::Materialized,
                        replicas: Vec::new(),
                    }),
                }),
            )
            .expect("tenant-a route should store");
        tenant_b
            .compare_and_swap_object_route(
                &ObjectKey::new("shared-key"),
                None,
                Some(&ObjectRoute {
                    key: ObjectKey::new("shared-key"),
                    namespace: None,
                    logical_key: None,
                    canonical_key: None,
                    sharing_scope: None,
                    qos_tier: None,
                    version: RouteVersion(1),
                    state: RouteState::Active,
                    compatibility: CompatibilityDescriptor::default(),
                    replicas: Vec::new(),
                    cold_backing: Some(ColdBackingRoute {
                        owner: ClientRuntimeId::new("store-b", ClientEpoch(8)),
                        cold_tier_id: "device-b".to_string(),
                        object_locator: "objects/b.bin".to_string(),
                        length: 21,
                        checksum: Some(9),
                        state: ColdBackingState::PendingOffload,
                        replicas: Vec::new(),
                    }),
                }),
            )
            .expect("tenant-b route should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", test_service_with_backend(shared))
                .expect("server start");
        let address = server.address().to_string();

        let tenant_a_lookup = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/shared-key?tenant=tenant-a HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(tenant_a_lookup.contains("HTTP/1.1 200 OK"));
        assert!(tenant_a_lookup.contains("\"device_id\":\"device-a\""));
        assert!(tenant_a_lookup.contains("\"locator\":\"objects/a.bin\""));
        assert!(tenant_a_lookup.contains("\"state\":\"materialized\""));
        assert!(!tenant_a_lookup.contains("device-b"));

        let tenant_b_lookup = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/objects/shared-key?tenant=tenant-b HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(tenant_b_lookup.contains("HTTP/1.1 200 OK"));
        assert!(tenant_b_lookup.contains("\"device_id\":\"device-b\""));
        assert!(tenant_b_lookup.contains("\"locator\":\"objects/b.bin\""));
        assert!(tenant_b_lookup.contains("\"state\":\"pending_offload\""));
        assert!(!tenant_b_lookup.contains("device-a"));

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
    fn admin_http_server_serves_tracing_status_fanout() {
        let service = test_service();
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!("GET /v1/tracing HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"),
        );

        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"action\":\"status\""));
        assert!(response.contains("\"total\":0"));
        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_rejects_bad_tracing_payloads() {
        let service = test_service();
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "POST /v1/tracing/on HTTP/1.1\r\nHost: {address}\r\nContent-Length: 1\r\nConnection: close\r\n\r\n{{"
            ),
        );

        assert!(response.contains("HTTP/1.1 400 Bad Request"));
        assert!(response.contains("invalid JSON body"));
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

        let cold_register = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-0/register HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                bad_body.len(),
                bad_body
            ),
        );
        assert!(cold_register.contains("HTTP/1.1 400 Bad Request"));
        assert!(cold_register.contains("invalid JSON body"));

        let cold_unregister = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-0/unregister HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                bad_body.len(),
                bad_body
            ),
        );
        assert!(cold_unregister.contains("HTTP/1.1 400 Bad Request"));
        assert!(cold_unregister.contains("invalid JSON body"));

        let cold_disable = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-0/disable HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                bad_body.len(),
                bad_body
            ),
        );
        assert!(cold_disable.contains("HTTP/1.1 400 Bad Request"));
        assert!(cold_disable.contains("invalid JSON body"));

        let cold_enable = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-0/enable HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                bad_body.len(),
                bad_body
            ),
        );
        assert!(cold_enable.contains("HTTP/1.1 400 Bad Request"));
        assert!(cold_enable.contains("invalid JSON body"));

        let unknown_field = serde_json::json!({"unknown": true}).to_string();
        let cold_unregister_unknown = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-0/unregister HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                unknown_field.len(),
                unknown_field
            ),
        );
        assert!(cold_unregister_unknown.contains("HTTP/1.1 400 Bad Request"));
        assert!(cold_unregister_unknown.contains("unknown field"));

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
        assert!(
            http_store_error(StoreError::Backpressure("busy".to_string()))
                .contains("HTTP/1.1 429 Too Many Requests")
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
                key: scoped_object_key("tenant-a", "object-a"),
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
                key: scoped_object_key("tenant-a", "object-b"),
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
                key: scoped_object_key("tenant-a", "object-abort"),
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
            ..FakeMigrationRpc::default()
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
            ..FakeMigrationRpc::default()
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

    #[test]
    fn admin_http_server_serves_cold_tier_blockers() {
        let rpc = Arc::new(FakeMigrationRpc::default());
        let service = test_migration_service(rpc);
        service
            .backend()
            .put_cold_tier_device_if_absent(&sample_cold_tier_device("ssd-blockers"))
            .expect("device should seed");
        service
            .backend()
            .compare_and_swap_object_route(
                &ObjectKey::new("blocker-object"),
                None,
                Some(&ObjectRoute {
                    key: ObjectKey::new("blocker-object"),
                    namespace: None,
                    logical_key: None,
                    canonical_key: None,
                    sharing_scope: None,
                    qos_tier: None,
                    version: RouteVersion(1),
                    state: RouteState::Active,
                    compatibility: CompatibilityDescriptor::default(),
                    replicas: Vec::new(),
                    cold_backing: Some(ColdBackingRoute {
                        owner: ClientRuntimeId::new("storage-a", ClientEpoch(1)),
                        cold_tier_id: "ssd-blockers".to_string(),
                        object_locator: "objects/blocker.bin".to_string(),
                        length: 64,
                        checksum: None,
                        state: ColdBackingState::Materialized,
                        replicas: Vec::new(),
                    }),
                }),
            )
            .expect("route should seed");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-blockers/blockers HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"device_id\":\"ssd-blockers\""));
        assert!(response.contains("\"blocked_objects\":1"));

        let not_found = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/nonexistent/blockers HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(not_found.contains("HTTP/1.1 404 Not Found"));

        let wrong_method = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-blockers/blockers HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(wrong_method.contains("HTTP/1.1 405 Method Not Allowed"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_serves_cold_tier_manual_gc() {
        let rpc = Arc::new(FakeMigrationRpc::default());
        let service = test_migration_service(rpc);
        service
            .backend()
            .put_cold_tier_device_if_absent(&sample_cold_tier_device("ssd-gc"))
            .expect("device should seed");
        service
            .backend()
            .upsert_client_lease(&live_lease("storage-a", 1))
            .expect("lease should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-gc/manual-gc HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"device_id\":\"ssd-gc\""));
        assert!(response.contains("\"collected_objects\":0"));
        assert!(response.contains("manual cold tier GC completed"));

        let not_found = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/nonexistent/manual-gc HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(not_found.contains("HTTP/1.1 404 Not Found"));

        let wrong_method = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-gc/manual-gc HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(wrong_method.contains("HTTP/1.1 405 Method Not Allowed"));

        server.shutdown().expect("server shutdown");
    }

    #[test]
    fn admin_http_server_serves_cold_tier_manual_free() {
        let rpc = Arc::new(FakeMigrationRpc::default());
        let service = test_migration_service(rpc);
        service
            .backend()
            .put_cold_tier_device_if_absent(&sample_cold_tier_device("ssd-free"))
            .expect("device should seed");
        service
            .backend()
            .upsert_client_lease(&live_lease("storage-a", 1))
            .expect("lease should store");
        let mut server =
            AdminHttpServerHandle::start("127.0.0.1:0", service).expect("server start");
        let address = server.address().to_string();

        let response = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/ssd-free/manual-free HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(response.contains("HTTP/1.1 200 OK"));
        assert!(response.contains("\"device_id\":\"ssd-free\""));
        assert!(response.contains("\"freed_backings\":0"));
        assert!(response.contains("\"reached_low_watermark\":true"));
        assert!(response.contains("manual cold tier free completed"));

        let not_found = http_request(
            &address,
            &format!(
                "POST /v1/cold-tier/devices/nonexistent/manual-free HTTP/1.1\r\nHost: {address}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(not_found.contains("HTTP/1.1 404 Not Found"));

        let wrong_method = http_request(
            &address,
            &format!(
                "GET /v1/cold-tier/devices/ssd-free/manual-free HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\n\r\n"
            ),
        );
        assert!(wrong_method.contains("HTTP/1.1 405 Method Not Allowed"));

        server.shutdown().expect("server shutdown");
    }
}
