// Cold tier admin HTTP handlers.
// Included via `include!()` at module level in http.rs.

fn create_cold_tier_device(service: &AdminService, path: &str, body: &[u8]) -> String {
    let tenant = query_param(path, "tenant");
    let payload = match serde_json::from_slice::<CreateColdTierDeviceRequest>(body) {
        Ok(payload) => payload,
        Err(error) => {
            return http_error_response("400 Bad Request", &format!("invalid JSON body: {error}"));
        }
    };
    match service.create_cold_tier_device(tenant.as_deref(), payload) {
        Ok(response) => http_json_response("201 Created", &response),
        Err(error) => http_store_error(error),
    }
}

fn list_cold_tier_devices(service: &AdminService, path: &str) -> String {
    let state = match query_param(path, "state") {
        Some(value) => match super::service::parse_cold_tier_device_state(&value) {
            Ok(state) => Some(state),
            Err(error) => return http_store_error(error),
        },
        None => None,
    };
    let schedulable = match query_param(path, "schedulable") {
        Some(value) => match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "YES" => Some(true),
            "0" | "false" | "FALSE" | "no" | "NO" => Some(false),
            _ => return http_error_response("400 Bad Request", "invalid schedulable filter"),
        },
        None => None,
    };
    match service.list_cold_tier_devices(
        query_param(path, "tenant").as_deref(),
        query_param(path, "stable_id").as_deref(),
        state,
        schedulable,
        query_param(path, "kind").as_deref(),
    ) {
        Ok(response) => http_json_response("200 OK", &response),
        Err(error) => http_store_error(error),
    }
}

fn trigger_cold_tier_offload(service: &AdminService, body: &[u8]) -> String {
    let payload = match serde_json::from_slice::<TriggerColdTierOffloadRequest>(body) {
        Ok(payload) => payload,
        Err(error) => {
            return http_error_response("400 Bad Request", &format!("invalid JSON body: {error}"));
        }
    };
    match service.trigger_cold_tier_offload(payload) {
        Ok(response) => http_json_response("202 Accepted", &response),
        Err(error) => http_store_error(error),
    }
}

fn route_cold_tier_device_request(
    service: &AdminService,
    request: HttpRequest,
    path_only: &str,
) -> String {
    let tenant = query_param(&request.path, "tenant");
    let tenant = tenant.as_deref();
    let Some(rest) = path_only.strip_prefix("/v1/cold-tier/devices/") else {
        return http_error_response("404 Not Found", "not found");
    };
    let parts = rest
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let Some(raw_device_id) = parts.first() else {
        return http_error_response("404 Not Found", "not found");
    };
    let Some(device_id) = percent_decode_component(raw_device_id) else {
        return http_error_response("400 Bad Request", "invalid cold tier device id encoding");
    };
    match parts.as_slice() {
        [_] if request.method == "GET" => match service.get_cold_tier_device(tenant, &device_id) {
            Ok(response) => http_json_response("200 OK", &response),
            Err(error) => http_store_error(error),
        },
        [_, "orphan-quarantine"] if request.method == "GET" => {
            match service.get_cold_tier_quarantine(tenant, &device_id) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_, "blockers"] if request.method == "GET" => {
            match service.get_cold_tier_blockers(tenant, &device_id) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_, "drain"] if request.method == "POST" => {
            let payload = match parse_json_body::<ColdTierDrainRequest>(&request.body) {
                Ok(payload) => payload,
                Err(response) => return response,
            };
            match service.drain_cold_tier_device(tenant, &device_id, payload) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_, "manual-gc"] if request.method == "POST" => {
            match service.manual_gc_cold_tier_device(tenant, &device_id) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_, "manual-free"] if request.method == "POST" => {
            match service.manual_free_cold_tier_device(tenant, &device_id) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_, "register"] if request.method == "POST" => {
            let payload = match parse_json_body::<ColdTierRegisterRequest>(&request.body) {
                Ok(payload) => payload,
                Err(response) => return response,
            };
            match service.register_cold_tier_device(tenant, &device_id, payload) {
                Ok(response) => http_json_response("201 Created", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_, "unregister"] if request.method == "POST" => {
            let payload = match parse_json_body::<ColdTierUnregisterRequest>(&request.body) {
                Ok(payload) => payload,
                Err(response) => return response,
            };
            match service.unregister_cold_tier_device(tenant, &device_id, payload) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_, "disable"] if request.method == "POST" => {
            let payload = match parse_json_body::<ColdTierDisableRequest>(&request.body) {
                Ok(payload) => payload,
                Err(response) => return response,
            };
            match service.disable_cold_tier_device(tenant, &device_id, payload) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_, "enable"] if request.method == "POST" => {
            let payload = match parse_json_body::<ColdTierEnableRequest>(&request.body) {
                Ok(payload) => payload,
                Err(response) => return response,
            };
            match service.enable_cold_tier_device(tenant, &device_id, payload) {
                Ok(response) => http_json_response("200 OK", &response),
                Err(error) => http_store_error(error),
            }
        }
        [_]
        | [_, "orphan-quarantine"]
        | [_, "blockers"]
        | [_, "drain"]
        | [_, "manual-gc"]
        | [_, "manual-free"]
        | [_, "register"]
        | [_, "unregister"]
        | [_, "disable"]
        | [_, "enable"] => http_error_response("405 Method Not Allowed", "method not allowed"),
        _ => http_error_response("404 Not Found", "not found"),
    }
}

fn route_cold_tier_offload_request(
    service: &AdminService,
    request: HttpRequest,
    path_only: &str,
) -> String {
    let Some(task_id) = path_only.strip_prefix("/v1/cold-tier/offloads/") else {
        return http_error_response("404 Not Found", "not found");
    };
    if task_id.is_empty() {
        return http_error_response("404 Not Found", "not found");
    }
    if request.method != "GET" {
        return http_error_response("405 Method Not Allowed", "method not allowed");
    }
    match service.get_cold_tier_offload_task(task_id) {
        Ok(response) => http_json_response("200 OK", &response),
        Err(error) => http_store_error(error),
    }
}

fn route_cold_tier_object_request(
    service: &AdminService,
    request: HttpRequest,
    path_only: &str,
) -> String {
    if request.method != "GET" {
        return http_error_response("405 Method Not Allowed", "method not allowed");
    }
    let Some(encoded_key) = path_only.strip_prefix("/v1/cold-tier/objects/") else {
        return http_error_response("404 Not Found", "not found");
    };
    let Some(key) = percent_decode_component(encoded_key) else {
        return http_error_response("400 Bad Request", "invalid object key encoding");
    };
    match service.get_cold_tier_object(query_param(&request.path, "tenant").as_deref(), &key) {
        Ok(response) => http_json_response("200 OK", &response),
        Err(error) => http_store_error(error),
    }
}
