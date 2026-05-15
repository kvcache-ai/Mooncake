// Tenant-quota HTTP admin endpoints + the WrappedMasterService quota
// methods that back them. Split out of rpc_service.cpp so the
// admin-plane registration code lives in a single dedicated TU
// instead of threading through the larger RPC dispatcher.
//
// The handlers themselves use a small set of JSON-response helpers
// (WriteJsonResponse / WriteErrorResponse / SetServiceUnavailable +
// EscapeJson). These are intentionally re-defined inside this TU's
// anonymous namespace rather than shared via a header: the helpers
// are tiny, stable, and exposing them publicly would invite their
// reuse in places where a caller-built response would be more
// appropriate.

#include "tenant_quota_http.h"

#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <glog/logging.h>

#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "master_metric_manager.h"
#include "master_service.h"
#include "rpc_service.h"
#include "tenant_quota.h"
#include "types.h"

namespace mooncake {

namespace {

using coro_http::coro_http_request;
using coro_http::coro_http_response;
using coro_http::coro_http_server;
using coro_http::DEL;
using coro_http::GET;
using coro_http::PUT;
using coro_http::status_type;

// ---- JSON-response helpers (mirror rpc_service.cpp's anonymous-namespace
// helpers; see file header for why they are duplicated rather than shared).

std::string EscapeJson(std::string_view input) {
    std::string escaped;
    escaped.reserve(input.size());
    for (char ch : input) {
        switch (ch) {
            case '\\':
                escaped += "\\\\";
                break;
            case '"':
                escaped += "\\\"";
                break;
            case '\b':
                escaped += "\\b";
                break;
            case '\f':
                escaped += "\\f";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(ch) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x",
                                  static_cast<unsigned char>(ch));
                    escaped += buf;
                } else {
                    escaped += ch;
                }
        }
    }
    return escaped;
}

void SetServiceUnavailable(coro_http_response& resp, std::string_view message) {
    const std::string payload =
        std::string("{\"success\":false,\"error_code\":") +
        std::to_string(toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE)) +
        ",\"error_message\":\"" + EscapeJson(message) + "\"}";
    resp.add_header("Content-Type", "application/json; charset=utf-8");
    resp.set_status_and_content(status_type::service_unavailable,
                                std::move(payload));
}

template <typename T>
void WriteJsonResponse(coro_http_response& resp, status_type status,
                       const T& payload) {
    std::string json;
    struct_json::to_json(payload, json);
    resp.add_header("Content-Type", "application/json; charset=utf-8");
    resp.set_status_and_content(status, std::move(json));
}

// ---- Tenant-quota request / response shapes ----

// Request body. max_bytes is parsed as int64_t (rather than uint64_t)
// so a client passing -1 yields a parsable value the handler can
// reject with 400; with uint64_t the JSON library would silently wrap
// -1 to UINT64_MAX, which we treat as the "unlimited" sentinel and
// would let any negative input bypass quota.
struct HttpTenantQuotaPolicyRequest {
    std::optional<int64_t> max_bytes;
};
YLT_REFL(HttpTenantQuotaPolicyRequest, max_bytes);

struct HttpTenantQuotaPolicyBody {
    uint64_t max_bytes{0};
};
YLT_REFL(HttpTenantQuotaPolicyBody, max_bytes);

struct HttpTenantQuotaStateBody {
    uint64_t used_bytes{0};
    uint64_t reserved_bytes{0};
    uint64_t committed_count{0};
};
YLT_REFL(HttpTenantQuotaStateBody, used_bytes, reserved_bytes, committed_count);

struct HttpTenantQuotaEntry {
    std::string tenant_id;
    HttpTenantQuotaPolicyBody policy;
    HttpTenantQuotaStateBody state;
};
YLT_REFL(HttpTenantQuotaEntry, tenant_id, policy, state);

struct HttpTenantQuotaListResponse {
    bool success{true};
    std::vector<HttpTenantQuotaEntry> tenants;
};
YLT_REFL(HttpTenantQuotaListResponse, success, tenants);

struct HttpTenantQuotaSingleResponse {
    bool success{true};
    HttpTenantQuotaEntry tenant;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpTenantQuotaSingleResponse, success, tenant, error_code,
         error_message);

struct HttpTenantQuotaDeleteResponse {
    bool success{true};
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpTenantQuotaDeleteResponse, success, error_code, error_message);

// Local error-response shape mirroring the one defined in
// rpc_service.cpp's anonymous namespace.
struct HttpErrorResponse {
    bool success{false};
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpErrorResponse, success, error_code, error_message);

void WriteErrorResponse(coro_http_response& resp, status_type status,
                        ErrorCode error, std::string message = {}) {
    HttpErrorResponse payload;
    payload.error_code = toInt(error);
    payload.error_message =
        message.empty() ? toString(error) : std::move(message);
    WriteJsonResponse(resp, status, payload);
}

HttpTenantQuotaEntry SnapshotToHttpEntry(const TenantQuotaSnapshot& snapshot) {
    HttpTenantQuotaEntry entry;
    entry.tenant_id = snapshot.tenant_id;
    entry.policy.max_bytes = snapshot.policy.max_bytes;
    entry.state.used_bytes = snapshot.state.used_bytes;
    entry.state.reserved_bytes = snapshot.state.reserved_bytes;
    entry.state.committed_count = snapshot.state.committed_count;
    return entry;
}

// Names beginning with `_` are reserved for endpoints that operate on
// pseudo-tenants (today: `_default` for the global default policy
// gauge). Letting a real tenant register under such a name would
// collide on the metric label and confuse `/tenants/quota` listings,
// so the per-tenant CRUD handlers refuse them up front.
bool IsReservedTenantId(std::string_view tenant_id) {
    return !tenant_id.empty() && tenant_id.front() == '_';
}

// Convert int64_t-on-the-wire max_bytes to the uint64_t the policy
// stores, rejecting negatives. Returns std::nullopt on rejection;
// caller surfaces the error as 400.
std::optional<uint64_t> NormalizeMaxBytes(int64_t raw) {
    if (raw < 0) {
        return std::nullopt;
    }
    return static_cast<uint64_t>(raw);
}

}  // namespace

// ============================================================================
// WrappedMasterService quota methods
// ============================================================================

std::vector<TenantQuotaSnapshot> WrappedMasterService::ListTenantQuotas() {
    return master_service_.GetTenantQuotas().ListAll();
}

std::optional<TenantQuotaSnapshot> WrappedMasterService::GetTenantQuota(
    const std::string& tenant_id) {
    return master_service_.GetTenantQuotas().GetSnapshot(tenant_id);
}

TenantQuotaSnapshot WrappedMasterService::UpsertTenantQuota(
    const std::string& tenant_id, const TenantQuotaPolicy& policy) {
    auto& quotas = master_service_.GetTenantQuotas();
    quotas.UpsertPolicy(tenant_id, policy);
    auto snapshot = quotas.GetSnapshot(tenant_id);
    // UpsertPolicy always creates the entry, so GetSnapshot must succeed.
    CHECK(snapshot.has_value()) << "GetSnapshot failed after UpsertPolicy";
    auto& metrics = MasterMetricManager::instance();
    metrics.set_tenant_quota_max_bytes(
        tenant_id, static_cast<int64_t>(snapshot->policy.max_bytes));
    metrics.set_tenant_quota_used_bytes(
        tenant_id, static_cast<int64_t>(snapshot->state.used_bytes));
    metrics.set_tenant_quota_reserved_bytes(
        tenant_id, static_cast<int64_t>(snapshot->state.reserved_bytes));
    return *snapshot;
}

bool WrappedMasterService::EraseTenantQuota(const std::string& tenant_id) {
    bool erased = master_service_.GetTenantQuotas().ErasePolicy(tenant_id);
    if (erased) {
        auto& metrics = MasterMetricManager::instance();
        // ErasePolicy removes the explicit policy but preserves the entry
        // if the tenant still has alive objects (used_bytes > 0). Refresh
        // metrics from the actual snapshot to avoid divergence.
        auto snapshot =
            master_service_.GetTenantQuotas().GetSnapshot(tenant_id);
        if (snapshot.has_value()) {
            // Entry still alive (has usage). Refresh with real values.
            metrics.set_tenant_quota_max_bytes(
                tenant_id, static_cast<int64_t>(snapshot->policy.max_bytes));
            metrics.set_tenant_quota_used_bytes(
                tenant_id, static_cast<int64_t>(snapshot->state.used_bytes));
            metrics.set_tenant_quota_reserved_bytes(
                tenant_id,
                static_cast<int64_t>(snapshot->state.reserved_bytes));
        } else {
            // Entry fully removed (no usage). Zero out stale gauges.
            metrics.set_tenant_quota_max_bytes(tenant_id, 0);
            metrics.set_tenant_quota_used_bytes(tenant_id, 0);
            metrics.set_tenant_quota_reserved_bytes(tenant_id, 0);
        }
    }
    return erased;
}

TenantQuotaPolicy WrappedMasterService::GetDefaultTenantQuota() {
    return master_service_.GetTenantQuotas().GetDefaultPolicy();
}

void WrappedMasterService::SetDefaultTenantQuota(
    const TenantQuotaPolicy& policy) {
    auto& quotas = master_service_.GetTenantQuotas();
    quotas.SetDefaultPolicy(policy);

    // Refresh metrics for the _default pseudo-tenant and for every
    // tenant that inherits the default policy (i.e. has no explicit
    // override). Tenants with their own policy are unaffected.
    auto& metrics = MasterMetricManager::instance();
    metrics.set_tenant_quota_max_bytes("_default",
                                       static_cast<int64_t>(policy.max_bytes));

    for (const auto& snapshot : quotas.ListAll()) {
        if (!snapshot.has_explicit_policy) {
            metrics.set_tenant_quota_max_bytes(
                snapshot.tenant_id,
                static_cast<int64_t>(snapshot.policy.max_bytes));
        }
    }
}

// ============================================================================
// HTTP endpoint registration
// ============================================================================

void RegisterTenantQuotaAdminEndpoints(MasterAdminServer* admin) {
    auto& server = admin->http_server_;

    // GET /tenants/quota — list all tenant quota states
    server.set_http_handler<GET>(
        "/tenants/quota",
        [admin](coro_http_request&, coro_http_response& resp) {
            auto service = admin->GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto snapshots = service->ListTenantQuotas();
            HttpTenantQuotaListResponse payload;
            payload.tenants.reserve(snapshots.size());
            for (const auto& snapshot : snapshots) {
                payload.tenants.push_back(SnapshotToHttpEntry(snapshot));
            }
            WriteJsonResponse(resp, status_type::ok, payload);
        });

    // GET /tenants/quota/_default — get default tenant quota policy
    server.set_http_handler<GET>(
        "/tenants/quota/_default",
        [admin](coro_http_request&, coro_http_response& resp) {
            auto service = admin->GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto policy = service->GetDefaultTenantQuota();
            HttpTenantQuotaSingleResponse payload;
            payload.tenant.tenant_id = "_default";
            payload.tenant.policy.max_bytes = policy.max_bytes;
            WriteJsonResponse(resp, status_type::ok, payload);
        });

    // PUT /tenants/quota/_default — update default tenant quota policy
    server.set_http_handler<PUT>(
        "/tenants/quota/_default",
        [admin](coro_http_request& req, coro_http_response& resp) {
            auto service = admin->GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            HttpTenantQuotaPolicyRequest body;
            try {
                struct_json::from_json(body, req.get_body());
            } catch (const std::exception& e) {
                WriteErrorResponse(
                    resp, status_type::bad_request, ErrorCode::INVALID_PARAMS,
                    std::string("Invalid JSON body: ") + e.what());
                return;
            }

            if (!body.max_bytes.has_value()) {
                WriteErrorResponse(
                    resp, status_type::bad_request, ErrorCode::INVALID_PARAMS,
                    "Request body must contain a valid \"max_bytes\" "
                    "integer field");
                return;
            }
            auto normalized = NormalizeMaxBytes(*body.max_bytes);
            if (!normalized.has_value()) {
                WriteErrorResponse(resp, status_type::bad_request,
                                   ErrorCode::INVALID_PARAMS,
                                   "Field \"max_bytes\" must be non-negative");
                return;
            }

            TenantQuotaPolicy policy;
            policy.max_bytes = *normalized;
            service->SetDefaultTenantQuota(policy);

            HttpTenantQuotaSingleResponse payload;
            payload.tenant.tenant_id = "_default";
            payload.tenant.policy.max_bytes = policy.max_bytes;
            WriteJsonResponse(resp, status_type::ok, payload);
        });

    // GET /tenants/quota/query?tenant_id=<id> — get single tenant quota
    server.set_http_handler<GET>(
        "/tenants/quota/query",
        [admin](coro_http_request& req, coro_http_response& resp) {
            auto service = admin->GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto tenant_id = req.get_query_value("tenant_id");
            if (tenant_id.empty()) {
                WriteErrorResponse(resp, status_type::bad_request,
                                   ErrorCode::INVALID_PARAMS,
                                   "Missing tenant_id query parameter");
                return;
            }
            if (IsReservedTenantId(tenant_id)) {
                WriteErrorResponse(
                    resp, status_type::bad_request, ErrorCode::INVALID_PARAMS,
                    "tenant_id starting with '_' is reserved (use the "
                    "/tenants/quota/_default endpoint for the default "
                    "policy)");
                return;
            }

            auto snapshot = service->GetTenantQuota(std::string(tenant_id));
            if (!snapshot.has_value()) {
                WriteErrorResponse(
                    resp, status_type::not_found,
                    ErrorCode::TENANT_QUOTA_NOT_FOUND,
                    "Tenant not found: " + std::string(tenant_id));
                return;
            }

            HttpTenantQuotaSingleResponse payload;
            payload.tenant = SnapshotToHttpEntry(*snapshot);
            WriteJsonResponse(resp, status_type::ok, payload);
        });

    // PUT /tenants/quota/query?tenant_id=<id> — upsert single tenant quota
    server.set_http_handler<PUT>(
        "/tenants/quota/query",
        [admin](coro_http_request& req, coro_http_response& resp) {
            auto service = admin->GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto tenant_id = req.get_query_value("tenant_id");
            if (tenant_id.empty()) {
                WriteErrorResponse(resp, status_type::bad_request,
                                   ErrorCode::INVALID_PARAMS,
                                   "Missing tenant_id query parameter");
                return;
            }
            if (IsReservedTenantId(tenant_id)) {
                WriteErrorResponse(
                    resp, status_type::bad_request, ErrorCode::INVALID_PARAMS,
                    "tenant_id starting with '_' is reserved (use the "
                    "/tenants/quota/_default endpoint for the default "
                    "policy)");
                return;
            }

            HttpTenantQuotaPolicyRequest body;
            try {
                struct_json::from_json(body, req.get_body());
            } catch (const std::exception& e) {
                WriteErrorResponse(
                    resp, status_type::bad_request, ErrorCode::INVALID_PARAMS,
                    std::string("Invalid JSON body: ") + e.what());
                return;
            }

            if (!body.max_bytes.has_value()) {
                WriteErrorResponse(
                    resp, status_type::bad_request, ErrorCode::INVALID_PARAMS,
                    "Request body must contain a valid \"max_bytes\" "
                    "integer field");
                return;
            }
            auto normalized = NormalizeMaxBytes(*body.max_bytes);
            if (!normalized.has_value()) {
                WriteErrorResponse(resp, status_type::bad_request,
                                   ErrorCode::INVALID_PARAMS,
                                   "Field \"max_bytes\" must be non-negative");
                return;
            }

            TenantQuotaPolicy policy;
            policy.max_bytes = *normalized;
            auto result =
                service->UpsertTenantQuota(std::string(tenant_id), policy);

            HttpTenantQuotaSingleResponse payload;
            payload.tenant = SnapshotToHttpEntry(result);
            WriteJsonResponse(resp, status_type::ok, payload);
        });

    // DELETE /tenants/quota/query?tenant_id=<id> — drop explicit policy
    server.set_http_handler<DEL>(
        "/tenants/quota/query",
        [admin](coro_http_request& req, coro_http_response& resp) {
            auto service = admin->GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto tenant_id = req.get_query_value("tenant_id");
            if (tenant_id.empty()) {
                WriteErrorResponse(resp, status_type::bad_request,
                                   ErrorCode::INVALID_PARAMS,
                                   "Missing tenant_id query parameter");
                return;
            }
            if (IsReservedTenantId(tenant_id)) {
                WriteErrorResponse(
                    resp, status_type::bad_request, ErrorCode::INVALID_PARAMS,
                    "tenant_id starting with '_' is reserved (use the "
                    "/tenants/quota/_default endpoint for the default "
                    "policy)");
                return;
            }

            bool erased = service->EraseTenantQuota(std::string(tenant_id));
            if (!erased) {
                WriteErrorResponse(
                    resp, status_type::not_found,
                    ErrorCode::TENANT_QUOTA_NOT_FOUND,
                    "Tenant quota not found: " + std::string(tenant_id));
                return;
            }

            HttpTenantQuotaDeleteResponse payload;
            WriteJsonResponse(resp, status_type::ok, payload);
        });
}

}  // namespace mooncake
