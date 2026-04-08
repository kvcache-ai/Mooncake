#include "rpc_service.h"
#include <csignal>

#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <sstream>
#include <thread>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/util/tl/expected.hpp>

#include "ha_metric_manager.h"
#include "master_metric_manager.h"
#include "master_service.h"
#include "rpc_helper.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

const uint64_t kMetricReportIntervalSeconds = 10;

namespace {

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
                escaped.push_back(ch);
                break;
        }
    }
    return escaped;
}

std::string AppendMetricSections(std::string primary, std::string secondary) {
    if (!primary.empty() && primary.back() != '\n') {
        primary.push_back('\n');
    }
    primary += secondary;
    return primary;
}

void SetServiceUnavailable(coro_http::coro_http_response& resp,
                           std::string_view message) {
    const std::string payload =
        std::string("{\"success\":false,\"error_code\":") +
        std::to_string(toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_MODE)) +
        ",\"error_message\":\"" + EscapeJson(message) + "\"}";
    resp.add_header("Content-Type", "application/json; charset=utf-8");
    resp.set_status_and_content(coro_http::status_type::service_unavailable,
                                payload);
}
struct HttpErrorResponse {
    bool success{false};
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpErrorResponse, success, error_code, error_message);

struct HttpCreateDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCreateDrainJobResponse, success, job_id, status, error_code,
         error_message);

struct HttpQueryDrainJobResponse {
    bool success{false};
    std::string job_id;
    int32_t type{0};
    std::string type_name;
    int32_t status{0};
    std::string status_name;
    int64_t created_at_ms_epoch{0};
    int64_t last_updated_at_ms_epoch{0};
    std::vector<std::string> segments;
    uint64_t succeeded_units{0};
    uint64_t failed_units{0};
    uint64_t blocked_units{0};
    uint64_t active_units{0};
    uint64_t migrated_bytes{0};
    std::string message;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpQueryDrainJobResponse, success, job_id, type, type_name, status,
         status_name, created_at_ms_epoch, last_updated_at_ms_epoch, segments,
         succeeded_units, failed_units, blocked_units, active_units,
         migrated_bytes, message, error_code, error_message);

struct HttpCancelDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCancelDrainJobResponse, success, job_id, status, error_code,
         error_message);

struct HttpSegmentStatusResponse {
    bool success{false};
    std::string segment;
    int32_t status{0};
    std::string status_name;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpSegmentStatusResponse, success, segment, status, status_name,
         error_code, error_message);

template <typename T>
void WriteJsonResponse(coro_http::coro_http_response& resp,
                       coro_http::status_type status, const T& payload) {
    std::string json;
    struct_json::to_json(payload, json);
    resp.add_header("Content-Type", "application/json; charset=utf-8");
    resp.set_status_and_content(status, std::move(json));
}

template <typename T>
std::string EnumToString(const T& value) {
    std::ostringstream oss;
    oss << value;
    return oss.str();
}

coro_http::status_type ErrorCodeToHttpStatus(ErrorCode error) {
    switch (error) {
        case ErrorCode::INVALID_PARAMS:
            return coro_http::status_type::bad_request;
        case ErrorCode::JOB_NOT_FOUND:
        case ErrorCode::SEGMENT_NOT_FOUND:
            return coro_http::status_type::not_found;
        default:
            return coro_http::status_type::internal_server_error;
    }
}

void WriteErrorResponse(coro_http::coro_http_response& resp,
                        coro_http::status_type status, ErrorCode error,
                        std::string message = {}) {
    HttpErrorResponse payload;
    payload.error_code = toInt(error);
    payload.error_message =
        message.empty() ? toString(error) : std::move(message);
    WriteJsonResponse(resp, status, payload);
}

tl::expected<UUID, ErrorCode> ParseJobId(std::string_view job_id_view) {
    if (job_id_view.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    UUID job_id;
    if (!StringToUuid(std::string(job_id_view), job_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return job_id;
}

HttpQueryDrainJobResponse ToHttpQueryDrainJobResponse(
    const QueryJobResponse& job) {
    HttpQueryDrainJobResponse payload;
    payload.success = true;
    payload.job_id = UuidToString(job.id);
    payload.type = static_cast<int32_t>(job.type);
    payload.type_name = EnumToString(job.type);
    payload.status = static_cast<int32_t>(job.status);
    payload.status_name = EnumToString(job.status);
    payload.created_at_ms_epoch = job.created_at_ms_epoch;
    payload.last_updated_at_ms_epoch = job.last_updated_at_ms_epoch;
    payload.segments = job.segments;
    payload.succeeded_units = job.succeeded_units;
    payload.failed_units = job.failed_units;
    payload.blocked_units = job.blocked_units;
    payload.active_units = job.active_units;
    payload.migrated_bytes = job.migrated_bytes;
    payload.message = job.message;
    return payload;
}

}  // namespace

WrappedMasterService::WrappedMasterService(
    const WrappedMasterServiceConfig& config)
    : master_service_(MasterServiceConfig(config)) {}

WrappedMasterService::~WrappedMasterService() = default;

MasterAdminServer::MasterAdminServer(uint16_t http_port,
                                     bool enable_metric_reporting)
    : http_port_(http_port),
      enable_metric_reporting_(enable_metric_reporting),
      http_server_(4, http_port) {}

MasterAdminServer::~MasterAdminServer() { Stop(); }

bool MasterAdminServer::Start() {
    HAMetricManager::Init();
    InitHttpServer();

    auto ec = http_server_.async_start();
    if (ec.hasResult()) {
        LOG(ERROR) << "Failed to start master admin server on port "
                   << http_port_;
        return false;
    }

    started_.store(true);
    if (enable_metric_reporting_) {
        metric_report_running_.store(true);
        metric_report_thread_ = std::thread([this]() {
            while (metric_report_running_.load()) {
                const auto snapshot = SnapshotState();
                LOG(INFO) << "Master Admin Metrics: role="
                          << ha::MasterRuntimeRoleToString(snapshot.state)
                          << ", state="
                          << ha::MasterRuntimeStateToString(snapshot.state)
                          << ", summary=" << BuildMetricsSummaryText();
                std::this_thread::sleep_for(
                    std::chrono::seconds(kMetricReportIntervalSeconds));
            }
        });
    }

    LOG(INFO) << "Master admin server started on port " << http_server_.port();
    return true;
}

void MasterAdminServer::Stop() {
    metric_report_running_.store(false);
    if (metric_report_thread_.joinable()) {
        metric_report_thread_.join();
    }
    if (started_.exchange(false)) {
        http_server_.stop();
    }
}

void MasterAdminServer::SetRuntimeState(ha::MasterRuntimeState state) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    state_ = state;
}

void MasterAdminServer::SetObservedLeader(
    const std::optional<ha::MasterView>& leader_view) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    leader_view_ = leader_view;
}

void MasterAdminServer::SetServiceDelegate(
    std::shared_ptr<WrappedMasterService> service) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    service_ = std::move(service);
    if (!service_) {
        service_available_ = false;
    }
}

void MasterAdminServer::SetServiceAvailable(bool available) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    service_available_ = available && service_ != nullptr;
}

MasterAdminServer::RuntimeSnapshot MasterAdminServer::SnapshotState() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return RuntimeSnapshot{
        .state = state_,
        .leader_view = leader_view_,
        .service = service_,
        .service_available = service_available_,
    };
}

std::string MasterAdminServer::BuildMetricsText() const {
    return AppendMetricSections(
        MasterMetricManager::instance().serialize_metrics(),
        HAMetricManager::instance().serialize_metrics());
}

std::string MasterAdminServer::BuildMetricsSummaryText() const {
    const auto snapshot = SnapshotState();
    std::ostringstream oss;
    oss << "role=" << ha::MasterRuntimeRoleToString(snapshot.state)
        << ", state=" << ha::MasterRuntimeStateToString(snapshot.state)
        << ", service_ready=" << (snapshot.service_available ? "true" : "false")
        << ", master={" << MasterMetricManager::instance().get_summary_string()
        << "}, ha={" << HAMetricManager::instance().get_summary_string() << "}";
    if (snapshot.leader_view.has_value()) {
        oss << ", leader=" << snapshot.leader_view->leader_address
            << ", view_version=" << snapshot.leader_view->view_version;
    }
    return oss.str();
}

std::string MasterAdminServer::BuildHealthJson() const {
    const auto snapshot = SnapshotState();
    std::ostringstream oss;
    oss << "{\"status\":\"ok\",\"role\":\""
        << ha::MasterRuntimeRoleToString(snapshot.state) << "\",\"ha_state\":\""
        << ha::MasterRuntimeStateToString(snapshot.state)
        << "\",\"service_ready\":"
        << (snapshot.service_available ? "true" : "false");
    if (snapshot.leader_view.has_value()) {
        oss << ",\"leader_address\":\""
            << EscapeJson(snapshot.leader_view->leader_address)
            << "\",\"view_version\":" << snapshot.leader_view->view_version;
    }
    oss << "}";
    return oss.str();
}

std::string MasterAdminServer::BuildLeaderJson() const {
    const auto snapshot = SnapshotState();
    if (!snapshot.leader_view.has_value()) {
        return "{\"present\":false}";
    }

    std::ostringstream oss;
    oss << "{\"present\":true,\"leader_address\":\""
        << EscapeJson(snapshot.leader_view->leader_address)
        << "\",\"view_version\":" << snapshot.leader_view->view_version << "}";
    return oss.str();
}

std::shared_ptr<WrappedMasterService> MasterAdminServer::GetActiveService()
    const {
    const auto snapshot = SnapshotState();
    if (!snapshot.service_available) {
        return nullptr;
    }
    return snapshot.service;
}

void MasterAdminServer::InitHttpServer() {
    using namespace coro_http;

    http_server_.set_http_handler<GET>(
        "/metrics", [this](coro_http_request&, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, BuildMetricsText());
        });

    http_server_.set_http_handler<GET>(
        "/metrics/summary",
        [this](coro_http_request&, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok,
                                        BuildMetricsSummaryText());
        });

    http_server_.set_http_handler<GET>(
        "/health", [this](coro_http_request&, coro_http_response& resp) {
            resp.add_header("Content-Type", "application/json; charset=utf-8");
            resp.set_status_and_content(status_type::ok, BuildHealthJson());
        });

    http_server_.set_http_handler<GET>(
        "/role", [this](coro_http_request&, coro_http_response& resp) {
            const auto snapshot = SnapshotState();
            resp.add_header("Content-Type", "text/plain; charset=utf-8");
            resp.set_status_and_content(
                status_type::ok, ha::MasterRuntimeRoleToString(snapshot.state));
        });

    http_server_.set_http_handler<GET>(
        "/ha_status", [this](coro_http_request&, coro_http_response& resp) {
            const auto snapshot = SnapshotState();
            resp.add_header("Content-Type", "text/plain; charset=utf-8");
            resp.set_status_and_content(
                status_type::ok,
                ha::MasterRuntimeStateToString(snapshot.state));
        });

    http_server_.set_http_handler<GET>(
        "/leader", [this](coro_http_request&, coro_http_response& resp) {
            resp.add_header("Content-Type", "application/json; charset=utf-8");
            resp.set_status_and_content(status_type::ok, BuildLeaderJson());
        });

    http_server_.set_http_handler<GET>(
        "/query_key", [this](coro_http_request& req, coro_http_response& resp) {
            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto key = req.get_query_value("key");
            auto get_result = service->GetReplicaList(std::string(key));
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            if (get_result) {
                std::string ss;
                const auto& replicas = get_result.value().replicas;
                for (const auto& replica : replicas) {
                    if (!replica.is_memory_replica()) {
                        continue;
                    }
                    std::string tmp;
                    struct_json::to_json(
                        replica.get_memory_descriptor().buffer_descriptor, tmp);
                    ss += tmp;
                    ss += "\n";
                }
                resp.set_status_and_content(status_type::ok, std::move(ss));
                return;
            }

            resp.set_status_and_content(status_type::not_found,
                                        toString(get_result.error()));
        });

    http_server_.set_http_handler<GET>(
        "/get_all_keys", [this](coro_http_request&, coro_http_response& resp) {
            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto result = service->GetAllKeysForAdmin();
            if (!result) {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to get all keys");
                return;
            }

            std::string body;
            for (const auto& key : result.value()) {
                body += key;
                body += "\n";
            }
            resp.set_status_and_content(status_type::ok, std::move(body));
        });

    http_server_.set_http_handler<GET>(
        "/get_all_segments",
        [this](coro_http_request&, coro_http_response& resp) {
            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto result = service->GetAllSegmentsForAdmin();
            if (!result) {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to get all segments");
                return;
            }

            std::string body;
            for (const auto& segment_name : result.value()) {
                body += segment_name;
                body += "\n";
            }
            resp.set_status_and_content(status_type::ok, std::move(body));
        });

    http_server_.set_http_handler<GET>(
        "/query_segment",
        [this](coro_http_request& req, coro_http_response& resp) {
            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto segment = req.get_query_value("segment");
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto result = service->QuerySegmentForAdmin(std::string(segment));

            if (!result) {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to query segment");
                return;
            }

            const auto [used, capacity] = result.value();
            std::string body(segment);
            body += "\nUsed(bytes): ";
            body += std::to_string(used);
            body += "\nCapacity(bytes) : ";
            body += std::to_string(capacity);
            body += "\n";
            resp.set_status_and_content(status_type::ok, std::move(body));
        });

    http_server_.set_http_handler<POST>(
        "/api/v1/drain_jobs",
        [&](coro_http_request& req, coro_http_response& resp) {
            CreateDrainJobRequest request;
            try {
                struct_json::from_json(request, req.get_body());
            } catch (const std::exception& e) {
                WriteErrorResponse(
                    resp, status_type::bad_request, ErrorCode::INVALID_PARAMS,
                    std::string("Invalid JSON body: ") + e.what());
                return;
            }

            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto result = service->CreateDrainJob(request);
            if (!result.has_value()) {
                WriteErrorResponse(resp, ErrorCodeToHttpStatus(result.error()),
                                   result.error());
                return;
            }

            HttpCreateDrainJobResponse payload;
            payload.success = true;
            payload.job_id = UuidToString(result.value());
            payload.status = "CREATED";
            WriteJsonResponse(resp, status_type::ok, payload);
        });

    http_server_.set_http_handler<GET>(
        "/api/v1/drain_jobs/query",
        [&](coro_http_request& req, coro_http_response& resp) {
            auto job_id_result =
                ParseJobId(req.get_decode_query_value("job_id"));
            if (!job_id_result.has_value()) {
                WriteErrorResponse(resp, status_type::bad_request,
                                   job_id_result.error(),
                                   "Missing or invalid job_id");
                return;
            }

            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto result = service->QueryDrainJob(job_id_result.value());
            if (!result.has_value()) {
                WriteErrorResponse(resp, ErrorCodeToHttpStatus(result.error()),
                                   result.error());
                return;
            }

            WriteJsonResponse(resp, status_type::ok,
                              ToHttpQueryDrainJobResponse(result.value()));
        });

    http_server_.set_http_handler<POST>(
        "/api/v1/drain_jobs/cancel",
        [&](coro_http_request& req, coro_http_response& resp) {
            auto job_id_result =
                ParseJobId(req.get_decode_query_value("job_id"));
            if (!job_id_result.has_value()) {
                WriteErrorResponse(resp, status_type::bad_request,
                                   job_id_result.error(),
                                   "Missing or invalid job_id");
                return;
            }

            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto result = service->CancelDrainJob(job_id_result.value());
            if (!result.has_value()) {
                WriteErrorResponse(resp, ErrorCodeToHttpStatus(result.error()),
                                   result.error());
                return;
            }

            HttpCancelDrainJobResponse payload;
            payload.success = true;
            payload.job_id = UuidToString(job_id_result.value());
            payload.status = "CANCELED";
            WriteJsonResponse(resp, status_type::ok, payload);
        });

    http_server_.set_http_handler<GET>(
        "/api/v1/segments/status",
        [&](coro_http_request& req, coro_http_response& resp) {
            auto segment_name = req.get_decode_query_value("segment");
            if (segment_name.empty()) {
                WriteErrorResponse(resp, status_type::bad_request,
                                   ErrorCode::INVALID_PARAMS,
                                   "Missing segment query parameter");
                return;
            }

            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto result =
                service->QuerySegmentStatus(std::string(segment_name));
            if (!result.has_value()) {
                WriteErrorResponse(resp, ErrorCodeToHttpStatus(result.error()),
                                   result.error());
                return;
            }

            HttpSegmentStatusResponse payload;
            payload.success = true;
            payload.segment = std::string(segment_name);
            payload.status = static_cast<int32_t>(result.value());
            payload.status_name = EnumToString(result.value());
            WriteJsonResponse(resp, status_type::ok, payload);
        });

    http_server_.set_http_handler<GET>(
        "/batch_query_keys",
        [this](coro_http_request& req, coro_http_response& resp) {
            auto service = GetActiveService();
            if (!service) {
                resp.add_header("Content-Type",
                                "application/json; charset=utf-8");
                resp.set_status_and_content(
                    status_type::service_unavailable,
                    "{\"success\":false,\"error\":\"service plane is not "
                    "active\"}");
                return;
            }

            auto keys_view = req.get_query_value("keys");
            std::vector<std::string> keys;
            if (!keys_view.empty()) {
                std::string keys_str(keys_view);
                std::string key;
                std::istringstream iss(keys_str);
                while (std::getline(iss, key, ',')) {
                    keys.push_back(std::move(key));
                }
            }

            resp.add_header("Content-Type", "application/json; charset=utf-8");
            if (keys.empty()) {
                resp.set_status_and_content(
                    status_type::bad_request,
                    "{\"success\":false,\"error\":\"No keys provided. Use "
                    "?keys=key1,key2,...\"}");
                return;
            }

            auto results = service->BatchGetReplicaList(keys);
            const size_t n = std::min(keys.size(), results.size());
            std::string body;
            body.reserve(n * 512);
            body += "{\"success\":true,\"data\":{";

            for (size_t i = 0; i < n; ++i) {
                if (i > 0) {
                    body += ",";
                }

                body += "\"";
                body += EscapeJson(keys[i]);
                body += "\":";

                const auto& result = results[i];
                if (!result.has_value()) {
                    body += "{\"ok\":false,\"error\":\"";
                    body += EscapeJson(toString(result.error()));
                    body += "\"}";
                    continue;
                }

                body += "{\"ok\":true,\"values\":[";
                bool first = true;
                for (const auto& replica : result.value().replicas) {
                    if (!replica.is_memory_replica()) {
                        continue;
                    }

                    std::string tmp;
                    struct_json::to_json(
                        replica.get_memory_descriptor().buffer_descriptor, tmp);
                    if (!first) {
                        body += ",";
                    }
                    body += tmp;
                    first = false;
                }
                body += "]}";
            }

            body += "}}";
            if (results.size() != keys.size()) {
                LOG(WARNING)
                    << "BatchGetReplicaList size mismatch: keys=" << keys.size()
                    << " results=" << results.size();
            }
            resp.set_status_and_content(status_type::ok, std::move(body));
        });
}

tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
WrappedMasterService::CalcCacheStats() {
    return MasterMetricManager::instance().calculate_cache_stats();
}

tl::expected<bool, ErrorCode> WrappedMasterService::ExistKey(
    const std::string& key) {
    return execute_rpc(
        "ExistKey", [&] { return master_service_.ExistKey(key); },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] { MasterMetricManager::instance().inc_exist_key_requests(); },
        [] { MasterMetricManager::instance().inc_exist_key_failures(); });
}

std::vector<tl::expected<bool, ErrorCode>> WrappedMasterService::BatchExistKey(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "BatchExistKey");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_exist_key_requests(total_keys);

    auto result = master_service_.BatchExistKey(keys);

    size_t failure_count = 0;
    for (size_t i = 0; i < result.size(); ++i) {
        if (!result[i].has_value()) {
            failure_count++;
            auto error = result[i].error();
            LOG(ERROR) << "BatchExistKey failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_exist_key_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_exist_key_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", result.size(),
                      ", success=", result.size() - failure_count,
                      ", failures=", failure_count);
    return result;
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
WrappedMasterService::BatchQueryIp(const std::vector<UUID>& client_ids) {
    ScopedVLogTimer timer(1, "BatchQueryIp");
    const size_t total_client_ids = client_ids.size();
    timer.LogRequest("client_ids_count=", total_client_ids);
    MasterMetricManager::instance().inc_batch_query_ip_requests(
        total_client_ids);

    auto result = master_service_.BatchQueryIp(client_ids);

    size_t failure_count = 0;
    if (!result.has_value()) {
        failure_count = total_client_ids;
    } else {
        for (size_t i = 0; i < client_ids.size(); ++i) {
            const auto& client_id = client_ids[i];
            if (result.value().find(client_id) == result.value().end()) {
                failure_count++;
                VLOG(1) << "BatchQueryIp failed for client_id[" << i << "] '"
                        << client_id << "': not found in results";
            }
        }
    }

    if (failure_count == total_client_ids) {
        MasterMetricManager::instance().inc_batch_query_ip_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_query_ip_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", total_client_ids,
                      ", success=", total_client_ids - failure_count,
                      ", failures=", failure_count);
    return result;
}

tl::expected<std::vector<std::string>, ErrorCode>
WrappedMasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    ScopedVLogTimer timer(1, "BatchReplicaClear");
    const size_t total_keys = object_keys.size();
    timer.LogRequest("object_keys_count=", total_keys,
                     ", client_id=", client_id,
                     ", segment_name=", segment_name);
    MasterMetricManager::instance().inc_batch_replica_clear_requests(
        total_keys);

    auto result =
        master_service_.BatchReplicaClear(object_keys, client_id, segment_name);

    size_t failure_count = 0;
    if (!result.has_value()) {
        failure_count = total_keys;
        LOG(WARNING) << "BatchReplicaClear failed: "
                     << toString(result.error());
    } else {
        const size_t cleared_count = result.value().size();
        failure_count = total_keys - cleared_count;
        timer.LogResponse("total=", total_keys, ", cleared=", cleared_count,
                          ", failed=", failure_count);
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_replica_clear_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_replica_clear_partial_success(
            failure_count);
    }

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
WrappedMasterService::GetReplicaListByRegex(const std::string& str) {
    return execute_rpc(
        "GetReplicaListByRegex",
        [&] { return master_service_.GetReplicaListByRegex(str); },
        [&](auto& timer) { timer.LogRequest("Regex=", str); },
        [] {
            MasterMetricManager::instance()
                .inc_get_replica_list_by_regex_requests();
        },
        [] {
            MasterMetricManager::instance()
                .inc_get_replica_list_by_regex_failures();
        });
}

tl::expected<GetReplicaListResponse, ErrorCode>
WrappedMasterService::GetReplicaList(const std::string& key) {
    return execute_rpc(
        "GetReplicaList", [&] { return master_service_.GetReplicaList(key); },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] { MasterMetricManager::instance().inc_get_replica_list_requests(); },
        [] {
            MasterMetricManager::instance().inc_get_replica_list_failures();
        });
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
WrappedMasterService::BatchGetReplicaList(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "BatchGetReplicaList");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_get_replica_list_requests(
        total_keys);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> results;
    results.reserve(keys.size());

    for (const auto& key : keys) {
        results.emplace_back(master_service_.GetReplicaList(key));
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            if (error == ErrorCode::OBJECT_NOT_FOUND ||
                error == ErrorCode::REPLICA_IS_NOT_READY) {
                VLOG(1) << "BatchGetReplicaList failed for key[" << i << "] '"
                        << keys[i] << "': " << toString(error);
            } else {
                LOG(ERROR) << "BatchGetReplicaList failed for key[" << i
                           << "] '" << keys[i] << "': " << toString(error);
            }
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_get_replica_list_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance()
            .inc_batch_get_replica_list_partial_success(failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
WrappedMasterService::PutStart(const UUID& client_id, const std::string& key,
                               const uint64_t slice_length,
                               const ReplicateConfig& config) {
    return execute_rpc(
        "PutStart",
        [&] {
            return master_service_.PutStart(client_id, key, slice_length,
                                            config);
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", slice_length=", slice_length);
        },
        [&] { MasterMetricManager::instance().inc_put_start_requests(); },
        [] { MasterMetricManager::instance().inc_put_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::PutEnd(
    const UUID& client_id, const std::string& key, ReplicaType replica_type) {
    return execute_rpc(
        "PutEnd",
        [&] { return master_service_.PutEnd(client_id, key, replica_type); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_end_requests(); },
        [] { MasterMetricManager::instance().inc_put_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::PutRevoke(
    const UUID& client_id, const std::string& key, ReplicaType replica_type) {
    return execute_rpc(
        "PutRevoke",
        [&] { return master_service_.PutRevoke(client_id, key, replica_type); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_put_revoke_failures(); });
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
WrappedMasterService::BatchPutStart(const UUID& client_id,
                                    const std::vector<std::string>& keys,
                                    const std::vector<uint64_t>& slice_lengths,
                                    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "BatchPutStart");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_start_requests(total_keys);

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    results.reserve(keys.size());

    if (config.prefer_alloc_in_same_node) {
        ReplicateConfig new_config = config;
        for (size_t i = 0; i < keys.size(); ++i) {
            auto result = master_service_.PutStart(
                client_id, keys[i], slice_lengths[i], new_config);
            results.emplace_back(result);
            if ((i == 0) && result.has_value()) {
                std::string preferred_segment;
                for (const auto& replica : result.value()) {
                    if (replica.is_memory_replica()) {
                        auto handles =
                            replica.get_memory_descriptor().buffer_descriptor;
                        if (!handles.transport_endpoint_.empty()) {
                            preferred_segment = handles.transport_endpoint_;
                        }
                    }
                }
                if (!preferred_segment.empty()) {
                    new_config.preferred_segment = preferred_segment;
                }
            }
        }
    } else {
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(master_service_.PutStart(
                client_id, keys[i], slice_lengths[i], config));
        }
    }

    size_t failure_count = 0;
    int no_available_handle_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            if (error == ErrorCode::OBJECT_ALREADY_EXISTS) {
                VLOG(1) << "BatchPutStart failed for key[" << i << "] '"
                        << keys[i] << "': " << toString(error);
            } else if (error == ErrorCode::NO_AVAILABLE_HANDLE) {
                no_available_handle_count++;
            } else {
                LOG(ERROR) << "BatchPutStart failed for key[" << i << "] '"
                           << keys[i] << "': " << toString(error);
            }
        }
    }

    if (no_available_handle_count > 0) {
        LOG(WARNING) << "BatchPutStart failed for " << no_available_handle_count
                     << " keys" << PUT_NO_SPACE_HELPER_STR;
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_start_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_start_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchPutEnd(
    const UUID& client_id, const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "BatchPutEnd");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_end_requests(total_keys);

    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());

    for (const auto& key : keys) {
        results.emplace_back(
            master_service_.PutEnd(client_id, key, ReplicaType::MEMORY));
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchPutEnd failed for key[" << i << "] '" << keys[i]
                       << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_end_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_end_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchPutRevoke(
    const UUID& client_id, const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "BatchPutRevoke");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_revoke_requests(total_keys);

    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());

    for (const auto& key : keys) {
        results.emplace_back(
            master_service_.PutRevoke(client_id, key, ReplicaType::MEMORY));
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchPutRevoke failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_revoke_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_revoke_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
WrappedMasterService::UpsertStart(const UUID& client_id, const std::string& key,
                                  const uint64_t slice_length,
                                  const ReplicateConfig& config) {
    return execute_rpc(
        "UpsertStart",
        [&] {
            return master_service_.UpsertStart(client_id, key, slice_length,
                                               config);
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", slice_length=", slice_length);
        },
        [&] { MasterMetricManager::instance().inc_put_start_requests(); },
        [] { MasterMetricManager::instance().inc_put_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::UpsertEnd(
    const UUID& client_id, const std::string& key, ReplicaType replica_type) {
    return execute_rpc(
        "UpsertEnd",
        [&] { return master_service_.UpsertEnd(client_id, key, replica_type); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_end_requests(); },
        [] { MasterMetricManager::instance().inc_put_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::UpsertRevoke(
    const UUID& client_id, const std::string& key, ReplicaType replica_type) {
    return execute_rpc(
        "UpsertRevoke",
        [&] {
            return master_service_.UpsertRevoke(client_id, key, replica_type);
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_put_revoke_failures(); });
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
WrappedMasterService::BatchUpsertStart(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<uint64_t>& slice_lengths, const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "BatchUpsertStart");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_start_requests(total_keys);

    auto results = master_service_.BatchUpsertStart(client_id, keys,
                                                    slice_lengths, config);

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchUpsertStart failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_start_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_start_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchUpsertEnd(
    const UUID& client_id, const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "BatchUpsertEnd");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_end_requests(total_keys);

    auto results = master_service_.BatchUpsertEnd(client_id, keys);

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchUpsertEnd failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_end_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_end_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

std::vector<tl::expected<void, ErrorCode>>
WrappedMasterService::BatchUpsertRevoke(const UUID& client_id,
                                        const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "BatchUpsertRevoke");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_revoke_requests(total_keys);

    auto results = master_service_.BatchUpsertRevoke(client_id, keys);

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchUpsertRevoke failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        MasterMetricManager::instance().inc_batch_put_revoke_failures(
            failure_count);
    } else if (failure_count != 0) {
        MasterMetricManager::instance().inc_batch_put_revoke_partial_success(
            failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

tl::expected<void, ErrorCode> WrappedMasterService::Remove(
    const std::string& key, bool force) {
    return execute_rpc(
        "Remove", [&] { return master_service_.Remove(key, force); },
        [&](auto& timer) { timer.LogRequest("key=", key, ", force=", force); },
        [] { MasterMetricManager::instance().inc_remove_requests(); },
        [] { MasterMetricManager::instance().inc_remove_failures(); });
}

tl::expected<long, ErrorCode> WrappedMasterService::RemoveByRegex(
    const std::string& str, bool force) {
    return execute_rpc(
        "RemoveByRegex",
        [&] { return master_service_.RemoveByRegex(str, force); },
        [&](auto& timer) {
            timer.LogRequest("regex=", str, ", force=", force);
        },
        [] { MasterMetricManager::instance().inc_remove_by_regex_requests(); },
        [] { MasterMetricManager::instance().inc_remove_by_regex_failures(); });
}

long WrappedMasterService::RemoveAll(bool force) {
    ScopedVLogTimer timer(1, "RemoveAll");
    timer.LogRequest("action=remove_all_objects, force=", force);
    MasterMetricManager::instance().inc_remove_all_requests();
    long result = master_service_.RemoveAll(force);
    timer.LogResponse("items_removed=", result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchRemove(
    const std::vector<std::string>& keys, bool force) {
    ScopedVLogTimer timer(1, "BatchRemove");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys, ", force=", force);
    MasterMetricManager::instance().inc_remove_requests(total_keys);

    auto results = master_service_.BatchRemove(keys, force);

    size_t failure_count = 0;
    for (const auto& result : results) {
        if (!result.has_value()) {
            failure_count++;
        }
    }
    if (failure_count > 0) {
        MasterMetricManager::instance().inc_remove_failures(failure_count);
    }

    timer.LogResponse("total=", total_keys, ", failures=", failure_count);
    return results;
}

tl::expected<void, ErrorCode> WrappedMasterService::MountSegment(
    const Segment& segment, const UUID& client_id) {
    return execute_rpc(
        "MountSegment",
        [&] { return master_service_.MountSegment(segment, client_id); },
        [&](auto& timer) {
            timer.LogRequest("base=", segment.base, ", size=", segment.size,
                             ", segment_name=", segment.name,
                             ", id=", segment.id);
        },
        [] { MasterMetricManager::instance().inc_mount_segment_requests(); },
        [] { MasterMetricManager::instance().inc_mount_segment_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::ReMountSegment(
    const std::vector<Segment>& segments, const UUID& client_id) {
    return execute_rpc(
        "ReMountSegment",
        [&] { return master_service_.ReMountSegment(segments, client_id); },
        [&](auto& timer) {
            timer.LogRequest("segments_count=", segments.size(),
                             ", client_id=", client_id);
        },
        [] { MasterMetricManager::instance().inc_remount_segment_requests(); },
        [] { MasterMetricManager::instance().inc_remount_segment_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::UnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    return execute_rpc(
        "UnmountSegment",
        [&] { return master_service_.UnmountSegment(segment_id, client_id); },
        [&](auto& timer) {
            timer.LogRequest("segment_id=", segment_id,
                             ", client_id=", client_id);
        },
        [] { MasterMetricManager::instance().inc_unmount_segment_requests(); },
        [] { MasterMetricManager::instance().inc_unmount_segment_failures(); });
}

tl::expected<CopyStartResponse, ErrorCode> WrappedMasterService::CopyStart(
    const UUID& client_id, const std::string& key,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    return execute_rpc(
        "CopyStart",
        [&] {
            return master_service_.CopyStart(client_id, key, src_segment,
                                             tgt_segments);
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", src_segment=", src_segment,
                             ", tgt_segments_count=", tgt_segments.size());
        },
        [] { MasterMetricManager::instance().inc_copy_start_requests(); },
        [] { MasterMetricManager::instance().inc_copy_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::CopyEnd(
    const UUID& client_id, const std::string& key) {
    return execute_rpc(
        "CopyEnd", [&] { return master_service_.CopyEnd(client_id, key); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key);
        },
        [] { MasterMetricManager::instance().inc_copy_end_requests(); },
        [] { MasterMetricManager::instance().inc_copy_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::CopyRevoke(
    const UUID& client_id, const std::string& key) {
    return execute_rpc(
        "CopyRevoke",
        [&] { return master_service_.CopyRevoke(client_id, key); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key);
        },
        [] { MasterMetricManager::instance().inc_copy_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_copy_revoke_failures(); });
}

tl::expected<MoveStartResponse, ErrorCode> WrappedMasterService::MoveStart(
    const UUID& client_id, const std::string& key,
    const std::string& src_segment, const std::string& tgt_segment) {
    return execute_rpc(
        "MoveStart",
        [&] {
            return master_service_.MoveStart(client_id, key, src_segment,
                                             tgt_segment);
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", src_segment=", src_segment,
                             ", tgt_segment=", tgt_segment);
        },
        [] { MasterMetricManager::instance().inc_move_start_requests(); },
        [] { MasterMetricManager::instance().inc_move_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::MoveEnd(
    const UUID& client_id, const std::string& key) {
    return execute_rpc(
        "MoveEnd", [&] { return master_service_.MoveEnd(client_id, key); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key);
        },
        [] { MasterMetricManager::instance().inc_move_end_requests(); },
        [] { MasterMetricManager::instance().inc_move_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::MoveRevoke(
    const UUID& client_id, const std::string& key) {
    return execute_rpc(
        "MoveRevoke",
        [&] { return master_service_.MoveRevoke(client_id, key); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key);
        },
        [] { MasterMetricManager::instance().inc_move_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_move_revoke_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::EvictDiskReplica(
    const UUID& client_id, const std::string& key, ReplicaType replica_type) {
    return execute_rpc(
        "EvictDiskReplica",
        [&] {
            return master_service_.EvictDiskReplica(client_id, key,
                                                    replica_type);
        },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", key=", key,
                             ", replica_type=", replica_type);
        },
        [] {
            MasterMetricManager::instance().inc_evict_disk_replica_requests();
        },
        [] {
            MasterMetricManager::instance().inc_evict_disk_replica_failures();
        });
}

std::vector<tl::expected<void, ErrorCode>>
WrappedMasterService::BatchEvictDiskReplica(
    const UUID& client_id, const std::vector<std::string>& keys,
    ReplicaType replica_type) {
    ScopedVLogTimer timer(1, "BatchEvictDiskReplica");
    const size_t total_keys = keys.size();
    timer.LogRequest("client_id=", client_id, ", keys_count=", total_keys,
                     ", replica_type=", replica_type);
    MasterMetricManager::instance().inc_evict_disk_replica_requests();

    auto results =
        master_service_.BatchEvictDiskReplica(client_id, keys, replica_type);

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            LOG(WARNING) << "BatchEvictDiskReplica failed for key[" << i
                         << "] '" << keys[i]
                         << "': " << toString(results[i].error());
        }
    }
    if (failure_count > 0) {
        MasterMetricManager::instance().inc_evict_disk_replica_failures();
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

tl::expected<UUID, ErrorCode> WrappedMasterService::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return execute_rpc(
        "CreateCopyTask",
        [&] { return master_service_.CreateCopyTask(key, targets); },
        [&](auto& timer) {
            timer.LogRequest("key=", key, ", targets_size=", targets.size());
        },
        [] { MasterMetricManager::instance().inc_create_copy_task_requests(); },
        [] {
            MasterMetricManager::instance().inc_create_copy_task_failures();
        });
}

tl::expected<UUID, ErrorCode> WrappedMasterService::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return execute_rpc(
        "CreateMoveTask",
        [&] { return master_service_.CreateMoveTask(key, source, target); },
        [&](auto& timer) {
            timer.LogRequest("key=", key, ", source=", source,
                             ", target=", target);
        },
        [] { MasterMetricManager::instance().inc_create_move_task_requests(); },
        [] {
            MasterMetricManager::instance().inc_create_move_task_failures();
        });
}

tl::expected<QueryTaskResponse, ErrorCode> WrappedMasterService::QueryTask(
    const UUID& task_id) {
    return execute_rpc(
        "QueryTask", [&] { return master_service_.QueryTask(task_id); },
        [&](auto& timer) { timer.LogRequest("task_id=", task_id); },
        [] { MasterMetricManager::instance().inc_query_task_requests(); },
        [] { MasterMetricManager::instance().inc_query_task_failures(); });
}

tl::expected<std::vector<TaskAssignment>, ErrorCode>
WrappedMasterService::FetchTasks(const UUID& client_id, size_t batch_size) {
    return execute_rpc(
        "FetchTasks",
        [&] { return master_service_.FetchTasks(client_id, batch_size); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id,
                             ", batch_size=", batch_size);
        },
        [] { MasterMetricManager::instance().inc_fetch_tasks_requests(); },
        [] { MasterMetricManager::instance().inc_fetch_tasks_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::MarkTaskToComplete(
    const UUID& client_id, const TaskCompleteRequest& request) {
    return execute_rpc(
        "MarkTaskToComplete",
        [&] { return master_service_.MarkTaskToComplete(client_id, request); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", client_id, ", task_id=", request.id);
        },
        [] { MasterMetricManager::instance().inc_update_task_requests(); },
        [] { MasterMetricManager::instance().inc_update_task_failures(); });
}

tl::expected<std::string, ErrorCode> WrappedMasterService::GetFsdir() {
    ScopedVLogTimer timer(1, "GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto result = master_service_.GetFsdir();

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
WrappedMasterService::GetStorageConfig() {
    ScopedVLogTimer timer(1, "GetStorageConfig");
    timer.LogRequest("action=get_storage_config");

    auto result = master_service_.GetStorageConfig();

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<PingResponse, ErrorCode> WrappedMasterService::Ping(
    const UUID& client_id) {
    ScopedVLogTimer timer(1, "Ping");
    timer.LogRequest("client_id=", client_id);

    MasterMetricManager::instance().inc_ping_requests();

    auto result = master_service_.Ping(client_id);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::string, ErrorCode> WrappedMasterService::ServiceReady() {
    return GetMooncakeStoreVersion();
}

tl::expected<std::vector<std::string>, ErrorCode>
WrappedMasterService::GetAllKeysForAdmin() {
    return master_service_.GetAllKeys();
}

tl::expected<std::vector<std::string>, ErrorCode>
WrappedMasterService::GetAllSegmentsForAdmin() {
    return master_service_.GetAllSegments();
}

tl::expected<std::pair<uint64_t, uint64_t>, ErrorCode>
WrappedMasterService::QuerySegmentForAdmin(const std::string& segment) {
    return master_service_.QuerySegments(segment);
}

tl::expected<void, ErrorCode> WrappedMasterService::MountLocalDiskSegment(
    const UUID& client_id, bool enable_offloading) {
    ScopedVLogTimer timer(1, "MountLocalDiskSegment");
    timer.LogRequest("action=mount_local_disk_segment");
    LOG(INFO) << "Mount local disk segment with client id is : " << client_id
              << ", enable offloading is: " << enable_offloading;
    auto result =
        master_service_.MountLocalDiskSegment(client_id, enable_offloading);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, int64_t, std::hash<std::string>>,
             ErrorCode>
WrappedMasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                             bool enable_offloading) {
    ScopedVLogTimer timer(1, "OffloadObjectHeartbeat");
    timer.LogRequest("action=offload_object_heartbeat");
    auto result =
        master_service_.OffloadObjectHeartbeat(client_id, enable_offloading);
    return result;
}

tl::expected<void, ErrorCode> WrappedMasterService::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    ScopedVLogTimer timer(1, "NotifyOffloadSuccess");
    timer.LogRequest("action=notify_offload_success");

    auto result =
        master_service_.NotifyOffloadSuccess(client_id, keys, metadatas);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UUID, ErrorCode> WrappedMasterService::CreateDrainJob(
    const CreateDrainJobRequest& request) {
    return master_service_.CreateDrainJob(request);
}

tl::expected<QueryJobResponse, ErrorCode> WrappedMasterService::QueryDrainJob(
    const UUID& job_id) {
    return master_service_.QueryDrainJob(job_id);
}

tl::expected<void, ErrorCode> WrappedMasterService::CancelDrainJob(
    const UUID& job_id) {
    return master_service_.CancelDrainJob(job_id);
}

tl::expected<SegmentStatus, ErrorCode> WrappedMasterService::QuerySegmentStatus(
    const std::string& segment_name) {
    return master_service_.QuerySegmentStatus(segment_name);
}

void RegisterRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedMasterService& wrapped_master_service) {
    server.register_handler<&mooncake::WrappedMasterService::ExistKey>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchQueryIp>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchReplicaClear>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::GetReplicaListByRegex>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetReplicaList>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedMasterService::BatchGetReplicaList>(
            &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::PutRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchPutRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UpsertStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UpsertEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UpsertRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchUpsertStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchUpsertEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchUpsertRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::Remove>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveByRegex>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveAll>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchRemove>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::ReMountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UnmountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::Ping>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetFsdir>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::GetStorageConfig>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchExistKey>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::ServiceReady>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::MountLocalDiskSegment>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::OffloadObjectHeartbeat>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::NotifyOffloadSuccess>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CopyStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CopyEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CopyRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MoveStart>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MoveEnd>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::MoveRevoke>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::EvictDiskReplica>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedMasterService::BatchEvictDiskReplica>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CreateCopyTask>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::CreateMoveTask>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::QueryTask>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::FetchTasks>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedMasterService::MarkTaskToComplete>(
            &wrapped_master_service);
}

}  // namespace mooncake
