#include "master_admin_service.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>
#include <ylt/util/tl/expected.hpp>

#include "ha_metric_manager.h"
#include "master_metric_manager.h"
#include "rpc_service.h"
#include "types.h"

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

struct HttpSegmentDetailItem {
    std::string segment_name;
    std::string segment_id;
    std::string client_id;
    std::string base_address;
    uint64_t size_bytes{0};
    std::string size_human;
    std::string te_endpoint;
    std::string protocol;
    std::string status;
    uint64_t allocator_used_bytes{0};
    uint64_t allocator_capacity_bytes{0};
    double allocator_usage_percent{0.0};
};
YLT_REFL(HttpSegmentDetailItem, segment_name, segment_id, client_id,
         base_address, size_bytes, size_human, te_endpoint, protocol, status,
         allocator_used_bytes, allocator_capacity_bytes,
         allocator_usage_percent);

struct HttpSegmentsDetailResponse {
    uint64_t total_segments{0};
    std::vector<HttpSegmentDetailItem> segments;
};
YLT_REFL(HttpSegmentsDetailResponse, total_segments, segments);

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

MasterAdminServer::MasterAdminServer(uint16_t http_port,
                                     bool enable_metric_reporting)
    : http_port_(http_port),
      enable_metric_reporting_(enable_metric_reporting),
      http_server_(4, http_port) {}

MasterAdminServer::~MasterAdminServer() { Stop(); }

bool MasterAdminServer::Start() {
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
                std::ostringstream log_stream;
                log_stream << "Master Admin Metrics: role="
                           << ha::MasterRuntimeRoleToString(snapshot.state)
                           << ", state="
                           << ha::MasterRuntimeStateToString(snapshot.state)
                           << ", service_ready="
                           << (snapshot.service_available ? "true" : "false")
                           << ", master={"
                           << MasterMetricManager::instance()
                                  .get_summary_string_and_update_snapshot()
                           << "}"
                           << ", ha={"
                           << HAMetricManager::instance().get_summary_string()
                           << "}";
                if (snapshot.leader_view.has_value()) {
                    log_stream
                        << ", leader=" << snapshot.leader_view->leader_address
                        << ", view_version="
                        << snapshot.leader_view->view_version;
                }
                LOG(INFO) << log_stream.str();
                if (metric_report_stop_sem_.try_acquire_for(
                        std::chrono::seconds(kMetricReportIntervalSeconds))) {
                    break;
                }
            }
        });
    }

    LOG(INFO) << "Master admin server started on port " << http_server_.port();
    return true;
}

void MasterAdminServer::Stop() {
    metric_report_running_.store(false, std::memory_order_relaxed);
    if (started_.exchange(false)) {
        http_server_.stop();
    }
    if (metric_report_thread_.joinable()) {
        metric_report_stop_sem_.release();
        metric_report_thread_.join();
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
            auto get_result =
                service->GetReplicaList(std::string(key), "default");
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
        "/get_segments_detail",
        [this](coro_http_request&, coro_http_response& resp) {
            auto service = GetActiveService();
            if (!service) {
                SetServiceUnavailable(resp, "service plane is not active");
                return;
            }

            auto result = service->GetSegmentsDetailForAdmin();
            if (!result) {
                WriteErrorResponse(resp, status_type::internal_server_error,
                                   result.error(),
                                   "Failed to get segments detail");
                return;
            }

            HttpSegmentsDetailResponse payload;
            payload.total_segments = result.value().size();
            for (const auto& info : result.value()) {
                HttpSegmentDetailItem item;
                item.segment_name = info.segment_name;
                item.segment_id = UuidToString(info.segment_id);
                item.client_id = UuidToString(info.client_id);

                std::ostringstream addr_oss;
                addr_oss << "0x" << std::hex << info.base_address;
                item.base_address = addr_oss.str();

                item.size_bytes = info.size_bytes;
                std::ostringstream size_oss;
                size_oss << (info.size_bytes / 1024.0 / 1024.0 / 1024.0)
                         << " GiB";
                item.size_human = size_oss.str();

                item.te_endpoint = info.te_endpoint;
                item.protocol = info.protocol;
                item.status = EnumToString(info.status);
                item.allocator_used_bytes = info.allocator_used_bytes;
                item.allocator_capacity_bytes = info.allocator_capacity_bytes;
                item.allocator_usage_percent =
                    info.allocator_capacity_bytes > 0
                        ? (static_cast<double>(info.allocator_used_bytes) /
                           info.allocator_capacity_bytes * 100.0)
                        : 0.0;
                payload.segments.push_back(std::move(item));
            }
            WriteJsonResponse(resp, status_type::ok, payload);
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

            auto results = service->BatchGetReplicaList(keys, "default");
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

}  // namespace mooncake
