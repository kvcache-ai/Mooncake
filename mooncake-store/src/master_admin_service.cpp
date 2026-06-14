#include "master_admin_service.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <exception>
#include <map>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>
#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "ha_metric_manager.h"
#include "master_metric_manager.h"
#include "rpc_service.h"
#include "types.h"

namespace mooncake {

const uint64_t kMetricReportIntervalSeconds = 10;

namespace {

std::string AppendMetricSections(std::string primary, std::string secondary) {
    if (!primary.empty() && primary.back() != '\n') {
        primary.push_back('\n');
    }
    primary += secondary;
    return primary;
}

struct HttpErrorResponse {
    bool success{false};
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpErrorResponse, success, error_code, error_message);

struct HttpSimpleErrorResponse {
    bool success{false};
    std::string error;
};
YLT_REFL(HttpSimpleErrorResponse, success, error);

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

void WriteSimpleErrorResponse(coro_http::coro_http_response& resp,
                              coro_http::status_type status,
                              std::string error) {
    HttpSimpleErrorResponse payload;
    payload.error = std::move(error);
    WriteJsonResponse(resp, status, payload);
}

void SetServiceUnavailable(coro_http::coro_http_response& resp,
                           std::string message) {
    WriteErrorResponse(resp, coro_http::status_type::service_unavailable,
                       ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
                       std::move(message));
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

}  // namespace

MasterAdminServer::MasterAdminServer(uint16_t http_port,
                                     bool enable_metric_reporting)
    : http_port_(http_port),
      enable_metric_reporting_(enable_metric_reporting),
      http_server_(4, http_port) {}

MasterAdminServer::~MasterAdminServer() { Stop(); }

bool MasterAdminServer::Start() {
    if (started_.load()) {
        return true;
    }

    RegisterHandler();

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

struct HttpHealthResponse {
    std::string status;
    std::string role;
    std::string ha_state;
    bool service_ready{false};
    std::optional<std::string> leader_address;
    std::optional<uint64_t> view_version;
};
YLT_REFL(HttpHealthResponse, status, role, ha_state, service_ready,
         leader_address, view_version);

void MasterAdminServer::HandleHealth(coro_http::coro_http_request&,
                                     coro_http::coro_http_response& resp) {
    const auto snapshot = SnapshotState();
    HttpHealthResponse payload;
    payload.status = "ok";
    payload.role = ha::MasterRuntimeRoleToString(snapshot.state);
    payload.ha_state = ha::MasterRuntimeStateToString(snapshot.state);
    payload.service_ready = snapshot.service_available;
    if (snapshot.leader_view.has_value()) {
        payload.leader_address = snapshot.leader_view->leader_address;
        payload.view_version = snapshot.leader_view->view_version;
    }
    WriteJsonResponse(resp, coro_http::status_type::ok, payload);
}

struct HttpLeaderResponse {
    bool present{false};
    std::optional<std::string> leader_address;
    std::optional<uint64_t> view_version;
};
YLT_REFL(HttpLeaderResponse, present, leader_address, view_version);

void MasterAdminServer::HandleLeader(coro_http::coro_http_request&,
                                     coro_http::coro_http_response& resp) {
    const auto snapshot = SnapshotState();
    HttpLeaderResponse payload;
    payload.present = snapshot.leader_view.has_value();
    if (snapshot.leader_view.has_value()) {
        payload.leader_address = snapshot.leader_view->leader_address;
        payload.view_version = snapshot.leader_view->view_version;
    }
    WriteJsonResponse(resp, coro_http::status_type::ok, payload);
}

std::shared_ptr<WrappedMasterService> MasterAdminServer::GetActiveService()
    const {
    const auto snapshot = SnapshotState();
    if (!snapshot.service_available) {
        return nullptr;
    }
    return snapshot.service;
}

template <typename Handler>
void MasterAdminServer::WithActiveService(coro_http::coro_http_response& resp,
                                          Handler&& handler) const {
    auto service = GetActiveService();
    if (!service) {
        SetServiceUnavailable(resp, "service plane is not active");
        return;
    }
    std::forward<Handler>(handler)(service);
}

void MasterAdminServer::HandleMetrics(coro_http::coro_http_request&,
                                      coro_http::coro_http_response& resp) {
    resp.add_header("Content-Type", "text/plain; version=0.0.4");
    resp.set_status_and_content(coro_http::status_type::ok, BuildMetricsText());
}

void MasterAdminServer::HandleMetricsSummary(
    coro_http::coro_http_request&, coro_http::coro_http_response& resp) {
    resp.add_header("Content-Type", "text/plain; version=0.0.4");
    resp.set_status_and_content(coro_http::status_type::ok,
                                BuildMetricsSummaryText());
}

void MasterAdminServer::HandleRole(coro_http::coro_http_request&,
                                   coro_http::coro_http_response& resp) {
    const auto snapshot = SnapshotState();
    resp.add_header("Content-Type", "text/plain; charset=utf-8");
    resp.set_status_and_content(coro_http::status_type::ok,
                                ha::MasterRuntimeRoleToString(snapshot.state));
}

void MasterAdminServer::HandleHaStatus(coro_http::coro_http_request&,
                                       coro_http::coro_http_response& resp) {
    const auto snapshot = SnapshotState();
    resp.add_header("Content-Type", "text/plain; charset=utf-8");
    resp.set_status_and_content(coro_http::status_type::ok,
                                ha::MasterRuntimeStateToString(snapshot.state));
}

void MasterAdminServer::HandleQueryKey(coro_http::coro_http_request& req,
                                       coro_http::coro_http_response& resp) {
    WithActiveService(resp, [&](auto service) {
        auto key = req.get_decode_query_value("key");
        auto get_result = service->GetReplicaList(std::string(key), "default");
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        if (get_result) {
            std::string body;
            const auto& replicas = get_result.value().replicas;
            for (const auto& replica : replicas) {
                if (!replica.is_memory_replica()) {
                    continue;
                }
                std::string tmp;
                struct_json::to_json(
                    replica.get_memory_descriptor().buffer_descriptor, tmp);
                body += tmp;
                body += "\n";
            }
            resp.set_status_and_content(coro_http::status_type::ok,
                                        std::move(body));
            return;
        }

        resp.set_status_and_content(coro_http::status_type::not_found,
                                    toString(get_result.error()));
    });
}

void MasterAdminServer::HandleGetAllKeys(coro_http::coro_http_request&,
                                         coro_http::coro_http_response& resp) {
    WithActiveService(resp, [&](auto service) {
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        auto result = service->GetAllKeysForAdmin();
        if (!result) {
            resp.set_status_and_content(
                coro_http::status_type::internal_server_error,
                "Failed to get all keys");
            return;
        }

        std::string body;
        for (const auto& key : result.value()) {
            body += key;
            body += "\n";
        }
        resp.set_status_and_content(coro_http::status_type::ok,
                                    std::move(body));
    });
}

void MasterAdminServer::HandleGetAllSegments(
    coro_http::coro_http_request&, coro_http::coro_http_response& resp) {
    WithActiveService(resp, [&](auto service) {
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        auto result = service->GetAllSegmentsForAdmin();
        if (!result) {
            resp.set_status_and_content(
                coro_http::status_type::internal_server_error,
                "Failed to get all segments");
            return;
        }

        std::string body;
        for (const auto& segment_name : result.value()) {
            body += segment_name;
            body += "\n";
        }
        resp.set_status_and_content(coro_http::status_type::ok,
                                    std::move(body));
    });
}

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

void MasterAdminServer::HandleGetSegmentsDetail(
    coro_http::coro_http_request&, coro_http::coro_http_response& resp) {
    WithActiveService(resp, [&](auto service) {
        auto result = service->GetSegmentsDetailForAdmin();
        if (!result) {
            WriteErrorResponse(resp,
                               coro_http::status_type::internal_server_error,
                               result.error(), "Failed to get segments detail");
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
            size_oss << (info.size_bytes / 1024.0 / 1024.0 / 1024.0) << " GiB";
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
        WriteJsonResponse(resp, coro_http::status_type::ok, payload);
    });
}

void MasterAdminServer::HandleQuerySegment(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    WithActiveService(resp, [&](auto service) {
        auto segment = req.get_decode_query_value("segment");
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        auto result = service->QuerySegmentForAdmin(std::string(segment));
        if (!result) {
            resp.set_status_and_content(
                coro_http::status_type::internal_server_error,
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
        resp.set_status_and_content(coro_http::status_type::ok,
                                    std::move(body));
    });
}

struct HttpCreateDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCreateDrainJobResponse, success, job_id, status, error_code,
         error_message);

void MasterAdminServer::HandleCreateDrainJob(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    CreateDrainJobRequest request;
    try {
        struct_json::from_json(request, req.get_body());
    } catch (const std::exception& e) {
        WriteErrorResponse(resp, coro_http::status_type::bad_request,
                           ErrorCode::INVALID_PARAMS,
                           std::string("Invalid JSON body: ") + e.what());
        return;
    }

    WithActiveService(resp, [&](auto service) {
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
        WriteJsonResponse(resp, coro_http::status_type::ok, payload);
    });
}

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

namespace {

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

void MasterAdminServer::HandleQueryDrainJob(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    auto job_id_result = ParseJobId(req.get_decode_query_value("job_id"));
    if (!job_id_result.has_value()) {
        WriteErrorResponse(resp, coro_http::status_type::bad_request,
                           job_id_result.error(), "Missing or invalid job_id");
        return;
    }

    WithActiveService(resp, [&](auto service) {
        auto result = service->QueryDrainJob(job_id_result.value());
        if (!result.has_value()) {
            WriteErrorResponse(resp, ErrorCodeToHttpStatus(result.error()),
                               result.error());
            return;
        }

        WriteJsonResponse(resp, coro_http::status_type::ok,
                          ToHttpQueryDrainJobResponse(result.value()));
    });
}

struct HttpCancelDrainJobResponse {
    bool success{false};
    std::string job_id;
    std::string status;
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpCancelDrainJobResponse, success, job_id, status, error_code,
         error_message);

void MasterAdminServer::HandleCancelDrainJob(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    auto job_id_result = ParseJobId(req.get_decode_query_value("job_id"));
    if (!job_id_result.has_value()) {
        WriteErrorResponse(resp, coro_http::status_type::bad_request,
                           job_id_result.error(), "Missing or invalid job_id");
        return;
    }

    WithActiveService(resp, [&](auto service) {
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
        WriteJsonResponse(resp, coro_http::status_type::ok, payload);
    });
}

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

void MasterAdminServer::HandleSegmentStatus(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    auto segment_name = req.get_decode_query_value("segment");
    if (segment_name.empty()) {
        WriteErrorResponse(resp, coro_http::status_type::bad_request,
                           ErrorCode::INVALID_PARAMS,
                           "Missing segment query parameter");
        return;
    }

    WithActiveService(resp, [&](auto service) {
        auto result = service->QuerySegmentStatus(std::string(segment_name));
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
        WriteJsonResponse(resp, coro_http::status_type::ok, payload);
    });
}

struct HttpBatchQueryKeyResult {
    bool ok{false};
    std::optional<std::string> error;
    std::optional<std::vector<AllocatedBuffer::Descriptor>> values;
};
YLT_REFL(HttpBatchQueryKeyResult, ok, error, values);

struct HttpBatchQueryKeysResponse {
    bool success{false};
    std::map<std::string, HttpBatchQueryKeyResult> data;
};
YLT_REFL(HttpBatchQueryKeysResponse, success, data);

void MasterAdminServer::HandleBatchQueryKeys(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    auto service = GetActiveService();
    if (!service) {
        WriteSimpleErrorResponse(resp,
                                 coro_http::status_type::service_unavailable,
                                 "service plane is not active");
        return;
    }

    auto keys_str = req.get_decode_query_value("keys");
    std::vector<std::string> keys;
    if (!keys_str.empty()) {
        std::string_view sv(keys_str);
        size_t pos = 0;
        while ((pos = sv.find(',')) != std::string_view::npos) {
            keys.emplace_back(sv.substr(0, pos));
            sv.remove_prefix(pos + 1);
        }
        keys.emplace_back(sv);
    }

    if (keys.empty()) {
        WriteSimpleErrorResponse(resp, coro_http::status_type::bad_request,
                                 "No keys provided. Use ?keys=key1,key2,...");
        return;
    }

    auto results = service->BatchGetReplicaList(keys, "default");
    const size_t n = std::min(keys.size(), results.size());
    HttpBatchQueryKeysResponse payload;
    payload.success = true;

    for (size_t i = 0; i < n; ++i) {
        const auto& result = results[i];
        HttpBatchQueryKeyResult item;
        if (!result.has_value()) {
            item.error = toString(result.error());
            payload.data.emplace(keys[i], std::move(item));
            continue;
        }

        item.ok = true;
        item.values = std::vector<AllocatedBuffer::Descriptor>{};
        for (const auto& replica : result.value().replicas) {
            if (!replica.is_memory_replica()) {
                continue;
            }
            item.values->emplace_back(
                replica.get_memory_descriptor().buffer_descriptor);
        }
        payload.data.emplace(keys[i], std::move(item));
    }

    if (results.size() != keys.size()) {
        LOG(WARNING) << "BatchGetReplicaList size mismatch: keys="
                     << keys.size() << " results=" << results.size();
    }
    WriteJsonResponse(resp, coro_http::status_type::ok, payload);
}

void MasterAdminServer::RegisterHandler() {
    using namespace coro_http;

    http_server_.set_http_handler<GET>(
        "/metrics", [this](coro_http_request& req, coro_http_response& resp) {
            HandleMetrics(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/metrics/summary",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleMetricsSummary(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/health", [this](coro_http_request& req, coro_http_response& resp) {
            HandleHealth(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/role", [this](coro_http_request& req, coro_http_response& resp) {
            HandleRole(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/ha_status", [this](coro_http_request& req, coro_http_response& resp) {
            HandleHaStatus(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/leader", [this](coro_http_request& req, coro_http_response& resp) {
            HandleLeader(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/query_key", [this](coro_http_request& req, coro_http_response& resp) {
            HandleQueryKey(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/get_all_keys",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleGetAllKeys(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/get_all_segments",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleGetAllSegments(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/get_segments_detail",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleGetSegmentsDetail(req, resp);
        });

    http_server_.set_http_handler<GET>(
        "/query_segment",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleQuerySegment(req, resp);
        });
    http_server_.set_http_handler<POST>(
        "/api/v1/drain_jobs",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleCreateDrainJob(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/api/v1/drain_jobs/query",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleQueryDrainJob(req, resp);
        });
    http_server_.set_http_handler<POST>(
        "/api/v1/drain_jobs/cancel",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleCancelDrainJob(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/api/v1/segments/status",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleSegmentStatus(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/batch_query_keys",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleBatchQueryKeys(req, resp);
        });
}
}  // namespace mooncake
