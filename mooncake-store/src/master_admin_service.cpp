#include "master_admin_service.h"

#include <algorithm>
#include <chrono>
#include <sstream>
#include <utility>
#include <vector>
#include <glog/logging.h>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_writer.h>

#include "master_metric_manager.h"
#include "rpc_service.h"
#include "types.h"
#include "version.h"

namespace mooncake {
const uint64_t kMetricReportIntervalSeconds = 10;

// TODO(F02-T015, replace-in=UPG1-F14-S65):
// Fixed U: d01447808d10af78011e9203d53796a15a032a03.
// Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
// Stage: UPG1 / F14-S65 / storage observability implementation; due before
// S65 acceptance. Replace the marked central-only fallbacks with fixed U
// master_admin_service.cpp after each named business provider is received.
// Every handler carries its receiver/stage/deadline. Do not close this item
// until all providers and U bodies are restored, with review, verification
// or explicit deferral, and receiving commits. T011 separately owns HA.
namespace {
struct HttpErrorResponse {
    bool success{false};
    int32_t error_code{0};
    std::string error_message;
};
YLT_REFL(HttpErrorResponse, success, error_code, error_message);

template <typename T>
void WriteJsonResponse(coro_http::coro_http_response& resp,
                       coro_http::status_type status, const T& payload) {
    std::string json;
    struct_json::to_json(payload, json);
    resp.add_header("Content-Type", "application/json; charset=utf-8");
    resp.set_status_and_content(status, std::move(json));
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

void SetServiceUnavailable(coro_http::coro_http_response& resp,
                           std::string message) {
    WriteErrorResponse(resp, coro_http::status_type::service_unavailable,
                       ErrorCode::UNAVAILABLE_IN_CURRENT_MODE,
                       std::move(message));
}

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

}  // namespace

MasterAdminServer::MasterAdminServer(uint16_t http_port,
                                     bool enable_metric_reporting,
                                     std::string http_host)
    : http_port_(http_port),
      http_host_(std::move(http_host)),
      enable_metric_reporting_(enable_metric_reporting),
      http_server_(4, http_port, http_host_) {}

MasterAdminServer::~MasterAdminServer() { Stop(); }

bool MasterAdminServer::Start() {
    if (started_.load()) {
        return true;
    }

    // TODO-REF(F02-T015, receiver=UPG1-F14-S65):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S65 / implementation; due before F14-S65 acceptance.
    // Replace this temporary path with fixed U MasterAdminServer::Start
    // storage/HA metric projection and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(WARNING) << "Additional storage and HA metrics are not available in this build";
    RegisterHandler();

    auto ec = http_server_.async_start();
    if (ec.hasResult()) {
        LOG(ERROR) << "Failed to start master admin server on " << http_host_
                   << ":" << http_port_ << ": " << ec.value().message();
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
                                  .get_summary_string()
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
    std::lock_guard<std::mutex> refresh_lock(storage_metrics_refresh_mutex_);
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        service_ = std::move(service);
        if (!service_) {
            service_available_ = false;
        }
    }
    // TODO-REF(F02-T015, receiver=UPG1-F14-S65):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S65 / implementation; due before F14-S65 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::SetServiceDelegate storage projection and its real
    // provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
}

void MasterAdminServer::SetServiceAvailable(bool available) {
    std::lock_guard<std::mutex> refresh_lock(storage_metrics_refresh_mutex_);
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        service_available_ = available && service_ != nullptr;
    }
    // TODO-REF(F02-T015, receiver=UPG1-F14-S65):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S65 / implementation; due before F14-S65 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::SetServiceAvailable storage projection and its real
    // provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
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
    // TODO-REF(F02-T015, receiver=UPG1-F14-S65):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S65 / implementation; due before F14-S65 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::BuildMetricsText and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    return MasterMetricManager::instance().serialize_metrics();
}

std::string MasterAdminServer::BuildMetricsSummaryText() const {
    // TODO-REF(F02-T015, receiver=UPG1-F14-S65):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S65 / implementation; due before F14-S65 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::BuildMetricsSummaryText and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    const auto snapshot = SnapshotState();
    std::ostringstream oss;
    oss << "role=" << ha::MasterRuntimeRoleToString(snapshot.state)
        << ", state=" << ha::MasterRuntimeStateToString(snapshot.state)
        << ", service_ready=" << (snapshot.service_available ? "true" : "false")
        << ", master={" << MasterMetricManager::instance().get_summary_string()
        << "}";
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
YLT_REFL(HttpHealthResponse, status, role, ha_state, service_ready, leader_address, view_version);

struct HttpVersionResponse {
    std::string version;
    std::string display_version;
};
YLT_REFL(HttpVersionResponse, version, display_version);

struct HttpLeaderResponse {
    bool present{false};
    std::optional<std::string> leader_address;
    std::optional<uint64_t> view_version;
};
YLT_REFL(HttpLeaderResponse, present, leader_address, view_version);

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

void MasterAdminServer::HandleVersion(coro_http::coro_http_request&,
                                      coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F14-S49):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S49 / version observability implementation.
    // Due: before S49 acceptance. Replace the real current build identity
    // with U MOONCAKE_DISPLAY_VERSION after its build provider is received;
    // close with installed version/JSON verification, review and receipt.
    WriteJsonResponse(
        resp, coro_http::status_type::ok,
        HttpVersionResponse{.version = GetMooncakeStoreVersion(),
                            .display_version = GetMooncakeStoreBuildInfo()});
}

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
        auto key = req.get_query_value("key");
    // TODO-REF(F02-T015, receiver=UPG1-F05-S44):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F05-S44 / implementation; due before F05-S44 acceptance.
    // Replace this temporary path with fixed U HandleQueryKey tenant-aware
    // GetReplicaList and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
        auto get_result = service->GetReplicaList(std::string(key));
        resp.add_header("Content-Type", "application/json; charset=utf-8");
        if (get_result) {
            std::string ss = "{\"success\":true,\"data\":[";
            const auto& replicas = get_result.value().replicas;
            bool first = true;
            for (const auto& replica : replicas) {
                if (!replica.is_memory_replica()) {
                    continue;
                }
                std::string tmp;
                struct_json::to_json(
                    replica.get_memory_descriptor().buffer_descriptor, tmp);
                if (!first) {
                    ss += ",";
                }
                ss += tmp;
                first = false;
            }
            ss += "]}";
            resp.set_status_and_content(coro_http::status_type::ok,
                                        std::move(ss));
            return;
        }

        resp.set_status_and_content(
            coro_http::status_type::not_found,
            std::string("{\"success\":false,\"error_code\":") +
                std::to_string(toInt(get_result.error())) +
                ",\"error_message\":\"" +
                EscapeJson(toString(get_result.error())) + "\"}");
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

void MasterAdminServer::HandleQuerySegment(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    WithActiveService(resp, [&](auto service) {
        auto segment = req.get_query_value("segment");
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

void MasterAdminServer::HandleBatchQueryKeys(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    using namespace coro_http;
    // TODO-REF(F02-T015, receiver=UPG1-F06-S80):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F06-S80 / implementation; due before F06-S80 acceptance.
    // Replace this temporary path with fixed U HandleBatchQueryKeys and
    // BatchGetReplicaListForAdmin and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    WithActiveService(resp, [&](auto service) {
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

            std::string ss;
            ss.reserve(n * 512);

            ss += "{\"success\":true,\"data\":{";

            for (size_t i = 0; i < n; ++i) {
                if (i > 0) ss += ",";

                const auto& key = keys[i];
                const auto& r = results[i];

                ss += "\"";
                ss += key;
                ss += "\":";

                if (!r.has_value()) {
                    ss += "{\"ok\":false,\"error\":\"";
                    ss += toString(r.error());
                    ss += "\"}";
                    continue;
                }

                ss += "{\"ok\":true,\"values\":[";
                bool first = true;

                const auto& replicas = r.value().replicas;
                for (const auto& rep : replicas) {
                    if (!rep.is_memory_replica()) continue;

                    auto& mem_desc = rep.get_memory_descriptor();
                    std::string tmp;
                    struct_json::to_json(mem_desc.buffer_descriptor, tmp);
                    if (!first) ss += ",";
                    ss += tmp;
                    first = false;
                }
                ss += "]}";
            }

            ss += "}}";

            if (results.size() != keys.size()) {
                LOG(WARNING)
                    << "BatchGetReplicaList size mismatch: keys=" << keys.size()
                    << " results=" << results.size();
            }

            resp.set_status_and_content(status_type::ok, std::move(ss));
            });
}

void MasterAdminServer::HandleGetSegmentsDetail(
    coro_http::coro_http_request&, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F14-S75):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S75 / implementation; due before F14-S75 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleGetSegmentsDetail and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleGetSegmentsDetail: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Segment details is not available in this build");
}

void MasterAdminServer::HandleGetDfsShardCount(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F10-S39):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F10-S39 / implementation; due before F10-S39 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleGetDfsShardCount and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleGetDfsShardCount: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "DFS shard management is not available in this build");
}

async_simple::coro::Lazy<void> MasterAdminServer::HandleExpandDfsShards(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F10-S39):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F10-S39 / implementation; due before F10-S39 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleExpandDfsShards and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleExpandDfsShards: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "DFS shard expansion is not available in this build");
    co_return;
}

void MasterAdminServer::HandleCreateDrainJob(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F14-S23):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S23 / implementation; due before F14-S23 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleCreateDrainJob and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleCreateDrainJob: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Drain jobs is not available in this build");
}

void MasterAdminServer::HandleQueryDrainJob(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F14-S23):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S23 / implementation; due before F14-S23 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleQueryDrainJob and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleQueryDrainJob: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Drain jobs is not available in this build");
}

void MasterAdminServer::HandleCancelDrainJob(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F14-S23):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S23 / implementation; due before F14-S23 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleCancelDrainJob and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleCancelDrainJob: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Drain jobs is not available in this build");
}

void MasterAdminServer::HandleSegmentStatus(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F14-S75):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S75 / implementation; due before F14-S75 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleSegmentStatus and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleSegmentStatus: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Segment status management is not available in this build");
}

void MasterAdminServer::HandleSetSegmentStatus(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F14-S75):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F14-S75 / implementation; due before F14-S75 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleSetSegmentStatus and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleSetSegmentStatus: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Segment status management is not available in this build");
}

void MasterAdminServer::HandleKvEventsStatus(
    coro_http::coro_http_request&, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F19-S47):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F19-S47 / implementation; due before F19-S47 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleKvEventsStatus and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleKvEventsStatus: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "KV event status is not available in this build");
}

void MasterAdminServer::HandleGetTenantQuotas(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F06-S45):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F06-S45 / implementation; due before F06-S45 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleGetTenantQuotas and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleGetTenantQuotas: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Tenant quotas is not available in this build");
}

void MasterAdminServer::HandleUpsertTenantQuota(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F06-S45):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F06-S45 / implementation; due before F06-S45 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleUpsertTenantQuota and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleUpsertTenantQuota: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Tenant quotas is not available in this build");
}

void MasterAdminServer::HandleDeleteTenantQuota(
    coro_http::coro_http_request& req, coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F06-S45):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F06-S45 / implementation; due before F06-S45 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleDeleteTenantQuota and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleDeleteTenantQuota: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Tenant quotas is not available in this build");
}

void MasterAdminServer::HandleRemoveAll(coro_http::coro_http_request& req,
                                        coro_http::coro_http_response& resp) {
    // TODO-REF(F02-T015, receiver=UPG1-F06-S02):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F06-S02 / implementation; due before F06-S02 acceptance.
    // Replace this temporary path with fixed U
    // MasterAdminServer::HandleRemoveAll and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleRemoveAll: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Tenant-scoped remove-all is not available in this build");
}

void MasterAdminServer::HandleSoftPin(coro_http::coro_http_request& req,
                                      coro_http::coro_http_response& resp,
                                      bool unpin) {
    // TODO-REF(F02-T015, receiver=UPG1-F06-S05):
    // Source: UPG1-F02-B05; owner: UPG1-F14-S65 (admin closure).
    // Stage: UPG1 / F06-S05 / implementation; due before F06-S05 acceptance.
    // Replace this temporary path with fixed U MasterAdminServer::HandleSoftPin
    // and its real provider.
    // Close with source/owner comparison, verification, review and receipt SHA.
    LOG(ERROR) << "HandleSoftPin: U business provider is not available yet";
    SetServiceUnavailable(
        resp, "Soft pin management is not available in this build");
}

void MasterAdminServer::RegisterHandler() {
    using namespace coro_http;

    http_server_.set_http_handler<POST>(
        "/api/v1/soft_pin",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleSoftPin(req, resp, false);
        });
    http_server_.set_http_handler<POST>(
        "/api/v1/soft_unpin",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleSoftPin(req, resp, true);
        });

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
        "/version", [this](coro_http_request& req, coro_http_response& resp) {
            HandleVersion(req, resp);
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
        "/kv_events/status",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleKvEventsStatus(req, resp);
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
    http_server_.set_http_handler<GET>(
        "/api/v1/dfs/shard_count",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleGetDfsShardCount(req, resp);
        });
    http_server_.set_http_handler<PUT>(
        "/api/v1/dfs/shard_count",
        [this](coro_http_request& req, coro_http_response& resp) {
            return HandleExpandDfsShards(req, resp);
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
    http_server_.set_http_handler<PUT>(
        "/api/v1/segments/status",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleSetSegmentStatus(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/api/v1/tenant_quotas",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleGetTenantQuotas(req, resp);
        });
    http_server_.set_http_handler<PUT>(
        "/api/v1/tenant_quotas",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleUpsertTenantQuota(req, resp);
        });
    http_server_.set_http_handler<DEL>(
        "/api/v1/tenant_quotas",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleDeleteTenantQuota(req, resp);
        });
    http_server_.set_http_handler<GET>(
        "/batch_query_keys",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleBatchQueryKeys(req, resp);
        });
    http_server_.set_http_handler<POST>(
        "/api/v1/remove_all",
        [this](coro_http_request& req, coro_http_response& resp) {
            HandleRemoveAll(req, resp);
        });
}

}  // namespace mooncake
