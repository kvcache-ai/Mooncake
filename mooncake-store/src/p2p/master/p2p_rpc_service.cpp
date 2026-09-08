#include "p2p/master/p2p_rpc_service.h"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <sstream>
#include <thread>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <glog/logging.h>

#include "p2p/master/p2p_master_metric_manager.h"
#include "rpc_helper.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

P2PMasterRpcService::P2PMasterRpcService(
    const P2PMasterConfig& config, ViewVersionId view_version)
    : master_service_(config, view_version),
      http_server_(4, static_cast<uint16_t>(config.metrics.http_port)),
      metric_report_running_(config.metrics.enable_reporting),
      heartbeat_rpc_port_(config.rpc.heartbeat_port) {}

P2PMasterRpcService::~P2PMasterRpcService() {
    metric_report_running_ = false;
    if (metric_report_thread_.joinable()) {
        metric_report_thread_.join();
    }
    http_server_.stop();
}

void P2PMasterRpcService::init() {
    init_http_server();

    if (metric_report_running_) {
        metric_report_thread_ = std::thread([this]() {
            while (metric_report_running_) {
                std::string metrics_summary =
                    P2PMasterMetricManager::instance().get_summary_string();
                LOG(INFO) << "Master Metrics: " << metrics_summary;
                std::this_thread::sleep_for(std::chrono::seconds(
                    kP2PMetricReportIntervalSeconds));
            }
        });
    }
}

void P2PMasterRpcService::init_http_server() {
    using namespace coro_http;

    http_server_.set_http_handler<GET>(
        "/metrics", [](coro_http_request& req, coro_http_response& resp) {
            std::string metrics =
                P2PMasterMetricManager::instance().serialize_metrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(metrics));
        });

    http_server_.set_http_handler<GET>(
        "/metrics/summary",
        [](coro_http_request& req, coro_http_response& resp) {
            std::string summary =
                P2PMasterMetricManager::instance().get_summary_string();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    http_server_.set_http_handler<GET>(
        "/get_all_keys", [&](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");

            auto result = master_service_.GetAllKeys();
            if (result) {
                std::string keys_text;
                for (const auto& key : result.value()) {
                    keys_text += key;
                    keys_text += "\n";
                }
                resp.set_status_and_content(status_type::ok,
                                            std::move(keys_text));
            } else {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to get all keys");
            }
        });

    http_server_.set_http_handler<GET>(
        "/get_key_count",
        [&](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(
                status_type::ok,
                std::to_string(master_service_.GetKeyCount()));
        });

    http_server_.set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, "OK");
        });

    http_server_.set_http_handler<GET>(
        "/batch_query_keys",
        [&](coro_http_request& req, coro_http_response& resp) {
            auto keys_view = req.get_query_value("keys");
            std::vector<std::string> keys;
            if (!keys_view.empty()) {
                std::istringstream iss{std::string(keys_view)};
                std::string key;
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

            std::vector<std::string_view> key_views;
            key_views.reserve(keys.size());
            for (const auto& key : keys) {
                key_views.push_back(key);
            }
            auto results = BatchGetReadRoute(key_views);
            const size_t count = std::min(keys.size(), results.size());

            std::string response{"{\"success\":true,\"data\":{"};
            response.reserve(count * 512);
            for (size_t i = 0; i < count; ++i) {
                if (i > 0) response += ",";
                response += "\"" + keys[i] + "\":";
                if (!results[i].has_value()) {
                    response += "{\"ok\":false,\"error\":\"";
                    response += toString(results[i].error());
                    response += "\"}";
                    continue;
                }

                response += "{\"ok\":true,\"values\":[";
                bool first = true;
                for (const auto& replica : results[i].value().replicas) {
                    if (!replica.is_memory_replica()) continue;
                    std::string json;
                    struct_json::to_json(
                        replica.get_memory_descriptor().buffer_descriptor,
                        json);
                    if (!first) response += ",";
                    response += json;
                    first = false;
                }
                response += "]}";
            }
            response += "}}";

            if (results.size() != keys.size()) {
                LOG(WARNING)
                    << "BatchGetReadRoute size mismatch: keys=" << keys.size()
                    << " results=" << results.size();
            }
            resp.set_status_and_content(status_type::ok, std::move(response));
        });

    http_server_.async_start();
    LOG(INFO) << "HTTP metrics server started on port " << http_server_.port();
}

tl::expected<bool, ErrorCode> P2PMasterRpcService::ExistKey(
    std::string_view key) {
    return execute_rpc(
        "ExistKey", [&] { return master_service_.ExistKey(key); },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] { P2PMasterMetricManager::instance().inc_exist_key_requests(); },
        [] { P2PMasterMetricManager::instance().inc_exist_key_failures(); });
}

std::vector<tl::expected<bool, ErrorCode>> P2PMasterRpcService::BatchExistKey(
    const std::vector<std::string_view>& keys) {
    ScopedVLogTimer timer(1, "BatchExistKey");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    P2PMasterMetricManager::instance().inc_batch_exist_key_requests(total_keys);

    auto result = master_service_.BatchExistKey(keys);
    size_t failure_count = 0;
    for (size_t i = 0; i < result.size(); ++i) {
        if (!result[i].has_value()) {
            ++failure_count;
            LOG(ERROR) << "BatchExistKey failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(result[i].error());
        }
    }
    if (failure_count == total_keys) {
        P2PMasterMetricManager::instance().inc_batch_exist_key_failures(
            failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance().inc_batch_exist_key_partial_success(
            failure_count);
    }
    timer.LogResponse("total=", result.size(), ", success=",
                      result.size() - failure_count,
                      ", failures=", failure_count);
    return result;
}

tl::expected<
    std::unordered_map<std::string, std::vector<P2PRouteDescriptor>>,
    ErrorCode>
P2PMasterRpcService::GetReadRouteByRegex(std::string_view regex) {
    return execute_rpc(
        "GetReadRouteByRegex",
        [&] { return master_service_.GetReadRouteByRegex(regex); },
        [&](auto& timer) { timer.LogRequest("Regex=", regex); },
        [] {
            P2PMasterMetricManager::instance()
                .inc_get_read_route_by_regex_requests();
        },
        [] {
            P2PMasterMetricManager::instance()
                .inc_get_read_route_by_regex_failures();
        });
}

tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>
P2PMasterRpcService::GetReadRoute(
    const P2PGetReadRouteRequest& req) {
    return execute_rpc(
        "GetReadRoute",
        [&] { return master_service_.GetReadRoute(req.key, req.config); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] {
            P2PMasterMetricManager::instance().inc_get_read_route_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_get_read_route_failures();
        });
}

P2PBatchGetReadRouteResponse P2PMasterRpcService::BatchGetReadRoute(
    const P2PBatchGetReadRouteRequest& req) {
    ScopedVLogTimer timer(1, "BatchGetReadRoute");
    const size_t total_requests = req.keys.size();
    timer.LogRequest("requests_count=", total_requests);
    P2PMasterMetricManager::instance().inc_batch_get_read_route_requests(
        total_requests);

    std::vector<tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>>
        results;
    results.reserve(total_requests);
    for (const auto& key : req.keys) {
        results.emplace_back(master_service_.GetReadRoute(key, req.config));
    }

    P2PBatchGetReadRouteResponse response;
    response.responses.resize(total_requests);
    response.error_codes.resize(total_requests, ErrorCode::OK);
    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            ++failure_count;
            auto error = results[i].error();
            response.error_codes[i] = error;
            if (error == ErrorCode::OBJECT_NOT_FOUND ||
                error == ErrorCode::REPLICA_IS_NOT_READY) {
                VLOG(1) << "BatchGetReadRoute failed for key[" << i << "] '"
                        << req.keys[i] << "': " << toString(error);
            } else {
                LOG(ERROR) << "BatchGetReadRoute failed for key[" << i
                           << "] '" << req.keys[i] << "': " << toString(error);
            }
        } else {
            response.responses[i] = std::move(*results[i]);
        }
    }
    if (failure_count == total_requests) {
        P2PMasterMetricManager::instance()
            .inc_batch_get_read_route_failures(failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance()
            .inc_batch_get_read_route_partial_success(failure_count);
    }
    timer.LogResponse("total=", results.size(), ", success=",
                      results.size() - failure_count,
                      ", failures=", failure_count);
    return response;
}

tl::expected<void, ErrorCode> P2PMasterRpcService::UnmountSegment(
    const P2PUnmountSegmentRequest& req) {
    return execute_rpc(
        "UnmountSegment",
        [&] {
            return master_service_.UnmountSegment(req.segment_id,
                                                  req.client_id);
        },
        [&](auto& timer) {
            timer.LogRequest("segment_id=", req.segment_id,
                             ", client_id=", req.client_id);
        },
        [] {
            P2PMasterMetricManager::instance().inc_unmount_segment_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_unmount_segment_failures();
        });
}

tl::expected<void, ErrorCode> P2PMasterRpcService::MountSegment(
    const P2PMountSegmentRequest& req) {
    return execute_rpc(
        "MountSegment",
        [&] { return master_service_.MountSegment(req.segment, req.client_id); },
        [&](auto& timer) {
            timer.LogRequest("segment_name=", req.segment.name,
                             ", client_id=", req.client_id);
        },
        [] {
            P2PMasterMetricManager::instance().inc_mount_segment_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_mount_segment_failures();
        });
}

tl::expected<P2PHeartbeatResponse, ErrorCode>
P2PMasterRpcService::Heartbeat(const P2PHeartbeatRequest& req) {
    ScopedVLogTimer timer(1, "Heartbeat");
    timer.LogRequest("client_id=", req.client_id);
    P2PMasterMetricManager::instance().inc_heartbeat_requests();
    auto result = master_service_.Heartbeat(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<P2PClientStatus, ErrorCode>
P2PMasterRpcService::QueryClientStatus(const UUID& client_id) {
    ScopedVLogTimer timer(1, "QueryClientStatus");
    timer.LogRequest("client_id=", client_id);
    auto result = master_service_.QueryClientStatus(client_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<ViewVersionId, ErrorCode>
P2PMasterRpcService::RegisterClient(
    const P2PRegisterClientRequest& req) {
    return execute_rpc(
        "RegisterClient", [&] { return master_service_.RegisterClient(req); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", req.client_id,
                             ", segments=", req.segments.size());
        },
        [] {
            P2PMasterMetricManager::instance().inc_register_client_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_register_client_failures();
        });
}

tl::expected<ViewVersionId, ErrorCode>
P2PMasterRpcService::UnregisterClient(const UUID& client_id) {
    return execute_rpc(
        "UnregisterClient",
        [&] { return master_service_.UnregisterClient(client_id); },
        [&](auto& timer) { timer.LogRequest("client_id=", client_id); },
        [] {
            P2PMasterMetricManager::instance()
                .inc_unregister_client_requests();
        },
        [] {
            P2PMasterMetricManager::instance()
                .inc_unregister_client_failures();
        });
}

tl::expected<std::string, ErrorCode>
P2PMasterRpcService::ServiceReady() {
    return GetMooncakeStoreVersion();
}

tl::expected<uint32_t, ErrorCode> P2PMasterRpcService::HeartbeatServiceReady() {
    return heartbeat_rpc_port_;
}

tl::expected<std::vector<P2PWriteCandidate>, ErrorCode>
P2PMasterRpcService::GetWriteRoute(const P2PGetWriteRouteRequest& req) {
    return execute_rpc(
        "GetWriteRoute", [&] { return master_service_.GetWriteRoute(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] {
            P2PMasterMetricManager::instance().inc_get_write_route_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_get_write_route_failures();
        });
}

P2PBatchGetWriteRouteResponse P2PMasterRpcService::BatchGetWriteRoute(
    const P2PBatchGetWriteRouteRequest& req) {
    ScopedVLogTimer timer(1, "BatchGetWriteRoute");
    const size_t total = req.keys.size();
    timer.LogRequest("client_id=", req.client_id, ", key_count=", total);
    P2PMasterMetricManager::instance().inc_batch_get_write_route_requests(
        total);

    auto response = master_service_.BatchGetWriteRoute(req);

    size_t failure_count = 0;
    for (size_t i = 0; i < response.error_codes.size(); ++i) {
        if (response.error_codes[i] != ErrorCode::OK) {
            failure_count++;
            LOG(ERROR) << "BatchGetWriteRoute failed for key '" << req.keys[i]
                       << "': " << toString(response.error_codes[i]);
        }
    }
    if (failure_count == total && total > 0) {
        P2PMasterMetricManager::instance().inc_batch_get_write_route_failures(
            failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance()
            .inc_batch_get_write_route_partial_success(failure_count);
    }
    timer.LogResponse("total=", total, ", success=", total - failure_count,
                      ", failures=", failure_count);
    return response;
}

tl::expected<void, ErrorCode> P2PMasterRpcService::PublishRoute(
    const P2PPublishRouteRequest& req) {
    return execute_rpc(
        "PublishRoute", [&] { return master_service_.AddReplica(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] { P2PMasterMetricManager::instance().inc_add_replica_requests(); },
        [] { P2PMasterMetricManager::instance().inc_add_replica_failures(); });
}

tl::expected<void, ErrorCode> P2PMasterRpcService::WithdrawRoute(
    const P2PWithdrawRouteRequest& req) {
    return execute_rpc(
        "WithdrawRoute", [&] { return master_service_.RemoveReplica(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] {
            P2PMasterMetricManager::instance().inc_remove_replica_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_remove_replica_failures();
        });
}

std::vector<tl::expected<void, ErrorCode>>
P2PMasterRpcService::BatchWithdrawRoute(
    const P2PBatchWithdrawRouteRequest& req) {
    ScopedVLogTimer timer(1, "BatchWithdrawRoute");
    const size_t total_requests = req.segment_ids.size();
    timer.LogRequest("key=", req.key, "segment_count=", total_requests);
    P2PMasterMetricManager::instance().inc_batch_remove_replica_requests(
        total_requests);

    auto results = master_service_.BatchRemoveReplica(req);

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchWithdrawRoute failed for key '" << req.key
                       << "', segment_id: " << req.segment_ids[i] << ": "
                       << toString(error);
        }
    }

    if (failure_count == total_requests && total_requests > 0) {
        P2PMasterMetricManager::instance().inc_batch_remove_replica_failures(
            failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance()
            .inc_batch_remove_replica_partial_success(failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

P2PBatchSyncRoutesResponse P2PMasterRpcService::BatchSyncRoutes(
    const P2PBatchSyncRoutesRequest& req) {
    ScopedVLogTimer timer(1, "BatchSyncRoutes");
    timer.LogRequest("client_id=", req.client_id,
                     ", adds=", req.publish_operations.size(),
                     ", removes=", req.withdraw_operations.size());

    auto response = master_service_.BatchSyncRoutes(req);

    size_t add_failures = 0;
    for (auto ec : response.publish_results) {
        if (ec != ErrorCode::OK) add_failures++;
    }
    size_t remove_failures = 0;
    for (auto ec : response.withdraw_results) {
        if (ec != ErrorCode::OK) remove_failures++;
    }

    P2PMasterMetricManager::instance().inc_add_replica_requests(
        req.publish_operations.size());
    P2PMasterMetricManager::instance().inc_add_replica_failures(add_failures);
    P2PMasterMetricManager::instance().inc_remove_replica_requests(
        req.withdraw_operations.size());
    P2PMasterMetricManager::instance().inc_remove_replica_failures(
        remove_failures);
    timer.LogResponse("add_failures=", add_failures,
                      ", remove_failures=", remove_failures);
    return response;
}

tl::expected<void, ErrorCode> P2PMasterRpcService::CompleteRouteSync(
    const UUID& client_id) {
    ScopedVLogTimer timer(1, "CompleteRouteSync");
    timer.LogRequest("client_id=", client_id);

    auto result = master_service_.SetSyncCompleted(client_id);
    if (!result) {
        LOG(ERROR) << "CompleteRouteSync failed: " << toString(result.error());
    }
    return result;
}

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::P2PMasterRpcService& wrapped_master_service,
    bool include_heartbeat) {
    server.register_handler<&P2PMasterRpcService::ExistKey>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::BatchExistKey>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::GetReadRouteByRegex>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::GetReadRoute>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::BatchGetReadRoute>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::UnmountSegment>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::MountSegment>(
        &wrapped_master_service);
    if (include_heartbeat) {
        server.register_handler<&P2PMasterRpcService::Heartbeat>(
            &wrapped_master_service);
    }
    server.register_handler<&P2PMasterRpcService::QueryClientStatus>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::RegisterClient>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::UnregisterClient>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::ServiceReady>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::HeartbeatServiceReady>(
        &wrapped_master_service);

    server.register_handler<&P2PMasterRpcService::GetWriteRoute>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::BatchGetWriteRoute>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::PublishRoute>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::WithdrawRoute>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::BatchWithdrawRoute>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::BatchSyncRoutes>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::CompleteRouteSync>(
        &wrapped_master_service);
}

void RegisterP2PHeartbeatRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::P2PMasterRpcService& wrapped_master_service) {
    server.register_handler<&P2PMasterRpcService::Heartbeat>(
        &wrapped_master_service);
    server.register_handler<&P2PMasterRpcService::ServiceReady>(
        &wrapped_master_service);
}

}  // namespace mooncake
