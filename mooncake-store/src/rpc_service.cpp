#include "rpc_service.h"

#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <chrono>
#include <thread>
#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_metric_manager.h"
#include "master_service.h"
#include "rpc_helper.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

WrappedMasterService::WrappedMasterService(
    const WrappedMasterServiceConfig& config)
    : http_server_(4, config.http_port),
      metric_report_running_(config.enable_metric_reporting) {
    init_http_server();

    if (config.enable_metric_reporting) {
        metric_report_thread_ = std::thread([this]() {
            while (metric_report_running_) {
                std::string metrics_summary =
                    MasterMetricManager::instance().get_summary_string();
                LOG(INFO) << "Master Metrics: " << metrics_summary;
                std::this_thread::sleep_for(
                    std::chrono::seconds(kMetricReportIntervalSeconds));
            }
        });
    }
}

WrappedMasterService::~WrappedMasterService() {
    metric_report_running_ = false;
    if (metric_report_thread_.joinable()) {
        metric_report_thread_.join();
    }
    http_server_.stop();
}

void WrappedMasterService::init_http_server() {
    using namespace coro_http;

    http_server_.set_http_handler<GET>(
        "/metrics", [](coro_http_request& req, coro_http_response& resp) {
            std::string metrics =
                MasterMetricManager::instance().serialize_metrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(metrics));
        });

    http_server_.set_http_handler<GET>(
        "/metrics/summary",
        [](coro_http_request& req, coro_http_response& resp) {
            std::string summary =
                MasterMetricManager::instance().get_summary_string();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    http_server_.set_http_handler<GET>(
        "/get_all_keys", [&](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");

            auto result = GetMasterService().GetAllKeys();
            if (result) {
                std::string ss = "";
                auto keys = result.value();
                for (const auto& key : keys) {
                    ss += key;
                    ss += "\n";
                }
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to get all keys");
            }
        });

    http_server_.set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, "OK");
        });

    http_server_.async_start();
    LOG(INFO) << "HTTP metrics server started on port " << http_server_.port();
}

tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
WrappedMasterService::CalcCacheStats() {
    return MasterMetricManager::instance().calculate_cache_stats();
}

tl::expected<bool, ErrorCode> WrappedMasterService::ExistKey(
    const std::string& key) {
    return execute_rpc(
        "ExistKey", [&] { return GetMasterService().ExistKey(key); },
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

    auto result = GetMasterService().BatchExistKey(keys);

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

    auto result = GetMasterService().BatchQueryIp(client_ids);

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

    auto result = GetMasterService().BatchReplicaClear(object_keys, client_id,
                                                       segment_name);

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
        [&] { return GetMasterService().GetReplicaListByRegex(str); },
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

tl::expected<void, ErrorCode> WrappedMasterService::Remove(
    const std::string& key) {
    return execute_rpc(
        "Remove", [&] { return GetMasterService().Remove(key); },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] { MasterMetricManager::instance().inc_remove_requests(); },
        [] { MasterMetricManager::instance().inc_remove_failures(); });
}

tl::expected<long, ErrorCode> WrappedMasterService::RemoveByRegex(
    const std::string& str) {
    return execute_rpc(
        "RemoveByRegex", [&] { return GetMasterService().RemoveByRegex(str); },
        [&](auto& timer) { timer.LogRequest("regex=", str); },
        [] { MasterMetricManager::instance().inc_remove_by_regex_requests(); },
        [] { MasterMetricManager::instance().inc_remove_by_regex_failures(); });
}

long WrappedMasterService::RemoveAll() {
    ScopedVLogTimer timer(1, "RemoveAll");
    timer.LogRequest("action=remove_all_objects");
    MasterMetricManager::instance().inc_remove_all_requests();
    long result = GetMasterService().RemoveAll();
    timer.LogResponse("items_removed=", result);
    return result;
}

tl::expected<void, ErrorCode> WrappedMasterService::UnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    return execute_rpc(
        "UnmountSegment",
        [&] {
            return GetMasterService().UnmountSegment(segment_id, client_id);
        },
        [&](auto& timer) {
            timer.LogRequest("segment_id=", segment_id,
                             ", client_id=", client_id);
        },
        [] { MasterMetricManager::instance().inc_unmount_segment_requests(); },
        [] { MasterMetricManager::instance().inc_unmount_segment_failures(); });
}

tl::expected<PingResponse, ErrorCode> WrappedMasterService::Ping(
    const UUID& client_id) {
    ScopedVLogTimer timer(1, "Ping");
    timer.LogRequest("client_id=", client_id);

    MasterMetricManager::instance().inc_ping_requests();

    auto result = GetMasterService().Ping(client_id);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::string, ErrorCode> WrappedMasterService::ServiceReady() {
    return GetMooncakeStoreVersion();
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
    server.register_handler<&mooncake::WrappedMasterService::Remove>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveByRegex>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveAll>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::UnmountSegment>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::Ping>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::BatchExistKey>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::ServiceReady>(
        &wrapped_master_service);
}

}  // namespace mooncake
