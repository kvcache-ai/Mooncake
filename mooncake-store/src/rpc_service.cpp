#include "rpc_service.h"

#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include <atomic>
#include <chrono>
#include <cstdint>
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

namespace mooncake {

const uint64_t kMetricReportIntervalSeconds = 10;

WrappedMasterService::WrappedMasterService(
    const WrappedMasterServiceConfig& config)
    : master_service_(MasterServiceConfig(config)),
      http_server_(4, config.http_port),
      metric_report_running_(config.enable_metric_reporting) {
    init_http_server();

    MasterMetricManager::instance().set_enable_ha(config.enable_ha);

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
        "/query_key", [&](coro_http_request& req, coro_http_response& resp) {
            auto key = req.get_query_value("key");
            auto get_result = GetReplicaList(std::string(key));
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            if (get_result) {
                std::string ss = "";
                const std::vector<Replica::Descriptor>& replicas =
                    get_result.value().replicas;
                for (size_t i = 0; i < replicas.size(); i++) {
                    if (replicas[i].is_memory_replica()) {
                        auto& memory_descriptors =
                            replicas[i].get_memory_descriptor();
                        for (const auto& handle :
                             memory_descriptors.buffer_descriptors) {
                            std::string tmp = "";
                            struct_json::to_json(handle, tmp);
                            ss += tmp;
                            ss += "\n";
                        }
                    }
                }
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::not_found,
                                            toString(get_result.error()));
            }
        });

    http_server_.set_http_handler<GET>(
        "/get_all_keys", [&](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");

            auto result = master_service_.GetAllKeys();
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
        "/get_all_segments",
        [&](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto result = master_service_.GetAllSegments();
            if (result) {
                std::string ss = "";
                auto segments = result.value();
                for (const auto& segment_name : segments) {
                    ss += segment_name;
                    ss += "\n";
                }
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to get all segments");
            }
        });

    http_server_.set_http_handler<GET>(
        "/query_segment",
        [&](coro_http_request& req, coro_http_response& resp) {
            auto segment = req.get_query_value("segment");
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            auto result = master_service_.QuerySegments(std::string(segment));

            if (result) {
                std::string ss = "";
                auto [used, capacity] = result.value();
                ss += segment;
                ss += "\n";
                ss += "Used(bytes): ";
                ss += std::to_string(used);
                ss += "\nCapacity(bytes) : ";
                ss += std::to_string(capacity);
                ss += "\n";
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to query segment");
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
            LOG(ERROR) << "BatchExistKey failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(result[i].error());
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
            LOG(ERROR) << "BatchGetReplicaList failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(results[i].error());
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
WrappedMasterService::PutStart(const std::string& key,
                               const std::vector<uint64_t>& slice_lengths,
                               const ReplicateConfig& config) {
    return execute_rpc(
        "PutStart",
        [&] { return master_service_.PutStart(key, slice_lengths, config); },
        [&](auto& timer) {
            timer.LogRequest("key=", key,
                             ", slice_lengths=", slice_lengths.size());
        },
        [&] { MasterMetricManager::instance().inc_put_start_requests(); },
        [] { MasterMetricManager::instance().inc_put_start_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::PutEnd(
    const std::string& key, ReplicaType replica_type) {
    return execute_rpc(
        "PutEnd", [&] { return master_service_.PutEnd(key, replica_type); },
        [&](auto& timer) {
            timer.LogRequest("key=", key, ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_end_requests(); },
        [] { MasterMetricManager::instance().inc_put_end_failures(); });
}

tl::expected<void, ErrorCode> WrappedMasterService::PutRevoke(
    const std::string& key, ReplicaType replica_type) {
    return execute_rpc(
        "PutRevoke",
        [&] { return master_service_.PutRevoke(key, replica_type); },
        [&](auto& timer) {
            timer.LogRequest("key=", key, ", replica_type=", replica_type);
        },
        [] { MasterMetricManager::instance().inc_put_revoke_requests(); },
        [] { MasterMetricManager::instance().inc_put_revoke_failures(); });
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
WrappedMasterService::BatchPutStart(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<uint64_t>>& slice_lengths,
    const ReplicateConfig& config) {
    ScopedVLogTimer timer(1, "BatchPutStart");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_start_requests(total_keys);

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
        results;
    results.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        results.emplace_back(
            master_service_.PutStart(keys[i], slice_lengths[i], config));
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            LOG(ERROR) << "BatchPutStart failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(results[i].error());
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

std::vector<tl::expected<void, ErrorCode>> WrappedMasterService::BatchPutEnd(
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "BatchPutEnd");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_end_requests(total_keys);

    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());

    for (const auto& key : keys) {
        results.emplace_back(master_service_.PutEnd(key, ReplicaType::MEMORY));
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            LOG(ERROR) << "BatchPutEnd failed for key[" << i << "] '" << keys[i]
                       << "': " << toString(results[i].error());
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
    const std::vector<std::string>& keys) {
    ScopedVLogTimer timer(1, "BatchPutRevoke");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    MasterMetricManager::instance().inc_batch_put_revoke_requests(total_keys);

    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());

    for (const auto& key : keys) {
        results.emplace_back(
            master_service_.PutRevoke(key, ReplicaType::MEMORY));
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            LOG(ERROR) << "BatchPutRevoke failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(results[i].error());
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
    const std::string& key) {
    return execute_rpc(
        "Remove", [&] { return master_service_.Remove(key); },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] { MasterMetricManager::instance().inc_remove_requests(); },
        [] { MasterMetricManager::instance().inc_remove_failures(); });
}

tl::expected<long, ErrorCode> WrappedMasterService::RemoveByRegex(
    const std::string& str) {
    return execute_rpc(
        "RemoveByRegex", [&] { return master_service_.RemoveByRegex(str); },
        [&](auto& timer) { timer.LogRequest("regex=", str); },
        [] { MasterMetricManager::instance().inc_remove_by_regex_requests(); },
        [] { MasterMetricManager::instance().inc_remove_by_regex_failures(); });
}

long WrappedMasterService::RemoveAll() {
    ScopedVLogTimer timer(1, "RemoveAll");
    timer.LogRequest("action=remove_all_objects");
    MasterMetricManager::instance().inc_remove_all_requests();
    long result = master_service_.RemoveAll();
    timer.LogResponse("items_removed=", result);
    return result;
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

tl::expected<std::string, ErrorCode> WrappedMasterService::GetFsdir() {
    ScopedVLogTimer timer(1, "GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto result = master_service_.GetFsdir();

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

tl::expected<void, ErrorCode> WrappedMasterService::ServiceReady() {
    return {};
}

void RegisterRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedMasterService& wrapped_master_service) {
    server.register_handler<&mooncake::WrappedMasterService::ExistKey>(
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
    server.register_handler<&mooncake::WrappedMasterService::Remove>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveByRegex>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::RemoveAll>(
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
    server.register_handler<&mooncake::WrappedMasterService::BatchExistKey>(
        &wrapped_master_service);
    server.register_handler<&mooncake::WrappedMasterService::ServiceReady>(
        &wrapped_master_service);
}

}  // namespace mooncake