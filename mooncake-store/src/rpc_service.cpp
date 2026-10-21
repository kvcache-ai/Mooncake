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

#include "master_metric_manager.h"
#include "master_service.h"
#include "rpc_helper.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

const uint64_t kMetricReportIntervalSeconds = 10;

WrappedMasterService::WrappedMasterService(
    const WrappedMasterServiceConfig& config)
    : master_service_(MasterServiceConfig(config)),
      http_server_(4, config.http_port),
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
                        std::string tmp = "";
                        struct_json::to_json(
                            memory_descriptors.buffer_descriptor, tmp);
                        ss += tmp;
                        ss += "\n";
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

    http_server_.set_http_handler<GET>(
        "/batch_query_keys",
        [&](coro_http_request& req, coro_http_response& resp) {
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

            auto results = this->BatchGetReplicaList(keys);
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
