#include "centralized_rpc_service.h"

#include "centralized_master_service.h"
#include "rpc_helper.h"

namespace mooncake {

WrappedCentralizedMasterService::WrappedCentralizedMasterService(
    const WrappedMasterServiceConfig& config)
    : WrappedMasterService(config),
      master_service_(MasterServiceConfig(config)) {
    init_centralized_http_server();
}

void WrappedCentralizedMasterService::init_centralized_http_server() {
    using namespace coro_http;

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

    LOG(INFO) << "Centralized HTTP handlers initialized";
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
WrappedCentralizedMasterService::PutStart(const UUID& client_id,
                                          const std::string& key,
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

tl::expected<void, ErrorCode> WrappedCentralizedMasterService::PutEnd(
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

tl::expected<void, ErrorCode> WrappedCentralizedMasterService::PutRevoke(
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
WrappedCentralizedMasterService::BatchPutStart(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<uint64_t>& slice_lengths, const ReplicateConfig& config) {
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

std::vector<tl::expected<void, ErrorCode>>
WrappedCentralizedMasterService::BatchPutEnd(
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

std::vector<tl::expected<void, ErrorCode>>
WrappedCentralizedMasterService::BatchPutRevoke(
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

tl::expected<void, ErrorCode> WrappedCentralizedMasterService::MountSegment(
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

tl::expected<void, ErrorCode> WrappedCentralizedMasterService::ReMountSegment(
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

tl::expected<std::string, ErrorCode>
WrappedCentralizedMasterService::GetFsdir() {
    ScopedVLogTimer timer(1, "GetFsdir");
    timer.LogRequest("action=get_fsdir");

    auto result = master_service_.GetFsdir();

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
WrappedCentralizedMasterService::GetStorageConfig() {
    ScopedVLogTimer timer(1, "GetStorageConfig");
    timer.LogRequest("action=get_storage_config");

    auto result = master_service_.GetStorageConfig();

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode>
WrappedCentralizedMasterService::MountLocalDiskSegment(const UUID& client_id,
                                                       bool enable_offloading) {
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
WrappedCentralizedMasterService::OffloadObjectHeartbeat(
    const UUID& client_id, bool enable_offloading) {
    ScopedVLogTimer timer(1, "OffloadObjectHeartbeat");
    timer.LogRequest("action=offload_object_heartbeat");
    auto result =
        master_service_.OffloadObjectHeartbeat(client_id, enable_offloading);
    return result;
}

tl::expected<void, ErrorCode>
WrappedCentralizedMasterService::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    ScopedVLogTimer timer(1, "NotifyOffloadSuccess");
    timer.LogRequest("action=notify_offload_success");

    auto result =
        master_service_.NotifyOffloadSuccess(client_id, keys, metadatas);
    timer.LogResponseExpected(result);
    return result;
}

void RegisterCentralizedRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedCentralizedMasterService& wrapped_master_service) {
    RegisterRpcService(server, wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedCentralizedMasterService::PutStart>(
            &wrapped_master_service);
    server.register_handler<&mooncake::WrappedCentralizedMasterService::PutEnd>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::PutRevoke>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::BatchPutStart>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::BatchPutEnd>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::BatchPutRevoke>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::MountSegment>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::ReMountSegment>(
        &wrapped_master_service);
    server
        .register_handler<&mooncake::WrappedCentralizedMasterService::GetFsdir>(
            &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::GetStorageConfig>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::MountLocalDiskSegment>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::OffloadObjectHeartbeat>(
        &wrapped_master_service);
    server.register_handler<
        &mooncake::WrappedCentralizedMasterService::NotifyOffloadSuccess>(
        &wrapped_master_service);
}

}  // namespace mooncake
