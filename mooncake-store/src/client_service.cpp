#include "client_service.h"
#include "file_storage.h"

#include <glog/logging.h>

#include <csignal>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <ranges>
#include <thread>

#include "transfer_engine.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

uint64_t calculate_total_size(const Replica::Descriptor& replica) {
    uint64_t total_length = 0;
    if (replica.is_disk_replica()) {
        auto& disk_descriptor = replica.get_disk_descriptor();
        total_length = disk_descriptor.object_size;
    } else if (replica.is_local_disk_replica()) {
        total_length = replica.get_local_disk_descriptor().object_size;
    } else {
        total_length = replica.get_memory_descriptor().buffer_descriptor.size_;
    }
    return total_length;
}

int allocateSlices(std::vector<Slice>& slices,
                   const Replica::Descriptor& replica, void* buffer_ptr) {
    if (replica.is_disk_replica()) {
        // For disk-based replica, split into slices based on file size
        uint64_t offset = 0;
        uint64_t total_length = replica.get_disk_descriptor().object_size;
        while (offset < total_length) {
            auto chunk_size = std::min(total_length - offset, kMaxSliceSize);
            void* chunk_ptr = static_cast<char*>(buffer_ptr) + offset;
            slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }
    } else if (replica.is_local_disk_replica()) {
        slices.emplace_back(
            Slice{buffer_ptr, replica.get_local_disk_descriptor().object_size});
    } else if (replica.is_memory_replica()) {
        // For memory-based replica, split into slices based on buffer
        // descriptors
        auto& handle = replica.get_memory_descriptor().buffer_descriptor;
        void* chunk_ptr = buffer_ptr;
        slices.emplace_back(Slice{chunk_ptr, handle.size_});
    }
    return 0;
}

Client::Client(const std::string& local_hostname,
               const std::string& metadata_connstring,
               const std::string& protocol,
               const std::map<std::string, std::string>& labels)
    : client_id_(generate_uuid()),
      metrics_(ClientMetric::Create(merge_labels(labels))),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr),
      local_hostname_(local_hostname),
      metadata_connstring_(metadata_connstring),
      protocol_(protocol),
      write_thread_pool_(2),
      client_requester_(std::make_unique<ClientRequester>()) {
    LOG(INFO) << "client_id=" << client_id_;

    if (metrics_) {
        if (metrics_->GetReportingInterval() > 0) {
            LOG(INFO) << "Client metrics enabled with reporting thread started "
                         "(interval: "
                      << metrics_->GetReportingInterval() << "s)";
        } else {
            LOG(INFO)
                << "Client metrics enabled but reporting disabled (interval=0)";
        }
    } else {
        LOG(INFO) << "Client metrics disabled (set MC_STORE_CLIENT_METRIC=1 to "
                     "enable)";
    }
}

Client::~Client() { Stop(); }

void Client::Stop() {
    if (!local_inflight_tracker_.Close()) {
        return;
    }
    local_inflight_tracker_.Wait();
    StopClientRpcServer();
    file_storage_.reset();
    // No admitted producer can enqueue more asynchronous writes.
    write_thread_pool_.stop();
    // Make a copy of mounted_segments_ to avoid modifying while iterating
    std::vector<Segment> segments_to_unmount;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        segments_to_unmount.reserve(mounted_segments_.size());
        for (auto& entry : mounted_segments_) {
            segments_to_unmount.emplace_back(entry.second);
        }
    }

    for (auto& segment : segments_to_unmount) {
        auto result =
            UnmountSegment(reinterpret_cast<void*>(segment.base), segment.size);
        if (!result) {
            LOG(ERROR) << "Failed to unmount segment: "
                       << toString(result.error());
        }
    }

    // Clear any remaining segments
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_.clear();
    }

    // Stop ping thread only after no need to contact master anymore
    if (ping_running_) {
        ping_running_ = false;
        if (ping_thread_.joinable()) {
            ping_thread_.join();
        }
    }
}

ErrorCode Client::ConnectToMaster(const std::string& master_server_entry) {
    if (master_server_entry.rfind("redis://", 0) == 0) {
        LOG(ERROR) << "Centralized master discovery does not support Redis";
        return ErrorCode::INVALID_PARAMS;
    }
    if (master_server_entry.find("etcd://") == 0) {
        std::string etcd_entry = master_server_entry.substr(strlen("etcd://"));

        // Get master address from etcd
        auto err = master_view_helper_.ConnectToEtcd(etcd_entry);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd";
            return err;
        }
        std::string master_address;
        ViewVersionId master_version = 0;
        err = master_view_helper_.GetMasterView(master_address, master_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get master address";
            return err;
        }

        err = master_client_.Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        // Start ping thread to monitor master health and trigger remount if
        // needed.
        ping_running_ = true;
        bool is_ha_mode = true;
        std::string current_master_address = master_address;
        ping_thread_ = std::thread([this, is_ha_mode,
                                    current_master_address]() mutable {
            this->PingThreadMain(is_ha_mode, std::move(current_master_address));
        });

        return ErrorCode::OK;
    } else {
        auto err = master_client_.Connect(master_server_entry);
        if (err != ErrorCode::OK) {
            return err;
        }
        // Non-HA mode also enables heartbeat/ping
        ping_running_ = true;
        bool is_ha_mode = false;
        std::string current_master_address = master_server_entry;
        ping_thread_ = std::thread([this, is_ha_mode,
                                    current_master_address]() mutable {
            this->PingThreadMain(is_ha_mode, std::move(current_master_address));
        });
        return ErrorCode::OK;
    }
}

ErrorCode Client::InitTransferEngine(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    auto error = resources_.InitTransferEngine(
        local_hostname, metadata_connstring, protocol, device_names);
    if (error == ErrorCode::OK) {
        local_hostname_ = resources_.GetLocalEndpoint();
    }
    return error;
}

void Client::InitTransferSubmitter() {
    // Initialize TransferSubmitter after transfer engine is ready
    // Keep using logical local_hostname for name-based behaviors; endpoint is
    // used separately where needed.
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *resources_.GetTransferEngine(), storage_backend_,
        metrics_ ? &metrics_->transfer_metric : nullptr);
}

std::optional<std::shared_ptr<Client>> Client::Create(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol, const std::optional<std::string>& device_names,
    const std::string& master_server_entry,
    const std::shared_ptr<TransferEngine>& transfer_engine,
    std::map<std::string, std::string> labels) {
    auto client = std::shared_ptr<Client>(
        new Client(local_hostname, metadata_connstring, protocol, labels));

    ErrorCode err = client->ConnectToMaster(master_server_entry);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    // Initialize storage backend if storage_root_dir is valid
    auto config_response = client->master_client_.GetStorageConfig();
    if (!config_response) {
        LOG(ERROR) << "Failed to get storage config from master";
        // Fallback to GetFsdir for backward compatibility
        auto response = client->master_client_.GetFsdir();
        if (!response) {
            LOG(ERROR) << "Failed to get fsdir from master";
        } else if (response.value().empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            auto dir_string = response.value();
            size_t pos = dir_string.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir = dir_string.substr(0, pos);
                std::string fs_subdir = dir_string.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                // Initialize storage backend with default eviction settings
                client->PrepareStorageBackend(storage_root_dir, fs_subdir, true,
                                              0);
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << dir_string;
            }
        }
    } else {
        auto config = config_response.value();
        if (config.fsdir.empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            size_t pos = config.fsdir.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir = config.fsdir.substr(0, pos);
                std::string fs_subdir = config.fsdir.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                LOG(INFO) << "Disk eviction enabled: "
                          << config.enable_disk_eviction;
                LOG(INFO) << "Quota bytes: " << config.quota_bytes;
                // Initialize storage backend with config from master
                client->PrepareStorageBackend(storage_root_dir, fs_subdir,
                                              config.enable_disk_eviction,
                                              config.quota_bytes);
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << config.fsdir;
            }
        }
    }

    // this only performs RPC calls
    if (protocol == "rpc_only") {
        LOG(INFO) << "Use rpc only. Skip initializing transfer engine.";
        return client;
    }

    // Initialize transfer engine
    if (transfer_engine == nullptr) {
        err = client->InitTransferEngine(local_hostname, metadata_connstring,
                                         protocol, device_names);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize transfer engine";
            return std::nullopt;
        }
    } else {
        client->resources_.InitTransferEngine(local_hostname, transfer_engine);
        LOG(INFO) << "Use existing transfer engine instance. Skip its "
                     "initialization.";
    }

    client->InitTransferSubmitter();

    return client;
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          std::vector<Slice>& slices) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "Get rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto query_result = QueryImpl(object_key);
    if (!query_result) {
        return tl::unexpected(query_result.error());
    }
    return GetImpl(object_key, query_result.value(), slices);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "BatchGet rejected: client is shutting down";
        return std::vector<tl::expected<void, ErrorCode>>(
            object_keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    auto batched_query_results = BatchQueryImpl(object_keys);

    // If any queries failed, return error results immediately for failed
    // queries
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_keys.size());

    std::vector<QueryResult> valid_query_results;
    std::vector<size_t> valid_indices;
    std::vector<std::string> valid_keys;

    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            valid_query_results.emplace_back(batched_query_results[i].value());
            valid_indices.emplace_back(i);
            valid_keys.emplace_back(object_keys[i]);
            results.emplace_back();  // placeholder for successful results
        } else {
            results.emplace_back(
                tl::unexpected(batched_query_results[i].error()));
        }
    }

    // If we have any valid queries, process them
    if (!valid_keys.empty()) {
        std::unordered_map<std::string, std::vector<Slice>> valid_slices;
        for (const auto& key : valid_keys) {
            auto it = slices.find(key);
            if (it != slices.end()) {
                valid_slices[key] = it->second;
            }
        }

        auto valid_results =
            BatchGetImpl(valid_keys, valid_query_results, valid_slices);

        // Merge results back
        for (size_t i = 0; i < valid_indices.size(); ++i) {
            results[valid_indices[i]] = valid_results[i];
        }
    }

    return results;
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
Client::BatchQueryIp(const std::vector<UUID>& client_ids) {
    auto result = master_client_.BatchQueryIp(client_ids);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
Client::QueryByRegex(const std::string& str) {
    auto result = master_client_.GetReplicaListByRegex(str);
    return result;
}

tl::expected<QueryResult, ErrorCode> Client::Query(
    const std::string& object_key) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "Query rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return QueryImpl(object_key);
}

tl::expected<QueryResult, ErrorCode> Client::QueryImpl(
    const std::string& object_key) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto result = master_client_.GetReplicaList(object_key);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return QueryResult(
        std::move(result.value().replicas),
        start_time + std::chrono::milliseconds(result.value().lease_ttl_ms));
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQuery(
    const std::vector<std::string>& object_keys) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "BatchQuery rejected: client is shutting down";
        return std::vector<tl::expected<QueryResult, ErrorCode>>(
            object_keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    return BatchQueryImpl(object_keys);
}

std::vector<tl::expected<QueryResult, ErrorCode>> Client::BatchQueryImpl(
    const std::vector<std::string>& object_keys) {
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto response = master_client_.BatchGetReplicaList(object_keys);

    // Check if we got the expected number of responses
    if (response.size() != object_keys.size()) {
        LOG(ERROR) << "BatchQuery response size mismatch. Expected: "
                   << object_keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<QueryResult, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }
    std::vector<tl::expected<QueryResult, ErrorCode>> results;
    results.reserve(response.size());
    for (size_t i = 0; i < response.size(); ++i) {
        if (response[i]) {
            results.emplace_back(QueryResult(
                std::move(response[i].value().replicas),
                start_time + std::chrono::milliseconds(
                                 response[i].value().lease_ttl_ms)));
        } else {
            results.emplace_back(tl::unexpected(response[i].error()));
        }
    }
    return results;
}

tl::expected<std::vector<std::string>, ErrorCode> Client::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    auto result =
        master_client_.BatchReplicaClear(object_keys, client_id, segment_name);
    return result;
}

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          const QueryResult& query_result,
                                          std::vector<Slice>& slices) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "Get rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return GetImpl(object_key, query_result, slices);
}

tl::expected<void, ErrorCode> Client::GetImpl(const std::string& object_key,
                                              const QueryResult& query_result,
                                              std::vector<Slice>& slices) {
    // Find the first complete replica
    Replica::Descriptor replica;
    ErrorCode err = FindFirstCompleteReplica(query_result.replicas, replica);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }

    auto t0_get = std::chrono::steady_clock::now();
    err = TransferRead(replica, slices);
    auto us_get = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_get)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.get_latency_us.observe(us_get);
    }

    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_failed key=" << object_key;
        return tl::unexpected(err);
    }
    if (query_result.IsLeaseExpired()) {
        LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                     << object_key;
        return tl::unexpected(ErrorCode::LEASE_EXPIRED);
    }
    return {};
}

struct BatchGetOperation {
    std::vector<Replica::Descriptor> replicas;
    std::vector<std::vector<Slice>> batched_slices;
    std::vector<size_t> key_indexes;
    std::vector<TransferFuture> futures;
};

std::vector<tl::expected<void, ErrorCode>> Client::BatchGetWhenPreferSameNode(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.resize(object_keys.size());

    std::unordered_map<std::string, BatchGetOperation> seg_to_op_map{};
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& replica_list = query_results[i].replicas;
        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }
        Replica::Descriptor replica;
        ErrorCode err = FindFirstCompleteReplica(replica_list, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }
        if (!replica.is_memory_replica()) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        auto& op = seg_to_op_map[seg];
        op.replicas.emplace_back(replica);
        op.batched_slices.emplace_back(slices_it->second);
        op.key_indexes.emplace_back(i);
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        auto future = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::READ);
        if (!future) {
            for (auto index : op.key_indexes) {
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
            continue;
        }
        op.futures.emplace_back(std::move(*future));
    }
    for (auto& seg_to_op : seg_to_op_map) {
        auto& op = seg_to_op.second;
        if (op.futures.empty()) {
            continue;
        }
        ErrorCode result = op.futures[0].get();
        if (result != ErrorCode::OK) {
            for (auto index : op.key_indexes) {
                results[index] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << object_keys[index];
            }
        } else {
            for (auto index : op.key_indexes) {
                VLOG(1) << "Transfer completed successfully for key: "
                        << object_keys[index];
                results[index] = {};
            }
        }
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices,
    bool prefer_alloc_in_same_node) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "BatchGet rejected: client is shutting down";
        return std::vector<tl::expected<void, ErrorCode>>(
            object_keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    return BatchGetImpl(object_keys, query_results, slices,
                        prefer_alloc_in_same_node);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGetImpl(
    const std::vector<std::string>& object_keys,
    const std::vector<QueryResult>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices,
    bool prefer_alloc_in_same_node) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Validate input size consistency
    if (query_results.size() != object_keys.size()) {
        LOG(ERROR) << "Query results size (" << query_results.size()
                   << ") doesn't match object keys size (" << object_keys.size()
                   << ")";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }
    if (prefer_alloc_in_same_node) {
        return BatchGetWhenPreferSameNode(object_keys, query_results, slices);
    }

    // Collect all transfer operations for parallel execution
    std::vector<std::tuple<size_t, std::string, TransferFuture>>
        pending_transfers;
    std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());
    // Record batch get transfer latency (Submit + Wait)
    auto t0_batch_get = std::chrono::steady_clock::now();

    // Submit all transfers in parallel
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& query_result = query_results[i];

        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        // Find the first complete replica for this key
        Replica::Descriptor replica;
        ErrorCode err =
            FindFirstCompleteReplica(query_result.replicas, replica);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }

        // Submit transfer operation asynchronously
        auto future = transfer_submitter_->submit(replica, slices_it->second,
                                                  TransferRequest::READ);
        if (!future) {
            LOG(ERROR) << "Failed to submit transfer operation for key: "
                       << key;
            results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            continue;
        }

        VLOG(1) << "Submitted transfer for key " << key
                << " using strategy: " << static_cast<int>(future->strategy());

        pending_transfers.emplace_back(i, key, std::move(*future));
    }

    // Wait for all transfers to complete
    for (auto& [index, key, future] : pending_transfers) {
        ErrorCode result = future.get();
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " with error: " << static_cast<int>(result);
            results[index] = tl::unexpected(result);
        } else {
            VLOG(1) << "Transfer completed successfully for key: " << key;
            results[index] = {};
        }
    }

    // As lease expired is a rare case, we check all the results with the same
    // time_point to avoid too many syscalls
    std::chrono::steady_clock::time_point now =
        std::chrono::steady_clock::now();
    for (size_t i = 0; i < object_keys.size(); ++i) {
        if (results[i].has_value() && query_results[i].IsLeaseExpired(now)) {
            LOG(WARNING) << "lease_expired_before_data_transfer_completed key="
                         << object_keys[i];
            results[i] = tl::unexpected(ErrorCode::LEASE_EXPIRED);
        }
    }

    auto us_batch_get = std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - t0_batch_get)
                            .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_get_latency_us.observe(us_batch_get);
    }

    VLOG(1) << "BatchGet completed for " << object_keys.size() << " keys";
    return results;
}

tl::expected<void, ErrorCode> Client::Put(
    const ObjectKey& key, std::vector<Slice>& slices,
    const std::optional<ReplicateConfig>& config_opt) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "Put rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    ReplicateConfig default_config;
    const auto& config = config_opt ? *config_opt : default_config;

    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    ReplicateConfig client_cfg = config;
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }

    // Start put operation
    auto start_result = master_client_.PutStart(key, slice_lengths, client_cfg);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            VLOG(1) << "object_already_exists key=" << key;
            return {};
        }
        if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
            LOG(WARNING) << "Failed to start put operation for key=" << key
                         << PUT_NO_SPACE_HELPER_STR;
        } else {
            LOG(ERROR) << "Failed to start put operation for key=" << key
                       << ": " << toString(err);
        }
        return tl::unexpected(err);
    }

    // Record Put transfer latency (all replicas)
    auto t0_put = std::chrono::steady_clock::now();

    // We must deal with disk replica first, then the disk putrevoke/putend can
    // be called surely
    if (storage_backend_) {
        for (auto it = start_result.value().rbegin();
             it != start_result.value().rend(); ++it) {
            const auto& replica = *it;
            if (replica.is_disk_replica()) {
                // Store to local file if storage backend is available
                auto disk_descriptor = replica.get_disk_descriptor();
                PutToLocalFile(key, slices, disk_descriptor);
                break;  // Only one disk replica is needed
            }
        }
    }

    for (const auto& replica : start_result.value()) {
        if (replica.is_memory_replica()) {
            // Transfer data using allocated handles from all replicas
            ErrorCode transfer_err = TransferWrite(replica, slices);
            if (transfer_err != ErrorCode::OK) {
                // Revoke put operation
                auto revoke_result =
                    master_client_.PutRevoke(key, ReplicaType::MEMORY);
                if (!revoke_result) {
                    LOG(ERROR) << "Failed to revoke put operation";
                    return tl::unexpected(revoke_result.error());
                }
                return tl::unexpected(transfer_err);
            }
        }
    }

    auto us_put = std::chrono::duration_cast<std::chrono::microseconds>(
                      std::chrono::steady_clock::now() - t0_put)
                      .count();
    if (metrics_) {
        metrics_->transfer_metric.put_latency_us.observe(us_put);
    }

    // End put operation
    auto end_result = master_client_.PutEnd(key, ReplicaType::MEMORY);
    if (!end_result) {
        ErrorCode err = end_result.error();
        LOG(ERROR) << "Failed to end put operation: " << err;
        return tl::unexpected(err);
    }

    return {};
}

// TODO: `client.cpp` is too long, consider split it into multiple files
enum class PutOperationState {
    PENDING,
    MASTER_FAILED,
    TRANSFER_FAILED,
    FINALIZE_FAILED,
    SUCCESS
};

class PutOperation {
   public:
    PutOperation(std::string_view k, const std::vector<Slice>& s)
        : key(k), slices(s) {
        value_length = CalculateSliceSize(slices);
        // Initialize with a pending error state to ensure result is always set
        result = tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }

    std::string key;
    std::vector<Slice> slices;
    size_t value_length;
    std::vector<std::vector<Slice>> batched_slices;

    // Enhanced state tracking
    PutOperationState state = PutOperationState::PENDING;
    tl::expected<void, ErrorCode> result;
    std::vector<Replica::Descriptor> replicas;
    std::vector<TransferFuture> pending_transfers;

    // Error context for debugging
    std::optional<std::string> failure_context;

    // Helper methods for robust state management
    void SetSuccess() {
        state = PutOperationState::SUCCESS;
        result = {};
        failure_context.reset();
    }

    void SetError(ErrorCode error, const std::string& context = "") {
        result = tl::unexpected(error);
        if (!context.empty()) {
            failure_context = toString(error) + ": " + context + "; " +
                              failure_context.value_or("");
        }

        // Update state based on current processing stage
        if (replicas.empty()) {
            state = PutOperationState::MASTER_FAILED;
        } else if (pending_transfers.empty()) {
            state = PutOperationState::TRANSFER_FAILED;
        } else {
            state = PutOperationState::FINALIZE_FAILED;
        }
    }

    bool IsResolved() const { return state != PutOperationState::PENDING; }

    bool IsSuccessful() const {
        return state == PutOperationState::SUCCESS && result.has_value();
    }
};

std::vector<PutOperation> Client::CreatePutOperations(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    std::vector<PutOperation> ops;
    ops.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ops.emplace_back(keys[i], batched_slices[i]);
    }
    return ops;
}

void Client::StartBatchPut(std::vector<PutOperation>& ops,
                           const ReplicateConfig& config) {
    std::vector<std::string> keys;
    std::vector<std::vector<uint64_t>> slice_lengths;

    keys.reserve(ops.size());
    slice_lengths.reserve(ops.size());

    for (const auto& op : ops) {
        keys.emplace_back(op.key);

        std::vector<uint64_t> slice_sizes;
        slice_sizes.reserve(op.slices.size());
        for (const auto& slice : op.slices) {
            slice_sizes.emplace_back(slice.size);
        }
        slice_lengths.emplace_back(std::move(slice_sizes));
    }

    auto start_responses =
        master_client_.BatchPutStart(keys, slice_lengths, config);

    // Ensure response size matches request size
    if (start_responses.size() != ops.size()) {
        LOG(ERROR) << "BatchPutStart response size mismatch: expected "
                   << ops.size() << ", got " << start_responses.size();
        for (auto& op : ops) {
            op.SetError(ErrorCode::RPC_FAIL,
                        "BatchPutStart response size mismatch");
        }
        return;
    }

    // Process individual responses with robust error handling
    for (size_t i = 0; i < ops.size(); ++i) {
        if (!start_responses[i]) {
            ops[i].SetError(start_responses[i].error(),
                            "Master failed to start put operation");
        } else {
            ops[i].replicas = start_responses[i].value();
            // Operation continues to next stage - result remains INTERNAL_ERROR
            // until fully successful
            VLOG(1) << "Successfully started put for key " << ops[i].key
                    << " with " << ops[i].replicas.size() << " replicas";
        }
    }
}

void Client::SubmitTransfers(std::vector<PutOperation>& ops) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        for (auto& op : ops) {
            op.SetError(ErrorCode::INVALID_PARAMS,
                        "TransferSubmitter not initialized");
        }
        return;
    }

    for (auto& op : ops) {
        // Skip operations that already failed in previous stages
        if (op.IsResolved()) {
            continue;
        }

        // Skip operations that don't have replicas (failed in StartBatchPut)
        if (op.replicas.empty()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "No replicas available for transfer");
            continue;
        }

        bool all_transfers_submitted = true;
        std::string failure_context;

        // We must deal with disk replica first, then the disk putrevoke/putend
        // can be called surely
        if (storage_backend_) {
            for (auto it = op.replicas.rbegin(); it != op.replicas.rend();
                 ++it) {
                const auto& replica = *it;
                if (replica.is_disk_replica()) {
                    auto disk_descriptor = replica.get_disk_descriptor();
                    PutToLocalFile(op.key, op.slices, disk_descriptor);
                    break;  // Only one disk replica is needed
                }
            }
        }

        for (size_t replica_idx = 0; replica_idx < op.replicas.size();
             ++replica_idx) {
            const auto& replica = op.replicas[replica_idx];
            if (replica.is_memory_replica()) {
                auto submit_result = transfer_submitter_->submit(
                    replica, op.slices, TransferRequest::WRITE);

                if (!submit_result) {
                    failure_context = "Failed to submit transfer for replica " +
                                      std::to_string(replica_idx);
                    all_transfers_submitted = false;
                    break;
                }

                op.pending_transfers.emplace_back(
                    std::move(submit_result.value()));
            }
        }

        if (!all_transfers_submitted) {
            LOG(ERROR) << "Transfer submission failed for key " << op.key
                       << ": " << failure_context;
            op.SetError(ErrorCode::TRANSFER_FAIL, failure_context);
            op.pending_transfers.clear();
        } else {
            VLOG(1) << "Successfully submitted " << op.pending_transfers.size()
                    << " transfers for key " << op.key;
        }
    }
}

void Client::WaitForTransfers(std::vector<PutOperation>& ops) {
    for (auto& op : ops) {
        // Skip operations that already failed or completed
        if (op.IsResolved()) {
            continue;
        }

        // Skip operations with no pending transfers (failed in SubmitTransfers)
        if (op.pending_transfers.empty()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "No pending transfers to wait for");
            continue;
        }

        bool all_transfers_succeeded = true;
        ErrorCode first_error = ErrorCode::OK;
        size_t failed_transfer_idx = 0;

        for (size_t i = 0; i < op.pending_transfers.size(); ++i) {
            ErrorCode transfer_result = op.pending_transfers[i].get();
            if (transfer_result != ErrorCode::OK) {
                if (all_transfers_succeeded) {
                    // Record the first error for reporting
                    first_error = transfer_result;
                    failed_transfer_idx = i;
                    all_transfers_succeeded = false;
                }
                // Continue waiting for all transfers to avoid resource leaks
            }
        }

        if (all_transfers_succeeded) {
            VLOG(1) << "All transfers completed successfully for key "
                    << op.key;
            // Transfer phase successful - continue to finalization
            // Note: Don't mark as SUCCESS yet, need to complete finalization
        } else {
            std::string error_context =
                "Transfer " + std::to_string(failed_transfer_idx) + " failed";
            LOG(ERROR) << "Transfer failed for key " << op.key << ": "
                       << toString(first_error) << " (" << error_context << ")";
            op.SetError(first_error, error_context);
        }
    }
}

void Client::FinalizeBatchPut(std::vector<PutOperation>& ops) {
    // For each operation,
    // If transfers completed successfully, we need to call BatchPutEnd
    // If the operation failed but has allocated replicas, we need to call
    // BatchPutRevoke

    std::vector<std::string> successful_keys;
    std::vector<size_t> successful_indices;
    std::vector<std::string> failed_keys;
    std::vector<size_t> failed_indices;

    // Reserve space to avoid reallocations
    successful_keys.reserve(ops.size());
    successful_indices.reserve(ops.size());
    failed_keys.reserve(ops.size());
    failed_indices.reserve(ops.size());

    for (size_t i = 0; i < ops.size(); ++i) {
        auto& op = ops[i];

        // Check if operation completed transfers successfully and needs
        // finalization
        if (!op.IsResolved() && !op.replicas.empty() &&
            !op.pending_transfers.empty()) {
            // Transfers completed, needs BatchPutEnd
            successful_keys.emplace_back(op.key);
            successful_indices.emplace_back(i);
        } else if (op.state != PutOperationState::PENDING &&
                   !op.replicas.empty()) {
            // Operation failed but has allocated replicas, needs BatchPutRevoke
            failed_keys.emplace_back(op.key);
            failed_indices.emplace_back(i);
        }
        // Operations without replicas (early failures) don't need finalization
    }

    // Process successful operations
    if (!successful_keys.empty()) {
        auto end_responses = master_client_.BatchPutEnd(successful_keys);
        if (end_responses.size() != successful_keys.size()) {
            LOG(ERROR) << "BatchPutEnd response size mismatch: expected "
                       << successful_keys.size() << ", got "
                       << end_responses.size();
            for (size_t idx : successful_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchPutEnd response size mismatch");
            }
        } else {
            // Process individual responses
            for (size_t i = 0; i < end_responses.size(); ++i) {
                const size_t op_idx = successful_indices[i];
                if (!end_responses[i]) {
                    LOG(ERROR) << "Failed to finalize put for key "
                               << successful_keys[i] << ": "
                               << toString(end_responses[i].error());
                    ops[op_idx].SetError(end_responses[i].error(),
                                         "BatchPutEnd failed");
                } else {
                    // Operation fully successful
                    ops[op_idx].SetSuccess();
                    VLOG(1) << "Successfully completed put for key "
                            << successful_keys[i];
                }
            }
        }
    }

    // Process failed operations that need cleanup
    if (!failed_keys.empty()) {
        auto revoke_responses = master_client_.BatchPutRevoke(failed_keys);
        if (revoke_responses.size() != failed_keys.size()) {
            LOG(ERROR) << "BatchPutRevoke response size mismatch: expected "
                       << failed_keys.size() << ", got "
                       << revoke_responses.size();
            // Mark all failed operations with revoke RPC failure
            for (size_t idx : failed_indices) {
                ops[idx].SetError(ErrorCode::RPC_FAIL,
                                  "BatchPutRevoke response size mismatch");
            }
        } else {
            // Process individual revoke responses
            for (size_t i = 0; i < revoke_responses.size(); ++i) {
                const size_t op_idx = failed_indices[i];
                if (!revoke_responses[i]) {
                    LOG(ERROR)
                        << "Failed to revoke put for key " << failed_keys[i]
                        << ": " << toString(revoke_responses[i].error());
                    // Preserve original error but note revoke failure in
                    // context
                    std::string original_context =
                        ops[op_idx].failure_context.value_or("unknown error");
                    ops[op_idx].failure_context =
                        original_context + "; revoke also failed";
                } else {
                    LOG(INFO) << "Successfully revoked failed put for key "
                              << failed_keys[i];
                }
            }
        }
    }

    // Ensure all operations have definitive results
    for (auto& op : ops) {
        if (!op.IsResolved()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "Operation not resolved after finalization");
            LOG(ERROR) << "Operation for key " << op.key
                       << " was not properly resolved";
        }
    }
}

std::vector<tl::expected<void, ErrorCode>> Client::CollectResults(
    const std::vector<PutOperation>& ops) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(ops.size());

    int no_available_handle_count = 0;
    for (const auto& op : ops) {
        // With the new structure, result is always set (never nullopt)
        results.emplace_back(op.result);

        // Additional validation and logging for debugging
        if (!op.result.has_value()) {
            // if error == object already exist, consider as ok
            if (op.result.error() == ErrorCode::OBJECT_ALREADY_EXISTS) {
                results.back() = {};
                continue;
            }
            if (op.result.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
                no_available_handle_count++;
            } else {
                LOG(ERROR) << "Operation for key " << op.key
                           << " failed: " << toString(op.result.error())
                           << (op.failure_context
                                   ? (" (" + *op.failure_context + ")")
                                   : "");
            }
        } else {
            VLOG(1) << "Operation for key " << op.key
                    << " completed successfully";
        }
    }
    if (no_available_handle_count > 0) {
        LOG(WARNING) << "BatchPut failed for " << no_available_handle_count
                     << " keys" << PUT_NO_SPACE_HELPER_STR;
    }

    return results;
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPutWhenPreferSameNode(
    std::vector<PutOperation>& ops) {
    auto t0 = std::chrono::steady_clock::now();
    std::unordered_map<std::string, PutOperation> seg_to_ops{};
    for (auto& op : ops) {
        if (op.IsResolved()) {
            continue;
        }
        if (op.replicas.empty()) {
            op.SetError(ErrorCode::INTERNAL_ERROR,
                        "No replicas available for transfer");
            continue;
        }
        auto replica = op.replicas[0];
        if (!replica.is_memory_replica()) {
            op.SetError(ErrorCode::INVALID_PARAMS, "only memory is supported.");
            continue;
        }
        auto& memory_descriptor = replica.get_memory_descriptor();
        if (memory_descriptor.buffer_descriptor.size_ == 0) {
            op.SetError(ErrorCode::INVALID_PARAMS, "buffer size is 0.");
            continue;
        }
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        if (seg_to_ops.find(seg) == seg_to_ops.end()) {
            seg_to_ops.emplace(seg, PutOperation(op.key, op.slices));
        }
        // multiple replica-slices
        seg_to_ops.at(seg).batched_slices.emplace_back(op.slices);
        seg_to_ops.at(seg).replicas.emplace_back(replica);
    }
    std::vector<PutOperation> merged_ops;
    merged_ops.reserve(seg_to_ops.size());
    for (auto& seg_to_op : seg_to_ops) {
        auto& op = seg_to_op.second;
        bool all_transfers_submitted = true;
        std::string failure_context;
        merged_ops.emplace_back(op.key, op.slices);
        auto& merged_op = merged_ops.back();
        merged_op.replicas = op.replicas;
        auto submit_result = transfer_submitter_->submit_batch(
            op.replicas, op.batched_slices, TransferRequest::WRITE);
        if (!submit_result) {
            failure_context = "Failed to submit batch transfer";
            all_transfers_submitted = false;
        } else {
            merged_op.pending_transfers.emplace_back(
                std::move(submit_result.value()));
        }
        if (!all_transfers_submitted) {
            LOG(ERROR) << "Transfer submission failed for key " << op.key
                       << ": " << failure_context;
            merged_op.SetError(ErrorCode::TRANSFER_FAIL, failure_context);
            merged_op.pending_transfers.clear();
        } else {
            VLOG(1) << "Successfully submitted "
                    << merged_op.pending_transfers.size()
                    << " transfers for key " << merged_ops.back().key;
        }
    }
    WaitForTransfers(merged_ops);
    for (auto& op : merged_ops) {
        auto& memory_descriptor = op.replicas[0].get_memory_descriptor();
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        seg_to_ops.at(seg).state = op.state;
    }
    for (auto& op : ops) {
        if (op.IsResolved()) {
            continue;
        }
        auto& memory_descriptor = op.replicas[0].get_memory_descriptor();
        auto& buffer_descriptor = memory_descriptor.buffer_descriptor;
        auto seg = buffer_descriptor.transport_endpoint_;
        op.state = seg_to_ops.at(seg).state;
        auto state = std::make_shared<EmptyOperationState>();
        auto future = TransferFuture(state);
        op.pending_transfers.emplace_back(std::move(future));
    }
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }
    FinalizeBatchPut(ops);
    return CollectResults(ops);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const std::optional<ReplicateConfig>& config_opt) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "BatchPut rejected: client is shutting down";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    ReplicateConfig default_config;
    const auto& config = config_opt ? *config_opt : default_config;

    ReplicateConfig client_cfg = config;
    if (protocol_ == "cxl") {
        client_cfg.preferred_segment = local_hostname_;
    }
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    if (client_cfg.prefer_alloc_in_same_node) {
        if (client_cfg.replica_num != 1) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                          "replica_num != 1";
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        StartBatchPut(ops, client_cfg);
        return BatchPutWhenPreferSameNode(ops);
    }
    StartBatchPut(ops, client_cfg);

    auto t0 = std::chrono::steady_clock::now();
    SubmitTransfers(ops);
    WaitForTransfers(ops);
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - t0)
                  .count();
    if (metrics_) {
        metrics_->transfer_metric.batch_put_latency_us.observe(us);
    }

    FinalizeBatchPut(ops);
    return CollectResults(ops);
}

tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key, bool force) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "Remove rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_.Remove(key, force);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<long, ErrorCode> Client::RemoveByRegex(const ObjectKey& str,
                                                    bool force) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "RemoveByRegex rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_.RemoveByRegex(str, force);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return result.value();
}

tl::expected<long, ErrorCode> Client::RemoveAll(bool force) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "RemoveAll rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return master_client_.RemoveAll(force);
}

tl::expected<void, ErrorCode> Client::MountSegment(
    const void* buffer, size_t size, const std::string& protocol) {
    auto check_result =
        ClientResources::CheckRegisterMemoryParams(buffer, size);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }

    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);

    // Check if the segment overlaps with any existing segment
    for (auto& it : mounted_segments_) {
        auto& mtseg = it.second;
        uintptr_t l1 = reinterpret_cast<uintptr_t>(mtseg.base);
        uintptr_t r1 = reinterpret_cast<uintptr_t>(mtseg.size) + l1;
        uintptr_t l2 = reinterpret_cast<uintptr_t>(buffer);
        uintptr_t r2 = reinterpret_cast<uintptr_t>(size) + l2;
        if (std::max(l1, l2) < std::min(r1, r2)) {
            LOG(ERROR) << "segment_overlaps base1=" << mtseg.base
                       << " size1=" << mtseg.size << " base2=" << buffer
                       << " size2=" << size;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    int rc = resources_.GetTransferEngine()->registerLocalMemory(
        (void*)buffer, size, kWildcardLocation, true, true);
    if (rc != 0) {
        LOG(ERROR) << "register_local_memory_failed base=" << buffer
                   << " size=" << size << ", error=" << rc;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Build segment with logical name; attach TE endpoint for transport
    Segment segment;
    segment.id = generate_uuid();
    segment.name = local_hostname_;
    segment.base = reinterpret_cast<uintptr_t>(buffer);
    segment.size = size;
    segment.protocol = protocol;
    // For P2P handshake mode, publish the actual transport endpoint that was
    // negotiated by the transfer engine. Otherwise, keep the logical hostname
    // so metadata backends (HTTP/etcd/redis) can resolve the segment by name.
    if (metadata_connstring_ == P2PHANDSHAKE) {
        segment.te_endpoint =
            resources_.GetTransferEngine()->getLocalIpAndPort();
    } else {
        segment.te_endpoint = local_hostname_;
    }

    auto mount_result = master_client_.MountSegment(segment);
    if (!mount_result) {
        ErrorCode err = mount_result.error();
        LOG(ERROR) << "mount_segment_to_master_failed base=" << buffer
                   << " size=" << size << ", error=" << err;
        return tl::unexpected(err);
    }

    mounted_segments_[segment.id] = segment;
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegment(const void* buffer,
                                                     size_t size) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.end();

    for (auto it = mounted_segments_.begin(); it != mounted_segments_.end();
         ++it) {
        if (it->second.base == reinterpret_cast<uintptr_t>(buffer) &&
            it->second.size == size) {
            segment = it;
            break;
        }
    }
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found base=" << buffer << " size=" << size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto unmount_result = master_client_.UnmountSegment(segment->second.id);
    if (!unmount_result) {
        ErrorCode err = unmount_result.error();
        LOG(ERROR) << "Failed to unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    int rc = resources_.GetTransferEngine()->unregisterLocalMemory(
        reinterpret_cast<void*>(segment->second.base));
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        if (rc != ERR_ADDRESS_NOT_REGISTERED) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        // Otherwise, the segment is already unregistered from transfer
        // engine, we can continue
    }

    mounted_segments_.erase(segment);
    return {};
}

tl::expected<void, ErrorCode> Client::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    return resources_.RegisterLocalMemory(addr, length, location,
                                          remote_accessible, update_metadata);
}

tl::expected<void, ErrorCode> Client::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    int rc = resources_.GetTransferEngine()->unregisterLocalMemory(
        addr, update_metadata);
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister local memory, addr=" << addr
                   << ", error=" << rc;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<bool, ErrorCode> Client::IsExist(const std::string& key) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "IsExist rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_.ExistKey(key);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchIsExist(
    const std::vector<std::string>& keys) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "BatchIsExist rejected: client is shutting down";
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    auto response = master_client_.BatchExistKey(keys);

    // Check if we got the expected number of responses
    if (response.size() != keys.size()) {
        LOG(ERROR) << "BatchExistKey response size mismatch. Expected: "
                   << keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    // Return the response directly as it's already in the correct
    // format
    return response;
}

void* Client::GetBaseAddr() {
    return resources_.GetTransferEngine()->getBaseAddr();
}

tl::expected<void, ErrorCode> Client::MountLocalDiskSegment(
    bool enable_offloading) {
    auto response =
        master_client_.MountLocalDiskSegment(client_id_, enable_offloading);

    if (!response) {
        LOG(ERROR) << "MountLocalDiskSegment failed, error code is "
                   << response.error();
    }
    return response;
}

tl::expected<void, ErrorCode> Client::OffloadObjectHeartbeat(
    bool enable_offloading,
    std::unordered_map<std::string, int64_t>& offloading_objects) {
    auto response =
        master_client_.OffloadObjectHeartbeat(client_id_, enable_offloading);
    if (!response) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    offloading_objects = std::move(response.value());
    return {};
}

tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, Slice>& batch_slices) {
    auto future = transfer_submitter_->submit_batch_get_offload_object(
        transfer_engine_addr, keys, pointers, batch_slices);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    VLOG(1) << "Using transfer strategy: " << future->strategy();
    auto result = future->get();
    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Transfer failed, error code is " << result;
        return tl::make_unexpected(result);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    auto response =
        master_client_.NotifyOffloadSuccess(client_id_, keys, metadatas);
    return response;
}

tl::expected<UUID, ErrorCode> Client::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "CreateCopyTask rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return master_client_.CreateCopyTask(key, targets);
}

tl::expected<UUID, ErrorCode> Client::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "CreateMoveTask rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return master_client_.CreateMoveTask(key, source, target);
}

tl::expected<QueryTaskResponse, ErrorCode> Client::QueryTask(
    const UUID& task_id) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "QueryTask rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return master_client_.QueryTask(task_id);
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> Client::FetchTasks(
    size_t batch_size) {
    return master_client_.FetchTasks(batch_size);
}

tl::expected<void, ErrorCode> Client::MarkTaskToComplete(
    const TaskCompleteRequest& update_request) {
    return master_client_.MarkTaskToComplete(update_request);
}

void Client::PrepareStorageBackend(const std::string& storage_root_dir,
                                   const std::string& fsdir,
                                   bool enable_eviction, uint64_t quota_bytes) {
    // Initialize storage backend
    storage_backend_ =
        StorageBackend::Create(storage_root_dir, fsdir, enable_eviction);
    if (!storage_backend_) {
        LOG(INFO) << "Failed to initialize storage backend";
    }
    auto init_result = storage_backend_->Init(quota_bytes);
    if (!init_result) {
        LOG(ERROR) << "Failed to initialize StorageBackend. Error: "
                   << init_result.error() << ". The backend will be unusable.";
    }
}

void Client::PutToLocalFile(const std::string& key,
                            const std::vector<Slice>& slices,
                            const DiskDescriptor& disk_descriptor) {
    if (!storage_backend_) return;

    size_t total_size = 0;
    for (const auto& slice : slices) {
        total_size += slice.size;
    }

    std::string path = disk_descriptor.file_path;
    // Currently, persistence is achieved through asynchronous writes, but
    // before asynchronous writing in 3FS, significant performance degradation
    // may occur due to data copying. Profiling reveals that the number of page
    // faults triggered in this scenario is nearly double the normal count.
    // Future plans include introducing a reuse buffer list to address this
    // performance degradation issue.

    std::string value;
    value.reserve(total_size);
    for (const auto& slice : slices) {
        value.append(static_cast<char*>(slice.ptr), slice.size);
    }

    write_thread_pool_.enqueue([this, backend = storage_backend_, key,
                                value = std::move(value), path] {
        // Store the object
        auto store_result = backend->StoreObject(path, value);
        ReplicaType replica_type = ReplicaType::DISK;

        if (!store_result) {
            // If storage failed, revoke the put operation
            LOG(ERROR) << "Failed to store object for key: " << key;
            auto revoke_result = master_client_.PutRevoke(key, replica_type);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke put operation for key: " << key;
            }
            return;
        }

        // If storage succeeded, end the put operation
        auto end_result = master_client_.PutEnd(key, replica_type);
        if (!end_result) {
            LOG(ERROR) << "Failed to end put operation for key: " << key;
        }
    });
}

ErrorCode Client::TransferData(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices,
                               TransferRequest::OpCode op_code) {
    if (!transfer_submitter_) {
        LOG(ERROR) << "TransferSubmitter not initialized";
        return ErrorCode::INVALID_PARAMS;
    }

    auto future =
        transfer_submitter_->submit(replica_descriptor, slices, op_code);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

ErrorCode Client::TransferWrite(const Replica::Descriptor& replica_descriptor,
                                std::vector<Slice>& slices) {
    return TransferData(replica_descriptor, slices, TransferRequest::WRITE);
}

ErrorCode Client::TransferRead(const Replica::Descriptor& replica_descriptor,
                               std::vector<Slice>& slices) {
    size_t total_size = 0;
    if (replica_descriptor.is_memory_replica()) {
        auto& mem_desc = replica_descriptor.get_memory_descriptor();
        total_size = mem_desc.buffer_descriptor.size_;
    } else {
        auto& disk_desc = replica_descriptor.get_disk_descriptor();
        total_size = disk_desc.object_size;
    }

    size_t slices_size = CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    return TransferData(replica_descriptor, slices, TransferRequest::READ);
}

void Client::PingThreadMain(bool is_ha_mode,
                            std::string current_master_address) {
    // How many failed pings before getting latest master view from etcd
    const int max_ping_fail_count = 3;
    // How long to wait for next ping after success
    const int success_ping_interval_ms = 1000;
    // How long to wait for next ping after failure
    const int fail_ping_interval_ms = 1000;
    // Increment after a ping failure, reset after a ping success
    int ping_fail_count = 0;

    auto remount_segment = [this]() {
        // This lock must be held until the remount rpc is finished,
        // otherwise there will be corner cases, e.g., a segment is
        // unmounted successfully first, and then remounted again in
        // this thread.
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        std::vector<Segment> segments;
        for (auto it : mounted_segments_) {
            auto& segment = it.second;
            segments.emplace_back(segment);
        }
        auto remount_result = master_client_.ReMountSegment(segments);
        if (!remount_result) {
            ErrorCode err = remount_result.error();
            LOG(ERROR) << "Failed to remount segments: " << err;
        }
    };
    // Use another thread to remount segments to avoid blocking the ping
    // thread
    std::future<void> remount_segment_future;

    while (ping_running_) {
        // Join the remount segment thread if it is ready
        if (remount_segment_future.valid() &&
            remount_segment_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_segment_future = std::future<void>();
        }

        // Ping master
        auto ping_result = master_client_.Ping();
        if (ping_result) {
            // Reset ping failure count
            ping_fail_count = 0;
            auto& ping_response = ping_result.value();
            if (ping_response.client_status == ClientStatus::NEED_REMOUNT &&
                !remount_segment_future.valid()) {
                // Ensure at most one remount segment thread is running
                remount_segment_future =
                    std::async(std::launch::async, remount_segment);
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        }

        ping_fail_count++;
        if (ping_fail_count < max_ping_fail_count) {
            LOG(ERROR) << "Failed to ping master";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        // Exceeded ping failure threshold. Reconnect based on mode.
        if (is_ha_mode) {
            LOG(ERROR)
                << "Failed to ping master for " << ping_fail_count
                << " times; fetching latest master view and reconnecting";
            std::string master_address;
            ViewVersionId next_version = 0;
            auto err =
                master_view_helper_.GetMasterView(master_address, next_version);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to get new master view: "
                           << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            err = master_client_.Connect(master_address);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to connect to master " << master_address
                           << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }

            current_master_address = master_address;
            LOG(INFO) << "Reconnected to master " << master_address;
            ping_fail_count = 0;
        } else {
            LOG(ERROR) << "Failed to ping master for " << ping_fail_count
                       << " times (non-HA); reconnecting to "
                       << current_master_address;
            auto err = master_client_.Connect(current_master_address);
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "Reconnect failed to " << current_master_address
                           << ": " << toString(err);
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(fail_ping_interval_ms));
                continue;
            }
            LOG(INFO) << "Reconnected to master " << current_master_address;
            ping_fail_count = 0;
        }
    }
    // Explicitly wait for the remount segment thread to finish
    if (remount_segment_future.valid()) {
        remount_segment_future.wait();
    }
}

ErrorCode Client::FindFirstCompleteReplica(
    const std::vector<Replica::Descriptor>& replica_list,
    Replica::Descriptor& replica) {
    // Find the first complete replica
    for (size_t i = 0; i < replica_list.size(); ++i) {
        if (replica_list[i].status == ReplicaStatus::COMPLETE) {
            replica = replica_list[i];
            return ErrorCode::OK;
        }
    }

    // No complete replica found
    return ErrorCode::INVALID_REPLICA;
}

tl::expected<Replica::Descriptor, ErrorCode> Client::GetPreferredReplica(
    const std::vector<Replica::Descriptor>& replica_list) {
    if (replica_list.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (mounted_segments_.empty() || replica_list.size() == 1) {
        return replica_list[0];
    }

    std::unordered_set<std::string> local_endpoints;
    local_endpoints.reserve(mounted_segments_.size());
    for (const auto& segment : mounted_segments_) {
        local_endpoints.insert(segment.second.te_endpoint);
    }

    for (const auto& rep : replica_list) {
        if (rep.is_memory_replica()) {
            const auto& mem_desc = rep.get_memory_descriptor();
            const std::string& endpoint =
                mem_desc.buffer_descriptor.transport_endpoint_;
            if (local_endpoints.count(endpoint)) {
                return rep;
            }
        }
    }

    return replica_list[0];
}

void Client::SegmentDeleter::operator()(void* ptr) const {
    if (ptr) free(ptr);
}
void Client::AscendSegmentDeleter::operator()(void* ptr) const {
    if (ptr) free_memory("ascend", ptr);
}
void Client::HugepageSegmentDeleter::operator()(void* ptr) const {
    if (ptr && size > 0) free_buffer_mmap_memory(ptr, size);
}

ErrorCode Client::InitStorage(uint64_t global_segment_size,
                              uint64_t local_buffer_size, bool enable_offload,
                              uint16_t client_rpc_port) {
    if (!resources_.GetTransferEngine() &&
        (global_segment_size != 0 || local_buffer_size != 0 || enable_offload)) {
        LOG(ERROR) << "Storage memory and file offload require a transfer engine";
        return ErrorCode::INVALID_PARAMS;
    }
    const bool should_use_hugepage =
        (std::getenv("MC_STORE_USE_HUGEPAGE") != nullptr) &&
        protocol_ != "ascend";
    auto buffer_error = resources_.InitLocalBufferAllocator(
        local_buffer_size, protocol_, should_use_hugepage);
    if (buffer_error != ErrorCode::OK) {
        LOG(ERROR) << "Failed to register local buffer memory: "
                   << buffer_error;
        return buffer_error;
    }

    // If global_segment_size is 0, skip mount segment;
    // If global_segment_size is larger than max_mr_size, split to multiple
    // mapped_shms.
    if (protocol_ == "cxl") {
        size_t cxl_dev_size = 0;
        const char* env = std::getenv("MC_CXL_DEV_SIZE");
        if (env) {
            char* end = nullptr;
            unsigned long long val = strtoull(env, &end, 10);
            if (end != env && *end == '\0')
                cxl_dev_size = static_cast<size_t>(val);
        } else {
            LOG(FATAL) << "MC_CXL_DEV_SIZE not set";
            return ErrorCode::INVALID_PARAMS;
        }

        void* ptr = GetBaseAddr();
        LOG(INFO) << "Mounting CXL segment: " << cxl_dev_size << " bytes, "
                  << ptr;
        auto mount_result = MountSegment(ptr, cxl_dev_size, protocol_);
        if (!mount_result.has_value()) {
            LOG(ERROR) << "Failed to mount segment: "
                       << toString(mount_result.error());
            return mount_result.error();
        }

    } else {
        auto max_mr_size = globalConfig().max_mr_size;     // Max segment size
        uint64_t total_glbseg_size = global_segment_size;  // For logging
        uint64_t current_glbseg_size = 0;                  // For logging
        while (global_segment_size > 0) {
            size_t segment_size = std::min(global_segment_size,
                                           static_cast<uint64_t>(max_mr_size));
            global_segment_size -= segment_size;
            current_glbseg_size += segment_size;
            LOG(INFO) << "Mounting segment: " << segment_size << " bytes, "
                      << current_glbseg_size << " of " << total_glbseg_size;

            size_t mapped_size = segment_size;
            void* ptr = nullptr;
            if (should_use_hugepage) {
                mapped_size =
                    align_up(segment_size, get_hugepage_size_from_env());
                ptr = allocate_buffer_mmap_memory(mapped_size,
                                                  get_hugepage_size_from_env());
            } else {
                ptr = allocate_buffer_allocator_memory(segment_size, protocol_);
            }

            if (!ptr) {
                LOG(ERROR) << "Failed to allocate segment memory";
                return ErrorCode::INVALID_PARAMS;
            }
            if (protocol_ == "ascend") {
                ascend_segment_ptrs_.emplace_back(ptr);
            } else if (should_use_hugepage) {
                hugepage_segment_ptrs_.emplace_back(
                    ptr, HugepageSegmentDeleter{mapped_size});
            } else {
                segment_ptrs_.emplace_back(ptr);
            }
            auto mount_result = MountSegment(ptr, mapped_size, protocol_);
            if (!mount_result.has_value()) {
                LOG(ERROR) << "Failed to mount segment: "
                           << toString(mount_result.error());
                return mount_result.error();
            }
        }
        if (total_glbseg_size == 0) {
            LOG(INFO) << "Global segment size is 0, skip mounting segment";
        }
    }

    auto rpc_error = StartClientRpcServer(client_rpc_port);
    if (rpc_error != ErrorCode::OK) {
        LOG(ERROR) << "Failed to start client rpc server: "
                   << toString(rpc_error);
        return rpc_error;
    }

    if (enable_offload) {
        auto file_storage_config = FileStorageConfig::FromEnvironment();
        file_storage_ = std::make_unique<FileStorage>(file_storage_config, this,
                                                      client_rpc_addr_);
        auto init_result = file_storage_->Init();
        if (!init_result) {
            LOG(ERROR) << "file storage init failed with error: "
                       << init_result.error();
            return init_result.error();
        }
    }
    client_rpc_ready_.store(true, std::memory_order_release);
    return ErrorCode::OK;
}

ErrorCode Client::StartClientRpcServer(uint16_t port) {
    std::string host;
    if (resources_.GetTransferEngine()) {
        const auto te_endpoint = GetTransportEndpoint();
        host = te_endpoint.substr(0, te_endpoint.rfind(':'));
    } else {
        // rpc_only clients have no transfer engine; use their configured host.
        host = parseHostNameWithPort(local_hostname_).first;
    }
    if (host.size() > 1 && host.front() == '[' && host.back() == ']') {
        host = host.substr(1, host.size() - 2);
    }
    auto listen_address =
        (host.find(':') == std::string::npos ? host : "[" + host + "]") + ":" +
        std::to_string(port);
    client_rpc_server_ =
        std::make_unique<coro_rpc::coro_rpc_server>(1, listen_address);
    client_rpc_server_
        ->register_handler<&Client::BatchGetOffloadObjectFromStorage>(this);
    auto server_exit = client_rpc_server_->async_start();
    if (server_exit.hasResult()) {
        LOG(ERROR) << "Failed to start client RPC server on port " << port
                   << ": " << server_exit.result().value().message();
        return ErrorCode::INTERNAL_ERROR;
    }
    auto bound_port = client_rpc_server_->port();
    if (bound_port == 0) {
        LOG(ERROR) << "Client RPC server has no bound port";
        return ErrorCode::INTERNAL_ERROR;
    }
    client_rpc_addr_ =
        (host.find(':') == std::string::npos ? host : "[" + host + "]") + ":" +
        std::to_string(bound_port);
    LOG(INFO) << "Native client-to-client RPC listening at "
              << client_rpc_addr_;
    return ErrorCode::OK;
}

void Client::StopClientRpcServer() {
    client_rpc_ready_.store(false, std::memory_order_release);
    if (client_rpc_server_) {
        client_rpc_server_->stop();
    }
    client_rpc_server_.reset();
}

tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Client::Get(
    const std::string& key, std::shared_ptr<ClientBufferAllocator> allocator) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "Get rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Query to get size first
    auto query_result = QueryImpl(key);
    if (!query_result) {
        if (query_result.error() != ErrorCode::OBJECT_NOT_FOUND &&
            query_result.error() != ErrorCode::REPLICA_IS_NOT_READY) {
            LOG(ERROR) << "Failed to query key"
                       << ", key:" << key << ", error:" << query_result.error();
        }
        return tl::unexpected(query_result.error());
    }

    const auto& replica_list = query_result.value().replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto preferred = GetPreferredReplica(replica_list);
    if (!preferred) {
        LOG(ERROR) << "No preferred replica for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    const auto& replica = preferred.value();
    uint64_t total_size = calculate_total_size(replica);
    if (total_size == 0) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    // Allocate buffer
    auto alloc_result = allocator->allocate(total_size);
    if (!alloc_result) {
        LOG(ERROR) << "Failed to allocate buffer for get, key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto buffer_handle = std::move(*alloc_result);

    // Create slices for the allocated buffer
    std::vector<Slice> slices;
    allocateSlices(slices, replica, buffer_handle.ptr());

    auto get_result = GetImpl(key, query_result.value(), slices);
    if (!get_result) {
        LOG(ERROR) << "Failed to get key"
                   << ", key:" << key << ", error:" << get_result.error();
        return tl::unexpected(get_result.error());
    }

    return std::make_shared<BufferHandle>(std::move(buffer_handle));
}

std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
Client::BatchGet(const std::vector<std::string>& keys,
                 std::shared_ptr<ClientBufferAllocator> allocator) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "BatchGet rejected: client is shutting down";
        return std::vector<
            tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>> results(
        keys.size(), tl::unexpected(ErrorCode::INTERNAL_ERROR));

    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        for (auto& r : results) {
            r = tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        return results;
    }

    // Query all keys
    auto query_results = BatchQueryImpl(keys);

    // Prepare valid operations for batch get
    struct KeyOp {
        size_t original_index;
        QueryResult query_result;
        std::unique_ptr<BufferHandle> buffer_handle;
        std::vector<Slice> slices;
    };
    std::vector<KeyOp> valid_ops;
    valid_ops.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        if (!query_results[i]) {
            auto error = query_results[i].error();
            if (error != ErrorCode::OBJECT_NOT_FOUND &&
                error != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << keys[i]
                           << "': " << toString(error);
            }
            results[i] = tl::unexpected(query_results[i].error());
            continue;
        }

        auto query_ptr = std::move(query_results[i].value());
        if (query_ptr.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        const auto& replica = query_ptr.replicas[0];
        uint64_t total_size = calculate_total_size(replica);
        if (total_size == 0) {
            LOG(ERROR) << "Empty replica list for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
            continue;
        }

        auto alloc_result = allocator->allocate(total_size);
        if (!alloc_result) {
            LOG(ERROR) << "Failed to allocate buffer for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        auto bh = std::make_unique<BufferHandle>(std::move(*alloc_result));
        std::vector<Slice> slices;
        allocateSlices(slices, replica, bh->ptr());

        valid_ops.push_back(
            {i, std::move(query_ptr), std::move(bh), std::move(slices)});
    }

    if (valid_ops.empty()) {
        return results;
    }

    // Build batch get structures
    std::vector<std::string> batch_keys;
    std::vector<QueryResult> batch_qr;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;
    batch_keys.reserve(valid_ops.size());
    batch_qr.reserve(valid_ops.size());

    for (auto& op : valid_ops) {
        batch_keys.push_back(keys[op.original_index]);
        batch_qr.push_back(std::move(op.query_result));
        batch_slices[keys[op.original_index]] = op.slices;
    }

    auto batch_results = BatchGetImpl(batch_keys, batch_qr, batch_slices);

    for (size_t j = 0; j < valid_ops.size(); ++j) {
        auto& op = valid_ops[j];
        if (batch_results[j]) {
            results[op.original_index] =
                std::make_shared<BufferHandle>(std::move(*op.buffer_handle));
        } else {
            results[op.original_index] =
                tl::unexpected(batch_results[j].error());
        }
    }

    return results;
}

tl::expected<int64_t, ErrorCode> Client::Get(const std::string& key,
                                             void* buffer, size_t size) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "Get rejected: client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    // Step 1: Get object info
    auto query_result = QueryImpl(key);
    if (!query_result) {
        if (query_result.error() == ErrorCode::OBJECT_NOT_FOUND ||
            query_result.error() == ErrorCode::REPLICA_IS_NOT_READY) {
            VLOG(1) << "Object not found for key: " << key;
            return tl::unexpected(query_result.error());
        }
        LOG(ERROR) << "Query failed for key: " << key
                   << " with error: " << toString(query_result.error());
        return tl::unexpected(query_result.error());
    }

    const std::vector<Replica::Descriptor>& replica_list =
        query_result.value().replicas;

    // Calculate total size from replica list
    if (replica_list.empty()) {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto& res = GetPreferredReplica(replica_list);
    if (!res) {
        LOG(ERROR) << "Internal error: replica_list is empty";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto& replica = res.value();
    uint64_t total_size = calculate_total_size(replica);

    // Check if user buffer is large enough
    if (size < total_size) {
        LOG(ERROR) << "User buffer too small. Required: " << total_size
                   << ", provided: " << size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Step 2: Split user buffer according to object info and create
    // slices
    std::vector<mooncake::Slice> slices;
    allocateSlices(slices, replica, buffer);

    // Step 3: Read data directly into user buffer
    auto get_result = GetImpl(key, query_result.value(), slices);
    if (!get_result) {
        LOG(ERROR) << "Get failed for key: " << key
                   << " with error: " << toString(get_result.error());
        return tl::unexpected(get_result.error());
    }

    return static_cast<int64_t>(total_size);
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& keys, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "BatchGet rejected: client is shutting down";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    auto start_time = std::chrono::steady_clock::now();
    // Validate preconditions
    if (keys.size() != buffers.size() || keys.size() != sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << buffers.size()
                   << ", sizes=" << sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    const size_t num_keys = keys.size();
    std::vector<tl::expected<int64_t, ErrorCode>> results(num_keys);

    if (num_keys == 0) {
        return results;
    }

    // Query metadata for all keys
    const auto query_results = BatchQueryImpl(keys);

    // Process each key individually and prepare for batch transfer
    struct ValidKeyInfo {
        std::string key;
        size_t original_index;
        QueryResult query_result;
        std::vector<Slice> slices;
        uint64_t total_size;
    };

    std::vector<ValidKeyInfo> valid_operations;
    std::unordered_map<std::string, ValidKeyInfo> valid_local_disk_operations;
    valid_operations.reserve(num_keys);

    for (size_t i = 0; i < num_keys; ++i) {
        const auto& key = keys[i];

        // Handle query failures
        if (!query_results[i]) {
            const auto error = query_results[i].error();
            results[i] = tl::unexpected(error);
            if (error != ErrorCode::OBJECT_NOT_FOUND &&
                error != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(error);
            }
            continue;
        }

        // Validate replica list
        auto query_result_values = query_results[i].value();
        if (query_result_values.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_REPLICA);
            continue;
        }

        // Calculate required buffer size
        const auto& replica = query_result_values.replicas[0];
        uint64_t total_size = calculate_total_size(replica);

        // Validate buffer capacity
        if (sizes[i] < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << key
                       << "': required=" << total_size
                       << ", available=" << sizes[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        // Create slices for this key's buffer
        std::vector<Slice> key_slices;
        allocateSlices(key_slices, replica, buffers[i]);

        if (query_result_values.replicas.size() == 1 &&
            query_result_values.replicas.at(0).is_local_disk_replica()) {
            valid_local_disk_operations.emplace(
                key,
                ValidKeyInfo{.key = key,
                             .original_index = i,
                             .query_result = std::move(query_result_values),
                             .slices = std::move(key_slices),
                             .total_size = total_size});
            results[i] = static_cast<int64_t>(total_size);
            continue;
        }
        // Store operation info for batch processing
        valid_operations.push_back(
            {.key = key,
             .original_index = i,
             .query_result = std::move(query_result_values),
             .slices = std::move(key_slices),
             .total_size = total_size});

        // Set success result (actual bytes transferred)
        results[i] = static_cast<int64_t>(total_size);
    }

    // Early return if no valid operations
    if (valid_operations.empty() && valid_local_disk_operations.empty()) {
        return results;
    }

    // Prepare batch transfer data structures
    std::vector<std::string> batch_keys;
    std::vector<QueryResult> batch_query_results;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;

    batch_keys.reserve(valid_operations.size());
    batch_query_results.reserve(valid_operations.size());

    for (const auto& op : valid_operations) {
        batch_keys.push_back(op.key);
        batch_query_results.push_back(op.query_result);
        batch_slices[op.key] = op.slices;
    }
    if (!valid_operations.empty()) {
        // Execute batch transfer
        const auto batch_get_results =
            BatchGetImpl(batch_keys, batch_query_results, batch_slices);

        // Process transfer results
        for (size_t j = 0; j < batch_get_results.size(); ++j) {
            const auto& op = valid_operations[j];

            if (!batch_get_results[j]) {
                const auto error = batch_get_results[j].error();
                LOG(ERROR) << "BatchGet failed for key '" << op.key
                           << "': " << toString(error);
                results[op.original_index] = tl::unexpected(error);
            }
        }
    }

    // Prepare batch transfer data structures
    std::unordered_map<std::string, std::unordered_map<std::string, Slice>>
        offload_objects;

    for (const auto& op_it : valid_local_disk_operations) {
        const auto& replica = op_it.second.query_result.replicas.at(0);
        auto [store_segment_it, _] = offload_objects.try_emplace(
            replica.get_local_disk_descriptor().transport_endpoint);
        store_segment_it->second.emplace(op_it.first,
                                         op_it.second.slices.at(0));
    }

    size_t offload_object_count = 0;
    auto start_read_store_time = std::chrono::steady_clock::now();
    for (auto& offload_objects_it : offload_objects) {
        offload_object_count += offload_objects_it.second.size();
        auto batch_get_offload_result = BatchGetIntoOffloadObjectInternal(
            offload_objects_it.first, offload_objects_it.second);
        if (!batch_get_offload_result) {
            LOG(ERROR) << "Batch get store object failed with error: "
                       << batch_get_offload_result.error();
            for (const auto& offload_object_it : offload_objects_it.second) {
                results[valid_local_disk_operations.at(offload_object_it.first)
                            .original_index] =
                    tl::make_unexpected(batch_get_offload_result.error());
            }
        }
    }

    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time)
                            .count();
    auto read_store_time =
        std::chrono::duration_cast<std::chrono::microseconds>(
            end_time - start_read_store_time)
            .count();
    LOG(INFO) << "Time taken for batch_get_into: " << elapsed_time
              << "us, read store: " << read_store_time
              << "us, with memory key count: " << valid_operations.size()
              << ", offload key count: " << offload_object_count;

    return results;
}

std::vector<tl::expected<int64_t, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    bool aggregate_same_segment_task) {
    auto guard = local_inflight_tracker_.Enter();
    if (!guard.is_valid()) {
        LOG(ERROR) << "BatchGet rejected: client is shutting down";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    // Validate preconditions
    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch: keys=" << keys.size()
                   << ", buffers=" << all_buffers.size()
                   << ", sizes=" << all_sizes.size();
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    const size_t num_keys = keys.size();
    std::vector<tl::expected<int64_t, ErrorCode>> results;
    results.reserve(num_keys);
    if (num_keys == 0) {
        return results;
    }
    // Query metadata for all keys
    const auto query_results = BatchQueryImpl(keys);
    // Process each key individually and prepare for batch transfer
    struct ValidKeyInfo {
        std::string key;
        size_t original_index;
        QueryResult query_result;
        std::vector<Slice> slices;
        uint64_t total_size;
    };

    std::vector<ValidKeyInfo> valid_operations;
    valid_operations.reserve(num_keys);
    for (size_t i = 0; i < num_keys; ++i) {
        const auto& key = keys[i];
        // Handle query failures
        if (!query_results[i]) {
            const auto error = query_results[i].error();
            results.emplace_back(tl::unexpected(error));
            if (error != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(ERROR) << "Query failed for key '" << key
                           << "': " << toString(error);
            }
            continue;
        }
        // Validate replica list
        auto query_result_values = query_results[i].value();
        if (query_result_values.replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << key;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_REPLICA));
            continue;
        }
        // Calculate required buffer size
        const auto& replica = query_result_values.replicas[0];
        uint64_t total_size = calculate_total_size(replica);
        const auto& sizes = all_sizes[i];
        uint64_t dst_total_size = 0;
        for (auto& size : sizes) {
            dst_total_size += size;
        }
        if (dst_total_size < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << key
                       << "': required=" << total_size
                       << ", available=" << dst_total_size;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }
        // Create slices for this key's buffer
        const auto& buffers = all_buffers[i];
        std::vector<Slice> key_slices;
        key_slices.reserve(buffers.size());
        if (replica.is_memory_replica()) {
            for (size_t j = 0; j < buffers.size(); ++j) {
                key_slices.emplace_back(Slice{buffers[j], sizes[j]});
            }
        } else {
            LOG(ERROR) << "Invalid replica type for key: " << key;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        valid_operations.push_back(
            {.key = key,
             .original_index = i,
             .query_result = std::move(query_result_values),
             .slices = std::move(key_slices),
             .total_size = total_size});
        // Set success result (actual bytes transferred)
        results.emplace_back(static_cast<int64_t>(total_size));
    }
    // Early return if no valid operations
    if (valid_operations.empty()) {
        return results;
    }

    // Prepare batch transfer data structures
    std::vector<std::string> batch_keys;
    std::vector<QueryResult> batch_query_results;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;
    batch_keys.reserve(valid_operations.size());
    batch_query_results.reserve(valid_operations.size());
    for (auto& op : valid_operations) {
        batch_keys.push_back(op.key);
        batch_query_results.push_back(op.query_result);
        batch_slices[op.key] = op.slices;
    }

    auto batch_get_results =
        BatchGetImpl(batch_keys, batch_query_results, batch_slices,
                     aggregate_same_segment_task);

    // Process transfer results
    for (size_t j = 0; j < batch_get_results.size(); ++j) {
        const auto& op = valid_operations[j];

        if (!batch_get_results[j]) {
            const auto error = batch_get_results[j].error();
            LOG(ERROR) << "BatchGet failed for key '" << op.key
                       << "': " << toString(error);
            results[op.original_index] = tl::unexpected(error);
        }
    }
    return results;
}

tl::expected<BatchGetOffloadObjectResponse, ErrorCode>
Client::BatchGetOffloadObjectFromStorage(const std::vector<std::string>& keys,
                                         const std::vector<int64_t>& sizes) {
    if (!client_rpc_ready_.load(std::memory_order_acquire)) {
        LOG(ERROR) << "Client RPC server is not ready";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!file_storage_) {
        LOG(ERROR) << "File offload is not enabled for this client";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto result = file_storage_->BatchGet(keys, sizes);
    if (!result) {
        LOG(ERROR) << "Batch get offload object failed, err_code = "
                   << result.error();
        return tl::make_unexpected(result.error());
    }
    return BatchGetOffloadObjectResponse(
        std::move(result.value()), GetTransportEndpoint(),
        file_storage_->config_.client_buffer_gc_ttl_ms);
}

tl::expected<void, ErrorCode> Client::BatchGetIntoOffloadObjectInternal(
    const std::string& target_rpc_service_addr,
    std::unordered_map<std::string, Slice>& objects) {
    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::string> keys;
    std::vector<int64_t> sizes;
    for (const auto& object_it : objects) {
        keys.emplace_back(object_it.first);
        sizes.emplace_back(object_it.second.size);
    }
    auto batchGetResp = client_requester_->batch_get_offload_object(
        target_rpc_service_addr, keys, sizes);
    if (!batchGetResp) {
        LOG(ERROR) << "Batch get offload object failed with error: "
                   << batchGetResp.error();
        return tl::make_unexpected(batchGetResp.error());
    }
    auto result = BatchGetOffloadObject(batchGetResp->transfer_engine_addr,
                                        keys, batchGetResp->pointers, objects);
    auto end_time = std::chrono::steady_clock::now();
    auto elapsed_time = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count());
    LOG(INFO) << "Time taken for batch_get_into_offload_object_internal: "
              << elapsed_time
              << "ms, with target_rpc_service_addr: " << target_rpc_service_addr
              << ", key size: " << objects.size()
              << ", gc ttl: " << batchGetResp->gc_ttl_ms << "ms.";
    if (!result) {
        LOG(ERROR) << "Batch get into offload object failed with error: "
                   << result.error();
        return result;
    }
    if (elapsed_time >= batchGetResp->gc_ttl_ms) {
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }
    return {};
}

ClientRequester::ClientRequester() {
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
    const char* value = std::getenv("MC_RPC_PROTOCOL");
    if (value && std::string_view(value) == "rdma") {
        pool_conf.client_config.socket_config =
            coro_io::ib_socket_t::config_t{};
    }
    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf);
}

tl::expected<BatchGetOffloadObjectResponse, ErrorCode>
ClientRequester::batch_get_offload_object(const std::string& client_addr,
                                          const std::vector<std::string>& keys,
                                          const std::vector<int64_t> sizes) {
    auto result =
        invoke_rpc<&Client::BatchGetOffloadObjectFromStorage,
                   BatchGetOffloadObjectResponse>(client_addr, keys, sizes);
    if (!result) {
        LOG(ERROR)
            << "Failed to invoke batch_get_offload_object, client_addr = "
            << client_addr << ", error is: " << result.error();
    }
    return result;
}

template <auto ServiceMethod, typename ReturnType, typename... Args>
tl::expected<ReturnType, ErrorCode> ClientRequester::invoke_rpc(
    const std::string& client_addr, Args&&... args) {
    auto client_pool = client_pools_->at(client_addr);
    return async_simple::coro::syncAwait(
        [&]() -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
            auto ret = co_await client_pool->send_request(
                [&](coro_io::client_reuse_hint,
                    coro_rpc::coro_rpc_client& client) {
                    return client.send_request<ServiceMethod>(
                        std::forward<Args>(args)...);
                });
            if (!ret.has_value()) {
                LOG(ERROR) << "Dummy Client not available";
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            auto result = co_await std::move(ret.value());
            if (!result) {
                LOG(ERROR) << "RPC call failed: " << result.error().msg;
                co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            co_return result->result();
        }());
}

}  // namespace mooncake
