#include "centralized_client_service.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <vector>
#include <memory>

#include "transfer_engine.h"
#include "transfer_task.h"
#include "config.h"
#include "types.h"
#include "file_storage.h"

namespace mooncake {

CentralizedClientService::CentralizedClientService(
    const std::string& local_ip, uint16_t te_port,
    const std::string& metadata_connstring,
    const std::map<std::string, std::string>& labels)
    : ClientService(local_ip, te_port, metadata_connstring, labels),
      master_client_(client_id_,
                     metrics_ ? &metrics_->master_client_metric : nullptr),
      write_thread_pool_(2) {}

CentralizedClientService::~CentralizedClientService() {
    Stop();
    Destroy();
}

void CentralizedClientService::Stop() {
    if (!MarkShuttingDown()) {
        return;  // Already shut down.
    }

    ClientService::Stop();
}

void CentralizedClientService::Destroy() {
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
        if (!segment.IsCentralizedSegment()) {
            LOG(ERROR) << "Segment " << segment.id << " is not centralized";
            continue;
        }
        auto result = InnerUnmountSegment(
            reinterpret_cast<void*>(segment.GetCentralizedExtra().base),
            segment.size);
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

    ClientService::Destroy();
}

ErrorCode CentralizedClientService::Init(
    const CentralizedClientConfig& config) {
    auto master_server_entry = config.master_server_entry;
    ErrorCode err = ConnectToMaster(master_server_entry);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to connect to master: " << err;
        return err;
    }

    // Initialize storage backend if storage_root_dir is valid
    auto config_response = master_client_.GetStorageConfig();
    if (!config_response) {
        LOG(ERROR) << "Failed to get storage config from master";
        // Fallback to GetFsdir for backward compatibility
        auto response = master_client_.GetFsdir();
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
                PrepareStorageBackend(storage_root_dir, fs_subdir, true, 0);
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << dir_string;
            }
        }
    } else {
        auto storage_config = config_response.value();
        if (storage_config.fsdir.empty()) {
            LOG(INFO)
                << "Storage root directory is not set. persisting data is "
                   "disabled.";
        } else {
            size_t pos = storage_config.fsdir.find_last_of('/');
            if (pos != std::string::npos) {
                std::string storage_root_dir =
                    storage_config.fsdir.substr(0, pos);
                std::string fs_subdir = storage_config.fsdir.substr(pos + 1);
                LOG(INFO) << "Storage root directory is: " << storage_root_dir;
                LOG(INFO) << "Fs subdir is: " << fs_subdir;
                LOG(INFO) << "Disk eviction enabled: "
                          << storage_config.enable_disk_eviction;
                LOG(INFO) << "Quota bytes: " << storage_config.quota_bytes;
                // Initialize storage backend with config from master
                PrepareStorageBackend(storage_root_dir, fs_subdir,
                                      storage_config.enable_disk_eviction,
                                      storage_config.quota_bytes);
            } else {
                LOG(ERROR) << "Invalid fsdir format: " << storage_config.fsdir;
            }
        }
    }

    // Initialize transfer engine
    if (config.transfer_engine == nullptr) {
        transfer_engine_ = std::make_shared<TransferEngine>();
        err = InitTransferEngine(local_endpoint(), metadata_connstring_,
                                 config.protocol, config.rdma_devices);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to initialize transfer engine";
            return err;
        }
    } else {
        transfer_engine_ = config.transfer_engine;
        LOG(INFO) << "Use existing transfer engine instance. Skip its "
                     "initialization.";
    }

    InitTransferSubmitter();

    is_running_ = true;

    auto reg = RegisterClient();
    if (!reg) {
        LOG(ERROR) << "Failed to register centralized client with master: "
                   << toString(reg.error());
        return reg.error();
    }

    // Mount global segments if specified
    if (config.global_segment_size > 0) {
        // If global_segment_size > max_mr_size, split to multiple mapped_shms.
        auto max_mr_size = globalConfig().max_mr_size;  // Max segment size
        uint64_t total_glbseg_size = config.global_segment_size;  // For logging
        uint64_t current_glbseg_size = 0;                         // For logging
        uint64_t remaining_size = config.global_segment_size;

        while (remaining_size > 0) {
            size_t segment_size =
                std::min(remaining_size, (uint64_t)max_mr_size);
            remaining_size -= segment_size;
            current_glbseg_size += segment_size;
            LOG(INFO) << "Mounting segment: " << segment_size << " bytes, "
                      << current_glbseg_size << " of " << total_glbseg_size;
            void* ptr =
                allocate_buffer_allocator_memory(segment_size, config.protocol);
            if (!ptr) {
                LOG(ERROR) << "Failed to allocate segment memory";
                return ErrorCode::INTERNAL_ERROR;
            }
            if (config.protocol == "ascend") {
                ascend_segment_ptrs_.emplace_back(ptr);
            } else {
                segment_ptrs_.emplace_back(ptr);
            }
            auto mount_result = MountSegment(ptr, segment_size);
            if (!mount_result.has_value()) {
                LOG(ERROR) << "Failed to mount segment: "
                           << toString(mount_result.error());
                return mount_result.error();
            }
        }
    } else {
        LOG(INFO) << "Global segment size is 0, skip mounting segment";
    }

    // Initialize file storage if enabled
    if (config.enable_offload) {
        auto file_storage_config = FileStorageConfig::FromEnvironment();
        file_storage_ = std::make_shared<FileStorage>(
            shared_from_this(), local_endpoint(), file_storage_config);
        auto init_result = file_storage_->Init();
        if (!init_result) {
            LOG(ERROR) << "file storage init failed with error: "
                       << init_result.error();
            return init_result.error();
        }
    }

    // Start heartbeat AFTER all initialization is complete
    StartHeartbeat(master_server_entry);

    return ErrorCode::OK;
}

void CentralizedClientService::InitTransferSubmitter() {
    // Initialize TransferSubmitter after transfer engine is ready
    // Keep using logical local_hostname for name-based behaviors; endpoint is
    // used separately where needed.
    transfer_submitter_ = std::make_unique<TransferSubmitter>(
        *transfer_engine_, storage_backend_,
        metrics_ ? &metrics_->transfer_metric : nullptr);
}

tl::expected<std::unique_ptr<QueryResult>, ErrorCode>
CentralizedClientService::Query(const std::string& object_key,
                                const ReadRouteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto result = master_client_.GetReplicaList(object_key, config);
    if (!result) {
        LOG(ERROR) << "Failed to get replica list: " << result.error();
        return tl::unexpected(result.error());
    }
    uint64_t lease_ttl_ms = 0;
    if (!result.value().centralized_extra) {
        LOG(ERROR)
            << "no_centralized_extra_found, lease_ttl_ms will be set to 0"
            << ", key=" << object_key;
    } else {
        lease_ttl_ms = result.value().centralized_extra->lease_ttl_ms;
    }
    return std::make_unique<CentralizedQueryResult>(
        std::move(result.value().replicas),
        start_time + std::chrono::milliseconds(lease_ttl_ms));
}

std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
CentralizedClientService::BatchQuery(
    const std::vector<std::string>& object_keys,
    const ReadRouteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
            results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::SHUTTING_DOWN));
        }
        return results;
    }
    std::chrono::steady_clock::time_point start_time =
        std::chrono::steady_clock::now();
    auto response = master_client_.BatchGetReplicaList(object_keys, config);

    // Check if we got the expected number of responses
    if (response.size() != object_keys.size()) {
        LOG(ERROR) << "BatchQuery response size mismatch. Expected: "
                   << object_keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
            results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }
    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>> results;
    results.reserve(response.size());
    for (size_t i = 0; i < response.size(); ++i) {
        if (response[i]) {
            uint64_t lease_ttl_ms = 0;
            if (!response[i].value().centralized_extra) {
                LOG(ERROR) << "no_centralized_extra_found, lease_ttl_ms will "
                              "be set to 0"
                           << ", key=" << object_keys[i];
            } else {
                lease_ttl_ms =
                    response[i].value().centralized_extra->lease_ttl_ms;
            }
            results.emplace_back(std::make_unique<CentralizedQueryResult>(
                std::move(response[i].value().replicas),
                start_time + std::chrono::milliseconds(lease_ttl_ms)));
        } else {
            results.emplace_back(tl::unexpected(response[i].error()));
        }
    }
    return results;
}

tl::expected<bool, ErrorCode> CentralizedClientService::IsExist(
    const std::string& key) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_.ExistKey(key);
    if (!result) {
        LOG(ERROR) << "Failed to query key"
                   << ", key:" << key << ", error:" << result.error();
        return tl::unexpected(result.error());
    }
    return result;
}

std::vector<tl::expected<bool, ErrorCode>>
CentralizedClientService::BatchIsExist(const std::vector<std::string>& keys) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return std::vector<tl::expected<bool, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::SHUTTING_DOWN));
    }
    auto results = master_client_.BatchExistKey(keys);
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i]) {
            LOG(ERROR) << "Failed to query key"
                       << ", key:" << keys[i]
                       << ", error:" << results[i].error();
        }
    }
    return results;
}

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedClientService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result =
        master_client_.BatchReplicaClear(object_keys, client_id, segment_name);
    return result;
}

tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>
CentralizedClientService::Get(const std::string& key,
                              std::shared_ptr<ClientBufferAllocator> allocator,
                              const ReadRouteConfig& config) {
    if (!allocator) {
        LOG(ERROR) << "Client buffer allocator is not provided";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Query to get size first
    auto query_result = Query(key, config);
    if (!query_result) {
        LOG(ERROR) << "Failed to query key"
                   << ", key:" << key << ", error:" << query_result.error();
        return tl::unexpected(query_result.error());
    }

    const auto& replica_list = query_result.value()->replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // currently, centralization path assumes only one replica
    const auto& replica = replica_list[0];
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

    auto get_result = InnerGet(key, *query_result.value(), slices);
    if (!get_result) {
        LOG(ERROR) << "Failed to get key"
                   << ", key:" << key << ", error:" << get_result.error();
        return tl::unexpected(get_result.error());
    }

    return std::make_shared<BufferHandle>(std::move(buffer_handle));
}

std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
CentralizedClientService::BatchGet(
    const std::vector<std::string>& keys,
    std::shared_ptr<ClientBufferAllocator> allocator,
    const ReadRouteConfig& config) {
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
    auto query_results = BatchQuery(keys, config);

    // Prepare valid operations for batch get
    struct KeyOp {
        size_t original_index;
        std::unique_ptr<QueryResult> query_result;
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
        if (query_ptr->replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << keys[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        const auto& replica = query_ptr->replicas[0];
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
    std::vector<std::unique_ptr<QueryResult>> batch_qr;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices;
    batch_keys.reserve(valid_ops.size());
    batch_qr.reserve(valid_ops.size());

    for (auto& op : valid_ops) {
        batch_keys.push_back(keys[op.original_index]);
        batch_qr.push_back(std::move(op.query_result));
        batch_slices[keys[op.original_index]] = op.slices;
    }

    auto batch_results = InnerBatchGet(batch_keys, batch_qr, batch_slices);

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

tl::expected<int64_t, ErrorCode> CentralizedClientService::Get(
    const std::string& key, const std::vector<void*>& buffers,
    const std::vector<size_t>& sizes, const ReadRouteConfig& config) {
    // Step 1: Query metadata from master
    auto query_result = Query(key, config);
    if (!query_result) {
        LOG(ERROR) << "Failed to query key: " << key;
        return tl::unexpected(query_result.error());
    }

    const auto& replica_list = query_result.value()->replicas;
    if (replica_list.empty()) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Step 2: Calculate total size and validate
    const auto& replica = replica_list[0];
    uint64_t total_size = calculate_total_size(replica);
    if (total_size == 0) {
        LOG(ERROR) << "Empty replica list for key: " << key;
        return tl::unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    size_t provided_size = 0;
    for (auto s : sizes) provided_size += s;
    if (provided_size < total_size) {
        LOG(ERROR) << "Buffer too small for key '" << key
                   << "': required=" << total_size
                   << ", provided=" << provided_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Step 3: Build correctly-sized slices and transfer data
    auto slices = BuildSlicesFromBuffers(buffers, sizes, total_size);
    auto get_result = InnerGet(key, *query_result.value(), slices);
    if (!get_result) {
        return tl::unexpected(get_result.error());
    }

    return static_cast<int64_t>(total_size);
}

std::vector<tl::expected<int64_t, ErrorCode>>
CentralizedClientService::BatchGet(
    const std::vector<std::string>& keys,
    const std::vector<std::vector<void*>>& all_buffers,
    const std::vector<std::vector<size_t>>& all_sizes,
    const ReadRouteConfig& config, bool aggregate_same_segment_task) {
    if (keys.size() != all_buffers.size() || keys.size() != all_sizes.size()) {
        LOG(ERROR) << "Input vector sizes mismatch";
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    // Query all keys
    auto query_results = BatchQuery(keys, config);

    std::vector<tl::expected<int64_t, ErrorCode>> results;
    results.reserve(keys.size());

    // Prepare valid operations
    struct ValidOp {
        size_t original_index;
        std::unique_ptr<QueryResult> query_result;
        std::vector<Slice> slices;
        uint64_t total_size;
    };
    std::vector<ValidOp> valid_ops;
    valid_ops.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        if (!query_results[i]) {
            auto error = query_results[i].error();
            results.emplace_back(tl::unexpected(error));
            if (error != ErrorCode::OBJECT_NOT_FOUND &&
                error != ErrorCode::REPLICA_IS_NOT_READY) {
                LOG(ERROR) << "Query failed for key '" << keys[i]
                           << "': " << toString(error);
            }
            continue;
        }

        // Validate replica list
        auto query_ptr = std::move(query_results[i].value());
        if (query_ptr->replicas.empty()) {
            LOG(ERROR) << "Empty replica list for key: " << keys[i];
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_REPLICA));
            continue;
        }

        const auto& replica = query_ptr->replicas[0];
        uint64_t total_size = calculate_total_size(replica);
        size_t provided_size = 0;
        for (auto s : all_sizes[i]) provided_size += s;

        if (provided_size < total_size) {
            LOG(ERROR) << "Buffer too small for key '" << keys[i]
                       << "': required=" << total_size
                       << ", available=" << provided_size;
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
            continue;
        }

        // Build correctly-sized slices from user buffers
        auto slices =
            BuildSlicesFromBuffers(all_buffers[i], all_sizes[i], total_size);

        // Optimistic: set success result now, override on failure
        results.emplace_back(static_cast<int64_t>(total_size));
        valid_ops.push_back(
            {i, std::move(query_ptr), std::move(slices), total_size});
    }

    if (valid_ops.empty()) {
        return results;
    }

    // Build batch get structures
    std::vector<std::string> batch_keys;
    std::vector<std::unique_ptr<QueryResult>> batch_qr;
    std::unordered_map<std::string, std::vector<Slice>> batch_slices_map;
    batch_keys.reserve(valid_ops.size());
    batch_qr.reserve(valid_ops.size());

    for (auto& op : valid_ops) {
        batch_keys.push_back(keys[op.original_index]);
        batch_qr.push_back(std::move(op.query_result));
        batch_slices_map[keys[op.original_index]] = op.slices;
    }

    auto batch_results = InnerBatchGet(batch_keys, batch_qr, batch_slices_map,
                                       aggregate_same_segment_task);

    for (size_t j = 0; j < valid_ops.size(); ++j) {
        if (!batch_results[j]) {
            const auto error = batch_results[j].error();
            LOG(ERROR) << "Batch get failed for key '"
                       << keys[valid_ops[j].original_index]
                       << "': " << toString(error);
            results[valid_ops[j].original_index] = tl::unexpected(error);
        }
    }

    return results;
}

tl::expected<void, ErrorCode> CentralizedClientService::InnerGet(
    const std::string& object_key, const QueryResult& query_result,
    std::vector<Slice>& slices) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
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
    auto* centralized_result =
        static_cast<const CentralizedQueryResult*>(&query_result);
    if (!centralized_result) {
        LOG(ERROR) << "query_result is not centralized key=" << object_key;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    } else if (centralized_result->IsLeaseExpired()) {
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

std::vector<tl::expected<void, ErrorCode>>
CentralizedClientService::InnerBatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<std::unique_ptr<QueryResult>>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices,
    bool aggregate_same_segment_task) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::SHUTTING_DOWN));
        }
        return results;
    }
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
    if (aggregate_same_segment_task) {
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
            FindFirstCompleteReplica(query_result->replicas, replica);
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
        const QueryResult* base_ptr = query_results[i].get();
        auto* centralized_result =
            static_cast<const CentralizedQueryResult*>(base_ptr);
        if (!centralized_result) {
            LOG(ERROR) << "query_result is not centralized key="
                       << object_keys[i];
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        } else if (results[i].has_value() &&
                   centralized_result->IsLeaseExpired(now)) {
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

std::vector<tl::expected<void, ErrorCode>>
CentralizedClientService::BatchGetWhenPreferSameNode(
    const std::vector<std::string>& object_keys,
    const std::vector<std::unique_ptr<QueryResult>>& query_results,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.resize(object_keys.size());

    std::unordered_map<std::string, BatchGetOperation> seg_to_op_map{};
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& replica_list = query_results[i]->replicas;
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

tl::expected<void, ErrorCode> CentralizedClientService::Put(
    const ObjectKey& key, std::vector<Slice>& slices,
    const WriteConfig& config) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    const auto* replicate_config = std::get_if<ReplicateConfig>(&config);
    if (!replicate_config) {
        LOG(ERROR) << "CentralizedClientService currently only supports "
                      "ReplicateConfig";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.emplace_back(slices[i].size);
    }

    // Start put operation
    auto start_result =
        master_client_.PutStart(key, slice_lengths, *replicate_config);
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
        value_length = ClientService::CalculateSliceSize(slices);
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

std::vector<PutOperation> CentralizedClientService::CreatePutOperations(
    const std::vector<ObjectKey>& keys,
    const std::vector<std::vector<Slice>>& batched_slices) {
    std::vector<PutOperation> ops;
    ops.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        ops.emplace_back(keys[i], batched_slices[i]);
    }
    return ops;
}

void CentralizedClientService::StartBatchPut(std::vector<PutOperation>& ops,
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

void CentralizedClientService::SubmitTransfers(std::vector<PutOperation>& ops) {
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

void CentralizedClientService::WaitForTransfers(
    std::vector<PutOperation>& ops) {
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

void CentralizedClientService::FinalizeBatchPut(
    std::vector<PutOperation>& ops) {
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

std::vector<tl::expected<void, ErrorCode>>
CentralizedClientService::CollectResults(const std::vector<PutOperation>& ops) {
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

std::vector<tl::expected<void, ErrorCode>>
CentralizedClientService::BatchPutWhenPreferSameNode(
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

std::vector<tl::expected<void, ErrorCode>> CentralizedClientService::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices,
    const WriteConfig& config) {
    const auto* replicate_config = std::get_if<ReplicateConfig>(&config);
    if (!replicate_config) {
        LOG(ERROR) << "CentralizedClientService currently only supports "
                      "ReplicateConfig";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }
    std::vector<PutOperation> ops = CreatePutOperations(keys, batched_slices);
    if (replicate_config->prefer_alloc_in_same_node) {
        if (replicate_config->replica_num != 1) {
            LOG(ERROR) << "prefer_alloc_in_same_node is not supported with "
                          "replica_num != 1";
            return std::vector<tl::expected<void, ErrorCode>>(
                keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        StartBatchPut(ops, *replicate_config);
        return BatchPutWhenPreferSameNode(ops);
    }
    StartBatchPut(ops, *replicate_config);

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

tl::expected<void, ErrorCode> CentralizedClientService::Remove(
    const ObjectKey& key) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_.Remove(key);
    // if (storage_backend_) {
    //     storage_backend_->RemoveFile(key);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<long, ErrorCode> CentralizedClientService::RemoveByRegex(
    const ObjectKey& str) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto result = master_client_.RemoveByRegex(str);
    // if (storage_backend_) {
    //     storage_backend_->RemoveByRegex(str);
    // }
    if (!result) {
        return tl::unexpected(result.error());
    }
    return result.value();
}

tl::expected<long, ErrorCode> CentralizedClientService::RemoveAll() {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    // if (storage_backend_) {
    //     storage_backend_->RemoveAll();
    // }
    return master_client_.RemoveAll();
}

tl::expected<void, ErrorCode> CentralizedClientService::MountSegment(
    const void* buffer, size_t size) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto check_result = CheckRegisterMemoryParams(buffer, size);
    if (!check_result) {
        return tl::unexpected(check_result.error());
    }

    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);

    // Check if the segment overlaps with any existing segment
    for (auto& it : mounted_segments_) {
        auto& mtseg = it.second;
        if (!mtseg.IsCentralizedSegment()) {
            continue;
        }
        auto& extra = mtseg.GetCentralizedExtra();
        uintptr_t l1 = extra.base;
        uintptr_t r1 = reinterpret_cast<uintptr_t>(mtseg.size) + l1;
        uintptr_t l2 = reinterpret_cast<uintptr_t>(buffer);
        uintptr_t r2 = reinterpret_cast<uintptr_t>(size) + l2;
        if (std::max(l1, l2) < std::min(r1, r2)) {
            LOG(ERROR) << "segment_overlaps base1=" << extra.base
                       << " size1=" << mtseg.size << " base2=" << buffer
                       << " size2=" << size;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    int rc = transfer_engine_->registerLocalMemory(
        (void*)buffer, size, kWildcardLocation, true, true);
    if (rc != 0) {
        LOG(ERROR) << "register_local_memory_failed base=" << buffer
                   << " size=" << size << ", error=" << rc;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Build segment with logical name; attach TE endpoint for transport
    Segment segment;
    segment.id = generate_uuid();
    segment.name = local_endpoint();
    segment.size = size;

    CentralizedSegmentExtraData extra;
    extra.base = reinterpret_cast<uintptr_t>(buffer);

    // For P2P handshake mode, publish the actual transport endpoint that was
    // negotiated by the transfer engine. Otherwise, keep the logical hostname
    // so metadata backends (HTTP/etcd/redis) can resolve the segment by name.
    if (metadata_connstring_ == P2PHANDSHAKE) {
        extra.te_endpoint = transfer_engine_->getLocalIpAndPort();
    } else {
        extra.te_endpoint = local_endpoint();
    }
    segment.extra = extra;

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

tl::expected<void, ErrorCode> CentralizedClientService::UnmountSegment(
    const void* buffer, size_t size) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return InnerUnmountSegment(buffer, size);
}

tl::expected<void, ErrorCode> CentralizedClientService::InnerUnmountSegment(
    const void* buffer, size_t size) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.end();

    for (auto it = mounted_segments_.begin(); it != mounted_segments_.end();
         ++it) {
        if (!it->second.IsCentralizedSegment()) {
            LOG(ERROR) << "segment_not_found base=" << buffer
                       << " size=" << size;
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (it->second.GetCentralizedExtra().base ==
                reinterpret_cast<uintptr_t>(buffer) &&
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

    int rc = transfer_engine_->unregisterLocalMemory(
        reinterpret_cast<void*>(segment->second.GetCentralizedExtra().base));
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

tl::expected<void, ErrorCode> CentralizedClientService::MountLocalDiskSegment(
    bool enable_offloading) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto response =
        master_client_.MountLocalDiskSegment(client_id_, enable_offloading);

    if (!response) {
        LOG(ERROR) << "MountLocalDiskSegment failed, error code is "
                   << response.error();
    }
    return response;
}

tl::expected<void, ErrorCode> CentralizedClientService::OffloadObjectHeartbeat(
    bool enable_offloading,
    std::unordered_map<std::string, int64_t>& offloading_objects) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto response =
        master_client_.OffloadObjectHeartbeat(client_id_, enable_offloading);
    if (!response) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed, error code is "
                   << response.error();
        return tl::unexpected(response.error());
    }
    offloading_objects = std::move(response.value());
    return {};
}

tl::expected<void, ErrorCode> CentralizedClientService::BatchPutOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, Slice>& batched_slices) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return {};
}

tl::expected<void, ErrorCode> CentralizedClientService::NotifyOffloadSuccess(
    const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    auto guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(ERROR) << "client is shutting down";
        return tl::unexpected(ErrorCode::SHUTTING_DOWN);
    }
    auto response =
        master_client_.NotifyOffloadSuccess(client_id_, keys, metadatas);
    return response;
}

void CentralizedClientService::PrepareStorageBackend(
    const std::string& storage_root_dir, const std::string& fsdir,
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

void CentralizedClientService::PutToLocalFile(
    const std::string& key, const std::vector<Slice>& slices,
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

ErrorCode CentralizedClientService::TransferData(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices,
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

ErrorCode CentralizedClientService::TransferWrite(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices) {
    return TransferData(replica_descriptor, slices, TransferRequest::WRITE);
}

ErrorCode CentralizedClientService::TransferRead(
    const Replica::Descriptor& replica_descriptor, std::vector<Slice>& slices) {
    size_t total_size = 0;
    if (replica_descriptor.is_memory_replica()) {
        auto& mem_desc = replica_descriptor.get_memory_descriptor();
        total_size = mem_desc.buffer_descriptor.size_;
    } else {
        auto& disk_desc = replica_descriptor.get_disk_descriptor();
        total_size = disk_desc.object_size;
    }

    size_t slices_size = ClientService::CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    return TransferData(replica_descriptor, slices, TransferRequest::READ);
}

HeartbeatRequest CentralizedClientService::build_heartbeat_request() {
    HeartbeatRequest req;
    req.client_id = client_id_;
    return req;
}

tl::expected<RegisterClientResponse, ErrorCode>
CentralizedClientService::RegisterClient() {
    // This lock must be held until the register rpc is finished,
    // otherwise there will be corner cases, e.g., a segment is
    // unmounted successfully first, and then registered again in
    // this thread.
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    std::vector<Segment> segments;
    for (auto it : mounted_segments_) {
        auto& segment = it.second;
        segments.emplace_back(segment);
    }

    RegisterClientRequest req;
    req.client_id = client_id_;
    req.segments = std::move(segments);
    req.deployment_mode = DeploymentMode::CENTRALIZATION;

    auto register_result = master_client_.RegisterClient(req);
    if (!register_result) {
        LOG(ERROR) << "Failed to register client: " << register_result.error();
    } else {
        view_version_ = register_result.value().view_version;
    }
    return register_result;
}

ErrorCode CentralizedClientService::FindFirstCompleteReplica(
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

}  // namespace mooncake
