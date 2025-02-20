#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <shared_mutex>

#include "types.h"

namespace mooncake {

// Helper function to convert a vector to a string using the << operator
template <typename T>
std::string VectorToString(const std::vector<T>& vec) {
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        ss << vec[i];
        if (i < vec.size() - 1) {
            ss << ", ";
        }
    }
    ss << "]";
    return ss.str();
}

ErrorCode BufferAllocatorManager::AddSegment(const std::string& segment_name,
                                             uint64_t base, uint64_t size) {
    std::unique_lock<std::shared_mutex> lock(allocator_mutex_);

    // Check if segment already exists
    if (buf_allocators_.find(segment_name) != buf_allocators_.end()) {
        LOG(WARNING) << "segment_name=" << segment_name
                     << ", error=segment_already_exists";
        return ErrorCode::INVALID_PARAMS;
    }

    auto allocator =
        std::make_shared<BufferAllocator>(segment_name, base, size);
    if (!allocator) {
        LOG(ERROR) << "segment_name=" << segment_name
                   << ", error=failed_to_create_allocator";
        return ErrorCode::INTERNAL_ERROR;
    }
    VLOG(1) << "segment_name=" << segment_name << ", base=" << base
            << ", size=" << size << ", allocator_ptr=" << allocator.get()
            << ", action=register_buffer";
    buf_allocators_[segment_name] = std::move(allocator);
    return ErrorCode::OK;
}

ErrorCode BufferAllocatorManager::RemoveSegment(
    const std::string& segment_name) {
    std::unique_lock<std::shared_mutex> lock(allocator_mutex_);

    auto it = buf_allocators_.find(segment_name);
    if (it == buf_allocators_.end()) {
        LOG(WARNING) << "segment_name=" << segment_name
                     << ", error=segment_not_found";
        return ErrorCode::INVALID_PARAMS;
    }

    VLOG(1) << "segment_name=" << segment_name << ", action=unregister_buffer";
    // Remove buffer allocator
    buf_allocators_.erase(it);
    return ErrorCode::OK;
}

MasterService::MasterService()
    : buffer_allocator_manager_(std::make_shared<BufferAllocatorManager>()),
      allocation_strategy_(std::make_shared<RandomAllocationStrategy>()) {}

MasterService::~MasterService() = default;

ErrorCode MasterService::MountSegment(uint64_t buffer, uint64_t size,
                                      const std::string& segment_name) {
    if (buffer == 0 || size == 0) {
        LOG(ERROR) << "buffer=" << buffer << ", size=" << size
                   << ", error=invalid_buffer_params";
        return ErrorCode::INVALID_PARAMS;
    }

    VLOG(1) << "segment_name=" << segment_name << ", buffer=" << buffer
            << ", size=" << size << ", action=mount_segment";
    return buffer_allocator_manager_->AddSegment(segment_name, buffer, size);
}

ErrorCode MasterService::UnmountSegment(const std::string& segment_name) {
    VLOG(1) << "segment_name=" << segment_name << ", action=unmount_segment";
    return buffer_allocator_manager_->RemoveSegment(segment_name);
}

ErrorCode MasterService::GetReplicaList(
    const std::string& key, std::vector<ReplicaInfo>& replica_list) {
    size_t shard_idx = getShardIndex(key);
    std::shared_lock<std::shared_mutex> lock(metadata_shards_[shard_idx].mutex);

    VLOG(1) << "key=" << key << ", action=get_replica_list_start";
    auto it = metadata_shards_[shard_idx].metadata.find(key);
    if (it == metadata_shards_[shard_idx].metadata.end()) {
        LOG(WARNING) << "key=" << key << ", error=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    // Check if the object is still in the process of being put
    for (const auto& replica : it->second.replicas) {
        if (replica.status != ReplicaStatus::COMPLETE) {
            LOG(WARNING) << "key=" << key << ", status=" << replica.status
                         << ", error=replica_not_ready";
            return ErrorCode::REPLICA_IS_NOT_READY;
        }
    }
    replica_list = it->second.replicas;
    if (VLOG_IS_ON(1)) {
        VLOG(1) << "key=" << key
                << ", replica_list=" << VectorToString(replica_list);
    }
    return ErrorCode::OK;
}

ErrorCode MasterService::PutStart(const std::string& key, uint64_t value_length,
                                  const std::vector<uint64_t>& slice_lengths,
                                  const ReplicateConfig& config,
                                  std::vector<ReplicaInfo>& replica_list) {
    size_t shard_idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(metadata_shards_[shard_idx].mutex);

    if (config.replica_num == 0 || value_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", value_length=" << value_length
                   << ", error=invalid_params";
        return ErrorCode::INVALID_PARAMS;
    }

    // Validate slice lengths
    uint64_t total_length = 0;
    for (size_t i = 0; i < slice_lengths.size(); ++i) {
        if (slice_lengths[i] > kMaxSliceSize) {
            LOG(ERROR) << "key=" << key << ", slice_index=" << i
                       << ", slice_size=" << slice_lengths[i]
                       << ", max_size=" << kMaxSliceSize
                       << ", error=invalid_slice_size";
            return ErrorCode::INVALID_PARAMS;
        }
        total_length += slice_lengths[i];
    }

    if (total_length != value_length) {
        LOG(ERROR) << "key=" << key << ", total_length=" << total_length
                   << ", expected_length=" << value_length
                   << ", error=slice_length_mismatch";
        return ErrorCode::INVALID_PARAMS;
    }

    VLOG(1) << "key=" << key << ", value_length=" << value_length
            << ", slice_count=" << slice_lengths.size() << ", config=" << config
            << ", action=put_start_begin";

    // Check if object already exists
    auto it = metadata_shards_[shard_idx].metadata.find(key);
    if (it != metadata_shards_[shard_idx].metadata.end()) {
        LOG(WARNING) << "key=" << key << ", error=object_already_exists";
        return ErrorCode::OBJECT_ALREADY_EXISTS;
    }

    // Initialize object metadata
    ObjectMetadata metadata;
    metadata.size = value_length;

    // Allocate replicas
    for (size_t i = 0; i < config.replica_num; ++i) {
        ReplicaInfo replica;
        replica.status = ReplicaStatus::PROCESSING;
        replica.replica_id = i;

        // Allocate space for each slice
        for (size_t j = 0; j < slice_lengths.size(); ++j) {
            auto chunk_size = slice_lengths[j];

            // Use allocation strategy to select an allocator
            std::shared_lock<std::shared_mutex> alloc_lock(
                buffer_allocator_manager_->GetMutex());
            const auto& allocators = buffer_allocator_manager_->GetAllocators();
            auto handle =
                allocation_strategy_->Allocate(allocators, chunk_size);
            alloc_lock.unlock();

            if (!handle) {
                LOG(ERROR) << "key=" << key << ", replica_id=" << i
                           << ", slice_index=" << j
                           << ", error=allocation_failed";
                replica_list.clear();
                return ErrorCode::NO_AVAILABLE_HANDLE;
            }

            CHECK_EQ(handle->status, BufStatus::INIT);
            handle->replica_meta.object_name = key;
            handle->replica_meta.replica_id = i;
            replica.handles.emplace_back(handle);

            VLOG(1) << "key=" << key << ", replica_id=" << i
                    << ", slice_index=" << j << ", handle=" << *handle
                    << ", action=slice_allocated";
        }

        metadata.replicas.emplace_back(replica);
        replica_list.emplace_back(std::move(replica));
    }

    metadata_shards_[shard_idx].metadata[key] = std::move(metadata);
    VLOG(1) << "key=" << key << ", replica_count=" << config.replica_num
            << ", slice_count=" << slice_lengths.size()
            << ", action=put_start_complete";
    return ErrorCode::OK;
}

ErrorCode MasterService::PutEnd(const std::string& key) {
    size_t shard_idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(metadata_shards_[shard_idx].mutex);

    VLOG(1) << "key=" << key << ", action=put_end_start";

    auto it = metadata_shards_[shard_idx].metadata.find(key);
    if (it == metadata_shards_[shard_idx].metadata.end()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    for (auto& replica : it->second.replicas) {
        if (replica.status != ReplicaStatus::PROCESSING) {
            LOG(ERROR) << "key=" << key << ", status=" << replica.status
                       << ", error=invalid_replica_status";
            return ErrorCode::INVALID_WRITE;
        }
        replica.status = ReplicaStatus::COMPLETE;
        for (const auto& handle : replica.handles) {
            CHECK_EQ(handle->status, BufStatus::INIT)
                << "key=" << key << ", error=invalid_handle_status";
            handle->status = BufStatus::COMPLETE;
        }
    }
    VLOG(1) << "key=" << key << ", action=put_end_complete";
    return ErrorCode::OK;
}

ErrorCode MasterService::Remove(const std::string& key) {
    size_t shard_idx = getShardIndex(key);
    std::unique_lock<std::shared_mutex> lock(metadata_shards_[shard_idx].mutex);

    VLOG(1) << "key=" << key << ", action=remove_start";
    auto it = metadata_shards_[shard_idx].metadata.find(key);
    if (it == metadata_shards_[shard_idx].metadata.end()) {
        LOG(WARNING) << "key=" << key << ", error=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    for (auto& replica : it->second.replicas) {
        if (replica.status != ReplicaStatus::COMPLETE) {
            LOG(ERROR) << "key=" << key << ", status=" << replica.status
                       << ", error=invalid_replica_status";
            return ErrorCode::REPLICA_IS_NOT_READY;
        }
    }

    // Remove object metadata
    metadata_shards_[shard_idx].metadata.erase(it);
    VLOG(1) << "key=" << key << ", action=remove_complete";
    return ErrorCode::OK;
}

}  // namespace mooncake
