#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <queue>
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
      allocation_strategy_(std::make_shared<RandomAllocationStrategy>()) {
    // Start the GC thread
    gc_running_ = true;
    gc_thread_ = std::thread(&MasterService::GCThreadFunc, this);
    VLOG(1) << "action=start_gc_thread";
}

MasterService::~MasterService() {
    // Stop and join the GC thread
    gc_running_ = false;
    if (gc_thread_.joinable()) {
        gc_thread_.join();
    }

    // Clean up any remaining GC tasks
    GCTask* task = nullptr;
    while (gc_queue_.pop(task)) {
        if (task) {
            delete task;
        }
    }
}

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
    VLOG(1) << "key=" << key << ", action=get_replica_list_start";

    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (const auto& replica : metadata.replicas) {
        if (replica.status != ReplicaStatus::COMPLETE) {
            LOG(WARNING) << "key=" << key << ", status=" << replica.status
                         << ", error=replica_not_ready";
            return ErrorCode::REPLICA_IS_NOT_READY;
        }
    }

    replica_list = metadata.replicas;
    if (VLOG_IS_ON(1)) {
        VLOG(1) << "key=" << key
                << ", replica_list=" << VectorToString(replica_list);
    }
    MarkForGC(key, 1000);  // After 1 second, the object will be removed
    return ErrorCode::OK;
}

ErrorCode MasterService::PutStart(const std::string& key, uint64_t value_length,
                                  const std::vector<uint64_t>& slice_lengths,
                                  const ReplicateConfig& config,
                                  std::vector<ReplicaInfo>& replica_list) {
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

    // Lock the shard and check if object already exists
    size_t shard_idx = getShardIndex(key);
    std::unique_lock<std::mutex> lock(metadata_shards_[shard_idx].mutex);

    auto it = metadata_shards_[shard_idx].metadata.find(key);
    if (it != metadata_shards_[shard_idx].metadata.end() &&
        !CleanupStaleHandles(it->second)) {
        LOG(INFO) << "key=" << key << ", info=object_already_exists";
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
    VLOG(1) << "key=" << key << ", action=put_end_start";

    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
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

ErrorCode MasterService::PutRevoke(const std::string& key) {
    VLOG(1) << "key=" << key << ", action=put_revoke_start";

    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
        if (replica.status != ReplicaStatus::PROCESSING) {
            LOG(ERROR) << "key=" << key << ", status=" << replica.status
                       << ", error=invalid_replica_status";
            return ErrorCode::INVALID_WRITE;
        }
    }

    accessor.Erase();
    VLOG(1) << "key=" << key << ", action=put_revoke_complete";
    return ErrorCode::OK;
}

ErrorCode MasterService::Remove(const std::string& key) {
    VLOG(1) << "key=" << key << ", action=remove_start";

    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
        if (replica.status != ReplicaStatus::COMPLETE) {
            LOG(ERROR) << "key=" << key << ", status=" << replica.status
                       << ", error=invalid_replica_status";
            return ErrorCode::REPLICA_IS_NOT_READY;
        }
    }

    // Remove object metadata
    accessor.Erase();
    VLOG(1) << "key=" << key << ", action=remove_complete";
    return ErrorCode::OK;
}

ErrorCode MasterService::MarkForGC(const std::string& key, uint64_t delay_ms) {
    VLOG(1) << "key=" << key << ", delay_ms=" << delay_ms
            << ", action=mark_for_gc";

    // Create a new GC task and add it to the queue
    GCTask* task = new GCTask(key, std::chrono::milliseconds(delay_ms));
    if (!gc_queue_.push(task)) {
        // Queue is full, delete the task to avoid memory leak
        delete task;
        LOG(ERROR) << "key=" << key << ", error=gc_queue_full";
        return ErrorCode::INTERNAL_ERROR;
    }

    VLOG(1) << "key=" << key << ", action=scheduled_for_gc";
    return ErrorCode::OK;
}

bool MasterService::CleanupStaleHandles(ObjectMetadata& metadata) {
    // Iterate through replicas and remove those with invalid allocators
    auto replica_it = metadata.replicas.begin();
    while (replica_it != metadata.replicas.end()) {
        // Use any_of algorithm to check if any handle has an invalid allocator
        bool has_invalid_handle = std::any_of(
            replica_it->handles.begin(), replica_it->handles.end(),
            [](const std::shared_ptr<BufHandle>& handle) {
                if (!handle->isAllocatorValid()) {
                    VLOG(1) << "key=" << handle->replica_meta.object_name
                            << ", segment=" << handle->segment_name
                            << ", action=found_invalid_handle";
                    return true;
                }
                return false;
            });

        // Remove replicas with invalid handles using erase-remove idiom
        if (has_invalid_handle) {
            VLOG(1) << "replica_id=" << replica_it->replica_id
                    << ", action=removing_replica_with_invalid_handle";
            replica_it = metadata.replicas.erase(replica_it);
        } else {
            ++replica_it;
        }
    }

    // Return true if no valid replicas remain after cleanup
    return metadata.replicas.empty();
}

void MasterService::GCThreadFunc() {
    VLOG(1) << "action=gc_thread_started";

    std::priority_queue<GCTask*, std::vector<GCTask*>, GCTaskComparator>
        local_pq;

    while (gc_running_) {
        GCTask* task = nullptr;
        while (gc_queue_.pop(task)) {
            if (task) {
                local_pq.push(task);
            }
        }

        while (!local_pq.empty()) {
            task = local_pq.top();
            if (!task->is_ready()) {
                break;
            }

            local_pq.pop();
            VLOG(1) << "key=" << task->key << ", action=gc_removing_key";
            ErrorCode result = Remove(task->key);
            if (result != ErrorCode::OK &&
                result != ErrorCode::OBJECT_NOT_FOUND) {
                LOG(WARNING)
                    << "key=" << task->key
                    << ", error=gc_remove_failed, error_code=" << result;
            }
            delete task;
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kGCThreadSleepMs));
    }

    while (!local_pq.empty()) {
        delete local_pq.top();
        local_pq.pop();
    }

    VLOG(1) << "action=gc_thread_stopped";
}

}  // namespace mooncake
