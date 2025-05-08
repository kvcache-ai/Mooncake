#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <queue>
#include <shared_mutex>

#include "master_metric_manager.h"
#include "types.h"

namespace mooncake {

ErrorCode BufferAllocatorManager::AddSegment(const std::string& segment_name,
                                             uint64_t base, uint64_t size) {
    // Check if parameters are valid before allocating memory.
    if (base == 0 || size == 0 ||
        reinterpret_cast<uintptr_t>(base) % facebook::cachelib::Slab::kSize ||
        size % facebook::cachelib::Slab::kSize) {
        LOG(ERROR) << "base_address=" << base << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize;
        return ErrorCode::INVALID_PARAMS;
    }

    std::unique_lock<std::shared_mutex> lock(allocator_mutex_);

    // Check if segment already exists
    if (buf_allocators_.find(segment_name) != buf_allocators_.end()) {
        LOG(WARNING) << "segment_name=" << segment_name
                     << ", error=segment_already_exists";
        return ErrorCode::INVALID_PARAMS;
    }

    std::shared_ptr<BufferAllocator> allocator;
    try {
        // SlabAllocator may throw an exception if the size or base is invalid
        // for the slab allocator.
        allocator = std::make_shared<BufferAllocator>(segment_name, base, size);
        if (!allocator) {
            LOG(ERROR) << "segment_name=" << segment_name
                       << ", error=failed_to_create_allocator";
            return ErrorCode::INVALID_PARAMS;
        }
    } catch (...) {
        LOG(ERROR) << "segment_name=" << segment_name
                   << ", error=unknown_exception_during_allocator_creation";
        return ErrorCode::INVALID_PARAMS;
    }

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

    MasterMetricManager::instance().dec_total_capacity(it->second->capacity());
    buf_allocators_.erase(it);
    return ErrorCode::OK;
}

MasterService::MasterService(bool enable_gc)
    : buffer_allocator_manager_(std::make_shared<BufferAllocatorManager>()),
      allocation_strategy_(std::make_shared<RandomAllocationStrategy>()),
      enable_gc_(enable_gc) {
    // Start the GC thread if enabled
    if (enable_gc_) {
        gc_running_ = true;
        gc_thread_ = std::thread(&MasterService::GCThreadFunc, this);
        VLOG(1) << "action=start_gc_thread";
    } else {
        VLOG(1) << "action=gc_disabled";
    }
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

    return buffer_allocator_manager_->AddSegment(segment_name, buffer, size);
}

ErrorCode MasterService::UnmountSegment(const std::string& segment_name) {
    return buffer_allocator_manager_->RemoveSegment(segment_name);
}

ErrorCode MasterService::ExistKey(const std::string& key) {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (const auto& replica : metadata.replicas) {
        auto status = replica.status();
        if (status != ReplicaStatus::COMPLETE) {
            LOG(WARNING) << "key=" << key << ", status=" << status
                         << ", error=replica_not_ready";
            return ErrorCode::REPLICA_IS_NOT_READY;
        }
    }

    return ErrorCode::OK;
}

ErrorCode MasterService::GetReplicaList(
    const std::string& key, std::vector<Replica::Descriptor>& replica_list) {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (const auto& replica : metadata.replicas) {
        auto status = replica.status();
        if (status != ReplicaStatus::COMPLETE) {
            LOG(WARNING) << "key=" << key << ", status=" << status
                         << ", error=replica_not_ready";
            return ErrorCode::REPLICA_IS_NOT_READY;
        }
    }

    replica_list.clear();
    replica_list.reserve(metadata.replicas.size());
    for (const auto& replica : metadata.replicas) {
        replica_list.emplace_back(replica.get_descriptor());
    }

    // Only mark for GC if enabled
    if (enable_gc_) {
        MarkForGC(key, 1000);  // After 1 second, the object will be removed
    }

    return ErrorCode::OK;
}

ErrorCode MasterService::PutStart(
    const std::string& key, uint64_t value_length,
    const std::vector<uint64_t>& slice_lengths, const ReplicateConfig& config,
    std::vector<Replica::Descriptor>& replica_list) {
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
    std::vector<Replica> replicas;
    replicas.reserve(config.replica_num);
    for (size_t i = 0; i < config.replica_num; ++i) {
        std::vector<std::unique_ptr<AllocatedBuffer>> handles;
        handles.reserve(slice_lengths.size());

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

            VLOG(1) << "key=" << key << ", replica_id=" << i
                    << ", slice_index=" << j << ", handle=" << *handle
                    << ", action=slice_allocated";
            handles.emplace_back(std::move(handle));
        }

        replicas.emplace_back(std::move(handles), ReplicaStatus::PROCESSING);
    }

    metadata.replicas = std::move(replicas);

    replica_list.clear();
    replica_list.reserve(metadata.replicas.size());
    for (const auto& replica : metadata.replicas) {
        replica_list.emplace_back(replica.get_descriptor());
    }

    metadata_shards_[shard_idx].metadata[key] = std::move(metadata);
    return ErrorCode::OK;
}

ErrorCode MasterService::PutEnd(const std::string& key) {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
        replica.mark_complete();
    }
    return ErrorCode::OK;
}

ErrorCode MasterService::PutRevoke(const std::string& key) {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
        auto status = replica.status();
        if (status != ReplicaStatus::PROCESSING) {
            LOG(ERROR) << "key=" << key << ", status=" << status
                       << ", error=invalid_replica_status";
            return ErrorCode::INVALID_WRITE;
        }
    }

    accessor.Erase();
    return ErrorCode::OK;
}

ErrorCode MasterService::Remove(const std::string& key) {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    for (auto& replica : metadata.replicas) {
        auto status = replica.status();
        if (status != ReplicaStatus::COMPLETE) {
            LOG(ERROR) << "key=" << key << ", status=" << status
                       << ", error=invalid_replica_status";
            return ErrorCode::REPLICA_IS_NOT_READY;
        }
    }

    // Remove object metadata
    accessor.Erase();
    return ErrorCode::OK;
}

long MasterService::RemoveAll() {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    for (auto& shard : metadata_shards_) {
        std::unique_lock lock(shard.mutex);
        std::size_t object_count = shard.metadata.size();
        if (object_count == 0)
            continue;
        for (const auto& [key, metadata] : shard.metadata) {
            // Calculate the total freed size of the object
            total_freed_size += metadata.size * metadata.replicas.size();
        }
        removed_count += object_count;
        // Reset related metrics on successful removed
        MasterMetricManager::instance().dec_key_count(removed_count);
        MasterMetricManager::instance().dec_allocated_size(total_freed_size);
        shard.metadata.clear();
    }
    VLOG(1) << "action=remove_all_objects" << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

ErrorCode MasterService::MarkForGC(const std::string& key, uint64_t delay_ms) {
    // Create a new GC task and add it to the queue
    GCTask* task = new GCTask(key, std::chrono::milliseconds(delay_ms));
    if (!gc_queue_.push(task)) {
        // Queue is full, delete the task to avoid memory leak
        delete task;
        LOG(ERROR) << "key=" << key << ", error=gc_queue_full";
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

bool MasterService::CleanupStaleHandles(ObjectMetadata& metadata) {
    // Iterate through replicas and remove those with invalid allocators
    auto replica_it = metadata.replicas.begin();
    while (replica_it != metadata.replicas.end()) {
        // Use any_of algorithm to check if any handle has an invalid allocator
        bool has_invalid_handle = replica_it->has_invalid_handle();

        // Remove replicas with invalid handles using erase-remove idiom
        if (has_invalid_handle) {
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
