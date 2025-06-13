#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <queue>
#include <shared_mutex>

#include "master_metric_manager.h"
#include "types.h"

namespace mooncake {

MasterService::MasterService(bool enable_gc, uint64_t default_kv_lease_ttl,
                             double eviction_ratio,
                             double eviction_high_watermark_ratio,
                             ViewVersionId view_version)
    : allocation_strategy_(std::make_shared<RandomAllocationStrategy>()),
      enable_gc_(enable_gc),
      default_kv_lease_ttl_(default_kv_lease_ttl),
      eviction_ratio_(eviction_ratio),
      eviction_high_watermark_ratio_(eviction_high_watermark_ratio) {
    if (eviction_ratio_ < 0.0 || eviction_ratio_ > 1.0) {
        LOG(ERROR) << "Eviction ratio must be between 0.0 and 1.0, "
                   << "current value: " << eviction_ratio_;
        throw std::invalid_argument("Invalid eviction ratio");
    }
    if (eviction_high_watermark_ratio_ < 0.0 ||
        eviction_high_watermark_ratio_ > 1.0) {
        LOG(ERROR)
            << "Eviction high watermark ratio must be between 0.0 and 1.0, "
            << "current value: " << eviction_high_watermark_ratio_;
        throw std::invalid_argument("Invalid eviction high watermark ratio");
    }
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

ErrorCode MasterService::MountSegment(const Segment& segment, const UUID& client_id) {
    const uintptr_t buffer = segment.base;
    const size_t size = segment.size;

    // Check if parameters are valid before allocating memory.
    if (buffer == 0 || size == 0 ||
        buffer % facebook::cachelib::Slab::kSize ||
        size % facebook::cachelib::Slab::kSize) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize;
        return ErrorCode::INVALID_PARAMS;
    }

    std::unique_lock<std::shared_mutex> lock(segment_mutex_);

    // Check if segment already exists
    auto exist_segment_it = mounted_segments_.find(segment.id);
    if (exist_segment_it != mounted_segments_.end()) {
        auto &exist_segment = exist_segment_it->second;
        if (exist_segment.status == SegmentStatus::OK) {
            // Return OK because this is an idempotent operation
            LOG(WARNING) << "segment_name=" << segment.name
                         << ", warn=segment_already_exists";
            return ErrorCode::OK;
        }
        LOG(ERROR) << "segment_name=" << segment.name
                     << ", error=segment_already_exists_but_not_ok"
                     << ", status=" << exist_segment.status;
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    }

    // Tell the client monitor thread to start timing for this client. To avoid
    // the following undesired situations, this message must be sent before the
    // segment lock is released:
    // 1. Sending the message before the lock: the client expires and unmouting
    // invokes before this mounting are completed, which prevent this segment
    // being able to be unmounted forever;
    // 2. Sending the message after mounting the segment: After mounting this
    // segment, when trying to push id to the queue, the queue is already full,
    // which may block this request forever.
    PodUUID pod_client_id;
    pod_client_id.first = client_id.first;
    pod_client_id.second = client_id.second;
    if (!client_ping_queue_.push(pod_client_id)) {
        LOG(ERROR) << "segment_name=" << segment.name
                   << ", error=client_ping_queue_full";
        return ErrorCode::INTERNAL_ERROR;
    }

    std::shared_ptr<BufferAllocator> allocator;
    try {
        // SlabAllocator may throw an exception if the size or base is invalid
        // for the slab allocator.
        allocator = std::make_shared<BufferAllocator>(segment.name, buffer, size);
        if (!allocator) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=failed_to_create_allocator";
            return ErrorCode::INVALID_PARAMS;
        }
    } catch (...) {
        LOG(ERROR) << "segment_name=" << segment.name
                   << ", error=unknown_exception_during_allocator_creation";
        return ErrorCode::INVALID_PARAMS;
    }

    ok_allocators_.push_back(allocator);
    ok_allocators_by_name_[segment.name].push_back(allocator);
    client_segments_[client_id].push_back(segment.id);
    mounted_segments_[segment.id] = {segment, SegmentStatus::OK, std::move(allocator)};

    return ErrorCode::OK;
}

//void MasterService::PrepareUnmountSegment(std::unique_lock<std::shared_mutex>& lock, const UUID& segment_id) {
//}

ErrorCode MasterService::UnmountSegment(const UUID& segment_id, const UUID& client_id) {
    size_t metrics_dec_capacity = 0; // to update the metrics
    {
        std::unique_lock<std::shared_mutex> lock(segment_mutex_);
        auto it = mounted_segments_.find(segment_id);
        if (it == mounted_segments_.end()) {
            // Return OK because this is an idempotent operation
            LOG(WARNING) << "segment_id=" << segment_id
                        << ", warn=segment_not_found";
            return ErrorCode::OK;
        }
        if (it->second.status == SegmentStatus::UNMOUNTING) {
            LOG(ERROR) << "segment_id=" << segment_id
                       << ", error=segment_is_being_unmounted";
            return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        }

        auto& mounted_segment = it->second;
        auto& segment = mounted_segment.segment;
        metrics_dec_capacity = segment.size;

        // 1. Remove the allocator
        std::shared_ptr<BufferAllocator> allocator = mounted_segment.buf_allocator;
        
        // 1.1 Remove from ok_allocators
        auto alloc_it = std::find(ok_allocators_.begin(), ok_allocators_.end(), allocator);
        if (alloc_it != ok_allocators_.end()) {
            ok_allocators_.erase(alloc_it);
        } else {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=alloctor_not_found_in_ok_allocators";
        }

        // 1.2 Remove from ok_allocators_by_name
        bool found_in_ok_allocators_by_name = false;
        auto name_it = ok_allocators_by_name_.find(segment.name);
        if (name_it != ok_allocators_by_name_.end()) {
            auto& allocators = name_it->second;
            auto alloc_it = std::find(allocators.begin(), allocators.end(), allocator);
            if (alloc_it != allocators.end()) {
                allocators.erase(alloc_it);
                found_in_ok_allocators_by_name = true;
            }
            if (allocators.empty()) {
                ok_allocators_by_name_.erase(name_it);
            }
        }
        if (!found_in_ok_allocators_by_name) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=alloctor_not_found_in_ok_allocators_by_name";
        }

        // 1.3 Remove from mounted_segment
        mounted_segment.buf_allocator.reset();

        // 1.4 Set the segment status to UNMOUNTING
        mounted_segment.status = SegmentStatus::UNMOUNTING;

        // 1.5 Remove from client_segments_
        bool found_in_client_segments = false;
        auto client_it = client_segments_.find(client_id);
        if (client_it != client_segments_.end()) {
            auto& segments = client_it->second;
            auto segment_it = std::find(segments.begin(), segments.end(), segment_id);
            if (segment_it != segments.end()) {
                segments.erase(segment_it);
                found_in_client_segments = true;
            }
        }
        if (!found_in_client_segments) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=segment_not_found_in_client_segments";
        }
    }

    // 2. Remove the metadata of the related objects
    for (auto& shard : metadata_shards_) {
        std::unique_lock lock(shard.mutex);
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            // Check if the object has any invalid replicas
            bool has_invalid = false;
            for (auto& replica : it->second.replicas) {
                if (replica.has_invalid_handle()) {
                    has_invalid = true;
                    break;
                }
            }
            // Remove the object if it has no valid replicas
            if (has_invalid || CleanupStaleHandles(it->second)) {
                it = shard.metadata.erase(it);
                MasterMetricManager::instance().dec_key_count(1);
            } else {
                ++it;
            }
        }
    }

    // 3. Remove segment
    {
        std::unique_lock<std::shared_mutex> lock(segment_mutex_);
        mounted_segments_.erase(segment_id);
        MasterMetricManager::instance().dec_total_capacity(metrics_dec_capacity);
    }
    return ErrorCode::OK;
}

ErrorCode MasterService::ExistKey(const std::string& key) {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
        LOG(WARNING) << "key=" << key << ", status=" << *status
                     << ", error=replica_not_ready";
        return ErrorCode::REPLICA_IS_NOT_READY;
    }

    // Grant a lease to the object as it may be further used by the client.
    metadata.GrantLease(default_kv_lease_ttl_);

    return ErrorCode::OK;
}

ErrorCode MasterService::GetAllKeys(std::vector<std::string> & all_keys) {
    all_keys.clear();
    for(size_t i = 0; i < kNumShards; i++) {
        for(const auto& item : metadata_shards_[i].metadata) {
            all_keys.push_back(item.first);
        }
    }
    return ErrorCode::OK;
}

ErrorCode MasterService::GetAllSegments(std::vector<std::string> & all_segments) {
    all_segments.clear();
    std::shared_lock<std::shared_mutex> alloc_lock(segment_mutex_);
    for(auto & segment : mounted_segments_) {
        all_segments.push_back(segment.second.segment.name);
    }
    alloc_lock.unlock();
    return ErrorCode::OK;
}

ErrorCode MasterService::QuerySegments(const std::string & segment,
                                       size_t & used,
                                       size_t & capacity) {
/* todo 
    std::shared_lock<std::shared_mutex> alloc_lock(
        allocator_mutex_);
    const auto& allocators = buf_allocators_;
    auto it = allocators.find(segment);
    if (it != allocators.end()) {
        auto& allocator = it -> second;
        capacity = allocator -> capacity();
        used = allocator -> size();
    } else {
        VLOG(1) << "### DEBUG ### MasterService::QuerySegments(" << segment << ") not found!";
        return ErrorCode::AVAILABLE_SEGMENT_EMPTY;
    }
    alloc_lock.unlock();*/
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
    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
        LOG(WARNING) << "key=" << key << ", status=" << *status
                     << ", error=replica_not_ready";
        return ErrorCode::REPLICA_IS_NOT_READY;
    }

    replica_list.clear();
    replica_list.reserve(metadata.replicas.size());
    for (const auto& replica : metadata.replicas) {
        replica_list.emplace_back(replica.get_descriptor());
    }

    // Only mark for GC if enabled
    if (enable_gc_) {
        MarkForGC(key, 1000);  // After 1 second, the object will be removed
    } else {
        // Grant a lease to the object so it will not be removed
        // when the client is reading it.
        metadata.GrantLease(default_kv_lease_ttl_);
    }

    return ErrorCode::OK;
}

ErrorCode MasterService::BatchGetReplicaList(
    const std::vector<std::string>& keys,
    std::unordered_map<std::string, std::vector<Replica::Descriptor>>&
        batch_replica_list) {
    for (const auto& key : keys) {
        if (GetReplicaList(key, batch_replica_list[key]) != ErrorCode::OK) {
            LOG(ERROR) << "key=" << key << ", error=object_not_found";
            return ErrorCode::OBJECT_NOT_FOUND;
        };
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
    {
        std::shared_lock<std::shared_mutex> alloc_lock(segment_mutex_);
        for (size_t i = 0; i < config.replica_num; ++i) {
            std::vector<std::unique_ptr<AllocatedBuffer>> handles;
            handles.reserve(slice_lengths.size());

            // Allocate space for each slice
            for (size_t j = 0; j < slice_lengths.size(); ++j) {
                auto chunk_size = slice_lengths[j];

                // Use the unified allocation strategy with replica config
                auto handle =
                    allocation_strategy_->Allocate(ok_allocators_, ok_allocators_by_name_, chunk_size, config);
                alloc_lock.unlock();

                if (!handle) {
                    LOG(ERROR) << "key=" << key << ", replica_id=" << i
                            << ", slice_index=" << j
                            << ", error=allocation_failed";
                    replica_list.clear();
                    // If the allocation failed, we need to evict some objects
                    // to free up space for future allocations.
                    need_eviction_ = true;
                    return ErrorCode::NO_AVAILABLE_HANDLE;
                }

                VLOG(1) << "key=" << key << ", replica_id=" << i
                        << ", slice_index=" << j << ", handle=" << *handle
                        << ", action=slice_allocated";
                handles.emplace_back(std::move(handle));
            }

            replicas.emplace_back(std::move(handles), ReplicaStatus::PROCESSING);
        }
    }

    metadata.replicas = std::move(replicas);

    replica_list.clear();
    replica_list.reserve(metadata.replicas.size());
    for (const auto& replica : metadata.replicas) {
        replica_list.emplace_back(replica.get_descriptor());
    }

    // No need to set lease here. The object will not be evicted until
    // PutEnd is called.
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
    // Set lease timeout to now, indicating that the object has no lease
    // at beginning
    metadata.GrantLease(0);
    return ErrorCode::OK;
}

ErrorCode MasterService::PutRevoke(const std::string& key) {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();
    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::PROCESSING)) {
        LOG(ERROR) << "key=" << key << ", status=" << *status
                   << ", error=invalid_replica_status";
        return ErrorCode::INVALID_WRITE;
    }

    accessor.Erase();
    return ErrorCode::OK;
}

ErrorCode MasterService::BatchPutStart(
    const std::vector<std::string>& keys,
    const std::unordered_map<std::string, uint64_t>& value_lengths,
    const std::unordered_map<std::string, std::vector<uint64_t>>& slice_lengths,
    const ReplicateConfig& config,
    std::unordered_map<std::string, std::vector<Replica::Descriptor>>& batch_replica_list) {
    if (config.replica_num == 0 || keys.empty()) {
        LOG(ERROR) << "replica_num=" << config.replica_num
                   << ", keys_size=" << keys.size() << ", error=invalid_params";
        return ErrorCode::INVALID_PARAMS;
    }

    for (const auto& key : keys) {
        auto value_length_it = value_lengths.find(key);
        auto slice_length_it = slice_lengths.find(key);
        if (value_length_it == value_lengths.end() ||
            slice_length_it == slice_lengths.end()) {
            LOG(ERROR) << "Key not found in value_lengths or slice_lengths: "
                       << key;
            return ErrorCode::OBJECT_NOT_FOUND;
        }

        auto result =
            PutStart(key, value_length_it->second, slice_length_it->second,
                     config, batch_replica_list[key]);
        if (result != ErrorCode::OK &&
            result != ErrorCode::OBJECT_ALREADY_EXISTS) {
            return result;
        }
    }
    return ErrorCode::OK;
}

ErrorCode MasterService::BatchPutEnd(const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
        auto result = PutEnd(key);
        if (result != ErrorCode::OK) {
            return result;
        }
    }
    return ErrorCode::OK;
}

ErrorCode MasterService::BatchPutRevoke(const std::vector<std::string>& keys) {
    for (const auto& key : keys) {
        auto result = PutRevoke(key);
        if (result != ErrorCode::OK) {
            return result;
        }
    }
    return ErrorCode::OK;
}

ErrorCode MasterService::Remove(const std::string& key) {
    MetadataAccessor accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return ErrorCode::OBJECT_NOT_FOUND;
    }

    auto& metadata = accessor.Get();

    if (!metadata.IsLeaseExpired()) {
        VLOG(1) << "key=" << key << ", error=object_has_lease";
        return ErrorCode::OBJECT_HAS_LEASE;
    }

    if (auto status = metadata.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
        LOG(ERROR) << "key=" << key << ", status=" << *status
                   << ", error=invalid_replica_status";
        return ErrorCode::REPLICA_IS_NOT_READY;
    }

    // Remove object metadata
    accessor.Erase();
    return ErrorCode::OK;
}

long MasterService::RemoveAll() {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    // Store the current time to avoid repeatedly
    // calling std::chrono::steady_clock::now()
    auto now = std::chrono::steady_clock::now();

    for (auto& shard : metadata_shards_) {
        std::unique_lock lock(shard.mutex);
        if (shard.metadata.empty()) {
            continue;
        }

        // Only remove objects with expired leases
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            if (it->second.IsLeaseExpired(now)) {
                total_freed_size +=
                    it->second.size * it->second.replicas.size();
                it = shard.metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    if (removed_count > 0) {
        // Update metrics only if objects were actually removed
        MasterMetricManager::instance().dec_key_count(removed_count);
    }
    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
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

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (const auto& shard : metadata_shards_) {
        std::unique_lock lock(shard.mutex);
        total += shard.metadata.size();
    }
    return total;
}

ErrorCode MasterService::Ping(const UUID& client_id, ViewVersionId& view_version, ClientStatus& client_status) {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    auto it = ok_client_.find(client_id);
    if (it != ok_client_.end()) {
        client_status = ClientStatus::OK;
    } else {
        client_status = ClientStatus::NEED_REMOUNT;
    }
    view_version = view_version_;
    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id << ", error=client_ping_queue_full";
        return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

void MasterService::GCThreadFunc() {
    VLOG(1) << "action=gc_thread_started";

    std::priority_queue<GCTask*, std::vector<GCTask*>, GCTaskComparator>
        local_pq;

    while (gc_running_) {
        GCTask* task = nullptr;
        long gc_count = 0;
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
                result != ErrorCode::OBJECT_NOT_FOUND &&
                result != ErrorCode::OBJECT_HAS_LEASE) {
                LOG(WARNING)
                    << "key=" << task->key
                    << ", error=gc_remove_failed, error_code=" << result;
            }
            if (result == ErrorCode::OK) {
                gc_count++;
            }
            delete task;
        }
        if (gc_count > 0) {
            MasterMetricManager::instance().dec_key_count(gc_count);
        }

        double used_ratio =
            MasterMetricManager::instance().get_global_used_ratio();
        if (used_ratio > eviction_high_watermark_ratio_ ||
            (need_eviction_ && eviction_ratio_ > 0.0)) {
            BatchEvict(std::max(
                eviction_ratio_,
                used_ratio - eviction_high_watermark_ratio_ + eviction_ratio_));
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

void MasterService::BatchEvict(double eviction_ratio) {
    auto now = std::chrono::steady_clock::now();
    long evicted_count = 0;
    long object_count = 0;
    uint64_t total_freed_size = 0;

    // Randomly select a starting shard to avoid imbalance eviction between
    // shards. No need to use expensive random_device here.
    size_t start_idx = rand() % metadata_shards_.size();
    for (size_t i = 0; i < metadata_shards_.size(); i++) {
        auto& shard =
            metadata_shards_[(start_idx + i) % metadata_shards_.size()];
        std::unique_lock lock(shard.mutex);

        // object_count must be updated at beginning as it will be used later
        // to compute ideal_evict_num
        object_count += shard.metadata.size();

        // To achieve evicted_count / object_count = eviction_ration,
        // ideally how many object should be evicted in this shard
        const long ideal_evict_num =
            std::ceil(object_count * eviction_ratio_) - evicted_count;

        if (ideal_evict_num <= 0) {
            // No need to evict any object in this shard
            continue;
        }

        std::vector<std::chrono::steady_clock::time_point>
            candidates;  // can be removed
        for (auto it = shard.metadata.begin(); it != shard.metadata.end();
             it++) {
            // Only evict objects that have not expired and are complete
            if (it->second.IsLeaseExpired(now) &&
                !it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
                candidates.push_back(it->second.lease_timeout);
            }
        }

        if (!candidates.empty()) {
            long evict_num = std::min(ideal_evict_num, (long)candidates.size());
            long shard_evicted_count =
                0;  // number of objects evicted from this shard
            std::nth_element(candidates.begin(),
                             candidates.begin() + (evict_num - 1),
                             candidates.end());
            auto target_timeout = candidates[evict_num - 1];
            // Evict objects with lease timeout less than or equal to target.
            auto it = shard.metadata.begin();
            while (it != shard.metadata.end() &&
                   shard_evicted_count < evict_num) {
                if (it->second.lease_timeout <= target_timeout &&
                    !it->second.HasDiffRepStatus(ReplicaStatus::COMPLETE)) {
                    total_freed_size +=
                        it->second.size * it->second.replicas.size();
                    it = shard.metadata.erase(it);
                    shard_evicted_count++;
                } else {
                    ++it;
                }
            }
            evicted_count += shard_evicted_count;
        }
    }

    if (evicted_count > 0) {
        need_eviction_ = false;
        MasterMetricManager::instance().dec_key_count(evicted_count);
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
    } else {
        if (object_count == 0) {
            // No objects to evict, no need to check again
            need_eviction_ = false;
        }
        MasterMetricManager::instance().inc_eviction_fail();
    }
    VLOG(1) << "action=evict_objects"
            << ", evicted_count=" << evicted_count
            << ", total_freed_size=" << total_freed_size;
}

void MasterService::ClientMonitorFunc() {
    std::unordered_map<UUID, std::chrono::steady_clock::time_point, boost::hash<UUID>> client_ttl;
    while (client_monitor_running_) {
        PodUUID pod_client_id;
        auto now = std::chrono::steady_clock::now();

        // Update the client ttl
        while (client_ping_queue_.pop(pod_client_id)) {
            UUID client_id = {pod_client_id.first, pod_client_id.second};
            auto it = client_ttl.find(client_id);
            if (it == client_ttl.end()) {
                client_ttl[client_id] = now + std::chrono::seconds(CLIENT_LIVE_TTL_SEC);
            } else {
                it->second = now + std::chrono::seconds(CLIENT_LIVE_TTL_SEC);
            }
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            if (it->second < now) {
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                ++it;
            }
        }

        std::vector<UUID> need_remount_clients;
        if (!expired_clients.empty()) {
            std::unique_lock<std::shared_mutex> lock(client_mutex_);
            for (auto& client_id : expired_clients) {
                auto it = ok_client_.find(client_id);
                if (it != ok_client_.end()) {
                    ok_client_.erase(it);
                    need_remount_clients.push_back(client_id);
                }
            }
            
            // For soundness, one of the following conditions must be hold:
            // 1. Changing the client status to NEED_REMOUNT and their segment
            // status to UNMOUNTING in an atomic way.
            // 2. Changing the segment status to UNMOUNTING first, then the client
            // status to NEED_REMOUNT
            // So we must hold the client_mutex_ here
            if (!need_remount_clients.empty()) {
                std::unique_lock<std::shared_mutex> lock(segment_mutex_);
                for (auto& client_id : need_remount_clients) {
                    for (auto& segment_id : client_segments_[client_id]) {
                        UnmountSegment(segment_id, client_id);
                    }
                }
            }
        }


        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));
    }
}

}  // namespace mooncake
