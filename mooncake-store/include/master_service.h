#pragma once

#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "allocation_strategy.h"
#include "allocator.h"
#include "types.h"

namespace mooncake {
// Forward declarations
class AllocationStrategy;

class BufferAllocatorManager {
   public:
    BufferAllocatorManager() = default;
    ~BufferAllocatorManager() = default;

    /**
     * @brief Register a new buffer for allocation
     * @return ErrorCode::OK on success, ErrorCode::INVALID_PARAMS if segment
     * exists
     */
    ErrorCode AddSegment(const std::string& segment_name, uint64_t base,
                         uint64_t size);

    /**
     * @brief Unregister a buffer
     * @return ErrorCode::OK on success, ErrorCode::INVALID_PARAMS if segment
     * not found
     */
    ErrorCode RemoveSegment(const std::string& segment_name);

    /**
     * @brief Get the map of buffer allocators
     * @note Caller must hold the mutex while accessing the map
     */
    const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
    GetAllocators() const {
        return buf_allocators_;
    }

    /**
     * @brief Get the mutex for thread-safe access
     */
    std::shared_mutex& GetMutex() { return allocator_mutex_; }

   private:
    // Protects the buffer allocator map (BufferAllocator is thread-safe by
    // itself)
    mutable std::shared_mutex allocator_mutex_;
    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        buf_allocators_;
};

class MasterService {
   public:
    MasterService();
    ~MasterService();

    /**
     * @brief Mount a memory segment for buffer allocation
     * @return ErrorCode::OK on success, ErrorCode::INVALID_PARAMS if segment
     * exists or params invalid, ErrorCode::INTERNAL_ERROR if allocation fails
     */
    ErrorCode MountSegment(uint64_t buffer, uint64_t size,
                           const std::string& segment_name);

    /**
     * @brief Unmount a memory segment
     * @return ErrorCode::OK on success, ErrorCode::INVALID_PARAMS if segment
     * not found
     */
    ErrorCode UnmountSegment(const std::string& segment_name);

    /**
     * @brief Get list of replicas for an object
     * @param[out] replica_list Vector to store replica information
     * @return ErrorCode::OK on success, ErrorCode::REPLICA_IS_NOT_READY if not
     * ready
     */
    ErrorCode GetReplicaList(const std::string& key,
                             std::vector<ReplicaInfo>& replica_list);

    /**
     * @brief Start a put operation for an object
     * @param[out] replica_list Vector to store replica information for slices
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if exists,
     *         ErrorCode::NO_AVAILABLE_HANDLE if allocation fails,
     *         ErrorCode::INVALID_PARAMS if slice size is invalid
     */
    ErrorCode PutStart(const std::string& key, uint64_t value_length,
                       const std::vector<uint64_t>& slice_lengths,
                       const ReplicateConfig& config,
                       std::vector<ReplicaInfo>& replica_list);

    /**
     * @brief Complete a put operation
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    ErrorCode PutEnd(const std::string& key);

    /**
     * @brief Remove an object and its replicas
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    ErrorCode Remove(const std::string& key);

   private:
    // Internal data structures
    struct ObjectMetadata {
        std::vector<ReplicaInfo> replicas;
        size_t size;
    };

    // Buffer allocator management
    std::shared_ptr<BufferAllocatorManager> buffer_allocator_manager_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, ObjectMetadata> metadata;
    };
    std::array<MetadataShard, kNumShards> metadata_shards_;

    // Helper to get shard index from key
    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % kNumShards;
    }
};

}  // namespace mooncake
