#pragma once

#include <array>
#include <shared_mutex>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <optional>
#include <ylt/util/tl/expected.hpp>
#include "tiered_cache/tiered_backend.h"
#include "transfer_engine.h"
#include "types.h"
#include "client_rpc_types.h"

namespace mooncake {

/**
 * @class DataManager
 * @brief Manages data access operations using TieredBackend and TransferEngine
 *
 * Provides unified interface for local and remote data operations, handling
 * tiered storage access and zero-copy transfers.
 */
class DataManager {
   public:
    /**
     * @brief Constructor
     * @param tiered_backend Unique pointer to TieredBackend instance (takes
     * ownership)
     * @param transfer_engine Shared pointer to TransferEngine instance (shared
     * with Client)
     */
    DataManager(std::unique_ptr<TieredBackend> tiered_backend,
                std::shared_ptr<TransferEngine> transfer_engine);

    /**
     * @brief Put data locally into tiered storage
     * @param key Object key
     * @param data Source data buffer (takes ownership, zero-copy)
     * @param size Data size in bytes
     * @param tier_id Optional tier ID (nullopt = use default tier selection)
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> Put(
        const std::string& key, std::unique_ptr<char[]> data, size_t size,
        std::optional<UUID> tier_id = std::nullopt);

    /**
     * @brief Get data handle from tiered storage (local access)
     * @param key Object key
     * @param tier_id Optional tier ID (nullopt = use highest priority tier)
     * @return AllocationHandle or error
     * @note Caller must keep the handle alive to access the data.
     *       Access data via handle->loc.data
     */
    tl::expected<AllocationHandle, ErrorCode> Get(
        const std::string& key, std::optional<UUID> tier_id = std::nullopt);

    /**
     * @brief Delete data from tiered storage
     * @param key Object key
     * @param tier_id Optional tier ID (nullopt = delete all replicas)
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> Delete(
        const std::string& key, std::optional<UUID> tier_id = std::nullopt);

    /**
     * @brief Read data and transfer to remote destination buffers
     *
     * This is the core method for remote data access:
     * 1. Get data handle from TieredBackend
     * 2. Use TransferEngine to transfer data via RDMA to destination buffers
     *
     * @param key Object key to read
     * @param dest_buffers Destination buffers on remote client (Client A)
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> ReadRemoteData(
        const std::string& key,
        const std::vector<RemoteBufferDesc>& dest_buffers);

    /**
     * @brief Write data from remote source buffers
     * @param key Object key to write
     * @param src_buffers Source buffers on remote client (Client A)
     * @param tier_id Optional tier ID (nullopt = use default tier selection)
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> WriteRemoteData(
        const std::string& key,
        const std::vector<RemoteBufferDesc>& src_buffers,
        std::optional<UUID> tier_id = std::nullopt);

    size_t GetLockShardCount() const { return lock_shard_count_; }

   private:
    std::unique_ptr<TieredBackend> tiered_backend_;    // Owned by DataManager
    std::shared_ptr<TransferEngine> transfer_engine_;  // Shared with Client

    // Sharded locks for concurrent access
    // Configurable via MOONCAKE_DM_LOCK_SHARD_COUNT environment variable
    // (default: 1024)
    size_t lock_shard_count_;
    std::vector<std::shared_mutex> lock_shards_;

    std::shared_mutex& GetKeyLock(const std::string& key) {
        size_t hash = std::hash<std::string>{}(key);
        return lock_shards_[hash % lock_shard_count_];
    }

    /**
     * @brief Transfer data from local source to remote destination buffers
     * @param handle Local allocation handle (source)
     * @param dest_buffers Remote destination buffers
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> TransferDataToRemote(
        AllocationHandle handle,
        const std::vector<RemoteBufferDesc>& dest_buffers);

    /**
     * @brief Transfer data from remote source buffers to local allocated space
     * @param handle Local allocation handle (destination)
     * @param src_buffers Remote source buffers
     * @return ErrorCode indicating success or failure
     */
    tl::expected<void, ErrorCode> TransferDataFromRemote(
        AllocationHandle handle,
        const std::vector<RemoteBufferDesc>& src_buffers);

private:
    std::unique_ptr<TieredBackend> tiered_backend_;  // Owned by DataManager
    std::shared_ptr<TransferEngine> transfer_engine_;  // Shared with Client
    
    // Sharded locks for concurrent access
    static constexpr size_t kLockShardCount = 1024;
    std::array<std::shared_mutex, kLockShardCount> lock_shards_;

    std::shared_mutex& GetKeyLock(const std::string& key) {
        size_t hash = std::hash<std::string>{}(key);
        return lock_shards_[hash % kLockShardCount];
    }
};

}  // namespace mooncake
