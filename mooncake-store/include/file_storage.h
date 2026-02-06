#pragma once

#include "client_service.h"
#include "client_buffer.hpp"
#include "storage_backend.h"

namespace mooncake {

class FileStorage {
   public:
    FileStorage(const FileStorageConfig& config, std::shared_ptr<Client> client,
                const std::string& local_rpc_addr);
    ~FileStorage();

    tl::expected<void, ErrorCode> Init();

    /**
     * @brief Result of BatchGet operation containing batch_id and buffer
     * pointers.
     */
    struct BatchGetResult {
        uint64_t batch_id;
        std::vector<uint64_t> pointers;
    };

    /**
     * @brief Reads multiple key-value (KV) entries from local storage and
     * forwards them to a remote node.
     * @param keys                 List of keys to read from the local KV store
     * @param sizes                Expected size in bytes for each value
     * @return tl::expected<BatchGetResult, ErrorCode> containing batch_id and
     * buffer pointers.
     */
    tl::expected<BatchGetResult, ErrorCode> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);

    FileStorageConfig config_;

    /**
     * @brief Releases buffer associated with a specific batch_id.
     * Called by remote client after transfer completion.
     * @param batch_id The unique identifier of the batch to release
     * @return true if batch was found and released, false otherwise
     */
    bool ReleaseBuffer(uint64_t batch_id);

   private:
    friend class FileStorageTest;
    struct AllocatedBatch {
        uint64_t batch_id;
        std::vector<BufferHandle> handles;
        std::unordered_map<std::string, Slice> slices;
        std::chrono::steady_clock::time_point lease_timeout;
        std::vector<uint64_t> pointers;
        uint64_t total_size;

        AllocatedBatch() : batch_id(0), total_size(0) {}
        AllocatedBatch(AllocatedBatch&&) = default;
        AllocatedBatch& operator=(AllocatedBatch&&) = default;

        AllocatedBatch(const AllocatedBatch&) = delete;
        AllocatedBatch& operator=(const AllocatedBatch&) = delete;

        ~AllocatedBatch() = default;
    };

    /**
     * @brief Offload object data and metadata.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> OffloadObjects(
        const std::unordered_map<std::string, int64_t>& offloading_objects);

    /**
     * @brief Performs a heartbeat operation for the FileStorage component.
     * 1. Sends object status (e.g., access frequency, size) to the master via
     * client.
     * 2. Receives feedback on which objects should be offloaded.
     * 3. Triggers asynchronous offloading of pending objects.
     * @return tl::expected<void, ErrorCode> indicating operation status.
     */
    tl::expected<void, ErrorCode> Heartbeat();

    tl::expected<bool, ErrorCode> IsEnableOffloading();

    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>& batch_object);

    tl::expected<void, ErrorCode> BatchQuerySegmentSlices(
        const std::vector<std::string>& keys,
        std::unordered_map<std::string, std::vector<Slice>>& batched_slices);

    tl::expected<void, ErrorCode> RegisterLocalMemory();

    tl::expected<std::shared_ptr<AllocatedBatch>, ErrorCode> AllocateBatch(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);

    void ClientBufferGCThreadFunc();

    std::shared_ptr<Client> client_;
    std::string local_rpc_addr_;
    std::shared_ptr<StorageBackendInterface> storage_backend_;
    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator_;
    mutable Mutex client_buffer_mutex_;
    std::unordered_map<uint64_t, std::shared_ptr<AllocatedBatch>> GUARDED_BY(
        client_buffer_mutex_) client_buffer_allocated_batches_;
    std::atomic<uint64_t> next_batch_id_{1};

    mutable Mutex offloading_mutex_;
    bool GUARDED_BY(offloading_mutex_) enable_offloading_;
    std::atomic<bool> heartbeat_running_;
    std::thread heartbeat_thread_;
    std::atomic<bool> client_buffer_gc_running_;
    std::thread client_buffer_gc_thread_;
};

}  // namespace mooncake