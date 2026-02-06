#pragma once

#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>
#include <ylt/util/tl/expected.hpp>
#include "client_rpc_types.h"
#include "data_manager.h"
#include "types.h"

namespace mooncake {

/**
 * @class DataManagerSubmitter
 * @brief Manages thread pool for DataManager operations
 *
 * This class provides:
 * - Thread pool for executing DataManager operations (default 64 threads)
 * - Task queue for queuing operations
 * - Unified interface for both local and remote operations
 * - Delete operation is directly forwarded to DataManager (not through thread pool)
 */
class DataManagerSubmitter {
   public:
    /**
     * @brief Constructor
     * @param data_manager Reference to DataManager instance (must outlive this
     * object)
     * @param thread_pool_size Number of worker threads (default: 64)
     */
    explicit DataManagerSubmitter(DataManager& data_manager,
                                  size_t thread_pool_size = 64);

    /**
     * @brief Destructor - stops thread pool and waits for completion
     */
    ~DataManagerSubmitter();

    // Local operations
    /**
     * @brief Submit Put operation
     * @param key Object key
     * @param data Source data buffer (takes ownership)
     * @param size Data size in bytes
     * @param tier_id Optional tier ID
     * @return Future containing result
     */
    std::future<tl::expected<void, ErrorCode>> SubmitPut(
        const std::string& key, std::unique_ptr<char[]> data, size_t size,
        std::optional<UUID> tier_id = std::nullopt);

    /**
     * @brief Submit Get operation
     * @param key Object key
     * @param tier_id Optional tier ID
     * @return Future containing result
     */
    std::future<tl::expected<AllocationHandle, ErrorCode>> SubmitGet(
        const std::string& key, std::optional<UUID> tier_id = std::nullopt);

    /**
     * @brief Submit Delete operation (directly calls DataManager, not through thread pool)
     * @param key Object key
     * @param tier_id Optional tier ID
     * @return Result (synchronous call)
     */
    tl::expected<void, ErrorCode> SubmitDelete(
        const std::string& key, std::optional<UUID> tier_id = std::nullopt);

    // Remote operations
    /**
     * @brief Submit ReadRemoteData operation
     * @param key Object key
     * @param dest_buffers Destination buffers
     * @return Future containing result
     */
    std::future<tl::expected<void, ErrorCode>> SubmitReadRemoteData(
        const std::string& key,
        const std::vector<RemoteBufferDesc>& dest_buffers);

    /**
     * @brief Submit WriteRemoteData operation
     * @param key Object key
     * @param src_buffers Source buffers
     * @param tier_id Optional tier ID
     * @return Future containing result
     */
    std::future<tl::expected<void, ErrorCode>> SubmitWriteRemoteData(
        const std::string& key,
        const std::vector<RemoteBufferDesc>& src_buffers,
        std::optional<UUID> tier_id = std::nullopt);

   private:
    // Task types
    enum class TaskType {
        PUT,
        GET,
        READ_REMOTE,
        WRITE_REMOTE
    };

    struct Task {
        TaskType type;
        std::string key;
        std::promise<tl::expected<void, ErrorCode>> void_promise;
        std::promise<tl::expected<AllocationHandle, ErrorCode>> handle_promise;
        
        // Parameters for different task types
        std::unique_ptr<char[]> put_data;
        size_t put_size;
        std::optional<UUID> tier_id;
        std::vector<RemoteBufferDesc> remote_buffers;
    };

    /**
     * @brief Worker thread main loop
     */
    void WorkerThreadMain();

    /**
     * @brief Process a task
     * @param task Task to process
     */
    void ProcessTask(std::unique_ptr<Task> task);

    DataManager& data_manager_;
    const size_t thread_pool_size_;

    // Thread pool
    std::vector<std::thread> worker_threads_;
    bool shutdown_;

    // Task queue
    std::queue<std::unique_ptr<Task>> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
};

}  // namespace mooncake
