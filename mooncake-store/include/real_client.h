#pragma once

#include <atomic>
#include <csignal>
#include <async_simple/Future.h>
#include <boost/lockfree/queue.hpp>
#include <boost/functional/hash.hpp>
#include <memory>
#include <future>
#include <string>
#include <thread>
#include <vector>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "pyclient.h"
#if defined(MOONCAKE_STORE_CLIENT_P2P)
#include "p2p/client/p2p_client_service.h"
#else
#include "client_service.h"
#endif
#include "client_buffer.hpp"
#include "mutex.h"
#include "utils.h"

namespace mooncake {

struct CentralizedClientConfig;
struct P2PClientConfig;

#if defined(MOONCAKE_STORE_CLIENT_P2P)
using ClientServiceImpl = P2PClientService;
using ClientConfig = P2PClientConfig;
#else
using ClientServiceImpl = Client;
using ClientConfig = CentralizedClientConfig;
#endif

class RealClient;

// Global resource tracker to handle cleanup on abnormal termination
class ResourceTracker {
   public:
    // Get the singleton instance
    static ResourceTracker& getInstance();

    // Register a DistributedObjectStore instance for cleanup
    void registerInstance(const std::shared_ptr<PyClient>& instance);

   private:
    ResourceTracker();
    ~ResourceTracker();

    // Prevent copying
    ResourceTracker(const ResourceTracker&) = delete;
    ResourceTracker& operator=(const ResourceTracker&) = delete;

    // Cleanup all registered resources
    void cleanupAllResources();

    // Signal handler function
    static void signalHandler(int signal);

    // Exit handler function
    static void exitHandler();

    Mutex mutex_;
    std::vector<std::weak_ptr<PyClient>> instances_ GUARDED_BY(mutex_);

    // Ensure cleanup runs at most once
    std::atomic<bool> cleaned_{false};

    // Dedicated signal handling thread
    void startSignalThread();
    std::once_flag signal_once_{};
    std::jthread signal_thread_{};  // joins on destruction
};

class RealClient : public PyClient {
   public:
    RealClient();
    ~RealClient();

    // Factory to create shared instances and auto-register to ResourceTracker
    static std::shared_ptr<RealClient> create();

    int setup(const ClientConfig& config);

    int wait_dummy_rpc_server();

    int initAll(const std::string& protocol, const std::string& device_name,
                size_t mount_segment_size = 1024 * 1024 * 16) override;

    uint64_t alloc_from_mem_pool(size_t size) override { return 0; };

    // Local readiness check used by the Python wrapper.
    bool is_initialized() const override {
        return state_.load(std::memory_order_acquire) == State::READY;
    }
    // Compile-time architecture, independent of setup or shutdown.
    DeploymentMode deployment_mode() const override;
    std::shared_ptr<ClientBufferAllocator> GetBufferAllocator() const;

    int put(const std::string& key, std::span<const char> value,
            const std::optional<WriteConfig>& config = std::nullopt) override;

    int register_buffer(void* buffer, size_t size) override;

    int unregister_buffer(void* buffer) override;

    /**
     * @brief Get object data directly into a pre-allocated buffer
     * @param key Key of the object to get
     * @param buffer Pointer to the pre-allocated buffer (must be registered
     * with register_buffer)
     * @param size Size of the buffer
     * @return Number of bytes read on success, negative value on error
     * @note The buffer address must be previously registered with
     * register_buffer() for zero-copy operations
     */
    int64_t get_into(
        const std::string& key, void* buffer, size_t size,
        const std::optional<ReadConfig>& config = std::nullopt) override;

    /**
     * @brief Get object data directly into pre-allocated buffers for multiple
     * keys (batch version)
     * @param keys Vector of keys of the objects to get
     * @param buffers Vector of pointers to the pre-allocated buffers
     * @param sizes Vector of sizes of the buffers
     * @return Vector of 64-bit integers, where each element is the number of
     * bytes read on success, or a negative value on error
     * @note The buffer addresses must be previously registered with
     * register_buffer() for zero-copy operations
     */
    std::vector<int64_t> batch_get_into(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const std::optional<ReadConfig>& config = std::nullopt) override;

    /**
     * @brief Get object data directly into pre-allocated buffers for multiple
     * keys
     * @param keys Vector of keys of the objects to get
     * @param all_buffers Vector of vectors of pointers to the pre-allocated
     * buffers
     * @param all_sizes Vector of vectors of sizes of the buffers
     * @return Vector of integers, where each element is the number of bytes
     * read on success, or a negative value on error
     * @note The buffer addresses must be previously registered with
     * register_buffer() for zero-copy operations
     */
    std::vector<int> batch_get_into_multi_buffers(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        bool aggregate_same_segment_task,
        const std::optional<ReadConfig>& config = std::nullopt) override;

    /**
     * @brief Put object data directly from a pre-allocated buffer
     * @param key Key of the object to put
     * @param buffer Pointer to the buffer containing data (must be registered
     * with register_buffer)
     * @param size Size of the data to put
     * @return 0 on success, negative value on error
     * @note The buffer address must be previously registered with
     * register_buffer() for zero-copy operations
     */
    int put_from(
        const std::string& key, void* buffer, size_t size,
        const std::optional<WriteConfig>& config = std::nullopt) override;

    /**
     * @brief Put object data directly from pre-allocated buffers for multiple
     * keys(metadata version, better not be directly used in Python)
     * @param keys Vector of keys of the objects to put
     * @param buffers Vector of pointers to the pre-allocated buffers
     * @param metadata_buffers Vector of pointers to the pre-allocated metadata
     * buffers
     * @param size Number of sizes of the buffers
     * @param metadata_size Number of sizes of the metadata buffers
     * @param config Replication configuration
     * @return Vector of integers, where each element is 0 on success, or a
     * negative value on error
     * @note The buffer addresses must be previously registered with
     * register_buffer() for zero-copy operations
     */
    int put_from_with_metadata(
        const std::string& key, void* buffer, void* metadata_buffer,
        size_t size, size_t metadata_size,
        const std::optional<WriteConfig>& config = std::nullopt) override;

    /**
     * @brief Put object data directly from pre-allocated buffers for multiple
     * keys (batch version)
     * @param keys Vector of keys of the objects to put
     * @param buffers Vector of pointers to the pre-allocated buffers
     * @param sizes Vector of sizes of the buffers
     * @param config Replication configuration
     * @return Vector of integers, where each element is 0 on success, or a
     * negative value on error
     * @note The buffer addresses must be previously registered with
     * register_buffer() for zero-copy operations
     */

    std::vector<int> batch_put_from(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const std::optional<WriteConfig>& config = std::nullopt,
        WriteOperation operation = WriteOperation::Put) override;

    /**
     * @brief Put object data directly from multiple pre-allocated buffers for
     * multiple keys (batch version)
     * @param keys Vector of keys of the objects to put
     * @param all_buffers Vector of vectors of pointers to the multiple
     * pre-allocated buffers
     * @param all_sizes Vector of vectors of sizes of the multiple buffers
     * @param config Replication configuration
     * @return Vector of integers, where each element is 0 on success, or a
     * negative value on error
     * @note The buffer addresses must be previously registered with
     * register_buffer() for zero-copy operations
     */
    std::vector<int> batch_put_from_multi_buffers(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const std::optional<WriteConfig>& config = std::nullopt) override;

    int put_parts(const std::string& key,
                  std::vector<std::span<const char>> values,
                  const std::optional<WriteConfig>& config = std::nullopt,
                  WriteOperation operation = WriteOperation::Put) override;

    int put_batch(
        const std::vector<std::string>& keys,
        const std::vector<std::span<const char>>& values,
        const std::optional<WriteConfig>& config = std::nullopt) override;

    [[nodiscard]] std::string get_hostname() const override;

    /**
     * @brief Get a buffer containing the data for a key
     * @param key Key to get data for
     * @return std::shared_ptr<BufferHandle> Buffer containing the data, or
     * nullptr if error
     */
    std::shared_ptr<BufferHandle> get_buffer(
        const std::string& key,
        const std::optional<ReadConfig>& config = std::nullopt) override;

    /**
     * @brief Get buffer information (address and size) for a key
     * @param key Key to get buffer information for
     * @return Tuple containing buffer address and size, or (0, 0) if error
     */
    std::tuple<uint64_t, size_t> get_buffer_info(
        const std::string& key,
        const std::optional<ReadConfig>& config = std::nullopt) override;

    /**
     * @brief Get buffers containing the data for multiple keys (batch version)
     * @param keys Vector of keys to get data for
     * @return Vector of std::shared_ptr<BufferHandle> buffers containing the
     * data, or nullptr for each key if error
     */
    std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer(
        const std::vector<std::string>& keys,
        const std::optional<ReadConfig>& config = std::nullopt) override;

    int remove(const std::string& key, bool force = false) override;

    long removeByRegex(const std::string& str, bool force = false) override;

    long removeAll(bool force = false) override;

    long removeAllLocal() override;

    int removeLocal(const std::string& key) override;

    int tearDownAll() override;

    /**
     * @brief Check if an object exists
     * @param key Key to check
     * @return 1 if exists, 0 if not exists, -1 if error
     */
    int isExist(const std::string& key) override;

    /**
     * @brief Check if multiple objects exist
     * @param keys Vector of keys to check
     * @return Vector of existence results: 1 if exists, 0 if not exists, -1 if
     * error
     */
    std::vector<int> batchIsExist(const std::vector<std::string>& keys);

    /**
     * @brief Get the size of an object
     * @param key Key of the object
     * @return Size of the object in bytes, or -1 if error or object doesn't
     * exist
     */
    int64_t getSize(const std::string& key) override;

#if !defined(MOONCAKE_STORE_CLIENT_P2P)
    /**
     * @brief Create a copy task to replicate an object's data to target
     * segments
     * @param key Object key
     * @param targets Target segments
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> create_copy_task(
        const std::string& key,
        const std::vector<std::string>& targets) override;

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @param key Object key
     * @param source Source segment
     * @param target Target segment
     * @return tl::expected<UUID, ErrorCode> Task ID on success, ErrorCode on
     * failure
     */
    tl::expected<UUID, ErrorCode> create_move_task(
        const std::string& key, const std::string& source,
        const std::string& target) override;

    /**
     * @brief Query a task by task id
     * @param task_id Task ID to query
     * @return tl::expected<QueryTaskResponse, ErrorCode> Task basic info
     * on success, ErrorCode on failure
     */
    tl::expected<QueryTaskResponse, ErrorCode> query_task(
        const UUID& task_id) override;
#endif

    // Dummy client helper functions that return tl::expected
    tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>
    get_buffer_info_dummy_helper(const std::string& key,
                                 const std::optional<ReadConfig>& config,
                                 const UUID& client_id);

    tl::expected<void, ErrorCode> put_dummy_helper(
        const std::string& key, std::span<const char> value,
        const std::optional<WriteConfig>& config, const UUID& client_id);

    tl::expected<void, ErrorCode> put_batch_dummy_helper(
        const std::vector<std::string>& keys,
        const std::vector<std::span<const char>>& values,
        const std::optional<WriteConfig>& config, const UUID& client_id);

    tl::expected<void, ErrorCode> put_parts_dummy_helper(
        const std::string& key, std::vector<std::span<const char>> values,
        const std::optional<WriteConfig>& config, const UUID& client_id,
        WriteOperation operation = WriteOperation::Put);

    std::vector<tl::expected<int64_t, ErrorCode>> batch_get_into_dummy_helper(
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& buffers, const std::vector<size_t>& sizes,
        const std::optional<ReadConfig>& config, const UUID& client_id);

    std::vector<tl::expected<void, ErrorCode>> batch_put_from_dummy_helper(
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& dummy_buffers,
        const std::vector<size_t>& sizes,
        const std::optional<WriteConfig>& config, const UUID& client_id,
        WriteOperation operation = WriteOperation::Put);

    tl::expected<void, ErrorCode> unmap_shm_internal(const UUID& client_id);

    tl::expected<void, ErrorCode> unregister_shm_buffer_internal(
        uint64_t dummy_base_addr, const UUID& client_id);

    // Dummy connection handshake: readiness and deployment architecture.
    tl::expected<DeploymentMode, ErrorCode> service_ready_internal();

    // Standalone deployment enables both Dummy RPC and heartbeat monitoring.
    tl::expected<void, ErrorCode> setup_internal(
        const ClientConfig& config, bool enable_dummy_service = false,
        size_t rpc_thread_count = 1, uint16_t rpc_port = 50052);

    tl::expected<void, ErrorCode> remove_internal(const std::string& key,
                                                  bool force = false);

    tl::expected<long, ErrorCode> removeByRegex_internal(const std::string& str,
                                                         bool force = false);

    tl::expected<int64_t, ErrorCode> removeAll_internal(bool force = false);

    tl::expected<int64_t, ErrorCode> removeAllLocal_internal();

    tl::expected<void, ErrorCode> removeLocal_internal(const std::string& key);

    tl::expected<bool, ErrorCode> isExist_internal(const std::string& key);

    std::vector<tl::expected<bool, ErrorCode>> batchIsExist_internal(
        const std::vector<std::string>& keys);

    tl::expected<int64_t, ErrorCode> getSize_internal(const std::string& key);

    std::map<std::string, std::vector<ObjectDescriptor>> batch_get_replica_desc(
        const std::vector<std::string>& keys);
    std::vector<ObjectDescriptor> get_replica_desc(const std::string& key);

    tl::expected<DummyHeartbeatResponse, ErrorCode> ping(const UUID& client_id);

   private:
    friend struct RealClientTestHelper;

    struct MappedShm {
        std::string shm_name_;
        // Offset = real_base - dummy_base
        uintptr_t shm_addr_offset_ = 0;
        void* shm_buffer_ = nullptr;
        size_t shm_size_ = 0;
        uintptr_t dummy_base_addr_ = 0;
    };

    struct ShmContext {
        mutable SharedMutex mutex_;
        std::vector<MappedShm> mapped_shms_ GUARDED_BY(mutex_);
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator_
            GUARDED_BY(mutex_) = nullptr;
    };

    // boost lockfree queue requires trivial assignment operator
    struct PodUUID {
        uint64_t first_;
        uint64_t second_;
    };

    // Internal implementations returning native results.
    // Share mem management for dummy client
    // Modified: map_shm_internal now takes fd instead of just name
    tl::expected<void, ErrorCode> map_shm_internal(int fd,
                                                   uint64_t shm_base_addr,
                                                   size_t shm_size,
                                                   bool is_local_buffer,
                                                   const UUID& client_id);

    tl::expected<void, ErrorCode> initAll_internal(
        const std::string& protocol, const std::string& device_name,
        size_t mount_segment_size = 1024 * 1024 * 16);

    tl::expected<void, ErrorCode> unregister_buffer_internal(void* buffer);

    tl::expected<void, ErrorCode> put_internal(
        const std::string& key, std::span<const char> value,
        const std::optional<WriteConfig>& config,
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> register_buffer_internal(void* buffer,
                                                           size_t size);

    tl::expected<int64_t, ErrorCode> get_into_internal(
        const std::string& key, void* buffer, size_t size,
        const std::optional<ReadConfig>& config = std::nullopt);

    std::vector<tl::expected<int64_t, ErrorCode>> batch_get_into_internal(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const std::optional<ReadConfig>& config = std::nullopt);

    std::vector<tl::expected<int64_t, ErrorCode>>
    batch_get_into_multi_buffers_internal(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        bool aggregate_same_segment_task,
        const std::optional<ReadConfig>& config = std::nullopt);

    tl::expected<void, ErrorCode> put_from_internal(
        const std::string& key, void* buffer, size_t size,
        const std::optional<WriteConfig>& config = std::nullopt);

    std::vector<tl::expected<void, ErrorCode>> batch_put_from_internal(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const std::optional<WriteConfig>& config = std::nullopt,
        WriteOperation operation = WriteOperation::Put);

    std::vector<tl::expected<void, ErrorCode>>
    batch_put_from_multi_buffers_internal(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const std::optional<WriteConfig>& config = std::nullopt);

    tl::expected<void, ErrorCode> put_parts_internal(
        const std::string& key, std::vector<std::span<const char>> values,
        const std::optional<WriteConfig>& config,
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr,
        WriteOperation operation = WriteOperation::Put);

    tl::expected<void, ErrorCode> put_batch_internal(
        const std::vector<std::string>& keys,
        const std::vector<std::span<const char>>& values,
        const std::optional<WriteConfig>& config,
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> tearDownAll_internal();

    std::shared_ptr<BufferHandle> get_buffer_internal(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr,
        const std::optional<ReadConfig>& config = std::nullopt);

    std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer_internal(
        const std::vector<std::string>& keys,
        const std::optional<ReadConfig>& config = std::nullopt);

    std::shared_ptr<ShmContext> find_shm_context(const UUID& client_id);

    void dummy_client_monitor_func();
    ErrorCode start_dummy_client_monitor();
    ErrorCode stop_dummy_client_monitor();

    ErrorCode start_ipc_server();
    ErrorCode stop_ipc_server();
    void ipc_server_func(int server_sock);

    ErrorCode start_dummy_rpc_server(size_t thread_count, uint16_t port);

    static tl::expected<void, ErrorCode> ValidateWriteOperation(
        WriteOperation operation, const std::optional<WriteConfig>& config);

    // Preserve the six legacy write-entry restrictions without constraining
    // native Put/BatchPut or the multi-buffer write entry.
    static tl::expected<void, ErrorCode> ValidateWriteEntryConfig(
        const std::optional<WriteConfig>& config);

    static tl::expected<std::shared_ptr<ClientServiceImpl>, ErrorCode>
    CreateService(const ClientConfig& config);

    void register_dummy_rpc_handlers();

   private:
    std::string protocol_;

    mutable SharedMutex dummy_client_mutex_;
    std::unordered_map<UUID, std::shared_ptr<ShmContext>, boost::hash<UUID>>
        shm_contexts_ GUARDED_BY(dummy_client_mutex_);

    // Dummy Client manage related members
    std::thread dummy_client_monitor_thread_;
    std::atomic<bool> dummy_client_monitor_running_{false};
    static constexpr uint64_t kDummyClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks
    static constexpr size_t kDummyClientPingQueueSize =
        128 * 1024;  // Size of the client ping queue
    boost::lockfree::queue<PodUUID> dummy_client_ping_queue_{
        kDummyClientPingQueueSize};
    const int64_t dummy_client_live_ttl_sec_ =
        DEFAULT_DUMMY_CLIENT_LIVE_TTL_SEC;

    // IPC Server members for receiving FD from Dummy Clients
    std::string ipc_socket_path_;
    std::jthread ipc_thread_;
    std::atomic<bool> ipc_running_{false};

    // The alias names the native class itself, not a forwarding wrapper.
    std::shared_ptr<ClientServiceImpl> client_service_;

    // Serializes setup/teardown, including resource initialization and cleanup.
    // Only setup/teardown holds this mutex.
    std::mutex lifecycle_mutex_;
    enum class State { UNINITIALIZED, READY, CLOSING, CLOSED };
    // Publishes lifecycle state for status queries and deployment management
    // without taking the lifecycle mutex.
    std::atomic<State> state_{State::UNINITIALIZED};
    std::unique_ptr<coro_rpc::coro_rpc_server> dummy_rpc_server_;
    async_simple::Future<coro_rpc::err_code> dummy_rpc_exit_{nullptr};
};

}  // namespace mooncake
