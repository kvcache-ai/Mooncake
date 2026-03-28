#pragma once

#include "client_config_builder.h"

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <condition_variable>
#include <mutex>

#include "pyclient.h"
#include "client_buffer.hpp"
#include "mutex.h"
#include "utils.h"
#include "rpc_types.h"

namespace mooncake {

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

    template <typename ConfigT>
    int setup(ConfigT& config);

    int initAll(const std::string& protocol, const std::string& device_name,
                size_t mount_segment_size = 1024 * 1024 * 16) override;

    uint64_t alloc_from_mem_pool(size_t size) override { return 0; };

    DeploymentMode deployment_mode() const override {
        return client_service_ ? client_service_->deployment_mode()
                               : DeploymentMode::UNKNOWN;
    }

    int put(const std::string& key, std::span<const char> value,
            const WriteConfig& config) override;

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
    int64_t get_into(const std::string& key, void* buffer, size_t size,
                     const ReadRouteConfig& config = {}) override;

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
        const ReadRouteConfig& config = {}) override;

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
        const ReadRouteConfig& config = {}) override;

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
    int put_from(const std::string& key, void* buffer, size_t size,
                 const WriteConfig& config) override;

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
    int put_from_with_metadata(const std::string& key, void* buffer,
                               void* metadata_buffer, size_t size,
                               size_t metadata_size,
                               const WriteConfig& config) override;

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

    std::vector<int> batch_put_from(const std::vector<std::string>& keys,
                                    const std::vector<void*>& buffers,
                                    const std::vector<size_t>& sizes,
                                    const WriteConfig& config) override;

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
        const WriteConfig& config) override;

    int put_parts(const std::string& key,
                  std::vector<std::span<const char>> values,
                  const WriteConfig& config) override;

    int put_batch(const std::vector<std::string>& keys,
                  const std::vector<std::span<const char>>& values,
                  const WriteConfig& config) override;

    [[nodiscard]] std::string get_hostname() const override;

    /**
     * @brief Get a buffer containing the data for a key
     * @param key Key to get data for
     * @return std::shared_ptr<BufferHandle> Buffer containing the data, or
     * nullptr if error
     */
    std::shared_ptr<BufferHandle> get_buffer(
        const std::string& key, const ReadRouteConfig& config = {}) override;

    /**
     * @brief Get buffer information (address and size) for a key
     * @param key Key to get buffer information for
     * @return Tuple containing buffer address and size, or (0, 0) if error
     */
    std::tuple<uint64_t, size_t> get_buffer_info(
        const std::string& key, const ReadRouteConfig& config = {}) override;

    /**
     * @brief Get buffers containing the data for multiple keys (batch version)
     * @param keys Vector of keys to get data for
     * @return Vector of std::shared_ptr<BufferHandle> buffers containing the
     * data, or nullptr for each key if error
     */
    std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer(
        const std::vector<std::string>& keys,
        const ReadRouteConfig& config = {}) override;

    int remove(const std::string& key) override;

    long removeByRegex(const std::string& str) override;

    long removeAll() override;

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

    // Dummy client helper functions that return tl::expected
    tl::expected<std::tuple<uint64_t, size_t>, ErrorCode>
    get_buffer_info_dummy_helper(const std::string& key,
                                 const ReadRouteConfig& config,
                                 const UUID& client_id);

    tl::expected<void, ErrorCode> put_dummy_helper(const std::string& key,
                                                   std::span<const char> value,
                                                   const WriteConfig& config,
                                                   const UUID& client_id);

    tl::expected<void, ErrorCode> put_batch_dummy_helper(
        const std::vector<std::string>& keys,
        const std::vector<std::span<const char>>& values,
        const WriteConfig& config, const UUID& client_id);

    tl::expected<void, ErrorCode> put_parts_dummy_helper(
        const std::string& key, std::vector<std::span<const char>> values,
        const WriteConfig& config, const UUID& client_id);

    std::vector<tl::expected<int64_t, ErrorCode>> batch_get_into_dummy_helper(
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& buffers, const std::vector<size_t>& sizes,
        const ReadRouteConfig& config, const UUID& client_id);

    std::vector<tl::expected<void, ErrorCode>> batch_put_from_dummy_helper(
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& dummy_buffers,
        const std::vector<size_t>& sizes, const WriteConfig& config,
        const UUID& client_id);

    // Share mem management for dummy client
    // Modified: map_shm_internal now takes fd instead of just name
    tl::expected<void, ErrorCode> map_shm_internal(int fd,
                                                   uint64_t shm_base_addr,
                                                   size_t shm_size,
                                                   bool is_local_buffer,
                                                   const UUID& client_id);

    tl::expected<void, ErrorCode> unmap_shm_internal(const UUID& client_id);

    tl::expected<void, ErrorCode> unregister_shm_buffer_internal(
        uint64_t dummy_base_addr, const UUID& client_id);

    // Internal versions that return tl::expected
    tl::expected<DeploymentMode, ErrorCode> service_ready_internal() {
        return deployment_mode();
    }

    template <typename ConfigT>
    tl::expected<void, ErrorCode> setup_internal(ConfigT& config);

    tl::expected<void, ErrorCode> initAll_internal(
        const std::string& protocol, const std::string& device_name,
        size_t mount_segment_size = 1024 * 1024 * 16);

    tl::expected<void, ErrorCode> unregister_buffer_internal(void* buffer);

    tl::expected<void, ErrorCode> put_internal(
        const std::string& key, std::span<const char> value,
        const WriteConfig& config,
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> register_buffer_internal(void* buffer,
                                                           size_t size);

    tl::expected<int64_t, ErrorCode> get_into_internal(
        const std::string& key, void* buffer, size_t size,
        const ReadRouteConfig& config = {});

    std::vector<tl::expected<int64_t, ErrorCode>> batch_get_into_internal(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes, const ReadRouteConfig& config = {});

    std::vector<tl::expected<int64_t, ErrorCode>>
    batch_get_into_multi_buffers_internal(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        bool aggregate_same_segment_task, const ReadRouteConfig& config = {});

    tl::expected<void, ErrorCode> put_from_internal(const std::string& key,
                                                    void* buffer, size_t size,
                                                    const WriteConfig& config);

    std::vector<tl::expected<void, ErrorCode>> batch_put_from_internal(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes, const WriteConfig& config);

    std::vector<tl::expected<void, ErrorCode>>
    batch_put_from_multi_buffers_internal(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const WriteConfig& config);

    tl::expected<void, ErrorCode> put_parts_internal(
        const std::string& key, std::vector<std::span<const char>> values,
        const WriteConfig& config,
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> put_batch_internal(
        const std::vector<std::string>& keys,
        const std::vector<std::span<const char>>& values,
        const WriteConfig& config,
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr);

    tl::expected<void, ErrorCode> remove_internal(const std::string& key);

    tl::expected<long, ErrorCode> removeByRegex_internal(
        const std::string& str);

    tl::expected<int64_t, ErrorCode> removeAll_internal();

    tl::expected<void, ErrorCode> tearDownAll_internal();

    tl::expected<bool, ErrorCode> isExist_internal(const std::string& key);

    std::vector<tl::expected<bool, ErrorCode>> batchIsExist_internal(
        const std::vector<std::string>& keys);

    tl::expected<int64_t, ErrorCode> getSize_internal(const std::string& key);

    std::shared_ptr<BufferHandle> get_buffer_internal(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr,
        const ReadRouteConfig& config = {});

    std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer_internal(
        const std::vector<std::string>& keys,
        const ReadRouteConfig& config = {});

    std::map<std::string, std::vector<Replica::Descriptor>>
    batch_get_replica_desc(const std::vector<std::string>& keys);
    std::vector<Replica::Descriptor> get_replica_desc(const std::string& key);

    tl::expected<HeartbeatResponse, ErrorCode> ping(const UUID& client_id);

    std::unique_ptr<AutoPortBinder> port_binder_ = nullptr;

    std::string protocol;
    std::string device_name;
    std::string local_ip;
    uint16_t te_port = 0;

    struct MappedShm {
        std::string shm_name;
        // Offset = real_base - dummy_base
        uintptr_t shm_addr_offset = 0;
        void* shm_buffer = nullptr;
        size_t shm_size = 0;
        uintptr_t dummy_base_addr = 0;
    };

    struct ShmContext {
        // List of all mapped shared memory for this client
        std::vector<MappedShm> mapped_shms;
        std::shared_ptr<ClientBufferAllocator> client_buffer_allocator =
            nullptr;
    };
    mutable std::shared_mutex dummy_client_mutex_;
    std::unordered_map<UUID, ShmContext, boost::hash<UUID>> shm_contexts_;

    // Ensure cleanup executes at most once across multiple entry points
    std::atomic<bool> closed_{false};

    // Dummy Client manage related members
    void dummy_client_monitor_func();
    int start_dummy_client_monitor();
    int stop_dummy_client_monitor();
    std::thread dummy_client_monitor_thread_;
    std::atomic<bool> dummy_client_monitor_running_{false};
    std::condition_variable dummy_client_monitor_cv_;
    std::mutex dummy_client_monitor_cv_mutex_;
    static constexpr uint64_t kDummyClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks
    // boost lockfree queue requires trivial assignment operator
    struct PodUUID {
        uint64_t first;
        uint64_t second;
    };
    static constexpr size_t kDummyClientPingQueueSize =
        128 * 1024;  // Size of the client ping queue
    boost::lockfree::queue<PodUUID> dummy_client_ping_queue_{
        kDummyClientPingQueueSize};
    const int64_t dummy_client_live_ttl_sec_ = DEFAULT_CLIENT_LIVE_TTL_SEC;
    int64_t view_version_ = 0;

    // IPC Server members for receiving FD from Dummy Clients
    std::string ipc_socket_path_;
    std::jthread ipc_thread_;
    std::atomic<bool> ipc_running_{false};
    int start_ipc_server();
    int stop_ipc_server();
    void ipc_server_func();
};

}  // namespace mooncake
