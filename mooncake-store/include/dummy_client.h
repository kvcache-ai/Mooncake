#pragma once

#include "client_config_builder.h"

#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "pyclient.h"
#include <memory>

namespace mooncake {

class ShmHelper {
   public:
    struct ShmSegment {
        int fd = -1;
        void* base_addr = nullptr;
        size_t size = 0;
        std::string name;
        bool registered = false;
        bool is_local = false;
    };

    static ShmHelper* getInstance();

    void* allocate(size_t size);
    int free(void* addr);

    bool cleanup();

    // Get the shm that contains the given address
    // Returns a shared_ptr to ensure the segment remains valid
    std::shared_ptr<ShmSegment> get_shm(void* addr);

    const std::vector<std::shared_ptr<ShmSegment>>& get_shms() const {
        return shms_;
    }

    ShmHelper(const ShmHelper&) = delete;
    ShmHelper& operator=(const ShmHelper&) = delete;

   private:
    ShmHelper();
    ~ShmHelper();

    std::vector<std::shared_ptr<ShmSegment>> shms_;
    static std::mutex shm_mutex_;
};

class DummyClient : public PyClient {
   public:
    DummyClient();
    ~DummyClient();

    int64_t unregister_shm();

    int setup(DummyClientConfig& config);

    int initAll(const std::string& protocol, const std::string& device_name,
                size_t mount_segment_size) override {
        // Dummy client does not support real setup
        return -1;
    }

    uint64_t alloc_from_mem_pool(size_t size) override;

    // if a dummy client has connected to a real client,
    // return the mode of real client
    DeploymentMode deployment_mode() const override { return deployment_mode_; }

    int put(const std::string& key, std::span<const char> value,
            const WriteConfig& config) override;

    int register_buffer(void* buffer, size_t size) override;

    int unregister_buffer(void* buffer) override;

    int64_t get_into(const std::string& key, void* buffer, size_t size,
                     const ReadRouteConfig& config = {}) override;

    std::vector<int64_t> batch_get_into(
        const std::vector<std::string>& keys, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const ReadRouteConfig& config = {}) override;

    std::vector<int> batch_get_into_multi_buffers(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        bool aggregate_same_segment_task,
        const ReadRouteConfig& config = {}) override;

    int put_from(const std::string& key, void* buffer, size_t size,
                 const WriteConfig& config) override;

    int put_from_with_metadata(const std::string& key, void* buffer,
                               void* metadata_buffer, size_t size,
                               size_t metadata_size,
                               const WriteConfig& config) override;

    std::vector<int> batch_put_from(const std::vector<std::string>& keys,
                                    const std::vector<void*>& buffers,
                                    const std::vector<size_t>& sizes,
                                    const WriteConfig& config) override;

    std::vector<int> batch_put_from_multi_buffers(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const WriteConfig& config) override;

    std::shared_ptr<BufferHandle> get_buffer(
        const std::string& key, const ReadRouteConfig& config = {}) override;

    std::tuple<uint64_t, size_t> get_buffer_info(
        const std::string& key, const ReadRouteConfig& config = {}) override;

    std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer(
        const std::vector<std::string>& keys,
        const ReadRouteConfig& config = {}) override;

    int put_parts(const std::string& key,
                  std::vector<std::span<const char>> values,
                  const WriteConfig& config) override;

    int put_batch(const std::vector<std::string>& keys,
                  const std::vector<std::span<const char>>& values,
                  const WriteConfig& config) override;

    [[nodiscard]] std::string get_hostname() const override;

    int remove(const std::string& key) override;

    long removeByRegex(const std::string& str) override;

    long removeAll() override;

    int isExist(const std::string& key) override;

    std::vector<int> batchIsExist(
        const std::vector<std::string>& keys) override;

    int64_t getSize(const std::string& key) override;

    std::map<std::string, std::vector<Replica::Descriptor>>
    batch_get_replica_desc(const std::vector<std::string>& keys);
    std::vector<Replica::Descriptor> get_replica_desc(const std::string& key);

    int tearDownAll() override;

   private:
    ErrorCode connect(const std::string& server_address);

    int register_shm_via_ipc(const ShmHelper::ShmSegment* shm,
                             bool is_local = false);

    /**
     * @brief Generic RPC invocation helper for single-result operations
     * @tparam ServiceMethod Pointer to WrappedMasterService member function
     * @tparam ReturnType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param args Arguments to pass to the RPC call
     * @return The result of the RPC call
     */
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc(
        Args&&... args);

    /**
     * @brief Generic RPC invocation helper for batch operations
     * @tparam ServiceMethod Pointer to WrappedMasterService member function
     * @tparam ResultType The expected return type of the RPC call
     * @tparam Args Parameter types for the RPC call
     * @param input_size Size of input batch for error handling
     * @param args Arguments to pass to the RPC call
     * @return Vector of results from the batch RPC call
     */
    template <auto ServiceMethod, typename ResultType, typename... Args>
    [[nodiscard]] std::vector<tl::expected<ResultType, ErrorCode>>
    invoke_batch_rpc(size_t input_size, Args&&... args);

    /**
     * @brief Accessor for the coro_rpc_client pool. Since coro_rpc_client
     * pool cannot reconnect to a different address, a new coro_rpc_client
     * pool is created if the address is different from the current one.
     */
    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_pool_ = client_pool;
        }

        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        GetClientPool() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_pool_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };
    RpcClientAccessor client_accessor_;

    // The client identification.
    const UUID client_id_;

    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;

    // Mutex to insure the Connect function is atomic.
    mutable Mutex connect_mutex_;
    // The address which is passed to the coro_rpc_client
    std::string client_addr_param_ GUARDED_BY(connect_mutex_);

    DeploymentMode deployment_mode_ = DeploymentMode::UNKNOWN;

    // For shared memory management
    ShmHelper* shm_helper_ = nullptr;
    std::string ipc_socket_path_;

    // For high availability
    std::thread ping_thread_;
    std::atomic<bool> ping_running_{false};
    void ping_thread_main();
    volatile bool connected_ = false;
};

}  // namespace mooncake
