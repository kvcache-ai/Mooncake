#pragma once

#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "pyclient.h"
#include "real_client.h"
#include <memory>

namespace mooncake {

class ShmHelper {
   public:
    struct ShmSegment {
        int fd = -1;
        void *base_addr = nullptr;
        size_t size = 0;
        std::string name;
        bool registered = false;
        bool is_local = false;
    };

    static ShmHelper *getInstance();

    void *allocate(size_t size);
    int free(void *addr);

    bool cleanup();

    // Get the shm that contains the given address
    // Returns a shared_ptr to ensure the segment remains valid
    std::shared_ptr<ShmSegment> get_shm(void *addr);

    const std::vector<std::shared_ptr<ShmSegment>> &get_shms() const {
        return shms_;
    }

    bool is_hugepage() const { return use_hugepage_; }

    ShmHelper(const ShmHelper &) = delete;
    ShmHelper &operator=(const ShmHelper &) = delete;

   private:
    ShmHelper();
    ~ShmHelper();

    std::vector<std::shared_ptr<ShmSegment>> shms_;
    static std::mutex shm_mutex_;
    bool use_hugepage_ = false;
};

class DummyClient : public PyClient {
   public:
    DummyClient();
    ~DummyClient();

    int64_t unregister_shm();

    int setup_real(const std::string &local_hostname,
                   const std::string &metadata_server,
                   size_t global_segment_size, size_t local_buffer_size,
                   const std::string &protocol, const std::string &rdma_devices,
                   const std::string &master_server_addr,
                   const std::shared_ptr<TransferEngine> &transfer_engine,
                   const std::string &ipc_socket_path) {
        // Dummy client does not support real setup
        return -1;
    };

    int setup_dummy(size_t mem_pool_size, size_t local_buffer_size,
                    const std::string &server_address,
                    const std::string &ipc_socket_path);

    int initAll(const std::string &protocol, const std::string &device_name,
                size_t mount_segment_size) {
        // Dummy client does not support real setup
        return -1;
    }

    uint64_t alloc_from_mem_pool(size_t size);

    int put(const std::string &key, std::span<const char> value,
            const ReplicateConfig &config = ReplicateConfig{});

    int register_buffer(void *buffer, size_t size);

    int unregister_buffer(void *buffer);

    int64_t get_into(const std::string &key, void *buffer, size_t size);

    std::vector<int64_t> batch_get_into(const std::vector<std::string> &keys,
                                        const std::vector<void *> &buffers,
                                        const std::vector<size_t> &sizes);

    std::vector<int> batch_get_into_multi_buffers(
        const std::vector<std::string> &keys,
        const std::vector<std::vector<void *>> &all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        bool prefer_same_node);

    int put_from(const std::string &key, void *buffer, size_t size,
                 const ReplicateConfig &config = ReplicateConfig{});

    int put_from_with_metadata(
        const std::string &key, void *buffer, void *metadata_buffer,
        size_t size, size_t metadata_size,
        const ReplicateConfig &config = ReplicateConfig{});

    std::vector<int> batch_put_from(
        const std::vector<std::string> &keys,
        const std::vector<void *> &buffers, const std::vector<size_t> &sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    std::vector<int> batch_put_from_multi_buffers(
        const std::vector<std::string> &keys,
        const std::vector<std::vector<void *>> &all_buffers,
        const std::vector<std::vector<size_t>> &all_sizes,
        const ReplicateConfig &config = ReplicateConfig{});

    std::shared_ptr<BufferHandle> get_buffer(const std::string &key);

    std::tuple<uint64_t, size_t> get_buffer_info(const std::string &key);

    std::vector<std::shared_ptr<BufferHandle>> batch_get_buffer(
        const std::vector<std::string> &keys);

    int put_parts(const std::string &key,
                  std::vector<std::span<const char>> values,
                  const ReplicateConfig &config = ReplicateConfig{});

    int put_batch(const std::vector<std::string> &keys,
                  const std::vector<std::span<const char>> &values,
                  const ReplicateConfig &config = ReplicateConfig{});

    [[nodiscard]] std::string get_hostname() const;

    int remove(const std::string &key, bool force = false);

    long removeByRegex(const std::string &str, bool force = false);

    long removeAll(bool force = false);

    int isExist(const std::string &key);

    std::vector<int> batchIsExist(const std::vector<std::string> &keys);

    int64_t getSize(const std::string &key);

    std::map<std::string, std::vector<Replica::Descriptor>>
    batch_get_replica_desc(const std::vector<std::string> &keys);
    std::vector<Replica::Descriptor> get_replica_desc(const std::string &key);

    int tearDownAll();

    tl::expected<UUID, ErrorCode> create_copy_task(
        const std::string &key, const std::vector<std::string> &targets);

    tl::expected<UUID, ErrorCode> create_move_task(const std::string &key,
                                                   const std::string &source,
                                                   const std::string &target);

    tl::expected<QueryTaskResponse, ErrorCode> query_task(const UUID &task_id);

   private:
    ErrorCode connect(const std::string &server_address);

    int register_shm_via_ipc(const ShmHelper::ShmSegment *shm,
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
        Args &&...args);

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
    invoke_batch_rpc(size_t input_size, Args &&...args);

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

    // For shared memory management
    ShmHelper *shm_helper_ = nullptr;
    std::string ipc_socket_path_;

    // For high availability
    std::thread ping_thread_;
    std::atomic<bool> ping_running_{false};
    void ping_thread_main();
    volatile bool connected_ = false;
};

}  // namespace mooncake
