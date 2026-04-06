#pragma once

#include <boost/functional/hash.hpp>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <ylt/util/tl/expected.hpp>
#include "mutex.h"

#include "client_metric.h"
#include "ha_helper.h"
#include "transfer_engine.h"
#include "types.h"
#include "p2p_rpc_types.h"
#include "replica.h"
#include "master_client.h"
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include "client_config_builder.h"
#include "client_buffer.hpp"

namespace mooncake {

using WriteConfig = std::variant<ReplicateConfig, WriteRouteRequestConfig>;

/**
 * @brief Result of a query operation containing replica information
 */
class QueryResult {
   public:
    /** @brief List of available replicas for the queried key */
    const std::vector<Replica::Descriptor> replicas;

    explicit QueryResult(std::vector<Replica::Descriptor>&& replicas_param)
        : replicas(std::move(replicas_param)) {}

    virtual ~QueryResult() = default;

    // Disable copy to prevent slicing; allow move
    QueryResult(const QueryResult&) = delete;
    QueryResult& operator=(const QueryResult&) = delete;
    QueryResult(QueryResult&&) = default;
    QueryResult& operator=(QueryResult&&) = default;
};

/**
 * @brief Client for interacting with the mooncake distributed object store
 */
class ClientService {
   public:
    virtual ~ClientService();

    /**
     * @brief stops background threads
     */
    virtual void Stop();

    /**
     * @brief stops heartbeat thread
     */
    virtual void StopHeartbeat();

    /**
     * @brief Release internal resources. Should be called after Stop()
     */
    virtual void Destroy();

    /**
     * @brief Creates and initializes a new ClientService instance
     * @param config The start up configuration for the client service.
     * @return std::optional containing a shared_ptr to ClientService if
     * successful, std::nullopt otherwise
     */
    static std::optional<std::shared_ptr<ClientService>> Create(
        const CentralizedClientConfig& config);
    static std::optional<std::shared_ptr<ClientService>> Create(
        const P2PClientConfig& config);

    /**
     * @brief Returns the deployment mode of the client service.
     * @return DeploymentMode (CENTRALIZATION or P2P).
     */
    virtual DeploymentMode deployment_mode() const = 0;

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure.
     */
    virtual tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    /**
     * @brief Queries replica lists for object keys that match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    virtual tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    QueryByRegex(const std::string& str);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @return QueryResult (or its subclass) containing replicas, or ErrorCode
     * indicating failure
     */
    virtual tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key, const ReadRouteConfig& config = {}) = 0;

    /**
     * @brief Batch query object metadata without transferring data
     * @param object_keys Keys to query
     * @return Vector of QueryResult (or its subclass) containing replicas
     */
    virtual std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) = 0;

    /**
     * @brief Gets data with memory allocation
     * @param key Object key
     * @param allocator Read buffer allocator
     * @param config Read route config
     * @return BufferHandle allocated by `allocator` on success.
     *         ErrorCode on failure.
     */
    virtual tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config = {}) = 0;

    virtual std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const ReadRouteConfig& config = {}) = 0;

    /**
     * @brief Gets data into user-provided buffers without memory allocation
     * @param key Object key
     * @param buffers Vector of destination buffer pointers
     * @param sizes Vector of buffer sizes (must match buffers.size())
     * @param config Read route config
     * @return Number of bytes read on success. ErrorCode on failure.
     */
    virtual tl::expected<int64_t, ErrorCode> Get(
        const std::string& key, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const ReadRouteConfig& config = {}) = 0;

    /**
     * @brief Batch get data into user-provided buffers
     * @param keys Object keys
     * @param all_buffers Vector of buffer pointer vectors (one per key)
     * @param all_sizes Vector of buffer size vectors (one per key)
     * @param config Read route config
     * @param aggregate_same_segment_task
     * Whether to aggregate read tasks on the same segment.
     * If false, each key will be generated as a independent task.
     * Otherwise, the tasks will be aggregated on the same segment.
     * @return Vector of bytes read on success. ErrorCode on failure.
     */
    virtual std::vector<tl::expected<int64_t, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const ReadRouteConfig& config = {},
        bool aggregate_same_segment_task = false) = 0;

    /**
     * @brief Stores data with replication
     * @param key Object key
     * @param slices Vector of data slices to store
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    virtual tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                              std::vector<Slice>& slices,
                                              const WriteConfig& config) = 0;

    /**
     * @brief Batch put data with replication
     * @param keys Object keys
     * @param batched_slices Vector of vectors of data slices to store (indexed
     * to match keys)
     * @param config Replication configuration
     */
    virtual std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) = 0;

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    virtual tl::expected<void, ErrorCode> Remove(const ObjectKey& key) = 0;

    /**
     * @brief Removes objects from the store whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    virtual tl::expected<long, ErrorCode> RemoveByRegex(
        const ObjectKey& str) = 0;

    /**
     * @brief Removes all objects and all its replicas
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    virtual tl::expected<long, ErrorCode> RemoveAll() = 0;

    /**
     * @brief Registers a memory segment to master for allocation
     * @param buffer Memory buffer to register
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    virtual tl::expected<void, ErrorCode> MountSegment(const void* buffer,
                                                       size_t size) = 0;

    /**
     * @brief Unregisters a memory segment from master
     * @param buffer Memory buffer to unregister
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    virtual tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                         size_t size) = 0;

    /**
     * @brief Registers memory buffer with TransferEngine for data transfer
     * @param addr Memory address to register
     * @param length Size of the memory region
     * @param location Device location (e.g. "cpu:0")
     * @param remote_accessible Whether the memory can be accessed remotely
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    /**
     * @brief Unregisters memory buffer from TransferEngine
     * @param addr Memory address to unregister
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    /**
     * @brief Checks if an object exists
     * @param key Key to check
     * @return True if exists, false if not, or ErrorCode for unexpected errors.
     */
    virtual tl::expected<bool, ErrorCode> IsExist(const std::string& key) = 0;

    /**
     * @brief Checks if multiple objects exist
     * @param keys Vector of keys to check
     * @return Vector of existence results for each key
     */
    virtual std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys) = 0;

    // For human-readable metrics
    tl::expected<std::string, ErrorCode> GetSummaryMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return metrics_->summary_metrics();
    }

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats() {
        auto guard = AcquireInflightGuard();
        if (!guard.is_valid()) {
            LOG(ERROR) << "client is shutting down";
            return tl::unexpected(ErrorCode::SHUTTING_DOWN);
        }
        return GetMasterClient().CalcCacheStats();
    }

    // For Prometheus-style metrics
    tl::expected<std::string, ErrorCode> SerializeMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        std::string str;
        metrics_->serialize(str);
        return str;
    }

   public:
    /**
     * @brief Gets the local transport endpoint (IP and port).
     * @return The transport endpoint string.
     */
    [[nodiscard]] std::string GetTransportEndpoint() {
        return transfer_engine_->getLocalIpAndPort();
    }
    UUID GetClientID() const { return client_id_; }
    ViewVersionId GetViewVersion() const { return view_version_; }

   public:
    /**
     * @brief Checks if memory registration parameters are valid
     * @param addr Memory address to check
     * @param length Size of the memory region
     * @return ErrorCode indicating success or failure
     */
    static tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
        const void* addr, size_t length);

    /**
     * @brief Calculate the total size of a list of slices.
     * @param slices Vector of slices.
     * @return Total size in bytes.
     */
    [[nodiscard]] static size_t CalculateSliceSize(
        const std::vector<Slice>& slices);

    /**
     * @brief Calculate the total size of a list of slices.
     * @param slices Span of slices.
     * @return Total size in bytes.
     */
    [[nodiscard]] static size_t CalculateSliceSize(
        std::span<const Slice> slices);

   protected:
    /**
     * @brief Private constructor to enforce creation through Create() method
     */
    ClientService(const std::string& local_ip, uint16_t te_port,
                  const std::string& metadata_connstring,
                  const std::map<std::string, std::string>& labels = {});

    /**
     * @brief Get the RPC Client for Master service calls
     * @return Reference to MasterClient
     */
    virtual MasterClient& GetMasterClient() = 0;

    /**
     * @brief Connects to the master server.
     * @param master_server_entry Entry point of the master server.
     * @return ErrorCode indicating success or failure.
     */
    ErrorCode ConnectToMaster(const std::string& master_server_entry);

    /**
     * @brief Initializes the Transfer Engine.
     * @param local_hostname Local hostname or IP.
     * @param metadata_connstring Connection string for metadata service.
     * @param protocol Transport protocol (e.g., "tcp", "rdma").
     * @param device_names Optional RDMA device names.
     * @return ErrorCode indicating success or failure.
     */
    ErrorCode InitTransferEngine(
        const std::string& endpoint, const std::string& metadata_connstring,
        const std::string& protocol,
        const std::optional<std::string>& device_names);

   protected:
    // Heartbeat-related function

    /**
     * @brief Starts the heartbeat thread.
     * @param master_server_entry Entry point of the master server.
     */
    void StartHeartbeat(const std::string& master_server_entry);

    void HeartbeatThreadMain(bool is_ha_mode,
                             std::string current_master_address);

    /**
     * @brief Handles a successful heartbeat response.
     * Triggers async RegisterClient if master reports UNDEFINED status.
     * @return true if heartbeat was successfully processed.
     */
    bool HandleHeartbeatResponse(const HeartbeatResponse& response,
                                 const std::string& current_master_address,
                                 const std::function<void()>& register_client,
                                 std::future<void>& register_client_future);

    /**
     * @brief Handles the result of a task received in a heartbeat response.
     * @param task_result The result of the task.
     */
    void HandleHeartbeatTaskResult(const HeartbeatTaskResult& task_result);

    /**
     * @brief Attempts to reconnect to master after heartbeat failures.
     * For HA mode, fetches the latest master address from etcd.
     * For non-HA mode, reconnects to the same address.
     * @return true if reconnect succeeded, false otherwise.
     */
    bool ReconnectToMaster(bool is_ha_mode,
                           std::string& current_master_address);

    /**
     * @brief Waits for the next heartbeat interval using condition variable.
     * @param interval_ms Milliseconds to wait.
     */
    void WaitForNextHeartbeat(int interval_ms);
    virtual HeartbeatRequest build_heartbeat_request() = 0;

    /**
     * @brief Registers the client into the master server.
     * @return An ErrorCode indicating success or failure.
     */
    virtual tl::expected<RegisterClientResponse, ErrorCode>
    RegisterClient() = 0;

   protected:
    /**
     * @brief RAII guard for managing in-flight requests during service
     * shutdown.
     */
    class InflightRequestGuard {
       public:
        explicit InflightRequestGuard(ClientService* client)
            : client_(client),
              valid_(false),
              lock_(&client_->running_rw_mtx_, shared_lock) {
            valid_ = client_->is_running_;
        }
        ~InflightRequestGuard() = default;

        InflightRequestGuard(const InflightRequestGuard&) = delete;
        InflightRequestGuard& operator=(const InflightRequestGuard&) = delete;
        InflightRequestGuard(InflightRequestGuard&& other) = delete;
        InflightRequestGuard& operator=(InflightRequestGuard&& other) = delete;

        bool is_valid() const { return valid_; }

       private:
        ClientService* client_;
        bool valid_;
        SharedMutexLocker lock_;
    };

    /**
     * @brief Acquires an inflight request guard.
     * @return An InflightRequestGuard. If shutting down, is_valid() will be
     * false.
     */
    InflightRequestGuard AcquireInflightGuard() {
        return InflightRequestGuard(this);
    }

    /**
     * @brief Marks the service as shutting down.
     * @return true if successfully marked, false if already shutting down.
     */
    bool MarkShuttingDown() {
        SharedMutexLocker lock(&running_rw_mtx_);
        if (!is_running_) return false;
        is_running_ = false;
        return true;
    }

    friend class InflightRequestGuard;

   protected:
    // Client identification
    const UUID client_id_;

    // Client-side metrics
    std::unique_ptr<ClientMetric> metrics_;

    // Core components
    std::shared_ptr<TransferEngine> transfer_engine_;
    // Global segment pointers
    struct SegmentDeleter {
        void operator()(void* ptr) {
            if (ptr) {
                free(ptr);
            }
        }
    };

    struct AscendSegmentDeleter {
        void operator()(void* ptr) {
            if (ptr) {
                free_memory("ascend", ptr);
            }
        }
    };
    std::vector<std::unique_ptr<void, SegmentDeleter>> segment_ptrs_;
    std::vector<std::unique_ptr<void, AscendSegmentDeleter>>
        ascend_segment_ptrs_;

    // Configuration
    const std::string local_ip_;
    const uint16_t te_port_;
    std::string local_endpoint() const {
        return local_ip_ + ":" + std::to_string(te_port_);
    }

    // The segment endpoint that the transfer engine registered with the
    // metadata backend.
    std::string te_endpoint_;
    void initTeEndpoint();
    const std::string& get_te_endpoint() const { return te_endpoint_; }

    const std::string metadata_connstring_;
    // For high availability
    MasterViewHelper master_view_helper_;
    std::thread heartbeat_thread_;
    std::atomic<bool> heartbeat_running_{false};
    std::condition_variable heartbeat_cv_;
    std::mutex heartbeat_mtx_;
    ViewVersionId view_version_{0};

    // Shutdown protection
    SharedMutex running_rw_mtx_;
    bool is_running_ GUARDED_BY(running_rw_mtx_) = false;
};

}  // namespace mooncake
