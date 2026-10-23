#pragma once

#include <atomic>
#include <condition_variable>
#include <csignal>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <coroutine>
#include <async_simple/Executor.h>
#include <async_simple/Future.h>
#include <async_simple/Promise.h>
#include <async_simple/Try.h>
#include <async_simple/coro/Lazy.h>

#include "p2p/client/async_metadata_notifier.h"
#include "client_buffer.hpp"
#include "client_resources.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/client/p2p_client_config_builder.h"
#include "mutex.h"
#include "inflight_tracker.h"
#include "p2p/client/runtime_config_store.h"
#include "transfer_engine.h"
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include "p2p/client/data_manager.h"
#include "p2p/client/data_manager_factory.h"
#include "p2p/client/client_rpc_service.h"
#include "p2p/ha/metadata_recovery_worker.h"
#include "p2p/ha/p2p_master_view.h"
#include "p2p/client/peer_client.h"
#include "p2p/client/p2p_client_metric.h"
#include "p2p/master/p2p_master_client.h"
#include "p2p/client/route_cache.h"
#include "p2p/client/task_handle.h"

namespace mooncake {

class P2PClientService final {
   public:
    // Native lifecycle, data and memory-registration API.
    static std::optional<std::shared_ptr<P2PClientService>> Create(
        const P2PClientConfig& config);

    /**
     * @brief Constructor for P2PClientService.
     * @param metadata_connstring Connection string for metadata server.
     * @param http_port Port for HTTP server.
     * @param enable_http_server Whether to enable HTTP server.
     * @param labels Optional labels for client metrics.
     */
    P2PClientService(const std::string& metadata_connstring,
                     uint16_t http_port = 9003, bool enable_http_server = true,
                     const std::map<std::string, std::string>& labels = {},
                     bool enable_metric_collection = true);

    ~P2PClientService();

    ErrorCode Init(const P2PClientConfig& config);

    /**
     * @brief
     * 1. Stops heartbeat, RPC server, and all background threads of submodules.
     * 2. Rejects all incoming requests.
     */
    void Stop();

    /**
     * @brief Release internal resources.
     */
    void Destroy();

    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    /**
     * @brief Proactively unregister from the master, pause heartbeats, and
     * switch to a stable LOCAL_ONLY service.
     */
    tl::expected<void, ErrorCode> UnregisterClient() EXCLUDES(lifecycle_mutex_);

    /**
     * @brief Single put data for a key.
     * @param key The object key.
     * @param slices Data slices.
     * @param config Replicate configuration.
     * @return An ErrorCode indicating the status.
     */
    tl::expected<void, ErrorCode> Put(
        const ObjectKey& key, std::vector<Slice>& slices,
        const std::optional<P2PWriteRouteConfig>& config = std::nullopt);

    /**
     * @brief Batch put data for multiple keys.
     * @param keys The list of object keys.
     * @param batched_slices The list of data slices for each key.
     * @param config Explicit routing config, or a runtime snapshot if omitted.
     * @return A vector of ErrorCode results for each key.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const std::optional<P2PWriteRouteConfig>& config = std::nullopt);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @return Native P2P routes, or ErrorCode
     * indicating failure
     */
    tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode> Query(
        const std::string& object_key, const P2PReadRouteConfig& config = {});

    /**
     * @brief Batch query object metadata without transferring data
     * @param object_keys Keys to query
     * @return Per-key native P2P routes in request order
     */
    std::vector<tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const P2PReadRouteConfig& config = {});

    tl::expected<bool, ErrorCode> IsExist(const std::string& key);

    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys);

    tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const std::optional<P2PReadRouteConfig>& config = std::nullopt);

    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const std::optional<P2PReadRouteConfig>& config = std::nullopt);

    tl::expected<int64_t, ErrorCode> Get(
        const std::string& key, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const std::optional<P2PReadRouteConfig>& config = std::nullopt);

    std::vector<tl::expected<int64_t, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const std::optional<P2PReadRouteConfig>& config = std::nullopt,
        bool aggregate_same_segment_task = false);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Remove(const ObjectKey& key,
                                         bool force = false);

    /**
     * @brief Removes objects from the store whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str,
                                                bool force = false);

    /**
     * @brief Removes all objects and all its replicas
     * @param force If true, skip lease and replication task checks.
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    tl::expected<long, ErrorCode> RemoveAll(bool force = false);

    /**
     * @brief Removes all objects from THIS client's local tiered storage
     * @return Number of removed objects, or ErrorCode on failure.
     */
    tl::expected<long, ErrorCode> RemoveAllLocal();

    /**
     * @brief Removes a single object from THIS client's local tiered storage.
     * @param key Key to remove
     * @return ErrorCode indicating success/failure.
     */
    tl::expected<void, ErrorCode> RemoveLocal(const ObjectKey& key);

    P2PMasterClient& GetMasterClient() { return master_client_; }

    // Missing or unavailable clients are omitted by the P2P master.
    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    tl::expected<
        std::unordered_map<std::string, std::vector<P2PRouteDescriptor>>,
        ErrorCode>
    QueryByRegex(const std::string& regex);

   public:
    // Diagnostics and runtime configuration access.
    P2PClientMetric* GetMetrics() { return metrics_.get(); }

    tl::expected<std::string, ErrorCode> GetSummaryMetrics();

    tl::expected<std::string, ErrorCode> SerializeMetrics();

    std::string GetHealthStatus() const;

    // Access service-owned client state.
    uint16_t GetHttpPort() const { return http_port_; }
    uint16_t GetRpcPort() const { return client_rpc_port_; }
    bool IsHttpServerEnabled() const { return http_server_ != nullptr; }

    // Buffers and defaults used by the shared deployment backend.
    std::shared_ptr<ClientBufferAllocator> GetBufferAllocator() const {
        return resources_.GetBufferAllocator();
    }

    RuntimeConfigStore& getRuntimeConfigStore() {
        return *runtime_config_store_;
    }
    std::string local_endpoint() const { return resources_.GetLocalEndpoint(); }
    UUID GetClientID() const { return client_id_; }

   private:
    tl::expected<P2PRouteDescriptor, ErrorCode> QueryLocalRoute(
        const std::string& key);
    /**
     * @brief init TieredBackend and DataManager
     *        1. build metadata and segment sync callback
     *        2. build tiered config
     *        3. init tiered backend and data manager
     */
    ErrorCode InitStorage(const P2PClientConfig& config);

    /**
     * @brief build add replica callback.
     *        when tier add replica, call master to update metadata
     */
    AddReplicaCallback BuildAddReplicaCallback();

    /**
     * @brief build remove replica callback.
     *        when tier remove replica, call master to update metadata
     */
    RemoveReplicaCallback BuildRemoveReplicaCallback();

    /**
     * @brief build segment sync callback.
     *        when tier add/remove segment, call master to mount/unmount segment
     */
    SegmentSyncCallback BuildSegmentSyncCallback();

    /**
     * @brief handle COMMIT type callback: notify master to add new replica
     */
    tl::expected<void, ErrorCode> SyncAddReplica(std::string_view key,
                                                 const UUID& tier_id,
                                                 size_t size);

    /**
     * @brief handle DELETE type callback: notify master to remove replica
     */
    tl::expected<void, ErrorCode> SyncRemoveReplica(std::string_view key,
                                                    const UUID& tier_id);

    /**
     * @brief handle batch DELETE: notify master to remove replicas from
     *        multiple segments in one RPC call
     * @param key Key to remove
     * @param segment_ids Vector of segment IDs to remove (it will be moved)
     * @return Vector of ErrorCode results for each segment
     */
    std::vector<tl::expected<void, ErrorCode>> SyncBatchRemoveReplica(
        std::string_view key, std::vector<UUID> segment_ids);

    /**
     * @brief Collect tier info from DataManager and build P2P Segments.
     */
    std::vector<P2PSegment> CollectTierSegments() const;

    tl::expected<ViewVersionId, ErrorCode> InnerRegisterClient()
        REQUIRES(lifecycle_mutex_);
    ErrorCode EnterOnline(P2PClientServiceState rollback_state)
        REQUIRES(lifecycle_mutex_);
    ErrorCode EnterLocalOnly() REQUIRES(lifecycle_mutex_);
    ErrorCode EnterDegraded(const char* reason) REQUIRES(lifecycle_mutex_);
    ErrorCode StopClusterResources(bool unregister) REQUIRES(lifecycle_mutex_);

   private:
    bool IsHAMode(const std::string& master_server_entry) const;
    void SetMasterDiscoveryConfig(const P2PClientConfig& config);
    ErrorCode ResolveMasterAddress(const std::string& master_server_entry,
                                   std::string& master_address);

   private:
    ErrorCode ConnectToMaster(const std::string& master_server_entry)
        REQUIRES(lifecycle_mutex_);
    ErrorCode StartHeartbeat() REQUIRES(lifecycle_mutex_);
    void HeartbeatThreadMain();
    ErrorCode ReconnectToMaster() REQUIRES(lifecycle_mutex_);
    enum class ClientEvent {
        INITIALIZE_ONLINE,
        INITIALIZE_LOCAL,
        REGISTER_REQUESTED,
        UNREGISTER_REQUESTED,
        HEARTBEAT_HEALTHY,
        REGISTRATION_REQUIRED,
        MASTER_UNREACHABLE,
        STOP_REQUESTED,
    };
    std::optional<ClientEvent> HandleHeartbeatResponse(
        const P2PHeartbeatResponse& response);
    void HandleHeartbeatTaskResult(const HeartbeatTaskResult& task_result);
    P2PHeartbeatRequest build_heartbeat_request();
    ErrorCode HandleEvent(ClientEvent event) EXCLUDES(lifecycle_mutex_);
    ErrorCode HandleEventLocked(ClientEvent event) REQUIRES(lifecycle_mutex_);
    ErrorCode HandleInitializeEventLocked(ClientEvent event)
        REQUIRES(lifecycle_mutex_);
    ErrorCode HandleRegisterEventLocked() REQUIRES(lifecycle_mutex_);
    ErrorCode HandleUnregisterEventLocked() REQUIRES(lifecycle_mutex_);
    ErrorCode HandleHeartbeatEventLocked(ClientEvent event)
        REQUIRES(lifecycle_mutex_);
    ErrorCode HandleStopEvent() EXCLUDES(lifecycle_mutex_);
    P2PClientServiceState GetServiceState() const {
        return service_state_.load(std::memory_order_acquire);
    }
    bool IsLocalService() const;
    void PublishServiceState(P2PClientServiceState state, const char* reason)
        REQUIRES(lifecycle_mutex_);

   private:
    bool IsLocalWrite(const P2PWriteRouteConfig& cfg) const;
    bool IsBelowLocalWaterline(const P2PWriteRouteConfig& cfg) const;

    std::vector<tl::expected<void, ErrorCode>> InnerBatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const std::vector<size_t>& sizes,
        const P2PWriteRouteConfig& route_config);

    std::vector<tl::expected<void, ErrorCode>> InnerBatchPutLocalOnly(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const std::vector<size_t>& sizes);

    std::vector<tl::expected<void, ErrorCode>> InnerBatchPutNormal(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const std::vector<size_t>& sizes,
        const P2PWriteRouteConfig& route_config);

    std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>
    CreatePutHandlesFromRoute(const std::vector<ObjectKey>& keys,
                              std::vector<std::vector<Slice>>& batched_slices,
                              const std::vector<size_t>& sizes,
                              const P2PWriteRouteConfig& route_config,
                              P2PBatchGetWriteRouteResponse& batch_resp);

    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
    CreatePutHandleFromLocal(std::string_view key, std::vector<Slice>& slices);

    std::vector<tl::expected<void, ErrorCode>> CollectResults(
        std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>&
            handles,
        const std::vector<ObjectKey>& keys, P2PClientMetric* metrics = nullptr,
        const std::vector<size_t>* sizes = nullptr);

    tl::expected<P2PBatchGetWriteRouteResponse, ErrorCode>
    BatchFetchWriteRoutes(const std::vector<ObjectKey>& keys,
                          const std::vector<size_t>& sizes,
                          const P2PWriteRouteConfig& config);

    struct WriteOp {
        virtual ~WriteOp() = default;
        virtual std::string_view route() const = 0;
        // starts an async write task, then generate a wait task handle
        virtual std::unique_ptr<TaskHandle<void>> Dispatch() = 0;
        // Set by Dispatch() before the actual I/O begins.
        std::chrono::steady_clock::time_point dispatch_start{};
    };

    struct LocalWriteOp : WriteOp {
        DataManager* data_manager;
        std::string_view key;
        std::vector<Slice>* slices;

        LocalWriteOp(DataManager* dm, std::string_view k, std::vector<Slice>* s)
            : data_manager(dm), key(k), slices(s) {}

        std::string_view route() const override { return "local"; }
        std::unique_ptr<TaskHandle<void>> Dispatch() override;
    };

    struct RemoteForwardWriteOp : WriteOp {
        using WritePromise =
            async_simple::Promise<tl::expected<void, ErrorCode>>;
        using TeTransferFn =
            std::function<async_simple::Future<tl::expected<void, ErrorCode>>(
                void* local_base, size_t size,
                const std::vector<RemoteBufferDesc>& dest_buffers)>;

        PeerClient* peer_ptr;
        std::shared_ptr<P2PClientMetric> metrics;
        std::shared_ptr<RemoteWriteRequest> write_req;
        std::string endpoint;
        std::vector<Slice>* slices;
        TeTransferFn te_transfer;
        async_simple::Executor* coro_executor = nullptr;

        RemoteForwardWriteOp(PeerClient* p, std::shared_ptr<P2PClientMetric> m,
                             std::shared_ptr<RemoteWriteRequest> wr,
                             std::string ep, std::vector<Slice>* s,
                             TeTransferFn transfer,
                             async_simple::Executor* executor)
            : peer_ptr(p),
              metrics(m),
              write_req(std::move(wr)),
              endpoint(std::move(ep)),
              slices(s),
              te_transfer(std::move(transfer)),
              coro_executor(executor) {}

        std::string_view route() const override { return endpoint; }
        std::unique_ptr<TaskHandle<void>> Dispatch() override;

       private:
        static async_simple::coro::Lazy<void> RunForwardRemotePut(
            std::shared_ptr<WritePromise> promise, PeerClient* peer,
            std::shared_ptr<P2PClientMetric> metrics, TeTransferFn te_transfer,
            std::shared_ptr<RemoteWriteRequest> write_req,
            std::vector<Slice>* slices);
    };

    struct RemoteReverseWriteOp : WriteOp {
        PeerClient* peer_ptr;
        std::shared_ptr<RemoteWriteRequest> write_req;
        P2PRouteDescriptor proxy;
        RouteCache* route_cache;
        std::string endpoint;

        RemoteReverseWriteOp(PeerClient* p,
                             std::shared_ptr<RemoteWriteRequest> wr,
                             P2PRouteDescriptor px, RouteCache* rc,
                             std::string ep)
            : peer_ptr(p),
              write_req(std::move(wr)),
              proxy(std::move(px)),
              route_cache(rc),
              endpoint(std::move(ep)) {}

        std::string_view route() const override { return endpoint; }
        std::unique_ptr<TaskHandle<void>> Dispatch() override;
    };

    tl::expected<std::vector<std::unique_ptr<WriteOp>>, ErrorCode>
    BuildWriteOps(std::string_view key, std::vector<Slice>& slices,
                  size_t object_size, const P2PWriteRouteConfig& config,
                  std::vector<P2PWriteCandidate> candidates);

    async_simple::coro::Lazy<void> RunWriteWithRetry(
        std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
            promise,
        std::unique_ptr<TaskHandle<void>> current_task,
        std::unique_ptr<WriteOp> current_op,
        std::vector<std::unique_ptr<WriteOp>> retry_op_list,
        std::string_view key, size_t object_size);

   private:
    struct ResolvedRoute {
        PeerClient* peer = nullptr;
        uint64_t object_size = 0;
        bool is_cached = false;
        P2PRouteDescriptor proxy;  // for RemoveReplica on stale-cache eviction
    };

    // Yields ResolvedRoute candidates from cache first, then a one-shot lazy
    // master fallback. Call Prime() to pre-load before accessing object_size().
    class RouteIterator {
       public:
        using MasterFetch = std::function<
            async_simple::coro::Lazy<std::vector<ResolvedRoute>>()>;

        RouteIterator(std::string_view key, std::vector<ResolvedRoute> initial,
                      uint64_t object_size, RouteCache* route_cache,
                      MasterFetch master_fetch);

        uint64_t object_size() const { return object_size_; }
        bool empty() const { return routes_.empty() && master_queried_; }

        void Prime();
        async_simple::coro::Lazy<std::optional<ResolvedRoute>> AsyncNext();
        void Evict(const ResolvedRoute& route);

       private:
        void UpsertToCache(const std::vector<ResolvedRoute>& routes);

        std::string key_;
        std::vector<ResolvedRoute> routes_;
        size_t idx_ = 0;
        bool master_queried_ = false;
        uint64_t object_size_ = 0;
        RouteCache* route_cache_ = nullptr;
        MasterFetch master_fetch_;
    };

    std::vector<ResolvedRoute> LoadCachedRoutes(std::string_view key);

    std::vector<ResolvedRoute> RouteDescriptorsToRoutes(
        const std::vector<P2PRouteDescriptor>& descriptors);

    tl::expected<RouteIterator, ErrorCode> BuildRouteIter(
        std::string_view key, const P2PReadRouteConfig& config,
        std::vector<ResolvedRoute> pre_fetched);

   private:
    template <typename ResultT, typename CreateHandlesFn, typename ExtractFn>
    std::vector<tl::expected<ResultT, ErrorCode>> BatchGetImpl(
        const std::vector<std::string>& keys, CreateHandlesFn&& create_handles,
        ExtractFn&& extract);

    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> BatchCreateGetHandles(
        const std::vector<std::string>& keys,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const P2PReadRouteConfig& config);

    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> BatchCreateGetHandles(
        const std::vector<std::string>& keys,
        std::vector<std::vector<Slice>>& all_slices,
        const P2PReadRouteConfig& config);

    template <typename LocalGetFn, typename RemoteGetFn>
    std::vector<tl::expected<ReadTaskHandle, ErrorCode>>
    BatchCreateGetHandlesImpl(const std::vector<std::string>& keys,
                              const P2PReadRouteConfig& config,
                              LocalGetFn&& local_get, RemoteGetFn&& remote_get);

    std::vector<tl::expected<std::vector<ResolvedRoute>, ErrorCode>>
    BatchFetchReadRoutes(const std::vector<std::string_view>& keys,
                         const P2PReadRouteConfig& config);

    tl::expected<ReadTaskHandle, ErrorCode> CreateRemoteGetHandle(
        std::string_view key, std::shared_ptr<ClientBufferAllocator> allocator,
        const P2PReadRouteConfig& config,
        std::vector<ResolvedRoute> pre_fetched);

    tl::expected<ReadTaskHandle, ErrorCode> CreateRemoteGetHandle(
        std::string_view key, std::vector<Slice>& slices,
        const P2PReadRouteConfig& config,
        std::vector<ResolvedRoute> pre_fetched);

    /**
     * @brief Launch async reads driven by a RouteIterator.
     *
     * Creates a ReadRetryContinuation that fires the first RPC immediately
     * and chains subsequent candidates on failure (no stack recursion).
     */
    tl::expected<ReadTaskHandle, ErrorCode> InnerGetViaRoute(
        std::string_view key, std::vector<Slice>& slices, RouteIterator iter);

    async_simple::coro::Lazy<void> RunReadWithRetry(
        RouteIterator iter, std::shared_ptr<RemoteReadRequest> req,
        std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
            promise);

    // Returns per-route ErrorCode. On OK, fulfills promise. INVALID_PARAMS /
    // NOT_IMPLEMENTED are terminal in RunReadWithRetry; other codes retry until
    // routes are exhausted (final_result set at end).
    async_simple::coro::Lazy<ErrorCode> RunForwardReadOnRoute(
        const ResolvedRoute& route, std::shared_ptr<RemoteReadRequest> req,
        std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
            promise);

    async_simple::coro::Lazy<std::vector<ResolvedRoute>>
    AsyncResolveRoutesFromMaster(std::string_view key,
                                 const P2PReadRouteConfig& config);

    /**
     * @brief Get or create a PeerClient for the given endpoint.
     * Thread-safe via peer_clients_mutex_.
     */
    PeerClient& GetOrCreatePeerClient(const std::string& endpoint);

    async_simple::Executor* GetCoroExecutor() const;

   private:
    // Keep the former base entrypoints; common resource bodies are delegated.
    ErrorCode InitLocalBufferAllocator(size_t pool_size,
                                       const std::string& protocol,
                                       bool use_hugepage = false);
    void initTeEndpoint();
    const std::string& get_te_endpoint() const { return te_endpoint_; }

    tl::expected<ViewVersionId, ErrorCode> RegisterClient()
        EXCLUDES(lifecycle_mutex_);

    void RegisterStatusHttpMethods();
    void RegisterRuntimeConfigHttpMethods();
    void RegisterBusinessHttpMethods();
    void StartHttpServer();
    void StopHttpServer();

    InflightTracker::Guard AcquireInflightGuard() {
        return local_inflight_tracker_.Enter();
    }

    void RegisterHttpMethods();
    void RecordLocalInflight(bool entering);

   private:
    tl::expected<size_t, ErrorCode> GetLocalKeyCount();
    tl::expected<std::vector<std::string>, ErrorCode> GetLocalKeys(
        size_t limit = 0);

   private:
    // Technical resources outlive every business member declared below.
    ClientResources resources_;

    struct MasterDiscoveryConfig {
        std::string cluster_id = DEFAULT_CLUSTER_ID;
        std::string redis_username;
        std::string redis_password;
        int redis_db_index = 0;
        int redis_master_view_ttl_sec = 4;
        int redis_heartbeat_interval_sec = 1;
    };
    std::unique_ptr<P2PMasterView> master_view_;
    std::string master_view_entry_;
    MasterDiscoveryConfig master_discovery_config_;

   private:
    const UUID client_id_;
    std::unique_ptr<RuntimeConfigStore> runtime_config_store_;
    std::string te_endpoint_;
    const std::string metadata_connstring_;

    std::thread heartbeat_thread_ GUARDED_BY(lifecycle_mutex_);
    std::condition_variable_any lifecycle_cv_;
    std::atomic<ViewVersionId> view_version_{0};
    std::string master_server_entry_;

    // Serializes events, control RPCs and resource transitions.
    Mutex lifecycle_mutex_;
    std::atomic<P2PClientServiceState> service_state_{
        P2PClientServiceState::INITIALIZING};
    InflightTracker local_inflight_tracker_{
        "local requests", [this] { RecordLocalInflight(true); },
        [this] { RecordLocalInflight(false); }};

    // The port must be initialized before constructing the HTTP server.
    uint16_t http_port_ = 0;
    std::unique_ptr<coro_http::coro_http_server> http_server_;
    std::shared_ptr<P2PClientMetric> metrics_;
    // Attach a SYNC_CLIENT_METRIC task every METRIC_SYNC_FREQ heartbeats.
    static constexpr int METRIC_SYNC_FREQ = 10;
    // Heartbeats since the last SYNC_CLIENT_METRIC task.
    int metric_sync_heartbeat_count_ = 0;
    P2PMasterClient master_client_;
    uint16_t client_rpc_port_ = 0;

    std::unique_ptr<coro_rpc::coro_rpc_server> client_rpc_server_;
    // Own the data-plane implementation; Destroy() releases it.
    std::unique_ptr<DataManager> data_manager_;
    std::optional<ClientRpcService> client_rpc_service_;

    // Each PeerClient instance maintains its own fixed-size connection pool.
    std::mutex peer_clients_mutex_;
    std::map<std::string, std::unique_ptr<PeerClient>> peer_clients_;

    // Route cache for reducing Master query pressure
    std::optional<RouteCache> route_cache_;

    // Async route notifier (nullptr when disabled)
    std::unique_ptr<AsyncMetadataNotifier> async_route_notifier_;

    // Stopped before DataManager/notifier destruction;
    // never owns Service state.
    std::unique_ptr<MetadataRecoveryWorker> recovery_worker_;

    // Cross-node transfer direction from P2PClientConfig at Init().
    TransferDirectionMode transfer_direction_mode_ =
        TransferDirectionMode::REVERSE;
};

}  // namespace mooncake
