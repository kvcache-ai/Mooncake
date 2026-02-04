#pragma once

#include <csignal>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <coroutine>
#include <async_simple/Future.h>
#include <async_simple/Promise.h>
#include <async_simple/Try.h>
#include <async_simple/coro/Lazy.h>

#include "async_metadata_notifier.h"
#include "client_service.h"
#include "data_manager.h"
#include "client_rpc_service.h"
#include "ha_recovery_manager.h"
#include "peer_client.h"
#include "p2p_client_metric.h"
#include "p2p_master_client.h"
#include "route_cache.h"
#include "task_handle.h"

namespace mooncake {

class P2PClientService final : public ClientService {
   public:
    /**
     * @brief Constructor for P2PClientService.
     * @param local_ip IP address of the local node.
     * @param te_port TE port of the local node.
     * @param metadata_connstring Connection string for metadata server.
     * @param metrics_port Port for metrics HTTP server.
     * @param enable_metrics_http Whether to enable metrics HTTP server.
     * @param labels Optional labels for client metrics.
     */
    P2PClientService(const std::string& metadata_connstring,
                     uint16_t metrics_port = 9003,
                     bool enable_metrics_http = true,
                     const std::map<std::string, std::string>& labels = {});

    virtual ~P2PClientService();

    ErrorCode Init(const P2PClientConfig& config);

    /**
     * @brief
     * 1. Stops heartbeat, RPC server, and all background threads of submodules.
     * 2. Rejects all incoming requests.
     */
    void Stop() override;

    /**
     * @brief Release internal resources.
     */
    void Destroy() override;

    /**
     * @brief Single put data for a key.
     * @param key The object key.
     * @param slices Data slices.
     * @param config Replicate configuration.
     * @return An ErrorCode indicating the status.
     */
    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const WriteConfig& config) override;

    /**
     * @brief Batch put data for multiple keys.
     * currently.
     * @param keys The list of object keys.
     * @param batched_slices The list of data slices for each key.
     * @param config Replicate configuration.
     * @return A vector of ErrorCode results for each key.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) override;

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @return QueryResult containing replicas, or ErrorCode
     * indicating failure
     */
    tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key,
        const ReadRouteConfig& config = {}) override;

    /**
     * @brief Batch query object metadata without transferring data
     * @param object_keys Keys to query
     * @return Vector of QueryResult objects containing replicas
     */
    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys) override;

    DeploymentMode deployment_mode() const override {
        return DeploymentMode::P2P;
    }

    tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const ReadRouteConfig& config = {}) override;

    tl::expected<int64_t, ErrorCode> Get(
        const std::string& key, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<int64_t, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const ReadRouteConfig& config = {},
        bool aggregate_same_segment_task = false) override;

    /**
     * @brief Mount a memory segment in P2P mode.
     * @param buffer Start address of the buffer.
     * @param size Size of the buffer in bytes.
     * @return An ErrorCode indicating success or failure.
     */
    tl::expected<void, ErrorCode> MountSegment(const void* buffer,
                                               size_t size,
                                               const std::string& protocol = "tcp") override;

    /**
     * @brief Unmount a memory segment in P2P mode.
     * @param buffer Start address of the buffer.
     * @param size Size of the buffer in bytes.
     * @return An ErrorCode indicating success or failure.
     */
    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size) override;

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Remove(const ObjectKey& key,
                                         bool force = false) override;

    /**
     * @brief Removes objects from the store whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str,
                                                 bool force = false) override;

    /**
     * @brief Removes all objects and all its replicas
     * @param force If true, skip lease and replication task checks.
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    tl::expected<long, ErrorCode> RemoveAll(bool force = false) override;

    MasterClient& GetMasterClient() override { return master_client_; }

    ClientMetric* GetMetrics() override { return metrics_.get(); }

    std::string GetHealthStatus() const override;

   private:
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
    std::vector<Segment> CollectTierSegments() const;

    /**
     * @brief Register the P2P client with the master server.
     * Collects segments from mounted_segments_ and registers them.
     * @return An ErrorCode indicating success or failure.
     */
    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient() override;

    HeartbeatRequest build_heartbeat_request() override;

   private:
    std::vector<tl::expected<void, ErrorCode>> InnerBatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteRouteRequestConfig& route_config);

    std::vector<tl::expected<void, ErrorCode>> InnerBatchPutDegraded(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices);

    std::vector<tl::expected<void, ErrorCode>> InnerBatchPutNormal(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteRouteRequestConfig& route_config);

    std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>
    CreatePutHandlesFromRoute(const std::vector<ObjectKey>& keys,
                              std::vector<std::vector<Slice>>& batched_slices,
                              const WriteRouteRequestConfig& route_config,
                              BatchGetWriteRouteResponse& batch_resp);

    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
    CreatePutHandleFromLocal(std::string_view key, std::vector<Slice>& slices);

    std::vector<tl::expected<void, ErrorCode>> CollectResults(
        std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>&
            handles,
        const std::vector<ObjectKey>& keys);

    tl::expected<BatchGetWriteRouteResponse, ErrorCode> BatchFetchWriteRoutes(
        const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>& batched_slices,
        const WriteRouteRequestConfig& config);

    struct WriteOp {
        virtual ~WriteOp() = default;
        virtual std::string_view route() const = 0;
        // starts an async write task, then generate a wait task handle
        virtual std::unique_ptr<TaskHandle<void>> Dispatch() = 0;
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

    struct RemoteWriteOp : WriteOp {
        PeerClient* peer_ptr;
        std::shared_ptr<RemoteWriteRequest> write_req;
        P2PProxyDescriptor proxy;
        RouteCache* route_cache;
        std::string endpoint;

        RemoteWriteOp(PeerClient* p, std::shared_ptr<RemoteWriteRequest> wr,
                      P2PProxyDescriptor px, RouteCache* rc, std::string ep)
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
                  const WriteRouteRequestConfig& config,
                  std::vector<WriteCandidate> candidates);

    async_simple::coro::Lazy<void> RunWriteWithRetry(
        std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
            promise,
        std::unique_ptr<TaskHandle<void>> current_task,
        std::string current_route,
        std::vector<std::unique_ptr<WriteOp>> retry_op_list,
        std::string_view key);

   private:
    struct ResolvedRoute {
        PeerClient* peer = nullptr;
        uint64_t object_size = 0;
        bool is_cached = false;
        P2PProxyDescriptor proxy;  // for RemoveReplica on stale-cache eviction
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

    std::vector<ResolvedRoute> ReplicasToRoutes(
        const std::vector<Replica::Descriptor>& replicas);

    tl::expected<RouteIterator, ErrorCode> BuildRouteIter(
        std::string_view key, const ReadRouteConfig& config);

    tl::expected<RouteIterator, ErrorCode> BuildRouteIter(
        std::string_view key, const ReadRouteConfig& config,
        std::vector<ResolvedRoute> pre_fetched);

   private:
    template <typename ResultT, typename CreateHandlesFn, typename ExtractFn>
    std::vector<tl::expected<ResultT, ErrorCode>> BatchGetImpl(
        const std::vector<std::string>& keys, CreateHandlesFn&& create_handles,
        ExtractFn&& extract);

    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> BatchCreateGetHandles(
        const std::vector<std::string>& keys,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config);

    std::vector<tl::expected<ReadTaskHandle, ErrorCode>> BatchCreateGetHandles(
        const std::vector<std::string>& keys,
        std::vector<std::vector<Slice>>& all_slices,
        const ReadRouteConfig& config);

    template <typename LocalGetFn, typename RemoteGetFn>
    std::vector<tl::expected<ReadTaskHandle, ErrorCode>>
    BatchCreateGetHandlesImpl(const std::vector<std::string>& keys,
                              const ReadRouteConfig& config,
                              LocalGetFn&& local_get, RemoteGetFn&& remote_get);

    std::vector<tl::expected<std::vector<ResolvedRoute>, ErrorCode>>
    BatchFetchReadRoutes(const std::vector<std::string_view>& keys,
                         const ReadRouteConfig& config);

    tl::expected<ReadTaskHandle, ErrorCode> CreateRemoteGetHandle(
        std::string_view key, std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config, std::vector<ResolvedRoute> pre_fetched);

    tl::expected<ReadTaskHandle, ErrorCode> CreateRemoteGetHandle(
        std::string_view key, std::vector<Slice>& slices,
        const ReadRouteConfig& config, std::vector<ResolvedRoute> pre_fetched);

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

    async_simple::coro::Lazy<std::vector<ResolvedRoute>>
    AsyncResolveRoutesFromMaster(std::string_view key,
                                 const ReadRouteConfig& config);

    /**
     * @brief Get or create a PeerClient for the given endpoint.
     * Thread-safe via peer_clients_mutex_.
     */
    PeerClient& GetOrCreatePeerClient(const std::string& endpoint);

   private:
    void OnHAEvent(HAEvent event) override;

   private:
    std::unique_ptr<P2PClientMetric> metrics_;
    P2PMasterClient master_client_;
    uint16_t client_rpc_port_ = 12345;

    std::unique_ptr<coro_rpc::coro_rpc_server> client_rpc_server_;
    std::thread client_rpc_server_thread_;
    std::optional<DataManager> data_manager_;
    std::optional<ClientRpcService> client_rpc_service_;

    // Each PeerClient instance maintains its own fixed-size connection pool.
    std::mutex peer_clients_mutex_;
    std::map<std::string, std::unique_ptr<PeerClient>> peer_clients_;

    // Route cache for reducing Master query pressure
    std::optional<RouteCache> route_cache_;

    // Async route notifier (nullptr when disabled)
    std::unique_ptr<AsyncMetadataNotifier> async_route_notifier_;

    // HA recovery manager
    std::unique_ptr<HARecoveryManager> ha_manager_;
};

}  // namespace mooncake
