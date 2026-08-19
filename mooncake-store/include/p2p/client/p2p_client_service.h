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

#include "client_service_base.h"
#include "p2p/async_metadata_notifier.h"
#include "p2p/client_rpc_service.h"
#include "p2p/data_manager.h"
#include "p2p/ha_recovery_manager.h"
#include "p2p/peer_client.h"
#include "p2p/p2p_client_metric.h"
#include "p2p/p2p_master_client.h"
#include "p2p/route_cache.h"
#include "p2p/task_handle.h"

namespace mooncake {

struct ClientMasterDiscoveryConfig {
    std::string redis_cluster_id = DEFAULT_CLUSTER_ID;
    std::string redis_username;
    std::string redis_password;
    int redis_db_index = 0;
    int redis_master_view_ttl_sec = 5;
    int redis_heartbeat_interval_sec = 2;
};

class P2PClientService final : public ClientService {
   public:
    P2PClientService(const std::string& metadata_connstring,
                     uint16_t http_port = 9003, bool enable_http_server = true,
                     const std::map<std::string, std::string>& labels = {},
                     bool enable_metric_collection = true);

    ~P2PClientService() override;

    ErrorCode Init(const P2PClientConfig& config);

    static std::optional<std::shared_ptr<P2PClientService>> Create(
        const P2PClientConfig& config);

    void Stop() override;

    void Destroy() override;

    void StopHeartbeat() override EXCLUDES(registration_mutex_);

    void StartKeepalive(const std::string& master_addr) override {
        StartHeartbeat(master_addr);
    }

    tl::expected<void, ErrorCode> UnregisterClient()
        EXCLUDES(registration_mutex_);

    DeploymentMode deployment_mode() const override {
        return DeploymentMode::P2P;
    }

    uint64_t GetViewVersion() const override {
        return view_version_.load();
    }

    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const WriteConfig& config) override;

    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) override;

    tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys) override;

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

    tl::expected<void, ErrorCode> MountSegment(
        const void* buffer, size_t size,
        const std::string& protocol = "tcp") override;

    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                  size_t size) override;

    tl::expected<void, ErrorCode> Remove(const ObjectKey& key,
                                          bool force = false) override;

    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str,
                                                 bool force = false) override;

    tl::expected<long, ErrorCode> RemoveAll(bool force = false) override;

    tl::expected<long, ErrorCode> RemoveAllLocal() override;

    tl::expected<void, ErrorCode> RemoveLocal(const ObjectKey& key) override;

    P2PMasterClient& GetMasterClient() override { return master_client_; }

    ClientMetric* GetMetrics() override { return metrics_.get(); }

    std::string GetHealthStatus() const override;

    void StartMetricsReportingThread() override {
        if (metrics_) {
            metrics_->StartMetricsReportingThread();
        }
    }

    void StopMetricsReportingThread() override {
        if (metrics_) {
            metrics_->StopMetricsReportingThread();
        }
    }

   private:
    void StartHeartbeat(const std::string& master_server_entry);

    void HeartbeatThreadMain(bool is_ha_mode,
                             std::string current_master_address,
                             const std::string& master_server_entry);

    void HandleHeartbeatResponse(const HeartbeatResponse& response,
                                 const std::string& current_master_address,
                                 const std::function<void()>& register_client,
                                 std::future<void>& register_client_future);

    void HandleHeartbeatTaskResult(const HeartbeatTaskResult& task_result);

    bool ReconnectToMaster(bool is_ha_mode,
                           std::string& current_master_address);

    void WaitForNextHeartbeat(int interval_ms);

    HeartbeatRequest build_heartbeat_request();

    void HeartbeatTryRegister();

    void InnerStopHeartbeat() REQUIRES(registration_mutex_);

    void RegisterHttpMethods() override;

    void RegisterRuntimeConfigHttpMethods();

    void RecordLocalInflight(bool entering) override;

    void OnHAEvent(HAEvent event) override;

    ErrorCode ConnectToMaster(const std::string& master_server_entry);

    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient()
        EXCLUDES(registration_mutex_);

    tl::expected<RegisterClientResponse, ErrorCode> InnerRegisterClient()
        override REQUIRES(registration_mutex_);

    bool IsHAMode(const std::string& master_server_entry) const;

    ErrorCode ResolveMasterAddress(const std::string& master_server_entry,
                                   std::string& master_address);

    void SetMasterDiscoveryConfig(const RealClientConfigBase& config);

   private:
    ErrorCode InitStorage(const P2PClientConfig& config);

    AddReplicaCallback BuildAddReplicaCallback();

    RemoveReplicaCallback BuildRemoveReplicaCallback();

    SegmentSyncCallback BuildSegmentSyncCallback();

    tl::expected<void, ErrorCode> SyncAddReplica(std::string_view key,
                                                  const UUID& tier_id,
                                                  size_t size);

    tl::expected<void, ErrorCode> SyncRemoveReplica(std::string_view key,
                                                     const UUID& tier_id);

    std::vector<tl::expected<void, ErrorCode>> SyncBatchRemoveReplica(
        std::string_view key, std::vector<UUID> segment_ids);

    std::vector<Segment> CollectTierSegments() const;

    tl::expected<void, ErrorCode> InnerUnregisterClient()
        REQUIRES(registration_mutex_);

    bool IsLocalWrite(const WriteRouteRequestConfig& cfg) const;
    bool IsBelowLocalWaterline(const WriteRouteRequestConfig& cfg) const;

    std::vector<tl::expected<void, ErrorCode>> InnerBatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const std::vector<size_t>& sizes,
        const WriteRouteRequestConfig& route_config);

    std::vector<tl::expected<void, ErrorCode>> InnerBatchPutLocalOnly(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const std::vector<size_t>& sizes);

    std::vector<tl::expected<void, ErrorCode>> InnerBatchPutNormal(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const std::vector<size_t>& sizes,
        const WriteRouteRequestConfig& route_config);

    std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>
    CreatePutHandlesFromRoute(const std::vector<ObjectKey>& keys,
                              std::vector<std::vector<Slice>>& batched_slices,
                              const std::vector<size_t>& sizes,
                              const WriteRouteRequestConfig& route_config,
                              BatchGetWriteRouteResponse& batch_resp);

    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>
    CreatePutHandleFromLocal(std::string_view key, std::vector<Slice>& slices);

    std::vector<tl::expected<void, ErrorCode>> CollectResults(
        std::vector<tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode>>&
            handles,
        const std::vector<ObjectKey>& keys, P2PClientMetric* metrics = nullptr,
        const std::vector<size_t>* sizes = nullptr);

    tl::expected<BatchGetWriteRouteResponse, ErrorCode> BatchFetchWriteRoutes(
        const std::vector<ObjectKey>& keys, const std::vector<size_t>& sizes,
        const WriteRouteRequestConfig& config);

    struct WriteOp {
        virtual ~WriteOp() = default;
        virtual std::string_view route() const = 0;
        virtual std::unique_ptr<TaskHandle<void>> Dispatch() = 0;
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
        using TeTransferFn = std::function<tl::expected<void, ErrorCode>(
            void* local_base, size_t size,
            const std::vector<RemoteBufferDesc>& dest_buffers)>;

        PeerClient* peer_ptr;
        std::shared_ptr<P2PClientMetric> metrics;
        std::shared_ptr<RemoteWriteRequest> write_req;
        std::string endpoint;
        std::vector<Slice>* slices;
        TeTransferFn te_transfer;

        RemoteForwardWriteOp(PeerClient* p, std::shared_ptr<P2PClientMetric> m,
                             std::shared_ptr<RemoteWriteRequest> wr,
                             std::string ep, std::vector<Slice>* s,
                             TeTransferFn transfer)
            : peer_ptr(p),
              metrics(m),
              write_req(std::move(wr)),
              endpoint(std::move(ep)),
              slices(s),
              te_transfer(std::move(transfer)) {}

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
        P2PProxyDescriptor proxy;
        RouteCache* route_cache;
        std::string endpoint;

        RemoteReverseWriteOp(PeerClient* p,
                             std::shared_ptr<RemoteWriteRequest> wr,
                             P2PProxyDescriptor px, RouteCache* rc,
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
                  size_t object_size, const WriteRouteRequestConfig& config,
                  std::vector<WriteCandidate> candidates);

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
        P2PProxyDescriptor proxy;
    };

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

    tl::expected<ReadTaskHandle, ErrorCode> InnerGetViaRoute(
        std::string_view key, std::vector<Slice>& slices, RouteIterator iter);

    async_simple::coro::Lazy<void> RunReadWithRetry(
        RouteIterator iter, std::shared_ptr<RemoteReadRequest> req,
        std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
            promise);

    async_simple::coro::Lazy<ErrorCode> RunForwardReadOnRoute(
        const ResolvedRoute& route, std::shared_ptr<RemoteReadRequest> req,
        std::shared_ptr<async_simple::Promise<tl::expected<void, ErrorCode>>>
            promise);

    async_simple::coro::Lazy<std::vector<ResolvedRoute>>
    AsyncResolveRoutesFromMaster(std::string_view key,
                                 const ReadRouteConfig& config);

    PeerClient& GetOrCreatePeerClient(const std::string& endpoint);

   private:
    tl::expected<size_t, ErrorCode> GetLocalKeyCount();
    tl::expected<std::vector<std::string>, ErrorCode> GetLocalKeys(
        size_t limit = 0);

   private:
    std::shared_ptr<P2PClientMetric> metrics_;
    // Attach a SYNC_CLIENT_METRIC task every METRIC_SYNC_FREQ heartbeats.
    static constexpr int METRIC_SYNC_FREQ = 10;
    // Heartbeats since the last SYNC_CLIENT_METRIC task.
    int metric_sync_heartbeat_count_ = 0;
    P2PMasterClient master_client_;
    uint16_t client_rpc_port_ = 12345;

    std::unique_ptr<coro_rpc::coro_rpc_server> client_rpc_server_;
    std::thread client_rpc_server_thread_;
    std::optional<DataManager> data_manager_;
    std::optional<ClientRpcService> client_rpc_service_;

    std::mutex peer_clients_mutex_;
    std::map<std::string, std::unique_ptr<PeerClient>> peer_clients_;

    std::optional<RouteCache> route_cache_;

    std::unique_ptr<AsyncMetadataNotifier> async_route_notifier_;

    std::unique_ptr<HARecoveryManager> ha_manager_;

    TransferDirectionMode transfer_direction_mode_ =
        TransferDirectionMode::REVERSE;

    std::unique_ptr<MasterViewHelper> master_view_helper_;
    std::string master_view_helper_entry_;
    ClientMasterDiscoveryConfig master_discovery_config_;
    std::thread heartbeat_thread_;
    std::atomic<bool> heartbeat_running_{false};
    std::condition_variable heartbeat_cv_;
    std::mutex heartbeat_mtx_;
    std::atomic<ViewVersionId> view_version_{0};
    bool connection_interrupted_ = false;
    std::string master_server_entry_;
};

}  // namespace mooncake