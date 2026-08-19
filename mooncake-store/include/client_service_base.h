#pragma once

#include <csignal>
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
#include <chrono>
#include <map>

#include "client_metric.h"
#include "ha_helper.h"
#include "inflight_tracker.h"
#include "mutex.h"
#include "transfer_engine.h"
#include "types.h"
#include "p2p/p2p_rpc_types.h"
#include "rpc_types.h"
#include "replica.h"
#include "master_client_interface.h"
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#include "p2p/client_config_builder.h"
#include "client_buffer.h"
#include "runtime_config_store.h"

namespace mooncake {

using WriteConfig = std::variant<ReplicateConfig, WriteRouteRequestConfig>;

class LocalHotCache;
class DistributedStorageBackend;

class QueryResult {
   public:
    const std::vector<Replica::Descriptor> replicas;
    const std::chrono::steady_clock::time_point lease_timeout;
    const std::optional<uint64_t> object_checksum;

    explicit QueryResult(
        std::vector<Replica::Descriptor>&& replicas_param,
        std::chrono::steady_clock::time_point lease_timeout_param =
            std::chrono::steady_clock::time_point::max(),
        std::optional<uint64_t> object_checksum_param = std::nullopt)
        : replicas(std::move(replicas_param)),
          lease_timeout(lease_timeout_param),
          object_checksum(object_checksum_param) {}

    virtual ~QueryResult() = default;

    QueryResult(const QueryResult&) = default;
    QueryResult& operator=(const QueryResult&) = default;
    QueryResult(QueryResult&&) = default;
    QueryResult& operator=(QueryResult&&) = default;

    bool IsLeaseExpired() const {
        return std::chrono::steady_clock::now() >= lease_timeout;
    }

    bool IsLeaseExpired(std::chrono::steady_clock::time_point now) const {
        return now >= lease_timeout;
    }
};

class RouteIterator;

class ClientService {
   public:
    virtual ~ClientService();

    using InflightRequestGuard = InflightTracker::Guard;

    virtual void Stop();

    virtual void StopHeartbeat() {}

    virtual void Destroy();

    static std::optional<std::shared_ptr<ClientService>> Create(
        const CentralizedClientConfig& config);
    static std::optional<std::shared_ptr<ClientService>> Create(
        const P2PClientConfig& config);

    virtual DeploymentMode deployment_mode() const = 0;

    virtual tl::expected<std::vector<std::string>, ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    virtual tl::expected<std::vector<Replica>, ErrorCode>
    QueryByRegex(const std::string& str);

    virtual tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key, const ReadRouteConfig& config = {}) = 0;

    virtual std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) = 0;

    virtual tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config = {}) = 0;

    virtual std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const ReadRouteConfig& config = {}) = 0;

    virtual tl::expected<int64_t, ErrorCode> Get(
        const std::string& key, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const ReadRouteConfig& config = {}) = 0;

    virtual std::vector<tl::expected<int64_t, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const ReadRouteConfig& config = {},
        bool aggregate_same_segment_task = false) = 0;

    virtual tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                              std::vector<Slice>& slices,
                                              const WriteConfig& config) = 0;

    virtual std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) = 0;

    virtual tl::expected<void, ErrorCode> Remove(const ObjectKey& key,
                                                 bool force = false) = 0;

    virtual tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str,
                                                        bool force = false) = 0;

    virtual tl::expected<long, ErrorCode> RemoveAll(bool force = false) = 0;

    virtual tl::expected<long, ErrorCode> RemoveAllLocal() {
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    virtual tl::expected<void, ErrorCode> RemoveLocal(const ObjectKey& key) {
        (void)key;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    // ====================================================================
    // Main-line extensions.
    // Default: NOT_IMPLEMENTED. CentralizedClientService overrides these to
    // bridge main Client behavior; P2PClientService leaves the defaults,
    // matching the P2P branch semantics (these APIs do not exist there).
    // ====================================================================

    // --- Slice-based Get (main Client style) ---
    virtual tl::expected<void, ErrorCode> Get(
        const std::string& key, std::vector<Slice>& slices,
        const ReadRouteConfig& config = {}) {
        (void)key; (void)slices; (void)config;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices,
        const ReadRouteConfig& config = {}) {
        (void)keys; (void)slices; (void)config;
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }

    // --- Upsert ---
    virtual tl::expected<void, ErrorCode> Upsert(
        const ObjectKey& key, std::vector<Slice>& slices,
        const WriteConfig& config) {
        (void)key; (void)slices; (void)config;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual std::vector<tl::expected<void, ErrorCode>> BatchUpsert(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) {
        (void)keys; (void)batched_slices; (void)config;
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }

    // --- Batch remove / replica clear ---
    virtual std::vector<tl::expected<void, ErrorCode>> BatchRemove(
        const std::vector<ObjectKey>& keys, bool force = false) {
        (void)force;
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }
    virtual tl::expected<std::vector<std::string>, ErrorCode>
    BatchReplicaClear(const std::vector<std::string>& object_keys,
                      const UUID& client_id,
                      const std::string& segment_name) {
        (void)object_keys; (void)client_id; (void)segment_name;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    // --- Segment management extensions ---
    virtual tl::expected<UUID, ErrorCode> MountSegmentAndGetId(
        const void* buffer, size_t size, const std::string& protocol = "tcp",
        const std::string& location = kWildcardLocation) {
        (void)buffer; (void)size; (void)protocol; (void)location;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<void, ErrorCode> UnmountSegmentById(
        const UUID& segment_id, uint64_t grace_period_ms = 0) {
        (void)segment_id; (void)grace_period_ms;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    // --- BatchPut session ---
    virtual std::vector<tl::expected<std::vector<Replica::Descriptor>,
                                     ErrorCode>>
    StartBatchPutForSizes(const std::vector<std::string>& keys,
                          const std::vector<uint64_t>& object_sizes,
                          const ReplicateConfig& config) {
        (void)keys; (void)object_sizes; (void)config;
        return std::vector<tl::expected<std::vector<Replica::Descriptor>,
                                        ErrorCode>>(
            keys.size(),
            tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }
    virtual std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<ObjectMeta>& object_metas,
        ReplicaType replica_type = ReplicaType::ALL) {
        (void)object_metas; (void)replica_type;
        return std::vector<tl::expected<void, ErrorCode>>(
            object_metas.size(),
            tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }
    virtual std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys,
        ReplicaType replica_type = ReplicaType::ALL) {
        (void)keys; (void)replica_type;
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }

    // --- Offload / SSD ---
    virtual tl::expected<void, ErrorCode> MountLocalDiskSegment(
        bool enable_offloading) {
        (void)enable_offloading;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>
    OffloadObjectHeartbeat(bool enable_offloading) {
        (void)enable_offloading;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas) {
        (void)keys; (void)metadatas;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<void, ErrorCode> BatchGetOffloadObject(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uintptr_t>& pointers,
        const std::unordered_map<std::string, std::vector<Slice>>&
            batch_slices) {
        (void)transfer_engine_addr; (void)keys; (void)pointers;
        (void)batch_slices;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    // --- Eviction ---
    virtual tl::expected<void, ErrorCode> EvictDiskReplica(
        const std::string& key, ReplicaType replica_type) {
        (void)key; (void)replica_type;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual std::vector<tl::expected<void, ErrorCode>>
    BatchEvictDiskReplica(const std::vector<std::string>& keys,
                           ReplicaType replica_type) {
        (void)replica_type;
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }

    // --- Ranged transfer ---
    virtual ErrorCode TransferWriteRange(
        const Replica::Descriptor& replica_descriptor,
        std::vector<Slice>& slices, uint64_t dst_offset) {
        (void)replica_descriptor; (void)slices; (void)dst_offset;
        return ErrorCode::NOT_IMPLEMENTED;
    }
    virtual std::vector<tl::expected<int64_t, ErrorCode>>
    BatchTransferReadRanges(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<std::vector<Slice>>& all_slices,
        const std::vector<std::vector<uint64_t>>& src_offsets) {
        (void)replicas; (void)all_slices; (void)src_offsets;
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            replicas.size(),
            tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }
    virtual std::vector<tl::expected<int64_t, ErrorCode>>
    BatchTransferWriteRanges(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<std::vector<Slice>>& all_slices,
        const std::vector<std::vector<uint64_t>>& dst_offsets) {
        (void)replicas; (void)all_slices; (void)dst_offsets;
        return std::vector<tl::expected<int64_t, ErrorCode>>(
            replicas.size(),
            tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED));
    }

    // --- Checksum / poll ---
    virtual tl::expected<void, ErrorCode> VerifyObjectChecksum(
        const std::string& key, const std::vector<Slice>& slices,
        size_t object_size, std::optional<uint64_t> expected_checksum) {
        (void)key; (void)slices; (void)object_size; (void)expected_checksum;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<bool, ErrorCode> PollRemoveAll() {
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<void, ErrorCode> ReportSsdCapacity(
        int64_t ssd_total_capacity_bytes) {
        (void)ssd_total_capacity_bytes;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    // --- DFS ---
    virtual void SetDfsStorageBackend(
        std::shared_ptr<DistributedStorageBackend> backend) {
        (void)backend;
    }

    // --- Promotion (main new feature; P2P not supported) ---
    virtual tl::expected<void, ErrorCode> PromotionObjectHeartbeat(
        std::vector<PromotionTaskItem>& promotion_objects) {
        (void)promotion_objects;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<PromotionAllocStartResponse, ErrorCode>
    PromotionAllocStart(const std::string& key, uint64_t size,
                        const std::vector<std::string>& preferred_segments) {
        (void)key; (void)size; (void)preferred_segments;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<void, ErrorCode> NotifyPromotionSuccess(
        const std::string& key) {
        (void)key;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual tl::expected<void, ErrorCode> NotifyPromotionFailure(
        const std::string& key) {
        (void)key;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }
    virtual ErrorCode PromotionWrite(
        const Replica::Descriptor& memory_descriptor,
        std::vector<Slice>& slices) {
        (void)memory_descriptor; (void)slices;
        return ErrorCode::NOT_IMPLEMENTED;
    }

    // --- Accessors ---
    virtual void* GetBaseAddr() { return nullptr; }
    virtual std::vector<int> GetNicNumaNodes() const { return {}; }
    virtual bool CanUseLocalMemcpy() const { return false; }
    virtual std::string GetProtocol() const { return ""; }
    virtual std::string GetSegmentEndpoint() const { return te_endpoint_; }
    virtual std::vector<std::string> GetLocalEndpoints() const {
        return {te_endpoint_};
    }
    virtual bool is_ping_healthy() const { return true; }
    virtual bool IsReplicaOnLocalMemory(const Replica::Descriptor& replica) {
        (void)replica;
        return false;
    }
    virtual SsdMetric* GetSsdMetricPtr() { return nullptr; }
    virtual void ObserveTransferOperation(TransferOperationKind kind,
                                          const std::string& op_name,
                                          uint64_t bytes, uint64_t latency_us) {
        (void)kind; (void)op_name; (void)bytes; (void)latency_us;
    }
    virtual tl::expected<Replica::Descriptor, ErrorCode> GetPreferredReplica(
        const std::vector<Replica::Descriptor>& replica_list) {
        (void)replica_list;
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    // --- Hot cache (main-only; P2P unsupported) ---
    virtual bool IsHotCacheEnabled() const { return false; }
    virtual std::shared_ptr<LocalHotCache> GetHotCache() const {
        return nullptr;
    }
    virtual size_t GetLocalHotCacheBlockCount() const { return 0; }
    virtual uint8_t GetAdmissionCount(const std::string& key) const {
        (void)key;
        return 0;
    }
    virtual bool ShouldAdmitToHotCache(const std::string& key,
                                       bool cache_used) {
        (void)key; (void)cache_used;
        return false;
    }

    // --- Tenant ---
    virtual std::string tenant_id() const { return "default"; }

    virtual tl::expected<void, ErrorCode> MountSegment(
        const void* buffer, size_t size,
        const std::string& protocol = "tcp") = 0;

    virtual tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                         size_t size) = 0;

    virtual void StartKeepalive(const std::string& master_addr) = 0;

    virtual tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    virtual tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    virtual tl::expected<bool, ErrorCode> IsExist(const std::string& key) = 0;

    virtual std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys) = 0;

    virtual tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets);

    virtual tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& source,
        const std::string& target);

    virtual tl::expected<QueryTaskResponse, ErrorCode> QueryTask(
        const UUID& task_id);

    virtual tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        size_t batch_size);

    virtual tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete);

    virtual void StartMetricsReportingThread() {}
    virtual void StopMetricsReportingThread() {}

    tl::expected<std::string, ErrorCode> GetSummaryMetrics() {
        ClientMetric* metrics = GetMetrics();
        if (metrics == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return metrics->summary_metrics();
    }

CacheStats CalcCacheStats() {
        auto guard = AcquireInflightGuard();
        if (!guard.is_valid()) {
            LOG(ERROR) << "client is shutting down";
            return CacheStats{};
        }
        return GetMasterClient().CalcCacheStats();
    }

    tl::expected<std::string, ErrorCode> SerializeMetrics() {
        ClientMetric* metrics = GetMetrics();
        if (metrics == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        std::string str;
        metrics->serialize(str);
        return str;
    }

    uint16_t GetHttpPort() const { return http_port_; }

    bool IsHttpServerEnabled() const { return http_server_ != nullptr; }

    std::shared_ptr<ClientBufferAllocator> GetBufferAllocator() const {
        return client_buffer_allocator_;
    }

    virtual std::string GetHealthStatus() const { return "OK"; }

    RuntimeConfigStore& getRuntimeConfigStore() {
        return *runtime_config_store_;
    }
    const RuntimeConfigStore& getRuntimeConfigStore() const {
        return *runtime_config_store_;
    }

    RuntimeConfigStore::WriteConfig getDefaultWriteConfig() const {
        return runtime_config_store_->getDefaultWriteConfig();
    }
    ReadRouteConfig getDefaultReadConfig() const {
        return runtime_config_store_->getDefaultReadConfig();
    }

    std::string local_endpoint() const {
        return local_ip_ + ":" + std::to_string(te_port_);
    }

    [[nodiscard]] std::string GetTransportEndpoint() {
        return transfer_engine_->getLocalIpAndPort();
    }

    UUID GetClientID() const { return client_id_; }

    virtual uint64_t GetViewVersion() const { return 0; }

    static tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
        const void* addr, size_t length);

    [[nodiscard]] static size_t CalculateSliceSize(
        const std::vector<Slice>& slices);

    [[nodiscard]] static size_t CalculateSliceSize(
        std::span<const Slice> slices);

   protected:
    ClientService(const std::string& metadata_connstring,
                  uint16_t http_port = 9003, bool enable_http_server = true,
                  const std::map<std::string, std::string>& labels = {},
                  bool enable_metric_collection = true);

    virtual MasterClientInterface& GetMasterClient() = 0;

    virtual ClientMetric* GetMetrics() = 0;

    ErrorCode ConnectToMaster(const std::string& master_server_entry);

    ErrorCode InitTransferEngine(
        uint16_t te_port, const std::string& metadata_connstring,
        const std::string& protocol,
        const std::optional<std::string>& device_names);

   protected:
    ErrorCode InnerInitTransferEngine(
        bool auto_discover, const std::string& protocol,
        const std::optional<std::string>& device_names);

    virtual void RegisterHttpMethods();

    void RegisterRuntimeConfigHttpMethods();

    void StartHttpServer();

    void StopHttpServer();

    void InitLocalBufferAllocator(size_t pool_size, const std::string& protocol,
                                  bool use_hugepage = false);

    virtual tl::expected<RegisterClientResponse, ErrorCode>
    InnerRegisterClient() = 0;

    virtual void RecordLocalInflight(bool entering) { (void)entering; }

    virtual void OnHAEvent(HAEvent event) { (void)event; }

    void initTeEndpoint();

    const std::string& get_te_endpoint() const { return te_endpoint_; }

    InflightTracker::Guard AcquireInflightGuard() {
        return inflight_tracker_.Enter();
    }

    bool MarkShuttingDown() {
        bool initiated = inflight_tracker_.Close();
        inflight_tracker_.Wait();
        return initiated;
    }

   protected:
    const UUID client_id_;

    std::shared_ptr<TransferEngine> transfer_engine_;

    std::string local_ip_;
    uint16_t te_port_ = 0;
    std::unique_ptr<RuntimeConfigStore> runtime_config_store_;

    std::string te_endpoint_;
    std::unique_ptr<AutoPortBinder> auto_port_binder_;

    const std::string metadata_connstring_;

    std::atomic<bool> registered_{false};

    Mutex registration_mutex_;

    InflightTracker inflight_tracker_{
        "local requests", [this] { RecordLocalInflight(true); },
        [this] { RecordLocalInflight(false); }};

    std::unique_ptr<coro_http::coro_http_server> http_server_;
    uint16_t http_port_ = 0;
    bool enable_http_server_ = true;
    bool enable_metric_collection_ = true;
    int metric_report_interval_seconds_ = 60;
    std::map<std::string, std::string> labels_;

    std::shared_ptr<ClientBufferAllocator> client_buffer_allocator_;
};

}  // namespace mooncake