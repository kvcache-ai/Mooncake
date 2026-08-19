#pragma once

#include "client_service_base.h"
#include "p2p/client/centralized_master_client.h"
#include "client_metric.h"
#include "master_metric_manager.h"
#include "local_hot_cache.h"
#include "storage_backend.h"
#include "transfer_task.h"
#include "thread_pool.h"
#include "rpc_types.h"
#include "mutex.h"
#include "utils.h"
#include <chrono>
#include <atomic>
#include <condition_variable>

namespace mooncake {

class DistributedStorageBackend;

class CentralizedClientService
    : public ClientService,
      public std::enable_shared_from_this<CentralizedClientService> {
public:
    CentralizedClientService(
        const std::string& metadata_connstring, const std::string& protocol,
        uint16_t http_port = 9003, bool enable_http_server = true,
        const std::map<std::string, std::string>& labels = {},
        bool enable_metric_collection = true);

    ~CentralizedClientService() override;

    static std::optional<std::shared_ptr<CentralizedClientService>> Create(
        const CentralizedClientConfig& config);

    ErrorCode Init(const CentralizedClientConfig& config);
    void Stop() override;
    void Destroy() override;

    void StopHeartbeat() override;

    void StartKeepalive(const std::string& master_addr) override;

    DeploymentMode deployment_mode() const override {
        return DeploymentMode::CENTRALIZATION;
    }

    tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) override;

    tl::expected<bool, ErrorCode> IsExist(const std::string& key) override;

    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys) override;

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

    tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const ReadRouteConfig& config = {}) override;

    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const WriteConfig& config) override;

    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) override;

    tl::expected<void, ErrorCode> Remove(const ObjectKey& key,
                                         bool force = false) override;

    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str,
                                                bool force = false) override;

    tl::expected<long, ErrorCode> RemoveAll(bool force = false) override;

    tl::expected<void, ErrorCode> MountSegment(
        const void* buffer, size_t size,
        const std::string& protocol = "tcp") override;

    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size) override;

    MasterClientInterface& GetMasterClient() override { return *master_client_; }

    ClientMetric* GetMetrics() override { return metrics_.get(); }

    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key,
        const std::vector<std::string>& targets) override;

    tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& source,
        const std::string& target) override;

    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(
        const UUID& task_id) override;

    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        size_t batch_size) override;

    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete) override;

// Mount local disk segment for offloading
    tl::expected<void, ErrorCode> MountLocalDiskSegment(bool enable_offloading) override;

    // Offload object heartbeat
    tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>
    OffloadObjectHeartbeat(bool enable_offloading) override;

    // Notify offload success
    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas) override;

    // Batch replica clear
    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys,
        const UUID& client_id, const std::string& segment_name) override;

    // Poll remove all
    tl::expected<bool, ErrorCode> PollRemoveAll() override;

    // Batch get offload object (simple variant, main-only)
    tl::expected<void, ErrorCode> BatchGetOffloadObject(
        const std::vector<std::string>& keys,
        const std::vector<int64_t>& sizes);

    // Accessor methods
    void* GetBaseAddr();
    bool is_ping_healthy() const { return ping_running_.load(); }
    std::string GetTransportEndpoint() const { return te_endpoint_; }
    std::string GetProtocol() const { return protocol_; }

    // Stub methods for compatibility
    std::string GetSegmentEndpoint() const { return te_endpoint_; }
    std::vector<std::string> GetLocalEndpoints() const { return {te_endpoint_}; }
    bool CanUseLocalMemcpy() const { return false; }

    // === Value-based Get (Slice-based) ===
    tl::expected<void, ErrorCode> Get(const std::string& key,
                                       std::vector<Slice>& slices,
                                       const ReadRouteConfig& config = {}) override;
    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices,
        const ReadRouteConfig& config = {}) override;

    // === QueryByRegex ===
    tl::expected<std::vector<Replica>, ErrorCode>
    QueryByRegex(const std::string& str);

    // === VerifyChecksum ===
    tl::expected<void, ErrorCode> VerifyObjectChecksum(
        const std::string& key, const std::vector<Slice>& slices,
        size_t object_size, std::optional<uint64_t> expected_checksum) override;

    // === BatchRemove ===
    std::vector<tl::expected<void, ErrorCode>> BatchRemove(
        const std::vector<ObjectKey>& keys, bool force = false) override;

    // === Eviction ===
    tl::expected<void, ErrorCode> EvictDiskReplica(const std::string& key,
                                                    ReplicaType replica_type) override;
    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const std::vector<std::string>& keys, ReplicaType replica_type) override;

    // === Upsert ===
    tl::expected<void, ErrorCode> Upsert(const ObjectKey& key,
                                          std::vector<Slice>& slices,
                                          const WriteConfig& config) override;
    std::vector<tl::expected<void, ErrorCode>> BatchUpsert(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) override;

    // === Register/Unregister memory ===
    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);
    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    // === MountSegmentAndGetId / UnmountSegmentById ===
    tl::expected<UUID, ErrorCode> MountSegmentAndGetId(
        const void* buffer, size_t size, const std::string& protocol = "tcp",
        const std::string& location = kWildcardLocation) override;
    tl::expected<void, ErrorCode> UnmountSegmentById(
        const UUID& segment_id, uint64_t grace_period_ms = 0) override;

    // === BatchPut session methods ===
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    StartBatchPutForSizes(const std::vector<std::string>& keys,
                           const std::vector<uint64_t>& object_sizes,
                           const ReplicateConfig& config) override;
    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<ObjectMeta>& object_metas,
        ReplicaType replica_type = ReplicaType::ALL) override;
    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys,
        ReplicaType replica_type = ReplicaType::ALL) override;

    // === SubmitScatter ===
    // std::optional<TransferEngine::ScatterTransferOperation> SubmitScatter(
    //     const std::string& key, std::vector<Slice>& slices,
    //     const Replica::Descriptor& replica_descriptor, uint64_t src_offset);

    // === Transfer methods ===
    ErrorCode TransferWriteRange(const Replica::Descriptor& replica_descriptor,
                                  std::vector<Slice>& slices, uint64_t dst_offset) override;
    std::vector<tl::expected<int64_t, ErrorCode>> BatchTransferReadRanges(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<std::vector<Slice>>& all_slices,
        const std::vector<std::vector<uint64_t>>& src_offsets) override;
    std::vector<tl::expected<int64_t, ErrorCode>> BatchTransferWriteRanges(
        const std::vector<Replica::Descriptor>& replicas,
        std::vector<std::vector<Slice>>& all_slices,
        const std::vector<std::vector<uint64_t>>& dst_offsets) override;

    // === Metrics & Accessors ===
    tl::expected<std::string, ErrorCode> GetSummaryMetrics();
    CacheStats CalcCacheStats();
    tl::expected<std::string, ErrorCode> SerializeMetrics();
    void ObserveTransferOperation(TransferOperationKind kind,
                                   const std::string& op_name, uint64_t bytes,
                                   uint64_t latency_us) override;
    SsdMetric* GetSsdMetricPtr() override;
    std::vector<int> GetNicNumaNodes() const;
    tl::expected<Replica::Descriptor, ErrorCode> GetPreferredReplica(
        const std::vector<Replica::Descriptor>& replica_list) override;
    bool IsReplicaOnLocalMemory(const Replica::Descriptor& replica) override;
    tl::expected<void, ErrorCode> ReportSsdCapacity(int64_t ssd_total_capacity_bytes) override;

    // === Promotion (stubs) ===
    tl::expected<void, ErrorCode> PromotionObjectHeartbeat(
        std::vector<PromotionTaskItem>& promotion_objects) override;
    tl::expected<PromotionAllocStartResponse, ErrorCode> PromotionAllocStart(
        const std::string& key, uint64_t size,
        const std::vector<std::string>& preferred_segments) override;
    tl::expected<void, ErrorCode> NotifyPromotionSuccess(const std::string& key) override;
    tl::expected<void, ErrorCode> NotifyPromotionFailure(const std::string& key) override;
    ErrorCode PromotionWrite(const Replica::Descriptor& memory_descriptor,
                              std::vector<Slice>& slices) override;

    // === Hot cache (stubs) ===
    bool IsHotCacheEnabled() const { return false; }
    std::shared_ptr<LocalHotCache> GetHotCache() const { return nullptr; }
    size_t GetLocalHotCacheBlockCount() const { return 0; }
    uint8_t GetAdmissionCount(const std::string& key) const { return 0; }
    bool ShouldAdmitToHotCache(const std::string& key, bool cache_used) { return false; }

    // === DFS ===
    void SetDfsStorageBackend(std::shared_ptr<DistributedStorageBackend> backend) {
        dfs_storage_backend_ = std::move(backend);
    }

    // === Offload (complex version) ===
    tl::expected<void, ErrorCode> BatchGetOffloadObject(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uintptr_t>& pointers,
        const std::unordered_map<std::string, std::vector<Slice>>& batch_slices);

private:
    void InitTransferSubmitter();

    void PingThreadMain();

    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient()
        EXCLUDES(registration_mutex_);

    tl::expected<RegisterClientResponse, ErrorCode> InnerRegisterClient()
        override REQUIRES(registration_mutex_);

    ErrorCode TransferData(const Replica::Descriptor& replica_descriptor,
                           std::vector<Slice>& slices,
                           TransferRequest::OpCode op_code);
    ErrorCode TransferWrite(const Replica::Descriptor& replica_descriptor,
                            std::vector<Slice>& slices);
    ErrorCode TransferRead(const Replica::Descriptor& replica_descriptor,
                           std::vector<Slice>& slices);

    ErrorCode GetPreferredReplica(
        const std::vector<Replica::Descriptor>& replica_list,
        Replica::Descriptor& replica);

    tl::expected<void, ErrorCode> InnerUnmountSegment(const void* buffer,
                                                       size_t size);

    void PrepareStorageBackend(const std::string& storage_root_dir,
                               const std::string& fsdir,
                               bool enable_eviction = true,
                               uint64_t quota_bytes = 0);

    void PutToLocalFile(const std::string& object_key,
                        const std::vector<Slice>& slices,
                        const DiskDescriptor& disk_descriptor);

private:
    std::unique_ptr<ClientMetric> metrics_;

    const std::string protocol_;

    std::unique_ptr<CentralizedMasterClient> master_client_;

    std::unique_ptr<TransferSubmitter> transfer_submitter_;

    SharedMutex mounted_segments_mutex_ ACQUIRED_AFTER(registration_mutex_);
    std::unordered_map<UUID, Segment, boost::hash<UUID>> mounted_segments_;

    std::shared_ptr<StorageBackend> storage_backend_;

    std::shared_ptr<DistributedStorageBackend> dfs_storage_backend_;

    std::string master_server_entry_;

    struct SegmentDeleter {
        void operator()(void* ptr) {
            if (ptr) free(ptr);
        }
    };
    struct AscendSegmentDeleter {
        void operator()(void* ptr) {
            if (ptr) free_memory("ascend", ptr);
        }
    };
    struct HugepageSegmentDeleter {
        size_t size = 0;
        void operator()(void* ptr) const {
            if (ptr && size > 0) free_buffer_mmap_memory(ptr, size);
        }
    };
    std::vector<std::unique_ptr<void, SegmentDeleter>> segment_ptrs_;
    std::vector<std::unique_ptr<void, AscendSegmentDeleter>>
        ascend_segment_ptrs_;
    std::vector<std::unique_ptr<void, HugepageSegmentDeleter>>
        hugepage_segment_ptrs_;

    ThreadPool write_thread_pool_;

    // Ping thread members
    std::thread ping_thread_;
    std::atomic<bool> ping_running_{false};
    std::mutex ping_mtx_;
    std::condition_variable ping_cv_;
};

}  // namespace mooncake