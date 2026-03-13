#pragma once

#include "client_service.h"
#include "centralized_master_client.h"
#include "storage_backend.h"
#include "file_storage.h"
#include "transfer_task.h"
#include "thread_pool.h"
#include <chrono>

namespace mooncake {

class PutOperation;
class FileStorage;

/**
 * @brief Centralized-specific query result with lease timeout information
 */
class CentralizedQueryResult final : public QueryResult {
   public:
    /** @brief Time point when the lease for this key expires */
    const std::chrono::steady_clock::time_point lease_timeout;

    CentralizedQueryResult(
        std::vector<Replica::Descriptor>&& replicas_param,
        std::chrono::steady_clock::time_point lease_timeout_param)
        : QueryResult(std::move(replicas_param)),
          lease_timeout(lease_timeout_param) {}

    bool IsLeaseExpired() const {
        return std::chrono::steady_clock::now() >= lease_timeout;
    }

    bool IsLeaseExpired(std::chrono::steady_clock::time_point& now) const {
        return now >= lease_timeout;
    }
};

class CentralizedClientService
    : public ClientService,
      public std::enable_shared_from_this<CentralizedClientService> {
   public:
    CentralizedClientService(
        const std::string& local_ip, uint16_t te_port,
        const std::string& metadata_connstring,
        const std::map<std::string, std::string>& labels = {});

    ~CentralizedClientService() override;

    ErrorCode Init(const CentralizedClientConfig& config);
    void Stop() override;
    void Destroy() override;

    tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key,
        const ReadRouteConfig& config = {}) override;

    std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) override;

    DeploymentMode deployment_mode() const override {
        return DeploymentMode::CENTRALIZATION;
    }

    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name);

    tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                      const QueryResult& query_result,
                                      std::vector<Slice>& slices) override;

    tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                      std::vector<Slice>& slices);

    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& object_keys,
        const std::vector<std::unique_ptr<QueryResult>>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices,
        bool prefer_same_node = false) override;

    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& object_keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const WriteConfig& config) override;

    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) override;

    tl::expected<void, ErrorCode> Remove(const ObjectKey& key) override;

    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str) override;

    tl::expected<long, ErrorCode> RemoveAll() override;

    tl::expected<void, ErrorCode> MountSegment(const void* buffer,
                                               size_t size) override;

    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size) override;

    tl::expected<void, ErrorCode> MountLocalDiskSegment(bool enable_offloading);

    tl::expected<void, ErrorCode> OffloadObjectHeartbeat(
        bool enable_offloading,
        std::unordered_map<std::string, int64_t>& offloading_objects);

    tl::expected<void, ErrorCode> BatchPutOffloadObject(
        const std::string& transfer_engine_addr,
        const std::vector<std::string>& keys,
        const std::vector<uintptr_t>& pointers,
        const std::unordered_map<std::string, Slice>& batched_slices);

    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas);

    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient() override;

   protected:
    HeartbeatRequest build_heartbeat_request() override;

    MasterClient& GetMasterClient() override { return master_client_; }

   private:
    void InitTransferSubmitter();

    std::vector<tl::expected<void, ErrorCode>> BatchGetWhenPreferSameNode(
        const std::vector<std::string>& object_keys,
        const std::vector<std::unique_ptr<QueryResult>>& query_results,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    std::vector<PutOperation> CreatePutOperations(
        const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>& batched_slices);
    void StartBatchPut(std::vector<PutOperation>& ops,
                       const ReplicateConfig& config);
    void SubmitTransfers(std::vector<PutOperation>& ops);
    void WaitForTransfers(std::vector<PutOperation>& ops);
    void FinalizeBatchPut(std::vector<PutOperation>& ops);
    std::vector<tl::expected<void, ErrorCode>> CollectResults(
        const std::vector<PutOperation>& ops);

    std::vector<tl::expected<void, ErrorCode>> BatchPutWhenPreferSameNode(
        std::vector<PutOperation>& ops);

    void PrepareStorageBackend(const std::string& storage_root_dir,
                               const std::string& fsdir,
                               bool enable_eviction = true,
                               uint64_t quota_bytes = 0);

    void PutToLocalFile(const std::string& object_key,
                        const std::vector<Slice>& slices,
                        const DiskDescriptor& disk_descriptor);

    ErrorCode TransferData(const Replica::Descriptor& replica_descriptor,
                           std::vector<Slice>& slices,
                           TransferRequest::OpCode op_code);
    ErrorCode TransferWrite(const Replica::Descriptor& replica_descriptor,
                            std::vector<Slice>& slices);
    ErrorCode TransferRead(const Replica::Descriptor& replica_descriptor,
                           std::vector<Slice>& slices);

    tl::expected<void, ErrorCode> InnerUnmountSegment(const void* buffer,
                                                      size_t size);

    ErrorCode FindFirstCompleteReplica(
        const std::vector<Replica::Descriptor>& replica_list,
        Replica::Descriptor& replica);

   private:
    CentralizedMasterClient master_client_;
    std::unique_ptr<TransferSubmitter> transfer_submitter_;

    // Mutex to protect mounted_segments_
    std::mutex mounted_segments_mutex_;
    std::unordered_map<UUID, Segment, boost::hash<UUID>> mounted_segments_;

    // File storage for offloading
    std::shared_ptr<FileStorage> file_storage_;

    // Client persistent thread pool for async operations
    ThreadPool write_thread_pool_;
    std::shared_ptr<StorageBackend> storage_backend_;
};

}  // namespace mooncake
