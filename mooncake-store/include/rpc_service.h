#pragma once

#include <csignal>

#include <string>
#include <boost/functional/hash.hpp>
#include <cstdint>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/struct_pack.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_service.h"
#include "types.h"
#include "rpc_types.h"
#include "master_config.h"
#include "segment.h"

namespace mooncake {

class WrappedMasterService {
   public:
    WrappedMasterService(const WrappedMasterServiceConfig& config);

    ~WrappedMasterService();

    tl::expected<bool, ErrorCode> ExistKey(const std::string& key);

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys);

    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name);

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& key);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys);

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const UUID& client_id, const std::string& key,
        const uint64_t slice_length, const ReplicateConfig& config,
        struct_pack::compatible<std::string> tenant_id = {});

    tl::expected<void, ErrorCode> PutEnd(
        const UUID& client_id, const std::string& key,
        ReplicaType replica_type = ReplicaType::ALL,
        struct_pack::compatible<std::string> tenant_id = {});

    tl::expected<void, ErrorCode> PutRevoke(
        const UUID& client_id, const std::string& key,
        ReplicaType replica_type = ReplicaType::ALL,
        struct_pack::compatible<std::string> tenant_id = {});

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const UUID& client_id, const std::vector<std::string>& keys,
                  const std::vector<uint64_t>& slice_lengths,
                  const ReplicateConfig& config,
                  struct_pack::compatible<std::string> tenant_id = {});

    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<std::string>& keys,
        ReplicaType replica_type = ReplicaType::ALL,
        struct_pack::compatible<std::string> tenant_id = {});

    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        ReplicaType replica_type = ReplicaType::ALL,
        struct_pack::compatible<std::string> tenant_id = {});

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> UpsertStart(
        const UUID& client_id, const std::string& key,
        const uint64_t slice_length, const ReplicateConfig& config,
        struct_pack::compatible<std::string> tenant_id = {});

    tl::expected<void, ErrorCode> UpsertEnd(
        const UUID& client_id, const std::string& key,
        ReplicaType replica_type = ReplicaType::ALL,
        struct_pack::compatible<std::string> tenant_id = {});

    tl::expected<void, ErrorCode> UpsertRevoke(
        const UUID& client_id, const std::string& key,
        ReplicaType replica_type = ReplicaType::ALL,
        struct_pack::compatible<std::string> tenant_id = {});

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchUpsertStart(const UUID& client_id,
                     const std::vector<std::string>& keys,
                     const std::vector<uint64_t>& slice_lengths,
                     const ReplicateConfig& config,
                     struct_pack::compatible<std::string> tenant_id = {});

    std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const UUID& client_id, const std::vector<std::string>& keys,
        struct_pack::compatible<std::string> tenant_id = {});

    std::vector<tl::expected<void, ErrorCode>> BatchUpsertRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        struct_pack::compatible<std::string> tenant_id = {});

    tl::expected<void, ErrorCode> Remove(
        const std::string& key, bool force = false,
        struct_pack::compatible<std::string> tenant_id = {});

    tl::expected<long, ErrorCode> RemoveByRegex(
        const std::string& str, bool force = false,
        struct_pack::compatible<std::string> tenant_id = {});

    long RemoveAll(bool force = false);

    std::vector<tl::expected<void, ErrorCode>> BatchRemove(
        const std::vector<std::string>& keys, bool force = false,
        struct_pack::compatible<std::string> tenant_id = {});

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment,
                                               const UUID& client_id);

    tl::expected<void, ErrorCode> MountNoFSegment(const NoFSegment& segment,
                                                  const UUID& client_id);

    tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id);

    tl::expected<void, ErrorCode> ReMountNoFSegment(
        const std::vector<NoFSegment>& segments, const UUID& client_id);

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);

    tl::expected<void, ErrorCode> GracefulUnmountSegment(
        const UUID& segment_id, const UUID& client_id,
        uint64_t grace_period_ms);

    tl::expected<void, ErrorCode> UnmountNoFSegment(const UUID& segment_id,
                                                    const UUID& client_id);

    [[nodiscard]] tl::expected<std::vector<NoFSegment>, ErrorCode>
    GetAllNoFSegments();

    [[nodiscard]] tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode>
    GetNoFSegmentsByName(const std::string& segment_name);

    tl::expected<std::string, ErrorCode> GetFsdir();

    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig();

    tl::expected<PingResponse, ErrorCode> Ping(const UUID& client_id);

    tl::expected<std::string, ErrorCode> ServiceReady();

    tl::expected<std::vector<std::string>, ErrorCode> GetAllKeysForAdmin();

    tl::expected<std::vector<std::string>, ErrorCode> GetAllSegmentsForAdmin();

    tl::expected<std::vector<MasterService::SegmentDetailInfo>, ErrorCode>
    GetSegmentsDetailForAdmin();

    tl::expected<std::pair<uint64_t, uint64_t>, ErrorCode> QuerySegmentForAdmin(
        const std::string& segment);

    tl::expected<void, ErrorCode> MountLocalDiskSegment(const UUID& client_id,
                                                        bool enable_offloading);

    tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading);

    tl::expected<void, ErrorCode> ReportSsdCapacity(
        const UUID& client_id, int64_t ssd_total_capacity_bytes);

    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas);

    // Promotion-on-hit RPCs.
    tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
    PromotionObjectHeartbeat(const UUID& client_id);

    tl::expected<PromotionAllocStartResponse, ErrorCode> PromotionAllocStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, uint64_t size,
        const std::vector<std::string>& preferred_segments);

    tl::expected<void, ErrorCode> NotifyPromotionSuccess(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id);

    tl::expected<void, ErrorCode> NotifyPromotionFailure(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id);

    tl::expected<UUID, ErrorCode> CreateDrainJob(
        const CreateDrainJobRequest& request);

    tl::expected<QueryJobResponse, ErrorCode> QueryDrainJob(const UUID& job_id);

    tl::expected<void, ErrorCode> CancelDrainJob(const UUID& job_id);

    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatus(
        const std::string& segment_name);
    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatusById(
        const UUID& segment_id);
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::string& tenant_id,
        const std::vector<std::string>& targets);

    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const std::string& tenant_id,
                                                 const std::string& source,
                                                 const std::string& target);

    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id);

    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        const UUID& client_id, size_t batch_size);

    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const UUID& client_id, const TaskCompleteRequest& request);

    tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments);

    tl::expected<void, ErrorCode> CopyEnd(const UUID& client_id,
                                          const std::string& key,
                                          const std::string& tenant_id);

    tl::expected<void, ErrorCode> CopyRevoke(const UUID& client_id,
                                             const std::string& key,
                                             const std::string& tenant_id);

    tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::string& tgt_segment);

    tl::expected<void, ErrorCode> MoveEnd(const UUID& client_id,
                                          const std::string& key,
                                          const std::string& tenant_id);

    tl::expected<void, ErrorCode> MoveRevoke(const UUID& client_id,
                                             const std::string& key,
                                             const std::string& tenant_id);

    tl::expected<void, ErrorCode> EvictDiskReplica(const UUID& client_id,
                                                   const std::string& key,
                                                   const std::string& tenant_id,
                                                   ReplicaType replica_type);

    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id, ReplicaType replica_type);

   private:
    MasterService master_service_;
};

void RegisterRpcService(coro_rpc::coro_rpc_server& server,
                        mooncake::WrappedMasterService& wrapped_master_service);

}  // namespace mooncake
