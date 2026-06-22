#pragma once

#include <cassert>

#include "master_service.h"
#include "metadata_backend.h"

namespace mooncake {

/**
 * @brief In-memory metadata backend that delegates to MasterService.
 *
 * NativeMetadataBackend preserves the existing in-memory sharded-metadata
 * behaviour by forwarding every MetadataBackend call to the corresponding
 * MasterService method. This class is an intermediate extraction layer:
 * future work will move the actual logic from MasterService into this class
 * so that MasterService becomes a thin RPC facade.
 *
 * Thread safety: identical to MasterService — safe for concurrent access
 * from multiple RPC threads.
 */
class NativeMetadataBackend final : public MetadataBackend {
   public:
    explicit NativeMetadataBackend(MasterService* master) : master_(master) {
        assert(master != nullptr);
    }

    // -----------------------------------------------------------------------
    // Key existence and enumeration
    // -----------------------------------------------------------------------
    auto ExistKey(const std::string& key, const std::string& tenant_id)
        -> tl::expected<bool, ErrorCode> override;

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys,
        const std::string& tenant_id) override;

    auto GetAllKeys(const std::string& tenant_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> override;

    size_t GetKeyCount() const override;

    // -----------------------------------------------------------------------
    // Replica queries
    // -----------------------------------------------------------------------
    auto GetReplicaList(const std::string& key, const std::string& tenant_id)
        -> tl::expected<GetReplicaListResponse, ErrorCode> override;

    auto GetReplicaListByRegex(const std::string& regex_pattern,
                               const std::string& tenant_id)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode> override;

    // -----------------------------------------------------------------------
    // Put lifecycle
    // -----------------------------------------------------------------------
    auto PutStart(const UUID& client_id, const std::string& key,
                  const std::string& tenant_id, const uint64_t slice_length,
                  const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> override;

    auto PutEnd(const UUID& client_id, const std::string& key,
                const std::string& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> override;

    auto AddReplica(const UUID& client_id, const std::string& key,
                    const std::string& tenant_id, Replica& replica)
        -> tl::expected<void, ErrorCode> override;

    auto PutRevoke(const UUID& client_id, const std::string& key,
                   const std::string& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> override;

    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id,
        ReplicaType replica_type = ReplicaType::ALL) override;

    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id,
        ReplicaType replica_type = ReplicaType::ALL) override;

    // -----------------------------------------------------------------------
    // Upsert lifecycle
    // -----------------------------------------------------------------------
    auto UpsertStart(const UUID& client_id, const std::string& key,
                     const std::string& tenant_id, const uint64_t slice_length,
                     const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> override;

    auto UpsertEnd(const UUID& client_id, const std::string& key,
                   const std::string& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> override;

    auto UpsertRevoke(const UUID& client_id, const std::string& key,
                      const std::string& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> override;

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchUpsertStart(const UUID& client_id,
                     const std::vector<std::string>& keys,
                     const std::string& tenant_id,
                     const std::vector<uint64_t>& slice_lengths,
                     const ReplicateConfig& config) override;

    std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id) override;

    std::vector<tl::expected<void, ErrorCode>> BatchUpsertRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id) override;

    // -----------------------------------------------------------------------
    // Disk replica eviction
    // -----------------------------------------------------------------------
    auto EvictDiskReplica(const UUID& client_id, const std::string& key,
                          const std::string& tenant_id,
                          ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> override;

    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id, ReplicaType replica_type) override;

    // -----------------------------------------------------------------------
    // Replica clear
    // -----------------------------------------------------------------------
    auto BatchReplicaClear(const std::vector<std::string>& object_keys,
                           const UUID& client_id,
                           const std::string& segment_name)
        -> tl::expected<std::vector<std::string>, ErrorCode> override;

    // -----------------------------------------------------------------------
    // Copy operations
    // -----------------------------------------------------------------------
    tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments) override;

    tl::expected<void, ErrorCode> CopyEnd(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id) override;

    tl::expected<void, ErrorCode> CopyRevoke(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id) override;

    // -----------------------------------------------------------------------
    // Move operations
    // -----------------------------------------------------------------------
    tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::string& tgt_segment) override;

    tl::expected<void, ErrorCode> MoveEnd(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id) override;

    tl::expected<void, ErrorCode> MoveRevoke(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id) override;

    // -----------------------------------------------------------------------
    // Object removal
    // -----------------------------------------------------------------------
    auto Remove(const std::string& key, const std::string& tenant_id,
                bool force = false) -> tl::expected<void, ErrorCode> override;

    auto RemoveByRegex(const std::string& str, const std::string& tenant_id,
                       bool force = false)
        -> tl::expected<long, ErrorCode> override;

    auto RemoveAll(bool force = false)
        -> tl::expected<long, ErrorCode> override;

    auto RemoveAll(const std::string& tenant_id, bool force = false)
        -> tl::expected<long, ErrorCode> override;

    auto BatchRemove(const std::vector<std::string>& keys,
                     const std::string& tenant_id, bool force = false)
        -> std::vector<tl::expected<void, ErrorCode>> override;

    // -----------------------------------------------------------------------
    // Segment management
    // -----------------------------------------------------------------------
    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

    auto MountNoFSegment(const NoFSegment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

    auto ReMountSegment(const std::vector<Segment>& segments,
                        const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

    auto ReMountNoFSegment(const std::vector<NoFSegment>& segments,
                           const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

    auto GracefulUnmountSegment(const UUID& segment_id, const UUID& client_id,
                                uint64_t grace_period_ms)
        -> tl::expected<void, ErrorCode> override;

    auto UnmountNoFSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

    auto MountLocalDiskSegment(const UUID& client_id, bool enable_offloading)
        -> tl::expected<void, ErrorCode> override;

    auto GetAllSegments()
        -> tl::expected<std::vector<std::string>, ErrorCode> override;

    auto GetAllNoFSegments()
        -> tl::expected<std::vector<NoFSegment>, ErrorCode> override;

    auto GetNoFSegmentsByName(const std::string& segment_name)
        -> tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode> override;

    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode> override;

    auto GetSegmentsDetail()
        -> tl::expected<std::vector<SegmentDetailInfo>, ErrorCode> override;

    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatus(
        const std::string& segment_name) override;

    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatusById(
        const UUID& segment_id) override;

    // -----------------------------------------------------------------------
    // Client identity and IP lookup
    // -----------------------------------------------------------------------
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> override;

    auto BatchQueryIp(const std::vector<UUID>& client_ids)
        -> tl::expected<
            std::unordered_map<UUID, std::vector<std::string>, UUIDHash>,
            ErrorCode> override;

    // -----------------------------------------------------------------------
    // Client heartbeat
    // -----------------------------------------------------------------------
    auto Ping(const UUID& client_id)
        -> tl::expected<PingResponse, ErrorCode> override;

    // -----------------------------------------------------------------------
    // Configuration queries
    // -----------------------------------------------------------------------
    tl::expected<std::string, ErrorCode> GetFsdir() const override;

    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig()
        const override;

    // -----------------------------------------------------------------------
    // Offload workflow
    // -----------------------------------------------------------------------
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode> override;

    auto ReportSsdCapacity(const UUID& client_id,
                           int64_t ssd_total_capacity_bytes)
        -> tl::expected<void, ErrorCode> override;

    auto NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas)
        -> tl::expected<void, ErrorCode> override;

    // -----------------------------------------------------------------------
    // Promotion workflow
    // -----------------------------------------------------------------------
    auto PromotionObjectHeartbeat(const UUID& client_id)
        -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode> override;

    auto PromotionAllocStart(const UUID& client_id, const std::string& key,
                             const std::string& tenant_id, uint64_t size,
                             const std::vector<std::string>& preferred_segments)
        -> tl::expected<PromotionAllocStartResponse, ErrorCode> override;

    auto NotifyPromotionSuccess(const UUID& client_id, const std::string& key,
                                const std::string& tenant_id)
        -> tl::expected<void, ErrorCode> override;

    auto NotifyPromotionFailure(const UUID& client_id, const std::string& key,
                                const std::string& tenant_id)
        -> tl::expected<void, ErrorCode> override;

    // -----------------------------------------------------------------------
    // Replication tasks
    // -----------------------------------------------------------------------
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::string& tenant_id,
        const std::vector<std::string>& targets) override;

    tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& tenant_id,
        const std::string& source, const std::string& target) override;

    tl::expected<UUID, ErrorCode> CreateDrainJob(
        const CreateDrainJobRequest& request) override;

    tl::expected<QueryJobResponse, ErrorCode> QueryDrainJob(
        const UUID& job_id) override;

    tl::expected<void, ErrorCode> CancelDrainJob(
        const UUID& job_id) override;

    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(
        const UUID& task_id) override;

    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        const UUID& client_id, size_t batch_size) override;

    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const UUID& client_id, const TaskCompleteRequest& request) override;

   private:
    MasterService* master_;  // Non-owning; must outlive this object.
};

}  // namespace mooncake
