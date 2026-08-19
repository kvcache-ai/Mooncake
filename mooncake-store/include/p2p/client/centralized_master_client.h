#pragma once

#include "master_client.h"
#include "p2p/master_client_interface.h"
#include <memory>
#include <string>
#include <vector>

namespace mooncake {

class CentralizedMasterClient final : public MasterClientInterface {
public:
    explicit CentralizedMasterClient(std::shared_ptr<MasterClient> client)
        : master_client_(std::move(client)) {}

    CentralizedMasterClient(const CentralizedMasterClient&) = delete;
    CentralizedMasterClient& operator=(const CentralizedMasterClient&) = delete;

    // ---- MasterClientInterface methods ----

    tl::expected<void, ErrorCode> Connect(
        const std::string& master_addr) override {
        auto result = master_client_->Connect(master_addr);
        if (result != ErrorCode::OK) return tl::unexpected(result);
        return {};
    }

    tl::expected<std::vector<std::string>, ErrorCode> BatchQueryIp(
        const std::vector<UUID>& client_ids) override {
        auto result = master_client_->BatchQueryIp(client_ids);
        if (!result) return tl::unexpected(result.error());
        std::vector<std::string> ips;
        for (const auto& [id, ip_list] : result.value()) {
            ips.insert(ips.end(), ip_list.begin(), ip_list.end());
        }
        return ips;
    }

    tl::expected<std::vector<Replica>, ErrorCode> GetReplicaListByRegex(
        const std::string& str) override {
        auto result = master_client_->GetReplicaListByRegex(str);
        if (!result) return tl::unexpected(result.error());
        std::vector<Replica> replicas;
        for (auto& [key, descriptors] : result.value()) {
            for (auto& desc : descriptors) {
                replicas.push_back(DescriptorToReplica(std::move(desc)));
            }
        }
        return replicas;
    }

    CacheStats CalcCacheStats() override {
        auto result = master_client_->CalcCacheStats();
        if (!result) return {};
        CacheStats stats;
        for (const auto& [k, v] : result.value()) {
            switch (k) {
            case MasterMetricManager::CacheHitStat::MEMORY_HITS:
            case MasterMetricManager::CacheHitStat::SSD_HITS:
                stats.cache_hit += static_cast<uint64_t>(v);
                break;
            case MasterMetricManager::CacheHitStat::MEMORY_TOTAL:
            case MasterMetricManager::CacheHitStat::SSD_TOTAL:
                stats.cache_miss += static_cast<uint64_t>(v);
                break;
            default:
                break;
            }
        }
        return stats;
    }

    // ---- Main MasterClient methods ----

    tl::expected<PingResponse, ErrorCode> Ping() {
        return master_client_->Ping();
    }

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const std::string& key, const std::vector<size_t>& slice_lengths,
        const ReplicateConfig& config) {
        return master_client_->PutStart(key, slice_lengths, config);
    }

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const std::vector<std::string>& keys,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config) {
        return master_client_->BatchPutStart(keys, slice_lengths, config);
    }

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> UpsertStart(
        const std::string& key, const std::vector<size_t>& slice_lengths,
        const ReplicateConfig& config) {
        return master_client_->UpsertStart(key, slice_lengths, config);
    }

    tl::expected<void, ErrorCode> UpsertEnd(const ObjectMeta& object_meta,
                                            ReplicaType replica_type) {
        return master_client_->UpsertEnd(object_meta, replica_type);
    }

    tl::expected<void, ErrorCode> UpsertRevoke(const std::string& key,
                                               ReplicaType replica_type) {
        return master_client_->UpsertRevoke(key, replica_type);
    }

    tl::expected<void, ErrorCode> PutEnd(const ObjectMeta& object_meta,
                                         ReplicaType replica_type) {
        return master_client_->PutEnd(object_meta, replica_type);
    }

    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<ObjectMeta>& object_metas,
        ReplicaType replica_type = ReplicaType::ALL) {
        return master_client_->BatchPutEnd(object_metas, replica_type);
    }

    tl::expected<void, ErrorCode> PutRevoke(const std::string& key,
                                            ReplicaType replica_type) {
        return master_client_->PutRevoke(key, replica_type);
    }

    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys,
        ReplicaType replica_type = ReplicaType::ALL) {
        return master_client_->BatchPutRevoke(keys, replica_type);
    }

    tl::expected<std::vector<std::string>, ErrorCode> BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name) {
        return master_client_->BatchReplicaClear(object_keys, client_id,
                                                 segment_name);
    }

    tl::expected<std::string, ErrorCode> GetFsdir() {
        return master_client_->GetFsdir();
    }

    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig() {
        return master_client_->GetStorageConfig();
    }

    tl::expected<void, ErrorCode> MountLocalDiskSegment(
        const UUID& client_id, bool enable_offloading) {
        return master_client_->MountLocalDiskSegment(client_id,
                                                     enable_offloading);
    }

    tl::expected<std::vector<OffloadTaskItem>, ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading) {
        return master_client_->OffloadObjectHeartbeat(client_id,
                                                      enable_offloading);
    }

    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas) {
        return master_client_->NotifyOffloadSuccess(client_id, keys, metadatas);
    }

    tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const std::string& key, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments) {
        return master_client_->CopyStart(key, src_segment, tgt_segments);
    }

    tl::expected<void, ErrorCode> CopyEnd(const std::string& key) {
        return master_client_->CopyEnd(key);
    }

    tl::expected<void, ErrorCode> CopyRevoke(const std::string& key) {
        return master_client_->CopyRevoke(key);
    }

    tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const std::string& key, const std::string& src_segment,
        const std::string& tgt_segment) {
        return master_client_->MoveStart(key, src_segment, tgt_segment);
    }

    tl::expected<void, ErrorCode> MoveEnd(const std::string& key) {
        return master_client_->MoveEnd(key);
    }

    tl::expected<void, ErrorCode> MoveRevoke(const std::string& key) {
        return master_client_->MoveRevoke(key);
    }

    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets) {
        return master_client_->CreateCopyTask(key, targets);
    }

    tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& source,
        const std::string& target) {
        return master_client_->CreateMoveTask(key, source, target);
    }

    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id) {
        return master_client_->QueryTask(task_id);
    }

    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        size_t batch_size) {
        return master_client_->FetchTasks(batch_size);
    }

    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete) {
        return master_client_->MarkTaskToComplete(task_complete);
    }

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& object_key) {
        return master_client_->GetReplicaList(object_key);
    }

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& object_key, const std::string& tenant_id) {
        return master_client_->GetReplicaList(object_key, tenant_id);
    }

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& object_keys) {
        return master_client_->BatchGetReplicaList(object_keys);
    }

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& object_keys,
                        const std::string& tenant_id) {
        return master_client_->BatchGetReplicaList(object_keys, tenant_id);
    }

    tl::expected<bool, ErrorCode> ExistKey(const std::string& object_key) {
        return master_client_->ExistKey(object_key);
    }

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& object_keys) {
        return master_client_->BatchExistKey(object_keys);
    }

    tl::expected<void, ErrorCode> Remove(const std::string& key,
                                         bool force = false) {
        return master_client_->Remove(key, force);
    }

    tl::expected<long, ErrorCode> RemoveByRegex(const std::string& str,
                                                bool force = false) {
        return master_client_->RemoveByRegex(str, force);
    }

    tl::expected<long, ErrorCode> RemoveAll(bool force = false) {
        return master_client_->RemoveAll(force);
    }

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment) {
        return master_client_->MountSegment(segment);
    }

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id) {
        return master_client_->UnmountSegment(segment_id);
    }

    const std::string& tenant_id() const { return master_client_->tenant_id(); }

    MasterClient& get() { return *master_client_; }

    std::vector<tl::expected<void, ErrorCode>> BatchRemove(
        const std::vector<std::string>& keys, bool force = false) {
        return master_client_->BatchRemove(keys, force);
    }

    tl::expected<void, ErrorCode> EvictDiskReplica(
        const std::string& key, ReplicaType replica_type) {
        return master_client_->EvictDiskReplica(key, replica_type);
    }

    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const std::vector<std::string>& keys, ReplicaType replica_type) {
        return master_client_->BatchEvictDiskReplica(keys, replica_type);
    }

    tl::expected<void, ErrorCode> ReportSsdCapacity(
        const UUID& client_id, int64_t ssd_total_capacity_bytes) {
        return master_client_->ReportSsdCapacity(client_id,
                                                  ssd_total_capacity_bytes);
    }

    tl::expected<std::vector<PromotionTaskItem>, ErrorCode>
    PromotionObjectHeartbeat(const UUID& client_id) {
        return master_client_->PromotionObjectHeartbeat(client_id);
    }

    tl::expected<PromotionAllocStartResponse, ErrorCode> PromotionAllocStart(
        const UUID& client_id, const std::string& key, uint64_t size,
        const std::vector<std::string>& preferred_segments) {
        return master_client_->PromotionAllocStart(client_id, key, size,
                                                    preferred_segments);
    }

    tl::expected<void, ErrorCode> NotifyPromotionSuccess(
        const UUID& client_id, const std::string& key) {
        return master_client_->NotifyPromotionSuccess(client_id, key);
    }

    tl::expected<void, ErrorCode> NotifyPromotionFailure(
        const UUID& client_id, const std::string& key) {
        return master_client_->NotifyPromotionFailure(client_id, key);
    }

    tl::expected<bool, ErrorCode> PollRemoveAll() {
        return master_client_->PollRemoveAll();
    }

private:
    static Replica DescriptorToReplica(Replica::Descriptor&& desc) {
        auto status = desc.status;
        return std::visit(
            [status](auto&& variant) -> Replica {
                using T = std::decay_t<decltype(variant)>;
                if constexpr (std::is_same_v<T, MemoryDescriptor> ||
                              std::is_same_v<T, NoFDescriptor>) {
                    auto alloc = std::make_shared<DummyBufferAllocator>(
                        "", "");
                    auto buf = std::make_unique<AllocatedBuffer>(
                        std::move(alloc),
                        reinterpret_cast<void*>(
                            variant.buffer_descriptor.buffer_address_),
                        variant.buffer_descriptor.size_);
                    if constexpr (std::is_same_v<T, MemoryDescriptor>) {
                        return Replica(std::move(buf), status);
                    } else {
                        return Replica(std::move(buf), status,
                                       ReplicaType::NOF_SSD);
                    }
                } else if constexpr (std::is_same_v<T, DiskDescriptor>) {
                    return Replica(variant.file_path, variant.object_size,
                                   status);
                } else if constexpr (std::is_same_v<T, LocalDiskDescriptor>) {
                    return Replica(variant.client_id, variant.object_size,
                                   variant.transport_endpoint, status);
                } else if constexpr (std::is_same_v<T,
                                                     DistributedFSDescriptor>) {
                    return Replica(variant, status);
                } else if constexpr (std::is_same_v<T, P2PProxyDescriptor>) {
                    P2PProxyReplicaData proxy_data;
                    proxy_data.object_size = variant.object_size;
                    return Replica(std::move(proxy_data), status);
                }
            },
            std::move(desc.descriptor_variant));
    }

    std::shared_ptr<MasterClient> master_client_;
};

}  // namespace mooncake