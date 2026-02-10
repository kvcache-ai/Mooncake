#include "centralized_client_manager.h"
#include "centralized_client_meta.h"

#include <glog/logging.h>

namespace mooncake {
CentralizedClientManager::CentralizedClientManager(
    const int64_t client_live_ttl_sec, const int64_t client_crashed_ttl_sec,
    const BufferAllocatorType memory_allocator_type,
    const ViewVersionId view_version)
    : ClientManager(client_live_ttl_sec, client_crashed_ttl_sec, view_version),
      memory_allocator_type_(memory_allocator_type),
      allocation_strategy_(std::make_shared<RandomAllocationStrategy>()) {}

auto CentralizedClientManager::MountLocalDiskSegment(const UUID& client_id,
                                                     bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    auto client_meta = GetClient(client_id);
    if (!client_meta) {
        LOG(WARNING) << "MountLocalDiskSegment: client not found, client_id="
                     << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    auto centralized_meta =
        std::dynamic_pointer_cast<CentralizedClientMeta>(client_meta);
    if (!centralized_meta) {
        LOG(ERROR) << "MountLocalDiskSegment: client meta type mismatch";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    auto ret = centralized_meta->MountLocalDiskSegment(enable_offloading);
    if (!ret.has_value()) {
        LOG(ERROR) << "fail to mount local disk segment"
                   << ", client_id=" << client_id << ", ret=" << ret.error();
        return ret;
    }
    return {};
}

auto CentralizedClientManager::OffloadObjectHeartbeat(const UUID& client_id,
                                                      bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    auto client_meta = GetClient(client_id);
    if (!client_meta) {
        LOG(WARNING) << "OffloadObjectHeartbeat: client not found, client_id="
                     << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    auto centralized_meta =
        std::dynamic_pointer_cast<CentralizedClientMeta>(client_meta);
    if (!centralized_meta) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    auto ret = centralized_meta->OffloadObjectHeartbeat(enable_offloading);
    if (!ret.has_value()) {
        LOG(ERROR) << "fail to offload object heartbeat"
                   << ", client_id=" << client_id << ", ret=" << ret.error();
        return ret;
    }
    return ret;
}

auto CentralizedClientManager::PushOffloadingQueue(
    const std::string& key, const int64_t size, const std::string& segment_name)
    -> tl::expected<void, ErrorCode> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    // Find which client owns this segment
    for (auto& [id, meta] : client_metas_) {
        // QuerySegments in ClientMeta checks whether segment exists
        auto query_res = meta->QuerySegments(segment_name);
        if (query_res.has_value()) {
            auto centralized_meta =
                std::dynamic_pointer_cast<CentralizedClientMeta>(meta);
            if (centralized_meta) {
                auto ret = centralized_meta->PushOffloadingQueue(key, size,
                                                                 segment_name);
                if (!ret.has_value()) {
                    LOG(ERROR) << "fail to push offloading queue"
                               << ", key=" << key << ", size=" << size
                               << ", segment_name=" << segment_name
                               << ", ret=" << ret.error();
                    return ret;
                }
                return {};
            }
        }
    }

    LOG(ERROR) << "Segment not found for offloading: " << segment_name;
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

std::shared_ptr<ClientMeta> CentralizedClientManager::CreateClientMeta(
    const RegisterClientRequest& req) {
    auto meta = std::make_shared<CentralizedClientMeta>(req.client_id,
                                                        memory_allocator_type_);
    // Register allocator change callback to sync global_allocator_manager_
    auto seg_mgr = std::dynamic_pointer_cast<CentralizedSegmentManager>(
        meta->GetSegmentManager());
    if (seg_mgr) {
        seg_mgr->SetAllocatorChangeCallback(
            [this](const std::string& segment_name,
                   const std::shared_ptr<BufferAllocatorBase>& allocator,
                   bool is_add) -> tl::expected<void, ErrorCode> {
                SharedMutexLocker lock(&global_allocator_mutex_);
                if (is_add) {
                    global_allocator_manager_.addAllocator(segment_name,
                                                           allocator);
                } else if (!global_allocator_manager_.removeAllocator(
                               segment_name, allocator)) {
                    LOG(WARNING)
                        << "Failed to remove allocator: " << segment_name;
                }
                return {};
            });
    }
    return meta;
}

auto CentralizedClientManager::Allocate(
    const uint64_t slice_length, const size_t replica_num,
    const std::vector<std::string>& preferred_segments)
    -> tl::expected<std::vector<Replica>, ErrorCode> {
    SharedMutexLocker lock(&global_allocator_mutex_, shared_lock);
    // WARNING: This function must NOT acquire segment_mutex_ which is in the
    // segment manager of client_meta.
    // For example, in SegmentManager's MountSegment/UnmountSegment():
    // 1. SegmentManager acquires SegmentManager lock (L1) at first
    // 2. Then it calls the allocator change callback, which acquires the
    // GlobalAllocatorManager lock (L2)
    // 3. Then, in CentralizedClientManager::Allocate, it acquires L2 at first.
    // Thus, if it attempts to acquire L1 here, it would cause a deadlock
    // (L2 -> L1 vs L1 -> L2).
    auto result =
        allocation_strategy_->Allocate(global_allocator_manager_, slice_length,
                                       replica_num, preferred_segments);
    if (!result.has_value()) {
        LOG(WARNING) << "No available replicas"
                     << ", slice_length=" << slice_length
                     << ", replica_num=" << replica_num;
    }
    return result;
}

HeartbeatTaskResult CentralizedClientManager::ProcessTask(
    const UUID& client_id, const HeartbeatTask& task) {
    HeartbeatTaskResult result;
    result.type = task.type_;
    result.error = ErrorCode::NOT_IMPLEMENTED;

    return result;
}

}  // namespace mooncake
