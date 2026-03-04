#include "centralized_client_meta.h"

#include <glog/logging.h>

namespace mooncake {
CentralizedClientMeta::CentralizedClientMeta(const UUID& client_id,
                                             BufferAllocatorType allocator_type)
    : ClientMeta(client_id) {
    segment_manager_ =
        std::make_shared<CentralizedSegmentManager>(allocator_type);
}

std::shared_ptr<SegmentManager> CentralizedClientMeta::GetSegmentManager() {
    return segment_manager_;
}
std::shared_ptr<CentralizedSegmentManager>
CentralizedClientMeta::GetCentralizedSegmentManager() {
    return segment_manager_;
}

tl::expected<std::vector<std::string>, ErrorCode>
CentralizedClientMeta::QueryIp(const UUID& client_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_->QueryIp();
}

tl::expected<void, ErrorCode> CentralizedClientMeta::MountLocalDiskSegment(
    bool enable_offloading) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }
    return segment_manager_->MountLocalDiskSegment(enable_offloading);
}

auto CentralizedClientMeta::OffloadObjectHeartbeat(bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_->OffloadObjectHeartbeat(enable_offloading);
}

tl::expected<void, ErrorCode> CentralizedClientMeta::PushOffloadingQueue(
    const std::string& key, const int64_t size,
    const std::string& segment_name) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }
    return segment_manager_->PushOffloadingQueue(key, size, segment_name);
}

void CentralizedClientMeta::DoOnDisconnected() {
    auto ret = segment_manager_->SetGlobalVisibility(false);
    if (!ret.has_value()) {
        LOG(ERROR) << "Failed to hide allocators for client " << client_id_
                   << " error=" << ret.error();
    }
}

void CentralizedClientMeta::DoOnRecovered() {
    auto ret = segment_manager_->SetGlobalVisibility(true);
    if (!ret.has_value()) {
        LOG(ERROR) << "Failed to show allocators for client " << client_id_
                   << " error=" << ret.error();
    }
}

}  // namespace mooncake
