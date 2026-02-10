#pragma once

#include "client_meta.h"
#include "centralized_segment_manager.h"

namespace mooncake {

class CentralizedClientMeta : public ClientMeta {
   public:
    CentralizedClientMeta(const UUID& client_id,
                          BufferAllocatorType allocator_type);

    std::shared_ptr<SegmentManager> GetSegmentManager() override;
    std::shared_ptr<CentralizedSegmentManager> GetCentralizedSegmentManager();
    tl::expected<std::vector<std::string>, ErrorCode> QueryIp(
        const UUID& client_id) override;

    auto MountLocalDiskSegment(bool enable_offloading)
        -> tl::expected<void, ErrorCode>;
    auto OffloadObjectHeartbeat(bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;
    auto PushOffloadingQueue(const std::string& key, const int64_t size,
                             const std::string& segment_name)
        -> tl::expected<void, ErrorCode>;

   public:
    void DoOnDisconnected() override;
    void DoOnRecovered() override;

   private:
    std::shared_ptr<CentralizedSegmentManager> segment_manager_;
};

}  // namespace mooncake
