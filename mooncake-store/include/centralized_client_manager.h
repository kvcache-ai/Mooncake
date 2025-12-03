#pragma once
#include <functional>

#include "client_manager.h"
#include "centralized_segment_manager.h"
namespace mooncake {
class CentralizedClientManager : public ClientManager {
   public:
    CentralizedClientManager(const int64_t client_live_ttl_sec, const BufferAllocatorType memory_allocator_type,
                             std::function<void()> segment_clean_func);
    virtual ErrorCode UnmountSegment(const UUID& segment_id, const UUID& client_id) override;

    ErrorCode MountLocalDiskSegment(const UUID& client_id, bool enable_offloading);
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;
    ErrorCode PushOffloadingQueue(const std::string& key, const int64_t size,
                                  const std::string& segment_name);
    auto Ping(const UUID& client_id) -> tl::expected<ClientStatus, ErrorCode>;

    inline tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const uint64_t slice_length, const size_t replica_num,
        const std::vector<std::string>& preferred_segments) {
        return segment_manager_->Allocate(slice_length, replica_num, preferred_segments);
    }

   protected:
    virtual std::shared_ptr<SegmentManager> GetSegmentManager() override {
        return std::static_pointer_cast<SegmentManager>(segment_manager_);
    }
    virtual void ClientMonitorFunc() override;

   protected:
    std::function<void()> segment_clean_func_;
    std::shared_ptr<CentralizedSegmentManager> segment_manager_;
};

}  // namespace mooncake
