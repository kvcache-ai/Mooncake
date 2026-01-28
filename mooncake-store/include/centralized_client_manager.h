#pragma once
#include <functional>

#include "client_manager.h"
#include "centralized_segment_manager.h"

namespace mooncake {
class CentralizedClientManager final : public ClientManager {
   public:
    CentralizedClientManager(const int64_t client_live_ttl_sec,
                             const BufferAllocatorType memory_allocator_type,
                             std::function<void()> segment_clean_func);
    virtual ErrorCode UnmountSegment(const UUID& segment_id,
                                     const UUID& client_id) override;

    ErrorCode MountLocalDiskSegment(const UUID& client_id,
                                    bool enable_offloading);
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;
    ErrorCode PushOffloadingQueue(const std::string& key, const int64_t size,
                                  const std::string& segment_name);
    virtual auto Ping(const UUID& client_id) -> tl::expected<ClientStatus, ErrorCode> override;

    inline tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const uint64_t slice_length, const size_t replica_num,
        const std::vector<std::string>& preferred_segments) {
        return segment_manager_->Allocate(slice_length, replica_num,
                                          preferred_segments);
    }

    virtual ErrorCode GetAllSegments(
        std::vector<std::string>& all_segments) override;
    virtual ErrorCode QuerySegments(const std::string& segment, size_t& used,
                                    size_t& capacity) override;
    virtual ErrorCode QueryIp(const UUID& client_id,
                              std::vector<std::string>& result) override;

   protected:
    virtual ErrorCode InnerMountSegment(
        const Segment& segment, const UUID& client_id,
        std::function<ErrorCode()>& pre_func) override;
    virtual ErrorCode InnerReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id,
        std::function<ErrorCode()>& pre_func) override;
    virtual void ClientMonitorFunc() override;

   protected:
    std::function<void()> segment_clean_func_;
    std::shared_ptr<CentralizedSegmentManager> segment_manager_;
};

}  // namespace mooncake
