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
    virtual auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

    auto MountLocalDiskSegment(const UUID& client_id, bool enable_offloading)
        -> tl::expected<void, ErrorCode>;
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;
    auto PushOffloadingQueue(const std::string& key, const int64_t size,
                             const std::string& segment_name)
        -> tl::expected<void, ErrorCode>;
    virtual auto Ping(const UUID& client_id)
        -> tl::expected<ClientStatus, ErrorCode> override;

    auto Allocate(const uint64_t slice_length, const size_t replica_num,
                  const std::vector<std::string>& preferred_segments)
        -> tl::expected<std::vector<Replica>, ErrorCode> {
        return segment_manager_->Allocate(slice_length, replica_num,
                                          preferred_segments);
    }

    virtual auto GetAllSegments()
        -> tl::expected<std::vector<std::string>, ErrorCode> override;
    virtual auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode> override;
    virtual auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> override;

   protected:
    virtual auto InnerMountSegment(const Segment& segment,
                                   const UUID& client_id,
                                   std::function<ErrorCode()>& pre_func)
        -> tl::expected<void, ErrorCode> override;
    virtual auto InnerReMountSegment(const std::vector<Segment>& segments,
                                     const UUID& client_id,
                                     std::function<ErrorCode()>& pre_func)
        -> tl::expected<void, ErrorCode> override;
    virtual void ClientMonitorFunc() override;

   protected:
    std::function<void()> segment_clean_func_;
    std::shared_ptr<CentralizedSegmentManager> segment_manager_;
};

}  // namespace mooncake
