#pragma once

#include "client_manager.h"
#include "allocation_strategy.h"

namespace mooncake {
namespace test {
class MasterServiceTest;
}

class CentralizedClientManager final : public ClientManager {
   public:
    /**
     * @brief CentralizedClientManager support to alloc buf for memory replic
     * and support some interfaces about local disk segment
     * @param client_live_ttl_sec Timeout for HEALTH -> DISCONNECTION
     * @param client_crashed_ttl_sec Timeout for DISCONNECTION -> CRASHED
     */
    CentralizedClientManager(const int64_t client_live_ttl_sec,
                             const int64_t client_crashed_ttl_sec,
                             const BufferAllocatorType memory_allocator_type,
                             const ViewVersionId view_version);

    auto MountLocalDiskSegment(const UUID& client_id, bool enable_offloading)
        -> tl::expected<void, ErrorCode>;
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;
    auto PushOffloadingQueue(const std::string& key, const int64_t size,
                             const std::string& segment_name)
        -> tl::expected<void, ErrorCode>;

    auto Allocate(const uint64_t slice_length, const size_t replica_num,
                  const std::vector<std::string>& preferred_segments)
        -> tl::expected<std::vector<Replica>, ErrorCode>;

   protected:
    std::shared_ptr<ClientMeta> CreateClientMeta(
        const RegisterClientRequest& req) override;

    HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                    const HeartbeatTask& task) override;

   private:
    BufferAllocatorType memory_allocator_type_;

    // Global allocator manager aggregates allocators from all clients.
    // Protected by its own mutex, independent of clients_mutex_.
    mutable SharedMutex global_allocator_mutex_;
    AllocatorManager global_allocator_manager_
        GUARDED_BY(global_allocator_mutex_);
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    friend class SegmentTest;
    friend class test::MasterServiceTest;
};

}  // namespace mooncake
