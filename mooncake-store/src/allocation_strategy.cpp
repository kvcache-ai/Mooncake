#include "allocation_strategy.h"

#include "local_ssd/manager.h"
#include "segment.h"

namespace mooncake {

tl::expected<std::vector<Replica>, ErrorCode> AllocationStrategy::Allocate(
    const ScopedAllocatorAccess& placement, const size_t slice_length,
    const size_t replica_num,
    const std::vector<std::string>& preferred_segments,
    const std::set<std::string>& excluded_segments,
    const ReplicaType replica_type) {
    return Allocate(placement.getAllocatorManager(), slice_length, replica_num,
                    preferred_segments, excluded_segments, replica_type);
}

tl::expected<std::vector<Replica>, ErrorCode>
SsdFreeRatioFirstAllocationStrategy::Allocate(
    const ScopedAllocatorAccess& placement, const size_t slice_length,
    const size_t replica_num,
    const std::vector<std::string>& preferred_segments,
    const std::set<std::string>& excluded_segments,
    const ReplicaType replica_type) {
    const auto& allocator_manager = placement.getAllocatorManager();
    return AllocateRanked(
        allocator_manager, slice_length, replica_num, preferred_segments,
        excluded_segments, replica_type, [&](const std::string& name) {
            auto client_id = placement.GetOwnerClientId(name);
            if (!client_id) {
                return 1.0;
            }
            auto usage = local_ssd_.GetUsage(*client_id);
            if (!usage || usage->total_capacity_bytes <= 0) {
                return 1.0;
            }
            const int64_t used = std::clamp<int64_t>(
                usage->used_bytes, 0, usage->total_capacity_bytes);
            return static_cast<double>(usage->total_capacity_bytes - used) /
                   static_cast<double>(usage->total_capacity_bytes);
        });
}

}  // namespace mooncake
