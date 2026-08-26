#pragma once

#include <cstddef>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "placement/replica_allocator.h"

namespace mooncake {

// Binds replica allocation to a concrete placement source without virtual
// dispatch. PlacementSource must provide AcquirePlacementAccess().
template <typename PlacementSource>
class ReplicaPlacement final {
   public:
    ReplicaPlacement(
        PlacementSource& source, PlacementPolicyType policy_type,
        std::optional<LocalSSDMetricsView> local_ssd_metrics = std::nullopt)
        : source_(&source),
          allocator_(policy_type, std::move(local_ssd_metrics)) {}

    PlacementPolicyType policy_type() const noexcept {
        return allocator_.policy_type();
    }
    bool UsesHostAffinity() const noexcept {
        return policy_type() == PlacementPolicyType::LOCAL_FIRST;
    }

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        const ReplicaAllocationRequest& request,
        PlacementDiagnostics* diagnostics = nullptr) const {
        auto access = source_->AcquirePlacementAccess();
        return allocator_.Allocate(access, request, diagnostics);
    }

    tl::expected<Replica, ErrorCode> AllocateFrom(
        size_t size, std::string_view group_name,
        ReplicaType replica_type = ReplicaType::MEMORY) const {
        auto access = source_->AcquirePlacementAccess();
        return allocator_.AllocateFrom(access, size, group_name, replica_type);
    }

   private:
    PlacementSource* source_;
    ReplicaAllocator allocator_;
};

}  // namespace mooncake
