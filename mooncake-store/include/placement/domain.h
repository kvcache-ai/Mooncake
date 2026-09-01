#pragma once

#include <cstddef>
#include <string_view>
#include <utility>
#include <vector>

#include "placement/replica_allocator.h"

namespace mooncake {

// Binds replica allocation to a concrete placement source without virtual
// dispatch. PlacementSource must provide AcquirePlacementAccess().
template <typename PlacementSource, ReplicaPlacementPolicy Policy>
class ReplicaPlacement final {
   public:
    ReplicaPlacement(PlacementSource& source, Policy policy)
        : source_(&source), allocator_(std::move(policy)) {}

    static constexpr bool UsesHostAffinity() noexcept {
        return ReplicaAllocator<Policy>::UsesHostAffinity();
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
    ReplicaAllocator<Policy> allocator_;
};

}  // namespace mooncake
