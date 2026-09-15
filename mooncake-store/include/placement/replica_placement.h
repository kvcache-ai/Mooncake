#pragma once

#include <variant>

#include "placement/replica_allocator.h"

namespace mooncake {

uint64_t StableSegmentScore(std::string_view key, std::string_view segment);

// Concrete policy executor shared by resource owners. It owns policy selection,
// not resources. LocalSSD metrics are borrowed and must outlive this object.
class ReplicaPlacement final {
   public:
    enum class Backend { Memory, NoF };
    explicit ReplicaPlacement(
        PlacementPolicyType type = PlacementPolicyType::RANDOM,
        Backend backend = Backend::Memory,
        const LocalSsdManager* local_ssd = nullptr);

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        ScopedPlacementReadAccess& access,
        const ReplicaAllocationRequest& request,
        PlacementDiagnostics* diagnostics = nullptr) const;
    tl::expected<Replica, ErrorCode> AllocateFrom(
        ScopedPlacementReadAccess& access, std::string_view name,
        size_t size) const;
    bool UsesHostAffinity() const;
    AllocationCandidateKind Kind() const;

   private:
    using Policy =
        std::variant<RandomPlacementPolicy, FreeRatioFirstPlacementPolicy,
                     LocalFirstPlacementPolicy, PreferredOnlyPlacementPolicy,
                     SsdFreeRatioFirstPlacementPolicy>;
    Policy policy_;
    Backend backend_;
};

}  // namespace mooncake
