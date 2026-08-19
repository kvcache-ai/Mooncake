#pragma once

#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "placement/index.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

class LocalSsdManager;

class LocalSSDMetricsView final {
   public:
    explicit LocalSSDMetricsView(const LocalSsdManager& local_ssd)
        : local_ssd_(&local_ssd) {}

    std::optional<double> GetFreeRatio(const UUID& client_id) const;

   private:
    const LocalSsdManager* local_ssd_;
};

struct ReplicaAllocationRequest final {
    size_t size{0};
    size_t replica_count{1};
    std::string_view preferred_group;
    std::span<const std::string> preferred_groups;
    std::span<PlacementGroup* const> resolved_preferred_groups;
    std::span<const std::string> excluded_groups;
    ReplicaType replica_type{ReplicaType::MEMORY};
};

class ReplicaAllocator final {
   public:
    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        ScopedPlacementAccess& placement, PlacementPolicyType policy_type,
        const ReplicaAllocationRequest& request,
        std::optional<LocalSSDMetricsView> local_ssd_metrics =
            std::nullopt) const;

    tl::expected<Replica, ErrorCode> AllocateFrom(
        ScopedPlacementAccess& placement, size_t size,
        std::string_view group_name,
        ReplicaType replica_type = ReplicaType::MEMORY) const;
};

PlacementPolicyType EffectiveNoFPlacementPolicy(
    PlacementPolicyType memory_policy);

}  // namespace mooncake
