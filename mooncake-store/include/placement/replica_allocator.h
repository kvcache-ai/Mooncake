#pragma once

#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "replica.h"
#include "types.h"

namespace mooncake {

class LocalSsdManager;
class ScopedPlacementReadAccess;

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
    std::span<const std::string> excluded_groups;
    ReplicaType replica_type{ReplicaType::MEMORY};
    std::string_view writer_host_id;
    std::string_view object_key;
};

struct PlacementDiagnostics final {
    bool has_sufficient_active_group_count{false};
};

class ReplicaAllocator final {
   public:
    explicit ReplicaAllocator(
        PlacementPolicyType policy_type,
        std::optional<LocalSSDMetricsView> local_ssd_metrics = std::nullopt)
        : policy_type_(policy_type),
          local_ssd_metrics_(std::move(local_ssd_metrics)) {}

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        ScopedPlacementReadAccess& placement,
        const ReplicaAllocationRequest& request,
        PlacementDiagnostics* diagnostics = nullptr) const;

    tl::expected<Replica, ErrorCode> AllocateFrom(
        ScopedPlacementReadAccess& placement, size_t size,
        std::string_view group_name,
        ReplicaType replica_type = ReplicaType::MEMORY) const;

    PlacementPolicyType policy_type() const noexcept { return policy_type_; }

   private:
    PlacementPolicyType policy_type_;
    std::optional<LocalSSDMetricsView> local_ssd_metrics_;
};

PlacementPolicyType EffectiveNoFPlacementPolicy(
    PlacementPolicyType memory_policy);

}  // namespace mooncake
