#pragma once

#include <concepts>
#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "placement/target.h"
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

struct ReplicaRequirements final {
    size_t size{0};
    size_t count{1};
    ReplicaType type{ReplicaType::MEMORY};
};

struct PlacementConstraints final {
    std::string_view preferred_group;
    std::span<const std::string> preferred_groups;
    std::span<const std::string> excluded_groups;
};

struct HostAffinity final {
    std::string_view writer_host_id;
    std::string_view object_key;
};

struct ReplicaAllocationRequest final {
    ReplicaRequirements replicas;
    PlacementConstraints placement;
    HostAffinity host_affinity;
};

struct PlacementDiagnostics final {
    bool has_sufficient_active_group_count{false};
};

struct RandomPlacementPolicy final {};
struct FreeRatioFirstPlacementPolicy final {};
struct LocalFirstPlacementPolicy final {};

struct PreferredOnlyPlacementPolicy final {
    explicit PreferredOnlyPlacementPolicy(PlacementTargetKind required_kind)
        : required_kind(required_kind) {}

    PlacementTargetKind required_kind;
};

struct SsdFreeRatioFirstPlacementPolicy final {
    explicit SsdFreeRatioFirstPlacementPolicy(LocalSSDMetricsView metrics_view)
        : metrics(std::move(metrics_view)) {}

    LocalSSDMetricsView metrics;
};

template <typename Policy>
concept ReplicaPlacementPolicy =
    std::same_as<Policy, RandomPlacementPolicy> ||
    std::same_as<Policy, FreeRatioFirstPlacementPolicy> ||
    std::same_as<Policy, SsdFreeRatioFirstPlacementPolicy> ||
    std::same_as<Policy, LocalFirstPlacementPolicy> ||
    std::same_as<Policy, PreferredOnlyPlacementPolicy>;

template <ReplicaPlacementPolicy Policy>
class ReplicaAllocator final {
   public:
    explicit ReplicaAllocator(Policy policy) : policy_(std::move(policy)) {}

    tl::expected<std::vector<Replica>, ErrorCode> Allocate(
        ScopedPlacementReadAccess& placement,
        const ReplicaAllocationRequest& request,
        PlacementDiagnostics* diagnostics = nullptr) const;

    tl::expected<Replica, ErrorCode> AllocateFrom(
        ScopedPlacementReadAccess& placement, size_t size,
        std::string_view group_name,
        ReplicaType replica_type = ReplicaType::MEMORY) const;

    static constexpr bool UsesHostAffinity() noexcept {
        return std::same_as<Policy, LocalFirstPlacementPolicy>;
    }

   private:
    Policy policy_;
};

extern template class ReplicaAllocator<RandomPlacementPolicy>;
extern template class ReplicaAllocator<FreeRatioFirstPlacementPolicy>;
extern template class ReplicaAllocator<SsdFreeRatioFirstPlacementPolicy>;
extern template class ReplicaAllocator<LocalFirstPlacementPolicy>;
extern template class ReplicaAllocator<PreferredOnlyPlacementPolicy>;

inline RandomPlacementPolicy MakeNoFPlacementPolicy(
    const SsdFreeRatioFirstPlacementPolicy&) {
    return {};
}

template <ReplicaPlacementPolicy Policy>
Policy MakeNoFPlacementPolicy(const Policy& memory_policy) {
    return memory_policy;
}

}  // namespace mooncake
