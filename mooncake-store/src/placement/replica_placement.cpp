#include "placement/replica_placement.h"

#include <algorithm>
#include <stdexcept>

#include "placement/index.h"

namespace mooncake {

uint64_t StableSegmentScore(std::string_view key, std::string_view segment) {
    uint64_t hash = 1469598103934665603ULL;
    const auto mix = [&hash](std::string_view value) {
        for (const unsigned char c : value) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        hash ^= 0xff;
        hash *= 1099511628211ULL;
    };
    mix(key);
    mix(segment);
    return hash;
}

ReplicaPlacement::ReplicaPlacement(PlacementPolicyType type, Backend backend,
                                   const LocalSsdManager* local_ssd)
    : backend_(backend) {
    switch (type) {
        case PlacementPolicyType::FREE_RATIO_FIRST:
            policy_.emplace<FreeRatioFirstPlacementPolicy>();
            break;
        case PlacementPolicyType::LOCAL_FIRST:
            policy_.emplace<LocalFirstPlacementPolicy>();
            break;
        case PlacementPolicyType::CXL:
            policy_.emplace<PreferredOnlyPlacementPolicy>(
                AllocationCandidateKind::CXL);
            break;
        case PlacementPolicyType::SSD_FREE_RATIO_FIRST:
            if (backend == Backend::NoF) break;
            if (!local_ssd)
                throw std::invalid_argument(
                    "SSD placement requires live metrics");
            policy_.emplace<SsdFreeRatioFirstPlacementPolicy>(
                LocalSSDMetricsView(*local_ssd));
            break;
        default:
            break;
    }
}

bool ReplicaPlacement::UsesHostAffinity() const {
    return std::holds_alternative<LocalFirstPlacementPolicy>(policy_);
}

AllocationCandidateKind ReplicaPlacement::Kind() const {
    const auto* policy = std::get_if<PreferredOnlyPlacementPolicy>(&policy_);
    return policy ? policy->required_kind : AllocationCandidateKind::NATIVE;
}

tl::expected<std::vector<Replica>, ErrorCode> ReplicaPlacement::Allocate(
    ScopedPlacementReadAccess& access, const ReplicaAllocationRequest& request,
    AllocationDiagnostics* diagnostics) const {
    auto resolved = request;
    std::vector<std::string> preferred;
    if (backend_ == Backend::Memory &&
        !request.host_affinity.writer_host_id.empty()) {
        const auto append = [&](std::string_view name) {
            if (!name.empty() && std::find(preferred.begin(), preferred.end(),
                                           name) == preferred.end()) {
                preferred.emplace_back(name);
            }
        };
        if (!request.placement.preferred_segment_name.empty()) {
            append(request.placement.preferred_segment_name);
        } else {
            for (const auto& name : request.placement.preferred_segment_names)
                append(name);
        }
        access.GetView().VisitHostOrderedSegmentNames(
            request.host_affinity.writer_host_id,
            request.host_affinity.object_key, [&](std::string_view name) {
                append(name);
                return false;
            });
        resolved.placement.preferred_segment_name = {};
        resolved.placement.preferred_segment_names = preferred;
        resolved.host_affinity = {};
    }
    PlacementDiagnostics placement_diagnostics;
    auto result = std::visit(
        [&](const auto& policy) {
            return ReplicaAllocator(policy).Allocate(
                access, resolved,
                diagnostics ? &placement_diagnostics : nullptr);
        },
        policy_);
    if (diagnostics) {
        diagnostics->reclamation_may_help =
            ((result && result->size() < request.replicas.count) ||
             (!result && result.error() == ErrorCode::NO_AVAILABLE_HANDLE)) &&
            placement_diagnostics.has_sufficient_active_entry_count;
    }
    return result;
}

tl::expected<Replica, ErrorCode> ReplicaPlacement::AllocateFrom(
    ScopedPlacementReadAccess& access, std::string_view name,
    size_t size) const {
    return ReplicaAllocator(PreferredOnlyPlacementPolicy(Kind()))
        .AllocateFrom(access, size, name,
                      backend_ == Backend::Memory ? ReplicaType::MEMORY
                                                  : ReplicaType::NOF_SSD);
}

}  // namespace mooncake
