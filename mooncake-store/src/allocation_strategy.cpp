#include "allocation_strategy.h"

#include "local_ssd/manager.h"
#include "segment.h"

namespace mooncake {

tl::expected<AllocationStrategy::ResolvedPlacement, ErrorCode>
AllocationStrategy::ResolvePlacement(const ReplicateConfig& config) {
    std::optional<PhysicalPlacement> requester;
    if (config.requester_placement) requester = *config.requester_placement;
    if (config.requester_self_id && !config.requester_self_id->empty()) {
        auto parsed = PhysicalPlacement::FromSelfId(*config.requester_self_id);
        if (!parsed) return tl::unexpected(ErrorCode::INVALID_PARAMS);
        if (requester && (requester->rack != parsed->rack ||
                          requester->tray != parsed->tray ||
                          requester->physical_gpu != parsed->physical_gpu))
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        if (!requester) requester = std::move(parsed);
    }
    std::string domain = config.fabric_domain ? *config.fabric_domain : "";
    if (requester) {
        if (!domain.empty() && !requester->fabric_domain.empty() &&
            domain != requester->fabric_domain)
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        if (domain.empty()) domain = requester->fabric_domain;
        requester->fabric_domain = domain;
        if (!requester->Valid())
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.fabric_domain && config.fabric_domain->empty())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    return ResolvedPlacement{std::move(domain), std::move(requester)};
}

tl::expected<std::vector<Replica>, ErrorCode>
AllocationStrategy::AllocateForRequest(
    const AllocatorManager& manager, size_t length,
    const ReplicateConfig& config,
    const std::vector<std::string>& preferred_segments) {
    if (!length || !config.replica_num)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    auto resolved = ResolvePlacement(config);
    if (!resolved) return tl::unexpected(resolved.error());
    const auto& domain = resolved->fabric_domain;
    const auto& requester = resolved->requester;
    auto eligible = manager.FilterForFabric(domain);
    for (const auto& name : preferred_segments) {
        if (!domain.empty() && !eligible.getAllocators(name))
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!requester) {
        return Allocate(eligible, length, config.replica_num,
                        preferred_segments, {}, ReplicaType::MEMORY);
    }

    // Fork self-id tiers: local tray/GPU, other tray/same GPU, cross-rack
    // same GPU, then one fallback per remaining rack. Never leave the fabric.
    std::vector<Replica> replicas;
    std::set<std::string> used_names, used_racks;
    auto pick = [&](const std::string& name, bool dedup_rack) {
        if (replicas.size() >= config.replica_num || used_names.contains(name))
            return;
        const auto* segment = eligible.GetSegmentMetadata(name);
        if (!segment || !segment->physical_placement) return;
        const auto& placement = *segment->physical_placement;
        if (dedup_rack && used_racks.contains(placement.rack)) return;
        auto replica = AllocateFrom(eligible, length, name);
        if (!replica) return;
        replicas.emplace_back(std::move(*replica));
        used_names.insert(name);
        used_racks.insert(placement.rack);
    };
    for (const auto& name : preferred_segments) pick(name, false);
    const auto names = eligible.getServingNames();
    for (int tier = 0; tier < 4 && replicas.size() < config.replica_num;
         ++tier) {
        if (tier == 2) used_racks.insert(requester->rack);
        std::vector<std::string> candidates;
        for (const auto& name : names) {
            const auto& p =
                *eligible.GetSegmentMetadata(name)->physical_placement;
            const bool same_rack = p.rack == requester->rack;
            const bool same_gpu = p.physical_gpu == requester->physical_gpu;
            const bool same_tray = p.tray == requester->tray;
            if ((tier == 0 && same_rack && same_gpu && same_tray) ||
                (tier == 1 && same_rack && same_gpu && !same_tray) ||
                (tier == 2 && !same_rack && same_gpu) ||
                (tier == 3 && !same_rack))
                candidates.push_back(name);
        }
        std::shuffle(candidates.begin(), candidates.end(),
                     threadLocalRandomEngine());
        for (const auto& name : candidates) pick(name, tier >= 2);
    }
    if (replicas.empty()) return tl::unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    return replicas;
}

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

tl::expected<std::vector<Replica>, ErrorCode>
SsdFreeRatioFirstAllocationStrategy::Allocate(
    const AllocatorManager& allocator_manager, const size_t slice_length,
    const size_t replica_num,
    const std::vector<std::string>& preferred_segments,
    const std::set<std::string>& excluded_segments,
    const ReplicaType replica_type) {
    return AllocateRanked(
        allocator_manager, slice_length, replica_num, preferred_segments,
        excluded_segments, replica_type, [&](const std::string& name) {
            auto client_id = allocator_manager.GetOwnerClientId(name);
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
