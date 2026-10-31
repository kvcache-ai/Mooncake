#include "control_plane/collective_selection.h"

#include <algorithm>
#include <limits>

#include <glog/logging.h>

namespace mooncake {
namespace {

bool memberSupportsGpuCollectiveBackend(const GroupMember& member,
                                        GpuCollectiveBackend backend) {
    if (backend == GpuCollectiveBackend::Legacy) return true;
    return member.endpoint.has_value() &&
           supportsGpuCollectiveBackend(member.endpoint->device_collective,
                                        backend);
}

// Prefer safe, performant defaults without assuming optional capabilities.
// Add capability-dependent defaults only after verifying their requirements
// for the group.
std::vector<DeviceCollectiveAlgorithmChoice>
defaultDeviceCollectiveAlgorithmChoices() {
    constexpr auto max_bytes = std::numeric_limits<uint64_t>::max();
    return {{OpType::AllReduce, max_bytes, DeviceCollectiveAlgorithm::Ring},
            {OpType::AllGather, max_bytes, DeviceCollectiveAlgorithm::Ring}};
}

// Returns whether OneShot can be selected for the group's active members.
// OneShot is not usable in every case: each active rank must advertise
// OneShot, and every pair of active ranks must have a directly addressable
// route with native atomics. This is a limitation of our current OneShot
// implementation.
//
// With the current implementation, a OneShot kernel captured in a CUDA Graph
// cannot communicate with new members whose routes lack these capabilities.
// TODO: Extend OneShot to support routes without direct memory access or
// native atomics, so captured kernels remain usable after arbitrary group
// expansion.
bool groupSupportsOneShot(
    const GroupView& view,
    std::span<const std::vector<DeviceTransferRouteInfo>> installed_routes) {
    if (installed_routes.empty()) return false;
    std::vector<GlobalRank> active;
    for (const auto rank : view.rank_order) {
        const auto& member = view.members[rank];
        if (!member.isActive()) continue;
        if (!member.endpoint ||
            !member.endpoint->device_collective.supportsAlgorithm(
                DeviceCollectiveAlgorithm::OneShot))
            return false;
        active.push_back(rank);
    }
    // The current OneShot/LL kernels access every peer's packet storage
    // directly. Check actual selected routes in both directions.
    for (const auto source : active) {
        const auto& routes = installed_routes[source];
        if (routes.empty()) return false;
        for (const auto destination : active) {
            if (source == destination) continue;
            const auto& route = routes[destination];
            if (!route.directly_addressable || !route.native_atomics)
                return false;
        }
    }
    return true;
}

std::optional<GpuCollectiveBackend> selectGpuCollectiveBackend(
    const GroupView& view) {
    std::optional<GpuCollectiveBackend> consensus;
    bool has_active_rank = false;
    bool preferences_conflict = false;
    size_t active_rank_count = 0;

    for (const auto& member : view.members) {
        if (!member.isActive()) continue;
        ++active_rank_count;

        const auto& preferred = member.preferred_gpu_collective_backend;
        if (!has_active_rank) {
            consensus = preferred;
            has_active_rank = true;
            continue;
        }
        preferences_conflict |= consensus != preferred;
    }

    if (!has_active_rank) return std::nullopt;

    if (preferences_conflict) {
        LOG(WARNING) << "[PG] Conflicting GPU collective backend preferences; "
                        "falling back to legacy"
                     << " group=" << view.group_id
                     << " active_rank_count=" << active_rank_count;
        consensus = GpuCollectiveBackend::Legacy;
    }

    // CPU groups consistently have no GPU backend preference.
    if (!consensus.has_value()) return std::nullopt;

    auto selected = *consensus;
    const bool all_support_selected = std::all_of(
        view.members.begin(), view.members.end(),
        [&](const GroupMember& member) {
            return !member.isActive() ||
                   memberSupportsGpuCollectiveBackend(member, selected);
        });
    if (!all_support_selected) {
        LOG(WARNING) << "[PG] Preferred GPU collective backend is not "
                        "available on every active rank; falling back to legacy"
                     << " group=" << view.group_id
                     << " preferred=" << gpuCollectiveBackendName(selected)
                     << " active_rank_count=" << active_rank_count;
        selected = GpuCollectiveBackend::Legacy;
    }

    if (view.gpu_collective_backend != selected) {
        LOG(INFO) << "[PG] Coordinator selected GPU collective backend"
                  << " group=" << view.group_id
                  << " backend=" << gpuCollectiveBackendName(selected)
                  << " active_rank_count=" << active_rank_count;
    }
    return selected;
}

std::vector<DeviceCollectiveAlgorithmChoice> selectDeviceCollectiveAlgorithms(
    const GroupView& view,
    std::span<const std::vector<DeviceTransferRouteInfo>> installed_routes,
    const std::optional<std::vector<DeviceCollectiveAlgorithmChoice>>&
        algorithm_choices_override) {
    auto choices = defaultDeviceCollectiveAlgorithmChoices();
    if (groupSupportsOneShot(view, installed_routes)) {
        choices.push_back({OpType::AllReduce, 512 * 1024,
                           DeviceCollectiveAlgorithm::OneShot});
    }
    if (algorithm_choices_override) {
        const auto& overrides = *algorithm_choices_override;
        // Explicit rules cover a prefix through their largest bound. Keep
        // default rules whose upper bounds extend beyond that prefix.
        std::erase_if(choices, [&](const auto& choice) {
            return std::any_of(
                overrides.begin(), overrides.end(),
                [&](const auto& configured) {
                    return configured.operation == choice.operation &&
                           configured.max_bytes >= choice.max_bytes;
                });
        });
        choices.insert(choices.end(), overrides.begin(), overrides.end());
    }
    std::stable_sort(choices.begin(), choices.end(),
                     [](const auto& left, const auto& right) {
                         return left.operation != right.operation
                                    ? left.operation < right.operation
                                    : left.max_bytes < right.max_bytes;
                     });
    return choices;
}

}  // namespace

bool supportsGpuCollectiveBackend(const DeviceGroupEndpoint& endpoint,
                                  GpuCollectiveBackend backend) noexcept {
    switch (backend) {
        case GpuCollectiveBackend::Legacy:
            return true;
        case GpuCollectiveBackend::New:
            // The new backend needs signals to synchronize view epochs.
            // Its current default algorithm choices require Ring.
            return endpoint.view_epoch_signal_count != 0 &&
                   endpoint.supportsAlgorithm(DeviceCollectiveAlgorithm::Ring);
    }
    return false;
}

std::optional<GpuCollectiveSelection> selectGpuCollective(
    const GroupView& view,
    std::span<const std::vector<DeviceTransferRouteInfo>> installed_routes,
    const std::optional<std::vector<DeviceCollectiveAlgorithmChoice>>&
        algorithm_choices_override) {
    auto backend = selectGpuCollectiveBackend(view);
    if (!backend) return std::nullopt;

    GpuCollectiveSelection selection{.backend = *backend};
    if (*backend == GpuCollectiveBackend::New) {
        selection.algorithm_choices = selectDeviceCollectiveAlgorithms(
            view, installed_routes, algorithm_choices_override);
    }
    return selection;
}

}  // namespace mooncake
