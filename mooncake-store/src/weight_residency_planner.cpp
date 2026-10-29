#include "weight_residency_planner.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <set>
#include <tuple>
#include <utility>

namespace mooncake {
namespace {

enum class ImprovementKind {
    NONE,
    ADD,
    REMOVE,
    SWAP,
};

struct Improvement {
    ImprovementKind kind{ImprovementKind::NONE};
    std::string removed_id;
    std::string added_id;
    uint64_t logical_bytes{0};
    long double distance{std::numeric_limits<long double>::infinity()};
};

}  // namespace

tl::expected<WeightMixedResidencyPlan, WeightManagementError>
PlanMixedWeightResidency(const std::vector<WeightAffinityUnit>& units,
                         double target_hot_ratio) {
    if (!std::isfinite(target_hot_ratio) || target_hot_ratio <= 0.0 ||
        target_hot_ratio >= 1.0 || units.size() < 2) {
        return tl::make_unexpected(WeightManagementError::POLICY_UNSATISFIABLE);
    }

    uint64_t total_bytes = 0;
    std::set<std::string> affinity_ids;
    for (const auto& unit : units) {
        if (unit.affinity_id.empty() || unit.logical_bytes == 0 ||
            !affinity_ids.insert(unit.affinity_id).second ||
            unit.logical_bytes >
                std::numeric_limits<uint64_t>::max() - total_bytes) {
            return tl::make_unexpected(
                WeightManagementError::POLICY_UNSATISFIABLE);
        }
        total_bytes += unit.logical_bytes;
    }

    auto ordered = units;
    std::sort(ordered.begin(), ordered.end(),
              [](const auto& lhs, const auto& rhs) {
                  if (lhs.logical_bytes != rhs.logical_bytes) {
                      return lhs.logical_bytes > rhs.logical_bytes;
                  }
                  return lhs.affinity_id < rhs.affinity_id;
              });
    const long double target =
        static_cast<long double>(total_bytes) * target_hot_ratio;

    std::set<std::string> selected{ordered.front().affinity_id};
    uint64_t hot_bytes = ordered.front().logical_bytes;
    for (const auto& unit : ordered) {
        if (selected.contains(unit.affinity_id) ||
            selected.size() + 1 == units.size()) {
            continue;
        }
        const auto before =
            std::fabs(static_cast<long double>(hot_bytes) - target);
        const auto after = std::fabs(static_cast<long double>(hot_bytes) +
                                     unit.logical_bytes - target);
        if (after < before) {
            selected.insert(unit.affinity_id);
            hot_bytes += unit.logical_bytes;
        }
    }

    auto consider = [](Improvement candidate, Improvement* best) {
        if (candidate.distance < best->distance ||
            (candidate.distance == best->distance &&
             std::tie(candidate.kind, candidate.removed_id,
                      candidate.added_id) <
                 std::tie(best->kind, best->removed_id, best->added_id))) {
            *best = std::move(candidate);
        }
    };
    constexpr size_t kMaxLocalImprovementPasses = 8;
    for (size_t pass = 0; pass < kMaxLocalImprovementPasses; ++pass) {
        const auto current_distance =
            std::fabs(static_cast<long double>(hot_bytes) - target);
        Improvement best;
        if (selected.size() + 1 < units.size()) {
            for (const auto& unit : ordered) {
                if (selected.contains(unit.affinity_id)) {
                    continue;
                }
                const uint64_t bytes = hot_bytes + unit.logical_bytes;
                consider(
                    Improvement{
                        .kind = ImprovementKind::ADD,
                        .removed_id = {},
                        .added_id = unit.affinity_id,
                        .logical_bytes = bytes,
                        .distance =
                            std::fabs(static_cast<long double>(bytes) - target),
                    },
                    &best);
            }
        }
        if (selected.size() > 1) {
            for (const auto& unit : ordered) {
                if (!selected.contains(unit.affinity_id)) {
                    continue;
                }
                const uint64_t bytes = hot_bytes - unit.logical_bytes;
                consider(
                    Improvement{
                        .kind = ImprovementKind::REMOVE,
                        .removed_id = unit.affinity_id,
                        .added_id = {},
                        .logical_bytes = bytes,
                        .distance =
                            std::fabs(static_cast<long double>(bytes) - target),
                    },
                    &best);
            }
        }

        std::vector<WeightAffinityUnit> unselected;
        for (const auto& unit : ordered) {
            if (!selected.contains(unit.affinity_id)) {
                unselected.push_back(unit);
            }
        }
        std::sort(unselected.begin(), unselected.end(),
                  [](const auto& lhs, const auto& rhs) {
                      if (lhs.logical_bytes != rhs.logical_bytes) {
                          return lhs.logical_bytes < rhs.logical_bytes;
                      }
                      return lhs.affinity_id < rhs.affinity_id;
                  });
        for (const auto& removed : ordered) {
            if (!selected.contains(removed.affinity_id)) {
                continue;
            }
            const long double desired =
                target -
                static_cast<long double>(hot_bytes - removed.logical_bytes);
            const auto lower = std::lower_bound(
                unselected.begin(), unselected.end(), desired,
                [](const auto& unit, long double bytes) {
                    return static_cast<long double>(unit.logical_bytes) < bytes;
                });
            for (const auto replacement :
                 {lower == unselected.end() ? std::prev(unselected.end())
                                            : lower,
                  lower == unselected.begin() ? lower : std::prev(lower)}) {
                const uint64_t bytes = hot_bytes - removed.logical_bytes +
                                       replacement->logical_bytes;
                consider(
                    Improvement{
                        .kind = ImprovementKind::SWAP,
                        .removed_id = removed.affinity_id,
                        .added_id = replacement->affinity_id,
                        .logical_bytes = bytes,
                        .distance =
                            std::fabs(static_cast<long double>(bytes) - target),
                    },
                    &best);
            }
        }
        if (best.kind == ImprovementKind::NONE ||
            best.distance >= current_distance) {
            break;
        }
        if (best.kind == ImprovementKind::REMOVE ||
            best.kind == ImprovementKind::SWAP) {
            selected.erase(best.removed_id);
        }
        if (best.kind == ImprovementKind::ADD ||
            best.kind == ImprovementKind::SWAP) {
            selected.insert(best.added_id);
        }
        hot_bytes = best.logical_bytes;
    }

    return WeightMixedResidencyPlan{
        .hot_affinity_ids = {selected.begin(), selected.end()},
        .hot_logical_bytes = hot_bytes,
        .total_logical_bytes = total_bytes,
    };
}

}  // namespace mooncake
