#include "weight_residency_planner.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <set>
#include <tuple>
#include <utility>

namespace mooncake {
namespace {

struct Subset {
    uint64_t bytes;
    uint32_t mask;
};

std::vector<Subset> Enumerate(const std::vector<WeightAffinityUnit>& units,
                              size_t begin, size_t count) {
    std::vector<Subset> sums{{0, 0}};
    sums.reserve(size_t{1} << count);
    for (size_t i = 0; i < count; ++i) {
        const auto size = sums.size();
        for (size_t j = 0; j < size; ++j) {
            sums.push_back({sums[j].bytes + units[begin + i].logical_bytes,
                            sums[j].mask | (uint32_t{1} << i)});
        }
    }
    return sums;
}

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
                  return lhs.affinity_id < rhs.affinity_id;
              });
    const long double target =
        static_cast<long double>(total_bytes) * target_hot_ratio;
    std::vector<std::string> selected;
    uint64_t hot_bytes = 0;
    long double best_distance = std::numeric_limits<long double>::infinity();
    constexpr size_t kExactAffinityLimit = 32;
    auto consider = [&](uint64_t bytes, auto make_ids) {
        if (bytes == 0 || bytes == total_bytes) {
            return;
        }
        const auto distance =
            std::fabs(static_cast<long double>(bytes) - target);
        if (distance < best_distance) {
            best_distance = distance;
            hot_bytes = bytes;
            selected = make_ids();
        } else if (ordered.size() <= kExactAffinityLimit &&
                   distance == best_distance) {
            auto ids = make_ids();
            if (ids < selected) {
                hot_bytes = bytes;
                selected = std::move(ids);
            }
        }
    };
    if (ordered.size() <= kExactAffinityLimit) {
        // Each half has at most 65536 states, independent of byte magnitudes.
        const size_t middle = ordered.size() / 2;
        const auto left = Enumerate(ordered, 0, middle);
        auto right = Enumerate(ordered, middle, ordered.size() - middle);
        std::sort(right.begin(), right.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return std::tie(lhs.bytes, lhs.mask) <
                             std::tie(rhs.bytes, rhs.mask);
                  });
        for (const auto& lhs : left) {
            const auto lower = std::lower_bound(
                right.begin(), right.end(), target - lhs.bytes,
                [](const auto& rhs, long double desired) {
                    return static_cast<long double>(rhs.bytes) < desired;
                });
            const auto index = static_cast<ptrdiff_t>(lower - right.begin());
            // Empty/full are the only invalid sums; inspect their next
            // neighbor.
            for (ptrdiff_t offset = -2; offset <= 1; ++offset) {
                const auto candidate = index + offset;
                if (candidate < 0 ||
                    candidate >= static_cast<ptrdiff_t>(right.size())) {
                    continue;
                }
                const auto& rhs = right[candidate];
                consider(lhs.bytes + rhs.bytes, [&] {
                    std::vector<std::string> ids;
                    for (size_t i = 0; i < ordered.size(); ++i) {
                        const bool included =
                            i < middle ? (lhs.mask & (uint32_t{1} << i)) != 0
                                       : (rhs.mask &
                                          (uint32_t{1} << (i - middle))) != 0;
                        if (included) ids.push_back(ordered[i].affinity_id);
                    }
                    return ids;
                });
            }
        }
    } else {
        std::sort(ordered.begin(), ordered.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return std::tie(lhs.logical_bytes, lhs.affinity_id) <
                             std::tie(rhs.logical_bytes, rhs.affinity_id);
                  });
        // Proper prefixes plus complement(min) span [min, total-min], with
        // gaps <= max. Nearest distance is <= max/2 inside that interval;
        // outside it, the closest feasible endpoint is selected exactly.
        uint64_t prefix = 0;
        size_t best_prefix = 0;
        for (size_t i = 0; i + 1 < ordered.size(); ++i) {
            prefix += ordered[i].logical_bytes;
            consider(prefix, [&] {
                best_prefix = i + 1;
                return std::vector<std::string>{};
            });
        }
        const auto before = best_distance;
        consider(total_bytes - ordered.front().logical_bytes, [&] {
            best_prefix = 0;
            return std::vector<std::string>{};
        });
        const size_t begin = best_distance < before ? 1 : 0;
        const size_t end = begin == 1 ? ordered.size() : best_prefix;
        for (size_t i = begin; i < end; ++i)
            selected.push_back(ordered[i].affinity_id);
        std::sort(selected.begin(), selected.end());
    }

    return WeightMixedResidencyPlan{
        .hot_affinity_ids = std::move(selected),
        .hot_logical_bytes = hot_bytes,
        .total_logical_bytes = total_bytes,
    };
}

}  // namespace mooncake
