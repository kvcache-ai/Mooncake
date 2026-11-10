#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>
#include "weight_management.h"

namespace mooncake {

struct WeightAffinityUnit {
    std::string affinity_id;
    uint64_t logical_bytes{0};

    friend bool operator==(const WeightAffinityUnit&,
                           const WeightAffinityUnit&) = default;
};

struct WeightMixedResidencyPlan {
    std::vector<std::string> hot_affinity_ids;
    uint64_t hot_logical_bytes{0};
    uint64_t total_logical_bytes{0};

    friend bool operator==(const WeightMixedResidencyPlan&,
                           const WeightMixedResidencyPlan&) = default;
};

tl::expected<WeightMixedResidencyPlan, WeightManagementError>
PlanMixedWeightResidency(const std::vector<WeightAffinityUnit>& units,
                         double target_hot_ratio);

}  // namespace mooncake
