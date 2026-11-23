#pragma once

#include <glog/logging.h>

#include <cstdint>
#include <stdexcept>

namespace mooncake {

struct PromotionConfig {
    static constexpr uint32_t kMinAdmissionThreshold = 1;
    static constexpr uint32_t kMaxAdmissionThreshold = 255;
    static constexpr uint32_t kMinMaxPerHeartbeat = 1;

    bool promotion_on_hit = false;
    uint32_t promotion_admission_threshold = 2;
    uint32_t promotion_queue_limit = 50000;
    uint32_t promotion_max_per_heartbeat = 1;

    void Validate() const {
        if (promotion_admission_threshold < kMinAdmissionThreshold ||
            promotion_admission_threshold > kMaxAdmissionThreshold) {
            LOG(ERROR) << "promotion_admission_threshold must be between "
                       << kMinAdmissionThreshold << " and "
                       << kMaxAdmissionThreshold
                       << ", current value: " << promotion_admission_threshold;
            throw std::invalid_argument(
                "Invalid promotion_admission_threshold");
        }
        if (promotion_max_per_heartbeat < kMinMaxPerHeartbeat) {
            LOG(ERROR) << "promotion_max_per_heartbeat must be at least "
                       << kMinMaxPerHeartbeat
                       << ", current value: " << promotion_max_per_heartbeat;
            throw std::invalid_argument("Invalid promotion_max_per_heartbeat");
        }
    }
};

}  // namespace mooncake
