#pragma once

#include <glog/logging.h>

#include <cmath>
#include <cstddef>
#include <stdexcept>

namespace mooncake {

struct OffloadConfig {
    static constexpr size_t kMaxOffloadingQueueLimit = 100'000'000ULL;

    bool enable_offload = false;
    bool offload_on_evict = false;
    bool offload_force_evict = false;
    size_t offloading_queue_limit = 50000;
    double offload_cap_ratio = 0.5;

    void Validate() const {
        if (std::isnan(offload_cap_ratio) || offload_cap_ratio < 0.0 ||
            offload_cap_ratio > 1.0) {
            LOG(ERROR) << "offload_cap_ratio must be between 0.0 and 1.0, "
                       << "current value: " << offload_cap_ratio;
            throw std::invalid_argument("Invalid offload_cap_ratio");
        }
        if (offloading_queue_limit == 0) {
            LOG(ERROR) << "offloading_queue_limit must be greater than 0";
            throw std::invalid_argument("Invalid offloading_queue_limit");
        }
        if (offloading_queue_limit > kMaxOffloadingQueueLimit) {
            LOG(ERROR) << "offloading_queue_limit must be <= "
                       << kMaxOffloadingQueueLimit
                       << " to avoid overflow when computing offload_cap, "
                       << "current value: " << offloading_queue_limit;
            throw std::invalid_argument("Invalid offloading_queue_limit");
        }
    }

    bool ShouldOffloadOnEvict() const {
        return enable_offload && offload_on_evict;
    }

    bool ShouldOffloadOnPutEnd() const {
        return enable_offload && !offload_on_evict;
    }

    long OffloadCap() const {
        return static_cast<long>(offloading_queue_limit * offload_cap_ratio);
    }
};

}  // namespace mooncake
