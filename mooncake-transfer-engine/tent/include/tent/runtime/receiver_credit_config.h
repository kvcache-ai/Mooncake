// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_RUNTIME_RECEIVER_CREDIT_CONFIG_H
#define TENT_RUNTIME_RECEIVER_CREDIT_CONFIG_H

#include <array>
#include <cstddef>
#include <cstdint>

#include "tent/common/config.h"
#include "tent/runtime/receiver_credit_control.h"

namespace mooncake::tent {

// Strict, default-off runtime configuration for receiver-credit control.
// Resource arrays use the CreditResource wire order (enum value minus one).
struct ReceiverCreditRuntimeConfig {
    static constexpr size_t kDefaultMaxPeers = 1024;
    static constexpr uint32_t kDefaultFreshnessTtlMs = 1000;
    static constexpr uint32_t kDefaultRetryAfterUs = 1000;
    static constexpr uint32_t kDefaultProgressIntervalUs = 1000;
    static constexpr size_t kDefaultAdaptiveMinOwners = 1;
    static constexpr size_t kDefaultAdaptiveInitialOwners = 2;
    static constexpr size_t kDefaultAdaptiveMaxOwners = 2;
    // The observed failure mode is a TCP minimum-RTO event (~200 ms). Keep
    // ample distance from healthy scheduler jitter, which can reach ~10 ms.
    static constexpr uint32_t kDefaultAdaptiveSlowRttUs = 20000;
    static constexpr uint32_t kDefaultAdaptiveHealthyPulls = 512;

    CreditRolloutMode mode{CreditRolloutMode::Disabled};
    std::array<uint64_t, kCreditResourceCount> capacity{};
    std::array<uint64_t, kCreditResourceCount> max_grant_per_pull{};
    size_t max_peers{kDefaultMaxPeers};
    uint32_t freshness_ttl_ms{kDefaultFreshnessTtlMs};
    uint32_t retry_after_us{kDefaultRetryAfterUs};
    uint32_t progress_interval_us{kDefaultProgressIntervalUs};
    uint32_t default_qos_class{0};
    bool adaptive_dispatch_enabled{true};
    size_t adaptive_dispatch_min_owners{kDefaultAdaptiveMinOwners};
    size_t adaptive_dispatch_initial_owners{kDefaultAdaptiveInitialOwners};
    size_t adaptive_dispatch_max_owners{kDefaultAdaptiveMaxOwners};
    uint32_t adaptive_dispatch_slow_rtt_us{kDefaultAdaptiveSlowRttUs};
    uint32_t adaptive_dispatch_healthy_pulls{kDefaultAdaptiveHealthyPulls};
};

// Loads and validates the receiver_credit section. The output is changed only
// after the complete section has passed validation.
Status loadReceiverCreditConfig(const Config& config,
                                bool runtime_queue_enabled,
                                ReceiverCreditRuntimeConfig& output);

}  // namespace mooncake::tent

#endif  // TENT_RUNTIME_RECEIVER_CREDIT_CONFIG_H
