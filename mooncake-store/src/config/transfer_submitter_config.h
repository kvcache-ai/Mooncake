#pragma once

#include <chrono>
#include <cstdint>
#include <optional>

namespace mooncake {

struct TransferSubmitterConfig {
    // Accepted range for MC_STORE_TRANSFER_TIMEOUT_MS. The upper bound keeps
    // the wait deadline far from clock overflow; no transfer needs a day.
    static constexpr int64_t kMinTransferTimeoutMs = 1;
    static constexpr int64_t kMaxTransferTimeoutMs = 24LL * 60 * 60 * 1000;

    // Unset leaves transport-dependent automatic selection to the submitter.
    std::optional<bool> memcpy_enabled_override;
    // Unset keeps the built-in wait timeout for Transfer Engine operations.
    std::optional<std::chrono::milliseconds> transfer_timeout_override;

    static TransferSubmitterConfig FromEnvironment();
};

}  // namespace mooncake
