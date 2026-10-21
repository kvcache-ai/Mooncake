#pragma once

#include <optional>

namespace mooncake {

struct TransferSubmitterConfig {
    // Unset leaves transport-dependent automatic selection to the submitter.
    std::optional<bool> memcpy_enabled_override;

    static TransferSubmitterConfig FromEnvironment();
};

struct TransferTimeoutConfig {
    // Upper bound on how long a store transfer may wait for the classic
    // transfer engine to complete a batch before failing with TRANSFER_FAIL.
    int timeout_seconds = 60;

    static TransferTimeoutConfig FromEnvironment();
};

}  // namespace mooncake
