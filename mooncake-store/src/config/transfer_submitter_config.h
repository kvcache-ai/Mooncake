#pragma once

#include <optional>

namespace mooncake {

struct TransferSubmitterConfig {
    // Unset leaves transport-dependent automatic selection to the submitter.
    std::optional<bool> memcpy_enabled_override;

    static TransferSubmitterConfig FromEnvironment();
};

}  // namespace mooncake
