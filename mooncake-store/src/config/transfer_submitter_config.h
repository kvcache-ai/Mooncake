#pragma once

#include <optional>

namespace mooncake {

class Environ;

struct TransferSubmitterConfig {
    // Unset leaves transport-dependent automatic selection to the submitter.
    std::optional<bool> memcpy_enabled_override;

    static TransferSubmitterConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
