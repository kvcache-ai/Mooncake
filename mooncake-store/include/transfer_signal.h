// Copyright 2026 Mooncake Authors

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

#include "types.h"

namespace mooncake {

class TransferSignalReservation {
   public:
    virtual ~TransferSignalReservation() = default;
    virtual void Complete(ErrorCode result) noexcept = 0;
};

class TransferSignalObserver {
   public:
    virtual ~TransferSignalObserver() = default;

    // Called only after a remote READ has been accepted by TransferEngine.
    // The returned RAII reservation must release queued bytes when destroyed;
    // Complete additionally attributes a terminal result to endpoint health.
    [[nodiscard]] virtual std::shared_ptr<TransferSignalReservation> Reserve(
        std::string_view endpoint, std::string_view protocol, uint64_t bytes,
        bool health_attributable) = 0;
};

}  // namespace mooncake
