// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "ssd_destination_registration.h"

namespace mooncake {
class TransferEngine;
namespace ssd_offload {
// Retains the engine through physical deregistration. Initially supports
// classic TE host destinations. TENT's duplicate-MR refcount semantics, device
// memory, and Dummy mappings require their own validated adapters.
class TransferEngineDestinationRegistration final
    : public DestinationRegistrationTransport {
   public:
    explicit TransferEngineDestinationRegistration(
        std::shared_ptr<TransferEngine>);
    TransportRegistrationResult RegisterWritable(
        const DestinationBinding&, const std::string&) noexcept override;
    DestinationError Unregister(uint64_t registered_base) noexcept override;

   private:
    std::shared_ptr<TransferEngine> engine_;
};
}  // namespace ssd_offload
}  // namespace mooncake
