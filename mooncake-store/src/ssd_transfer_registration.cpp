// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_transfer_registration.h"
#include "transfer_engine.h"
#include <limits>

namespace mooncake::ssd_offload {
TransferEngineDestinationRegistration::TransferEngineDestinationRegistration(
    std::shared_ptr<TransferEngine> engine)
    : engine_(std::move(engine)) {}
TransportRegistrationResult
TransferEngineDestinationRegistration::RegisterWritable(
    const DestinationBinding& binding, const std::string& location) noexcept {
    if (!engine_ || engine_->isUsingTent() ||
        binding.memory != DestinationMemory::kHost)
        return {DestinationError::kNotSupported, false};
    if (!binding.registered_base || !binding.registered_bytes ||
        location.empty() ||
        binding.registered_base > std::numeric_limits<uintptr_t>::max() ||
        binding.registered_bytes > std::numeric_limits<size_t>::max() ||
        binding.registered_bytes >
            std::numeric_limits<uintptr_t>::max() - binding.registered_base)
        return {DestinationError::kInvalidArgument, false};
    try {
        const int rc = engine_->registerLocalMemory(
            reinterpret_cast<void*>(binding.registered_base),
            static_cast<size_t>(binding.registered_bytes), location,
            /*remote_accessible=*/true, /*update_metadata=*/true);
        if (rc == 0) return {DestinationError::kOk, true, true};
        // TE's overlap reservation fails before calling any transport. Never
        // unregister a region owned by another registration API on this error.
        if (rc == ERR_ADDRESS_OVERLAPPED)
            return {DestinationError::kOverlap, false};
    } catch (...) {
        // A transport may have partially registered/published before failure.
    }
    // TE rollback is best-effort. Failure may also precede the overlap check,
    // so it cannot establish ownership of any existing MR at this base. Keep
    // backing quarantined, but never blindly unregister a possibly foreign MR.
    return {DestinationError::kTransportError, true, false};
}
DestinationError TransferEngineDestinationRegistration::Unregister(
    uint64_t base) noexcept {
    if (!engine_ || engine_->isUsingTent())
        return DestinationError::kNotSupported;
    try {
        if (engine_->unregisterLocalMemory(reinterpret_cast<void*>(base),
                                           true) == 0)
            return DestinationError::kOk;
    } catch (...) {
    }
    return DestinationError::kTransportError;
}
}  // namespace mooncake::ssd_offload
