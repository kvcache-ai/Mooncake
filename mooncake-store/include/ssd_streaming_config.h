// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "offset_allocator/offset_allocator.h"
#include "ssd_read_protocol.h"

namespace mooncake::ssd_offload {

// Delegates to OffsetAllocator::slotBudget() so config validation stays in
// lockstep with OffsetAllocator's bin rounding and multiplier calculation.
inline std::pair<uint64_t, uint64_t> OffsetAllocatorSlotBudget(
    size_t slot_bytes, uint64_t arena_bytes) noexcept {
    return mooncake::offset_allocator::OffsetAllocator::slotBudget(slot_bytes,
                                                                   arena_bytes);
}

struct StreamingReadConfig {
    // Owner-push accommodates 128-page MHA batches (256 keys) without changing
    // the protocol's default 64-object limit for direct callers.
    static constexpr size_t kPushMaxBatchObjects = 512;

    bool enabled{false};
    bool push_enabled{false};
    bool use_uring{false};
    size_t slot_bytes{1024 * 1024};
    size_t slots{4};
    size_t read_workers{4};
    size_t write_workers{2};
    size_t max_outstanding{64};
    size_t max_chunks{65536};
    size_t max_fragments{65536};

    size_t DefaultMaxOutstanding() const {
        return push_enabled ? kPushMaxBatchObjects : max_outstanding;
    }

    SsdReadProtocolLimits ProtocolLimits() const {
        SsdReadProtocolLimits limits;
        if (push_enabled) {
            limits.max_objects = kPushMaxBatchObjects;
        }
        return limits;
    }

    bool Validate(uint64_t arena_bytes) const {
        if (!enabled && !push_enabled) return true;
        if (slot_bytes == 0 || slot_bytes > 64ULL * 1024 * 1024 || slots == 0 ||
            slots > 64 || read_workers == 0 || read_workers > 64 ||
            write_workers == 0 || write_workers > 64 ||
            max_outstanding < slots || max_outstanding > 65536 ||
            read_workers > max_outstanding || max_chunks == 0 ||
            max_fragments == 0) {
            return false;
        }
        if (push_enabled && max_outstanding < 2 * slots) {
            return false;
        }
        const auto [normalized_slot, effective_arena] =
            OffsetAllocatorSlotBudget(slot_bytes, arena_bytes);
        return normalized_slot > 0 &&
               normalized_slot <= effective_arena / slots;
    }
};

}  // namespace mooncake::ssd_offload
