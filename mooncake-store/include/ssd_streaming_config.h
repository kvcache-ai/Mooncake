// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstddef>
#include <cstdint>
#include "ssd_read_protocol.h"

namespace mooncake::ssd_offload {

struct StreamingReadConfig {
    // Runtime Push accommodates the existing 128-page MHA batch (256 keys),
    // without changing the legacy advanced protocol's default of 64 objects.
    static constexpr size_t kPushMaxBatchObjects = 512;
    bool enabled{false};
    size_t slot_bytes{1024 * 1024};
    size_t slots{4};
    size_t read_workers{4};
    size_t write_workers{2};
    size_t max_outstanding{64};
    size_t max_chunks{65536};
    size_t max_fragments{65536};
    bool push_enabled{false};
    bool use_uring{false};
    // Explicit file I/O policy, independent of the streaming executor.
    // Direct reads require 4 KiB-aligned staging and never retry buffered.
    bool direct_io{false};
    // Optional request-local registration of the existing staging slots.
    // Failure retains ordinary READ on the same file I/O policy.
    bool fixed_buffers{false};

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
        constexpr size_t alignment = 4096;
        if (slot_bytes < alignment || slot_bytes > 64ULL * 1024 * 1024 ||
            slot_bytes % alignment != 0 || slots == 0 || slots > 64 ||
            read_workers == 0 || read_workers > 64 || write_workers == 0 ||
            write_workers > 64 || max_outstanding < 2 * slots ||
            max_outstanding > 65536 || max_chunks == 0 || max_fragments == 0)
            return false;
        // Each allocator reservation includes room to align its base.
        return (slot_bytes + alignment - 1) <= arena_bytes / slots;
    }
};

}  // namespace mooncake::ssd_offload
