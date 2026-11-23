// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstddef>
#include <cstdint>

namespace mooncake::ssd_offload {

struct StreamingReadConfig {
    bool enabled{false};
    bool use_uring{false};
    bool fixed_buffers{false};
    size_t slot_bytes{1024 * 1024};
    size_t slots{4};
    size_t read_workers{4};
    size_t write_workers{2};
    size_t max_outstanding{64};
    size_t max_chunks{65536};
    size_t max_fragments{65536};

    bool Validate(uint64_t arena_bytes) const {
        if (!enabled) return true;
        return slot_bytes > 0 && slot_bytes <= 64ULL * 1024 * 1024 &&
               slots > 0 && slots <= 64 && read_workers > 0 &&
               read_workers <= 64 && write_workers > 0 && write_workers <= 64 &&
               max_outstanding >= slots && max_outstanding <= 65536 &&
               max_chunks > 0 && max_fragments > 0 &&
               slot_bytes <= arena_bytes / slots;
    }
};

}  // namespace mooncake::ssd_offload
