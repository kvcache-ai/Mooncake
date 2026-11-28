// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>
#include <optional>

#include "integer_parser.h"

namespace mooncake::detail {

// Conservative per-client default to avoid thread explosion in multi-client
// processes; override via MOONCAKE_OFFLOAD_READ_IO_THREADS (1..128) for
// high-concurrency multi-rank SSD restores.
inline constexpr uint32_t kDefaultOffloadReadIoThreads = 8;
inline constexpr uint32_t kMaxOffloadReadIoThreads = 128;

inline std::optional<uint32_t> ParseOffloadReadIoThreads(const char* value) {
    if (!value || !*value) return kDefaultOffloadReadIoThreads;
    auto parsed = TryParseInteger<uint32_t>(
        value, {.trim_ascii_whitespace = true, .allow_leading_plus = true});
    if (!parsed || *parsed == 0 || *parsed > kMaxOffloadReadIoThreads) {
        return std::nullopt;
    }
    return parsed;
}

}  // namespace mooncake::detail
