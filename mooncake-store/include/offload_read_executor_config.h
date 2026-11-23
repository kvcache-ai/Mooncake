// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>
#include <optional>

#include "integer_parser.h"

namespace mooncake::detail {

// Validated with concurrent multi-rank SSD restores. Do not scale this pool
// directly with host CPU count: disk and RDMA concurrency are separate limits.
inline constexpr uint32_t kDefaultOffloadReadIoThreads = 64;
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
