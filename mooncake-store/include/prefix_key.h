// Copyright 2025 Alibaba Cloud and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#pragma once

#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <string>

namespace mooncake {

// Forge RL Design 01 §3.4 — canonical encoding for chained-prefix hashes.
//
// The master indexes prefix-chain entries in the same string-keyed metadata
// shard map that backs ordinary objects. To avoid collisions with user keys
// and to keep the encoding stable across server / client / test / bench, we
// share a single helper here.
//
// Format: "__pfx__" + zero-padded 16-char lowercase hex of the uint64 hash.
// PRIx64 is used so the formatter is correct on platforms where
// `unsigned long` is 32-bit (Windows MSVC).
inline std::string MakePrefixHashKey(uint64_t hash) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "__pfx__%016" PRIx64, hash);
    return std::string(buf);
}

// Alias for callers that prefer the encode/decode wording.
inline std::string EncodePrefixKey(uint64_t hash) {
    return MakePrefixHashKey(hash);
}

}  // namespace mooncake
