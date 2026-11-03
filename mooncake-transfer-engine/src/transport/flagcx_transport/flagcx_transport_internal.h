// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

#ifndef FLAGCX_TRANSPORT_INTERNAL_H_
#define FLAGCX_TRANSPORT_INTERNAL_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace mooncake::flagcx_internal {

inline bool containsRange(uintptr_t region_start, size_t region_length,
                          uintptr_t range_start, size_t range_length) {
    if (range_start < region_start) return false;

    const uintptr_t offset = range_start - region_start;
    return offset <= region_length &&
           range_length <= region_length - static_cast<size_t>(offset);
}

inline uint32_t descriptorLength(size_t transfer_length) {
    return static_cast<uint32_t>(std::min<size_t>(
        transfer_length, std::numeric_limits<uint32_t>::max()));
}

}  // namespace mooncake::flagcx_internal

#endif  // FLAGCX_TRANSPORT_INTERNAL_H_
