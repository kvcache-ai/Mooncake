// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

// Shared internal helpers used by multiple client_service*.cpp
// translation units, extracted verbatim from client_service.cpp's
// anonymous namespace when the file was split. Nothing here is part of
// the public Client API; do not include this header outside
// mooncake-store/src.

#include <cstdint>
#include <limits>
#include <optional>
#include <span>
#include <vector>

#include "types.h"

namespace mooncake {
namespace detail {

struct ContiguousSliceRange {
    void* ptr = nullptr;
    size_t size = 0;
};

inline std::optional<ContiguousSliceRange> GetContiguousSliceRange(
    std::span<const Slice> slices) {
    if (slices.empty()) {
        return std::nullopt;
    }

    uintptr_t expected_ptr = 0;
    uintptr_t start_ptr = 0;
    size_t total_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        const auto& slice = slices[i];
        if (slice.ptr == nullptr) {
            return std::nullopt;
        }

        const auto current_ptr = reinterpret_cast<uintptr_t>(slice.ptr);
        if (i == 0) {
            start_ptr = current_ptr;
            expected_ptr = current_ptr;
        } else if (current_ptr != expected_ptr) {
            return std::nullopt;
        }

        if (slice.size > std::numeric_limits<size_t>::max() - total_size) {
            return std::nullopt;
        }
        if (slice.size > std::numeric_limits<uintptr_t>::max() - current_ptr) {
            return std::nullopt;
        }

        total_size += slice.size;
        expected_ptr = current_ptr + slice.size;
    }

    if (total_size == 0) {
        return std::nullopt;
    }

    return ContiguousSliceRange{.ptr = reinterpret_cast<void*>(start_ptr),
                                .size = total_size};
}

[[nodiscard]] inline size_t CalculateSliceSize(
    const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

[[nodiscard]] inline size_t CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

}  // namespace detail
}  // namespace mooncake
