// Copyright 2026 KVCache.AI
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

#ifndef TENT_SRC_PLATFORM_XPU_DMABUF_UTILS_H_
#define TENT_SRC_PLATFORM_XPU_DMABUF_UTILS_H_

#include <cstdint>

#include "tent/common/status.h"

namespace mooncake::tent::xpu_detail {

// Device-memory errors must not trigger the host ibv_reg_mr fallback. Check
// offset before subtracting, and reject the full range rather than clipping it.
inline Status validateDmabufRange(uint64_t offset, uint64_t length,
                                  uint64_t allocation_size) {
    if (offset > allocation_size || length > allocation_size - offset)
        return Status::InternalError(
            "range spans more than one XPU allocation" LOC_MARK);
    return Status::OK();
}

// Only an explicit export allocation establishes that any extra backing bytes
// are padding, not another allocation in a pool. For foreign/fallback USM,
// require an exact size match: rounding up to a guessed page size is ambiguous.
inline bool hasDedicatedDmabuf(uint64_t allocation_size, uint64_t backing_size,
                               bool explicitly_exported) {
    return allocation_size > 0 && backing_size >= allocation_size &&
           (explicitly_exported || backing_size == allocation_size);
}

}  // namespace mooncake::tent::xpu_detail

#endif  // TENT_SRC_PLATFORM_XPU_DMABUF_UTILS_H_
