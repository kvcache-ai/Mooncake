// Copyright 2024 KVCache.AI
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

#ifndef HIP_GEM_PIN_H
#define HIP_GEM_PIN_H

#include <functional>

namespace mooncake {

// The RDMA device, of devices 0..count-1, that pinned a new buffer of
// hip_gem_allocator.so with a one-page dma-buf MR, or -1 if none did.
// `preferred` (the device that pinned the previous buffer, or -1) is tried
// first, then the others in order. A device can allocate a protection domain
// and still reject dma-buf MRs, so only a successful pin makes it preferred.
inline int pickPinDevice(int count, int preferred,
                         const std::function<bool(int)> &try_pin) {
    if (preferred >= 0 && preferred < count && try_pin(preferred))
        return preferred;
    for (int i = 0; i < count; ++i)
        if (i != preferred && try_pin(i)) return i;
    return -1;
}

}  // namespace mooncake

#endif  // HIP_GEM_PIN_H
