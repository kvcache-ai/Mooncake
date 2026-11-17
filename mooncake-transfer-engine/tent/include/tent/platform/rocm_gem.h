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

#ifndef TENT_ROCM_GEM_H
#define TENT_ROCM_GEM_H

#include <dlfcn.h>

#include <atomic>
#include <cstddef>

namespace mooncake {
namespace tent {

// True when addr lies in a buffer of hip_gem_allocator.so
// (mooncake.allocator.HipGemAllocator, which loads it with RTLD_GLOBAL): an
// amdgpu GEM BO imported into HIP. Such a buffer must be registered through
// its own dma-buf (fd: a new descriptor, when requested) and never with
// ibv_reg_mr, which reaches the amdgpu peer-memory client and its
// dereference of the GEM BO's NULL kfd_bo. device: the HIP device it was
// allocated on, which hipPointerGetAttributes misreports (as device 0) for
// imported memory. Finding the allocator is a dlsym() over every loaded
// library, so per-transfer paths pass resolve = false and only use one an
// earlier call (at registration) found. Mirrors the classic transfer engine's
// transport/hip_gem_lookup.h.
inline bool lookupHipGemBuffer(const void *addr, void **base, size_t *size,
                               int *fd, int *device, bool resolve = true) {
    using LookupFn = int (*)(const void *, void **, size_t *, int *, int *);
    static std::atomic<LookupFn> cached{nullptr};
    LookupFn fn = cached.load(std::memory_order_acquire);
    if (!fn) {
        if (!resolve) return false;
        fn = reinterpret_cast<LookupFn>(
            dlsym(RTLD_DEFAULT, "mc_hip_gem_lookup"));
        if (!fn) return false;
        cached.store(fn, std::memory_order_release);
    }
    return fn(addr, base, size, fd, device) == 0;
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_ROCM_GEM_H
