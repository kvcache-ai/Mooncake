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

#ifndef HIP_GEM_LOOKUP_H
#define HIP_GEM_LOOKUP_H

#include <dlfcn.h>

#include <atomic>
#include <cstddef>

namespace mooncake {

// A buffer of hip_gem_allocator.so: an amdgpu GEM BO imported into HIP, which
// HIP neither reports an address range nor an IPC handle for, and which ROCr
// cannot export as a dma-buf. It must be registered with the BO's own dma-buf
// and never through the amdgpu peer-memory client (ibv_reg_mr), whose
// amd_get_pages dereferences the NULL kfd_bo of a GEM BO.
struct HipGemBuffer {
    void *base = nullptr;
    size_t size = 0;
    int fd = -1;  // dup of the BO's dma-buf fd when requested; caller closes
    int device = -1;
};

// True when addr lies in a buffer of hip_gem_allocator.so. The allocator is
// found at run time (mooncake.allocator.HipGemAllocator loads it with
// RTLD_GLOBAL), so the transfer engine has no link-time dependency on it.
// Finding it is a dlsym() over every loaded library, so per-transfer paths
// pass resolve = false and only use an allocator an earlier call found: a
// buffer is registered, and looked up there, before it is transferred.
// With want_fd, out.fd receives a new descriptor the caller must close.
inline bool lookupHipGemBuffer(const void *addr, HipGemBuffer &out,
                               bool want_fd, bool resolve = true) {
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
    return fn(addr, &out.base, &out.size, want_fd ? &out.fd : nullptr,
              &out.device) == 0;
}

// The device of the HIP device memory at addr, given the one
// hipPointerGetAttributes reported. HIP reports memory imported with
// hipImportExternalMemory as device 0's whichever device imported it, so for a
// hip_gem_allocator.so buffer this is the device it was allocated on.
inline int hipDeviceOfPointer(const void *addr, int reported_device,
                              bool resolve = true) {
    HipGemBuffer gem;
    return lookupHipGemBuffer(addr, gem, false, resolve) ? gem.device
                                                         : reported_device;
}

}  // namespace mooncake

#endif  // HIP_GEM_LOOKUP_H
