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

#ifndef TENT_PLATFORM_TPU_PJRT_SHIM_H_
#define TENT_PLATFORM_TPU_PJRT_SHIM_H_

#include <cstddef>
#include <cstdint>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

// TpuPjrtShim isolates every TPU/PJRT dependency behind a narrow interface.
//
// The TPU device I/O primitives (HBM<->host DMA, device-pointer classification,
// device topology) are provided by a separate adapter shared library built
// against the PJRT runtime. TENT itself carries no build-time dependency on
// that runtime: the adapter is resolved at runtime via dlopen(), matching the
// way the other accelerator backends keep vendor SDKs out of the core build.
//
// The adapter library must export the C ABI declared in tpu_pjrt_abi.h. Its
// path defaults to "libmooncake_tpu_pjrt.so" and can be overridden with the
// MC_TPU_PJRT_LIB environment variable. When the adapter cannot be loaded,
// available() returns false and every operation returns a non-OK Status; this
// keeps a USE_TPU build functional (and unit-testable with a mock adapter)
// without the real runtime present.
class TpuPjrtShim {
   public:
    // Process-wide singleton. The adapter is loaded (and initialized) lazily on
    // first use; loading is attempted at most once.
    static TpuPjrtShim &instance();

    // True when the adapter library was loaded and initialized successfully.
    bool available() const { return available_; }

    // Returns true if `addr` refers to memory owned by the TPU runtime (HBM).
    // Returns false when the adapter is unavailable or the pointer is host
    // memory, so a caller can safely treat "not TPU" as host memory.
    bool isDevicePtr(const void *addr) const;

    // Device ordinal backing `addr`, or -1 if `addr` is not TPU device memory.
    int deviceIndex(const void *addr) const;

    // Synchronous HBM -> host DMA copy of `length` bytes.
    Status copyD2H(void *host_dst, const void *device_src, size_t length) const;

    // Synchronous host -> HBM DMA copy of `length` bytes.
    Status copyH2D(void *device_dst, const void *host_src, size_t length) const;

    // Number of visible TPU devices (0 when the adapter is unavailable).
    int deviceCount() const;

    // NUMA node closest to TPU device `index`, or -1 if unknown.
    int deviceNumaNode(int index) const;

   private:
    TpuPjrtShim();
    ~TpuPjrtShim();
    TpuPjrtShim(const TpuPjrtShim &) = delete;
    TpuPjrtShim &operator=(const TpuPjrtShim &) = delete;

    void load();

    void *handle_ = nullptr;
    bool available_ = false;

    // Resolved adapter entrypoints (see tpu_pjrt_abi.h for the contract).
    int (*fn_init_)() = nullptr;
    int (*fn_is_device_ptr_)(const void *) = nullptr;
    int (*fn_device_index_)(const void *) = nullptr;
    int (*fn_copy_d2h_)(void *, const void *, size_t) = nullptr;
    int (*fn_copy_h2d_)(void *, const void *, size_t) = nullptr;
    int (*fn_device_count_)() = nullptr;
    int (*fn_device_numa_)(int) = nullptr;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLATFORM_TPU_PJRT_SHIM_H_
