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

// Synthetic-address registry for Mooncake's built-in TPU adapter.
//
// TENT is built around host-style pointer arithmetic: it tracks buffers by
// virtual address, divides staged transfers into chunks via pointer arithmetic
// (`addr + chunk_offset`), and expects memory transfers to operate on those
// pointers.
//
// TPU device memory has no CPU virtual addresses. Instead, TPU copy APIs
// address device memory using an opaque handle and an integer byte offset:
// `(mc_tpu_buffer_t handle, size_t offset)`.
//
// This registry bridges the two models:
//
//   * Each registered TPU buffer is assigned a synthetic virtual address:
//     the base of a reserved, inaccessible (PROT_NONE) address range spanning
//     the buffer's size. It costs zero RAM and immediately segfaults if
//     accidentally dereferenced.
//   * When TENT transfers a chunk using an address
//     (`synthetic_addr + chunk_offset`), the registry resolves that address
//     back to the `(buffer_handle, offset)` required by the TPU hardware copy
//     function (`copy_to_host` / `copy_from_host`).

#ifndef TENT_PLUGINS_TPU_SYNTHETIC_ADDRESS_REGISTRY_H_
#define TENT_PLUGINS_TPU_SYNTHETIC_ADDRESS_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

namespace mooncake {
namespace tent {

// Logs to stderr: the process that dlopen()s the adapter may lack glog.
void tpuAdapterLog(const std::string &message);

// One contiguous on-device buffer, e.g. one addressable shard of a JAX array.
// Implementations must be safe to call from several threads at once.
class DeviceRegion {
   public:
    virtual ~DeviceRegion() = default;

    // Size in bytes of the on-device representation.
    virtual size_t size() const = 0;

    // Local device ordinal; becomes the N in TENT's "tpu:N" location.
    virtual int deviceIndex() const = 0;

    // NUMA node closest to the device, or -1 if unknown.
    virtual int numaNode() const = 0;

    // Blocking copies of [offset, offset + len). The registry has already
    // checked the range. Return false on failure.
    virtual bool copyToHost(void *host_dst, size_t offset, size_t len) = 0;
    virtual bool copyFromHost(const void *host_src, size_t offset,
                              size_t len) = 0;
};

class TpuSyntheticAddrRegistry {
   public:
    TpuSyntheticAddrRegistry() = default;
    ~TpuSyntheticAddrRegistry();
    TpuSyntheticAddrRegistry(const TpuSyntheticAddrRegistry &) = delete;
    TpuSyntheticAddrRegistry &operator=(const TpuSyntheticAddrRegistry &) =
        delete;

    static TpuSyntheticAddrRegistry &instance();

    // Registers `region` and returns its synthetic address, or 0 on failure
    // (the region is then destroyed).
    uintptr_t add(std::unique_ptr<DeviceRegion> region);

    // Unregisters the region that corresponds to `synthetic_addr`. Returns
    // false if there is none. Copies already in flight keep the region alive
    // until they finish. The device stays known (see deviceCount) if all
    // buffers are removed.
    bool remove(uintptr_t synthetic_addr);

    // --- Operations behind the mc_tpu_pjrt_* ABI ---------------------------

    // True if `addr` lies inside a registered region (base or interior).
    bool contains(const void *addr) const;

    // Device index of the region containing `addr`, or -1.
    int deviceIndex(const void *addr) const;

    // Same contract as mc_tpu_pjrt_copy_d2h/h2d: 0 on success, 1 on failure.
    int copyD2H(void *host_dst, const void *device_src, size_t len) const;
    int copyH2D(void *device_dst, const void *host_src, size_t len) const;

    // 1 + the highest device index ever registered. TENT reads this once,
    // when the engine is created, so devices are never forgotten on remove().
    int deviceCount() const;

    // NUMA node recorded for device `index`, or -1.
    int deviceNuma(int index) const;

   private:
    struct Entry {
        size_t size = 0;
        std::shared_ptr<DeviceRegion> region;
    };

    // Region and offset of [addr, addr + len); len == 0 checks only addr.
    std::shared_ptr<DeviceRegion> resolve(const void *addr, size_t len,
                                          size_t *offset) const;

    mutable std::shared_mutex mutex_;
    std::map<uintptr_t, Entry> regions_;  // keyed by synthetic base address
    std::map<int, int> device_numa_;      // device index -> NUMA (sticky)
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLUGINS_TPU_SYNTHETIC_ADDRESS_REGISTRY_H_
