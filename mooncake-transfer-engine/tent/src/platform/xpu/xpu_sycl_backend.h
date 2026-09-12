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

// Real oneAPI SYCL backend for the Intel XPU platform.
//
// USE_XPU is a direct-link (native) build: XpuPlatform calls this backend
// directly and libsycl is linked into the binary -- there is no dlopen shim and
// no C ABI boundary. Because this header includes <sycl/sycl.hpp>, every
// translation unit that pulls it in MUST be compiled by the Intel DPC++
// compiler (icpx / clang with -fsycl); plain g++ cannot parse the SYCL headers.
// It is therefore a private header of platform_xpu (kept under src/, not the
// public include tree) and is included only by xpu_platform.cpp.
//
// Device selection prefers Intel GPUs but falls back to any available SYCL
// device (e.g. the OpenCL CPU runtime shipped in the oneAPI images), so the USM
// alloc/copy path is exercisable even on a host with no Intel GPU.

#ifndef TENT_SRC_PLATFORM_XPU_SYCL_BACKEND_H_
#define TENT_SRC_PLATFORM_XPU_SYCL_BACKEND_H_

#include <sycl/sycl.hpp>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

namespace mooncake {
namespace tent {

// Minimal USM-device backend with an interior-pointer registry.
//
// SYCL's sycl::get_pointer_type only classifies base addresses reliably across
// backends, but the platform requires classifying addresses interior to an
// allocation (staging hands the backend base + chunk_offset). We therefore keep
// our own [base, base + size) interval registry and resolve interior addresses
// against it, backed by real SYCL allocations.
//
// Every method returns 0 on success and non-zero on failure (classification
// helpers return bool / an ordinal); XpuPlatform maps these to Status.
class XpuSyclBackend {
   public:
    static XpuSyclBackend &instance() {
        static XpuSyclBackend g;
        return g;
    }

    // Enumerate devices and build one in-order queue per device. Idempotent.
    int init() {
        std::lock_guard<std::mutex> lock(mu_);
        if (initialized_) return 0;
        try {
            // Only keep devices that support USM device allocations; a device
            // that lacks this aspect would make probe() advertise an XPU memory
            // node whose every subsequent allocation then fails.
            auto usable = [](const sycl::device &d) {
                return d.has(sycl::aspect::usm_device_allocations);
            };
            std::vector<sycl::device> devices;
            for (const auto &d : sycl::device::get_devices()) {
                if (d.is_gpu() && usable(d)) devices.push_back(d);
            }
            if (devices.empty()) {
                // No usable Intel GPU visible -- fall back to any device that
                // still supports USM device allocations (e.g. the OpenCL CPU
                // runtime shipped in the oneAPI images) so the USM path stays
                // exercisable.
                for (const auto &d : sycl::device::get_devices()) {
                    if (usable(d)) devices.push_back(d);
                }
            }
            if (devices.empty()) return 1;
            for (auto &d : devices) {
                queues_.emplace_back(d, sycl::property::queue::in_order());
            }
            initialized_ = true;
            return 0;
        } catch (const sycl::exception &) {
            // Leave no partially-built state: a later init() retry must start
            // from an empty queue list, not resume with duplicate ordinals.
            queues_.clear();
            return 1;
        }
    }

    int allocDevice(void **pptr, size_t size, int device_index) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!initialized_ || device_index < 0 ||
            device_index >= static_cast<int>(queues_.size()) || !pptr)
            return 1;
        try {
            void *p = sycl::malloc_device(size, queues_[device_index]);
            if (!p) return 1;
            allocs_.push_back(
                Alloc{reinterpret_cast<uintptr_t>(p), size, device_index});
            *pptr = p;
            return 0;
        } catch (const sycl::exception &) {
            return 1;
        }
    }

    int freeDevice(void *ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!ptr) return 1;
        const uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
        for (size_t i = 0; i < allocs_.size(); ++i) {
            if (allocs_[i].base == addr) {
                try {
                    sycl::free(ptr, queues_[allocs_[i].device]);
                } catch (const sycl::exception &) {
                    return 1;
                }
                allocs_.erase(allocs_.begin() + i);
                return 0;
            }
        }
        return 1;  // not a base pointer we handed out
    }

    bool isDevicePtr(const void *addr) {
        std::lock_guard<std::mutex> lock(mu_);
        return findLocked(addr) != nullptr;
    }

    int deviceIndex(const void *addr) {
        std::lock_guard<std::mutex> lock(mu_);
        const Alloc *a = findLocked(addr);
        return a ? a->device : -1;
    }

    int copyD2H(void *host_dst, const void *device_src, size_t len) {
        return copy(host_dst, device_src, len, /*to_host=*/true);
    }

    int copyH2D(void *device_dst, const void *host_src, size_t len) {
        return copy(device_dst, host_src, len, /*to_host=*/false);
    }

    int deviceCount() {
        std::lock_guard<std::mutex> lock(mu_);
        return static_cast<int>(queues_.size());
    }

    // NUMA affinity of the device's PCIe root. Returning -1 means "unknown",
    // which the topology layer treats as "not cross-NUMA" (isCrossNuma), so
    // staging still works -- just without NUMA-local placement of the host
    // bounce buffer. A real value would come from the device's PCI address
    // (Intel's pci_address device-info extension) resolved against
    // /sys/bus/pci/devices/<bdf>/numa_node; that lookup is deferred.
    int deviceNumaNode(int /*index*/) { return -1; }

   private:
    struct Alloc {
        uintptr_t base;
        size_t size;
        int device;
    };

    // Resolve an address (base or interior) to its owning allocation.
    const Alloc *findLocked(const void *addr) const {
        const uintptr_t a = reinterpret_cast<uintptr_t>(addr);
        for (const auto &al : allocs_) {
            if (a >= al.base && a < al.base + al.size) return &al;
        }
        return nullptr;
    }

    int copy(void *dst, const void *src, size_t len, bool to_host) {
        // Validate and resolve the target queue under the lock, but run the
        // blocking memcpy().wait() outside it so copies on different devices are
        // not serialized against one another. queues_ is only appended during
        // init() and never erased, so the queue handle stays valid after the
        // lock is released.
        std::optional<sycl::queue> q;
        {
            std::lock_guard<std::mutex> lock(mu_);
            const void *dev = to_host ? src : dst;
            const Alloc *a = findLocked(dev);
            if (!a) return 1;  // device side must be a known allocation
            const uintptr_t start = reinterpret_cast<uintptr_t>(dev);
            if (start + len > a->base + a->size) return 1;  // runs past end
            q = queues_[a->device];
        }
        try {
            q->memcpy(dst, src, len).wait();
            return 0;
        } catch (const sycl::exception &) {
            return 1;
        }
    }

    std::mutex mu_;
    bool initialized_ = false;
    std::vector<sycl::queue> queues_;
    std::vector<Alloc> allocs_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SRC_PLATFORM_XPU_SYCL_BACKEND_H_
