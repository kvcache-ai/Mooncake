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
// Level Zero interop: sycl::get_native<ext_oneapi_level_zero> hands back the
// ze_context_handle_t behind a SYCL context, which is what the dma-buf export
// below needs. <level_zero/ze_api.h> supplies the types (it ships with the
// oneAPI toolchain / level-zero-dev); the functions themselves are resolved
// from libze_loader at runtime (see ZeLoader), so platform_xpu adds no
// link-time dependency beyond libsycl.
#include <level_zero/ze_api.h>
#include <sycl/ext/oneapi/backend/level_zero.hpp>

#include <dlfcn.h>
#include <unistd.h>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

namespace mooncake {
namespace tent {

// The Level Zero entry points the dma-buf path needs, resolved lazily from
// the loader the SYCL runtime already has open (libze_loader.so.1). The SYCL
// Level Zero adapter has called zeInit by the time a SYCL context exists, so
// no separate initialisation is performed here.
class ZeLoader {
   public:
    static ZeLoader &instance() {
        static ZeLoader g;
        return g;
    }

    // Export (zeMemGetAllocProperties + zeMemGetAddressRange) is available.
    bool available() const { return get_alloc_properties_ && get_range_; }
    // Exportable allocation (zeMemAllocDevice + zeMemFree) is available.
    bool canAllocate() const { return alloc_device_ && mem_free_; }

    ze_result_t memAllocDevice(ze_context_handle_t ctx,
                               const ze_device_mem_alloc_desc_t *desc,
                               size_t size, size_t alignment,
                               ze_device_handle_t device, void **pptr) const {
        return alloc_device_(ctx, desc, size, alignment, device, pptr);
    }

    ze_result_t memFree(ze_context_handle_t ctx, void *ptr) const {
        return mem_free_(ctx, ptr);
    }

    ze_result_t memGetAllocProperties(ze_context_handle_t ctx, const void *ptr,
                                      ze_memory_allocation_properties_t *props,
                                      ze_device_handle_t *device) const {
        return get_alloc_properties_(ctx, ptr, props, device);
    }

    ze_result_t memGetAddressRange(ze_context_handle_t ctx, const void *ptr,
                                   void **base, size_t *size) const {
        return get_range_(ctx, ptr, base, size);
    }

   private:
    ZeLoader() {
        handle_ = dlopen("libze_loader.so.1", RTLD_NOW | RTLD_LOCAL);
        if (!handle_) return;
        get_alloc_properties_ = reinterpret_cast<GetAllocProperties>(
            dlsym(handle_, "zeMemGetAllocProperties"));
        get_range_ = reinterpret_cast<GetAddressRange>(
            dlsym(handle_, "zeMemGetAddressRange"));
        alloc_device_ =
            reinterpret_cast<AllocDevice>(dlsym(handle_, "zeMemAllocDevice"));
        mem_free_ = reinterpret_cast<MemFree>(dlsym(handle_, "zeMemFree"));
    }
    ~ZeLoader() = default;  // Keep the loader mapped for the process lifetime.

    using GetAllocProperties = ze_result_t (*)(
        ze_context_handle_t, const void *, ze_memory_allocation_properties_t *,
        ze_device_handle_t *);
    using GetAddressRange = ze_result_t (*)(ze_context_handle_t, const void *,
                                            void **, size_t *);
    using AllocDevice = ze_result_t (*)(ze_context_handle_t,
                                        const ze_device_mem_alloc_desc_t *,
                                        size_t, size_t, ze_device_handle_t,
                                        void **);
    using MemFree = ze_result_t (*)(ze_context_handle_t, void *);

    void *handle_ = nullptr;
    GetAllocProperties get_alloc_properties_ = nullptr;
    GetAddressRange get_range_ = nullptr;
    AllocDevice alloc_device_ = nullptr;
    MemFree mem_free_ = nullptr;
};

// Minimal USM-device backend with an interior-pointer registry.
//
// Two classification paths are combined:
//  1. Allocations made through this backend are tracked in a [base, base+size)
//    interval registry, so interior addresses (staging hands the backend
//    base + chunk_offset) resolve without touching the SYCL runtime.
//  2. Anything else is classified with sycl::get_pointer_type against the
//    platform default context. That is the context PyTorch's XPU allocator
//    uses (device.get_platform().ext_oneapi_get_default_context()), so a
//    PyTorch XPU tensor handed to registerLocalMemory is recognised as device
//    memory and can be copied. Queues are created on that same default
//    context so queue::memcpy on foreign USM is legal. Only usm::alloc::device
//    is reported as device memory; host/shared USM is host-accessible and is
//    treated as ordinary host memory.
//
// Every method returns 0 on success and non-zero on failure (classification
// helpers return bool / an ordinal); XpuPlatform maps these to Status.
class XpuSyclBackend {
   public:
    static XpuSyclBackend &instance() {
        // Intentionally leaked: SYCL queues/contexts must not be destroyed
        // during static teardown, after the SYCL runtime may already be gone.
        static XpuSyclBackend *g = new XpuSyclBackend();
        return *g;
    }

    // Enumerate devices and build one in-order queue per device on the
    // platform default context. Idempotent.
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
                sycl::context ctx =
                    d.get_platform().ext_oneapi_get_default_context();
                bool known = false;
                for (const auto &c : contexts_) {
                    if (c == ctx) {
                        known = true;
                        break;
                    }
                }
                if (!known) contexts_.push_back(ctx);
                queues_.emplace_back(ctx, d, sycl::property::queue::in_order());
            }
            initialized_ = true;
            return 0;
        } catch (const sycl::exception &) {
            // Leave no partially-built state: a later init() retry must start
            // from an empty queue list, not resume with duplicate ordinals.
            queues_.clear();
            contexts_.clear();
            return 1;
        }
    }

    // Allocate USM device memory on `device_index`.
    //
    // On the Level Zero backend the allocation is made directly through
    // zeMemAllocDevice with a ze_external_memory_export_desc_t(DMA_BUF) so
    // that it is exportable as its own dma-buf. This matters because the
    // Intel compute runtime pools small device allocations (below a few MB)
    // into shared 2 MB / 16 MB buffers: a pooled allocation exports the
    // *pool's* dma-buf, and there is no public API to learn the allocation's
    // offset inside it, so it cannot be registered for direct RDMA (see
    // exportDmabuf). Requesting export at allocation time makes the runtime
    // skip the pool. The pointer is ordinary USM in the queue's context, so
    // every other operation (queue::memcpy, get_pointer_type) works as
    // usual; it must simply be released with zeMemFree, which freeDevice
    // does. Everything else falls back to sycl::malloc_device.
    int allocDevice(void **pptr, size_t size, int device_index) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!initialized_ || device_index < 0 ||
            device_index >= static_cast<int>(queues_.size()) || !pptr)
            return 1;
        sycl::queue &q = queues_[device_index];
        void *p = nullptr;
        bool ze_owned = false;
        try {
            auto &ze = ZeLoader::instance();
            if (ze.canAllocate() && q.get_context().get_backend() ==
                                        sycl::backend::ext_oneapi_level_zero) {
                auto ze_ctx =
                    sycl::get_native<sycl::backend::ext_oneapi_level_zero>(
                        q.get_context());
                auto ze_dev =
                    sycl::get_native<sycl::backend::ext_oneapi_level_zero>(
                        q.get_device());
                ze_external_memory_export_desc_t export_desc{};
                export_desc.stype =
                    ZE_STRUCTURE_TYPE_EXTERNAL_MEMORY_EXPORT_DESC;
                export_desc.flags = ZE_EXTERNAL_MEMORY_TYPE_FLAG_DMA_BUF;
                ze_device_mem_alloc_desc_t desc{};
                desc.stype = ZE_STRUCTURE_TYPE_DEVICE_MEM_ALLOC_DESC;
                desc.pNext = &export_desc;
                if (ze.memAllocDevice(ze_ctx, &desc, size, /*alignment=*/0,
                                      ze_dev, &p) == ZE_RESULT_SUCCESS &&
                    p) {
                    ze_owned = true;
                } else {
                    p = nullptr;
                }
            }
            if (!p) p = sycl::malloc_device(size, q);
            if (!p) return 1;
            allocs_.push_back(Alloc{reinterpret_cast<uintptr_t>(p), size,
                                    device_index, ze_owned});
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
            if (allocs_[i].base != addr) continue;
            sycl::queue &q = queues_[allocs_[i].device];
            try {
                if (allocs_[i].ze_owned) {
                    auto ze_ctx =
                        sycl::get_native<sycl::backend::ext_oneapi_level_zero>(
                            q.get_context());
                    if (ZeLoader::instance().memFree(ze_ctx, ptr) !=
                        ZE_RESULT_SUCCESS)
                        return 1;
                } else {
                    sycl::free(ptr, q);
                }
            } catch (const sycl::exception &) {
                return 1;
            }
            allocs_.erase(allocs_.begin() + i);
            return 0;
        }
        return 1;  // not a base pointer we handed out
    }

    bool isDevicePtr(const void *addr) {
        std::lock_guard<std::mutex> lock(mu_);
        return classifyLocked(addr) >= 0;
    }

    int deviceIndex(const void *addr) {
        std::lock_guard<std::mutex> lock(mu_);
        return classifyLocked(addr);
    }

    int copyD2H(void *host_dst, const void *device_src, size_t len) {
        return copy(host_dst, device_src, len, /*to_host=*/true);
    }

    int copyH2D(void *device_dst, const void *host_src, size_t len) {
        return copy(device_dst, host_src, len, /*to_host=*/false);
    }

    enum ExportResult {
        kExportOk = 0,
        // Not device memory, not Level Zero, or the driver refused.
        kExportUnavailable = 1,
        // The allocation lives in a runtime-managed pool whose dma-buf it
        // shares with other allocations; its offset inside is unknowable.
        kExportPooled = 2,
    };

    // Export the USM device allocation containing `addr` as a dma-buf fd.
    // This is the Level Zero external-memory export used by libfabric's ZE
    // HMEM support: zeMemGetAllocProperties with a
    // ze_external_memory_export_fd_t(DMA_BUF) extension returns an fd for the
    // allocation, and zeMemGetAddressRange gives its base so the caller can
    // compute the offset of `addr` inside it. Works for allocations made here
    // and for foreign USM (e.g. PyTorch XPU tensors) alike, since the export
    // is a property of the allocation, not of how it was created.
    //
    // One caveat for foreign memory: the compute runtime pools small device
    // allocations into shared buffers, and for those the exported fd is the
    // pool's dma-buf while zeMemGetAddressRange still describes the
    // sub-allocation. The pool offset is not exposed through any public API,
    // so such an allocation cannot be registered correctly -- registering it
    // at offset 0 would silently alias whatever sits at the pool's start.
    // Pooling is detected by comparing the dma-buf's size with the
    // allocation's: a dedicated dma-buf is the allocation rounded up to the
    // runtime's page granularity (64 KB below 2 MB, 2 MB above), anything
    // larger is a shared pool. Allocations made by allocDevice() request
    // export up front and therefore never land in a pool; users who need
    // direct RDMA on pooled foreign memory can disable pooling with
    // NEOReadDebugKeys=1 EnableDeviceUsmAllocationPool=0.
    //
    // Returns kExportOk and fills fd/offset (and the allocation's size, so
    // the caller can check that its range stays inside) on success. The fd is
    // owned by the Level Zero runtime, which caches it per allocation and
    // returns the same descriptor on every export; callers must not close it.
    // Any other result means callers fall back to host staging.
    int exportDmabuf(const void *addr, int *fd, uint64_t *offset,
                     uint64_t *alloc_size) {
        if (!fd || !offset || !alloc_size) return kExportUnavailable;
        std::optional<sycl::context> ctx;
        {
            std::lock_guard<std::mutex> lock(mu_);
            int device_index = classifyLocked(addr);
            if (device_index < 0) return 1;
            ctx = queues_[device_index].get_context();
        }
        auto &ze = ZeLoader::instance();
        if (!ze.available()) return kExportUnavailable;
        ze_context_handle_t ze_ctx = nullptr;
        try {
            if (ctx->get_backend() != sycl::backend::ext_oneapi_level_zero)
                return kExportUnavailable;
            ze_ctx =
                sycl::get_native<sycl::backend::ext_oneapi_level_zero>(*ctx);
        } catch (const sycl::exception &) {
            return kExportUnavailable;
        }
        void *base = nullptr;
        size_t size = 0;
        if (ze.memGetAddressRange(ze_ctx, addr, &base, &size) !=
                ZE_RESULT_SUCCESS ||
            !base)
            return kExportUnavailable;
        ze_external_memory_export_fd_t export_fd{};
        export_fd.stype = ZE_STRUCTURE_TYPE_EXTERNAL_MEMORY_EXPORT_FD;
        export_fd.flags = ZE_EXTERNAL_MEMORY_TYPE_FLAG_DMA_BUF;
        export_fd.fd = -1;
        ze_memory_allocation_properties_t props{};
        props.stype = ZE_STRUCTURE_TYPE_MEMORY_ALLOCATION_PROPERTIES;
        props.pNext = &export_fd;
        ze_device_handle_t device = nullptr;
        if (ze.memGetAllocProperties(ze_ctx, addr, &props, &device) !=
                ZE_RESULT_SUCCESS ||
            props.type != ZE_MEMORY_TYPE_DEVICE || export_fd.fd < 0)
            return kExportUnavailable;
        const off_t dmabuf_size = lseek(export_fd.fd, 0, SEEK_END);
        if (dmabuf_size < 0) return kExportUnavailable;
        const size_t granule =
            size >= (size_t{2} << 20) ? (size_t{2} << 20) : (size_t{64} << 10);
        const size_t rounded = (size + granule - 1) / granule * granule;
        if (static_cast<size_t>(dmabuf_size) > rounded) return kExportPooled;
        *fd = export_fd.fd;
        *offset = reinterpret_cast<uintptr_t>(addr) -
                  reinterpret_cast<uintptr_t>(base);
        *alloc_size = size;
        return kExportOk;
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
        bool ze_owned;  // allocated with zeMemAllocDevice, freed with zeMemFree
    };

    // Resolve an address (base or interior) to its owning allocation.
    const Alloc *findLocked(const void *addr) const {
        const uintptr_t a = reinterpret_cast<uintptr_t>(addr);
        for (const auto &al : allocs_) {
            if (a >= al.base && a < al.base + al.size) return &al;
        }
        return nullptr;
    }

    // Device ordinal owning `addr`, or -1 when it is not USM device memory.
    // Own allocations resolve through the registry; anything else is asked of
    // the SYCL runtime against every default context we hold.
    int classifyLocked(const void *addr) const {
        if (const Alloc *a = findLocked(addr)) return a->device;
        for (const auto &ctx : contexts_) {
            try {
                if (sycl::get_pointer_type(addr, ctx) !=
                    sycl::usm::alloc::device)
                    continue;
                sycl::device owner = sycl::get_pointer_device(addr, ctx);
                for (size_t i = 0; i < queues_.size(); ++i) {
                    if (queues_[i].get_device() == owner)
                        return static_cast<int>(i);
                }
                // Device memory on a GPU we did not enumerate: still device
                // memory; the context is shared, so device 0's queue can copy.
                return 0;
            } catch (const sycl::exception &) {
                // Not a pointer this context knows about.
            }
        }
        return -1;
    }

    int copy(void *dst, const void *src, size_t len, bool to_host) {
        // Validate and resolve the target queue under the lock, but run the
        // blocking memcpy().wait() outside it so copies on different devices
        // are not serialized against one another. queues_ is only appended
        // during init() and never erased, so the queue handle stays valid after
        // the lock is released.
        std::optional<sycl::queue> q;
        {
            std::lock_guard<std::mutex> lock(mu_);
            const void *dev = to_host ? src : dst;
            const Alloc *a = findLocked(dev);
            int device_index;
            if (a) {
                const uintptr_t start = reinterpret_cast<uintptr_t>(dev);
                if (start + len > a->base + a->size) return 1;  // runs past end
                device_index = a->device;
            } else {
                device_index = classifyLocked(dev);
                if (device_index < 0) return 1;  // not device memory
            }
            q = queues_[device_index];
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
    std::vector<sycl::context> contexts_;
    std::vector<sycl::queue> queues_;
    std::vector<Alloc> allocs_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SRC_PLATFORM_XPU_SYCL_BACKEND_H_
