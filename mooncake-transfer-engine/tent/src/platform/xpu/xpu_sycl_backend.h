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
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

namespace mooncake {
namespace tent {

// The Level Zero entry points the dma-buf and IPC paths need, resolved lazily
// from the loader the SYCL runtime already has open (libze_loader.so.1). The
// SYCL Level Zero adapter has called zeInit by the time a SYCL context exists,
// so no separate initialisation is performed here.
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

    // Cross-process sharing (zeMemGetIpcHandle on the exporter,
    // zeMemOpenIpcHandle + zeMemCloseIpcHandle on the importer) is available.
    bool canShare() const { return get_ipc_ && open_ipc_ && close_ipc_; }

    ze_result_t memGetIpcHandle(ze_context_handle_t ctx, const void *ptr,
                                ze_ipc_mem_handle_t *handle) const {
        return get_ipc_(ctx, ptr, handle);
    }

    ze_result_t memOpenIpcHandle(ze_context_handle_t ctx,
                                 ze_device_handle_t device,
                                 ze_ipc_mem_handle_t handle,
                                 ze_ipc_memory_flags_t flags,
                                 void **pptr) const {
        return open_ipc_(ctx, device, handle, flags, pptr);
    }

    ze_result_t memCloseIpcHandle(ze_context_handle_t ctx,
                                  const void *ptr) const {
        return close_ipc_(ctx, ptr);
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
        get_ipc_ =
            reinterpret_cast<GetIpc>(dlsym(handle_, "zeMemGetIpcHandle"));
        open_ipc_ =
            reinterpret_cast<OpenIpc>(dlsym(handle_, "zeMemOpenIpcHandle"));
        close_ipc_ =
            reinterpret_cast<CloseIpc>(dlsym(handle_, "zeMemCloseIpcHandle"));
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
    using GetIpc = ze_result_t (*)(ze_context_handle_t, const void *,
                                   ze_ipc_mem_handle_t *);
    using OpenIpc = ze_result_t (*)(ze_context_handle_t, ze_device_handle_t,
                                    ze_ipc_mem_handle_t, ze_ipc_memory_flags_t,
                                    void **);
    using CloseIpc = ze_result_t (*)(ze_context_handle_t, const void *);

    void *handle_ = nullptr;
    GetAllocProperties get_alloc_properties_ = nullptr;
    GetAddressRange get_range_ = nullptr;
    AllocDevice alloc_device_ = nullptr;
    MemFree mem_free_ = nullptr;
    GetIpc get_ipc_ = nullptr;
    OpenIpc open_ipc_ = nullptr;
    CloseIpc close_ipc_ = nullptr;
};

// Minimal USM-device backend with an interior-pointer registry.
//
// Two classification paths are combined:
//  1. Allocations made through this backend, and peer allocations imported
//    with importIpc, are tracked in a [base, base+size) interval registry,
//    so interior addresses (staging hands the backend base + chunk_offset)
//    resolve without touching the SYCL runtime.
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
            // The oneAPI runtime exposes each Intel GPU twice, once through
            // Level Zero and once through the OpenCL adapter. Keep only the
            // Level Zero view when it is present: it is the one that supports
            // dma-buf export and peer access, and advertising the OpenCL
            // duplicates would turn a 2-GPU node into four "xpu:N" nodes.
            const bool has_level_zero = std::any_of(
                devices.begin(), devices.end(), [](const sycl::device &d) {
                    return d.get_backend() ==
                           sycl::backend::ext_oneapi_level_zero;
                });
            if (has_level_zero) {
                devices.erase(
                    std::remove_if(
                        devices.begin(), devices.end(),
                        [](const sycl::device &d) {
                            return d.get_backend() !=
                                   sycl::backend::ext_oneapi_level_zero;
                        }),
                    devices.end());
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
            enablePeerAccessLocked();
            initialized_ = true;
            return 0;
        } catch (const sycl::exception &) {
            // Leave no partially-built state: a later init() retry must start
            // from an empty queue list, not resume with duplicate ordinals.
            queues_.clear();
            contexts_.clear();
            peer_.clear();
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

    // Device-to-device copy. Same device: a plain memcpy on its queue. Two
    // devices with peer access (see enablePeerAccessLocked): one memcpy on the
    // source device's queue, which pushes the bytes to the destination over
    // PCIe (or Xe Link) without touching host memory; if only the reverse
    // direction is enabled, the destination's queue pulls instead. Devices
    // that cannot reach each other fall back to a chunked bounce through a
    // host buffer, so the copy is always correct and callers never need to
    // stage device<->device traffic themselves.
    int copyD2D(void *dst, const void *src, size_t len) {
        std::optional<sycl::queue> direct, src_q, dst_q;
        {
            std::lock_guard<std::mutex> lock(mu_);
            const int src_dev = resolveLocked(src, len);
            const int dst_dev = resolveLocked(dst, len);
            if (src_dev < 0 || dst_dev < 0) return 1;
            if (src_dev == dst_dev || peerLocked(src_dev, dst_dev)) {
                direct = queues_[src_dev];
            } else if (peerLocked(dst_dev, src_dev)) {
                direct = queues_[dst_dev];
            } else {
                src_q = queues_[src_dev];
                dst_q = queues_[dst_dev];
            }
        }
        try {
            if (direct) {
                direct->memcpy(dst, src, len).wait();
                return 0;
            }
            std::vector<uint8_t> bounce(std::min(len, kBounceChunk));
            for (size_t off = 0; off < len; off += bounce.size()) {
                const size_t n = std::min(bounce.size(), len - off);
                src_q
                    ->memcpy(bounce.data(),
                             static_cast<const uint8_t *>(src) + off, n)
                    .wait();
                dst_q
                    ->memcpy(static_cast<uint8_t *>(dst) + off, bounce.data(),
                             n)
                    .wait();
            }
            return 0;
        } catch (const sycl::exception &) {
            return 1;
        }
    }

    // True when device `from` may directly access USM device memory that
    // lives on device `to` (always true for from == to).
    bool canAccessPeer(int from, int to) {
        std::lock_guard<std::mutex> lock(mu_);
        return peerLocked(from, to);
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

    static constexpr size_t kIpcHandleSize = ZE_MAX_IPC_HANDLE_SIZE;
    using IpcHandleBytes = std::array<uint8_t, kIpcHandleSize>;

    // Describe the USM device allocation containing `addr` for another
    // process: its base and size (zeMemGetAddressRange) and the Level Zero
    // IPC handle of the allocation (zeMemGetIpcHandle). The handle is an
    // opaque ZE_MAX_IPC_HANDLE_SIZE-byte blob whose first field is the
    // allocation's dma-buf fd in this process; pooled allocations carry their
    // pool offset inside it, so unlike exportDmabuf this works for small
    // foreign allocations too. The fd behind the handle stays open for the
    // allocation's lifetime (the runtime caches one per allocation and closes
    // it on free); zeMemPutIpcHandle is deliberately not used since the same
    // descriptor doubles as exportDmabuf's for direct RDMA. Returns 0 on
    // success; non-zero when `addr` is not device memory this backend can
    // resolve or the driver cannot produce a handle.
    int exportIpc(const void *addr, uint64_t *base, uint64_t *size,
                  IpcHandleBytes *handle) {
        if (!base || !size || !handle) return 1;
        std::optional<sycl::context> ctx;
        {
            std::lock_guard<std::mutex> lock(mu_);
            int device_index = classifyLocked(addr);
            if (device_index < 0) return 1;
            ctx = queues_[device_index].get_context();
        }
        auto &ze = ZeLoader::instance();
        if (!ze.available() || !ze.canShare()) return 1;
        ze_context_handle_t ze_ctx = nullptr;
        try {
            if (ctx->get_backend() != sycl::backend::ext_oneapi_level_zero)
                return 1;
            ze_ctx =
                sycl::get_native<sycl::backend::ext_oneapi_level_zero>(*ctx);
        } catch (const sycl::exception &) {
            return 1;
        }
        void *range_base = nullptr;
        size_t range_size = 0;
        if (ze.memGetAddressRange(ze_ctx, addr, &range_base, &range_size) !=
                ZE_RESULT_SUCCESS ||
            !range_base || !range_size)
            return 1;
        ze_ipc_mem_handle_t ipc{};
        if (ze.memGetIpcHandle(ze_ctx, range_base, &ipc) != ZE_RESULT_SUCCESS)
            return 1;
        static_assert(sizeof(ipc.data) == kIpcHandleSize);
        std::memcpy(handle->data(), ipc.data, kIpcHandleSize);
        *base = reinterpret_cast<uintptr_t>(range_base);
        *size = range_size;
        return 0;
    }

    // Map an allocation described by exportIpc in process `exporter_pid`
    // (possibly this one) into this process as USM device memory that device
    // `device_index` can address, so a queue on that device reads or writes
    // the peer's VRAM over PCIe P2P (or Xe Link) without a host bounce.
    //
    // The handle names the exporter's dma-buf by *its* fd number, which is
    // meaningless here. The compute runtime can translate it itself ("opaque"
    // handles, 2024+ runtimes: pidfd_getfd on the exporter with a socket
    // fallback), but that path caches the fetched descriptor per (exporter
    // pid, exporter fd) and never drops the entry when the mapping is closed,
    // so once the exporter frees the allocation and a new one lands on the
    // same fd number, importers silently map the *old* buffer (observed with
    // compute-runtime 1.17). The translation is therefore always done here --
    // dup() within one process, pidfd_getfd() otherwise, which needs ptrace
    // permission over the exporter -- and the handle is rewritten so that the
    // runtime takes its uncached legacy route: the fd field gets the local
    // descriptor and the opaque mirror field a value that can never equal it.
    // Legacy runtimes ignore the mirror and read the same fd/poolOffset/type
    // fields, so one encoding serves both generations.
    //
    // The fetched descriptor is owned by the mapping and closed in
    // closeImport; the runtime does not close it. The mapping is recorded in
    // the registry with `size` so classification, bounds checks and copyD2D
    // treat it as ordinary device memory on `device_index`. Returns 0 and
    // fills *pptr on success; non-zero when the runtime lacks IPC, the device
    // cannot map the peer, the handle is stale, or pidfd_getfd is refused --
    // errno is left describing that last case (EPERM: no ptrace permission
    // over the exporter).
    int importIpc(const IpcHandleBytes &handle, pid_t exporter_pid, size_t size,
                  int device_index, void **pptr) {
        if (!size || !pptr || exporter_pid <= 0) return 1;
        auto &ze = ZeLoader::instance();
        if (!ze.canShare()) return 1;
        std::lock_guard<std::mutex> lock(mu_);
        if (!initialized_ || device_index < 0 ||
            device_index >= static_cast<int>(queues_.size()))
            return 1;
        sycl::queue &q = queues_[device_index];
        ze_context_handle_t ze_ctx = nullptr;
        ze_device_handle_t ze_dev = nullptr;
        try {
            if (q.get_context().get_backend() !=
                sycl::backend::ext_oneapi_level_zero)
                return 1;
            ze_ctx = sycl::get_native<sycl::backend::ext_oneapi_level_zero>(
                q.get_context());
            ze_dev = sycl::get_native<sycl::backend::ext_oneapi_level_zero>(
                q.get_device());
        } catch (const sycl::exception &) {
            return 1;
        }
        int64_t exporter_fd = 0;
        std::memcpy(&exporter_fd, handle.data(), sizeof(exporter_fd));
        if (exporter_fd < 0 || exporter_fd > INT32_MAX) return 1;
        const int fd = fetchFd(exporter_pid, static_cast<int>(exporter_fd));
        if (fd < 0) return 1;
        ze_ipc_mem_handle_t ipc{};
        std::memcpy(ipc.data, handle.data(), kIpcHandleSize);
        const uint64_t local = static_cast<uint64_t>(fd);
        std::memcpy(ipc.data + kIpcHandleFdOffset, &local, sizeof(local));
        const uint64_t mirror = ~uint64_t{0};
        std::memcpy(ipc.data + kIpcHandleMirrorOffset, &mirror, sizeof(mirror));
        void *p = nullptr;
        if (ze.memOpenIpcHandle(ze_ctx, ze_dev, ipc, /*flags=*/0, &p) !=
                ZE_RESULT_SUCCESS ||
            !p) {
            close(fd);
            return 1;
        }
        imports_.push_back(
            Import{reinterpret_cast<uintptr_t>(p), size, device_index, fd});
        *pptr = p;
        return 0;
    }

    // Unmap a range returned by importIpc and close any descriptor fetched
    // for it.
    int closeImport(void *ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        const uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
        for (size_t i = 0; i < imports_.size(); ++i) {
            if (imports_[i].base != addr) continue;
            int rc = 0;
            try {
                auto ze_ctx =
                    sycl::get_native<sycl::backend::ext_oneapi_level_zero>(
                        queues_[imports_[i].device].get_context());
                if (ZeLoader::instance().memCloseIpcHandle(ze_ctx, ptr) !=
                    ZE_RESULT_SUCCESS)
                    rc = 1;
            } catch (const sycl::exception &) {
                rc = 1;
            }
            if (imports_[i].fd >= 0) close(imports_[i].fd);
            imports_.erase(imports_.begin() + i);
            return rc;
        }
        return 1;
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
    // A peer allocation mapped by importIpc: `device` is the local device it
    // was opened on (not where the bytes live), `fd` the descriptor fetched
    // from the exporter that keeps the mapping alive.
    struct Import {
        uintptr_t base;
        size_t size;
        int device;
        int fd;
    };

    // Layout of the Intel compute runtime's IPC memory handle, from
    // level_zero/core/source/context/context.h (#pragma pack(1)). Legacy
    // IpcMemoryData: handle (8) | poolOffset (8) | type (1). Opaque
    // IpcOpaqueMemoryData: handle (8) | poolOffset (8) | processId (4) |
    // type (1) | memoryType (1) | opaqueHandle (8) | reservedHandleData (32)
    // | compressedMemory (1). Both keep the fd in the first 8 bytes; the
    // opaque runtime treats a handle whose opaqueHandle differs from handle
    // as user-modified and imports the fd directly.
    static constexpr size_t kIpcHandleFdOffset = 0;
    static constexpr size_t kIpcHandleMirrorOffset = 22;

    // Duplicate descriptor `fd` of process `pid` into this process (dup()
    // within the process, pidfd_open + pidfd_getfd otherwise, which needs
    // ptrace permission over `pid`). Returns -1 with errno set on failure.
    static int fetchFd(pid_t pid, int fd) {
        if (pid == getpid()) return dup(fd);
        const int pidfd = static_cast<int>(syscall(SYS_pidfd_open, pid, 0));
        if (pidfd < 0) return -1;
        const int local =
            static_cast<int>(syscall(SYS_pidfd_getfd, pidfd, fd, 0));
        const int saved = errno;
        close(pidfd);
        errno = saved;
        return local;
    }

    struct Range {
        uintptr_t base;
        size_t size;
        int device;
    };

    // Resolve an address (base or interior) to its owning allocation or
    // imported mapping.
    std::optional<Range> findLocked(const void *addr) const {
        const uintptr_t a = reinterpret_cast<uintptr_t>(addr);
        for (const auto &al : allocs_) {
            if (a >= al.base && a < al.base + al.size)
                return Range{al.base, al.size, al.device};
        }
        for (const auto &im : imports_) {
            if (a >= im.base && a < im.base + im.size)
                return Range{im.base, im.size, im.device};
        }
        return std::nullopt;
    }

    // Device ordinal owning `addr`, or -1 when it is not USM device memory.
    // Own allocations resolve through the registry; anything else is asked of
    // the SYCL runtime against every default context we hold.
    int classifyLocked(const void *addr) const {
        if (auto a = findLocked(addr)) return a->device;
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

    // Device ordinal owning the device range [dev, dev+len), or -1 when it is
    // not device memory or (for our own allocations) runs past the end.
    int resolveLocked(const void *dev, size_t len) const {
        if (auto a = findLocked(dev)) {
            const uintptr_t start = reinterpret_cast<uintptr_t>(dev);
            if (start + len > a->base + a->size) return -1;
            return a->device;
        }
        return classifyLocked(dev);
    }

    bool peerLocked(int from, int to) const {
        const int n = static_cast<int>(queues_.size());
        if (from < 0 || to < 0 || from >= n || to >= n) return false;
        if (from == to) return true;
        return peer_[from * n + to];
    }

    // Probe every ordered Level Zero device pair with
    // sycl_ext_oneapi_peer_access and enable access where the driver allows
    // it, so a later copyD2D between the pair runs as one direct copy. Peer
    // access needs both devices in the same SYCL context; pairs that fail the
    // query, or whose enable call fails, are simply left disabled and copyD2D
    // bounces through the host for them. Called from init() under mu_.
    //
    // Only Level Zero is probed: it is the sole Intel backend implementing the
    // extension, and the OpenCL adapter aborts the process (ur_die) instead of
    // throwing when asked. Not guarded by SYCL_EXT_ONEAPI_PEER_ACCESS either:
    // USE_XPU builds require the Intel DPC++ compiler, whose headers ship
    // device::ext_oneapi_*_peer_access but (as of oneAPI 2026.1) do not define
    // the feature-test macro, so the guard would compile the probe out.
    void enablePeerAccessLocked() {
        const size_t n = queues_.size();
        peer_.assign(n * n, false);
        for (size_t i = 0; i < n; ++i) {
            if (queues_[i].get_backend() !=
                sycl::backend::ext_oneapi_level_zero)
                continue;
            for (size_t j = 0; j < n; ++j) {
                if (i == j) continue;
                if (queues_[i].get_context() != queues_[j].get_context())
                    continue;
                sycl::device from = queues_[i].get_device();
                sycl::device to = queues_[j].get_device();
                try {
                    if (!from.ext_oneapi_can_access_peer(
                            to,
                            sycl::ext::oneapi::peer_access::access_supported))
                        continue;
                    from.ext_oneapi_enable_peer_access(to);
                    peer_[i * n + j] = true;
                } catch (const sycl::exception &e) {
                    // errc::invalid means access is already enabled (by
                    // another component sharing the runtime); the query
                    // above said it is possible, so keep it.
                    if (e.code() == sycl::errc::invalid)
                        peer_[i * n + j] = true;
                }
            }
        }
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
            const int device_index = resolveLocked(to_host ? src : dst, len);
            if (device_index < 0) return 1;  // not device memory / past end
            q = queues_[device_index];
        }
        try {
            q->memcpy(dst, src, len).wait();
            return 0;
        } catch (const sycl::exception &) {
            return 1;
        }
    }

    // Host bounce granularity for device pairs without peer access.
    static constexpr size_t kBounceChunk = size_t{8} << 20;

    std::mutex mu_;
    bool initialized_ = false;
    std::vector<sycl::context> contexts_;
    std::vector<sycl::queue> queues_;
    // Row-major queues_.size() x queues_.size(): peer_[i*n+j] is true when
    // device i has peer access to device j's memory.
    std::vector<bool> peer_;
    std::vector<Alloc> allocs_;
    std::vector<Import> imports_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SRC_PLATFORM_XPU_SYCL_BACKEND_H_
