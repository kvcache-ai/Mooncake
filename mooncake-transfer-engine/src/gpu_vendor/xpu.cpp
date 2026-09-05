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

// Intel XPU (Level Zero) implementation of the cuda-alike API surface.
// This file is compiled only when USE_XPU is defined.

#include "gpu_vendor/xpu.h"

#include <glog/logging.h>
#include <level_zero/ze_api.h>

#include <cstdlib>
#include <unistd.h>  // dup()

#include <cstring>
#include <algorithm>  // std::find
#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace {

// Thread-local current device index (matches CUDA per-thread semantics).
static thread_local int tl_current_device = 0;

// Constant-initialised and never destroyed, so it stays readable even after the
// ZeRuntime singleton's destructor has run. A cudaStreamDestroy() arriving that
// late must not dereference the destroyed runtime.
static std::atomic<bool> g_runtime_torn_down{false};

}  // anonymous namespace

// A cudaStream_t is a pointer to one of these. It owns an in-order
// asynchronous immediate command list, and remembers the device it was created
// on: the copy must run there regardless of what the *calling* thread's
// tl_current_device happens to be.
struct XpuStreamImpl {
    ze_command_list_handle_t list = nullptr;
    int device = 0;
    // Level Zero command lists are not safe for concurrent appends. CUDA
    // streams are, so serialise here to keep the shim's contract the same.
    std::mutex mu;
};

namespace {

// ---- Singleton Level Zero runtime state ----
struct ZeRuntime {
    bool initialized = false;
    ze_driver_handle_t driver = nullptr;
    ze_context_handle_t context = nullptr;
    std::vector<ze_device_handle_t> devices;
    // Per-device immediate command list for synchronous copies.
    std::vector<ze_command_list_handle_t> imm_cmd_lists;
    // One mutex per device list. zeCommandListAppendMemoryCopy must not be
    // called on the same command list from simultaneous threads (ze_api.h), and
    // the per-device list is shared by every thread that calls cudaMemcpy.
    std::vector<std::unique_ptr<std::mutex>> imm_cmd_muts;
    // Live streams, so they can be torn down before the context they belong to.
    std::vector<XpuStreamImpl *> streams;
    std::mutex mu;

    ~ZeRuntime() {
        std::lock_guard<std::mutex> lock(mu);
        g_runtime_torn_down.store(true, std::memory_order_release);
        // Streams first: their command lists belong to `context`, so destroying
        // the context before them would leave dangling handles.
        for (auto *s : streams) {
            if (s->list) zeCommandListDestroy(s->list);
            delete s;
        }
        streams.clear();
        for (auto cl : imm_cmd_lists) {
            if (cl) zeCommandListDestroy(cl);
        }
        if (context) zeContextDestroy(context);
    }
};

static ZeRuntime &runtime() {
    static ZeRuntime rt;
    return rt;
}

static int initRuntime() {
    auto &rt = runtime();
    std::lock_guard<std::mutex> lock(rt.mu);
    if (rt.initialized) return 0;

    ze_result_t res = zeInit(ZE_INIT_FLAG_GPU_ONLY);
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeInit failed: 0x" << std::hex << res;
        return -1;
    }

    // Get the first GPU driver.
    uint32_t driver_count = 0;
    res = zeDriverGet(&driver_count, nullptr);
    if (res != ZE_RESULT_SUCCESS || driver_count == 0) {
        LOG(ERROR) << "No Level Zero GPU drivers found (res=0x" << std::hex
                   << res << ", count=" << std::dec << driver_count << ")";
        return -1;
    }
    std::vector<ze_driver_handle_t> drivers(driver_count);
    res = zeDriverGet(&driver_count, drivers.data());
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeDriverGet (fill) failed: 0x" << std::hex << res;
        return -1;
    }
    rt.driver = drivers[0];

    // Create context.
    ze_context_desc_t ctx_desc = {};
    ctx_desc.stype = ZE_STRUCTURE_TYPE_CONTEXT_DESC;
    res = zeContextCreate(rt.driver, &ctx_desc, &rt.context);
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeContextCreate failed: 0x" << std::hex << res;
        return -1;
    }

    // Enumerate GPU devices.
    uint32_t device_count = 0;
    res = zeDeviceGet(rt.driver, &device_count, nullptr);
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeDeviceGet (count) failed: 0x" << std::hex << res;
        return -1;
    }
    if (device_count == 0) {
        LOG(WARNING) << "No Level Zero GPU devices found";
        rt.initialized = true;
        return 0;
    }
    rt.devices.resize(device_count);
    res = zeDeviceGet(rt.driver, &device_count, rt.devices.data());
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeDeviceGet (fill) failed: 0x" << std::hex << res;
        return -1;
    }

    // Create one immediate command list per device for sync memcpy.
    rt.imm_cmd_lists.resize(device_count, nullptr);
    rt.imm_cmd_muts.resize(device_count);
    for (uint32_t i = 0; i < device_count; ++i) {
        rt.imm_cmd_muts[i] = std::make_unique<std::mutex>();
        ze_command_queue_desc_t cq_desc = {};
        cq_desc.stype = ZE_STRUCTURE_TYPE_COMMAND_QUEUE_DESC;
        cq_desc.ordinal = 0;
        cq_desc.mode = ZE_COMMAND_QUEUE_MODE_SYNCHRONOUS;
        res = zeCommandListCreateImmediate(rt.context, rt.devices[i], &cq_desc,
                                           &rt.imm_cmd_lists[i]);
        if (res != ZE_RESULT_SUCCESS) {
            LOG(ERROR) << "zeCommandListCreateImmediate failed for device " << i
                       << ": 0x" << std::hex << res;
            return -1;
        }
    }

    rt.initialized = true;
    LOG(INFO) << "Level Zero XPU runtime initialized: " << device_count
              << " device(s)";
    return 0;
}

}  // anonymous namespace

// ========================================================================
// cuda-alike API
// ========================================================================

cudaError_t cudaSetDevice(int device) {
    if (initRuntime()) return -1;
    auto &rt = runtime();
    if (device < 0 || device >= (int)rt.devices.size()) return -1;
    tl_current_device = device;
    return cudaSuccess;
}

cudaError_t cudaGetDevice(int *device) {
    if (initRuntime()) return -1;
    *device = tl_current_device;
    return cudaSuccess;
}

cudaError_t cudaGetDeviceCount(int *count) {
    if (initRuntime()) return -1;
    *count = (int)runtime().devices.size();
    return cudaSuccess;
}

cudaError_t cudaMalloc(void **devPtr, size_t size) {
    if (initRuntime()) return -1;
    auto &rt = runtime();
    ze_device_mem_alloc_desc_t desc = {};
    desc.stype = ZE_STRUCTURE_TYPE_DEVICE_MEM_ALLOC_DESC;
    desc.ordinal = 0;
    ze_result_t res = zeMemAllocDevice(rt.context, &desc, size, 64,
                                       rt.devices[tl_current_device], devPtr);
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeMemAllocDevice failed: 0x" << std::hex << res;
        return -1;
    }
    return cudaSuccess;
}

// Waits for all outstanding work on every live stream and every per-device
// list. Used by the free paths: real CUDA's cudaFree implicitly synchronises
// the device, so callers are entitled to free a buffer straight after an async
// copy that used it. Level Zero's zeMemFree does no such checking ("without any
// safety checking"), so without this a free-after-async-copy silently corrupts.
// Every device shares one context here, so a buffer may be referenced by a
// stream on any device -- drain them all, not just the current one.
static cudaError_t syncAllOutstanding() {
    auto &rt = runtime();
    std::vector<ze_command_list_handle_t> to_wait;
    {
        std::lock_guard<std::mutex> lock(rt.mu);
        for (auto cl : rt.imm_cmd_lists) {
            if (cl) to_wait.push_back(cl);
        }
        for (auto *s : rt.streams) {
            if (s->list) to_wait.push_back(s->list);
        }
    }
    cudaError_t status = cudaSuccess;
    for (auto list : to_wait) {
        if (zeCommandListHostSynchronize(list, UINT64_MAX) != ZE_RESULT_SUCCESS)
            status = -1;
    }
    return status;
}

cudaError_t cudaFree(void *devPtr) {
    if (!devPtr) return cudaSuccess;
    if (initRuntime()) return -1;
    // Matches cudaFree's implicit device synchronisation: an async copy may
    // still be reading or writing this allocation.
    syncAllOutstanding();
    ze_result_t res = zeMemFree(runtime().context, devPtr);
    return (res == ZE_RESULT_SUCCESS) ? cudaSuccess : -1;
}

cudaError_t cudaMemcpy(void *dst, const void *src, size_t count,
                       enum cudaMemcpyKind kind) {
    (void)kind;  // Level Zero figures out direction automatically.
    if (initRuntime()) return -1;
    auto &rt = runtime();
    int dev = tl_current_device;
    if (dev < 0 || dev >= (int)rt.imm_cmd_lists.size()) return -1;
    // The per-device list is shared by all threads, and Level Zero forbids
    // concurrent appends to one command list, so serialise here.
    std::lock_guard<std::mutex> lock(*rt.imm_cmd_muts[dev]);
    // The list is created in SYNCHRONOUS mode, so the append does not return
    // until the copy has completed -- which is what cudaMemcpy promises.
    ze_result_t res = zeCommandListAppendMemoryCopy(
        rt.imm_cmd_lists[dev], dst, src, count, nullptr, 0, nullptr);
    return (res == ZE_RESULT_SUCCESS) ? cudaSuccess : -1;
}

cudaError_t cudaMemcpyAsync(void *dst, const void *src, size_t count,
                            enum cudaMemcpyKind kind, cudaStream_t stream) {
    (void)kind;  // Level Zero figures out direction automatically.
    // Null stream == CUDA's default stream. Level Zero has no equivalent of the
    // legacy default stream's implicit synchronisation with other streams, so
    // do a fully synchronous copy: stricter than CUDA, never weaker.
    if (stream == nullptr) return cudaMemcpy(dst, src, count, kind);

    if (initRuntime()) return -1;
    auto *s = static_cast<XpuStreamImpl *>(stream);
    std::lock_guard<std::mutex> lock(s->mu);
    if (!s->list) return -1;
    // The stream's list is ASYNCHRONOUS + IN_ORDER, so this returns as soon as
    // the copy is submitted and stays ordered against the stream's other work.
    ze_result_t res = zeCommandListAppendMemoryCopy(s->list, dst, src, count,
                                                    nullptr, 0, nullptr);
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeCommandListAppendMemoryCopy (async) failed: 0x"
                   << std::hex << res;
        return -1;
    }
    return cudaSuccess;
}

// ---------- Stream management ----------

cudaError_t cudaStreamCreateWithFlags(cudaStream_t *stream,
                                      unsigned int flags) {
    // Both cudaStreamDefault and cudaStreamNonBlocking map to the same thing
    // here: the null stream is synchronous, so there is no
    // legacy-default-stream interaction for the flag to change.
    (void)flags;
    if (!stream) return -1;
    *stream = nullptr;
    if (initRuntime()) return -1;
    auto &rt = runtime();

    std::lock_guard<std::mutex> lock(rt.mu);
    int dev = tl_current_device;
    if (dev < 0 || dev >= (int)rt.devices.size()) return -1;

    ze_command_queue_desc_t cq_desc = {};
    cq_desc.stype = ZE_STRUCTURE_TYPE_COMMAND_QUEUE_DESC;
    cq_desc.ordinal = 0;
    cq_desc.mode = ZE_COMMAND_QUEUE_MODE_ASYNCHRONOUS;
    // IN_ORDER is required, not cosmetic: without it the driver may execute
    // appended commands out of order, so two dependent copies on one stream
    // could be applied in the wrong order and silently corrupt data.
    cq_desc.flags = ZE_COMMAND_QUEUE_FLAG_IN_ORDER;

    ze_command_list_handle_t list = nullptr;
    ze_result_t res = zeCommandListCreateImmediate(rt.context, rt.devices[dev],
                                                   &cq_desc, &list);
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeCommandListCreateImmediate (async, in-order) failed "
                      "for device "
                   << dev << ": 0x" << std::hex << res;
        return -1;
    }

    auto *s = new XpuStreamImpl();
    s->list = list;
    s->device = dev;
    rt.streams.push_back(s);
    *stream = s;
    return cudaSuccess;
}

cudaError_t cudaStreamCreate(cudaStream_t *stream) {
    return cudaStreamCreateWithFlags(stream, cudaStreamDefault);
}

cudaError_t cudaStreamSynchronize(cudaStream_t stream) {
    if (stream == nullptr) {
        // CUDA's default stream waits for all blocking streams on the device,
        // and in-tree callers use cudaStreamSynchronize(0) that way, so defer
        // to the device-wide wait rather than returning immediately.
        return cudaDeviceSynchronize();
    }
    auto *s = static_cast<XpuStreamImpl *>(stream);
    // Intentionally NOT holding s->mu: zeCommandListHostSynchronize may be
    // called from simultaneous threads and is lock-free, whereas holding the
    // append mutex for the duration of the wait would block other submits on
    // this stream and re-serialise exactly what async is for.
    ze_command_list_handle_t list = s->list;
    if (!list) return -1;
    ze_result_t res = zeCommandListHostSynchronize(list, UINT64_MAX);
    return (res == ZE_RESULT_SUCCESS) ? cudaSuccess : -1;
}

cudaError_t cudaStreamQuery(cudaStream_t stream) {
    if (stream == nullptr) return cudaSuccess;
    auto *s = static_cast<XpuStreamImpl *>(stream);
    ze_command_list_handle_t list = s->list;  // see cudaStreamSynchronize
    if (!list) return -1;
    // timeout 0 == report the current status without blocking.
    ze_result_t res = zeCommandListHostSynchronize(list, 0);
    if (res == ZE_RESULT_SUCCESS) return cudaSuccess;
    if (res == ZE_RESULT_NOT_READY) return cudaErrorNotReady;
    return -1;
}

cudaError_t cudaStreamDestroy(cudaStream_t stream) {
    if (stream == nullptr) return cudaSuccess;
    // Checked before touching the runtime: after static destruction it has
    // already destroyed every stream, and dereferencing it would be UB.
    if (g_runtime_torn_down.load(std::memory_order_acquire)) return cudaSuccess;
    auto &rt = runtime();
    auto *s = static_cast<XpuStreamImpl *>(stream);

    std::lock_guard<std::mutex> lock(rt.mu);
    auto it = std::find(rt.streams.begin(), rt.streams.end(), s);
    if (it == rt.streams.end()) return cudaSuccess;
    rt.streams.erase(it);

    // Drain before destroying, so in-flight copies cannot outlive the list.
    if (s->list) {
        zeCommandListHostSynchronize(s->list, UINT64_MAX);
        zeCommandListDestroy(s->list);
        s->list = nullptr;
    }
    delete s;
    return cudaSuccess;
}

cudaError_t cudaDeviceSynchronize(void) {
    if (initRuntime()) return -1;
    auto &rt = runtime();
    int dev = tl_current_device;

    // Copy the handles out under the lock, then wait without holding it: a
    // long-running stream must not block cudaStreamCreate/Destroy meanwhile.
    std::vector<ze_command_list_handle_t> to_wait;
    {
        std::lock_guard<std::mutex> lock(rt.mu);
        if (dev >= 0 && dev < (int)rt.imm_cmd_lists.size() &&
            rt.imm_cmd_lists[dev]) {
            to_wait.push_back(rt.imm_cmd_lists[dev]);
        }
        for (auto *s : rt.streams) {
            if (s->device == dev && s->list) to_wait.push_back(s->list);
        }
    }

    cudaError_t status = cudaSuccess;
    for (auto list : to_wait) {
        if (zeCommandListHostSynchronize(list, UINT64_MAX) !=
            ZE_RESULT_SUCCESS) {
            status = -1;
        }
    }
    return status;
}

cudaError_t cudaHostAlloc(void **pHost, size_t size, unsigned int flags) {
    (void)flags;
    if (initRuntime()) return -1;
    auto &rt = runtime();
    ze_host_mem_alloc_desc_t desc = {};
    desc.stype = ZE_STRUCTURE_TYPE_HOST_MEM_ALLOC_DESC;
    ze_result_t res = zeMemAllocHost(rt.context, &desc, size, 64, pHost);
    return (res == ZE_RESULT_SUCCESS) ? cudaSuccess : -1;
}

cudaError_t cudaFreeHost(void *ptr) {
    if (!ptr) return cudaSuccess;
    if (initRuntime()) return -1;
    // Host staging buffers are the usual source of an in-flight H2D copy, so
    // the same implicit synchronisation as cudaFree applies.
    syncAllOutstanding();
    ze_result_t res = zeMemFree(runtime().context, ptr);
    return (res == ZE_RESULT_SUCCESS) ? cudaSuccess : -1;
}

cudaError_t cudaPointerGetAttributes(struct cudaPointerAttributes *attributes,
                                     const void *ptr) {
    if (initRuntime()) return -1;
    auto &rt = runtime();
    ze_memory_allocation_properties_t props = {};
    props.stype = ZE_STRUCTURE_TYPE_MEMORY_ALLOCATION_PROPERTIES;
    ze_device_handle_t alloc_device = nullptr;
    ze_result_t res =
        zeMemGetAllocProperties(rt.context, ptr, &props, &alloc_device);
    if (res != ZE_RESULT_SUCCESS) {
        // Level Zero doesn't recognise this pointer.
        attributes->type = cudaMemoryTypeUnregistered;
        attributes->device = -1;
        attributes->devicePointer = nullptr;
        attributes->hostPointer = nullptr;
        return cudaSuccess;
    }

    if (props.type == ZE_MEMORY_TYPE_UNKNOWN) {
        // Not an L0 allocation — treat as unregistered (likely stack/heap).
        attributes->type = cudaMemoryTypeUnregistered;
        attributes->device = -1;
        attributes->devicePointer = nullptr;
        attributes->hostPointer = nullptr;
        return cudaSuccess;
    }

    switch (props.type) {
        case ZE_MEMORY_TYPE_DEVICE:
            attributes->type = cudaMemoryTypeDevice;
            attributes->devicePointer = const_cast<void *>(ptr);
            attributes->hostPointer = nullptr;
            // Find device ordinal.
            attributes->device = 0;
            for (size_t i = 0; i < rt.devices.size(); ++i) {
                if (rt.devices[i] == alloc_device) {
                    attributes->device = (int)i;
                    break;
                }
            }
            break;
        case ZE_MEMORY_TYPE_HOST:
        case ZE_MEMORY_TYPE_SHARED:
            attributes->type = cudaMemoryTypeHost;
            attributes->device = 0;
            attributes->devicePointer = nullptr;
            attributes->hostPointer = const_cast<void *>(ptr);
            break;
        default:
            attributes->type = cudaMemoryTypeUnregistered;
            attributes->device = -1;
            attributes->devicePointer = nullptr;
            attributes->hostPointer = nullptr;
            break;
    }
    return cudaSuccess;
}

const char *cudaGetErrorString(cudaError_t error) {
    if (error == cudaSuccess) return "success";
    return "Level Zero error";
}

cudaError_t cudaDeviceGetPCIBusId(char *pciBusId, int len, int device) {
    if (initRuntime()) return -1;
    auto &rt = runtime();
    if (device < 0 || device >= (int)rt.devices.size()) return -1;

    ze_device_properties_t props = {};
    props.stype = ZE_STRUCTURE_TYPE_DEVICE_PROPERTIES;
    ze_result_t res = zeDeviceGetProperties(rt.devices[device], &props);
    if (res != ZE_RESULT_SUCCESS) return -1;

    ze_pci_ext_properties_t pci_props = {};
    pci_props.stype = ZE_STRUCTURE_TYPE_PCI_EXT_PROPERTIES;
    res = zeDevicePciGetPropertiesExt(rt.devices[device], &pci_props);
    if (res == ZE_RESULT_SUCCESS) {
        snprintf(pciBusId, len, "%04x:%02x:%02x.%01x", pci_props.address.domain,
                 pci_props.address.bus, pci_props.address.device,
                 pci_props.address.function);
        return cudaSuccess;
    }

    // Fallback: use the device UUID or a placeholder.
    snprintf(pciBusId, len, "0000:00:%02x.0", device);
    return cudaSuccess;
}

// ========================================================================
// mooncake::xpu helpers
// ========================================================================

namespace mooncake {
namespace xpu {

int ensureInitialized() { return initRuntime(); }

ze_context_handle_t getContext() {
    initRuntime();
    return runtime().context;
}

ze_device_handle_t getDevice(int ordinal) {
    initRuntime();
    auto &rt = runtime();
    if (ordinal < 0 || ordinal >= (int)rt.devices.size()) return nullptr;
    return rt.devices[ordinal];
}

bool isDeviceMemory(const void *ptr) {
    if (initRuntime()) return false;
    auto &rt = runtime();
    ze_memory_allocation_properties_t props = {};
    props.stype = ZE_STRUCTURE_TYPE_MEMORY_ALLOCATION_PROPERTIES;
    ze_result_t res = zeMemGetAllocProperties(rt.context, ptr, &props, nullptr);
    return (res == ZE_RESULT_SUCCESS && props.type == ZE_MEMORY_TYPE_DEVICE);
}

int getAllocBase(const void *ptr, void **base, size_t *size) {
    if (initRuntime()) return -1;
    auto &rt = runtime();
    ze_result_t res = zeMemGetAddressRange(rt.context, ptr, base, size);
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeMemGetAddressRange failed for " << ptr << ": 0x"
                   << std::hex << res;
        return -1;
    }
    return 0;
}

int exportDmaBufFd(void *devPtr, size_t size) {
    (void)size;  // the dma_buf covers the whole allocation
    if (initRuntime()) return -1;
    auto &rt = runtime();
    ze_ipc_mem_handle_t ipc_handle;
    ze_result_t res = zeMemGetIpcHandle(rt.context, devPtr, &ipc_handle);
    if (res != ZE_RESULT_SUCCESS) {
        LOG(ERROR) << "zeMemGetIpcHandle failed: 0x" << std::hex << res;
        return -1;
    }
    // The IPC handle on Linux with the xe/i915 driver carries a dma_buf fd in
    // the first sizeof(int) bytes of the opaque handle data.
    int driver_fd = -1;
    memcpy(&driver_fd, ipc_handle.data, sizeof(driver_fd));
    if (driver_fd < 0) {
        LOG(ERROR) << "DMA-BUF fd extraction failed (fd=" << driver_fd << ")";
        zeMemPutIpcHandle(rt.context, ipc_handle);
        return -1;
    }
    // The fd inside the IPC handle belongs to the driver: zeMemPutIpcHandle()
    // closes it. Callers here own what we return and close() it once every NIC
    // has registered, so hand back a dup() and release the driver's reference
    // immediately. Returning driver_fd directly would leak the handle and later
    // double-close the same descriptor number.
    int fd = dup(driver_fd);
    if (fd < 0) {
        PLOG(ERROR) << "dup() of dma_buf fd " << driver_fd << " failed";
    }
    zeMemPutIpcHandle(rt.context, ipc_handle);
    return fd;
}

}  // namespace xpu
}  // namespace mooncake
