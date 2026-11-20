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

// A torch.cuda.memory.CUDAPluggableAllocator backend (mooncake.allocator.
// HipGemAllocator) for HIP device memory that RDMA NICs will pin, such as a
// prefill/decode KV cache.
//
// amdgpu-dkms 6.18.4 through 7.1.9, and mainline kernels before
// torvalds/linux 9131cc85d0de ("drm/amdkfd: correct vram_available
// calculation", v7.4-rc1), charge a KFD BO that a NIC pins twice in the KFD
// allocation limit: once in kfd.vram_used and once in vram_pin_size. Once the
// registered KV cache exceeds the VRAM left after allocating it, every later
// device allocation of the process fails (a new HIP stream segfaults in
// c10::hip::HIPStream::stream()). Each allocation here is instead an amdgpu GEM
// BO created on the GPU's render node and imported with
// hipImportExternalMemory: a dma-buf import is not reserved in kfd.vram_used,
// so the pinned BO is charged once. A one-page dma-buf MR pins the BO right
// away, so the KFD free VRAM that hipMemGetInfo reports covers it from the
// start and the BO cannot be evicted before the transfer engine registers it.
//
// The transfer engine registers these buffers with the BO's own dma-buf, found
// through mc_hip_gem_lookup(): HIP reports no address range for imported
// memory and ROCr cannot export it. They must never go through the amdgpu
// peer-memory client (ibv_reg_mr): amd_get_pages dereferences bo->kfd_bo,
// which is NULL for a GEM BO. mc_hip_gem_lookup() also gives the device a
// buffer was allocated on: hipPointerGetAttributes reports imported memory as
// device 0's, whichever device imported it.
//
// Environment: MC_HIP_GEM_PIN_NIC  RDMA device used for the one-page pin
//              (default: the first one that accepts it).

#include <fcntl.h>
#include <glob.h>
#include <hip/hip_runtime_api.h>
#include <infiniband/verbs.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "hip_gem_pin.h"

namespace {

// <drm/drm.h>, <drm/amdgpu_drm.h>
constexpr unsigned long kDrmIoctlAmdgpuGemCreate = 0xC0206440UL;
constexpr unsigned long kDrmIoctlPrimeHandleToFd = 0xC00C642DUL;
constexpr unsigned long kDrmIoctlGemClose = 0x40086409UL;
constexpr uint64_t kAmdgpuGemDomainVram = 0x4;
constexpr size_t kAlign = 2ull << 20;

union GemCreate {
    struct {
        uint64_t bo_size, alignment, domains, domain_flags;
    } in;
    struct {
        uint32_t handle, pad;
    } out;
};
struct PrimeHandle {
    uint32_t handle, flags;
    int32_t fd;
};
struct GemClose {
    uint32_t handle, pad;
};

struct GemBuffer {
    size_t size;
    uint32_t handle;
    int render_fd;
    int dmabuf_fd;
    hipExternalMemory_t ext;
    ibv_mr *pin;
    int device;
};

std::mutex g_mutex;
std::map<uintptr_t, GemBuffer> g_buffers;  // by base address
std::map<int, int> g_render_fds;           // HIP device -> render node fd
ibv_device **g_ibv_devices = nullptr;
int g_ibv_count = 0;
std::vector<ibv_pd *> g_pin_pds;  // by RDMA device, opened on first use
int g_pin_device = -1;            // the device that pinned the last buffer

void logError(const char *what, int device, size_t size, const char *detail) {
    fprintf(stderr, "[hip_gem_allocator] %s (device %d, %zu bytes): %s\n", what,
            device, size, detail);
}

// Caller holds g_mutex.
int renderFd(int device) {
    auto it = g_render_fds.find(device);
    if (it != g_render_fds.end()) return it->second;
    char bus[64];
    if (hipDeviceGetPCIBusId(bus, sizeof bus, device) != hipSuccess) return -1;
    for (char *c = bus; *c; ++c)
        if (*c >= 'A' && *c <= 'F') *c += 'a' - 'A';
    std::string pattern =
        std::string("/sys/bus/pci/devices/") + bus + "/drm/render*";
    glob_t g;
    if (glob(pattern.c_str(), 0, nullptr, &g) != 0 || g.gl_pathc < 1) {
        globfree(&g);
        return -1;
    }
    std::string node =
        std::string("/dev/dri/") + (strrchr(g.gl_pathv[0], '/') + 1);
    globfree(&g);
    int fd = open(node.c_str(), O_RDWR | O_CLOEXEC);
    if (fd >= 0) g_render_fds[device] = fd;
    return fd;
}

// Caller holds g_mutex.
ibv_pd *pinPd(int device) {
    if (!g_pin_pds[device]) {
        ibv_context *ctx = ibv_open_device(g_ibv_devices[device]);
        if (!ctx) return nullptr;
        g_pin_pds[device] = ibv_alloc_pd(ctx);
        if (!g_pin_pds[device]) ibv_close_device(ctx);
    }
    return g_pin_pds[device];
}

// Caller holds g_mutex. A one-page dma-buf MR on the buffer at ptr, through
// MC_HIP_GEM_PIN_NIC or else the first RDMA device that accepts one; errno is
// left from the last attempt.
ibv_mr *pinPage(void *ptr, int dmabuf_fd) {
    if (!g_ibv_devices) {
        g_ibv_devices = ibv_get_device_list(&g_ibv_count);
        if (!g_ibv_devices) return nullptr;
        g_pin_pds.assign(g_ibv_count, nullptr);
    }
    const char *want = getenv("MC_HIP_GEM_PIN_NIC");
    ibv_mr *mr = nullptr;
    errno = ENODEV;
    const int device =
        mooncake::pickPinDevice(g_ibv_count, g_pin_device, [&](int i) {
            if (want && strcmp(ibv_get_device_name(g_ibv_devices[i]), want))
                return false;
            ibv_pd *pd = pinPd(i);
            mr = pd ? ibv_reg_dmabuf_mr(pd, 0, getpagesize(),
                                        reinterpret_cast<uintptr_t>(ptr),
                                        dmabuf_fd, IBV_ACCESS_LOCAL_WRITE)
                    : nullptr;
            return mr != nullptr;
        });
    if (device >= 0) g_pin_device = device;
    return mr;
}

void destroy(void *ptr, GemBuffer &b) {
    if (b.pin) ibv_dereg_mr(b.pin);
    if (ptr) (void)hipFree(ptr);  // unmaps the buffer of the external memory
    if (b.ext) (void)hipDestroyExternalMemory(b.ext);
    if (b.dmabuf_fd >= 0) close(b.dmabuf_fd);
    GemClose gc{b.handle, 0};
    if (b.handle) ioctl(b.render_fd, kDrmIoctlGemClose, &gc);
    (void)hipGetLastError();
}

struct DeviceGuard {
    int prev = -1;
    explicit DeviceGuard(int device) {
        if (hipGetDevice(&prev) != hipSuccess) prev = -1;
        (void)hipSetDevice(device);
    }
    ~DeviceGuard() {
        if (prev >= 0) (void)hipSetDevice(prev);
    }
};

}  // namespace

extern "C" {

void *mc_hip_gem_malloc(ssize_t size, int device, hipStream_t stream) {
    (void)stream;
    if (size <= 0) return nullptr;
    std::lock_guard<std::mutex> lock(g_mutex);
    DeviceGuard guard(device);
    const size_t sz = (static_cast<size_t>(size) + kAlign - 1) & ~(kAlign - 1);
    GemBuffer b{sz, 0, renderFd(device), -1, nullptr, nullptr, device};
    if (b.render_fd < 0) {
        logError("no render node", device, sz, strerror(errno));
        return nullptr;
    }
    GemCreate gc{};
    gc.in = {sz, kAlign, kAmdgpuGemDomainVram, 0};
    if (ioctl(b.render_fd, kDrmIoctlAmdgpuGemCreate, &gc) != 0) {
        logError("DRM_IOCTL_AMDGPU_GEM_CREATE", device, sz, strerror(errno));
        return nullptr;
    }
    b.handle = gc.out.handle;
    PrimeHandle ph{b.handle, O_CLOEXEC | O_RDWR, -1};
    if (ioctl(b.render_fd, kDrmIoctlPrimeHandleToFd, &ph) != 0) {
        logError("DRM_IOCTL_PRIME_HANDLE_TO_FD", device, sz, strerror(errno));
        destroy(nullptr, b);
        return nullptr;
    }
    b.dmabuf_fd = ph.fd;

    hipExternalMemoryHandleDesc hd{};
    hd.type = hipExternalMemoryHandleTypeOpaqueFd;
    hd.handle.fd = dup(b.dmabuf_fd);  // HIP takes ownership of this fd
    hd.size = sz;
    hipError_t err = hipImportExternalMemory(&b.ext, &hd);
    if (err != hipSuccess) {
        close(hd.handle.fd);
        b.ext = nullptr;
        logError("hipImportExternalMemory", device, sz, hipGetErrorString(err));
        destroy(nullptr, b);
        return nullptr;
    }
    hipExternalMemoryBufferDesc bd{};
    bd.size = sz;
    void *ptr = nullptr;
    err = hipExternalMemoryGetMappedBuffer(&ptr, b.ext, &bd);
    if (err != hipSuccess) {
        logError("hipExternalMemoryGetMappedBuffer", device, sz,
                 hipGetErrorString(err));
        destroy(nullptr, b);
        return nullptr;
    }
    // The transfer engine picks the dma-buf path only for device memory.
    hipPointerAttribute_t attr{};
    if (hipPointerGetAttributes(&attr, ptr) != hipSuccess ||
        attr.type != hipMemoryTypeDevice) {
        logError("imported buffer is not reported as device memory", device, sz,
                 "");
        destroy(ptr, b);
        return nullptr;
    }
    b.pin = pinPage(ptr, b.dmabuf_fd);
    if (!b.pin) {
        logError("one-page dma-buf pin", device, sz, strerror(errno));
        destroy(ptr, b);
        return nullptr;
    }
    g_buffers[reinterpret_cast<uintptr_t>(ptr)] = b;
    return ptr;
}

void mc_hip_gem_free(void *ptr, ssize_t size, int device, hipStream_t stream) {
    (void)size;
    GemBuffer b{};
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        auto it = g_buffers.find(reinterpret_cast<uintptr_t>(ptr));
        if (it == g_buffers.end()) {
            logError("free of an unknown pointer", device, 0, "");
            return;
        }
        b = it->second;
        g_buffers.erase(it);
    }
    DeviceGuard guard(device);
    if (stream) (void)hipStreamSynchronize(stream);
    (void)hipDeviceSynchronize();
    destroy(ptr, b);
}

// 0 when addr lies in a buffer of this allocator, with its base, size and HIP
// device and, if dmabuf_fd is not NULL, a new descriptor of its dma-buf
// (caller closes).
int mc_hip_gem_lookup(const void *addr, void **base, size_t *size,
                      int *dmabuf_fd, int *device) {
    std::lock_guard<std::mutex> lock(g_mutex);
    const auto a = reinterpret_cast<uintptr_t>(addr);
    auto it = g_buffers.upper_bound(a);
    if (it == g_buffers.begin()) return -1;
    --it;
    if (a >= it->first + it->second.size) return -1;
    if (dmabuf_fd) {
        *dmabuf_fd = dup(it->second.dmabuf_fd);
        if (*dmabuf_fd < 0) return -1;
    }
    if (base) *base = reinterpret_cast<void *>(it->first);
    if (size) *size = it->second.size;
    if (device) *device = it->second.device;
    return 0;
}

}  // extern "C"
