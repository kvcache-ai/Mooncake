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

// Intel XPU (oneAPI SYCL) accelerator device for the Store client.
//
// USE_XPU is a direct-link (native) build: this translation unit includes
// <sycl/sycl.hpp> and is compiled by the Intel DPC++ compiler with -fsycl,
// which propagates from TENT's platform_xpu through transfer_engine. It gives
// the Store the same view of device memory as the CUDA/HIP devices do:
//
//  - QueryPointer classifies any USM pointer -- including memory the Store
//    never allocated, such as a PyTorch XPU tensor -- against the platform
//    default context, which is the context PyTorch allocates from. Only
//    usm::alloc::device is reported as device memory; host and shared USM are
//    host-accessible and treated as host memory, mirroring how the CUDA device
//    reports only cudaMemoryTypeDevice.
//  - Copy stages through a queue bound to that same default context so a
//    memcpy touching foreign USM is legal.
//  - AllocatePinnedHost returns USM host memory (sycl::malloc_host).

#include "device/accelerator_device.h"
#include "device/accelerator_registry.h"
#include "pinned_host_buffer.h"

#if defined(USE_XPU)

#include <sycl/sycl.hpp>

#include <mutex>
#include <optional>
#include <vector>

namespace mooncake {
namespace device {

// Referenced from GetAcceleratorRegistry() so the static registrar below is
// linked in even when mooncake_store is consumed as a static archive.
void EnsureXpuAcceleratorDeviceLinked() {}

namespace {

// Process-wide SYCL state shared by every XpuAcceleratorDevice call.
class XpuRuntime {
   public:
    static XpuRuntime& Instance() {
        // Intentionally leaked: SYCL queues/contexts must not be destroyed
        // during static teardown, after the SYCL runtime may already be gone.
        static XpuRuntime* runtime = new XpuRuntime();
        return *runtime;
    }

    // Enumerate GPU devices that support USM device allocations and build
    // one out-of-order queue per device on the platform default context.
    // Idempotent; returns false when no usable device is visible.
    bool Init() {
        std::lock_guard<std::mutex> lock(mu_);
        if (initialized_) return !queues_.empty();
        initialized_ = true;
        try {
            for (const auto& d : sycl::device::get_devices()) {
                if (!d.is_gpu() || !d.has(sycl::aspect::usm_device_allocations))
                    continue;
                sycl::context ctx =
                    d.get_platform().ext_oneapi_get_default_context();
                bool known = false;
                for (const auto& c : contexts_) {
                    if (c == ctx) {
                        known = true;
                        break;
                    }
                }
                if (!known) contexts_.push_back(ctx);
                queues_.emplace_back(ctx, d);
            }
        } catch (const sycl::exception&) {
            queues_.clear();
            contexts_.clear();
        }
        return !queues_.empty();
    }

    int DeviceCount() {
        std::lock_guard<std::mutex> lock(mu_);
        return static_cast<int>(queues_.size());
    }

    // Classify `ptr`; returns the device ordinal when it is USM device
    // memory, -1 otherwise.
    int DeviceIndexOf(const void* ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& ctx : contexts_) {
            try {
                if (sycl::get_pointer_type(ptr, ctx) !=
                    sycl::usm::alloc::device)
                    continue;
                sycl::device owner = sycl::get_pointer_device(ptr, ctx);
                for (size_t i = 0; i < queues_.size(); ++i) {
                    if (queues_[i].get_device() == owner)
                        return static_cast<int>(i);
                }
                // Device memory on a GPU we did not enumerate (e.g. one lacking
                // usm_device_allocations): still device memory, use device 0's
                // queue which shares the context.
                return 0;
            } catch (const sycl::exception&) {
                // Fall through: not a pointer this context knows about.
            }
        }
        return -1;
    }

    bool Copy(void* dst, const void* src, size_t size, int device_id) {
        std::optional<sycl::queue> q;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (queues_.empty()) return false;
            if (device_id < 0 || device_id >= static_cast<int>(queues_.size()))
                device_id = 0;
            q = queues_[device_id];
        }
        try {
            q->memcpy(dst, src, size).wait();
            return true;
        } catch (const sycl::exception&) {
            return false;
        }
    }

    void* AllocHost(size_t size) {
        std::lock_guard<std::mutex> lock(mu_);
        if (contexts_.empty()) return nullptr;
        try {
            return sycl::malloc_host(size, contexts_.front());
        } catch (const sycl::exception&) {
            return nullptr;
        }
    }

    void FreeHost(void* ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        if (contexts_.empty() || !ptr) return;
        try {
            sycl::free(ptr, contexts_.front());
        } catch (const sycl::exception&) {
        }
    }

   private:
    std::mutex mu_;
    bool initialized_ = false;
    std::vector<sycl::context> contexts_;
    std::vector<sycl::queue> queues_;
};

// SYCL has no implicit "current device"; emulate the CUDA-style SetContext /
// Copy pairing the RuntimeAccelerator helpers rely on with a thread-local.
thread_local int32_t g_current_device = 0;

void FreeXpuPinnedHostBuffer(void* addr) {
    XpuRuntime::Instance().FreeHost(addr);
}

class XpuAcceleratorDevice final : public ProbeCachedAcceleratorDevice {
   public:
    AcceleratorVendor Vendor() const override {
        return AcceleratorVendor::kIntel;
    }

    bool ProbeAvailable() const override {
        return XpuRuntime::Instance().Init();
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        auto& runtime = XpuRuntime::Instance();
        if (!runtime.Init()) {
            return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
        }
        const int device_id = runtime.DeviceIndexOf(ptr);
        if (device_id >= 0) {
            return PointerInfo{.kind = MemoryKind::kDevice,
                               .device_id = device_id};
        }
        return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
    }

    int32_t CurrentDeviceId() const override {
        return XpuRuntime::Instance().DeviceCount() > 0 ? g_current_device : -1;
    }

    void SetContext(int32_t device_id) const override {
        if (device_id >= 0) g_current_device = device_id;
    }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection /*direction*/) const override {
        // queue::memcpy resolves the direction from the USM pointer types.
        return XpuRuntime::Instance().Copy(dst, src, size, g_current_device);
    }

    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        auto& runtime = XpuRuntime::Instance();
        if (!runtime.Init()) return PinnedHostBuffer();
        void* addr = runtime.AllocHost(size);
        if (!addr) return PinnedHostBuffer();
        return PinnedHostBuffer(addr, size, FreeXpuPinnedHostBuffer);
    }
};

const AcceleratorDevice& XpuDeviceInstance() {
    static XpuAcceleratorDevice device;
    return device;
}

const AcceleratorDeviceRegistrar registered_xpu_device(XpuDeviceInstance());

}  // namespace
}  // namespace device
}  // namespace mooncake

#endif  // USE_XPU
