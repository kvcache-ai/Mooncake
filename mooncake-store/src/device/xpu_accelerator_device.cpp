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
//  - AllocatePinnedHost returns USM host memory (sycl::malloc_host) from that
//    context, so any device queue may consume it. All queues share one
//    context (see XpuRuntime::Init).

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
    // one out-of-order queue per device, all on a single platform default
    // context. USM allocations are only valid within the context they were
    // made in: the pinned host buffers handed out by AllocHost are consumed
    // by whichever device queue Copy() picks, so every queue must share the
    // context those buffers come from. When several SYCL platforms expose
    // GPUs (e.g. Level Zero and OpenCL both visible) only one is used: the
    // Level Zero one if present -- that is the backend PyTorch XPU allocates
    // from, so its tensors are recognised -- otherwise the platform with the
    // most usable GPUs. Idempotent; returns false when no usable device is
    // visible.
    bool Init() {
        std::lock_guard<std::mutex> lock(mu_);
        if (initialized_) return !queues_.empty();
        initialized_ = true;
        try {
            std::vector<sycl::device> best;
            bool best_is_level_zero = false;
            for (const auto& platform : sycl::platform::get_platforms()) {
                std::vector<sycl::device> usable;
                for (const auto& d : platform.get_devices()) {
                    if (d.is_gpu() &&
                        d.has(sycl::aspect::usm_device_allocations))
                        usable.push_back(d);
                }
                if (usable.empty()) continue;
                const bool is_level_zero = platform.get_backend() ==
                                           sycl::backend::ext_oneapi_level_zero;
                if (best.empty() || (is_level_zero && !best_is_level_zero) ||
                    (is_level_zero == best_is_level_zero &&
                     usable.size() > best.size())) {
                    best = std::move(usable);
                    best_is_level_zero = is_level_zero;
                }
            }
            if (best.empty()) return false;
            context_ =
                best.front().get_platform().ext_oneapi_get_default_context();
            for (const auto& d : best) queues_.emplace_back(*context_, d);
        } catch (const sycl::exception&) {
            queues_.clear();
            context_.reset();
        }
        return !queues_.empty();
    }

    int DeviceCount() {
        std::lock_guard<std::mutex> lock(mu_);
        return static_cast<int>(queues_.size());
    }

    // Classify `ptr`; returns the device ordinal when it is USM device
    // memory in our context, -1 otherwise.
    int DeviceIndexOf(const void* ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!context_) return -1;
        try {
            if (sycl::get_pointer_type(ptr, *context_) !=
                sycl::usm::alloc::device)
                return -1;
            sycl::device owner = sycl::get_pointer_device(ptr, *context_);
            for (size_t i = 0; i < queues_.size(); ++i) {
                if (queues_[i].get_device() == owner)
                    return static_cast<int>(i);
            }
            // Device memory on a GPU we did not enumerate (e.g. one lacking
            // usm_device_allocations): still device memory, use device 0's
            // queue which shares the context.
            return 0;
        } catch (const sycl::exception&) {
            return -1;  // not a pointer this context knows about
        }
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

    // Pinned host memory in the shared context, usable by every queue.
    void* AllocHost(size_t size) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!context_) return nullptr;
        try {
            return sycl::malloc_host(size, *context_);
        } catch (const sycl::exception&) {
            return nullptr;
        }
    }

    void FreeHost(void* ptr) {
        std::lock_guard<std::mutex> lock(mu_);
        if (!context_ || !ptr) return;
        try {
            sycl::free(ptr, *context_);
        } catch (const sycl::exception&) {
        }
    }

   private:
    std::mutex mu_;
    bool initialized_ = false;
    std::optional<sycl::context> context_;
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
