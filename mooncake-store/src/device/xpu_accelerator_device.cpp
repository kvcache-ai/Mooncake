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

#include <glog/logging.h>
#include <sycl/sycl.hpp>

#include <atomic>
#include <cstdint>
#include <exception>
#include <mutex>
#include <new>
#include <optional>
#include <typeinfo>
#include <vector>

#include "device/xpu_accelerator_device_internal.h"

namespace mooncake {
namespace device {

// Referenced from GetAcceleratorRegistry() so the static registrar below is
// linked in even when mooncake_store is consumed as a static archive.
void EnsureXpuAcceleratorDeviceLinked() {}

namespace {
// Armed by xpu::testing::InjectCopyFault; g_fault_kind is 0 otherwise.
std::atomic<int> g_fault_kind{0};
std::atomic<int64_t> g_fault_skip{0};
}  // namespace

namespace xpu::testing {
void InjectCopyFault(CopyFault fault, int64_t skip) {
    g_fault_skip.store(skip, std::memory_order_relaxed);
    g_fault_kind.store(static_cast<int>(fault), std::memory_order_relaxed);
}
}  // namespace xpu::testing

namespace {

// Throws the fault armed by xpu::testing::InjectCopyFault, if it is due.
void MaybeInjectCopyFault() {
    if (g_fault_kind.load(std::memory_order_relaxed) == 0) return;
    if (g_fault_skip.fetch_sub(1, std::memory_order_relaxed) != 0) return;
    switch (static_cast<xpu::testing::CopyFault>(g_fault_kind.exchange(0))) {
        case xpu::testing::CopyFault::kSyclError:
            throw sycl::exception(sycl::make_error_code(sycl::errc::runtime),
                                  "injected copy fault");
        case xpu::testing::CopyFault::kBadAlloc:
            throw std::bad_alloc();
        case xpu::testing::CopyFault::kNone:
            break;
    }
}

// Process-wide SYCL state shared by every XpuAcceleratorDevice call.
//
// context_ and queues_ are written once, by the first Init(), and published
// through ready_; every other method reads them without a lock. sycl::queue
// submissions are thread-safe, so concurrent copies share the queues.
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
        if (ready_.load(std::memory_order_acquire)) return !queues_.empty();
        std::lock_guard<std::mutex> lock(mu_);
        if (ready_.load(std::memory_order_relaxed)) return !queues_.empty();
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
            if (!best.empty()) {
                context_ = best.front()
                               .get_platform()
                               .ext_oneapi_get_default_context();
                for (const auto& d : best) queues_.emplace_back(*context_, d);
                devices_ = std::move(best);
            }
        } catch (const sycl::exception& e) {
            LOG(ERROR) << "XPU runtime init failed: " << e.what();
            Reset();
        } catch (const std::exception& e) {
            LOG(ERROR) << "XPU runtime init failed (" << typeid(e).name()
                       << "): " << e.what();
            Reset();
        } catch (...) {
            LOG(ERROR) << "XPU runtime init failed: unknown exception";
            Reset();
        }
        ready_.store(true, std::memory_order_release);
        return !queues_.empty();
    }

    int DeviceCount() const {
        if (!ready_.load(std::memory_order_acquire)) return 0;
        return static_cast<int>(queues_.size());
    }

    // Classify `ptr`: the queue index when it is USM device memory in our
    // context, xpu::kUnsupportedDevice when it is device memory on a GPU
    // without a queue, xpu::kNotDeviceMemory otherwise.
    int DeviceIndexOf(const void* ptr) {
        if (!ready_.load(std::memory_order_acquire) || !context_)
            return xpu::kNotDeviceMemory;
        try {
            if (sycl::get_pointer_type(ptr, *context_) !=
                sycl::usm::alloc::device)
                return xpu::kNotDeviceMemory;
            const sycl::device owner = sycl::get_pointer_device(ptr, *context_);
            const int index = xpu::OwnerQueueIndex(owner, devices_);
            if (index == xpu::kUnsupportedDevice &&
                unknown_owners_.First(owner))
                LOG(WARNING)
                    << "XPU device memory at " << ptr << " belongs to GPU '"
                    << owner.get_info<sycl::info::device::name>()
                    << "', which has no Store queue (it lacks USM device "
                       "allocations or was not enumerated); copies touching "
                       "its memory fail";
            return index;
        } catch (const sycl::exception&) {
            // Not a pointer this context knows about.
        } catch (const std::exception& e) {
            LOG(ERROR) << "XPU pointer query failed (" << typeid(e).name()
                       << "): " << e.what();
        } catch (...) {
            LOG(ERROR) << "XPU pointer query failed: unknown exception";
        }
        return xpu::kNotDeviceMemory;
    }

    // True when `ptr` is not USM memory of our context, i.e. ordinary
    // pageable host memory.
    bool IsPageable(const void* ptr) {
        if (!ready_.load(std::memory_order_acquire) || !context_) return true;
        try {
            return sycl::get_pointer_type(ptr, *context_) ==
                   sycl::usm::alloc::unknown;
        } catch (...) {
            return true;
        }
    }

    // Start a copy on `device_id`'s queue. Returns nullopt, with nothing in
    // flight, when it could not be started.
    std::optional<sycl::event> Submit(void* dst, const void* src, size_t size,
                                      int device_id) {
        if (!ready_.load(std::memory_order_acquire) || queues_.empty())
            return std::nullopt;
        if (device_id < 0 || device_id >= static_cast<int>(queues_.size())) {
            LOG(ERROR) << "XPU copy rejected: device " << device_id
                       << " has no queue (" << queues_.size() << " devices)";
            return std::nullopt;
        }
        try {
            MaybeInjectCopyFault();
            return queues_[device_id].memcpy(dst, src, size);
        } catch (const sycl::exception& e) {
            LOG(ERROR) << "XPU memcpy submit failed: " << e.what();
        } catch (const std::exception& e) {
            LOG(ERROR) << "XPU memcpy submit failed (" << typeid(e).name()
                       << "): " << e.what();
        } catch (...) {
            LOG(ERROR) << "XPU memcpy submit failed: unknown exception";
        }
        return std::nullopt;
    }

    // Wait for a submitted copy; false when it failed.
    static bool Wait(sycl::event& event) {
        try {
            event.wait();
            return true;
        } catch (const sycl::exception& e) {
            LOG(ERROR) << "XPU memcpy failed: " << e.what();
        } catch (const std::exception& e) {
            LOG(ERROR) << "XPU memcpy failed (" << typeid(e).name()
                       << "): " << e.what();
        } catch (...) {
            LOG(ERROR) << "XPU memcpy failed: unknown exception";
        }
        return false;
    }

    bool Copy(void* dst, const void* src, size_t size, int device_id) {
        auto event = Submit(dst, src, size, device_id);
        return event && Wait(*event);
    }

    // Pinned host memory in the shared context, usable by every queue.
    void* AllocHost(size_t size) {
        if (!ready_.load(std::memory_order_acquire) || !context_)
            return nullptr;
        try {
            return sycl::malloc_host(size, *context_);
        } catch (const sycl::exception& e) {
            LOG(ERROR) << "XPU pinned host allocation failed: " << e.what();
        } catch (const std::exception& e) {
            LOG(ERROR) << "XPU pinned host allocation failed ("
                       << typeid(e).name() << "): " << e.what();
        } catch (...) {
            LOG(ERROR) << "XPU pinned host allocation failed: unknown "
                          "exception";
        }
        return nullptr;
    }

    void FreeHost(void* ptr) {
        if (!ready_.load(std::memory_order_acquire) || !context_ || !ptr)
            return;
        try {
            sycl::free(ptr, *context_);
        } catch (const sycl::exception& e) {
            LOG(ERROR) << "XPU pinned host free failed: " << e.what();
        } catch (const std::exception& e) {
            LOG(ERROR) << "XPU pinned host free failed (" << typeid(e).name()
                       << "): " << e.what();
        } catch (...) {
            LOG(ERROR) << "XPU pinned host free failed: unknown exception";
        }
    }

   private:
    // Leave no partially built state after a failed Init().
    void Reset() {
        queues_.clear();
        devices_.clear();
        context_.reset();
    }

    std::mutex mu_;  // serialises Init() only
    std::atomic<bool> ready_{false};
    std::optional<sycl::context> context_;
    std::vector<sycl::queue> queues_;
    std::vector<sycl::device> devices_;  // queues_[i].get_device()
    xpu::OncePerKey<sycl::device> unknown_owners_;
};

// SYCL has no implicit "current device"; emulate the CUDA-style SetContext /
// Copy pairing the RuntimeAccelerator helpers rely on with a thread-local.
// A negative value (from a pointer on a GPU without a queue) makes Copy fail.
thread_local int32_t g_current_device = 0;

// Copies this thread started with CopyAsync and has not waited for yet.
thread_local std::vector<sycl::event> g_pending_copies;

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
        if (device_id == xpu::kUnsupportedDevice) {
            // Still device memory: it must not be touched with a host
            // memcpy. device_id -1 makes Copy fail.
            return PointerInfo{.kind = MemoryKind::kDevice, .device_id = -1};
        }
        return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
    }

    int32_t CurrentDeviceId() const override {
        return XpuRuntime::Instance().DeviceCount() > 0 ? g_current_device : -1;
    }

    void SetContext(int32_t device_id) const override {
        g_current_device = device_id;
    }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection /*direction*/) const override {
        // queue::memcpy resolves the direction from the USM pointer types.
        return XpuRuntime::Instance().Copy(dst, src, size, g_current_device);
    }

    // Starts the memcpy and keeps its event; Synchronize waits for all of
    // them, so a batch of copies pays one round trip instead of one each.
    bool CopyAsync(void* dst, const void* src, size_t size,
                   CopyDirection direction) const override {
        auto& runtime = XpuRuntime::Instance();
        // The driver copies to or from pageable host memory before submit
        // returns, so keeping such an event buys nothing and only adds
        // bookkeeping: wait for it now.
        const void* host = direction == CopyDirection::kHostToDevice ? src
                           : direction == CopyDirection::kDeviceToHost
                               ? static_cast<const void*>(dst)
                               : nullptr;
        if (host && runtime.IsPageable(host))
            return runtime.Copy(dst, src, size, g_current_device);
        try {
            // Reserve first so that keeping the event cannot throw once the
            // copy is in flight.
            g_pending_copies.reserve(g_pending_copies.size() + 1);
        } catch (...) {
            LOG(ERROR) << "XPU copy rejected: out of memory";
            return false;
        }
        auto event = runtime.Submit(dst, src, size, g_current_device);
        if (!event) return false;
        g_pending_copies.push_back(std::move(*event));
        return true;
    }

    bool Synchronize() const override {
        bool ok = true;
        for (auto& event : g_pending_copies) ok = XpuRuntime::Wait(event) && ok;
        g_pending_copies.clear();
        return ok;
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
