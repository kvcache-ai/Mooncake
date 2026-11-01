// Copyright(C) 2025 Advanced Micro Devices, Inc. All rights reserved.
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

#include "transport/hip_transport/batch_copy.h"

#include <dlfcn.h>
#include <glog/logging.h>
#include <hip/hiprtc.h>

#include <algorithm>
#include <cstring>
#include <type_traits>

#include "hip_device_guard.h"

namespace mooncake {

namespace {

// One workgroup per range at a time. 16-byte aligned ranges move in 16-byte
// vectors. The system-scope fence at the end makes the stores to peer memory
// visible to the peer once the stream's completion event fires.
const char* const kKernelSource = R"(
struct __attribute__((aligned(16))) Vec16 {
    unsigned int x, y, z, w;
};

extern "C" __global__ void __launch_bounds__(256)
    mooncake_batch_copy(const unsigned long long* ranges, unsigned int n) {
    for (unsigned int i = blockIdx.x; i < n; i += gridDim.x) {
        const unsigned long long src = ranges[3 * i];
        const unsigned long long dst = ranges[3 * i + 1];
        const unsigned long long len = ranges[3 * i + 2];
        unsigned long long done = 0;
        if (((src | dst) & 15) == 0) {
            const Vec16* s = (const Vec16*)src;
            Vec16* d = (Vec16*)dst;
            const unsigned long long nv = len >> 4;
            for (unsigned long long k = threadIdx.x; k < nv; k += blockDim.x)
                d[k] = s[k];
            done = nv << 4;
        }
        const char* s = (const char*)src;
        char* d = (char*)dst;
        for (unsigned long long k = done + threadIdx.x; k < len;
             k += blockDim.x)
            d[k] = s[k];
    }
    __threadfence_system();
}
)";

constexpr unsigned int kBlockThreads = 256;

struct Hiprtc {
    decltype(&hiprtcCreateProgram) create = nullptr;
    decltype(&hiprtcCompileProgram) compile = nullptr;
    decltype(&hiprtcGetProgramLogSize) log_size = nullptr;
    decltype(&hiprtcGetProgramLog) log = nullptr;
    decltype(&hiprtcGetCodeSize) code_size = nullptr;
    decltype(&hiprtcGetCode) code = nullptr;
    decltype(&hiprtcDestroyProgram) destroy = nullptr;

    // Never unloaded: the handle lives for the process.
    static const Hiprtc* get() {
        static const Hiprtc* instance = []() -> const Hiprtc* {
            void* handle = nullptr;
            for (const char* name :
                 {"libhiprtc.so", "libhiprtc.so.7", "libhiprtc.so.6"}) {
                handle = dlopen(name, RTLD_NOW | RTLD_LOCAL);
                if (handle) break;
            }
            if (!handle) {
                LOG(WARNING)
                    << "HipTransport: libhiprtc not found (" << dlerror()
                    << "), using hipMemcpyAsync for every copy";
                return nullptr;
            }
            auto* rtc = new Hiprtc();
            bool ok = true;
            auto resolve = [&](auto& fn, const char* symbol) {
                fn = reinterpret_cast<std::remove_reference_t<decltype(fn)>>(
                    dlsym(handle, symbol));
                ok = ok && fn != nullptr;
            };
            resolve(rtc->create, "hiprtcCreateProgram");
            resolve(rtc->compile, "hiprtcCompileProgram");
            resolve(rtc->log_size, "hiprtcGetProgramLogSize");
            resolve(rtc->log, "hiprtcGetProgramLog");
            resolve(rtc->code_size, "hiprtcGetCodeSize");
            resolve(rtc->code, "hiprtcGetCode");
            resolve(rtc->destroy, "hiprtcDestroyProgram");
            if (!ok) {
                LOG(WARNING) << "HipTransport: libhiprtc lacks a required "
                                "symbol, using hipMemcpyAsync for every copy";
                delete rtc;
                return nullptr;
            }
            return rtc;
        }();
        return instance;
    }
};

bool compileKernel(const std::string& arch, std::string& code_object) {
    const Hiprtc* rtc = Hiprtc::get();
    if (!rtc) return false;
    hiprtcProgram program;
    if (rtc->create(&program, kKernelSource, "mooncake_batch_copy.hip", 0,
                    nullptr, nullptr) != HIPRTC_SUCCESS) {
        LOG(WARNING) << "HipTransport: hiprtcCreateProgram failed";
        return false;
    }
    std::string arch_option = "--gpu-architecture=" + arch;
    const char* options[] = {arch_option.c_str(), "-O3"};
    bool ok = rtc->compile(program, 2, options) == HIPRTC_SUCCESS;
    if (!ok) {
        size_t size = 0;
        std::string log;
        if (rtc->log_size(program, &size) == HIPRTC_SUCCESS && size > 1) {
            log.resize(size);
            rtc->log(program, log.data());
        }
        LOG(WARNING) << "HipTransport: compiling the batch copy kernel for "
                     << arch << " failed: " << log;
    } else {
        size_t size = 0;
        ok = rtc->code_size(program, &size) == HIPRTC_SUCCESS && size > 0;
        if (ok) {
            code_object.resize(size);
            ok = rtc->code(program, code_object.data()) == HIPRTC_SUCCESS;
        }
    }
    rtc->destroy(&program);
    return ok;
}

}  // namespace

BatchCopyKernel::BatchCopyKernel(int num_slots, int max_grid)
    : num_slots_(num_slots), max_grid_(max_grid) {}

BatchCopyKernel::~BatchCopyKernel() {
    HipDeviceGuard device_guard;
    for (auto& [device_id, dev] : devices_) {
        if (!dev.loaded) continue;
        if (hipSetDevice(device_id) != hipSuccess) continue;
        for (hipEvent_t event : dev.events) (void)hipEventSynchronize(event);
        for (hipEvent_t event : dev.events) (void)hipEventDestroy(event);
        (void)hipModuleUnload(dev.module);
        (void)hipHostFree(dev.host);
        (void)hipFree(dev.device);
    }
}

// Called with mutex_ held and device_id current.
bool BatchCopyKernel::load(int device_id, Device& dev) {
    hipDeviceProp_t prop;
    if (hipGetDeviceProperties(&prop, device_id) != hipSuccess) return false;
    std::string arch = prop.gcnArchName;
    auto it = code_objects_.find(arch);
    if (it == code_objects_.end()) {
        std::string code_object;
        if (!compileKernel(arch, code_object)) return false;
        it = code_objects_.emplace(arch, std::move(code_object)).first;
    }
    if (hipModuleLoadData(&dev.module, it->second.data()) != hipSuccess)
        return false;
    const size_t bytes = sizeof(Range) * kSlotRanges * num_slots_;
    bool ok =
        hipModuleGetFunction(&dev.function, dev.module,
                             "mooncake_batch_copy") == hipSuccess &&
        hipHostMalloc(reinterpret_cast<void**>(&dev.host), bytes) ==
            hipSuccess &&
        hipMalloc(reinterpret_cast<void**>(&dev.device), bytes) == hipSuccess;
    for (int slot = 0; ok && slot < num_slots_; ++slot) {
        hipEvent_t event;
        ok = hipEventCreateWithFlags(&event, hipEventDisableTiming) ==
             hipSuccess;
        if (ok) dev.events.push_back(event);
    }
    if (!ok) {
        for (hipEvent_t event : dev.events) (void)hipEventDestroy(event);
        dev.events.clear();
        if (dev.device) (void)hipFree(dev.device);
        if (dev.host) (void)hipHostFree(dev.host);
        (void)hipModuleUnload(dev.module);
        return false;
    }
    for (int slot = num_slots_ - 1; slot >= 0; --slot)
        dev.free_slots.push_back(slot);
    return true;
}

int BatchCopyKernel::acquireSlot(Device& dev) {
    while (!dev.busy_slots.empty() &&
           hipEventQuery(dev.events[dev.busy_slots.front()]) == hipSuccess) {
        dev.free_slots.push_back(dev.busy_slots.front());
        dev.busy_slots.pop_front();
    }
    if (dev.free_slots.empty()) {
        int oldest = dev.busy_slots.front();
        if (hipEventSynchronize(dev.events[oldest]) != hipSuccess) return -1;
        dev.busy_slots.pop_front();
        return oldest;
    }
    int slot = dev.free_slots.back();
    dev.free_slots.pop_back();
    return slot;
}

bool BatchCopyKernel::available(int device_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    Device& dev = devices_[device_id];
    if (!dev.loaded && !dev.failed) {
        dev.loaded = load(device_id, dev);
        dev.failed = !dev.loaded;
        if (dev.failed) {
            // The caller falls back to hipMemcpyAsync, so drop the error a
            // failed HIP call left behind; a later hipGetLastError in the
            // application (e.g. PyTorch's kernel launch check) would see it.
            (void)hipGetLastError();
            LOG(WARNING) << "HipTransport: batch copy kernel unavailable on "
                            "device "
                         << device_id << ", using hipMemcpyAsync";
        }
    }
    return dev.loaded;
}

size_t BatchCopyKernel::enqueue(int device_id, hipStream_t stream,
                                const Range* ranges, size_t n) {
    Device* found;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        found = &devices_[device_id];
        if (!found->loaded) return 0;
    }
    Device& dev = *found;
    std::lock_guard<std::mutex> lock(dev.mutex);
    size_t done = 0;
    while (done < n) {
        int slot = acquireSlot(dev);
        if (slot < 0) {
            (void)hipGetLastError();  // the rest falls back, as below
            break;
        }
        Range* host = dev.host + slot * kSlotRanges;
        Range* device = dev.device + slot * kSlotRanges;
        const size_t count = std::min(kSlotRanges, n - done);
        memcpy(host, ranges + done, count * sizeof(Range));
        unsigned int num = static_cast<unsigned int>(count);
        const void* arg_ranges = device;
        void* args[] = {&arg_ranges, &num};
        unsigned int grid =
            std::min<unsigned int>(num, static_cast<unsigned int>(max_grid_));
        if (hipMemcpyAsync(device, host, count * sizeof(Range),
                           hipMemcpyHostToDevice, stream) != hipSuccess ||
            hipModuleLaunchKernel(dev.function, grid, 1, 1, kBlockThreads, 1, 1,
                                  0, stream, args, nullptr) != hipSuccess ||
            hipEventRecord(dev.events[slot], stream) != hipSuccess) {
            LOG(ERROR) << "HipTransport: batch copy launch failed on device "
                       << device_id;
            // Whatever of this launch was enqueued may still read the slot.
            (void)hipStreamSynchronize(stream);
            dev.free_slots.push_back(slot);
            // The caller copies the rest with hipMemcpyAsync; do not leave
            // this failure as the application's last error.
            (void)hipGetLastError();
            break;
        }
        dev.busy_slots.push_back(slot);
        done += count;
    }
    return done;
}

}  // namespace mooncake
