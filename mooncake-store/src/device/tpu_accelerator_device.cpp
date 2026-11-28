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

// Google TPU accelerator device for the Store client.
//
// TPU HBM has no CPU address. What the Store holds as a device pointer is the
// token the serving framework registered with the TPU adapter (see
// tent/platform/tpu_pjrt_abi.h), and only the adapter can copy to or from it.
// This device classifies and copies through TENT's TpuPjrtShim, so the Store
// and TENT agree on which addresses are TPU memory.

#include "device/accelerator_registry.h"
#include "pinned_host_buffer.h"

#if defined(USE_TPU)

#include "tent/platform/tpu_pjrt_shim.h"

namespace mooncake {
namespace device {

// Referenced from GetAcceleratorRegistry() so the static registrar below is
// linked in even when mooncake_store is consumed as a static archive.
void EnsureTpuAcceleratorDeviceLinked() {}

namespace {

class TpuAcceleratorDevice final : public ProbeCachedAcceleratorDevice {
   public:
    AcceleratorVendor Vendor() const override {
        return AcceleratorVendor::kTpu;
    }

    bool ProbeAvailable() const override {
        return tent::TpuPjrtShim::instance().available();
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        auto& shim = tent::TpuPjrtShim::instance();
        if (!shim.isDevicePtr(ptr)) {
            return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
        }
        return PointerInfo{.kind = MemoryKind::kDevice,
                           .device_id = shim.deviceIndex(ptr)};
    }

    // The adapter finds the device from the token, so there is no current
    // device to track.
    int32_t CurrentDeviceId() const override { return -1; }

    void SetContext(int32_t /*device_id*/) const override {}

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection /*direction*/) const override {
        // The adapter knows which side is the device, so ask it instead of
        // trusting `direction`. The ABI has no HBM-to-HBM copy, and neither
        // side being a device means the token was released after the caller
        // classified it: fail both rather than dereference a token.
        auto& shim = tent::TpuPjrtShim::instance();
        const bool from_device = shim.isDevicePtr(src);
        if (from_device == shim.isDevicePtr(dst)) return false;
        return (from_device ? shim.copyD2H(dst, src, size)
                            : shim.copyH2D(dst, src, size))
            .ok();
    }

    // The adapter ABI has no pinned host allocator: callers fall back to
    // pageable memory.
    PinnedHostBuffer AllocatePinnedHost(size_t /*size*/) const override {
        return PinnedHostBuffer();
    }
};

const AcceleratorDevice& TpuDeviceInstance() {
    static TpuAcceleratorDevice device;
    return device;
}

const AcceleratorDeviceRegistrar registered_tpu_device(TpuDeviceInstance());

}  // namespace
}  // namespace device
}  // namespace mooncake

#endif  // USE_TPU
