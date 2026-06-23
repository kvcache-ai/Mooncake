#include "device/accelerator_registry.h"
#include "pinned_buffer_pool.h"

#include <cstring>

namespace mooncake {
namespace device {
namespace {

class HostAcceleratorDevice final : public AcceleratorDevice {
   public:
    AcceleratorVendor Vendor() const override { return AcceleratorVendor::kHost; }
    bool Available(bool ensure = false) const override {
        (void)ensure;
        return true;
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        (void)ptr;
        return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
    }

    int32_t CurrentDeviceId() const override { return -1; }

    void SetContext(int32_t device_id) const override { (void)device_id; }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection direction) const override {
        if (direction != CopyDirection::kHostToHost &&
            direction != CopyDirection::kAuto) {
            return false;
        }
        std::memcpy(dst, src, size);
        return true;
    }

    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        (void)size;
        return PinnedHostBuffer();
    }
};

const AcceleratorDevice& HostDeviceInstance() {
    static HostAcceleratorDevice device;
    return device;
}

const AcceleratorDeviceRegistrar registered_host_device(HostDeviceInstance());

}  // namespace
}  // namespace device
}  // namespace mooncake
