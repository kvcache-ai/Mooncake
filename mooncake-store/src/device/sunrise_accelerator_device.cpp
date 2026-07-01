#include "device/accelerator_registry.h"
#include "pinned_host_buffer.h"
#include "sunrise_allocator.h"

#if defined(USE_SUNRISE)
#include <tang_runtime_api.h>
#include <cstring>
#include <mutex>
#include <vector>

namespace mooncake {
namespace device {
namespace {

struct SavedTangDevice {
    int dev{-1};
    SavedTangDevice() { tangGetDevice(&dev); }
    ~SavedTangDevice() {
        if (dev >= 0) tangSetDevice(dev);
    }
    SavedTangDevice(const SavedTangDevice&) = delete;
    SavedTangDevice& operator=(const SavedTangDevice&) = delete;
};

void FreeSunrisePinnedHostBuffer(void* addr) {
    if (!addr) return;
    {
        std::lock_guard<std::mutex> guard(
            mooncake::sunrise_alloc_detail::tangAllocMutex());
        mooncake::sunrise_alloc_detail::tangHostAllocatedSet().erase(addr);
    }
    mooncake::sunrise_alloc_detail::removeStoreMemRange(addr);
    tangFreeHost(addr);
}

class SunriseAcceleratorDevice final : public ProbeCachedAcceleratorDevice {
   public:
    AcceleratorVendor Vendor() const override {
        return AcceleratorVendor::kSunrise;
    }

    bool ProbeAvailable() const override {
        int dev = -1;
        return tangGetDevice(&dev) == tangSuccess;
    }

    PointerInfo QueryPointer(const void* ptr) const override {
        if (sunrise_is_device_memory_range(const_cast<void*>(ptr))) {
            return PointerInfo{.kind = MemoryKind::kDevice, .device_id = 0};
        }
        return PointerInfo{.kind = MemoryKind::kHost, .device_id = -1};
    }

    int32_t CurrentDeviceId() const override {
        int dev = -1;
        return tangGetDevice(&dev) == tangSuccess ? dev : -1;
    }

    void SetContext(int32_t device_id) const override {
        if (device_id >= 0) tangSetDevice(device_id);
    }

    bool Copy(void* dst, const void* src, size_t size,
              CopyDirection direction) const override {
        switch (direction) {
            case CopyDirection::kHostToHost:
                std::memcpy(dst, src, size);
                return true;

            case CopyDirection::kDeviceToHost: {
                bool dst_host_alloc = sunrise_is_host_allocated(dst);
                bool dst_dev = sunrise_is_device_memory_range(dst);

                if (dst_host_alloc && !dst_dev) {
                    SavedTangDevice saved;
                    tangSetDevice(0);
                    std::vector<char> staging(size);
                    if (tangMemcpy(staging.data(), src, size,
                                   tangMemcpyDeviceToHost) != tangSuccess)
                        return false;
                    tangDeviceSynchronize();
                    std::memcpy(dst, staging.data(), size);
                    return true;
                }
                if (tangMemcpy(dst, src, size, tangMemcpyDeviceToHost) !=
                    tangSuccess)
                    return false;
                tangDeviceSynchronize();
                return true;
            }

            case CopyDirection::kHostToDevice: {
                bool src_host_alloc =
                    sunrise_is_host_allocated(const_cast<void*>(src));
                bool src_dev =
                    sunrise_is_device_memory_range(const_cast<void*>(src));

                if (src_host_alloc && !src_dev) {
                    SavedTangDevice saved;
                    tangSetDevice(0);
                    std::vector<char> staging(size);
                    std::memcpy(staging.data(), src, size);
                    return tangMemcpy(dst, staging.data(), size,
                                      tangMemcpyHostToDevice) == tangSuccess;
                }
                return tangMemcpy(dst, src, size, tangMemcpyHostToDevice) ==
                       tangSuccess;
            }

            case CopyDirection::kDeviceToDevice:
                return tangMemcpy(dst, src, size, tangMemcpyDeviceToDevice) ==
                       tangSuccess;

            case CopyDirection::kAuto: {
                bool src_dev =
                    sunrise_is_device_memory_range(const_cast<void*>(src));
                bool dst_dev = sunrise_is_device_memory_range(dst);
                if (!src_dev && !dst_dev) {
                    std::memcpy(dst, src, size);
                    return true;
                }
                if (src_dev && dst_dev)
                    return Copy(dst, src, size, CopyDirection::kDeviceToDevice);
                if (src_dev)
                    return Copy(dst, src, size, CopyDirection::kDeviceToHost);
                return Copy(dst, src, size, CopyDirection::kHostToDevice);
            }
        }
        return false;
    }

    PinnedHostBuffer AllocatePinnedHost(size_t size) const override {
        void* addr = nullptr;
        if (tangHostAlloc(&addr, size, tangHostAllocDefault) == tangSuccess) {
            {
                std::lock_guard<std::mutex> guard(
                    mooncake::sunrise_alloc_detail::tangAllocMutex());
                mooncake::sunrise_alloc_detail::tangHostAllocatedSet().insert(
                    addr);
            }
            mooncake::sunrise_alloc_detail::addStoreMemRange(addr, size);
            return PinnedHostBuffer(addr, size, FreeSunrisePinnedHostBuffer);
        }
        return PinnedHostBuffer();
    }
};

const AcceleratorDevice& SunriseDeviceInstance() {
    static SunriseAcceleratorDevice device;
    return device;
}

const AcceleratorDeviceRegistrar registered_sunrise_device(
    SunriseDeviceInstance());

}  // namespace
}  // namespace device
}  // namespace mooncake

#endif
