#pragma once

#include <cstddef>
#include <span>
#include <vector>

#include "device/accelerator_device.h"

namespace mooncake {
namespace device {

class RuntimeAccelerator {
   public:
    RuntimeAccelerator() = default;
    explicit RuntimeAccelerator(std::vector<const AcceleratorDevice*> devices);

    std::span<const AcceleratorDevice* const> Devices() const;

    const AcceleratorDevice* FindDeviceForPointer(
        const void* ptr, PointerInfo* out_info = nullptr) const;

    bool CopyToHost(void* dst, const void* src, size_t size) const;

    bool CopyFromHost(void* dst, const void* src, size_t size) const;

    // Copy between two pointers that may each be host or device resident;
    // falls back to memcpy when neither side is on a known accelerator.
    bool CopyAuto(void* dst, const void* src, size_t size) const;

   private:
    std::vector<const AcceleratorDevice*> devices_;
};

}  // namespace device
}  // namespace mooncake
