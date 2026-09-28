#pragma once

#include <span>
#include <vector>

#include "device/accelerator_device.h"
#include "device/runtime_accelerator.h"

namespace mooncake {
namespace device {

class AcceleratorRegistry {
   public:
    virtual ~AcceleratorRegistry() = default;

    virtual std::span<const AcceleratorDevice* const> RegisteredDevices()
        const = 0;
    virtual RuntimeAccelerator RuntimeAccelerators(
        bool ensure = false) const = 0;
    virtual const AcceleratorDevice* GetDevice(
        AcceleratorVendor vendor) const = 0;
};

const AcceleratorRegistry& GetAcceleratorRegistry();

class AcceleratorDeviceRegistrar {
   public:
    explicit AcceleratorDeviceRegistrar(const AcceleratorDevice& device);
};

}  // namespace device
}  // namespace mooncake
