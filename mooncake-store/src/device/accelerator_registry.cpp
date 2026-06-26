#include "device/accelerator_registry.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

namespace mooncake {
namespace device {
namespace {

void RegisterStaticAcceleratorDevice(const AcceleratorDevice& device);

class AcceleratorRegistryImpl final : public AcceleratorRegistry {
    using DeviceList = std::vector<const AcceleratorDevice*>;

   public:
    std::span<const AcceleratorDevice* const> RegisteredDevices()
        const override {
        return std::span<const AcceleratorDevice* const>(
            registered_devices_.data(), registered_devices_.size());
    }

    RuntimeAccelerator RuntimeAccelerators(bool ensure = false) const override {
        auto available_devices = std::atomic_load(&available_devices_);
        if (ensure || !available_devices) {
            std::lock_guard<std::mutex> lock(refresh_mutex_);
            available_devices = std::atomic_load(&available_devices_);
            if (ensure || !available_devices) {
                available_devices = BuildAvailableDevices();
                std::atomic_store(&available_devices_, available_devices);
            }
        }
        return RuntimeAccelerator(*available_devices);
    }

    const AcceleratorDevice* GetDevice(
        AcceleratorVendor vendor) const override {
        for (auto* device : registered_devices_) {
            if (device->Vendor() == vendor) return device;
        }
        return nullptr;
    }

   private:
    void Register(const AcceleratorDevice& device) {
        for (auto*& registered_device : registered_devices_) {
            if (registered_device->Vendor() == device.Vendor()) {
                registered_device = &device;
                return;
            }
        }
        registered_devices_.push_back(&device);
    }

    std::shared_ptr<const DeviceList> BuildAvailableDevices() const {
        auto available_devices = std::make_shared<DeviceList>();
        for (auto* device : registered_devices_) {
            if (device->Available(true)) {
                available_devices->push_back(device);
            }
        }
        return available_devices;
    }

    DeviceList registered_devices_;
    mutable std::mutex refresh_mutex_;
    mutable std::shared_ptr<const DeviceList> available_devices_;

    friend void RegisterStaticAcceleratorDevice(
        const AcceleratorDevice& device);
};

AcceleratorRegistryImpl& MutableRegistry() {
    static AcceleratorRegistryImpl registry;
    return registry;
}

void RegisterStaticAcceleratorDevice(const AcceleratorDevice& device) {
    MutableRegistry().Register(device);
}

}  // namespace

const AcceleratorRegistry& GetAcceleratorRegistry() {
    return MutableRegistry();
}

AcceleratorDeviceRegistrar::AcceleratorDeviceRegistrar(
    const AcceleratorDevice& device) {
    RegisterStaticAcceleratorDevice(device);
}

}  // namespace device
}  // namespace mooncake
