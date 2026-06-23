#include "device/accelerator_registry.h"

#include <array>
#include <cstddef>
#include <mutex>
#include <vector>

namespace mooncake {
namespace device {
namespace {

std::vector<const AcceleratorDevice*>& RegisteredDeviceStorage() {
    static std::vector<const AcceleratorDevice*> devices;
    return devices;
}

constexpr size_t kVendorCount = 9;

size_t VendorIndex(AcceleratorVendor vendor) {
    switch (vendor) {
        case AcceleratorVendor::kHost:
            return 0;
        case AcceleratorVendor::kNvidia:
            return 1;
        case AcceleratorVendor::kMusa:
            return 2;
        case AcceleratorVendor::kMaca:
            return 3;
        case AcceleratorVendor::kHygon:
            return 4;
        case AcceleratorVendor::kCorex:
            return 5;
        case AcceleratorVendor::kHip:
            return 6;
        case AcceleratorVendor::kAscend:
            return 7;
        case AcceleratorVendor::kSunrise:
            return 8;
    }
    return 0;
}

class AcceleratorRegistryImpl final : public AcceleratorRegistry {
   public:
    std::span<const AcceleratorDevice* const> RegisteredDevices()
        const override {
        const auto& devices = RegisteredDeviceStorage();
        return std::span<const AcceleratorDevice* const>(devices.data(),
                                                        devices.size());
    }

    std::vector<const AcceleratorDevice*> AvailableDevices(
        bool ensure = false) const override {
        std::call_once(available_init_once_,
                       [this] { UpdateAvailableDevices(false); });
        if (ensure) UpdateAvailableDevices(true);
        return CollectAvailableDevices();
    }

    const AcceleratorDevice* GetDevice(AcceleratorVendor vendor)
        const override {
        for (auto* device : RegisteredDeviceStorage()) {
            if (device->Vendor() == vendor) return device;
        }
        return nullptr;
    }

   private:
    void UpdateAvailableDevices(bool ensure) const {
        for (auto* device : RegisteredDeviceStorage()) {
            const auto vendor = device->Vendor();
            if (vendor == AcceleratorVendor::kHost) continue;
            const size_t index = VendorIndex(vendor);
            std::lock_guard<std::mutex> lock(available_mutexes_[index]);
            available_devices_[index] =
                device->Available(ensure) ? device : nullptr;
        }
    }

    std::vector<const AcceleratorDevice*> CollectAvailableDevices() const {
        std::vector<const AcceleratorDevice*> available;
        for (size_t index = 0; index < available_devices_.size(); ++index) {
            std::lock_guard<std::mutex> lock(available_mutexes_[index]);
            if (available_devices_[index]) {
                available.push_back(available_devices_[index]);
            }
        }
        return available;
    }

    mutable std::once_flag available_init_once_;
    mutable std::array<std::mutex, kVendorCount> available_mutexes_;
    mutable std::array<const AcceleratorDevice*, kVendorCount>
        available_devices_{};
};

AcceleratorRegistryImpl& MutableRegistry() {
    static AcceleratorRegistryImpl registry;
    return registry;
}

}  // namespace

const AcceleratorRegistry& GetAcceleratorRegistry() { return MutableRegistry(); }

void RegisterAcceleratorDevice(const AcceleratorDevice& device) {
    RegisteredDeviceStorage().push_back(&device);
}

}  // namespace device
}  // namespace mooncake
