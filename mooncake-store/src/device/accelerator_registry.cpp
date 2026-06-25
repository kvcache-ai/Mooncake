#include "device/accelerator_registry.h"

#include <array>
#include <cstddef>
#include <mutex>
#include <vector>

namespace mooncake {
namespace device {
namespace {

constexpr size_t kVendorCount = 8;

size_t VendorIndex(AcceleratorVendor vendor) {
    switch (vendor) {
        case AcceleratorVendor::kNvidia:
            return 0;
        case AcceleratorVendor::kMusa:
            return 1;
        case AcceleratorVendor::kMaca:
            return 2;
        case AcceleratorVendor::kHygon:
            return 3;
        case AcceleratorVendor::kCorex:
            return 4;
        case AcceleratorVendor::kHip:
            return 5;
        case AcceleratorVendor::kAscend:
            return 6;
        case AcceleratorVendor::kSunrise:
            return 7;
    }
    return 0;
}

class AcceleratorRegistryImpl final : public AcceleratorRegistry {
   public:
    std::span<const AcceleratorDevice* const> RegisteredDevices()
        const override {
        return std::span<const AcceleratorDevice* const>(
            registered_device_list_.data(), registered_device_count_);
    }

    RuntimeAccelerator RuntimeAccelerators(
        bool ensure = false) const override {
        // The registry-level available list represents devices that have been
        // verified usable. We probe once on first use and then optimistically
        // assume devices stay alive during the process lifetime.
        std::call_once(available_init_once_,
                       [this] { RefreshAvailableDevices(true); });
        if (ensure) {
            RefreshAvailableDevices(true);
        }
        std::lock_guard<std::mutex> lock(available_mutex_);
        return RuntimeAccelerator(available_devices_);
    }

    const AcceleratorDevice* GetDevice(AcceleratorVendor vendor)
        const override {
        return registered_devices_[VendorIndex(vendor)];
    }

    void Register(const AcceleratorDevice& device) {
        const size_t index = VendorIndex(device.Vendor());
        if (!registered_devices_[index] &&
            registered_device_count_ < registered_device_list_.size()) {
            registered_device_list_[registered_device_count_++] = &device;
        }
        registered_devices_[index] = &device;
    }

   private:
    void RefreshAvailableDevices(bool ensure) const {
        std::vector<const AcceleratorDevice*> available_devices{};
        for (size_t index = 0; index < registered_devices_.size(); ++index) {
            auto* device = registered_devices_[index];
            if (!device) continue;
            if (device->Available(ensure)) {
                available_devices.push_back(device);
            }
        }
        std::lock_guard<std::mutex> lock(available_mutex_);
        available_devices_ = std::move(available_devices);
    }

    std::array<const AcceleratorDevice*, kVendorCount> registered_devices_{};
    std::array<const AcceleratorDevice*, kVendorCount> registered_device_list_{};
    size_t registered_device_count_ = 0;
    mutable std::once_flag available_init_once_;
    mutable std::mutex available_mutex_;
    mutable std::vector<const AcceleratorDevice*> available_devices_;
};

AcceleratorRegistryImpl& MutableRegistry() {
    static AcceleratorRegistryImpl registry;
    return registry;
}

}  // namespace

const AcceleratorRegistry& GetAcceleratorRegistry() { return MutableRegistry(); }

void RegisterAcceleratorDevice(const AcceleratorDevice& device) {
    MutableRegistry().Register(device);
}

}  // namespace device
}  // namespace mooncake
