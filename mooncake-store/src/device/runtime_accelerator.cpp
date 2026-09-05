#include "device/runtime_accelerator.h"

#include <cstring>
#include <utility>

namespace mooncake {
namespace device {

RuntimeAccelerator::RuntimeAccelerator(
    std::vector<const AcceleratorDevice*> devices)
    : devices_(std::move(devices)) {}

std::span<const AcceleratorDevice* const> RuntimeAccelerator::Devices() const {
    return std::span<const AcceleratorDevice* const>(devices_.data(),
                                                     devices_.size());
}

const AcceleratorDevice* RuntimeAccelerator::FindDeviceForPointer(
    const void* ptr, PointerInfo* out_info) const {
    if (!ptr) return nullptr;
    for (auto* accelerator : devices_) {
        auto info = accelerator->QueryPointer(ptr);
        if (info.kind != MemoryKind::kDevice) continue;
        if (out_info) *out_info = info;
        return accelerator;
    }
    return nullptr;
}

bool RuntimeAccelerator::CopyToHost(void* dst, const void* src,
                                    size_t size) const {
    PointerInfo pointer_info;
    auto* accelerator = FindDeviceForPointer(src, &pointer_info);
    if (!accelerator) {
        std::memcpy(dst, src, size);
        return true;
    }
    const int32_t previous_device = accelerator->CurrentDeviceId();
    accelerator->SetContext(pointer_info.device_id);
    const bool copied =
        accelerator->Copy(dst, src, size, CopyDirection::kDeviceToHost);
    if (previous_device >= 0 && previous_device != pointer_info.device_id) {
        accelerator->SetContext(previous_device);
    }
    return copied;
}

bool RuntimeAccelerator::CopyFromHost(void* dst, const void* src,
                                      size_t size) const {
    PointerInfo pointer_info;
    auto* accelerator = FindDeviceForPointer(dst, &pointer_info);
    if (!accelerator) {
        std::memcpy(dst, src, size);
        return true;
    }
    const int32_t previous_device = accelerator->CurrentDeviceId();
    accelerator->SetContext(pointer_info.device_id);
    const bool copied =
        accelerator->Copy(dst, src, size, CopyDirection::kHostToDevice);
    if (previous_device >= 0 && previous_device != pointer_info.device_id) {
        accelerator->SetContext(previous_device);
    }
    return copied;
}

}  // namespace device
}  // namespace mooncake
