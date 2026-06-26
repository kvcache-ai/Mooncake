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
    accelerator->SetContext(pointer_info.device_id);
    return accelerator->Copy(dst, src, size, CopyDirection::kDeviceToHost);
}

bool RuntimeAccelerator::CopyFromHost(void* dst, const void* src,
                                      size_t size) const {
    PointerInfo pointer_info;
    auto* accelerator = FindDeviceForPointer(dst, &pointer_info);
    if (!accelerator) {
        std::memcpy(dst, src, size);
        return true;
    }
    accelerator->SetContext(pointer_info.device_id);
    return accelerator->Copy(dst, src, size, CopyDirection::kHostToDevice);
}

bool RuntimeAccelerator::CopyMaybeAccelerator(void* dst, const void* src,
                                              size_t size) const {
    PointerInfo src_info;
    PointerInfo dst_info;
    auto* src_device = FindDeviceForPointer(src, &src_info);
    auto* dst_device = FindDeviceForPointer(dst, &dst_info);

    if (!src_device && !dst_device) {
        std::memcpy(dst, src, size);
        return true;
    }
    if (src_device && dst_device && src_device != dst_device) {
        return false;
    }

    auto* accelerator = src_device ? src_device : dst_device;
    const auto& info = src_device ? src_info : dst_info;
    accelerator->SetContext(info.device_id);
    CopyDirection direction = CopyDirection::kAuto;
    if (src_device && dst_device) {
        direction = CopyDirection::kDeviceToDevice;
    } else if (src_device) {
        direction = CopyDirection::kDeviceToHost;
    } else if (dst_device) {
        direction = CopyDirection::kHostToDevice;
    }
    return accelerator->Copy(dst, src, size, direction);
}

}  // namespace device
}  // namespace mooncake
