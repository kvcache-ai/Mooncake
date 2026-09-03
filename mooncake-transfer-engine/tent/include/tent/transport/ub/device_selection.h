// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_DEVICE_SELECTION_H_
#define TENT_TRANSPORT_UB_DEVICE_SELECTION_H_

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include <vector>

#include "tent/transport/ub/urma_adapter.h"

namespace mooncake {
namespace tent {
namespace ub {

// Heuristic used when device_filter is empty: prefer UBAGG bonding devices
// over underlying physical ports (e.g. udmac*) that appear alongside them.
inline bool isBondingDeviceName(std::string_view name) {
    std::string lower(name);
    std::transform(
        lower.begin(), lower.end(), lower.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (lower.rfind("bonding", 0) == 0) return true;
    if (lower.find(":bonding") != std::string::npos) return true;
    if (lower.find("_bond") != std::string::npos) return true;
    if (lower.find("-bond") != std::string::npos) return true;
    return false;
}

inline bool isBondingDevice(const DeviceInfo& device) {
    return isBondingDeviceName(device.native_device_name) ||
           isBondingDeviceName(device.topology_name);
}

// When explicit_filter is true, returns devices unchanged (caller already
// applied device_filter). When false and at least one bonding device is
// present, returns only bonding devices; otherwise returns all devices.
inline std::vector<DeviceInfo> preferBondingDevicesIfPresent(
    const std::vector<DeviceInfo>& devices, bool explicit_filter) {
    if (explicit_filter || devices.empty()) return devices;
    const bool has_bonding =
        std::any_of(devices.begin(), devices.end(),
                    [](const DeviceInfo& d) { return isBondingDevice(d); });
    if (!has_bonding) return devices;

    std::vector<DeviceInfo> selected;
    selected.reserve(devices.size());
    for (const auto& device : devices) {
        if (isBondingDevice(device)) selected.push_back(device);
    }
    return selected;
}

}  // namespace ub
}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_UB_DEVICE_SELECTION_H_
