// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#ifndef TENT_TRANSPORT_UB_TOPOLOGY_ATTRS_H_
#define TENT_TRANSPORT_UB_TOPOLOGY_ATTRS_H_

#include <string>
#include <string_view>
#include <unordered_map>

#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {

inline constexpr std::string_view kTopologyNativeNameAttr = "ub.native_name";
inline constexpr std::string_view kTopologyDeviceIndexAttr = "ub.device_index";
inline constexpr std::string_view kTopologyEidIndexAttr = "ub.eid_index";
inline constexpr std::string_view kTopologyEidAttr = "ub.eid";
inline constexpr std::string_view kTopologyDiscoveryActiveAttr =
    "ub.discovery_active";

inline void encodeTopologyDeviceAttributes(
    const DeviceInfo& device, int device_index,
    std::unordered_map<std::string, std::string>& attributes) {
    attributes[std::string(kTopologyNativeNameAttr)] =
        device.native_device_name;
    attributes[std::string(kTopologyDeviceIndexAttr)] =
        std::to_string(device_index);
    attributes[std::string(kTopologyEidIndexAttr)] =
        std::to_string(device.eid_index);
    attributes[std::string(kTopologyEidAttr)] = device.eid;
    attributes[std::string(kTopologyDiscoveryActiveAttr)] =
        device.active ? "true" : "false";
}

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_TOPOLOGY_ATTRS_H_
