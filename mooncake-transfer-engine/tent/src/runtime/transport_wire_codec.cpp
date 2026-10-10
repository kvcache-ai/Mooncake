// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport_wire_codec.h"

#include <glog/logging.h>

#include <algorithm>
#include <mutex>
#include <unordered_set>

namespace mooncake {
namespace tent {

namespace {

bool isPluginSlotValue(int type) {
    return type >= kFirstPluginSlot && type < kNumTransportTypes;
}

bool isBuiltinValue(int type) {
    return type >= 0 && type < kNumBuiltinTransportTypes;
}

bool usesPluginSlots(const MemorySegmentDesc& memory) {
    for (const auto& buffer : memory.buffers) {
        for (auto type : buffer.transports)
            if (isPluginSlotValue(type)) return true;
        for (const auto& entry : buffer.transport_attrs)
            if (isPluginSlotValue(entry.first)) return true;
    }
    for (const auto& entry : memory.transport_attrs)
        if (isPluginSlotValue(entry.first)) return true;
    for (const auto& device : memory.devices)
        for (const auto& entry : device.transport_attrs)
            if (isPluginSlotValue(entry.first)) return true;
    return false;
}

void encodeBuffer(BufferDesc& buffer) {
    std::vector<TransportType> builtin;
    for (auto type : buffer.transports) {
        if (isPluginSlotValue(type))
            buffer.plugin_transports.emplace_back(transportTypeName(type));
        else
            builtin.push_back(type);
    }
    buffer.transports.swap(builtin);
    for (auto it = buffer.transport_attrs.begin();
         it != buffer.transport_attrs.end();) {
        if (isPluginSlotValue(it->first)) {
            buffer.plugin_transport_attrs[transportTypeName(it->first)] =
                it->second;
            it = buffer.transport_attrs.erase(it);
        } else {
            ++it;
        }
    }
}

TransportType lookupPlugin(const std::string& name,
                           const std::string& segment) {
    auto type = pluginTransportSlotByName(name);
    if (type != UNSPEC) return type;
    // Once per name; the set is capped because the names come from peers.
    static std::mutex mutex;
    static auto* reported = new std::unordered_set<std::string>();
    bool first = false;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (reported->size() < 64) first = reported->insert(name).second;
    }
    if (first) {
        LOG(INFO) << "[TransportPlugin] segment " << segment
                  << " advertises transport plugin '" << name
                  << "', which is not loaded here; ignoring it";
    }
    return UNSPEC;
}

void decodeBuffer(BufferDesc& buffer, const std::string& segment) {
    buffer.transports.erase(
        std::remove_if(
            buffer.transports.begin(), buffer.transports.end(),
            [](TransportType type) { return !isBuiltinValue(type); }),
        buffer.transports.end());
    for (auto it = buffer.transport_attrs.begin();
         it != buffer.transport_attrs.end();) {
        if (isBuiltinValue(it->first))
            ++it;
        else
            it = buffer.transport_attrs.erase(it);
    }
    for (const auto& name : buffer.plugin_transports) {
        auto type = lookupPlugin(name, segment);
        if (type == UNSPEC) continue;
        if (std::find(buffer.transports.begin(), buffer.transports.end(),
                      type) == buffer.transports.end())
            buffer.transports.push_back(type);
    }
    for (const auto& [name, attrs] : buffer.plugin_transport_attrs) {
        auto type = lookupPlugin(name, segment);
        if (type != UNSPEC) buffer.transport_attrs[type] = attrs;
    }
    buffer.plugin_transports.clear();
    buffer.plugin_transport_attrs.clear();
}

}  // namespace

json segmentDescToWireJson(const SegmentDesc& desc) {
    if (desc.type != SegmentType::Memory || !usesPluginSlots(desc.getMemory()))
        return desc;
    SegmentDesc copy = desc;
    auto& memory = std::get<MemorySegmentDesc>(copy.detail);
    for (auto& buffer : memory.buffers) encodeBuffer(buffer);
    for (auto it = memory.transport_attrs.begin();
         it != memory.transport_attrs.end();) {
        if (isPluginSlotValue(it->first)) {
            memory.plugin_transport_attrs[transportTypeName(
                static_cast<TransportType>(it->first))] = it->second;
            it = memory.transport_attrs.erase(it);
        } else {
            ++it;
        }
    }
    for (auto& device : memory.devices) {
        for (auto it = device.transport_attrs.begin();
             it != device.transport_attrs.end();) {
            if (isPluginSlotValue(it->first)) {
                LOG_FIRST_N(WARNING, 1)
                    << "[TransportPlugin] per-device attributes of plugin "
                       "transport '"
                    << transportTypeName(it->first)
                    << "' are not published to peers";
                it = device.transport_attrs.erase(it);
            } else {
                ++it;
            }
        }
    }
    return copy;
}

void decodeSegmentDescFromWire(SegmentDesc& desc) {
    if (desc.type != SegmentType::Memory) return;
    auto& memory = std::get<MemorySegmentDesc>(desc.detail);
    for (auto& buffer : memory.buffers) decodeBuffer(buffer, desc.name);
    for (auto it = memory.transport_attrs.begin();
         it != memory.transport_attrs.end();) {
        if (isBuiltinValue(it->first))
            ++it;
        else
            it = memory.transport_attrs.erase(it);
    }
    for (const auto& [name, attrs] : memory.plugin_transport_attrs) {
        auto type = lookupPlugin(name, desc.name);
        if (type != UNSPEC) memory.transport_attrs[type] = attrs;
    }
    memory.plugin_transport_attrs.clear();
    for (auto& device : memory.devices) {
        for (auto it = device.transport_attrs.begin();
             it != device.transport_attrs.end();) {
            if (isBuiltinValue(it->first))
                ++it;
            else
                it = device.transport_attrs.erase(it);
        }
    }
}

}  // namespace tent
}  // namespace mooncake
