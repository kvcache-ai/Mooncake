// Copyright 2024 KVCache.AI
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

#include <jsoncpp/json/value.h>

#include <cassert>
#include <set>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transfer_metadata.h"
#include "transfer_metadata_plugin.h"

namespace mooncake {
static inline std::string getItem(const Json::Value &parent,
                                  const std::string &key) {
    const static Json::Value kEmpty;
    auto data = parent.get(key, kEmpty);
    return (data == kEmpty) ? "" : data.asString();
}

static inline std::string getStyledJsonString(const Json::Value &parent,
                                              const std::string &key) {
    const static Json::Value kEmpty;
    auto data = parent.get(key, kEmpty);
    return (data == kEmpty) ? "" : data.toStyledString();
}

static inline uint64_t getItemUInt64(const Json::Value &parent,
                                     const std::string &key) {
    const static Json::Value kEmpty;
    auto data = parent.get(key, kEmpty);
    return (data == kEmpty) ? 0 : data.asUInt64();
}

std::shared_ptr<SegmentDesc> TransferMetadata::praseSegmentDesc(
    const Json::Value &segmentJSON) {
    try {
        auto desc = std::make_shared<SegmentDesc>();
        desc->name = getItem(segmentJSON, "name");
        desc->protocol = getItem(segmentJSON, "protocol");
        if (desc->name.empty() || desc->protocol.empty()) return nullptr;

        if (desc->protocol == "rdma" || desc->protocol == "shm") {
            auto &detail = std::get<MemorySegmentDesc>(desc->detail);
            if (segmentJSON.isMember("devices"))
                for (const auto &deviceJSON : segmentJSON["devices"]) {
                    DeviceDesc device;
                    device.name = getItem(deviceJSON, "name");
                    device.lid = getItemUInt64(deviceJSON, "lid");
                    device.gid = getItem(deviceJSON, "gid");
                    if (device.name.empty() || device.gid.empty()) {
                        return nullptr;
                    }
                    detail.devices.push_back(device);
                }

            if (segmentJSON.isMember("buffers"))
                for (const auto &bufferJSON : segmentJSON["buffers"]) {
                    BufferDesc buffer;
                    buffer.location = getItem(bufferJSON, "name");
                    buffer.addr = getItemUInt64(bufferJSON, "addr");
                    buffer.length = getItemUInt64(bufferJSON, "length");
                    for (const auto &rkeyJSON : bufferJSON["rkey"])
                        buffer.rkey.push_back(rkeyJSON.asUInt());
                    for (const auto &lkeyJSON : bufferJSON["lkey"])
                        buffer.lkey.push_back(lkeyJSON.asUInt());
                    buffer.shm_path = getItem(bufferJSON, "shm_path");
                    if (buffer.location.empty() || !buffer.addr ||
                        !buffer.length ||
                        detail.devices.size() != buffer.rkey.size() ||
                        buffer.rkey.size() != buffer.lkey.size()) {
                        return nullptr;
                    }
                    detail.buffers.push_back(buffer);
                }

            auto topo_string =
                getStyledJsonString(segmentJSON, "priority_matrix");
            if (!topo_string.empty()) detail.topology.parse(topo_string);
        } else if (desc->protocol == "tcp") {
            auto &detail = std::get<MemorySegmentDesc>(desc->detail);
            if (segmentJSON.isMember("buffers"))
                for (const auto &bufferJSON : segmentJSON["buffers"]) {
                    BufferDesc buffer;
                    buffer.location = getItem(bufferJSON, "name");
                    buffer.addr = getItemUInt64(bufferJSON, "addr");
                    buffer.length = getItemUInt64(bufferJSON, "length");
                    if (buffer.location.empty() || !buffer.addr ||
                        !buffer.length) {
                        return nullptr;
                    }
                    detail.buffers.push_back(buffer);
                }
        } else if (desc->protocol == "nvmeof") {
            auto &detail = std::get<FileSegmentDesc>(desc->detail);
            if (segmentJSON.isMember("buffers"))
                for (const auto &bufferJSON : segmentJSON["buffers"]) {
                    FileBufferDesc buffer;
                    buffer.original_path = getItem(bufferJSON, "file_path");
                    buffer.length = getItemUInt64(bufferJSON, "length");
                    const Json::Value &mounted_path_map =
                        bufferJSON["local_path_map"];
                    for (const auto &key : mounted_path_map.getMemberNames()) {
                        buffer.mounted_path_map[key] =
                            mounted_path_map[key].asString();
                    }
                    if (buffer.original_path.empty()) return nullptr;
                    detail.buffers.push_back(buffer);
                }
        } else {
            return nullptr;
        }
        return desc;
    } catch (...) {
        return nullptr;
    }
}

Json::Value TransferMetadata::exportSegmentDesc(const SegmentDesc &desc) {
    try {
        Json::Value segmentJSON;
        if (desc.name.empty() || desc.protocol.empty()) return Json::Value();
        segmentJSON["name"] = desc.name;
        segmentJSON["protocol"] = desc.protocol;
        if (desc.protocol == "rdma" || desc.protocol == "shm") {
            auto &detail = std::get<MemorySegmentDesc>(desc.detail);
            Json::Value devicesJSON(Json::arrayValue);
            for (const auto &device : detail.devices) {
                Json::Value deviceJSON;
                if (device.name.empty() || device.gid.empty())
                    return Json::Value();
                deviceJSON["name"] = device.name;
                deviceJSON["lid"] = device.lid;
                deviceJSON["gid"] = device.gid;
                devicesJSON.append(deviceJSON);
            }
            if (!detail.devices.empty()) segmentJSON["devices"] = devicesJSON;
            Json::Value buffersJSON(Json::arrayValue);
            for (const auto &buffer : detail.buffers) {
                Json::Value bufferJSON;
                if (buffer.location.empty()) return Json::Value();
                bufferJSON["name"] = buffer.location;
                bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
                bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
                Json::Value rkeyJSON(Json::arrayValue);
                for (auto &entry : buffer.rkey) rkeyJSON.append(entry);
                if (!buffer.rkey.empty()) bufferJSON["rkey"] = rkeyJSON;
                Json::Value lkeyJSON(Json::arrayValue);
                for (auto &entry : buffer.lkey) lkeyJSON.append(entry);
                if (!buffer.lkey.empty()) bufferJSON["lkey"] = lkeyJSON;
                if (!buffer.shm_path.empty())
                    bufferJSON["shm_path"] = buffer.shm_path;
                buffersJSON.append(bufferJSON);
            }
            if (!detail.buffers.empty()) segmentJSON["buffers"] = buffersJSON;
            segmentJSON["priority_matrix"] = detail.topology.toJson();
        } else if (desc.protocol == "tcp") {
            Json::Value buffersJSON(Json::arrayValue);
            auto &detail = std::get<MemorySegmentDesc>(desc.detail);
            for (const auto &buffer : detail.buffers) {
                Json::Value bufferJSON;
                if (buffer.location.empty()) return Json::Value();
                bufferJSON["name"] = buffer.location;
                bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
                bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
                buffersJSON.append(bufferJSON);
            }
            if (!detail.buffers.empty()) segmentJSON["buffers"] = buffersJSON;
        } else if (desc.protocol == "nvmeof") {
            Json::Value buffersJSON(Json::arrayValue);
            auto &detail = std::get<FileSegmentDesc>(desc.detail);
            for (const auto &buffer : detail.buffers) {
                Json::Value bufferJSON;
                bufferJSON["file_path"] = buffer.original_path;
                bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
                Json::Value localPathMapJSON;
                for (const auto &entry : buffer.mounted_path_map) {
                    localPathMapJSON[entry.first] = entry.second;
                }
                if (!buffer.mounted_path_map.empty())
                    bufferJSON["local_path_map"] = localPathMapJSON;
                buffersJSON.append(bufferJSON);
            }
            if (!detail.buffers.empty()) segmentJSON["buffers"] = buffersJSON;
        }
        return segmentJSON;
    } catch (...) {
        return Json::Value();
    }
}
}  // namespace mooncake