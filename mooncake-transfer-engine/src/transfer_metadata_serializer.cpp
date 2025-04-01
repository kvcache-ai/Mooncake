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
std::shared_ptr<SegmentDesc>
TransferMetadata::praseSegmentDesc(const Json::Value &segmentJSON) {
    auto desc = std::make_shared<SegmentDesc>();
    desc->name = segmentJSON["name"].asString();
    desc->protocol = segmentJSON["protocol"].asString();

    if (desc->protocol == "rdma") {
        auto &detail = std::get<MemorySegmentDesc>(desc->detail);
        for (const auto &deviceJSON : segmentJSON["devices"]) {
            DeviceDesc device;
            device.name = deviceJSON["name"].asString();
            device.lid = deviceJSON["lid"].asUInt();
            device.gid = deviceJSON["gid"].asString();
            if (device.name.empty() || device.gid.empty()) {
                return nullptr;
            }
            detail.devices.push_back(device);
        }
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
            for (const auto &rkeyJSON : bufferJSON["rkey"])
                buffer.rkey.push_back(rkeyJSON.asUInt());
            for (const auto &lkeyJSON : bufferJSON["lkey"])
                buffer.lkey.push_back(lkeyJSON.asUInt());
            if (buffer.name.empty() || !buffer.addr || !buffer.length ||
                buffer.rkey.empty() ||
                buffer.rkey.size() != buffer.lkey.size()) {
                return nullptr;
            }
            detail.buffers.push_back(buffer);
        }

        int ret = detail.topology.parse(
            segmentJSON["priority_matrix"].toStyledString());
    } else if (desc->protocol == "tcp") {
        auto &detail = std::get<MemorySegmentDesc>(desc->detail);
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
            if (buffer.name.empty() || !buffer.addr || !buffer.length) {
                return nullptr;
            }
            detail.buffers.push_back(buffer);
        }
    } else if (desc->protocol == "nvmeof") {
        auto &detail = std::get<FileSegmentDesc>(desc->detail);
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            FileBufferDesc buffer;
            buffer.original_path = bufferJSON["file_path"].asString();
            buffer.length = bufferJSON["length"].asUInt64();
            const Json::Value &mounted_path_map = bufferJSON["local_path_map"];
            for (const auto &key : mounted_path_map.getMemberNames()) {
                buffer.mounted_path_map[key] = mounted_path_map[key].asString();
            }
            detail.buffers.push_back(buffer);
        }
    } else {
        return nullptr;
    }

    return desc;
}

Json::Value TransferMetadata::exportSegmentDesc(const SegmentDesc &desc) {
    Json::Value segmentJSON;
    segmentJSON["name"] = desc.name;
    segmentJSON["protocol"] = desc.protocol;

    if (segmentJSON["protocol"] == "rdma") {
        auto &detail = std::get<MemorySegmentDesc>(desc.detail);
        Json::Value devicesJSON(Json::arrayValue);
        for (const auto &device : detail.devices) {
            Json::Value deviceJSON;
            deviceJSON["name"] = device.name;
            deviceJSON["lid"] = device.lid;
            deviceJSON["gid"] = device.gid;
            devicesJSON.append(deviceJSON);
        }
        segmentJSON["devices"] = devicesJSON;
        Json::Value buffersJSON(Json::arrayValue);
        for (const auto &buffer : detail.buffers) {
            Json::Value bufferJSON;
            bufferJSON["name"] = buffer.name;
            bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
            bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
            Json::Value rkeyJSON(Json::arrayValue);
            for (auto &entry : buffer.rkey) rkeyJSON.append(entry);
            bufferJSON["rkey"] = rkeyJSON;
            Json::Value lkeyJSON(Json::arrayValue);
            for (auto &entry : buffer.lkey) lkeyJSON.append(entry);
            bufferJSON["lkey"] = lkeyJSON;
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;
        segmentJSON["priority_matrix"] = detail.topology.toJson();
    } else if (segmentJSON["protocol"] == "tcp") {
        Json::Value buffersJSON(Json::arrayValue);
        auto &detail = std::get<MemorySegmentDesc>(desc.detail);
        for (const auto &buffer : detail.buffers) {
            Json::Value bufferJSON;
            bufferJSON["name"] = buffer.name;
            bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
            bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;
    } else if (segmentJSON["protocol"] == "nvmeof") {
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
            bufferJSON["local_path_map"] = localPathMapJSON;
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;
    } 
    return segmentJSON;
}
}  // namespace mooncake