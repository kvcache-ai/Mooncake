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

#include "v1/runtime/segment_registry.h"

#include <jsoncpp/json/value.h>

#include <cassert>
#include <set>

#include "v1/common/status.h"
#include "v1/platform/system.h"
#include "v1/runtime/control_plane.h"

namespace mooncake {
namespace v1 {

static inline std::string getFullMetadataKey(const std::string &segment_name) {
    const static std::string kCommonKeyPrefix = "mooncake/v1/";
    return kCommonKeyPrefix + segment_name;
}

static inline std::string getItem(const Json::Value &parent,
                                  const std::string &key) {
    const static Json::Value kEmpty;
    auto data = parent.get(key, kEmpty);
    return (data == kEmpty) ? "" : data.asString();
}

static inline uint64_t getItemUInt64(const Json::Value &parent,
                                     const std::string &key) {
    const static Json::Value kEmpty;
    auto data = parent.get(key, kEmpty);
    return (data == kEmpty) ? 0 : data.asUInt64();
}

static Json::Value exportSegmentDesc(const SegmentDesc &desc) {
    try {
        Json::Value segmentJSON;
        if (desc.name.empty()) return Json::Value();
        segmentJSON["name"] = desc.name;
        segmentJSON["machine_id"] = desc.machine_id;
        if (desc.type == SegmentType::Memory) {
            segmentJSON["type"] = "memory";
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
                if (!buffer.shm_path.empty())
                    bufferJSON["shm_path"] = buffer.shm_path;
                if (!buffer.mnnvl_handle.empty())
                    bufferJSON["mnnvl_handle"] = buffer.mnnvl_handle;
                Json::Value transports(Json::arrayValue);
                for (auto t : buffer.transports) transports.append((int)t);
                bufferJSON["transports"] = transports;
                buffersJSON.append(bufferJSON);
            }
            if (!detail.buffers.empty()) segmentJSON["buffers"] = buffersJSON;
            segmentJSON["topology"] = detail.topology.toString();
            segmentJSON["rpc_server_addr"] = detail.rpc_server_addr;
        } else if (desc.type == SegmentType::File) {
            segmentJSON["type"] = "file";
            Json::Value buffersJSON(Json::arrayValue);
            auto &detail = std::get<FileSegmentDesc>(desc.detail);
            for (const auto &buffer : detail.buffers) {
                Json::Value bufferJSON;
                bufferJSON["path"] = buffer.path;
                bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
                bufferJSON["offset"] = static_cast<Json::UInt64>(buffer.offset);
                buffersJSON.append(bufferJSON);
            }
            if (!detail.buffers.empty()) segmentJSON["buffers"] = buffersJSON;
        }
        return segmentJSON;
    } catch (...) {
        return Json::Value();
    }
}

static std::shared_ptr<SegmentDesc> importSegmentDesc(
    const Json::Value &segmentJSON) {
    try {
        auto desc = std::make_shared<SegmentDesc>();
        desc->name = getItem(segmentJSON, "name");
        auto type_str = getItem(segmentJSON, "type");
        desc->machine_id = getItem(segmentJSON, "machine_id");
        if (desc->name.empty() || type_str.empty()) return nullptr;
        if (type_str == "memory") {
            desc->type = SegmentType::Memory;
            desc->detail = MemorySegmentDesc{};
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
                    buffer.shm_path = getItem(bufferJSON, "shm_path");
                    buffer.mnnvl_handle = getItem(bufferJSON, "mnnvl_handle");
                    if (buffer.location.empty() || !buffer.addr ||
                        !buffer.length ||
                        detail.devices.size() != buffer.rkey.size()) {
                        return nullptr;
                    }
                    for (const auto &typeJSON : bufferJSON["transports"]) {
                        buffer.transports.push_back(
                            (TransportType)typeJSON.asInt());
                    }
                    detail.buffers.push_back(buffer);
                }

            auto topo_string = getItem(segmentJSON, "topology");
            if (!topo_string.empty()) detail.topology.parse(topo_string);
            detail.rpc_server_addr = getItem(segmentJSON, "rpc_server_addr");
        } else if (type_str == "file") {
            desc->type = SegmentType::File;
            desc->detail = FileSegmentDesc{};
            auto &detail = std::get<FileSegmentDesc>(desc->detail);
            if (segmentJSON.isMember("buffers")) {
                for (const auto &bufferJSON : segmentJSON["buffers"]) {
                    FileBufferDesc buffer;
                    buffer.path = getItem(bufferJSON, "path");
                    buffer.length = getItemUInt64(bufferJSON, "length");
                    buffer.offset = getItemUInt64(bufferJSON, "offset");
                    detail.buffers.push_back(buffer);
                }
            }
        }
        return desc;
    } catch (std::exception &ex) {
        LOG(ERROR) << "importSegmentDesc: Found internal exception: "
                   << ex.what();
        return nullptr;
    }
}

CentralSegmentRegistry::CentralSegmentRegistry(const std::string &type,
                                               const std::string &servers) {
    plugin_ = MetaStore::Create(type, servers);
}

Status CentralSegmentRegistry::getSegmentDesc(SegmentDescRef &desc,
                                              const std::string &segment_name) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    std::string jstr;
    desc = nullptr;

    auto status = plugin_->get(getFullMetadataKey(segment_name), jstr);
    if (!status.ok()) return status;

    Json::Value j;
    if (Json::Reader{}.parse(jstr, j)) {
        desc = importSegmentDesc(j);
        desc->name = segment_name;
    }

    if (!desc)
        return Status::MalformedJson(
            "Failed to parse segment from json" LOC_MARK);
    return Status::OK();
}

Status CentralSegmentRegistry::putSegmentDesc(SegmentDescRef &desc) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    auto j = exportSegmentDesc(*desc);
    if (j.empty())
        return Status::MalformedJson(
            "Failed to serialize segment descriptor" LOC_MARK);
    const std::string jstr = Json::FastWriter{}.write(j);
    return plugin_->set(getFullMetadataKey(desc->name), jstr);
}

Status CentralSegmentRegistry::deleteSegmentDesc(
    const std::string &segment_name) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    return plugin_->remove(getFullMetadataKey(segment_name));
}

Status PeerSegmentRegistry::getSegmentDesc(SegmentDescRef &desc,
                                           const std::string &segment_name) {
    std::string response;
    if (segment_name.empty())
        return Status::InvalidArgument("Empty segment name" LOC_MARK);
    CHECK_STATUS(ControlClient::getSegmentDesc(segment_name, response));
    if (response.empty()) {
        return Status::InvalidEntry(std::string("Segment ") + segment_name +
                                    "not found" + LOC_MARK);
    }

    Json::Value j;
    if (Json::Reader{}.parse(response, j)) {
        desc = importSegmentDesc(j);
        if (!desc)
            return Status::MalformedJson(
                "Failed to import segment from json" LOC_MARK);
        desc->name = segment_name;
    }

    if (!desc)
        return Status::MalformedJson(
            "Failed to import segment from json" LOC_MARK);
    return Status::OK();
}

}  // namespace v1
}  // namespace mooncake
