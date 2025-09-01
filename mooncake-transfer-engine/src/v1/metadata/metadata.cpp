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

#include "v1/metadata/metadata.h"

#include <jsoncpp/json/value.h>

#include <cassert>
#include <set>

#include "v1/common/status.h"
#include "v1/utility/system.h"

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
                buffersJSON.append(bufferJSON);
            }
            if (!detail.buffers.empty()) segmentJSON["buffers"] = buffersJSON;
            segmentJSON["topology"] = detail.topology.toJson();
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
                    detail.buffers.push_back(buffer);
                }

            auto topo_string = getStyledJsonString(segmentJSON, "topology");
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

CentralMetadataStore::CentralMetadataStore(const std::string &type,
                                           const std::string &servers) {
    plugin_ = MetadataPlugin::Create(type, servers);
}

Status CentralMetadataStore::getSegmentDesc(SegmentDescRef &desc,
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

Status CentralMetadataStore::putSegmentDesc(SegmentDescRef &desc) {
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

Status CentralMetadataStore::deleteSegmentDesc(
    const std::string &segment_name) {
    if (!plugin_)
        return Status::MetadataError(
            "Central metadata store not started" LOC_MARK);
    return plugin_->remove(getFullMetadataKey(segment_name));
}

thread_local CoroRpcAgent tl_rpc_agent;

Status P2PMetadataStore::getSegmentDesc(SegmentDescRef &desc,
                                        const std::string &segment_name) {
    std::string request, response;
    auto status =
        tl_rpc_agent.call(segment_name, GetSegmentDesc, request, response);
    if (!status.ok()) return status;
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

static Json::Value exportBootstrapDesc(const BootstrapDesc &desc) {
    try {
        Json::Value root;
        root["local_nic_path"] = desc.local_nic_path;
        root["peer_nic_path"] = desc.peer_nic_path;
        Json::Value qpNums(Json::arrayValue);
        for (const auto &qp : desc.qp_num) qpNums.append(qp);
        root["qp_num"] = qpNums;
        root["reply_msg"] = desc.reply_msg;
        return root;
    } catch (...) {
        return Json::Value();
    }
}

static Status importBootstrapDesc(const Json::Value &root,
                                  BootstrapDesc &desc) {
    try {
        Json::Reader reader;
        desc.local_nic_path = root["local_nic_path"].asString();
        desc.peer_nic_path = root["peer_nic_path"].asString();
        for (const auto &qp : root["qp_num"])
            desc.qp_num.push_back(qp.asUInt());
        desc.reply_msg = root["reply_msg"].asString();
        return Status::OK();
    } catch (std::exception &ex) {
        return Status::MalformedJson(
            "Failed to import handshake message from json" LOC_MARK);
    }
}

Status serializeBootstrapDesc(const BootstrapDesc &desc, std::string &stream) {
    auto json = exportBootstrapDesc(desc);
    stream = Json::FastWriter{}.write(json);
    return Status::OK();
}

Status deserializeBootstrapDesc(BootstrapDesc &desc,
                                const std::string &stream) {
    if (stream.empty())
        return Status::MalformedJson(
            "Failed to import handshake message from json" LOC_MARK);
    Json::Value peer_json;
    if (Json::Reader{}.parse(stream, peer_json))
        return importBootstrapDesc(peer_json, desc);
    return Status::MalformedJson(
        "Failed to import handshake message from json" LOC_MARK);
}

Status RpcClient::bootstrap(const std::string &server_addr,
                            const BootstrapDesc &request,
                            BootstrapDesc &response) {
    std::string request_raw, response_raw;
    auto status = serializeBootstrapDesc(request, request_raw);
    if (!status.ok()) return status;
    status = tl_rpc_agent.call(server_addr, BootstrapRdma, request_raw,
                               response_raw);
    if (!status.ok()) return status;
    return deserializeBootstrapDesc(response, response_raw);
}

Status RpcClient::sendData(const std::string &server_addr,
                           uint64_t peer_mem_addr, void *local_mem_addr,
                           size_t length) {
    std::string request, response;
    XferDataDesc desc{htole64(peer_mem_addr), htole64(length)};
    request.resize(sizeof(XferDataDesc) + length);
    memcpy(&request[0], &desc, sizeof(desc));
    genericMemcpy(&request[sizeof(desc)], local_mem_addr, length);
    return tl_rpc_agent.call(server_addr, SendData, request, response);
}

Status RpcClient::recvData(const std::string &server_addr,
                           uint64_t peer_mem_addr, void *local_mem_addr,
                           size_t length) {
    std::string request, response;
    XferDataDesc desc{htole64(peer_mem_addr), htole64(length)};
    request.resize(sizeof(XferDataDesc) + length);
    memcpy(&request[0], &desc, sizeof(desc));
    auto status = tl_rpc_agent.call(server_addr, RecvData, request, response);
    if (!status.ok()) return status;
    genericMemcpy(local_mem_addr, response.data(), length);
    return Status::OK();
}

Status RpcClient::notify(const std::string &server_addr,
                         const Notification &message) {
    std::string request, response;
    request.resize(message.size());
    memcpy(&request[0], message.c_str(), message.size());
    return tl_rpc_agent.call(server_addr, Notify, request, response);
}

MetadataService::MetadataService(const std::string &type,
                                 const std::string &servers)
    : bootstrap_callback_(nullptr), notify_callback_(nullptr) {
    if (type == "p2p") {
        auto agent = std::make_unique<P2PMetadataStore>();
        manager_ = std::make_unique<SegmentManager>(std::move(agent));
    } else {
        auto agent = std::make_unique<CentralMetadataStore>(type, servers);
        manager_ = std::make_unique<SegmentManager>(std::move(agent));
    }
    rpc_server_ = std::make_shared<CoroRpcAgent>();
    rpc_server_->registerFunction(
        GetSegmentDesc,
        [this](const std::string_view &request, std::string &response) {
            onGetSegmentDesc(request, response);
        });
    rpc_server_->registerFunction(
        BootstrapRdma,
        [this](const std::string_view &request, std::string &response) {
            onBootstrapRdma(request, response);
        });
    rpc_server_->registerFunction(
        SendData,
        [this](const std::string_view &request, std::string &response) {
            onSendData(request, response);
        });
    rpc_server_->registerFunction(
        RecvData,
        [this](const std::string_view &request, std::string &response) {
            onRecvData(request, response);
        });
    rpc_server_->registerFunction(
        Notify, [this](const std::string_view &request, std::string &response) {
            onNotify(request, response);
        });
}

MetadataService::~MetadataService() {}

Status MetadataService::start(uint16_t &port, bool ipv6_) {
    return rpc_server_->start(port, ipv6_);
}

void MetadataService::onGetSegmentDesc(const std::string_view &request,
                                       std::string &response) {
    auto local_json = exportSegmentDesc(*manager_->getLocal());
    response = Json::FastWriter{}.write(local_json);
}

void MetadataService::onBootstrapRdma(const std::string_view &request,
                                      std::string &response) {
    BootstrapDesc request_desc, response_desc;
    std::string mutable_request(request);
    auto status = deserializeBootstrapDesc(request_desc, mutable_request);
    assert(status.ok());
    if (bootstrap_callback_) bootstrap_callback_(request_desc, response_desc);
    serializeBootstrapDesc(response_desc, response);
}

void MetadataService::onSendData(const std::string_view &request,
                                 std::string &response) {
    XferDataDesc *desc = (XferDataDesc *)request.data();
    auto local_desc = manager_->getLocal().get();
    auto peer_mem_addr = le64toh(desc->peer_mem_addr);
    auto length = le64toh(desc->length);
    if (getBufferDesc(local_desc, peer_mem_addr, length)) {
        genericMemcpy((void *)peer_mem_addr, &desc[1], length);
    }
}

void MetadataService::onRecvData(const std::string_view &request,
                                 std::string &response) {
    XferDataDesc *desc = (XferDataDesc *)request.data();
    auto local_desc = manager_->getLocal().get();
    auto peer_mem_addr = le64toh(desc->peer_mem_addr);
    auto length = le64toh(desc->length);
    response.resize(length);
    if (getBufferDesc(local_desc, peer_mem_addr, length)) {
        genericMemcpy(response.data(), (void *)peer_mem_addr, length);
    }
}

void MetadataService::onNotify(const std::string_view &request,
                               std::string &response) {
    std::string message(request.data(), request.size());
    if (notify_callback_) notify_callback_(message);
}

}  // namespace v1
}  // namespace mooncake
