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

#include "metadata/metadata_v1.h"

#include <json/value.h>

#include <cassert>
#include <set>

#include "common/common.h"
#include "common/config.h"
#include "common/error.h"
#include "metadata/handshake.h"

namespace mooncake {
namespace v1 {
const static std::string kCommonKeyPrefix = "mooncake/";
const static std::string kRpcMetaPrefix = kCommonKeyPrefix + "rpc_meta/";

// mooncake/segments/[...]
static inline std::string getFullMetadataKey(const std::string &segment_name) {
    auto pos = segment_name.find("/");
    if (pos == segment_name.npos)
        return kCommonKeyPrefix + "ram/" + segment_name;
    else
        return kCommonKeyPrefix + segment_name;
}

struct TransferHandshakeUtil {
    static Json::Value encode(const HandShakeDesc &desc) {
        Json::Value root;
        root["local_nic_path"] = desc.local_nic_path;
        root["peer_nic_path"] = desc.peer_nic_path;
        Json::Value qpNums(Json::arrayValue);
        for (const auto &qp : desc.qp_num) qpNums.append(qp);
        root["qp_num"] = qpNums;
        root["reply_msg"] = desc.reply_msg;
        return root;
    }

    static int decode(Json::Value root, HandShakeDesc &desc) {
        Json::Reader reader;
        desc.local_nic_path = root["local_nic_path"].asString();
        desc.peer_nic_path = root["peer_nic_path"].asString();
        for (const auto &qp : root["qp_num"])
            desc.qp_num.push_back(qp.asUInt());
        desc.reply_msg = root["reply_msg"].asString();
        return 0;
    }
};

TransferMetadata::TransferMetadata(const std::string &conn_string) {
    next_segment_id_.store(1);
    handshake_plugin_ = HandShakePlugin::Create(conn_string);
    if (!handshake_plugin_) {
        LOG(ERROR)
            << "Unable to create metadata handshake plugin with conn string: "
            << conn_string;
    }
    if (conn_string == P2PHANDSHAKE) {
        p2p_handshake_mode_ = true;
        return;
    }
    storage_plugin_ = MetadataPlugin::Create(conn_string);
    if (!storage_plugin_) {
        LOG(ERROR)
            << "Unable to create metadata storage plugin with conn string "
            << conn_string;
    }
}

TransferMetadata::~TransferMetadata() { handshake_plugin_.reset(); }

int TransferMetadata::updateSegmentDesc(const std::string &segment_name,
                                        const SegmentDesc &desc) {
    if (p2p_handshake_mode_) return 0;
    auto segmentJSON = exportSegmentDesc(desc);
    if (segmentJSON.empty()) {
        LOG(ERROR) << "export segment descriptor failed, name " << desc.name
                   << " protocol " << desc.protocol;
        return ERR_METADATA;
    }

    const std::string value = Json::FastWriter{}.write(segmentJSON);
    auto status = storage_plugin_->set(getFullMetadataKey(segment_name), value);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to register segment descriptor, name "
                   << desc.name << " protocol " << desc.protocol << " detail "
                   << status.ToString();
        return ERR_METADATA;
    }

    return 0;
}

int TransferMetadata::removeSegmentDesc(const std::string &segment_name) {
    if (p2p_handshake_mode_) return 0;
    auto status = storage_plugin_->remove(getFullMetadataKey(segment_name));
    if (!status.ok()) {
        LOG(ERROR) << "Failed to unregister segment descriptor, name "
                   << segment_name << " detail " << status.ToString();
        return ERR_METADATA;
    }
    return 0;
}

int TransferMetadata::receivePeerMetadata(const Json::Value &peer_json,
                                          Json::Value &local_json) {
    // TODO: save to local cache
    // auto peer_desc = decodeSegmentDesc(peer_json,
    // peer_json["name"].asString());
    auto local_desc = segment_id_to_desc_map_[LOCAL_SEGMENT_ID];
    local_json = exportSegmentDesc(*local_desc.get());
    return 0;
}

std::shared_ptr<SegmentDesc> TransferMetadata::getSegmentDesc(
    const std::string &segment_name) {
    Json::Value peer_json;

    if (p2p_handshake_mode_) {
        auto [ip, port] = parseHostNameWithPort(segment_name);
        Json::Value local_json;
        auto desc = segment_id_to_desc_map_[LOCAL_SEGMENT_ID];
        local_json = exportSegmentDesc(*desc.get());
        int ret = handshake_plugin_->exchangeMetadata(ip, port, local_json,
                                                      peer_json);
        if (ret) {
            return nullptr;
        }
    } else {
        std::string value;
        auto status =
            storage_plugin_->get(getFullMetadataKey(segment_name), value);
        if (status.IsNotSuchKey()) {
            LOG(WARNING) << "segment descriptor not found, name "
                         << segment_name;
            return nullptr;
        } else if (!status.ok()) {
            LOG(ERROR) << "get segment descriptor failed, name " << segment_name
                       << " detail " << status.ToString();
            return nullptr;
        }
        if (!Json::Reader{}.parse(value, peer_json)) {
            LOG(ERROR) << "parse segment descriptor to JSON failed, name "
                       << segment_name;
            return nullptr;
        }
    }
    auto desc = praseSegmentDesc(peer_json);
    if (!desc) {
        LOG(WARNING) << "Corrupted segment descriptor, name " << segment_name;
    }
    return desc;
}

int TransferMetadata::syncSegmentCache(const std::string &segment_name) {
    RWSpinlock::WriteGuard guard(segment_lock_);
    for (auto &entry : segment_id_to_desc_map_) {
        if (entry.first == LOCAL_SEGMENT_ID) continue;
        if (!segment_name.empty() && entry.second->name != segment_name)
            continue;
        auto segment_desc = getSegmentDesc(entry.second->name);
        if (segment_desc)
            entry.second = segment_desc;
        else
            LOG(WARNING) << "segment " << entry.second->name
                         << " is now invalid";
    }
    return 0;
}

std::shared_ptr<SegmentDesc> TransferMetadata::getSegmentDescByName(
    const std::string &segment_name, bool force_update) {
    if (globalConfig().metacache && !force_update) {
        RWSpinlock::ReadGuard guard(segment_lock_);
        auto iter = segment_name_to_id_map_.find(segment_name);
        if (iter != segment_name_to_id_map_.end())
            return segment_id_to_desc_map_[iter->second];
    }

    RWSpinlock::WriteGuard guard(segment_lock_);
    auto iter = segment_name_to_id_map_.find(segment_name);
    SegmentID segment_id;
    if (iter != segment_name_to_id_map_.end())
        segment_id = iter->second;
    else
        segment_id = next_segment_id_.fetch_add(1);
    if (segment_id == LOCAL_SEGMENT_ID)
        return segment_id_to_desc_map_[iter->second];
    auto segment_desc = this->getSegmentDesc(segment_name);
    if (!segment_desc) return nullptr;
    segment_id_to_desc_map_[segment_id] = segment_desc;
    segment_name_to_id_map_[segment_name] = segment_id;
    return segment_desc;
}

std::shared_ptr<SegmentDesc> TransferMetadata::getSegmentDescByID(
    SegmentID segment_id, bool force_update) {
    if (segment_id != LOCAL_SEGMENT_ID &&
        (!globalConfig().metacache || force_update)) {
        RWSpinlock::WriteGuard guard(segment_lock_);
        if (!segment_id_to_desc_map_.count(segment_id)) return nullptr;
        auto segment_desc =
            getSegmentDesc(segment_id_to_desc_map_[segment_id]->name);
        if (!segment_desc) return nullptr;
        segment_id_to_desc_map_[segment_id] = segment_desc;
        return segment_id_to_desc_map_[segment_id];
    } else {
        RWSpinlock::ReadGuard guard(segment_lock_);
        if (!segment_id_to_desc_map_.count(segment_id)) return nullptr;
        return segment_id_to_desc_map_[segment_id];
    }
}

SegmentID TransferMetadata::getSegmentID(const std::string &segment_name) {
    {
        RWSpinlock::ReadGuard guard(segment_lock_);
        if (segment_name_to_id_map_.count(segment_name))
            return segment_name_to_id_map_[segment_name];
    }

    RWSpinlock::WriteGuard guard(segment_lock_);
    if (segment_name_to_id_map_.count(segment_name))
        return segment_name_to_id_map_[segment_name];
    auto segment_desc = this->getSegmentDesc(segment_name);
    if (!segment_desc) return -1;
    SegmentID id = next_segment_id_.fetch_add(1);
    segment_id_to_desc_map_[id] = segment_desc;
    segment_name_to_id_map_[segment_name] = id;
    return id;
}

int TransferMetadata::updateLocalSegmentDesc(uint64_t segment_id) {
    RWSpinlock::ReadGuard guard(segment_lock_);
    auto desc = segment_id_to_desc_map_[segment_id];
    return this->updateSegmentDesc(desc->name, *desc);
}

int TransferMetadata::addLocalSegment(SegmentID segment_id,
                                      const std::string &segment_name,
                                      std::shared_ptr<SegmentDesc> &&desc) {
    RWSpinlock::WriteGuard guard(segment_lock_);
    segment_id_to_desc_map_[segment_id] = desc;
    segment_name_to_id_map_[segment_name] = segment_id;
    return 0;
}

int TransferMetadata::removeLocalSegment(const std::string &segment_name) {
    RWSpinlock::WriteGuard guard(segment_lock_);
    if (segment_name_to_id_map_.count(segment_name)) {
        int segment_id = segment_name_to_id_map_[segment_name];
        segment_name_to_id_map_.erase(segment_name);
        segment_id_to_desc_map_.erase(segment_id);
    }
    return 0;
}

int TransferMetadata::addLocalMemoryBuffer(const BufferDesc &buffer_desc,
                                           bool update_metadata) {
    {
        RWSpinlock::WriteGuard guard(segment_lock_);
        auto new_segment_desc = std::make_shared<SegmentDesc>();
        auto &segment_desc = segment_id_to_desc_map_[LOCAL_SEGMENT_ID];
        *new_segment_desc = *segment_desc;
        segment_desc = new_segment_desc;
        std::get<MemorySegmentDesc>(segment_desc->detail)
            .buffers.push_back(buffer_desc);
    }
    if (update_metadata) return updateLocalSegmentDesc();
    return 0;
}

int TransferMetadata::removeLocalMemoryBuffer(void *addr,
                                              bool update_metadata) {
    bool addr_exist = false;
    {
        RWSpinlock::WriteGuard guard(segment_lock_);
        auto new_segment_desc = std::make_shared<SegmentDesc>();
        auto &segment_desc = segment_id_to_desc_map_[LOCAL_SEGMENT_ID];
        *new_segment_desc = *segment_desc;
        segment_desc = new_segment_desc;
        auto &buffers =
            std::get<MemorySegmentDesc>(segment_desc->detail).buffers;
        for (auto iter = buffers.begin(); iter != buffers.end(); ++iter) {
            if (iter->addr == (uint64_t)addr) {
                buffers.erase(iter);
                addr_exist = true;
                break;
            }
        }
    }
    if (addr_exist) {
        if (update_metadata) return updateLocalSegmentDesc();
        return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

int TransferMetadata::addRpcMetaEntry(const std::string &server_name,
                                      RpcMetaDesc &desc) {
    local_rpc_meta_ = desc;

    if (p2p_handshake_mode_) {
        int rc = handshake_plugin_->startDaemon(desc.rpc_port, desc.sockfd);
        if (rc != 0) {
            return rc;
        }
        handshake_plugin_->registerOnMetadataCallBack(
            [this](const Json::Value &peer, Json::Value &local) -> int {
                return receivePeerMetadata(peer, local);
            });

        return 0;
    }

    Json::Value rpcMetaJSON;
    rpcMetaJSON["ip_or_host_name"] = desc.ip_or_host_name;
    rpcMetaJSON["rpc_port"] = static_cast<Json::UInt64>(desc.rpc_port);
    const std::string value = Json::FastWriter{}.write(rpcMetaJSON);
    auto status = storage_plugin_->set(kRpcMetaPrefix + server_name, value);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to update rpc metadata entry, name "
                   << server_name << " detail " << status.ToString();
        return ERR_METADATA;
    }
    return 0;
}

int TransferMetadata::removeRpcMetaEntry(const std::string &server_name) {
    if (p2p_handshake_mode_) {
        return 0;
    }
    auto status = storage_plugin_->remove(kRpcMetaPrefix + server_name);
    if (!status.ok()) {
        LOG(ERROR) << "Failed to remove rpc metadata entry, name "
                   << server_name << " detail " << status.ToString();
        return ERR_METADATA;
    }
    return 0;
}

int TransferMetadata::getRpcMetaEntry(const std::string &server_name,
                                      RpcMetaDesc &desc) {
    {
        RWSpinlock::ReadGuard guard(rpc_meta_lock_);
        if (rpc_meta_map_.count(server_name)) {
            desc = rpc_meta_map_[server_name];
            return 0;
        }
    }
    RWSpinlock::WriteGuard guard(rpc_meta_lock_);
    if (p2p_handshake_mode_) {
        auto [ip, port] = parseHostNameWithPort(server_name);
        desc.ip_or_host_name = ip;
        desc.rpc_port = port;
    } else {
        Json::Value rpcMetaJSON;
        std::string value;
        auto status = storage_plugin_->get(kRpcMetaPrefix + server_name, value);
        if (status.IsNotSuchKey()) {
            LOG(ERROR) << "rpc metadata entry not found, name " << server_name;
            return ERR_METADATA;
        } else if (!status.ok()) {
            LOG(ERROR) << "find rpc metadata entry failed, name " << server_name
                       << " detail " << status.ToString();
            return ERR_METADATA;
        }
        if (!Json::Reader{}.parse(value, rpcMetaJSON)) {
            LOG(ERROR) << "parse rpc metadata entry to JSON failed, name "
                       << server_name;
            return ERR_METADATA;
        }
        desc.ip_or_host_name = rpcMetaJSON["ip_or_host_name"].asString();
        desc.rpc_port = (uint16_t)rpcMetaJSON["rpc_port"].asUInt();
    }
    rpc_meta_map_[server_name] = desc;
    return 0;
}

int TransferMetadata::startHandshakeDaemon(
    OnReceiveHandShake on_receive_handshake, uint16_t listen_port, int sockfd) {
    int rc = handshake_plugin_->startDaemon(listen_port, sockfd);
    if (rc != 0) {
        return rc;
    }

    handshake_plugin_->registerOnConnectionCallBack(
        [on_receive_handshake](const Json::Value &peer,
                               Json::Value &local) -> int {
            HandShakeDesc local_desc, peer_desc;
            TransferHandshakeUtil::decode(peer, peer_desc);
            int ret = on_receive_handshake(peer_desc, local_desc);
            if (ret) return ret;
            local = TransferHandshakeUtil::encode(local_desc);
            return 0;
        });

    return 0;
}

int TransferMetadata::sendHandshake(const std::string &peer_server_name,
                                    const HandShakeDesc &local_desc,
                                    HandShakeDesc &peer_desc) {
    RpcMetaDesc peer_location;
    if (getRpcMetaEntry(peer_server_name, peer_location)) {
        return ERR_METADATA;
    }
    auto local = TransferHandshakeUtil::encode(local_desc);
    Json::Value peer;
    int ret = handshake_plugin_->send(peer_location.ip_or_host_name,
                                      peer_location.rpc_port, local, peer);
    if (ret) return ret;
    TransferHandshakeUtil::decode(peer, peer_desc);
    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "Handshake rejected by " << peer_server_name << ": "
                   << peer_desc.reply_msg;
        return ERR_METADATA;
    }
    return 0;
}

void SegmentDesc::dump() const {
    LOG(INFO) << "  segment name: " << name;
    LOG(INFO) << "  protocol: " << protocol;
    if (protocol == "rdma" || protocol == "shm") {
        auto &det = std::get<MemorySegmentDesc>(detail);
        LOG(INFO) << "  topology: " << det.topology.toString();
        LOG(INFO) << "  devices: ";
        for (auto &device : det.devices) {
            LOG(INFO) << "    device name " << device.name << ", lid "
                      << device.lid << ", " << device.gid;
        }
        LOG(INFO) << "  buffers: ";
        for (auto &buffer : det.buffers) {
            LOG(INFO) << "    buffer type " << buffer.location << ", address "
                      << (void *)buffer.addr << "--"
                      << (void *)(buffer.addr + buffer.length);
        }
    }
    LOG(INFO) << "  timestamp: " << timestamp;
}

void TransferMetadata::dumpMetadataContent(const std::string &segment_name,
                                           uint64_t offset, uint64_t length) {
    thread_local uint64_t last_ts = 0;
    uint64_t current_ts = getCurrentTimeInNano();
    const static uint64_t kMinDisplayThreshold = 500000000;  // 0.5 sec

    auto segment_locked = segment_lock_.tryLockShared();
    auto rpc_meta_locked = rpc_meta_lock_.tryLockShared();
    if (!segment_locked || !rpc_meta_locked) {
        LOG(WARNING) << "Dump without lock protection";
    }

    if (current_ts - last_ts > kMinDisplayThreshold || globalConfig().trace) {
        LOG(INFO) << "Failed to get segment descriptor for segment "
                  << segment_name << " address " << (void *)offset << "--"
                  << (void *)(offset + length);
        dumpMetadataContentUnlocked();
        last_ts = current_ts;
    }

    if (rpc_meta_locked) rpc_meta_lock_.unlockShared();
    if (segment_locked) segment_lock_.unlockShared();
}

void TransferMetadata::dumpMetadataContentUnlocked() {
    LOG(INFO) << "-----------------------------------------------------------";
    LOG(INFO) << "TransferMetadata::dumpMetadataContent";
    LOG(INFO) << "-----------------------------------------------------------";
    LOG(INFO) << "=== Cached Segment Descriptors ===";
    for (auto &entry : segment_id_to_desc_map_) {
        auto &desc = entry.second;
        if (!desc) {
            LOG(INFO) << "segment id: " << entry.first << ", ref object nil";
        } else {
            LOG(INFO) << "segment id: " << entry.first << ", ref object "
                      << &desc;
            desc->dump();
        }
    }
    LOG(INFO) << "=== Local RPC Route ===";
    LOG(INFO) << "location: " << local_rpc_meta_.ip_or_host_name << ":"
              << local_rpc_meta_.rpc_port;
    LOG(INFO) << "=== Remote RPC Routes ===";
    for (auto &entry : rpc_meta_map_) {
        LOG(INFO) << "segment name: " << entry.first
                  << ", location: " << entry.second.ip_or_host_name << ":"
                  << entry.second.rpc_port;
    }
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

std::shared_ptr<SegmentDesc> TransferMetadata::praseSegmentDesc(
    const Json::Value &segmentJSON) {
    try {
        auto desc = std::make_shared<SegmentDesc>();
        desc->name = getItem(segmentJSON, "name");
        desc->protocol = getItem(segmentJSON, "protocol");
        if (desc->name.empty() || desc->protocol.empty()) return nullptr;
        if (desc->protocol == "rdma" || desc->protocol == "shm") {
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
            desc->detail = MemorySegmentDesc{};
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
            desc->detail = FileSegmentDesc{};
            auto &detail = std::get<FileSegmentDesc>(desc->detail);
            if (segmentJSON.isMember("buffers")) {
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
            }
        } else {
            return nullptr;
        }
        return desc;
    } catch (std::exception &ex) {
        LOG(ERROR) << "internal exception: " << ex.what();
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
}  // namespace v1
}  // namespace mooncake
