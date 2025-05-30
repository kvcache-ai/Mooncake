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

#include "transfer_metadata.h"

#include <json/value.h>

#include <cassert>
#include <set>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transfer_metadata_plugin.h"

namespace mooncake {

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
    static Json::Value encode(const TransferMetadata::HandShakeDesc &desc) {
        Json::Value root;
        root["local_nic_path"] = desc.local_nic_path;
        root["peer_nic_path"] = desc.peer_nic_path;
        Json::Value qpNums(Json::arrayValue);
        for (const auto &qp : desc.qp_num) qpNums.append(qp);
        root["qp_num"] = qpNums;
        root["reply_msg"] = desc.reply_msg;
        return root;
    }

    static int decode(Json::Value root, TransferMetadata::HandShakeDesc &desc) {
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
    storage_plugin_ = MetadataStoragePlugin::Create(conn_string);
    if (!storage_plugin_) {
        LOG(ERROR)
            << "Unable to create metadata storage plugin with conn string "
            << conn_string;
    }
}

TransferMetadata::~TransferMetadata() { handshake_plugin_.reset(); }

int TransferMetadata::encodeSegmentDesc(const SegmentDesc &desc,
                                        Json::Value &segmentJSON) {
    segmentJSON["name"] = desc.name;
    segmentJSON["protocol"] = desc.protocol;
    segmentJSON["timestamp"] = getCurrentDateTime();

    if (segmentJSON["protocol"] == "rdma") {
        Json::Value devicesJSON(Json::arrayValue);
        for (const auto &device : desc.devices) {
            Json::Value deviceJSON;
            deviceJSON["name"] = device.name;
            deviceJSON["lid"] = device.lid;
            deviceJSON["gid"] = device.gid;
            devicesJSON.append(deviceJSON);
        }
        segmentJSON["devices"] = devicesJSON;

        Json::Value buffersJSON(Json::arrayValue);
        for (const auto &buffer : desc.buffers) {
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
        segmentJSON["priority_matrix"] = desc.topology.toJson();
    } else if (segmentJSON["protocol"] == "tcp") {
        Json::Value buffersJSON(Json::arrayValue);
        for (const auto &buffer : desc.buffers) {
            Json::Value bufferJSON;
            bufferJSON["name"] = buffer.name;
            bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
            bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;
    } else if (segmentJSON["protocol"] == "ascend"){
        Json::Value devicesJSON(Json::arrayValue);
        for (const auto &device : desc.devices) {
            Json::Value deviceJSON;
            deviceJSON["name"] = device.name;
            deviceJSON["lid"] = device.lid;
            devicesJSON.append(deviceJSON);
        }
        segmentJSON["devices"] = devicesJSON;
        Json::Value buffersJSON(Json::arrayValue);
        for (const auto &buffer : desc.buffers) {
            Json::Value bufferJSON;
            bufferJSON["name"] = buffer.name;
            bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
            bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;

        Json::Value rankInfoJSON;
        rankInfoJSON["rankId"] = static_cast<Json::UInt64>(desc.rank_info.rankId);
        rankInfoJSON["hostIp"] = desc.rank_info.hostIp;
        rankInfoJSON["hostPort"] = static_cast<Json::UInt64>(desc.rank_info.hostPort);
        rankInfoJSON["deviceLogicId"] = static_cast<Json::UInt64>(desc.rank_info.deviceLogicId);
        rankInfoJSON["devicePhyId"] = static_cast<Json::UInt64>(desc.rank_info.devicePhyId);
        rankInfoJSON["deviceType"] = static_cast<Json::UInt64>(desc.rank_info.deviceType);
        rankInfoJSON["deviceIp"] = desc.rank_info.deviceIp;
        rankInfoJSON["devicePort"] = static_cast<Json::UInt64>(desc.rank_info.devicePort);

        segmentJSON["rank_info"] = rankInfoJSON;
    } else {
        LOG(ERROR) << "Unsupported segment descriptor for register, name "
                   << desc.name << " protocol " << desc.protocol;
        return ERR_METADATA;
    }
    return 0;
}

int TransferMetadata::updateSegmentDesc(const std::string &segment_name,
                                        const SegmentDesc &desc) {
    if (p2p_handshake_mode_) {
        return 0;
    }

    Json::Value segmentJSON;
    int ret = encodeSegmentDesc(desc, segmentJSON);
    if (ret) {
        return ret;
    }

    if (!storage_plugin_->set(getFullMetadataKey(segment_name), segmentJSON)) {
        LOG(ERROR) << "Failed to register segment descriptor, name "
                   << desc.name << " protocol " << desc.protocol;
        return ERR_METADATA;
    }

    return 0;
}

int TransferMetadata::removeSegmentDesc(const std::string &segment_name) {
    if (p2p_handshake_mode_) {
        return 0;
    }
    if (!storage_plugin_->remove(getFullMetadataKey(segment_name))) {
        LOG(ERROR) << "Failed to unregister segment descriptor, name "
                   << segment_name;
        return ERR_METADATA;
    }
    return 0;
}

std::shared_ptr<TransferMetadata::SegmentDesc>
TransferMetadata::decodeSegmentDesc(Json::Value &segmentJSON,
                                    const std::string &segment_name) {
    auto desc = std::make_shared<SegmentDesc>();
    desc->name = segmentJSON["name"].asString();
    desc->protocol = segmentJSON["protocol"].asString();
    if (segmentJSON.isMember("timestamp"))
        desc->timestamp = segmentJSON["timestamp"].asString();

    if (desc->protocol == "rdma") {
        for (const auto &deviceJSON : segmentJSON["devices"]) {
            DeviceDesc device;
            device.name = deviceJSON["name"].asString();
            device.lid = deviceJSON["lid"].asUInt();
            device.gid = deviceJSON["gid"].asString();
            if (device.name.empty() || device.gid.empty()) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol " << desc->protocol;
                return nullptr;
            }
            desc->devices.push_back(device);
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
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol " << desc->protocol;
                return nullptr;
            }
            desc->buffers.push_back(buffer);
        }

        int ret = desc->topology.parse(
            segmentJSON["priority_matrix"].toStyledString());
        if (ret) {
            LOG(WARNING) << "Corrupted segment descriptor, name "
                         << segment_name << " protocol " << desc->protocol;
        }
    } else if (desc->protocol == "tcp") {
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
            if (buffer.name.empty() || !buffer.addr || !buffer.length) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol " << desc->protocol;
                return nullptr;
            }
            desc->buffers.push_back(buffer);
        }
    } else if (desc->protocol == "nvmeof") {
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            NVMeoFBufferDesc buffer;
            buffer.file_path = bufferJSON["file_path"].asString();
            buffer.length = bufferJSON["length"].asUInt64();
            const Json::Value &local_path_map = bufferJSON["local_path_map"];
            for (const auto &key : local_path_map.getMemberNames()) {
                buffer.local_path_map[key] = local_path_map[key].asString();
            }
            desc->nvmeof_buffers.push_back(buffer);
        }
    } else if (desc->protocol == "ascend") {
        for (const auto &deviceJSON : segmentJSON["devices"]) {
            DeviceDesc device;
            device.name = deviceJSON["name"].asString();
            device.lid = deviceJSON["lid"].asUInt();
            if (device.name.empty()) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol " << desc->protocol;
                return nullptr;
            }
            desc->devices.push_back(device);
        }

        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
            if (buffer.name.empty() || !buffer.addr || !buffer.length) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol " << desc->protocol;
                return nullptr;
            }
            desc->buffers.push_back(buffer);
        }

        Json::Value rankInfoJSON = segmentJSON["rank_info"];
        desc->rank_info.rankId = rankInfoJSON["rankId"].asUInt64();
        desc->rank_info.hostIp = rankInfoJSON["hostIp"].asString();
        desc->rank_info.hostPort = rankInfoJSON["hostPort"].asUInt64();
        desc->rank_info.deviceLogicId = rankInfoJSON["deviceLogicId"].asUInt64();
        desc->rank_info.devicePhyId = rankInfoJSON["devicePhyId"].asUInt64();
        desc->rank_info.deviceType = rankInfoJSON["deviceType"].asUInt64();
        desc->rank_info.deviceIp = rankInfoJSON["deviceIp"].asString();
        desc->rank_info.devicePort = rankInfoJSON["devicePort"].asUInt64();
    } else {
        LOG(ERROR) << "Unsupported segment descriptor, name " << segment_name
                   << " protocol " << desc->protocol;
        return nullptr;
    }
    return desc;
}

int TransferMetadata::receivePeerMetadata(const Json::Value &peer_json,
                                          Json::Value &local_json) {
    // TODO: save to local cache
    // auto peer_desc = decodeSegmentDesc(peer_json,
    // peer_json["name"].asString());
    auto local_desc = segment_id_to_desc_map_[LOCAL_SEGMENT_ID];
    int ret = encodeSegmentDesc(*local_desc.get(), local_json);
    return ret;
}

std::shared_ptr<TransferMetadata::SegmentDesc> TransferMetadata::getSegmentDesc(
    const std::string &segment_name) {
    Json::Value peer_json;

    if (p2p_handshake_mode_) {
        auto [ip, port] = parseHostNameWithPort(segment_name);
        Json::Value local_json;
        auto desc = segment_id_to_desc_map_[LOCAL_SEGMENT_ID];
        int ret = encodeSegmentDesc(*desc.get(), local_json);
        if (ret) {
            return nullptr;
        }
        ret = handshake_plugin_->exchangeMetadata(ip, port, local_json,
                                                  peer_json);
        if (ret) {
            return nullptr;
        }
    } else {
        if (!storage_plugin_->get(getFullMetadataKey(segment_name),
                                  peer_json)) {
            LOG(WARNING) << "Failed to retrieve segment descriptor, name "
                         << segment_name;
            return nullptr;
        }
    }

    return decodeSegmentDesc(peer_json, segment_name);
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

std::shared_ptr<TransferMetadata::SegmentDesc>
TransferMetadata::getSegmentDescByName(const std::string &segment_name,
                                       bool force_update) {
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

std::shared_ptr<TransferMetadata::SegmentDesc>
TransferMetadata::getSegmentDescByID(SegmentID segment_id, bool force_update) {
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

TransferMetadata::SegmentID TransferMetadata::getSegmentID(
    const std::string &segment_name) {
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
        segment_desc->buffers.push_back(buffer_desc);
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
        for (auto iter = segment_desc->buffers.begin();
             iter != segment_desc->buffers.end(); ++iter) {
            if (iter->addr == (uint64_t)addr) {
                segment_desc->buffers.erase(iter);
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
    if (!storage_plugin_->set(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        LOG(ERROR) << "Failed to set location of " << server_name;
        return ERR_METADATA;
    }
    return 0;
}

int TransferMetadata::removeRpcMetaEntry(const std::string &server_name) {
    if (p2p_handshake_mode_) {
        return 0;
    }
    if (!storage_plugin_->remove(kRpcMetaPrefix + server_name)) {
        LOG(ERROR) << "Failed to remove location of " << server_name;
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
        if (!storage_plugin_->get(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
            LOG(ERROR) << "Failed to find location of " << server_name;
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

}  // namespace mooncake
