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

#include <jsoncpp/json/value.h>

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
    handshake_plugin_ = MetadataHandShakePlugin::Create(conn_string);
    storage_plugin_ = MetadataStoragePlugin::Create(conn_string);
    next_segment_id_.store(1);
}

TransferMetadata::~TransferMetadata() { handshake_plugin_.reset(); }

int TransferMetadata::updateSegmentDesc(const std::string &segment_name,
                                        const SegmentDesc &desc) {
    Json::Value segmentJSON;
    segmentJSON["name"] = desc.name;
    segmentJSON["protocol"] = desc.protocol;

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

        Json::Value priorityMatrixJSON;
        for (auto &entry : desc.priority_matrix) {
            Json::Value priorityItemJSON(Json::arrayValue);
            Json::Value preferredRnicListJSON(Json::arrayValue);
            for (auto &device_name : entry.second.preferred_rnic_list)
                preferredRnicListJSON.append(device_name);
            priorityItemJSON.append(preferredRnicListJSON);
            Json::Value availableRnicListJSON(Json::arrayValue);
            for (auto &device_name : entry.second.available_rnic_list)
                availableRnicListJSON.append(device_name);
            priorityItemJSON.append(availableRnicListJSON);
            priorityMatrixJSON[entry.first] = priorityItemJSON;
        }
        segmentJSON["priority_matrix"] = priorityMatrixJSON;
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
    } else {
        LOG(FATAL)
            << "For NVMeoF, the transfer engine should not modify the metadata";
        return ERR_METADATA;
    }

    if (!storage_plugin_->set(getFullMetadataKey(segment_name), segmentJSON)) {
        return ERR_METADATA;
    }

    return 0;
}

int TransferMetadata::removeSegmentDesc(const std::string &segment_name) {
    if (!storage_plugin_->remove(getFullMetadataKey(segment_name))) {
        return ERR_METADATA;
    }
    return 0;
}

std::shared_ptr<TransferMetadata::SegmentDesc> TransferMetadata::getSegmentDesc(
    const std::string &segment_name) {
    Json::Value segmentJSON;
    if (!storage_plugin_->get(getFullMetadataKey(segment_name), segmentJSON)) {
        return nullptr;
    }

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) {
        return nullptr;
    }

    desc->name = segmentJSON["name"].asString();
    desc->protocol = segmentJSON["protocol"].asString();

    if (desc->protocol == "rdma") {
        for (const auto &deviceJSON : segmentJSON["devices"]) {
            DeviceDesc device;
            device.name = deviceJSON["name"].asString();
            device.lid = deviceJSON["lid"].asUInt();
            device.gid = deviceJSON["gid"].asString();
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
            desc->buffers.push_back(buffer);
        }

        auto priorityMatrixJSON = segmentJSON["priority_matrix"];
        for (const auto &key : priorityMatrixJSON.getMemberNames()) {
            const Json::Value &value = priorityMatrixJSON[key];
            if (value.isArray() && value.size() == 2) {
                PriorityItem item;
                for (const auto &array : value[0]) {
                    auto device_name = array.asString();
                    item.preferred_rnic_list.push_back(device_name);
                    int device_index = 0;
                    for (auto &entry : desc->devices) {
                        if (entry.name == device_name) {
                            item.preferred_rnic_id_list.push_back(device_index);
                            break;
                        }
                        device_index++;
                    }
                    LOG_ASSERT(device_index != (int)desc->devices.size());
                }
                for (const auto &array : value[1]) {
                    auto device_name = array.asString();
                    item.available_rnic_list.push_back(device_name);
                    int device_index = 0;
                    for (auto &entry : desc->devices) {
                        if (entry.name == device_name) {
                            item.available_rnic_id_list.push_back(device_index);
                            break;
                        }
                        device_index++;
                    }
                    LOG_ASSERT(device_index != (int)desc->devices.size());
                }
                desc->priority_matrix[key] = item;
            }
        }
    } else if (desc->protocol == "tcp") {
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
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
    }
    return desc;
}

int TransferMetadata::syncSegmentCache() {
    RWSpinlock::WriteGuard guard(segment_lock_);
    for (auto &entry : segment_id_to_desc_map_) {
        if (entry.first == LOCAL_SEGMENT_ID) continue;
        auto segment_desc = getSegmentDesc(entry.second->name);
        if (segment_desc) entry.second = segment_desc;
    }
    return 0;
}

std::shared_ptr<TransferMetadata::SegmentDesc>
TransferMetadata::getSegmentDescByName(const std::string &segment_name,
                                       bool force_update) {
    if (!force_update) {
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
    auto segment_desc = this->getSegmentDesc(segment_name);
    if (!segment_desc) return nullptr;
    segment_id_to_desc_map_[segment_id] = segment_desc;
    segment_name_to_id_map_[segment_name] = segment_id;
    return segment_desc;
}

std::shared_ptr<TransferMetadata::SegmentDesc>
TransferMetadata::getSegmentDescByID(SegmentID segment_id, bool force_update) {
    if (force_update) {
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

int TransferMetadata::addLocalMemoryBuffer(const BufferDesc &buffer_desc,
                                           bool update_metadata) {
    {
        RWSpinlock::WriteGuard guard(segment_lock_);
        auto new_segment_desc = std::make_shared<SegmentDesc>();
        if (!new_segment_desc) {
            return ERR_MEMORY;
        }
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
        if (!new_segment_desc) {
            return ERR_MEMORY;
        }
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
    Json::Value rpcMetaJSON;
    rpcMetaJSON["ip_or_host_name"] = desc.ip_or_host_name;
    rpcMetaJSON["rpc_port"] = static_cast<Json::UInt64>(desc.rpc_port);
    if (!storage_plugin_->set(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        return ERR_METADATA;
    }
    local_rpc_meta_ = desc;
    return 0;
}

int TransferMetadata::removeRpcMetaEntry(const std::string &server_name) {
    if (!storage_plugin_->remove(kRpcMetaPrefix + server_name)) {
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
    Json::Value rpcMetaJSON;
    if (!storage_plugin_->get(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        return ERR_METADATA;
    }
    desc.ip_or_host_name = rpcMetaJSON["ip_or_host_name"].asString();
    desc.rpc_port = (uint16_t)rpcMetaJSON["rpc_port"].asUInt();
    rpc_meta_map_[server_name] = desc;
    return 0;
}

int TransferMetadata::startHandshakeDaemon(
    OnReceiveHandShake on_receive_handshake, uint16_t listen_port) {
    return handshake_plugin_->startDaemon(
        [on_receive_handshake](const Json::Value &local,
                               Json::Value &peer) -> int {
            HandShakeDesc local_desc, peer_desc;
            TransferHandshakeUtil::decode(local, local_desc);
            int ret = on_receive_handshake(local_desc, peer_desc);
            if (ret) return ret;
            peer = TransferHandshakeUtil::encode(peer_desc);
            return 0;
        },
        listen_port);
}

int TransferMetadata::sendHandshake(const std::string &peer_server_name,
                                    const HandShakeDesc &local_desc,
                                    HandShakeDesc &peer_desc) {
    RpcMetaDesc peer_location;
    if (getRpcMetaEntry(peer_server_name, peer_location)) {
        LOG(ERROR) << "cannot find location of " << peer_server_name;
        return ERR_METADATA;
    }
    auto local = TransferHandshakeUtil::encode(local_desc);
    Json::Value peer;
    int ret = handshake_plugin_->send(peer_location, local, peer);
    if (ret) return ret;
    TransferHandshakeUtil::decode(peer, peer_desc);
    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "connection rejected by peer: " << peer_desc.reply_msg;
        return ERR_METADATA;
    }
    return 0;
}

int TransferMetadata::parseNicPriorityMatrix(
    const std::string &nic_priority_matrix, PriorityMatrix &priority_map,
    std::vector<std::string> &rnic_list) {
    std::set<std::string> rnic_set;
    Json::Value root;
    Json::Reader reader;

    if (nic_priority_matrix.empty() ||
        !reader.parse(nic_priority_matrix, root)) {
        LOG(ERROR) << "malformed format of NIC priority matrix";
        return ERR_MALFORMED_JSON;
    }

    if (!root.isObject()) {
        LOG(ERROR) << "malformed format of NIC priority matrix";
        return ERR_MALFORMED_JSON;
    }

    priority_map.clear();
    for (const auto &key : root.getMemberNames()) {
        const Json::Value &value = root[key];
        if (value.isArray() && value.size() == 2) {
            PriorityItem item;
            for (const auto &array : value[0]) {
                auto device_name = array.asString();
                item.preferred_rnic_list.push_back(device_name);
                auto iter = rnic_set.find(device_name);
                if (iter == rnic_set.end()) {
                    item.preferred_rnic_id_list.push_back(rnic_set.size());
                    rnic_set.insert(device_name);
                } else {
                    item.preferred_rnic_id_list.push_back(
                        std::distance(rnic_set.begin(), iter));
                }
            }
            for (const auto &array : value[1]) {
                auto device_name = array.asString();
                item.available_rnic_list.push_back(device_name);
                auto iter = rnic_set.find(device_name);
                if (iter == rnic_set.end()) {
                    item.available_rnic_id_list.push_back(rnic_set.size());
                    rnic_set.insert(device_name);
                } else {
                    item.available_rnic_id_list.push_back(
                        std::distance(rnic_set.begin(), iter));
                }
            }
            priority_map[key] = item;
        } else {
            LOG(ERROR) << "malformed format of NIC priority matrix";
            return ERR_MALFORMED_JSON;
        }
    }

    rnic_list.clear();
    for (auto &entry : rnic_set) rnic_list.push_back(entry);

    return 0;
}

}  // namespace mooncake