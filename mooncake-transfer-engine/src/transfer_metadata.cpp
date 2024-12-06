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

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <jsoncpp/json/value.h>
#include <netdb.h>
#include <sys/socket.h>

#ifdef USE_REDIS
#include <hiredis/hiredis.h>
#endif

#include <cassert>
#include <set>

#include "common.h"
#include "config.h"
#include "error.h"

namespace mooncake {

const static std::string kRpcMetaPrefix = "mooncake/rpc_meta/";
// mooncake/segments/[...]
static inline std::string getFullMetadataKey(const std::string &segment_name) {
    const static std::string keyPrefix = "mooncake/";
    auto pos = segment_name.find("/");
    if (pos == segment_name.npos)
        return keyPrefix + "ram/" + segment_name;
    else
        return keyPrefix + segment_name;
}

struct TransferMetadataImpl {
    TransferMetadataImpl() {}
    virtual ~TransferMetadataImpl() {}
    virtual bool get(const std::string &key, Json::Value &value) = 0;
    virtual bool set(const std::string &key, const Json::Value &value) = 0;
    virtual bool remove(const std::string &key) = 0;
};

#ifdef USE_REDIS
struct TransferMetadataImpl4Redis : public TransferMetadataImpl {
    TransferMetadataImpl4Redis(const std::string &metadata_uri)
        : client_(nullptr), metadata_uri_(metadata_uri) {
        auto hostname_port = parseHostNameWithPort(metadata_uri);
        client_ = redisConnect(hostname_port.first.c_str(), hostname_port.second);
        if (!client_ || client_->err) {
            LOG(ERROR) << "redis error: " << client_->errstr;
        }
    }

    virtual ~TransferMetadataImpl4Redis() {}

    virtual bool get(const std::string &key, Json::Value &value) {
        Json::Reader reader;
        redisReply *resp = (redisReply *) redisCommand(client_, "GET %s", key.c_str());
        if (!resp) {
            LOG(ERROR) << "Error from redis client uri: " << metadata_uri_;
            return false;
        }
        auto json_file = std::string(resp->str);
        freeReplyObject(resp);
        if (!reader.parse(json_file, value)) return false;
        if (globalConfig().verbose)
            LOG(INFO) << "Get segment desc, key=" << key
                      << ", value=" << json_file;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "Put segment desc, key=" << key
                      << ", value=" << json_file;
        
        redisReply *resp = (redisReply *) redisCommand(client_,"SET %s %s", key.c_str(), json_file.c_str());
        if (!resp) {
            LOG(ERROR) << "Error from redis client uri: " << metadata_uri_;
            return false;
        }
        freeReplyObject(resp);
        return true;
    }

    virtual bool remove(const std::string &key) {
        redisReply *resp = (redisReply *) redisCommand(client_,"DEL %s", key.c_str());
        if (!resp) {
            LOG(ERROR) << "Error from redis client uri: " << metadata_uri_;
            return false;
        }
        freeReplyObject(resp);
        return true;
    }

    redisContext *client_;
    const std::string metadata_uri_;
};
#endif // USE_REDIS

struct TransferMetadataImpl4Etcd : public TransferMetadataImpl {
    TransferMetadataImpl4Etcd(const std::string &metadata_uri)
        : client_(metadata_uri), metadata_uri_(metadata_uri) {}

    virtual ~TransferMetadataImpl4Etcd() {}

    virtual bool get(const std::string &key, Json::Value &value) {
        Json::Reader reader;
        auto resp = client_.get(key);
        if (!resp.is_ok()) {
            LOG(ERROR) << "Error from etcd client, etcd uri: " << metadata_uri_
                       << " error: " << resp.error_code()
                       << " message: " << resp.error_message();
            return false;
        }
        auto json_file = resp.value().as_string();
        if (!reader.parse(json_file, value)) return false;
        if (globalConfig().verbose)
            LOG(INFO) << "Get segment desc, key=" << key
                      << ", value=" << json_file;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "Put segment desc, key=" << key
                      << ", value=" << json_file;
        auto resp = client_.put(key, json_file);
        if (!resp.is_ok()) {
            LOG(ERROR) << "Error from etcd client, etcd uri: " << metadata_uri_
                       << " error: " << resp.error_code()
                       << " message: " << resp.error_message();
            return false;
        }
        return true;
    }

    virtual bool remove(const std::string &key) {
        auto resp = client_.rm(key);
        if (!resp.is_ok()) {
            LOG(ERROR) << "Error from etcd client, etcd uri: " << metadata_uri_
                       << " error: " << resp.error_code()
                       << " message: " << resp.error_message();
            return false;
        }
        return true;
    }

    etcd::SyncClient client_;
    const std::string metadata_uri_;
};

TransferMetadata::TransferMetadata(const std::string &metadata_uri, const std::string &protocol)
    : listener_running_(false) {
    if (protocol == "etcd") {
        impl_ = std::make_shared<TransferMetadataImpl4Etcd>(metadata_uri);
        if (!impl_) {
            LOG(ERROR) << "Cannot allocate TransferMetadataImpl objects";
            exit(EXIT_FAILURE);
        }
#ifdef USE_REDIS
    } else if (protocol == "redis") {
        impl_ = std::make_shared<TransferMetadataImpl4Redis>(metadata_uri);
        if (!impl_) {
            LOG(ERROR) << "Cannot allocate TransferMetadataImpl objects";
            exit(EXIT_FAILURE);
        }
#endif // USE_REDIS
    } else {
        LOG(ERROR) << "Unsupported metdata protocol " << protocol;
        exit(EXIT_FAILURE);
    }
    next_segment_id_.store(1);
}

TransferMetadata::~TransferMetadata() {
    if (listener_running_) {
        listener_running_ = false;
        listener_.join();
    }
}

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

    if (!impl_->set(getFullMetadataKey(segment_name), segmentJSON)) {
        LOG(ERROR) << "Failed to put segment description: " << segment_name;
        return ERR_METADATA;
    }

    return 0;
}

int TransferMetadata::removeSegmentDesc(const std::string &segment_name) {
    if (!impl_->remove(getFullMetadataKey(segment_name))) {
        LOG(ERROR) << "Failed to remove segment description: " << segment_name;
        return ERR_METADATA;
    }
    return 0;
}

std::shared_ptr<TransferMetadata::SegmentDesc> TransferMetadata::getSegmentDesc(
    const std::string &segment_name) {
    Json::Value segmentJSON;
    if (!impl_->get(getFullMetadataKey(segment_name), segmentJSON)) {
        LOG(ERROR) << "Failed to get segment description: " << segment_name;
        return nullptr;
    }

    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) {
        LOG(ERROR) << "Failed to allocate SegmentDesc object";
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
    // LOG(INFO) << "put " << id;
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
                                      const string &segment_name,
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
            LOG(ERROR) << "Failed to allocate segment description";
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
            LOG(ERROR) << "Failed to allocate segment description";
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
    if (!impl_->set(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        LOG(ERROR) << "Failed to insert rpc meta of " << server_name;
        return ERR_METADATA;
    }
    local_rpc_meta_ = desc;
    return 0;
}

int TransferMetadata::removeRpcMetaEntry(const std::string &server_name) {
    if (!impl_->remove(kRpcMetaPrefix + server_name)) {
        LOG(ERROR) << "Failed to remove rpc meta of " << server_name;
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
    if (!impl_->get(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        LOG(ERROR) << "Failed to get rpc meta of " << server_name;
        return ERR_METADATA;
    }
    desc.ip_or_host_name = rpcMetaJSON["ip_or_host_name"].asString();
    desc.rpc_port = (uint16_t)rpcMetaJSON["rpc_port"].asUInt();
    rpc_meta_map_[server_name] = desc;
    return 0;
}

std::string TransferMetadata::encode(const HandShakeDesc &desc) {
    Json::Value root;
    root["local_nic_path"] = desc.local_nic_path;
    root["peer_nic_path"] = desc.peer_nic_path;
    Json::Value qpNums(Json::arrayValue);
    for (const auto &qp : desc.qp_num) qpNums.append(qp);
    root["qp_num"] = qpNums;
    root["reply_msg"] = desc.reply_msg;
    Json::FastWriter writer;
    auto serialized = writer.write(root);
    if (globalConfig().verbose)
        LOG(INFO) << "Send Endpoint Handshake Info: " << serialized;
    return serialized;
}

int TransferMetadata::decode(const std::string &serialized,
                             HandShakeDesc &desc) {
    Json::Value root;
    Json::Reader reader;

    if (serialized.empty() || !reader.parse(serialized, root))
        return ERR_MALFORMED_JSON;

    if (globalConfig().verbose)
        LOG(INFO) << "Receive Endpoint Handshake Info: " << serialized;
    desc.local_nic_path = root["local_nic_path"].asString();
    desc.peer_nic_path = root["peer_nic_path"].asString();
    for (const auto &qp : root["qp_num"]) desc.qp_num.push_back(qp.asUInt());
    desc.reply_msg = root["reply_msg"].asString();

    return 0;
}

static inline const std::string toString(struct sockaddr *addr) {
    if (addr->sa_family == AF_INET) {
        struct sockaddr_in *sock_addr = (struct sockaddr_in *)addr;
        char ip[INET_ADDRSTRLEN];
        if (inet_ntop(addr->sa_family, &(sock_addr->sin_addr), ip,
                      INET_ADDRSTRLEN) != NULL)
            return ip;
    } else if (addr->sa_family == AF_INET6) {
        struct sockaddr_in6 *sock_addr = (struct sockaddr_in6 *)addr;
        char ip[INET6_ADDRSTRLEN];
        if (inet_ntop(addr->sa_family, &(sock_addr->sin6_addr), ip,
                      INET6_ADDRSTRLEN) != NULL)
            return ip;
    }
    LOG(ERROR) << "Invalid address, cannot convert to string";
    return "<unknown>";
}

int TransferMetadata::startHandshakeDaemon(
    OnReceiveHandShake on_receive_handshake, uint16_t listen_port) {
    sockaddr_in bind_address;
    int on = 1, listen_fd = -1;
    memset(&bind_address, 0, sizeof(sockaddr_in));
    bind_address.sin_family = AF_INET;
    bind_address.sin_port = htons(listen_port);
    bind_address.sin_addr.s_addr = INADDR_ANY;

    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        PLOG(ERROR) << "Failed to create socket";
        return ERR_SOCKET;
    }

    struct timeval timeout;
    timeout.tv_sec = 1;
    timeout.tv_usec = 0;
    if (setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout))) {
        PLOG(ERROR) << "Failed to set socket timeout";
        close(listen_fd);
        return ERR_SOCKET;
    }

    if (setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
        PLOG(ERROR) << "Failed to set address reusable";
        close(listen_fd);
        return ERR_SOCKET;
    }

    if (bind(listen_fd, (sockaddr *)&bind_address, sizeof(sockaddr_in)) < 0) {
        PLOG(ERROR) << "Failed to bind address, rpc port: " << listen_port;
        close(listen_fd);
        return ERR_SOCKET;
    }

    if (listen(listen_fd, 5)) {
        PLOG(ERROR) << "Failed to listen";
        close(listen_fd);
        return ERR_SOCKET;
    }

    listener_running_ = true;
    listener_ = std::thread([this, listen_fd, on_receive_handshake]() {
        while (listener_running_) {
            sockaddr_in addr;
            socklen_t addr_len = sizeof(sockaddr_in);
            int conn_fd = accept(listen_fd, (sockaddr *)&addr, &addr_len);
            if (conn_fd < 0) {
                if (errno != EWOULDBLOCK)
                    PLOG(ERROR) << "Failed to accept socket connection";
                continue;
            }

            if (addr.sin_family != AF_INET && addr.sin_family != AF_INET6) {
                LOG(ERROR)
                    << "Unsupported socket type, should be AF_INET or AF_INET6";
                close(conn_fd);
                continue;
            }

            struct timeval timeout;
            timeout.tv_sec = 60;
            timeout.tv_usec = 0;
            if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                           sizeof(timeout))) {
                PLOG(ERROR) << "Failed to set socket timeout";
                close(conn_fd);
                continue;
            }

            if (globalConfig().verbose)
                LOG(INFO) << "New connection: "
                          << toString((struct sockaddr *)&addr) << ":"
                          << ntohs(addr.sin_port);

            HandShakeDesc local_desc, peer_desc;
            int ret = decode(readString(conn_fd), peer_desc);
            if (ret) {
                PLOG(ERROR) << "Failed to receive handshake message: malformed "
                               "json format, check tcp connection";
                close(conn_fd);
                continue;
            }

            on_receive_handshake(peer_desc, local_desc);
            ret = writeString(conn_fd, encode(local_desc));
            if (ret) {
                PLOG(ERROR) << "Failed to send handshake message: malformed "
                               "json format, check tcp connection";
                close(conn_fd);
                continue;
            }

            close(conn_fd);
        }
        return;
    });

    return 0;
}

int TransferMetadata::sendHandshake(const std::string &peer_server_name,
                                    const HandShakeDesc &local_desc,
                                    HandShakeDesc &peer_desc) {
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    RpcMetaDesc desc;
    if (getRpcMetaEntry(peer_server_name, desc)) {
        PLOG(ERROR) << "Cannot find rpc meta entry for " << peer_server_name;
        return ERR_METADATA;
    }

    char service[16];
    sprintf(service, "%u", desc.rpc_port);
    if (getaddrinfo(desc.ip_or_host_name.c_str(), service, &hints, &result)) {
        PLOG(ERROR)
            << "Failed to get IP address of peer server " << peer_server_name
            << ", check DNS and /etc/hosts, or use IPv4 address instead";
        return ERR_DNS;
    }

    int ret = 0;
    for (rp = result; rp; rp = rp->ai_next) {
        ret = doSendHandshake(rp, local_desc, peer_desc);
        if (ret == 0) {
            freeaddrinfo(result);
            return 0;
        }
        if (ret == ERR_MALFORMED_JSON) {
            return ret;
        }
    }

    freeaddrinfo(result);
    return ret;
}

int TransferMetadata::doSendHandshake(struct addrinfo *addr,
                                      const HandShakeDesc &local_desc,
                                      HandShakeDesc &peer_desc) {
    if (globalConfig().verbose)
        LOG(INFO) << "Try connecting " << toString(addr->ai_addr);

    int on = 1;
    int conn_fd = socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
    if (conn_fd == -1) {
        PLOG(ERROR) << "Failed to create socket";
        return ERR_SOCKET;
    }
    if (setsockopt(conn_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
        PLOG(ERROR) << "Failed to set address reusable";
        close(conn_fd);
        return ERR_SOCKET;
    }

    struct timeval timeout;
    timeout.tv_sec = 60;
    timeout.tv_usec = 0;
    if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                   sizeof(timeout))) {
        PLOG(ERROR) << "Failed to set socket timeout";
        close(conn_fd);
        return ERR_SOCKET;
    }

    if (connect(conn_fd, addr->ai_addr, addr->ai_addrlen)) {
        PLOG(ERROR) << "Failed to connect " << toString(addr->ai_addr)
                    << " via socket";
        close(conn_fd);
        return ERR_SOCKET;
    }

    int ret = writeString(conn_fd, encode(local_desc));
    if (ret) {
        LOG(ERROR) << "Failed to send handshake message: malformed json "
                      "format, check tcp connection";
        close(conn_fd);
        return ret;
    }

    ret = decode(readString(conn_fd), peer_desc);
    if (ret) {
        LOG(ERROR) << "Failed to receive handshake message: malformed json "
                      "format, check tcp connection";
        close(conn_fd);
        return ret;
    }

    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "Handshake request is rejected by peer endpoint "
                   << peer_desc.local_nic_path
                   << ", message: " << peer_desc.reply_msg
                   << ". Please check peer's configuration.";
        close(conn_fd);
        return ERR_REJECT_HANDSHAKE;
    }

    close(conn_fd);
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
        LOG(ERROR)
            << "Malformed format of NIC priority matrix: illegal JSON format";
        return ERR_MALFORMED_JSON;
    }

    if (!root.isObject()) {
        LOG(ERROR)
            << "Malformed format of NIC priority matrix: root is not an object";
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
            LOG(ERROR)
                << "Malformed format of NIC priority matrix: format error";
            return ERR_MALFORMED_JSON;
        }
    }

    rnic_list.clear();
    for (auto &entry : rnic_set) rnic_list.push_back(entry);

    return 0;
}

}  // namespace mooncake