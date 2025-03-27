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

const static std::string kCommonKeyPrefix = "mooncake_ngxl/";
const static std::string kSegmentPrefix = kCommonKeyPrefix + "segments/";
const static std::string kRpcMetaPrefix = kCommonKeyPrefix + "rpc_meta/";

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
        if (globalConfig().verbose) {
            LOG(INFO) << "TransferHandshakeUtil::decode: local_nic_path "
                      << desc.local_nic_path << " peer_nic_path "
                      << desc.peer_nic_path << " qp_num count "
                      << desc.qp_num.size();
        }
        return 0;
    }
};

TransferMetadata::TransferMetadata(const std::string &conn_string,
                                   const std::string &local_segment_name) {
    handshake_plugin_ = HandShakePlugin::Create(conn_string);
    storage_plugin_ = MetadataStoragePlugin::Create(conn_string);
    if (!handshake_plugin_ || !storage_plugin_) {
        LOG(ERROR) << "Unable to create metadata plugins with conn string "
                   << conn_string;
    }
    SegmentCache::Callbacks callbacks;
    callbacks.on_get_segment_desc = std::bind(&TransferMetadata::getSegmentDesc,
                                              this, std::placeholders::_1);
    callbacks.on_put_segment_desc = std::bind(&TransferMetadata::putSegmentDesc,
                                              this, std::placeholders::_1);
    callbacks.on_delete_segment_desc = std::bind(
        &TransferMetadata::deleteSegmentDesc, this, std::placeholders::_1);
    cache_ = std::make_shared<SegmentCache>(callbacks, local_segment_name);
}

TransferMetadata::~TransferMetadata() {
    cache_.reset();
    handshake_plugin_.reset();
    storage_plugin_.reset();
}

SegmentCache::SegmentDescRef TransferMetadata::getSegmentDesc(
    const std::string &segment_name) {
    Json::Value segmentJSON;
    if (!storage_plugin_->get(kSegmentPrefix + segment_name, segmentJSON)) {
        LOG(ERROR) << "Failed to get segment attr for " << segment_name;
        return nullptr;
    }
    auto desc = std::make_shared<SegmentDesc>();
    int ret = SegmentDescUtils::decode(segmentJSON, *desc.get());
    if (ret) {
        LOG(ERROR) << "Failed to decode segment attr json for " << segment_name;
        return nullptr;
    }
    return desc;
}

Status TransferMetadata::putSegmentDesc(SegmentCache::SegmentDescRef segment) {
    Json::Value segmentJSON = SegmentDescUtils::encode(*segment);
    auto segment_name = segment->name;
    if (segmentJSON.empty()) {
        LOG(ERROR) << "Failed to encode segment attr json for " << segment_name;
        return Status::Metadata("failed to encode segment attr");
    }
    if (!storage_plugin_->set(kSegmentPrefix + segment_name, segmentJSON)) {
        LOG(ERROR) << "Failed to set segment attr for " << segment_name;
        return Status::Metadata("failed to set segment attr");
    }
    return Status::OK();
}

Status TransferMetadata::deleteSegmentDesc(const std::string &segment_name) {
    if (!storage_plugin_->remove(kSegmentPrefix + segment_name)) {
        LOG(ERROR) << "Failed to delete segment attr for " << segment_name;
        return Status::Metadata("failed to set segment attr");
    }
    return Status::OK();
}

int TransferMetadata::syncSegmentCache(const std::string &segment_name) {
    auto segment_id = cache_->open(segment_name);
    if (segment_id == InvalidSegmentID) return ERR_INVALID_ARGUMENT;
    cache_->get(segment_id, true);
    cache_->close(segment_id);
    return 0;
}

std::shared_ptr<SegmentDesc> TransferMetadata::getSegmentDescByName(
    const std::string &segment_name, bool force_update) {
    SegmentID segment_id = cache_->open(segment_name);
    if (segment_id == InvalidSegmentID) return nullptr;
    return cache_->get(segment_id, force_update);
}

std::shared_ptr<SegmentDesc> TransferMetadata::getSegmentDescByID(
    SegmentID segment_id, bool force_update) {
    return cache_->get(segment_id, force_update);
}

TransferMetadata::SegmentID TransferMetadata::getSegmentID(
    const std::string &segment_name) {
    return cache_->open(segment_name);
}

int TransferMetadata::updateLocalSegmentDesc() {
    Status status = cache_->flushLocal();
    return (int)status.code();
}

std::shared_ptr<SegmentDesc> TransferMetadata::getLocalSegment() {
    return cache_->getLocal();
}

int TransferMetadata::setLocalSegment(std::shared_ptr<SegmentDesc> &&desc) {
    Status status = cache_->setAndFlushLocal(std::move(desc));
    return (int)status.code();
}

int TransferMetadata::addLocalMemoryBuffer(const BufferAttr &buffer_desc,
                                           bool update_metadata) {
    Status status = cache_->getLocal()->addBuffer(buffer_desc);
    if (status != Status::OK()) return (int)status.code();
    if (update_metadata) return (int)cache_->flushLocal().code();
    return 0;
}

int TransferMetadata::removeLocalMemoryBuffer(void *addr,
                                              bool update_metadata) {
    Status status = cache_->getLocal()->removeBuffer(addr);
    if (status != Status::OK()) return (int)status.code();
    if (update_metadata) return (int)cache_->flushLocal().code();
    return 0;
}

int TransferMetadata::addRpcMetaEntry(const std::string &server_name,
                                      RpcMetaDesc &desc) {
    Json::Value rpcMetaJSON;
    rpcMetaJSON["ip_or_host_name"] = desc.ip_or_host_name;
    rpcMetaJSON["rpc_port"] = static_cast<Json::UInt64>(desc.rpc_port);
    if (!storage_plugin_->set(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        LOG(ERROR) << "Failed to set location of " << server_name;
        return ERR_METADATA;
    }
    local_rpc_meta_ = desc;
    return 0;
}

int TransferMetadata::removeRpcMetaEntry(const std::string &server_name) {
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
    Json::Value rpcMetaJSON;
    if (!storage_plugin_->get(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        LOG(ERROR) << "Failed to find location of " << server_name;
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
        [on_receive_handshake](const Json::Value &peer,
                               Json::Value &local) -> int {
            HandShakeDesc local_desc, peer_desc;
            TransferHandshakeUtil::decode(peer, peer_desc);
            int ret = on_receive_handshake(peer_desc, local_desc);
            if (ret) return ret;
            local = TransferHandshakeUtil::encode(local_desc);
            return 0;
        },
        listen_port);
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