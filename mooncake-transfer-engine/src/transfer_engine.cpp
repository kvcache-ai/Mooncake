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

#ifndef USE_TENT
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include <utility>

namespace mooncake {

TransferEngine::TransferEngine(bool auto_discover)
    : impl_(std::make_shared<TransferEngineImpl>(auto_discover)) {}

TransferEngine::TransferEngine(bool auto_discover,
                               const std::vector<std::string>& filter)
    : impl_(std::make_shared<TransferEngineImpl>(auto_discover, filter)) {}

TransferEngine::~TransferEngine() { freeEngine(); }

int TransferEngine::init(const std::string& metadata_conn_string,
                         const std::string& local_server_name,
                         const std::string& ip_or_host_name,
                         uint64_t rpc_port) {
    return impl_->init(metadata_conn_string, local_server_name, ip_or_host_name,
                       rpc_port);
}

int TransferEngine::freeEngine() {
    if (impl_) {
        impl_->freeEngine();
        impl_.reset();
    }
    return 0;
}

Transport* TransferEngine::installTransport(const std::string& proto,
                                            void** args) {
    return impl_->installTransport(proto, args);
}

int TransferEngine::uninstallTransport(const std::string& proto) {
    return impl_->uninstallTransport(proto);
}

std::string TransferEngine::getLocalIpAndPort() {
    return impl_->getLocalIpAndPort();
}

int TransferEngine::getRpcPort() { return impl_->getRpcPort(); }

SegmentHandle TransferEngine::openSegment(const std::string& segment_name) {
    return impl_->openSegment(segment_name);
}

Status TransferEngine::CheckSegmentStatus(SegmentID sid) {
    return impl_->CheckSegmentStatus(sid);
}

int TransferEngine::closeSegment(SegmentHandle handle) {
    return impl_->closeSegment(handle);
}

int TransferEngine::removeLocalSegment(const std::string& segment_name) {
    return impl_->removeLocalSegment(segment_name);
}

int TransferEngine::registerLocalMemory(void* addr, size_t length,
                                        const std::string& location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    return impl_->registerLocalMemory(addr, length, location, remote_accessible,
                                      update_metadata);
}

int TransferEngine::unregisterLocalMemory(void* addr, bool update_metadata) {
    return impl_->unregisterLocalMemory(addr, update_metadata);
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    return impl_->registerLocalMemoryBatch(buffer_list, location);
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    return impl_->unregisterLocalMemoryBatch(addr_list);
}

BatchID TransferEngine::allocateBatchID(size_t batch_size) {
    return impl_->allocateBatchID(batch_size);
}

Status TransferEngine::freeBatchID(BatchID batch_id) {
    return impl_->freeBatchID(batch_id);
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    return impl_->submitTransfer(batch_id, entries);
}

Status TransferEngine::submitTransferWithNotify(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    TransferMetadata::NotifyDesc notify_msg) {
    return impl_->submitTransferWithNotify(batch_id, entries, notify_msg);
}

int TransferEngine::getNotifies(
    std::vector<TransferMetadata::NotifyDesc>& notifies) {
    return impl_->getNotifies(notifies);
}

int TransferEngine::sendNotifyByID(SegmentID target_id,
                                   TransferMetadata::NotifyDesc notify_msg) {
    return impl_->sendNotifyByID(target_id, notify_msg);
}

int TransferEngine::sendNotifyByName(std::string remote_agent,
                                     TransferMetadata::NotifyDesc notify_msg) {
    return impl_->sendNotifyByName(std::move(remote_agent), notify_msg);
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    return impl_->getTransferStatus(batch_id, task_id, status);
}

Status TransferEngine::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    return impl_->getBatchTransferStatus(batch_id, status);
}

Transport* TransferEngine::getTransport(const std::string& proto) {
    return impl_->getTransport(proto);
}

int TransferEngine::syncSegmentCache(const std::string& segment_name) {
    return impl_->syncSegmentCache(segment_name);
}

std::shared_ptr<TransferMetadata> TransferEngine::getMetadata() {
    return impl_->getMetadata();
}

bool TransferEngine::checkOverlap(void* addr, uint64_t length) {
    return impl_->checkOverlap(addr, length);
}

void TransferEngine::setAutoDiscover(bool auto_discover) {
    impl_->setAutoDiscover(auto_discover);
}

void TransferEngine::setWhitelistFilters(std::vector<std::string>&& filters) {
    impl_->setWhitelistFilters(std::move(filters));
}

int TransferEngine::numContexts() const { return impl_->numContexts(); }

std::shared_ptr<Topology> TransferEngine::getLocalTopology() {
    return impl_->getLocalTopology();
}

}  // namespace mooncake
#else
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#include "tent/common/config.h"

#include <utility>

namespace mooncake {

TransferEngine::TransferEngine(bool auto_discover) {
    if (getenv("MC_USE_TENT") || getenv("MC_USE_TEV1")) {
        use_tent_ = true;
    }
    if (!use_tent_) {
        impl_ = std::make_shared<TransferEngineImpl>(auto_discover);
    }
}

TransferEngine::TransferEngine(bool auto_discover,
                               const std::vector<std::string>& filter) {
    if (getenv("MC_USE_TENT") || getenv("MC_USE_TEV1")) {
        use_tent_ = true;
    }
    if (!use_tent_) {
        impl_ = std::make_shared<TransferEngineImpl>(auto_discover, filter);
    }
}

TransferEngine::~TransferEngine() { freeEngine(); }

static std::pair<std::string, std::string> parseConnectionStringInternal(
    const std::string& conn_string) {
    std::pair<std::string, std::string> result;
    std::string proto = "etcd";
    std::string domain;
    std::size_t pos = conn_string.find("://");

    if (pos != std::string::npos) {
        proto = conn_string.substr(0, pos);
        domain = conn_string.substr(pos + 3);
    } else if (conn_string == P2PHANDSHAKE) {
        proto = "";
        domain = P2PHANDSHAKE;
    } else {
        domain = conn_string;
    }

    result.first = proto;
    result.second = domain;
    return result;
}

int TransferEngine::init(const std::string& metadata_conn_string,
                         const std::string& local_server_name,
                         const std::string& ip_or_host_name,
                         uint64_t rpc_port) {
    if (!use_tent_) {
        return impl_->init(metadata_conn_string, local_server_name,
                           ip_or_host_name, rpc_port);
    } else {
        auto config = std::make_shared<mooncake::tent::Config>();
        if (!local_server_name.empty())
            config->set("local_segment_name", local_server_name);
        if (metadata_conn_string == P2PHANDSHAKE) {
            config->set("metadata_type", "p2p");
        } else {
            auto [type, servers] =
                parseConnectionStringInternal(metadata_conn_string);
            if (!type.empty()) config->set("metadata_type", type);
            if (!servers.empty()) config->set("metadata_servers", servers);
        }
        impl_tent_ = std::make_shared<mooncake::tent::TransferEngine>(config);
        return impl_tent_->available();
    }
}

int TransferEngine::freeEngine() {
    if (!use_tent_ && impl_) {
        impl_->freeEngine();
        impl_.reset();
    } else {
        impl_tent_.reset();
    }
    return 0;
}

Transport* TransferEngine::installTransport(const std::string& proto,
                                            void** args) {
    if (use_tent_) {
        static bool g_present = false;
        if (!g_present) {
            LOG(INFO) << "installTransport not used by TENT";
            g_present = true;
        }
        return nullptr;
    } else {
        return impl_->installTransport(proto, args);
    }
}

int TransferEngine::uninstallTransport(const std::string& proto) {
    if (use_tent_)
        return 0;
    else
        return impl_->uninstallTransport(proto);
}

std::string TransferEngine::getLocalIpAndPort() {
    if (use_tent_) {
        return impl_tent_->getRpcServerAddress() + ":" +
               std::to_string(impl_tent_->getRpcServerPort());
    } else
        return impl_->getLocalIpAndPort();
}

int TransferEngine::getRpcPort() {
    if (use_tent_) {
        return impl_tent_->getRpcServerPort();
    } else
        return impl_->getRpcPort();
}

SegmentHandle TransferEngine::openSegment(const std::string& segment_name) {
    if (use_tent_) {
        SegmentHandle handle;
        auto status = impl_tent_->openSegment(handle, segment_name);
        if (!status.ok()) return (SegmentHandle)(-1);
        return handle;
    } else
        return impl_->openSegment(segment_name);
}

Status TransferEngine::CheckSegmentStatus(SegmentID sid) {
    if (use_tent_)
        return Status::OK();
    else
        return impl_->CheckSegmentStatus(sid);
}

int TransferEngine::closeSegment(SegmentHandle handle) {
    if (use_tent_) {
        auto status = impl_tent_->closeSegment(handle);
        return (int)status.code();
    } else
        return impl_->closeSegment(handle);
}

int TransferEngine::removeLocalSegment(const std::string& segment_name) {
    if (use_tent_)
        return 0;
    else
        return impl_->removeLocalSegment(segment_name);
}

int TransferEngine::registerLocalMemory(void* addr, size_t length,
                                        const std::string& location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    if (use_tent_) {
        mooncake::tent::MemoryOptions option;
        if (!location.empty() && location != kWildcardLocation)
            option.location = location;
        auto status = impl_tent_->registerLocalMemory(addr, length, option);
        return (int)status.code();
    } else
        return impl_->registerLocalMemory(addr, length, location,
                                          remote_accessible, update_metadata);
}

int TransferEngine::unregisterLocalMemory(void* addr, bool update_metadata) {
    if (use_tent_) {
        auto status = impl_tent_->unregisterLocalMemory(addr);
        return (int)status.code();
    } else
        return impl_->unregisterLocalMemory(addr, update_metadata);
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    if (use_tent_) {
        mooncake::tent::MemoryOptions option;
        if (!location.empty() && location != kWildcardLocation)
            option.location = location;
        std::vector<void*> addr_list;
        std::vector<size_t> size_list;
        for (auto& buffer : buffer_list) {
            addr_list.push_back(buffer.addr);
            size_list.push_back(buffer.length);
        }
        auto status =
            impl_tent_->registerLocalMemory(addr_list, size_list, option);
        return (int)status.code();
    } else {
        return impl_->registerLocalMemoryBatch(buffer_list, location);
    }
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    if (use_tent_) {
        auto status = impl_tent_->unregisterLocalMemory(addr_list);
        return (int)status.code();
    } else {
        return impl_->unregisterLocalMemoryBatch(addr_list);
    }
}

BatchID TransferEngine::allocateBatchID(size_t batch_size) {
    if (use_tent_) {
        return impl_tent_->allocateBatch(batch_size);
    } else {
        return impl_->allocateBatchID(batch_size);
    }
}

Status TransferEngine::freeBatchID(BatchID batch_id) {
    if (use_tent_) {
        auto status = impl_tent_->freeBatch(batch_id);
        if (!status.ok())
            return Status::Context(status.ToString());
        else
            return Status::OK();
    } else {
        return impl_->freeBatchID(batch_id);
    }
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    if (use_tent_) {
        std::vector<mooncake::tent::Request> requests;
        for (auto& item : entries) {
            mooncake::tent::Request req;
            req.opcode = (mooncake::tent::Request::OpCode)(int)item.opcode;
            req.length = item.length;
            req.source = item.source;
            req.target_id = item.target_id;
            req.target_offset = item.target_offset;
            requests.push_back(req);
        }
        auto status = impl_tent_->submitTransfer(batch_id, requests);
        if (!status.ok())
            return Status::Context(status.ToString());
        else
            return Status::OK();
    } else {
        return impl_->submitTransfer(batch_id, entries);
    }
}

Status TransferEngine::submitTransferWithNotify(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    TransferMetadata::NotifyDesc notify_msg) {
    if (use_tent_) {
        std::vector<mooncake::tent::Request> requests;
        for (auto& item : entries) {
            mooncake::tent::Request req;
            req.opcode = (mooncake::tent::Request::OpCode)(int)item.opcode;
            req.length = item.length;
            req.source = item.source;
            req.target_id = item.target_id;
            req.target_offset = item.target_offset;
            requests.push_back(req);
        }
        mooncake::tent::Notification notifi;
        notifi.name = notify_msg.name;
        notifi.msg = notify_msg.notify_msg;
        auto status = impl_tent_->submitTransfer(batch_id, requests, notifi);
        if (!status.ok())
            return Status::Context(status.ToString());
        else
            return Status::OK();
    } else {
        return impl_->submitTransferWithNotify(batch_id, entries, notify_msg);
    }
}

int TransferEngine::getNotifies(
    std::vector<TransferMetadata::NotifyDesc>& notifies) {
    if (use_tent_) {
        std::vector<mooncake::tent::Notification> notifi_list;
        auto status = impl_tent_->receiveNotification(notifi_list);
        for (auto& entry : notifi_list) {
            TransferMetadata::NotifyDesc desc;
            desc.name = entry.name;
            desc.notify_msg = entry.msg;
            notifies.push_back(desc);
        }
        return (int)status.code();
    } else
        return impl_->getNotifies(notifies);
}

int TransferEngine::sendNotifyByID(SegmentID target_id,
                                   TransferMetadata::NotifyDesc notify_msg) {
    if (use_tent_) {
        mooncake::tent::Notification notifi;
        notifi.name = notify_msg.name;
        notifi.msg = notify_msg.notify_msg;
        auto status = impl_tent_->sendNotification(target_id, notifi);
        return (int)status.code();
    } else
        return impl_->sendNotifyByID(target_id, notify_msg);
}

int TransferEngine::sendNotifyByName(std::string remote_agent,
                                     TransferMetadata::NotifyDesc notify_msg) {
    if (use_tent_) {
        mooncake::tent::Notification notifi;
        notifi.name = notify_msg.name;
        notifi.msg = notify_msg.notify_msg;
        SegmentHandle handle;
        auto status = impl_tent_->openSegment(handle, remote_agent);
        if (!status.ok()) return (int)status.code();
        status = impl_tent_->sendNotification(handle, notifi);
        impl_tent_->closeSegment(handle);
        return (int)status.code();
    } else
        return impl_->sendNotifyByName(std::move(remote_agent), notify_msg);
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    if (use_tent_) {
        mooncake::tent::TransferStatus tent_status;
        auto s = impl_tent_->getTransferStatus(batch_id, task_id, tent_status);
        status.s = (TransferStatusEnum)(int)tent_status.s;
        status.transferred_bytes = tent_status.transferred_bytes;
        if (!s.ok())
            return Status::Context(s.ToString());
        else
            return Status::OK();
    } else {
        return impl_->getTransferStatus(batch_id, task_id, status);
    }
}

Status TransferEngine::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    if (use_tent_) {
        mooncake::tent::TransferStatus tent_status;
        auto s = impl_tent_->getTransferStatus(batch_id, tent_status);
        status.s = (TransferStatusEnum)(int)tent_status.s;
        status.transferred_bytes = tent_status.transferred_bytes;
        if (!s.ok())
            return Status::Context(s.ToString());
        else
            return Status::OK();
    } else
        return impl_->getBatchTransferStatus(batch_id, status);
}

Transport* TransferEngine::getTransport(const std::string& proto) {
    if (use_tent_)
        return nullptr;
    else
        return impl_->getTransport(proto);
}

int TransferEngine::syncSegmentCache(const std::string& segment_name) {
    if (use_tent_)
        return 0;
    else
        return impl_->syncSegmentCache(segment_name);
}

std::shared_ptr<TransferMetadata> TransferEngine::getMetadata() {
    if (use_tent_) {
        LOG(WARNING) << "API deprecated in Mooncake TENT";
        return nullptr;
    } else
        return impl_->getMetadata();
}

bool TransferEngine::checkOverlap(void* addr, uint64_t length) {
    if (!use_tent_) return impl_->checkOverlap(addr, length);
    return false;
}

void TransferEngine::setAutoDiscover(bool auto_discover) {
    if (!use_tent_) impl_->setAutoDiscover(auto_discover);
}

void TransferEngine::setWhitelistFilters(std::vector<std::string>&& filters) {
    if (!use_tent_) impl_->setWhitelistFilters(std::move(filters));
}

int TransferEngine::numContexts() const {
    if (use_tent_)
        return 1;  // placeholder
    else
        return impl_->numContexts();
}

std::shared_ptr<Topology> TransferEngine::getLocalTopology() {
    if (use_tent_) {
        LOG(WARNING) << "API deprecated in Mooncake TENT";
        return std::make_shared<Topology>();
    } else
        return impl_->getLocalTopology();
}

}  // namespace mooncake
#endif