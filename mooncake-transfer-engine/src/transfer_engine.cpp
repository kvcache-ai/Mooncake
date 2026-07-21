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

#include <chrono>
#include <limits>
#include <thread>
#include <unordered_map>

#ifndef USE_TENT
#include "transfer_engine.h"
#include "show_links.h"
#include "transfer_engine_impl.h"
#include "graceful_shutdown.h"
#include <mutex>
#include <utility>

namespace mooncake {
namespace {

class TransferEngineShutdownToken : public ShutdownToken {
   public:
    explicit TransferEngineShutdownToken(TransferEngine* engine)
        : engine_(engine) {}

    void shutdown() override {
        TransferEngine* engine = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            engine = engine_;
            engine_ = nullptr;
        }
        if (engine) engine->freeEngine();
    }

    void detach() override {
        std::lock_guard<std::mutex> lock(mutex_);
        engine_ = nullptr;
    }

   private:
    std::mutex mutex_;
    TransferEngine* engine_;
};

std::shared_ptr<ShutdownToken> registerTransferEngineShutdownToken(
    TransferEngine* engine) {
    auto token = std::make_shared<TransferEngineShutdownToken>(engine);
    registerTokenForShutdown(token);
    return token;
}

void detachShutdownToken(std::shared_ptr<ShutdownToken>& token) {
    if (!token) return;
    token->detach();
    token.reset();
}

}  // namespace

TransferEngine::TransferEngine(bool auto_discover)
    : impl_(std::make_shared<TransferEngineImpl>(auto_discover)) {}

TransferEngine::TransferEngine(bool auto_discover,
                               const std::vector<std::string>& filter)
    : impl_(std::make_shared<TransferEngineImpl>(auto_discover, filter)) {}

TransferEngine::TransferEngine(TransferEngine&& other) noexcept
    : impl_(std::move(other.impl_)),
      impl_tent_(std::move(other.impl_tent_)),
      use_tent_(other.use_tent_) {
    const bool shutdown_enabled = static_cast<bool>(other.shutdown_token_);
    detachShutdownToken(other.shutdown_token_);
    if (shutdown_enabled) {
        shutdown_token_ = registerTransferEngineShutdownToken(this);
        installGracefulShutdownHandlers();
    }
}

TransferEngine& TransferEngine::operator=(TransferEngine&& other) noexcept {
    if (this == &other) return *this;
    freeEngine();
    impl_ = std::move(other.impl_);
    impl_tent_ = std::move(other.impl_tent_);
    use_tent_ = other.use_tent_;
    const bool shutdown_enabled = static_cast<bool>(other.shutdown_token_);
    detachShutdownToken(other.shutdown_token_);
    if (shutdown_enabled) {
        shutdown_token_ = registerTransferEngineShutdownToken(this);
        installGracefulShutdownHandlers();
    }
    return *this;
}

TransferEngine::~TransferEngine() { freeEngine(); }

int TransferEngine::init(const std::string& metadata_conn_string,
                         const std::string& local_server_name,
                         const std::string& ip_or_host_name,
                         uint64_t rpc_port) {
    return impl_->init(metadata_conn_string, local_server_name, ip_or_host_name,
                       rpc_port);
}

int TransferEngine::freeEngine() {
    detachShutdownToken(shutdown_token_);
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

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    return impl_->submitTransfer(batch_id, entries);
}

Status TransferEngine::submitTransferWithNotify(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    TransferMetadata::NotifyDesc notify_msg) {
    return impl_->submitTransferWithNotify(batch_id, entries, notify_msg);
}

#ifdef ENABLE_MULTI_PROTOCOL
// Multi-protocol API (only available when ENABLE_MULTI_PROTOCOL is defined)
int TransferEngine::mp_registerLocalMemory(
    std::unordered_map<std::string, std::vector<RegisteredBuffer>>&
        buffer_map) {
    return impl_->mp_registerLocalMemory(buffer_map);
}

int TransferEngine::mp_unregisterLocalMemory(
    std::unordered_map<std::string, std::vector<RegisteredBuffer>>&
        buffer_map) {
    return impl_->mp_unregisterLocalMemory(buffer_map);
}

Status TransferEngine::mp_submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    std::string& proto) {
    return impl_->mp_submitTransfer(batch_id, entries, proto);
}

Status TransferEngine::mp_submitTransferWithNotify(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    TransferMetadata::NotifyDesc notify_msg, std::string& proto) {
    return impl_->mp_submitTransferWithNotify(batch_id, entries, notify_msg,
                                              proto);
}
#endif

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

PeerLiveness TransferEngine::probePeerAliveByID(SegmentID target_id) {
    return impl_->probePeerAliveByID(target_id) == 0
               ? PeerLiveness::Alive
               : PeerLiveness::Unreachable;
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    return impl_->getTransferStatus(batch_id, task_id, status);
}

Status TransferEngine::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    return impl_->getBatchTransferStatus(batch_id, status);
}

Status TransferEngine::getNicLoadStats(std::vector<NicLoadStats>& stats) const {
    stats.clear();
    return Status::OK();
}

Transport* TransferEngine::getTransport(const std::string& proto) {
    return impl_->getTransport(proto);
}

#if (defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)) && \
    !defined(USE_CXI)
device::P2pTransport* TransferEngine::getOrCreateP2pTransport(int num_ranks) {
    return impl_->getOrCreateP2pTransport(num_ranks);
}

device::RdmaTransport* TransferEngine::getOrCreateRdmaTransport(
    const std::vector<std::string>& device_filter) {
    return impl_->getOrCreateRdmaTransport(device_filter);
}
#endif

bool TransferEngine::isTcpOnly() const { return impl_->isTcpOnly(); }

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

void* TransferEngine::getBaseAddr() { return impl_->getBaseAddr(); }

void TransferEngine::setWhitelistFilters(std::vector<std::string>&& filters) {
    impl_->setWhitelistFilters(std::move(filters));
}

int TransferEngine::numContexts() const { return impl_->numContexts(); }

std::shared_ptr<Topology> TransferEngine::getLocalTopology() {
    return impl_->getLocalTopology();
}

void TransferEngine::enableGracefulShutdown() {
    if (!shutdown_token_) {
        shutdown_token_ = registerTransferEngineShutdownToken(this);
    }
    installGracefulShutdownHandlers();
}

std::string TransferEngine::showLinks(bool json) const {
    if (!impl_) return "{}";
    return json ? buildShowLinksJson(impl_.get())
                : buildShowLinksReadable(impl_.get());
}

}  // namespace mooncake
#else
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include "tent/transfer_engine.h"
#include "tent/common/config.h"

#include <mutex>
#include <utility>
#include "graceful_shutdown.h"
#include "show_links.h"

namespace mooncake {
namespace {

class TransferEngineShutdownToken : public ShutdownToken {
   public:
    explicit TransferEngineShutdownToken(TransferEngine* engine)
        : engine_(engine) {}

    void shutdown() override {
        TransferEngine* engine = nullptr;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            engine = engine_;
            engine_ = nullptr;
        }
        if (engine) engine->freeEngine();
    }

    void detach() override {
        std::lock_guard<std::mutex> lock(mutex_);
        engine_ = nullptr;
    }

   private:
    std::mutex mutex_;
    TransferEngine* engine_;
};

std::shared_ptr<ShutdownToken> registerTransferEngineShutdownToken(
    TransferEngine* engine) {
    auto token = std::make_shared<TransferEngineShutdownToken>(engine);
    registerTokenForShutdown(token);
    return token;
}

void detachShutdownToken(std::shared_ptr<ShutdownToken>& token) {
    if (!token) return;
    token->detach();
    token.reset();
}

}  // namespace

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

TransferEngine::TransferEngine(TransferEngine&& other) noexcept
    : impl_(std::move(other.impl_)),
      impl_tent_(std::move(other.impl_tent_)),
      shutdown_token_(nullptr),
      use_tent_(other.use_tent_) {
    const bool shutdown_enabled = static_cast<bool>(other.shutdown_token_);
    detachShutdownToken(other.shutdown_token_);
    if (shutdown_enabled) {
        shutdown_token_ = registerTransferEngineShutdownToken(this);
        installGracefulShutdownHandlers();
    }
}

TransferEngine& TransferEngine::operator=(TransferEngine&& other) noexcept {
    if (this == &other) return *this;
    freeEngine();
    impl_ = std::move(other.impl_);
    impl_tent_ = std::move(other.impl_tent_);
    use_tent_ = other.use_tent_;
    const bool shutdown_enabled = static_cast<bool>(other.shutdown_token_);
    detachShutdownToken(other.shutdown_token_);
    if (shutdown_enabled) {
        shutdown_token_ = registerTransferEngineShutdownToken(this);
        installGracefulShutdownHandlers();
    }
    return *this;
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
        return impl_tent_->available() ? 0 : 1;
    }
}

int TransferEngine::freeEngine() {
    detachShutdownToken(shutdown_token_);
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
            req.transport_hint =
                mooncake::tent::c_to_transport_hint(item.transport_hint);
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
            req.transport_hint =
                mooncake::tent::c_to_transport_hint(item.transport_hint);
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

PeerLiveness TransferEngine::probePeerAliveByID(SegmentID target_id) {
    if (use_tent_) {
        auto status = impl_tent_->probePeerAliveByID(target_id);
        return status.ok() ? PeerLiveness::Alive : PeerLiveness::Unreachable;
    }
    return impl_->probePeerAliveByID(target_id) == 0
               ? PeerLiveness::Alive
               : PeerLiveness::Unreachable;
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

Status TransferEngine::getNicLoadStats(std::vector<NicLoadStats>& stats) const {
    stats.clear();
    if (use_tent_) {
        std::vector<mooncake::tent::NicLoadStats> tent_stats;
        auto status = impl_tent_->getNicLoadStats(tent_stats);
        if (!status.ok()) return Status::Context(status.ToString());
        stats.reserve(tent_stats.size());
        for (const auto& stat : tent_stats) {
            NicLoadStats load_stats;
            load_stats.device_name = stat.device_name;
            load_stats.inflight_bytes = stat.inflight_bytes;
            load_stats.ewma_bandwidth_bps = stat.ewma_bandwidth_bps;
            stats.push_back(load_stats);
        }
    }
    return Status::OK();
}

Transport* TransferEngine::getTransport(const std::string& proto) {
    if (use_tent_)
        return nullptr;
    else
        return impl_->getTransport(proto);
}

#if (defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)) && \
    !defined(USE_CXI)
device::P2pTransport* TransferEngine::getOrCreateP2pTransport(int num_ranks) {
    if (use_tent_) return nullptr;
    return impl_->getOrCreateP2pTransport(num_ranks);
}

device::RdmaTransport* TransferEngine::getOrCreateRdmaTransport(
    const std::vector<std::string>& device_filter) {
    if (use_tent_) return nullptr;
    return impl_->getOrCreateRdmaTransport(device_filter);
}
#endif

bool TransferEngine::isTcpOnly() const {
    if (use_tent_)
        // TENT already rejects TCP loopback transfers when MC_STORE_MEMCPY
        // is disabled, so auto-enabling memcpy is unnecessary in TENT mode.
        return false;
    else
        return impl_->isTcpOnly();
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

void* TransferEngine::getBaseAddr() {
    if (use_tent_) {
        // TENT version does not support CXL base address
        return nullptr;
    } else
        return impl_->getBaseAddr();
}

void TransferEngine::enableGracefulShutdown() {
    if (!shutdown_token_) {
        shutdown_token_ = registerTransferEngineShutdownToken(this);
    }
    installGracefulShutdownHandlers();
}

std::string TransferEngine::showLinks(bool json) const {
    if (use_tent_ || !impl_) {
        return json ? "{}" : "(TENT mode or not initialized)";
    }
    return json ? buildShowLinksJson(impl_.get())
                : buildShowLinksReadable(impl_.get());
}

}  // namespace mooncake
#endif

namespace mooncake {

Status TransferEngine::transferScatter(
    const std::vector<ScatterTransferRange>& ranges) {
    std::vector<TransferRequest> requests;
    std::vector<std::pair<size_t, size_t>> request_fragments;
    std::unordered_map<std::string, SegmentHandle> segment_cache;

    auto complete = [&](size_t range_index, size_t fragment_index,
                        const Status& status) {
        const auto& callback = ranges[range_index].on_fragment_complete;
        if (callback) callback(fragment_index, status);
    };

    for (size_t range_index = 0; range_index < ranges.size(); ++range_index) {
        const auto& range = ranges[range_index];
        const size_t fragment_count = range.local_offsets.size();
        if (range.remote_offsets.size() != fragment_count ||
            range.lengths.size() != fragment_count ||
            range.local_buffer == nullptr || range.remote_segment.empty()) {
            const auto status =
                Status::InvalidArgument("invalid scatter transfer range");
            for (size_t i = 0; i < fragment_count; ++i) {
                complete(range_index, i, status);
            }
            continue;
        }

        for (size_t fragment_index = 0; fragment_index < fragment_count;
             ++fragment_index) {
            const size_t length = range.lengths[fragment_index];
            const size_t local_offset = range.local_offsets[fragment_index];
            const size_t remote_offset = range.remote_offsets[fragment_index];
            if (local_offset > range.local_capacity ||
                length > range.local_capacity - local_offset ||
                remote_offset > range.remote_size ||
                length > range.remote_size - remote_offset ||
                range.remote_base_offset >
                    std::numeric_limits<uint64_t>::max() - remote_offset ||
                length > std::numeric_limits<uint64_t>::max() -
                             (range.remote_base_offset + remote_offset)) {
                complete(range_index, fragment_index,
                         Status::InvalidArgument(
                             "invalid scatter transfer fragment"));
                continue;
            }

            if (length == 0) {
                complete(range_index, fragment_index, Status::OK());
                continue;
            }

            auto [seg_it, inserted] = segment_cache.emplace(
                range.remote_segment,
                static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT));
            if (inserted) seg_it->second = openSegment(range.remote_segment);
            if (seg_it->second ==
                static_cast<SegmentHandle>(ERR_INVALID_ARGUMENT)) {
                complete(range_index, fragment_index,
                         Status::InvalidArgument(
                             "failed to open scatter transfer segment"));
                continue;
            }

            requests.push_back(TransferRequest{
                .opcode = range.opcode,
                .source = static_cast<char*>(range.local_buffer) + local_offset,
                .target_id = seg_it->second,
                .target_offset = range.remote_base_offset + remote_offset,
                .length = length,
            });
            request_fragments.emplace_back(range_index, fragment_index);
        }
    }

    if (requests.empty()) return Status::OK();

    std::vector<uint8_t> terminated(requests.size(), false);
    auto fail_pending = [&](const Status& status) {
        for (size_t i = 0; i < request_fragments.size(); ++i) {
            if (terminated[i]) continue;
            const auto [range_index, fragment_index] = request_fragments[i];
            complete(range_index, fragment_index, status);
        }
    };

    BatchID batch_id = allocateBatchID(requests.size());
    if (batch_id == INVALID_BATCH_ID) {
        Status status = Status::InvalidArgument(
            "failed to allocate scatter transfer batch");
        fail_pending(status);
        return status;
    }
    auto abort = [&](Status status) {
        fail_pending(status);
        freeBatchID(batch_id);
        return status;
    };
    Status result = submitTransfer(batch_id, requests);
    if (!result.ok()) return abort(result);

    constexpr auto timeout = std::chrono::seconds(60);
    auto start = std::chrono::steady_clock::now();
    size_t remaining = requests.size();
    bool has_failure = false;
    while (remaining > 0) {
        for (size_t i = 0; i < requests.size(); ++i) {
            if (terminated[i]) continue;
            TransferStatus status;
            result = getTransferStatus(batch_id, i, status);
            if (!result.ok()) return abort(result);

            const auto [range_index, fragment_index] = request_fragments[i];
            if (status.s == TransferStatusEnum::COMPLETED) {
                complete(range_index, fragment_index, Status::OK());
                terminated[i] = true;
                --remaining;
            } else if (status.s != TransferStatusEnum::WAITING &&
                       status.s != TransferStatusEnum::PENDING) {
                complete(range_index, fragment_index,
                         Status::Socket("scatter transfer fragment failed"));
                terminated[i] = true;
                --remaining;
                has_failure = true;
            }
        }
        if (remaining == 0) break;
        if (std::chrono::steady_clock::now() - start > timeout) {
            return abort(Status::Socket("scatter transfer timed out"));
        }
        std::this_thread::yield();
    }
    freeBatchID(batch_id);
    return has_failure ? Status::Socket("scatter transfer failed")
                       : Status::OK();
}

}  // namespace mooncake
