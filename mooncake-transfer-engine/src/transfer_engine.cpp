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
#include "transfer_metadata_plugin.h"
#include "tent/transfer_engine.h"
#include "tent/common/config.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <mutex>
#include <utility>
#include <unistd.h>
#include "error.h"
#include "graceful_shutdown.h"
#include "show_links.h"

namespace mooncake {
namespace {

mooncake::tent::MemoryOptions makeTentMemoryOptions(const std::string& location,
                                                    bool remote_accessible,
                                                    bool update_metadata) {
    mooncake::tent::MemoryOptions option;
    option.perm = remote_accessible ? mooncake::tent::kGlobalReadWrite
                                    : mooncake::tent::kLocalReadWrite;
    option.internal = !update_metadata;
    if (!location.empty() && location != kWildcardLocation) {
        option.location = location;
    }
    return option;
}

bool rangesOverlap(uintptr_t lhs_base, uint64_t lhs_length, uintptr_t rhs_base,
                   uint64_t rhs_length) {
    if (lhs_length == 0 || rhs_length == 0) return false;
    const auto max_addr = std::numeric_limits<uintptr_t>::max();
    const uintptr_t lhs_end =
        lhs_length > max_addr - lhs_base ? max_addr : lhs_base + lhs_length;
    const uintptr_t rhs_end =
        rhs_length > max_addr - rhs_base ? max_addr : rhs_base + rhs_length;
    return lhs_base < rhs_end && rhs_base < lhs_end;
}

Status tentStatusToClassicStatus(const mooncake::tent::Status& status) {
    if (status.ok()) return Status::OK();
    const auto msg = status.ToString();
    switch (status.code()) {
        case mooncake::tent::Status::Code::kInvalidArgument:
        case mooncake::tent::Status::Code::kInvalidEntry:
            return Status::InvalidArgument(msg);
        case mooncake::tent::Status::Code::kTooManyRequests:
            return Status::TooManyRequests(msg);
        case mooncake::tent::Status::Code::kAddressNotRegistered:
            return Status::AddressNotRegistered(msg);
        case mooncake::tent::Status::Code::kDeviceNotFound:
            return Status::DeviceNotFound(msg);
        case mooncake::tent::Status::Code::kInvalidMetadataType:
        case mooncake::tent::Status::Code::kMetadataError:
            return Status::Metadata(msg);
        case mooncake::tent::Status::Code::kMalformedJson:
            return Status::MalformedJson(msg);
        case mooncake::tent::Status::Code::kNotImplemented:
            return Status::NotImplemented(msg);
        case mooncake::tent::Status::Code::kNeedsRefreshCache:
        case mooncake::tent::Status::Code::kRdmaError:
        case mooncake::tent::Status::Code::kCudaError:
        case mooncake::tent::Status::Code::kRpcServiceError:
        case mooncake::tent::Status::Code::kInternalError:
        default:
            return Status::Context(msg);
    }
}

int tentStatusToClassicReturn(const mooncake::tent::Status& status) {
    if (status.ok()) return 0;
    switch (status.code()) {
        case mooncake::tent::Status::Code::kInvalidArgument:
        case mooncake::tent::Status::Code::kInvalidEntry:
            return ERR_INVALID_ARGUMENT;
        case mooncake::tent::Status::Code::kTooManyRequests:
            return ERR_TOO_MANY_REQUESTS;
        case mooncake::tent::Status::Code::kAddressNotRegistered:
            return ERR_ADDRESS_NOT_REGISTERED;
        case mooncake::tent::Status::Code::kDeviceNotFound:
            return ERR_DEVICE_NOT_FOUND;
        case mooncake::tent::Status::Code::kNotImplemented:
            return ERR_NOT_IMPLEMENTED;
        case mooncake::tent::Status::Code::kInvalidMetadataType:
        case mooncake::tent::Status::Code::kMetadataError:
            return ERR_METADATA;
        case mooncake::tent::Status::Code::kMalformedJson:
            return ERR_MALFORMED_JSON;
        case mooncake::tent::Status::Code::kRdmaError:
        case mooncake::tent::Status::Code::kCudaError:
            return ERR_MEMORY;
        case mooncake::tent::Status::Code::kRpcServiceError:
        case mooncake::tent::Status::Code::kInternalError:
        case mooncake::tent::Status::Code::kNeedsRefreshCache:
        default:
            return ERR_CONTEXT;
    }
}

bool containsOverlappingRange(
    const std::vector<std::pair<uintptr_t, uint64_t>>& ranges, uintptr_t base,
    uint64_t length) {
    return std::any_of(
        ranges.begin(), ranges.end(), [base, length](const auto& range) {
            return rangesOverlap(base, length, range.first, range.second);
        });
}

bool containsRegisteredBase(
    const std::vector<std::pair<uintptr_t, uint64_t>>& ranges, uintptr_t base) {
    return std::any_of(ranges.begin(), ranges.end(), [base](const auto& range) {
        return range.first == base;
    });
}

bool batchHasInvalidOrOverlappingRanges(
    const std::vector<BufferEntry>& buffer_list) {
    for (size_t i = 0; i < buffer_list.size(); ++i) {
        if (buffer_list[i].length == 0) return true;
        const auto base = reinterpret_cast<uintptr_t>(buffer_list[i].addr);
        for (size_t j = i + 1; j < buffer_list.size(); ++j) {
            if (rangesOverlap(base, buffer_list[i].length,
                              reinterpret_cast<uintptr_t>(buffer_list[j].addr),
                              buffer_list[j].length)) {
                return true;
            }
        }
    }
    return false;
}

bool protoToTentTransportHint(const std::string& proto,
                              mooncake::tent::TransportType& hint) {
    if (proto.empty()) {
        hint = mooncake::tent::UNSPEC;
        return true;
    }
    if (proto == "rdma" || proto == "barex") {
        hint = mooncake::tent::RDMA;
    } else if (proto == "mnnvl") {
        hint = mooncake::tent::MNNVL;
    } else if (proto == "shm") {
        hint = mooncake::tent::SHM;
    } else if (proto == "nvlink" || proto == "nvlink_intra") {
        hint = mooncake::tent::NVLINK;
    } else if (proto == "gds") {
        hint = mooncake::tent::GDS;
    } else if (proto == "io_uring" || proto == "iouring") {
        hint = mooncake::tent::IOURING;
    } else if (proto == "tcp") {
        hint = mooncake::tent::TCP;
    } else if (proto == "ascend_direct" || proto == "ascend") {
        hint = mooncake::tent::AscendDirect;
    } else if (proto == "sunrise_link") {
        hint = mooncake::tent::SUNRISE_LINK;
    } else if (proto == "tpu") {
        hint = mooncake::tent::TPU;
    } else {
        return false;
    }
    return true;
}

std::vector<std::string> extractRdmaWhitelistFromInstallArgs(void** args) {
    std::vector<std::string> devices;
    if (!args || !args[0]) return devices;

    auto parsed = mooncake::tent::json::parse(
        static_cast<const char*>(args[0]), nullptr, /*allow_exceptions=*/false);
    if (parsed.is_discarded() || !parsed.is_object()) return devices;

    for (auto& entry : parsed.items()) {
        const auto& location_rules = entry.value();
        if (!location_rules.is_array() || location_rules.empty() ||
            !location_rules[0].is_array()) {
            continue;
        }
        for (const auto& item : location_rules[0]) {
            if (!item.is_string()) continue;
            auto device = item.get<std::string>();
            if (std::find(devices.begin(), devices.end(), device) ==
                devices.end()) {
                devices.push_back(std::move(device));
            }
        }
    }
    return devices;
}

bool configureLegacyP2pIdentity(mooncake::tent::Config& config,
                                const std::string& local_server_name) {
    auto [host_name, ignored_port] = parseHostNameWithPort(local_server_name);
    (void)ignored_port;
    int sockfd = -1;
    const auto port = findAvailableTcpPort(sockfd);
    if (port == 0) {
        LOG(ERROR) << "P2P: No valid port found for local TENT RPC service.";
        return false;
    }
    if (sockfd >= 0) close(sockfd);

    config.set("metadata_type", "p2p");
    config.set("rpc_server_hostname", host_name);
    config.set("rpc_server_port", port);
    config.set("local_segment_name",
               maybeWrapIpV6(host_name) + ":" + std::to_string(port));
    return true;
}

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
    } else {
        tent_whitelist_filters_ = filter;
    }
}

TransferEngine::TransferEngine(TransferEngine&& other) noexcept
    : impl_(std::move(other.impl_)),
      impl_tent_(std::move(other.impl_tent_)),
      shutdown_token_(nullptr),
      tent_whitelist_filters_(std::move(other.tent_whitelist_filters_)),
      tent_registered_ranges_(std::move(other.tent_registered_ranges_)),
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
    tent_whitelist_filters_ = std::move(other.tent_whitelist_filters_);
    tent_registered_ranges_ = std::move(other.tent_registered_ranges_);
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
        if (!local_server_name.empty() && metadata_conn_string != P2PHANDSHAKE)
            config->set("local_segment_name", local_server_name);
        if (!ip_or_host_name.empty() && metadata_conn_string != P2PHANDSHAKE)
            config->set("rpc_server_hostname", ip_or_host_name);
        if (metadata_conn_string != P2PHANDSHAKE)
            config->set("rpc_server_port", rpc_port);
        std::vector<std::string> whitelist_filters;
        {
            std::lock_guard<std::mutex> lock(tent_compat_mutex_);
            whitelist_filters = tent_whitelist_filters_;
        }
        if (!whitelist_filters.empty()) {
            config->set("topology/rdma_whitelist", whitelist_filters);
        }
        if (metadata_conn_string == P2PHANDSHAKE) {
            if (!configureLegacyP2pIdentity(*config, local_server_name))
                return ERR_CONTEXT;
        } else {
            auto [type, servers] =
                parseConnectionStringInternal(metadata_conn_string);
            if (!type.empty()) config->set("metadata_type", type);
            if (!servers.empty()) config->set("metadata_servers", servers);
        }
        impl_tent_ = std::make_shared<mooncake::tent::TransferEngine>(config);
        if (!impl_tent_->available()) {
            impl_tent_.reset();
            return ERR_CONTEXT;
        }
        return 0;
    }
}

int TransferEngine::freeEngine() {
    detachShutdownToken(shutdown_token_);
    if (!use_tent_ && impl_) {
        impl_->freeEngine();
        impl_.reset();
    } else {
        impl_tent_.reset();
        std::lock_guard<std::mutex> lock(tent_compat_mutex_);
        tent_registered_ranges_.clear();
    }
    return 0;
}

Transport* TransferEngine::installTransport(const std::string& proto,
                                            void** args) {
    if (use_tent_) {
        mooncake::tent::TransportType ignored;
        if (!protoToTentTransportHint(proto, ignored)) {
            LOG(ERROR) << "installTransport(" << proto
                       << ") is not supported by TENT compatibility mode";
            return nullptr;
        }
        if ((proto == "rdma" || proto == "barex") && args && args[0]) {
            auto filters = extractRdmaWhitelistFromInstallArgs(args);
            if (!filters.empty() && !impl_tent_) {
                std::lock_guard<std::mutex> lock(tent_compat_mutex_);
                tent_whitelist_filters_ = std::move(filters);
            } else if (!filters.empty()) {
                LOG(WARNING)
                    << "TENT is already initialized; installTransport(" << proto
                    << ") NIC hints cannot change the active TENT topology";
            }
        }
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
    if (use_tent_) {
        mooncake::tent::TransportType ignored;
        if (!protoToTentTransportHint(proto, ignored)) {
            LOG(WARNING) << "uninstallTransport(" << proto
                         << ") ignored by TENT compatibility mode";
        }
        return 0;
    } else
        return impl_->uninstallTransport(proto);
}

std::string TransferEngine::getLocalIpAndPort() {
    if (use_tent_) {
        if (!impl_tent_) return "";
        return impl_tent_->getRpcServerAddress() + ":" +
               std::to_string(impl_tent_->getRpcServerPort());
    } else
        return impl_->getLocalIpAndPort();
}

int TransferEngine::getRpcPort() {
    if (use_tent_) {
        if (!impl_tent_) return 0;
        return impl_tent_->getRpcServerPort();
    } else
        return impl_->getRpcPort();
}

SegmentHandle TransferEngine::openSegment(const std::string& segment_name) {
    if (use_tent_) {
        if (!impl_tent_) return static_cast<SegmentHandle>(-1);
        SegmentHandle handle;
        auto status = impl_tent_->openSegment(handle, segment_name);
        if (!status.ok()) return (SegmentHandle)(-1);
        return handle;
    } else
        return impl_->openSegment(segment_name);
}

Status TransferEngine::getSegmentBufferBase(SegmentHandle handle,
                                            size_t buffer_index,
                                            uint64_t& base) {
    if (use_tent_) {
        if (!impl_tent_)
            return Status::Context("TENT engine is not initialized");
        mooncake::tent::SegmentInfo info;
        auto status = impl_tent_->getSegmentInfo(handle, info);
        if (!status.ok()) return tentStatusToClassicStatus(status);
        if (buffer_index >= info.buffers.size()) {
            return Status::InvalidArgument(
                "Segment buffer index out of range in TENT mode");
        }
        base = info.buffers[buffer_index].base;
        return Status::OK();
    }

    auto metadata = impl_->getMetadata();
    if (!metadata) {
        return Status::Metadata("Transfer metadata is not available");
    }
    auto segment_desc = metadata->getSegmentDescByID(handle);
    if (!segment_desc) {
        return Status::Metadata("Unable to get target segment descriptor");
    }
    if (buffer_index >= segment_desc->buffers.size()) {
        return Status::InvalidArgument("Segment buffer index out of range");
    }
    base = segment_desc->buffers[buffer_index].addr;
    return Status::OK();
}

Status TransferEngine::CheckSegmentStatus(SegmentID sid) {
    if (use_tent_) {
        if (!impl_tent_)
            return Status::Context("TENT engine is not initialized");
        mooncake::tent::SegmentInfo info;
        auto status = impl_tent_->getSegmentInfo(sid, info);
        return tentStatusToClassicStatus(status);
    } else
        return impl_->CheckSegmentStatus(sid);
}

int TransferEngine::closeSegment(SegmentHandle handle) {
    if (use_tent_) {
        if (!impl_tent_) return ERR_CONTEXT;
        auto status = impl_tent_->closeSegment(handle);
        return tentStatusToClassicReturn(status);
    } else
        return impl_->closeSegment(handle);
}

int TransferEngine::removeLocalSegment(const std::string& segment_name) {
    if (use_tent_) {
        LOG(WARNING)
            << "removeLocalSegment is not supported by TENT facade for "
            << segment_name;
        return -1;
    } else
        return impl_->removeLocalSegment(segment_name);
}

int TransferEngine::registerLocalMemory(void* addr, size_t length,
                                        const std::string& location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    if (use_tent_) {
        if (!impl_tent_) return ERR_CONTEXT;
        if (length == 0) {
            LOG(ERROR)
                << "Transfer Engine does not support zero length memory region";
            return ERR_INVALID_ARGUMENT;
        }
        const auto base = reinterpret_cast<uintptr_t>(addr);
        auto option =
            makeTentMemoryOptions(location, remote_accessible, update_metadata);
        {
            std::lock_guard<std::mutex> lock(tent_compat_mutex_);
            if (containsOverlappingRange(tent_registered_ranges_, base,
                                         length)) {
                LOG(ERROR)
                    << "Transfer Engine does not support overlapped memory "
                       "region";
                return ERR_ADDRESS_OVERLAPPED;
            }
            auto status = impl_tent_->registerLocalMemory(addr, length, option);
            if (status.ok()) {
                tent_registered_ranges_.push_back({base, length});
            }
            return tentStatusToClassicReturn(status);
        }
    } else
        return impl_->registerLocalMemory(addr, length, location,
                                          remote_accessible, update_metadata);
}

int TransferEngine::unregisterLocalMemory(void* addr, bool update_metadata) {
    if (use_tent_) {
        if (!impl_tent_) return ERR_CONTEXT;
        auto base = reinterpret_cast<uintptr_t>(addr);
        {
            std::lock_guard<std::mutex> lock(tent_compat_mutex_);
            if (!containsRegisteredBase(tent_registered_ranges_, base)) {
                return ERR_ADDRESS_NOT_REGISTERED;
            }
        }
        auto status = impl_tent_->unregisterLocalMemory(addr);
        if (status.ok()) {
            std::lock_guard<std::mutex> lock(tent_compat_mutex_);
            auto it = std::find_if(
                tent_registered_ranges_.begin(), tent_registered_ranges_.end(),
                [base](const auto& range) { return range.first == base; });
            if (it != tent_registered_ranges_.end()) {
                tent_registered_ranges_.erase(it);
            }
        }
        return tentStatusToClassicReturn(status);
    } else
        return impl_->unregisterLocalMemory(addr, update_metadata);
}

int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry>& buffer_list, const std::string& location) {
    if (use_tent_) {
        if (!impl_tent_) return ERR_CONTEXT;
        if (batchHasInvalidOrOverlappingRanges(buffer_list)) {
            for (const auto& buffer : buffer_list) {
                if (buffer.length == 0) {
                    LOG(ERROR)
                        << "Transfer Engine does not support zero length "
                           "memory region";
                    return ERR_INVALID_ARGUMENT;
                }
            }
            LOG(ERROR)
                << "Transfer Engine does not support overlapped memory region";
            return ERR_ADDRESS_OVERLAPPED;
        }
        auto option = makeTentMemoryOptions(
            location, /*remote_accessible=*/true, /*update_metadata=*/true);
        std::vector<void*> addr_list;
        std::vector<size_t> size_list;
        for (auto& buffer : buffer_list) {
            addr_list.push_back(buffer.addr);
            size_list.push_back(buffer.length);
        }
        {
            std::lock_guard<std::mutex> lock(tent_compat_mutex_);
            for (const auto& buffer : buffer_list) {
                if (containsOverlappingRange(
                        tent_registered_ranges_,
                        reinterpret_cast<uintptr_t>(buffer.addr),
                        buffer.length)) {
                    LOG(ERROR) << "Transfer Engine does not support overlapped "
                                  "memory region";
                    return ERR_ADDRESS_OVERLAPPED;
                }
            }
            auto status =
                impl_tent_->registerLocalMemory(addr_list, size_list, option);
            if (status.ok()) {
                for (auto& buffer : buffer_list) {
                    tent_registered_ranges_.push_back(
                        {reinterpret_cast<uintptr_t>(buffer.addr),
                         buffer.length});
                }
            }
            return tentStatusToClassicReturn(status);
        }
    } else {
        return impl_->registerLocalMemoryBatch(buffer_list, location);
    }
}

int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void*>& addr_list) {
    if (use_tent_) {
        if (!impl_tent_) return ERR_CONTEXT;
        {
            std::lock_guard<std::mutex> lock(tent_compat_mutex_);
            for (auto* addr : addr_list) {
                if (!containsRegisteredBase(
                        tent_registered_ranges_,
                        reinterpret_cast<uintptr_t>(addr))) {
                    return ERR_ADDRESS_NOT_REGISTERED;
                }
            }
        }
        auto status = impl_tent_->unregisterLocalMemory(addr_list);
        if (status.ok()) {
            std::lock_guard<std::mutex> lock(tent_compat_mutex_);
            for (auto* addr : addr_list) {
                auto base = reinterpret_cast<uintptr_t>(addr);
                auto it = std::find_if(
                    tent_registered_ranges_.begin(),
                    tent_registered_ranges_.end(),
                    [base](const auto& range) { return range.first == base; });
                if (it != tent_registered_ranges_.end()) {
                    tent_registered_ranges_.erase(it);
                }
            }
        }
        return tentStatusToClassicReturn(status);
    } else {
        return impl_->unregisterLocalMemoryBatch(addr_list);
    }
}

BatchID TransferEngine::allocateBatchID(size_t batch_size) {
    if (use_tent_) {
        if (!impl_tent_) return INVALID_BATCH_ID;
        auto batch_id = impl_tent_->allocateBatch(batch_size);
        return batch_id == 0 ? INVALID_BATCH_ID : batch_id;
    } else {
        return impl_->allocateBatchID(batch_size);
    }
}

Status TransferEngine::freeBatchID(BatchID batch_id) {
    if (use_tent_) {
        if (!impl_tent_)
            return Status::Context("TENT engine is not initialized");
        if (batch_id == INVALID_BATCH_ID) {
            return Status::InvalidArgument("Invalid batch ID");
        }
        auto status = impl_tent_->freeBatch(batch_id);
        return tentStatusToClassicStatus(status);
    } else {
        return impl_->freeBatchID(batch_id);
    }
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    if (use_tent_) {
        if (!impl_tent_)
            return Status::Context("TENT engine is not initialized");
        if (entries.empty()) {
            return Status::InvalidArgument("entries must not be empty");
        }
        if (batch_id == INVALID_BATCH_ID) {
            return Status::InvalidArgument("Invalid batch ID");
        }
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
        return tentStatusToClassicStatus(status);
    } else {
        return impl_->submitTransfer(batch_id, entries);
    }
}

Status TransferEngine::submitTransferWithNotify(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    TransferMetadata::NotifyDesc notify_msg) {
    if (use_tent_) {
        if (!impl_tent_)
            return Status::Context("TENT engine is not initialized");
        if (entries.empty()) {
            return Status::InvalidArgument("entries must not be empty");
        }
        if (batch_id == INVALID_BATCH_ID) {
            return Status::InvalidArgument("Invalid batch ID");
        }
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
        return tentStatusToClassicStatus(status);
    } else {
        return impl_->submitTransferWithNotify(batch_id, entries, notify_msg);
    }
}

#ifdef ENABLE_MULTI_PROTOCOL
int TransferEngine::mp_registerLocalMemory(
    std::unordered_map<std::string, std::vector<RegisteredBuffer>>&
        buffer_map) {
    if (!use_tent_) return impl_->mp_registerLocalMemory(buffer_map);
    if (!impl_tent_) return ERR_CONTEXT;

    std::vector<RegisteredBuffer> unique_buffers;
    for (auto& entry : buffer_map) {
        mooncake::tent::TransportType ignored;
        if (!protoToTentTransportHint(entry.first, ignored)) {
            return ERR_INVALID_ARGUMENT;
        }
        for (auto& buffer : entry.second) {
            bool duplicate = false;
            for (const auto& existing : unique_buffers) {
                if (existing.addr == buffer.addr &&
                    existing.length == buffer.length) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) unique_buffers.push_back(buffer);
        }
    }

    std::vector<void*> registered_addrs;
    for (auto& buffer : unique_buffers) {
        int ret = registerLocalMemory(buffer.addr, buffer.length,
                                      buffer.location, buffer.remote_accessible,
                                      buffer.update_metadata);
        if (ret != 0) {
            for (auto it = registered_addrs.rbegin();
                 it != registered_addrs.rend(); ++it) {
                int rollback_ret = unregisterLocalMemory(*it);
                if (rollback_ret != 0 &&
                    rollback_ret != ERR_ADDRESS_NOT_REGISTERED) {
                    LOG(WARNING)
                        << "Failed to roll back TENT mp registration, ret="
                        << rollback_ret;
                }
            }
            return ret;
        }
        registered_addrs.push_back(buffer.addr);
    }
    return 0;
}

int TransferEngine::mp_unregisterLocalMemory(
    std::unordered_map<std::string, std::vector<RegisteredBuffer>>&
        buffer_map) {
    if (!use_tent_) return impl_->mp_unregisterLocalMemory(buffer_map);
    if (!impl_tent_) return ERR_CONTEXT;

    std::vector<void*> addr_list;
    for (auto& entry : buffer_map) {
        mooncake::tent::TransportType ignored;
        if (!protoToTentTransportHint(entry.first, ignored)) {
            return ERR_INVALID_ARGUMENT;
        }
        for (auto& buffer : entry.second) {
            if (std::find(addr_list.begin(), addr_list.end(), buffer.addr) ==
                addr_list.end()) {
                addr_list.push_back(buffer.addr);
            }
        }
    }
    return unregisterLocalMemoryBatch(addr_list);
}

Status TransferEngine::mp_submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    std::string& proto) {
    if (!use_tent_) return impl_->mp_submitTransfer(batch_id, entries, proto);
    mooncake::tent::TransportType hint;
    if (!protoToTentTransportHint(proto, hint)) {
        return Status::InvalidArgument("Unsupported TENT transport hint: " +
                                       proto);
    }
    std::vector<TransferRequest> hinted_entries = entries;
    if (hint != mooncake::tent::UNSPEC) {
        for (auto& entry : hinted_entries) {
            entry.transport_hint = static_cast<int>(hint);
        }
    }
    return submitTransfer(batch_id, hinted_entries);
}

Status TransferEngine::mp_submitTransferWithNotify(
    BatchID batch_id, const std::vector<TransferRequest>& entries,
    TransferMetadata::NotifyDesc notify_msg, std::string& proto) {
    if (!use_tent_)
        return impl_->mp_submitTransferWithNotify(batch_id, entries, notify_msg,
                                                  proto);
    mooncake::tent::TransportType hint;
    if (!protoToTentTransportHint(proto, hint)) {
        return Status::InvalidArgument("Unsupported TENT transport hint: " +
                                       proto);
    }
    std::vector<TransferRequest> hinted_entries = entries;
    if (hint != mooncake::tent::UNSPEC) {
        for (auto& entry : hinted_entries) {
            entry.transport_hint = static_cast<int>(hint);
        }
    }
    return submitTransferWithNotify(batch_id, hinted_entries, notify_msg);
}
#endif

int TransferEngine::getNotifies(
    std::vector<TransferMetadata::NotifyDesc>& notifies) {
    if (use_tent_) {
        if (!impl_tent_) return ERR_CONTEXT;
        std::vector<mooncake::tent::Notification> notifi_list;
        auto status = impl_tent_->receiveNotification(notifi_list);
        for (auto& entry : notifi_list) {
            TransferMetadata::NotifyDesc desc;
            desc.name = entry.name;
            desc.notify_msg = entry.msg;
            notifies.push_back(desc);
        }
        return tentStatusToClassicReturn(status);
    } else
        return impl_->getNotifies(notifies);
}

int TransferEngine::sendNotifyByID(SegmentID target_id,
                                   TransferMetadata::NotifyDesc notify_msg) {
    if (use_tent_) {
        if (!impl_tent_) return ERR_CONTEXT;
        mooncake::tent::Notification notifi;
        notifi.name = notify_msg.name;
        notifi.msg = notify_msg.notify_msg;
        auto status = impl_tent_->sendNotification(target_id, notifi);
        return tentStatusToClassicReturn(status);
    } else
        return impl_->sendNotifyByID(target_id, notify_msg);
}

int TransferEngine::sendNotifyByName(std::string remote_agent,
                                     TransferMetadata::NotifyDesc notify_msg) {
    if (use_tent_) {
        if (!impl_tent_) return ERR_CONTEXT;
        mooncake::tent::Notification notifi;
        notifi.name = notify_msg.name;
        notifi.msg = notify_msg.notify_msg;
        SegmentHandle handle;
        auto status = impl_tent_->openSegment(handle, remote_agent);
        if (!status.ok()) return tentStatusToClassicReturn(status);
        status = impl_tent_->sendNotification(handle, notifi);
        impl_tent_->closeSegment(handle);
        return tentStatusToClassicReturn(status);
    } else
        return impl_->sendNotifyByName(std::move(remote_agent), notify_msg);
}

PeerLiveness TransferEngine::probePeerAliveByID(SegmentID target_id) {
    if (use_tent_) {
        if (!impl_tent_) return PeerLiveness::Unreachable;
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
        if (!impl_tent_)
            return Status::Context("TENT engine is not initialized");
        if (batch_id == INVALID_BATCH_ID) {
            return Status::InvalidArgument("Invalid batch ID");
        }
        mooncake::tent::TransferStatus tent_status;
        auto s = impl_tent_->getTransferStatus(batch_id, task_id, tent_status);
        if (!s.ok()) return tentStatusToClassicStatus(s);
        status.s = (TransferStatusEnum)(int)tent_status.s;
        status.transferred_bytes = tent_status.transferred_bytes;
        return Status::OK();
    } else {
        return impl_->getTransferStatus(batch_id, task_id, status);
    }
}

Status TransferEngine::getBatchTransferStatus(BatchID batch_id,
                                              TransferStatus& status) {
    if (use_tent_) {
        if (!impl_tent_)
            return Status::Context("TENT engine is not initialized");
        if (batch_id == INVALID_BATCH_ID) {
            return Status::InvalidArgument("Invalid batch ID");
        }
        mooncake::tent::TransferStatus tent_status;
        auto s = impl_tent_->getTransferStatus(batch_id, tent_status);
        if (!s.ok()) return tentStatusToClassicStatus(s);
        status.s = (TransferStatusEnum)(int)tent_status.s;
        status.transferred_bytes = tent_status.transferred_bytes;
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
    auto base = reinterpret_cast<uintptr_t>(addr);
    std::lock_guard<std::mutex> lock(tent_compat_mutex_);
    return std::any_of(
        tent_registered_ranges_.begin(), tent_registered_ranges_.end(),
        [base, length](const auto& range) {
            return rangesOverlap(base, length, range.first, range.second);
        });
}

void TransferEngine::setAutoDiscover(bool auto_discover) {
    if (!use_tent_) impl_->setAutoDiscover(auto_discover);
}

void TransferEngine::setWhitelistFilters(std::vector<std::string>&& filters) {
    if (!use_tent_) {
        impl_->setWhitelistFilters(std::move(filters));
        return;
    }
    if (impl_tent_) {
        LOG(WARNING)
            << "TENT is already initialized; whitelist filters will be kept "
               "for compatibility state but will not change active topology";
    }
    std::lock_guard<std::mutex> lock(tent_compat_mutex_);
    tent_whitelist_filters_ = std::move(filters);
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
    if (use_tent_) {
        if (json) {
            mooncake::tent::json result;
            result["mode"] = "tent";
            result["initialized"] = static_cast<bool>(impl_tent_);
            {
                std::lock_guard<std::mutex> lock(tent_compat_mutex_);
                result["rdma_whitelist"] = tent_whitelist_filters_;
            }
            if (impl_tent_) {
                result["available"] = impl_tent_->available();
                result["rpc_server_address"] =
                    impl_tent_->getRpcServerAddress();
                result["rpc_server_port"] = impl_tent_->getRpcServerPort();
            } else {
                result["available"] = false;
            }
            return result.dump();
        }
        return "(TENT mode)";
    }
    if (!impl_) {
        return json ? "{}" : "(not initialized)";
    }
    return json ? buildShowLinksJson(impl_.get())
                : buildShowLinksReadable(impl_.get());
}

}  // namespace mooncake
#endif
