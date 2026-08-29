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

#include "tent/runtime/control_plane.h"
#include "tent/runtime/transfer_engine_impl.h"

#include <cassert>
#include <set>
#include <utility>

#include "tent/common/status.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/segment_registry.h"

namespace mooncake {
namespace tent {
namespace {

template <typename Fn>
class CallbackInvocationGuard {
   public:
    explicit CallbackInvocationGuard(Fn on_exit)
        : on_exit_(std::move(on_exit)) {}

    ~CallbackInvocationGuard() { on_exit_(); }

    CallbackInvocationGuard(const CallbackInvocationGuard&) = delete;
    CallbackInvocationGuard& operator=(const CallbackInvocationGuard&) = delete;

   private:
    Fn on_exit_;
};

}  // namespace

thread_local const ControlService* ControlService::active_bootstrap_service_ =
    nullptr;
thread_local const ControlService* ControlService::active_notify_service_ =
    nullptr;
thread_local CoroRpcAgent tl_rpc_agent;

Status ControlClient::getSegmentDesc(const std::string& server_addr,
                                     std::string& response) {
    std::string request;
    return tl_rpc_agent.call(server_addr, GetSegmentDesc, request, response);
}

Status ControlClient::bootstrap(const std::string& server_addr,
                                const BootstrapDesc& request,
                                BootstrapDesc& response) {
    std::string request_raw, response_raw;
    json j = request;
    request_raw = j.dump();
    CHECK_STATUS(tl_rpc_agent.call(server_addr, BootstrapRdma, request_raw,
                                   response_raw));
    response = json::parse(response_raw).get<BootstrapDesc>();
    return Status::OK();
}

Status ControlClient::sendData(const std::string& server_addr,
                               uint64_t peer_mem_addr, void* local_mem_addr,
                               size_t length) {
    std::string response;
    XferDataDesc desc{htole64(peer_mem_addr), htole64(length)};
    std::string request;
    request.reserve(sizeof(XferDataDesc) + length);
    request.resize(sizeof(XferDataDesc));
    memcpy(request.data(), &desc, sizeof(desc));
    auto& loader = Platform::getLoader();
    if (loader.getMemoryType(local_mem_addr) == MTYPE_CPU) {
        // Single copy into the RPC attachment; avoids the resize() zero-fill
        // and the extra copy in call().
        request.append(reinterpret_cast<const char*>(local_mem_addr), length);
    } else {
        request.resize(sizeof(XferDataDesc) + length);
        loader.copy(request.data() + sizeof(desc), local_mem_addr, length);
    }
    auto status = tl_rpc_agent.callOwned(server_addr, SendData,
                                         std::move(request), response);
    if (!status.ok()) return status;
    if (!response.empty()) return Status::RpcServiceError(response);
    return Status::OK();
}

Status ControlClient::recvData(const std::string& server_addr,
                               uint64_t peer_mem_addr, void* local_mem_addr,
                               size_t length) {
    std::string request, response;
    XferDataDesc desc{htole64(peer_mem_addr), htole64(length)};
    request.resize(sizeof(XferDataDesc));
    memcpy(&request[0], &desc, sizeof(desc));
    auto status = tl_rpc_agent.call(server_addr, RecvData, request, response);
    if (!status.ok()) return status;
    if (response.size() != length)
        return Status::RpcServiceError(
            "RecvData failed: target address not in registered buffer");
    Platform::getLoader().copy(local_mem_addr, response.data(), length);
    return Status::OK();
}

inline void to_json(nlohmann::json& j, const Notification& n) {
    j = nlohmann::json{{"name", n.name}, {"msg", n.msg}};
}

inline void from_json(const nlohmann::json& j, Notification& n) {
    j.at("name").get_to(n.name);
    j.at("msg").get_to(n.msg);
}

Status ControlClient::notify(const std::string& server_addr,
                             const Notification& message) {
    json j = message;
    std::string request = j.dump();
    std::string response;
    return tl_rpc_agent.call(server_addr, Notify, request, response);
}

Status ControlClient::probe(const std::string& server_addr) {
    std::string request, response;
    return tl_rpc_agent.call(server_addr, Probe, request, response);
}

inline void to_json(json& j, const Request& r) {
    j = json{{"opcode", r.opcode == Request::READ ? "READ" : "WRITE"},
             {"source", reinterpret_cast<uintptr_t>(r.source)},
             {"target_id", r.target_id},
             {"target_offset", r.target_offset},
             {"length", r.length}};
}

inline void from_json(const json& j, Request& r) {
    std::string opcode_str = j.at("opcode").get<std::string>();
    if (opcode_str == "READ")
        r.opcode = Request::READ;
    else if (opcode_str == "WRITE")
        r.opcode = Request::WRITE;
    else
        throw std::runtime_error("Invalid opcode");

    r.source = reinterpret_cast<void*>(j.at("source").get<uintptr_t>());
    r.target_id = j.at("target_id").get<int>();
    r.target_offset = j.at("target_offset").get<uint64_t>();
    r.length = j.at("length").get<size_t>();
}

Status ControlClient::delegate(const std::string& server_addr,
                               const Request& request) {
    std::string request_raw, response_raw;
    json j = request;
    request_raw = j.dump();
    CHECK_STATUS(
        tl_rpc_agent.call(server_addr, Delegate, request_raw, response_raw));
    return response_raw.empty() ? Status::OK()
                                : Status::RpcServiceError(response_raw);
}

void ControlClient::delegateAsync(const std::string& server_addr,
                                  const Request& request,
                                  DelegateCallback callback) {
    json j = request;
    std::string request_raw = j.dump();
    tl_rpc_agent.callAsync(
        server_addr, Delegate, request_raw,
        [callback = std::move(callback)](Status status,
                                         std::string response_raw) mutable {
            if (!status.ok()) {
                callback(std::move(status), false);
                return;
            }
            callback(response_raw.empty()
                         ? Status::OK()
                         : Status::RpcServiceError(response_raw),
                     true);
        });
}

Status ControlClient::pinStageBuffer(const std::string& server_addr,
                                     const std::string& location,
                                     uint64_t& addr) {
    std::string request_raw, response_raw;
    json j = location;
    request_raw = j.dump();
    CHECK_STATUS(
        tl_rpc_agent.call(server_addr, Pin, request_raw, response_raw));
    addr = json::parse(response_raw).get<uint64_t>();
    return Status::OK();
}

Status ControlClient::unpinStageBuffer(const std::string& server_addr,
                                       uint64_t addr) {
    std::string request_raw, response_raw;
    json j = addr;
    request_raw = j.dump();
    CHECK_STATUS(
        tl_rpc_agent.call(server_addr, Unpin, request_raw, response_raw));
    return Status::OK();
}

void ControlClient::unpinStageBufferAsync(const std::string& server_addr,
                                          uint64_t addr,
                                          UnpinStageBufferCallback callback) {
    json j = addr;
    std::string request_raw = j.dump();
    tl_rpc_agent.callAsync(
        server_addr, Unpin, request_raw,
        [callback = std::move(callback)](Status status,
                                         std::string response_raw) mutable {
            if (!status.ok()) {
                callback(std::move(status));
                return;
            }
            callback(response_raw.empty()
                         ? Status::OK()
                         : Status::RpcServiceError(response_raw));
        });
}

ControlService::ControlService(const std::string& type,
                               const std::string& servers,
                               TransferEngineImpl* impl)
    : bootstrap_callback_(nullptr), notify_callback_(nullptr), impl_(impl) {
    if (type == "p2p") {
        auto agent = std::make_unique<PeerSegmentRegistry>();
        manager_ = std::make_unique<SegmentManager>(std::move(agent));
    } else {
        auto agent = std::make_unique<CentralSegmentRegistry>(type, servers);
        manager_ = std::make_unique<SegmentManager>(std::move(agent));
    }
    rpc_server_ = std::make_shared<CoroRpcAgent>();
    rpc_server_->registerFunction(
        GetSegmentDesc,
        [this](const std::string_view& request, std::string& response) {
            onGetSegmentDesc(request, response);
        });
    rpc_server_->registerFunction(
        BootstrapRdma,
        [this](const std::string_view& request, std::string& response) {
            onBootstrapRdma(request, response);
        });
    // SendData/RecvData copy the full TCP payload. Running them inline on the
    // io_context serializes every bulk transfer and stalls Probe/Bootstrap
    // on the same thread. Offload matches Delegate: the connection coroutine
    // suspends, copies run on the blocking executor, and other RPCs proceed.
    rpc_server_->registerFunction(
        SendData,
        [this](const std::string_view& request, std::string& response) {
            onSendData(request, response);
        },
        /*offload=*/true);
    rpc_server_->registerFunction(
        RecvData,
        [this](const std::string_view& request, std::string& response) {
            onRecvData(request, response);
        },
        /*offload=*/true);
    rpc_server_->registerFunction(
        Notify, [this](const std::string_view& request, std::string& response) {
            onNotify(request, response);
        });
    rpc_server_->registerFunction(
        Probe, [this](const std::string_view& request, std::string& response) {
            onProbe(request, response);
        });
    // onDelegate runs a whole transfer to completion; holding the io_context
    // thread for that long blocked every other RPC on this node.
    rpc_server_->registerFunction(
        Delegate,
        [this](const std::string_view& request, std::string& response) {
            onDelegate(request, response);
        },
        /*offload=*/true);
    rpc_server_->registerFunction(
        Pin, [this](const std::string_view& request, std::string& response) {
            onPinStageBuffer(request, response);
        });
    rpc_server_->registerFunction(
        Unpin, [this](const std::string_view& request, std::string& response) {
            onUnpinStageBuffer(request, response);
        });
    rpc_server_->registerFunction(
        SubscribeSegmentUpdate,
        [this](const std::string_view& request, std::string& response) {
            onSubscribeSegmentUpdate(request, response);
        });
    rpc_server_->registerFunction(
        NotifySegmentUpdated,
        [this](const std::string_view& request, std::string& response) {
            onSegmentUpdated(request, response);
        });
}

ControlService::~ControlService() {
    // Stop RPC workers while callback state and synchronization primitives are
    // still alive. Member destruction would otherwise tear them down first.
    rpc_server_.reset();
}

void ControlService::setBootstrapRdmaCallback(
    const OnReceiveBootstrap& callback) {
    std::unique_lock<std::mutex> guard(bootstrap_cb_mutex_);
    if (active_bootstrap_service_ == this) {
        bootstrap_callback_ = callback;
        return;
    }
    bootstrap_callback_ = nullptr;
    if (!bootstrap_cb_cv_.wait_for(guard, callback_drain_timeout_, [this] {
            return bootstrap_callbacks_in_flight_ == 0;
        })) {
        LOG(ERROR)
            << "Timed out waiting for BootstrapRdma callbacks to drain, "
            << "in_flight=" << bootstrap_callbacks_in_flight_
            << ", timeout_ms=" << callback_drain_timeout_.count()
            << ". Continue replacing the callback to keep shutdown bounded.";
    }
    bootstrap_callback_ = callback;
}

void ControlService::setNotifyCallback(const OnNotify& callback) {
    std::unique_lock<std::mutex> guard(notify_cb_mutex_);
    if (active_notify_service_ == this) {
        notify_callback_ = callback;
        return;
    }
    notify_callback_ = nullptr;
    if (!notify_cb_cv_.wait_for(guard, callback_drain_timeout_, [this] {
            return notify_callbacks_in_flight_ == 0;
        })) {
        LOG(ERROR) << "Timed out waiting for Notify callbacks to drain, "
                   << "in_flight=" << notify_callbacks_in_flight_
                   << ", timeout_ms=" << callback_drain_timeout_.count()
                   << ". Continue replacing the callback to keep shutdown "
                   << "bounded.";
    }
    notify_callback_ = callback;
}

void ControlService::finishBootstrapCallback() {
    std::lock_guard<std::mutex> guard(bootstrap_cb_mutex_);
    --bootstrap_callbacks_in_flight_;
    bootstrap_cb_cv_.notify_all();
}

void ControlService::finishNotifyCallback() {
    std::lock_guard<std::mutex> guard(notify_cb_mutex_);
    --notify_callbacks_in_flight_;
    notify_cb_cv_.notify_all();
}

Status ControlService::start(uint16_t& port, bool ipv6_, size_t threads) {
    return rpc_server_->start(port, ipv6_, threads);
}

void ControlService::onGetSegmentDesc(const std::string_view& request,
                                      std::string& response) {
    // Reuse the cached dump shared across concurrent peer fetches.
    auto cached = manager_->getLocalDumpedJson();
    response = *cached;
}

void ControlService::onBootstrapRdma(const std::string_view& request,
                                     std::string& response) {
    std::string mutable_request(request);
    OnReceiveBootstrap callback;
    {
        std::lock_guard<std::mutex> guard(bootstrap_cb_mutex_);
        if (!bootstrap_callback_) {
            BootstrapDesc response_desc;
            response_desc.reply_msg = "NOT_READY: transport not initialized";
            json j = response_desc;
            response = j.dump();
            return;
        }
        callback = bootstrap_callback_;
        ++bootstrap_callbacks_in_flight_;
    }

    BootstrapDesc response_desc;
    {
        const ControlService* previous_service = active_bootstrap_service_;
        active_bootstrap_service_ = this;
        CallbackInvocationGuard invocation_guard([this, previous_service] {
            active_bootstrap_service_ = previous_service;
            finishBootstrapCallback();
        });

        try {
            BootstrapDesc request_desc =
                json::parse(mutable_request).get<BootstrapDesc>();
            int rc = callback(request_desc, response_desc);
            if (rc != 0 && response_desc.reply_msg.empty()) {
                response_desc.reply_msg = "BootstrapRdma callback failed";
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "onBootstrapRdma failed: " << e.what();
            response_desc.reply_msg =
                std::string("BootstrapRdma callback failed: ") + e.what();
        } catch (...) {
            LOG(ERROR) << "onBootstrapRdma failed with unknown exception";
            response_desc.reply_msg = "BootstrapRdma callback failed";
        }
    }

    json j = response_desc;
    response = j.dump();
}

void ControlService::onSendData(const std::string_view& request,
                                std::string& response) {
    if (request.size() < sizeof(XferDataDesc)) {
        response = "SendData failed: request too short";
        return;
    }
    XferDataDesc* desc = (XferDataDesc*)request.data();
    auto local_desc = manager_->getLocal();
    auto peer_mem_addr = le64toh(desc->peer_mem_addr);
    auto length = le64toh(desc->length);

    // Validate request size to prevent buffer over-read
    if (request.size() < sizeof(XferDataDesc) + length) {
        response = "SendData failed: invalid request size";
        return;
    }

    if (local_desc->findBuffer(peer_mem_addr, length)) {
        Platform::getLoader().copy((void*)peer_mem_addr, &desc[1], length);
    } else {
        response = "SendData failed: target address not in registered buffer";
    }
}

void ControlService::onRecvData(const std::string_view& request,
                                std::string& response) {
    if (request.size() < sizeof(XferDataDesc)) {
        response = "RecvData failed: request too short";
        return;
    }
    XferDataDesc* desc = (XferDataDesc*)request.data();
    auto local_desc = manager_->getLocal();
    auto peer_mem_addr = le64toh(desc->peer_mem_addr);
    auto length = le64toh(desc->length);

    // Validate length to prevent DoS via excessive memory allocation
    constexpr size_t kMaxTransferSize = 1ULL << 30;  // 1GB max per RPC
    if (length > kMaxTransferSize) {
        response = "RecvData failed: length exceeds maximum allowed";
        return;
    }

    if (local_desc->findBuffer(peer_mem_addr, length)) {
        auto& loader = Platform::getLoader();
        if (loader.getMemoryType((void*)peer_mem_addr) == MTYPE_CPU) {
            // assign() skips the resize() zero-fill pass.
            response.assign(reinterpret_cast<const char*>(peer_mem_addr),
                            length);
        } else {
            response.resize(length);
            loader.copy(response.data(), (void*)peer_mem_addr, length);
        }
    } else {
        response = "RecvData failed: target address not in registered buffer";
    }
}

void ControlService::onNotify(const std::string_view& request,
                              std::string& response) {
    (void)response;
    OnNotify callback;
    {
        std::lock_guard<std::mutex> guard(notify_cb_mutex_);
        if (!notify_callback_) return;
        callback = notify_callback_;
        ++notify_callbacks_in_flight_;
    }

    const ControlService* previous_service = active_notify_service_;
    active_notify_service_ = this;
    CallbackInvocationGuard invocation_guard([this, previous_service] {
        active_notify_service_ = previous_service;
        finishNotifyCallback();
    });

    try {
        Notification message = json::parse(request).get<Notification>();
        callback(message);
    } catch (const std::exception& e) {
        LOG(ERROR) << "onNotify failed: " << e.what();
    } catch (...) {
        LOG(ERROR) << "onNotify failed with unknown exception";
    }
}

void ControlService::onProbe(const std::string_view& request,
                             std::string& response) {
    (void)request;
    (void)response;
}

void ControlService::onDelegate(const std::string_view& request,
                                std::string& response) {
    Request user_request = json::parse(std::string(request)).get<Request>();
    auto status = impl_->transferSync({user_request});
    if (!status.ok()) response = status.ToString();
}

void ControlService::onPinStageBuffer(const std::string_view& request,
                                      std::string& response) {
    std::string location = json::parse(request).get<std::string>();
    uint64_t addr = impl_->lockStageBuffer(location);
    json j = addr;
    response = j.dump();
}

void ControlService::onUnpinStageBuffer(const std::string_view& request,
                                        std::string& response) {
    uint64_t addr = json::parse(request).get<uint64_t>();
    impl_->unlockStageBuffer(addr);
}

void ControlService::onSubscribeSegmentUpdate(const std::string_view& request,
                                              std::string& response) {
    std::string peer_addr =
        json::parse(std::string(request)).get<std::string>();
    manager_->addSubscriber(peer_addr);
}

void ControlService::onSegmentUpdated(const std::string_view& request,
                                      std::string& response) {
    std::string segment_name =
        json::parse(std::string(request)).get<std::string>();

    manager_->invalidateAllCacheForRemote(segment_name);

    VLOG(1) << "Invalidated cache for segment " << segment_name
            << " due to remote update notification";
}

void ControlClient::subscribeSegmentUpdateAsync(
    const std::string& server_addr, const std::string& subscriber_addr) {
    json j = subscriber_addr;
    std::string request = j.dump();
    tl_rpc_agent.callAsync(
        server_addr, SubscribeSegmentUpdate, request,
        [](const Status& status, const std::string&) {
            if (!status.ok()) {
                LOG(ERROR) << "SubscribeSegmentUpdate RPC failed with: "
                           << status.ToString();
            }
        });
}

void ControlClient::notifySegmentUpdatedAsync(
    const std::string& server_addr, const std::string& segment_name,
    const onNotifySegmentUpdateFailure& on_failure) {
    json j = segment_name;
    std::string request = j.dump();
    tl_rpc_agent.callAsync(
        server_addr, NotifySegmentUpdated, request,
        [on_failure](const Status& status, const std::string&) {
            if (!status.ok()) {
                on_failure();
            }
        });
}

}  // namespace tent
}  // namespace mooncake
