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
#include <exception>
#include <limits>
#include <optional>
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

// onNotify() answers a request it can never take with this prefix, which
// ControlClient::notify() tells apart from a notification not taken yet.
constexpr char kMalformedNotification[] = "malformed notification: ";
constexpr char kNotifyCallbackFailed[] = "notify callback failed: ";

}  // namespace

thread_local const ControlService* ControlService::active_bootstrap_service_ =
    nullptr;
thread_local const ControlService* ControlService::active_notify_service_ =
    nullptr;
thread_local CoroRpcAgent tl_rpc_agent;

namespace {

constexpr uint64_t kDelegatedCreditProtocolMagic = 0x54454e54444c4732ULL;

struct ParsedXferData {
    uint64_t peer_mem_addr{0};
    size_t length{0};
    size_t header_size{0};
    bool version_two{false};
    uint64_t receiver_credit_session_high{0};
    uint64_t receiver_credit_session_low{0};
    uint64_t receiver_credit_epoch{0};
};

Status parseXferData(const std::string_view& request, bool carries_payload,
                     ParsedXferData& parsed) {
    if (request.size() < sizeof(LegacyXferDataDesc)) {
        return Status::InvalidArgument("request too short" LOC_MARK);
    }
    LegacyXferDataDesc legacy;
    memcpy(&legacy, request.data(), sizeof(legacy));
    const uint64_t length = le64toh(legacy.length);
    if (length > std::numeric_limits<size_t>::max()) {
        return Status::InvalidArgument("transfer length overflow" LOC_MARK);
    }

    parsed = ParsedXferData{};
    parsed.peer_mem_addr = le64toh(legacy.peer_mem_addr);
    parsed.length = static_cast<size_t>(length);
    parsed.header_size = sizeof(LegacyXferDataDesc);

    if (request.size() >= sizeof(XferDataDesc)) {
        XferDataDesc current;
        memcpy(&current, request.data(), sizeof(current));
        const bool magic_matches =
            le64toh(current.protocol_magic) == kXferDataProtocolMagic;
        const bool size_matches =
            carries_payload
                ? parsed.length <= std::numeric_limits<size_t>::max() -
                                       sizeof(XferDataDesc) &&
                      request.size() == sizeof(XferDataDesc) + parsed.length
                : request.size() == sizeof(XferDataDesc);
        if (magic_matches && size_matches) {
            parsed.header_size = sizeof(XferDataDesc);
            parsed.version_two = true;
            parsed.peer_mem_addr = le64toh(current.peer_mem_addr);
            parsed.receiver_credit_session_high =
                le64toh(current.receiver_credit_session_high);
            parsed.receiver_credit_session_low =
                le64toh(current.receiver_credit_session_low);
            parsed.receiver_credit_epoch =
                le64toh(current.receiver_credit_epoch);
        }
    }

    if (carries_payload) {
        if (parsed.length >
                std::numeric_limits<size_t>::max() - parsed.header_size ||
            request.size() != parsed.header_size + parsed.length) {
            return Status::InvalidArgument(
                "invalid transfer request size" LOC_MARK);
        }
    } else if (request.size() != parsed.header_size) {
        return Status::InvalidArgument(
            "invalid transfer request header size" LOC_MARK);
    }
    return Status::OK();
}

Status validateXferReceiverCreditFence(const SegmentDescRef& local_desc,
                                       const ParsedXferData& request) {
    const bool has_fence = request.receiver_credit_session_high != 0 ||
                           request.receiver_credit_session_low != 0 ||
                           request.receiver_credit_epoch != 0;
    if (!local_desc || local_desc->type != SegmentType::Memory) {
        return has_fence ? Status::InvalidEntry(
                               "receiver-credit target is not memory" LOC_MARK)
                         : Status::OK();
    }

    const auto& advert = local_desc->getMemory().receiver_credit;
    if (!advert) {
        return has_fence
                   ? Status::InvalidEntry(
                         "receiver-credit session is unavailable" LOC_MARK)
                   : Status::OK();
    }
    if (advert->schema_version != kReceiverCreditProtocolVersion ||
        advert->flags != kReceiverCreditRequired ||
        advert->receiver_session_id.empty() || advert->epoch == 0) {
        return Status::InvalidEntry(
            "invalid local receiver-credit advertisement" LOC_MARK);
    }
    if (!request.version_two ||
        request.receiver_credit_session_high !=
            advert->receiver_session_id.high ||
        request.receiver_credit_session_low !=
            advert->receiver_session_id.low ||
        request.receiver_credit_epoch != advert->epoch) {
        return Status::InvalidEntry(
            "receiver-credit session fence mismatch" LOC_MARK);
    }
    return Status::OK();
}

}  // namespace

Status ControlClient::getSegmentDesc(const std::string& server_addr,
                                     std::string& response) {
    std::string request;
    auto status =
        tl_rpc_agent.call(server_addr, GetSegmentDesc, request, response);
    if (status.IsRpcServiceError()) {
        // A failed stale connection is discarded by the RPC agent. Retry this
        // read-only operation once; other control RPCs may have side effects.
        return tl_rpc_agent.call(server_addr, GetSegmentDesc, request,
                                 response);
    }
    return status;
}

Status ControlClient::decodeBootstrapResponse(const std::string& response_raw,
                                              BootstrapDesc& response) {
    try {
        response = json::parse(response_raw).get<BootstrapDesc>();
    } catch (const std::exception& e) {
        return Status::MalformedJson(
            std::string("Invalid bootstrap response: ") + e.what() + LOC_MARK);
    }
    if (!response.reply_msg.empty()) {
        return Status::RpcServiceError(response.reply_msg);
    }
    if (response.local_gid.empty()) {
        return Status::InvalidArgument(
            "Missing peer GID in bootstrap" LOC_MARK);
    }
    return Status::OK();
}

Status ControlClient::bootstrap(const std::string& server_addr,
                                const BootstrapDesc& request,
                                BootstrapDesc& response) {
    std::string request_raw, response_raw;
    json j = request;
    request_raw = j.dump();
    CHECK_STATUS(tl_rpc_agent.call(server_addr, BootstrapRdma, request_raw,
                                   response_raw));
    return decodeBootstrapResponse(response_raw, response);
}

Status ControlClient::bootstrapUb(const std::string& server_addr,
                                  const UbBootstrapDesc& request,
                                  UbBootstrapDesc& response) {
    std::string request_raw, response_raw;
    json j = request;
    request_raw = j.dump();
    CHECK_STATUS(
        tl_rpc_agent.call(server_addr, BootstrapUb, request_raw, response_raw));
    try {
        response = json::parse(response_raw).get<UbBootstrapDesc>();
    } catch (const std::exception& e) {
        return Status::MalformedJson(
            std::string("Malformed UB bootstrap response: ") + e.what() +
            LOC_MARK);
    }
    return Status::OK();
}

Status ControlClient::exchangeReceiverCredit(
    const std::string& server_addr,
    const ReceiverCreditExchangeRequestV1& request,
    ReceiverCreditExchangeReplyV1& response) {
    std::string request_raw, response_raw;
    json encoded = request;
    request_raw = encoded.dump();
    CHECK_STATUS(tl_rpc_agent.call(server_addr, ExchangeReceiverCredit,
                                   request_raw, response_raw));
    try {
        response =
            json::parse(response_raw).get<ReceiverCreditExchangeReplyV1>();
    } catch (const std::exception& error) {
        return Status::MalformedJson(
            std::string("Malformed receiver-credit response: ") + error.what() +
            LOC_MARK);
    }
    if (response.schema_version != kReceiverCreditProtocolVersion ||
        response.flags != 0) {
        return Status::RpcServiceError(
            "Unsupported receiver-credit response version" LOC_MARK);
    }
    if (!response.reply_msg.empty()) {
        return Status::RpcServiceError(response.reply_msg);
    }
    return Status::OK();
}

Status ControlClient::sendData(const std::string& server_addr,
                               uint64_t peer_mem_addr, void* local_mem_addr,
                               size_t length,
                               uint64_t receiver_credit_session_high,
                               uint64_t receiver_credit_session_low,
                               uint64_t receiver_credit_epoch) {
    if (length > kTcpMaxWriteBytes)
        return Status::InvalidArgument("TCP WRITE exceeds RPC payload limit");
    std::string request, response;
    const bool has_any_fence = receiver_credit_session_high != 0 ||
                               receiver_credit_session_low != 0 ||
                               receiver_credit_epoch != 0;
    const bool has_complete_fence = (receiver_credit_session_high != 0 ||
                                     receiver_credit_session_low != 0) &&
                                    receiver_credit_epoch != 0;
    if (has_any_fence && !has_complete_fence) {
        return Status::InvalidArgument(
            "partial SendData receiver-credit fence" LOC_MARK);
    }
    if (length > std::numeric_limits<size_t>::max() - sizeof(XferDataDesc)) {
        return Status::InvalidArgument(
            "SendData request size overflow" LOC_MARK);
    }
    size_t header_size = sizeof(LegacyXferDataDesc);
    request.reserve(sizeof(XferDataDesc) + length);
    if (has_complete_fence) {
        const XferDataDesc desc{htole64(kXferDataV2LegacyRejectAddress),
                                htole64(static_cast<uint64_t>(length)),
                                htole64(kXferDataProtocolMagic),
                                htole64(peer_mem_addr),
                                htole64(receiver_credit_session_high),
                                htole64(receiver_credit_session_low),
                                htole64(receiver_credit_epoch)};
        header_size = sizeof(desc);
        request.resize(header_size);
        memcpy(request.data(), &desc, sizeof(desc));
    } else {
        const LegacyXferDataDesc desc{htole64(peer_mem_addr),
                                      htole64(static_cast<uint64_t>(length))};
        request.resize(header_size);
        memcpy(request.data(), &desc, sizeof(desc));
    }
    auto& loader = Platform::getLoader();
    if (loader.getMemoryType(local_mem_addr) == MTYPE_CPU) {
        request.append(reinterpret_cast<const char*>(local_mem_addr), length);
    } else {
        request.resize(header_size + length);
        CHECK_STATUS(
            loader.copy(request.data() + header_size, local_mem_addr, length));
    }
    auto status = tl_rpc_agent.callOwned(server_addr, SendData,
                                         std::move(request), response);
    if (!status.ok()) return status;
    if (!response.empty()) return Status::RpcServiceError(response);
    return Status::OK();
}

Status ControlClient::recvData(const std::string& server_addr,
                               uint64_t peer_mem_addr, void* local_mem_addr,
                               size_t length,
                               uint64_t receiver_credit_session_high,
                               uint64_t receiver_credit_session_low,
                               uint64_t receiver_credit_epoch) {
    if (length > kTcpMaxReadBytes)
        return Status::InvalidArgument("TCP READ exceeds RPC payload limit");
    std::string request, response;
    const bool has_any_fence = receiver_credit_session_high != 0 ||
                               receiver_credit_session_low != 0 ||
                               receiver_credit_epoch != 0;
    const bool has_complete_fence = (receiver_credit_session_high != 0 ||
                                     receiver_credit_session_low != 0) &&
                                    receiver_credit_epoch != 0;
    if (has_any_fence && !has_complete_fence) {
        return Status::InvalidArgument(
            "partial RecvData receiver-credit fence" LOC_MARK);
    }
    if (has_complete_fence) {
        const XferDataDesc desc{htole64(kXferDataV2LegacyRejectAddress),
                                htole64(static_cast<uint64_t>(length)),
                                htole64(kXferDataProtocolMagic),
                                htole64(peer_mem_addr),
                                htole64(receiver_credit_session_high),
                                htole64(receiver_credit_session_low),
                                htole64(receiver_credit_epoch)};
        request.resize(sizeof(desc));
        memcpy(request.data(), &desc, sizeof(desc));
    } else {
        const LegacyXferDataDesc desc{htole64(peer_mem_addr),
                                      htole64(static_cast<uint64_t>(length))};
        request.resize(sizeof(desc));
        memcpy(request.data(), &desc, sizeof(desc));
    }
    auto status = tl_rpc_agent.call(server_addr, RecvData, request, response);
    if (!status.ok()) return status;
    // A short reply is an error, not payload: surface the server's message so
    // callers keep the rejection reason (e.g. "permission denied") instead of
    // a generic string, matching the pre-v2 client behavior. An empty or
    // unpadded reply still gets a descriptive fallback.
    if (!has_complete_fence) {
        if (response.size() != length) {
            return Status::RpcServiceError(
                response.empty()
                    ? "RecvData failed: invalid or rejected legacy response"
                    : response);
        }
        return Platform::getLoader().copy(local_mem_addr, response.data(),
                                          length);
    }
    if (length >
            std::numeric_limits<size_t>::max() - sizeof(XferDataReplyHeader) ||
        response.size() != sizeof(XferDataReplyHeader) + length) {
        return Status::RpcServiceError(
            response.empty() ? "RecvData failed: invalid or rejected response"
                             : response);
    }
    XferDataReplyHeader header;
    memcpy(&header, response.data(), sizeof(header));
    if (le64toh(header.protocol_magic) != kXferDataProtocolMagic) {
        return Status::RpcServiceError(
            "RecvData failed: unsupported response version");
    }
    return Platform::getLoader().copy(
        local_mem_addr, response.data() + sizeof(XferDataReplyHeader), length);
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
    CHECK_STATUS(tl_rpc_agent.call(server_addr, Notify, request, response));
    // Not an RPC error: the peer answered. A malformed request fails the
    // same way on every retry, while a missing callback may be set later.
    if (response.empty()) return Status::OK();
    if (response.rfind(kMalformedNotification, 0) == 0)
        return Status::InvalidArgument(response);
    return Status::InternalError(response);
}

Status ControlClient::probe(const std::string& server_addr) {
    std::string request, response;
    return tl_rpc_agent.call(server_addr, Probe, request, response);
}

inline void to_json(json& j, const Request& r) {
    const bool has_complete_fence = (r.receiver_credit_session_high != 0 ||
                                     r.receiver_credit_session_low != 0) &&
                                    r.receiver_credit_epoch != 0;
    j = json{
        {"opcode", r.opcode == Request::READ ? "READ" : "WRITE"},
        {"source", reinterpret_cast<uintptr_t>(r.source)},
        {"target_id", has_complete_fence ? std::numeric_limits<SegmentID>::max()
                                         : r.target_id},
        {"target_offset", has_complete_fence
                              ? std::numeric_limits<uint64_t>::max()
                              : r.target_offset},
        {"length", r.length},
        {"receiver_credit_session_high", r.receiver_credit_session_high},
        {"receiver_credit_session_low", r.receiver_credit_session_low},
        {"receiver_credit_epoch", r.receiver_credit_epoch}};
    if (has_complete_fence) {
        j["delegate_protocol_magic"] = kDelegatedCreditProtocolMagic;
        j["actual_target_id"] = r.target_id;
        j["actual_target_offset"] = r.target_offset;
    }
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
    const bool fenced_delegate =
        j.value("delegate_protocol_magic", uint64_t{0}) ==
        kDelegatedCreditProtocolMagic;
    r.target_id = fenced_delegate ? j.at("actual_target_id").get<SegmentID>()
                                  : j.at("target_id").get<SegmentID>();
    r.target_offset = fenced_delegate
                          ? j.at("actual_target_offset").get<uint64_t>()
                          : j.at("target_offset").get<uint64_t>();
    r.length = j.at("length").get<size_t>();
    r.receiver_credit_session_high =
        j.value("receiver_credit_session_high", uint64_t{0});
    r.receiver_credit_session_low =
        j.value("receiver_credit_session_low", uint64_t{0});
    r.receiver_credit_epoch = j.value("receiver_credit_epoch", uint64_t{0});
}

Status ControlClient::delegate(const std::string& server_addr,
                               const Request& request) {
    const bool has_any_fence = request.receiver_credit_session_high != 0 ||
                               request.receiver_credit_session_low != 0 ||
                               request.receiver_credit_epoch != 0;
    const bool has_complete_fence =
        (request.receiver_credit_session_high != 0 ||
         request.receiver_credit_session_low != 0) &&
        request.receiver_credit_epoch != 0;
    if (has_any_fence && !has_complete_fence) {
        return Status::InvalidArgument(
            "partial delegated receiver-credit fence" LOC_MARK);
    }
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
        BootstrapUb,
        [this](const std::string_view& request, std::string& response) {
            onBootstrapUb(request, response);
        });
    rpc_server_->registerFunction(
        ExchangeReceiverCredit,
        [this](const std::string_view& request, std::string& response) {
            onExchangeReceiverCredit(request, response);
        });
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
    {
        std::lock_guard<std::mutex> lock(ub_bootstrap_callback_mutex_);
        ub_bootstrap_callback_ = {};
    }
    {
        std::lock_guard<std::mutex> lock(receiver_credit_callback_mutex_);
        receiver_credit_callback_ = {};
    }
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

void ControlService::onBootstrapUb(const std::string_view& request,
                                   std::string& response) {
    UbBootstrapDesc response_desc;
    try {
        auto request_desc =
            json::parse(std::string(request)).get<UbBootstrapDesc>();
        int ret = -1;
        {
            // Callback replacement during uninstall is serialized with
            // invocation, so an in-flight bootstrap cannot outlive the UB
            // transport object it targets.
            std::lock_guard<std::mutex> lock(ub_bootstrap_callback_mutex_);
            if (ub_bootstrap_callback_) {
                ret = ub_bootstrap_callback_(request_desc, response_desc);
            } else {
                response_desc.reply_msg =
                    "UB bootstrap callback is not registered";
            }
        }
        if (ret != 0 && response_desc.reply_msg.empty()) {
            response_desc.reply_msg =
                "UB bootstrap callback failed, ret=" + std::to_string(ret);
        }
    } catch (const std::exception& e) {
        response_desc.reply_msg =
            std::string("Malformed UB bootstrap request: ") + e.what();
    }
    json j = response_desc;
    response = j.dump();
}

void ControlService::onExchangeReceiverCredit(const std::string_view& request,
                                              std::string& response) {
    ReceiverCreditExchangeReplyV1 reply;
    try {
        auto parsed = json::parse(std::string(request))
                          .get<ReceiverCreditExchangeRequestV1>();
        Status status = Status::InvalidEntry(
            "receiver credit callback is not registered" LOC_MARK);
        {
            // Serialize callback replacement with invocation so teardown
            // cannot destroy the authority while an exchange is in flight.
            std::lock_guard<std::mutex> lock(receiver_credit_callback_mutex_);
            if (receiver_credit_callback_) {
                status = receiver_credit_callback_(parsed, reply);
            }
        }
        if (!status.ok()) reply.reply_msg = status.ToString();
    } catch (const std::exception& error) {
        reply.reply_msg =
            std::string("Malformed receiver-credit request: ") + error.what();
    }
    json encoded = reply;
    response = encoded.dump();
}

void ControlService::onSendData(const std::string_view& request,
                                std::string& response) {
    ParsedXferData parsed;
    auto status = parseXferData(request, true, parsed);
    if (!status.ok()) {
        response = "SendData failed: " + status.ToString();
        return;
    }
    auto local_desc = manager_->getLocal();
    status = validateXferReceiverCreditFence(local_desc, parsed);
    if (!status.ok()) {
        response = "SendData failed: " + status.ToString();
        return;
    }

    if (parsed.length > kTcpMaxWriteBytes) {
        response = "SendData failed: invalid request size";
        return;
    }
    if (auto* buffer =
            local_desc->findBuffer(parsed.peer_mem_addr, parsed.length)) {
        if (buffer->permission != kGlobalReadWrite) {
            response = "SendData failed: remote write permission denied";
            return;
        }
        auto copy_status = Platform::getLoader().copy(
            reinterpret_cast<void*>(parsed.peer_mem_addr),
            const_cast<char*>(request.data() + parsed.header_size),
            parsed.length);
        if (!copy_status.ok()) {
            response = "SendData failed: copy: " + copy_status.ToString();
        }
    } else {
        response = "SendData failed: target address not in registered buffer";
    }
}

void ControlService::onRecvData(const std::string_view& request,
                                std::string& response) {
    ParsedXferData parsed;
    auto status = parseXferData(request, false, parsed);
    // Legacy readers accept any response of exactly the requested length.
    // Fenced readers also require the reply magic; pad errors to keep both
    // response formats unambiguous, including parse and session failures.
    const size_t reply_header_size =
        parsed.version_two ? sizeof(XferDataReplyHeader) : 0;
    auto fail = [&response, &parsed, reply_header_size](std::string message) {
        response = std::move(message);
        if (parsed.length <=
                std::numeric_limits<size_t>::max() - reply_header_size &&
            response.size() == reply_header_size + parsed.length) {
            response.push_back(' ');
        }
    };
    if (!status.ok()) {
        fail("RecvData failed: " + status.ToString());
        return;
    }
    auto local_desc = manager_->getLocal();
    status = validateXferReceiverCreditFence(local_desc, parsed);
    if (!status.ok()) {
        fail("RecvData failed: " + status.ToString());
        return;
    }
    if (parsed.length > kTcpMaxReadBytes) {
        fail("RecvData failed: length exceeds maximum allowed");
        return;
    }
    if (auto* buffer =
            local_desc->findBuffer(parsed.peer_mem_addr, parsed.length)) {
        if (buffer->permission != kGlobalReadOnly &&
            buffer->permission != kGlobalReadWrite) {
            fail("RecvData failed: remote read permission denied");
            return;
        }
        auto& loader = Platform::getLoader();
        auto* source = reinterpret_cast<void*>(parsed.peer_mem_addr);
        response.clear();
        if (parsed.version_two) {
            const XferDataReplyHeader reply{htole64(kXferDataProtocolMagic)};
            response.assign(reinterpret_cast<const char*>(&reply),
                            sizeof(reply));
        }
        if (loader.getMemoryType(source) == MTYPE_CPU) {
            response.append(reinterpret_cast<const char*>(source),
                            parsed.length);
        } else {
            response.resize(reply_header_size + parsed.length);
            auto copy_status = loader.copy(response.data() + reply_header_size,
                                           source, parsed.length);
            if (!copy_status.ok()) {
                fail("RecvData failed: copy: " + copy_status.ToString());
            }
        }
    } else {
        fail("RecvData failed: target address not in registered buffer");
    }
}

void ControlService::onNotify(const std::string_view& request,
                              std::string& response) {
    OnNotify callback;
    {
        std::lock_guard<std::mutex> guard(notify_cb_mutex_);
        // An empty response is the sender's ack; before the callback is
        // registered and after teardown clears it, nothing takes the message.
        if (!notify_callback_) {
            response = "notify callback not registered";
            return;
        }
        callback = notify_callback_;
        ++notify_callbacks_in_flight_;
    }

    const ControlService* previous_service = active_notify_service_;
    active_notify_service_ = this;
    CallbackInvocationGuard invocation_guard([this, previous_service] {
        active_notify_service_ = previous_service;
        finishNotifyCallback();
    });

    std::optional<Notification> message;
    try {
        message = json::parse(request).get<Notification>();
        int rc = callback(*message);
        if (rc != 0) {
            response = std::string(kNotifyCallbackFailed) + "returned " +
                       std::to_string(rc);
        }
    } catch (const json::exception& e) {
        LOG(ERROR) << "onNotify failed: " << e.what();
        // Only a request that does not parse fails again on every retry.
        response = std::string(message ? kNotifyCallbackFailed
                                       : kMalformedNotification) +
                   e.what();
    } catch (const std::exception& e) {
        LOG(ERROR) << "onNotify failed: " << e.what();
        response = std::string(kNotifyCallbackFailed) + e.what();
    } catch (...) {
        LOG(ERROR) << "onNotify failed with unknown exception";
        response = kNotifyCallbackFailed;
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
                VLOG(1) << "SubscribeSegmentUpdate RPC failed with: "
                        << status.ToString();
                LOG_EVERY_N(ERROR, 100)
                    << "SubscribeSegmentUpdate RPC failed with: "
                    << status.ToString();
            }
        });
}

void ControlClient::notifySegmentUpdatedAsync(
    const std::string& server_addr, const std::string& segment_name,
    const onNotifySegmentUpdateFailure& on_failure) {
    json j = segment_name;
    std::string request = j.dump();
    tl_rpc_agent.callAsyncRetryOnce(
        server_addr, NotifySegmentUpdated, request,
        [on_failure](const Status& status, const std::string&) {
            if (!status.ok()) on_failure();
        });
}

}  // namespace tent
}  // namespace mooncake
