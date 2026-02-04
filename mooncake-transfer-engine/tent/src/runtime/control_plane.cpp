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

#include "tent/common/status.h"
#include "tent/common/utils/os.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/segment_registry.h"

namespace mooncake {
namespace tent {
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
    std::string request, response;
    XferDataDesc desc{htole64(peer_mem_addr), htole64(length)};
    request.resize(sizeof(XferDataDesc) + length);
    memcpy(&request[0], &desc, sizeof(desc));
    Platform::getLoader().copy(&request[sizeof(desc)], local_mem_addr, length);
    auto status = tl_rpc_agent.call(server_addr, SendData, request, response);
    if (!status.ok()) return status;
    if (!response.empty())
        return Status::RpcServiceError(response);
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

ControlService::ControlService(const std::string& type,
                               const std::string& servers,
                               TransferEngineImpl* impl)
    : ControlService(type, servers, "", 0, impl) {}

ControlService::ControlService(const std::string& type,
                               const std::string& servers,
                               const std::string& password, uint8_t db_index,
                               TransferEngineImpl* impl)
    : bootstrap_callback_(nullptr), notify_callback_(nullptr), impl_(impl) {
    if (type == "p2p") {
        auto agent = std::make_unique<PeerSegmentRegistry>();
        manager_ = std::make_unique<SegmentManager>(std::move(agent));
    } else {
        auto agent = std::make_unique<CentralSegmentRegistry>(
            type, servers, password, db_index);
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
    rpc_server_->registerFunction(
        SendData,
        [this](const std::string_view& request, std::string& response) {
            onSendData(request, response);
        });
    rpc_server_->registerFunction(
        RecvData,
        [this](const std::string_view& request, std::string& response) {
            onRecvData(request, response);
        });
    rpc_server_->registerFunction(
        Notify, [this](const std::string_view& request, std::string& response) {
            onNotify(request, response);
        });
    rpc_server_->registerFunction(
        Delegate,
        [this](const std::string_view& request, std::string& response) {
            onDelegate(request, response);
        });
    rpc_server_->registerFunction(
        Pin, [this](const std::string_view& request, std::string& response) {
            onPinStageBuffer(request, response);
        });
    rpc_server_->registerFunction(
        Unpin, [this](const std::string_view& request, std::string& response) {
            onUnpinStageBuffer(request, response);
        });
}

ControlService::~ControlService() {}

Status ControlService::start(uint16_t& port, bool ipv6_) {
    return rpc_server_->start(port, ipv6_);
}

void ControlService::onGetSegmentDesc(const std::string_view& request,
                                      std::string& response) {
    json j = *manager_->getLocal();
    response = j.dump();
}

void ControlService::onBootstrapRdma(const std::string_view& request,
                                     std::string& response) {
    std::string mutable_request(request);
    BootstrapDesc request_desc =
        json::parse(std::string(request)).get<BootstrapDesc>();
    BootstrapDesc response_desc;
    if (bootstrap_callback_) bootstrap_callback_(request_desc, response_desc);
    json j = response_desc;
    response = j.dump();
}

void ControlService::onSendData(const std::string_view& request,
                                std::string& response) {
    XferDataDesc* desc = (XferDataDesc*)request.data();
    auto local_desc = manager_->getLocal().get();
    auto peer_mem_addr = le64toh(desc->peer_mem_addr);
    auto length = le64toh(desc->length);
    if (local_desc->findBuffer(peer_mem_addr, length)) {
        Platform::getLoader().copy((void*)peer_mem_addr, &desc[1], length);
    } else {
        response = "SendData failed: target address not in registered buffer";
    }
}

void ControlService::onRecvData(const std::string_view& request,
                                std::string& response) {
    XferDataDesc* desc = (XferDataDesc*)request.data();
    auto local_desc = manager_->getLocal().get();
    auto peer_mem_addr = le64toh(desc->peer_mem_addr);
    auto length = le64toh(desc->length);
    if (local_desc->findBuffer(peer_mem_addr, length)) {
        response.resize(length);
        Platform::getLoader().copy(response.data(), (void*)peer_mem_addr,
                                   length);
    }
}

void ControlService::onNotify(const std::string_view& request,
                              std::string& response) {
    Notification message = json::parse(request).get<Notification>();
    if (notify_callback_) notify_callback_(message);
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

}  // namespace tent
}  // namespace mooncake
