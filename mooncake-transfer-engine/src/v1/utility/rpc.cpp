// Copyright 2025 KVCache.AI
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

#include "v1/utility/rpc.h"

namespace mooncake {
namespace v1 {

AsioRpcServer::AsioRpcServer() {}

AsioRpcServer::~AsioRpcServer() { stop(); }

Status AsioRpcServer::registerFunction(int func_id, const Function &func) {
    func_map_mutex_.lock();
    func_map_[func_id] = func;
    func_map_mutex_.unlock();
    return Status::OK();
}

Status AsioRpcServer::start(uint16_t &port) {
    const static uint16_t kStartPort = 15000;
    const static uint16_t kPortRange = 2000;
    const static int kMaxRetry = 10;

    if (running_)
        return Status::InvalidArgument(
            "AsioRpcServer: rpc server is already started");

    for (int retry = 0; retry < kMaxRetry; ++retry) {
        if (port == 0) port = kStartPort + SimpleRandom::Get().next(kPortRange);
        try {
            acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(
                io_context_,
                asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port));
            running_ = true;
            worker_ = std::thread([this]() {
                while (running_) {
                    try {
                        doAccept();
                        io_context_.run();
                    } catch (const std::exception &e) {
                        LOG(WARNING) << "[TE RPC] Detect exception in worker: "
                                     << e.what();
                    }
                }
            });
            return Status::OK();
        } catch (const std::exception &e) {
            LOG(WARNING) << "[TE RPC] Port " << port
                         << " is occupied, try other ports";
            port = 0;
        }
    }

    return Status::Socket("AsioRpcServer: no available tcp port");
}

Status AsioRpcServer::stop() {
    if (running_) {
        running_ = false;
        io_context_.stop();
        worker_.join();
    }
    return Status::OK();
}

void AsioRpcServer::doAccept() {
    acceptor_->async_accept(
        [this](asio::error_code ec, asio::ip::tcp::socket socket) {
            if (!ec) readRequest(socket);
            doAccept();
        });
}

struct RequestHeader {
    int func_id;
    int payload_len;
};

struct ResponseHeader {
    int error_code;
    int payload_len;
};

void AsioRpcServer::readRequest(asio::ip::tcp::socket &socket) {
    RequestHeader header;
    if (asio::read(socket, asio::buffer(&header, sizeof(RequestHeader))) !=
        sizeof(RequestHeader)) {
        writeResponse(socket, ErrIncompleted);
        return;
    }

    RpcRawData request(header.payload_len);
    RpcRawData response;
    if (!request.empty()) {
        if (asio::read(socket,
                       asio::buffer(request.data(), header.payload_len)) !=
            (size_t)header.payload_len) {
            writeResponse(socket, ErrIncompleted);
            return;
        }
    }

    if (!func_map_.count(header.func_id)) {
        writeResponse(socket, ErrInvalidFunc);
        return;
    }

    auto func = func_map_.at(header.func_id);
    func(request, response);
    writeResponse(socket, response);
}

void AsioRpcServer::writeResponse(asio::ip::tcp::socket &socket,
                                  int error_code) {
    ResponseHeader header;
    header.error_code = error_code;
    header.payload_len = 0;
    asio::write(socket, asio::buffer(&header, sizeof(ResponseHeader)));
}

void AsioRpcServer::writeResponse(asio::ip::tcp::socket &socket,
                                  const RpcRawData &response) {
    ResponseHeader header;
    header.error_code = OK;
    header.payload_len = (int)response.size();
    asio::write(socket, asio::buffer(&header, sizeof(ResponseHeader)));
    if (!response.empty())
        asio::write(socket, asio::buffer(response.data(), response.size()));
}

AsioRpcClient::AsioRpcClient(const std::string &hostname_port) 
    : socket_(io_context_) {
    size_t pos = hostname_port.find(':');
    if (pos == std::string::npos) return;
    auto hostname = hostname_port.substr(0, pos);
    auto port = hostname_port.substr(pos + 1);
    asio::ip::tcp::resolver resolver(io_context_);
    auto endpoints = resolver.resolve(hostname, port);
    asio::connect(socket_, endpoints);
}

AsioRpcClient::AsioRpcClient(const std::string &hostname, uint16_t port)
    : socket_(io_context_) {
    asio::ip::tcp::resolver resolver(io_context_);
    auto endpoints = resolver.resolve(hostname, std::to_string(port));
    asio::connect(socket_, endpoints);
}

AsioRpcClient::~AsioRpcClient() {}

RpcErrorCode AsioRpcClient::call(int func_id, const RpcRawData &request,
                                 RpcRawData &response) {
    RequestHeader request_header;
    request_header.func_id = func_id;
    request_header.payload_len = (int)request.size();
    if (asio::write(socket_,
                    asio::buffer(&request_header, sizeof(RequestHeader))) !=
        sizeof(RequestHeader)) {
        return ErrIncompleted;
    }
    if (!request.empty()) {
        if (asio::write(socket_,
                        asio::buffer(request.data(), request.size())) !=
            request.size()) {
            return ErrIncompleted;
        }
    }

    ResponseHeader response_header;
    if (asio::read(socket_,
                   asio::buffer(&response_header, sizeof(ResponseHeader))) !=
        sizeof(ResponseHeader)) {
        return ErrIncompleted;
    }

    if (response_header.error_code)
        return (RpcErrorCode)response_header.error_code;

    response.resize(response_header.payload_len);
    if (!response.empty()) {
        if (asio::read(socket_,
                       asio::buffer(response.data(), response.size())) !=
            response.size()) {
            return ErrIncompleted;
        }
    }

    return OK;
}

}  // namespace v1
}  // namespace mooncake