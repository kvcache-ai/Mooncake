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

// TODO the implementation needs to be updated
// - Caching sockets, and supporting multiple calls
// - Keep it lightweighted and tidy

#include "v1/utility/rpc.h"

#include "v1/utility/ip.h"

namespace mooncake {
namespace v1 {

using tcpsocket = asio::ip::tcp::socket;
struct Session : public std::enable_shared_from_this<Session> {
   public:
    struct Header {
        uint16_t func_id;
        uint16_t error_code;
        uint32_t length;
    };

   public:
    explicit Session(tcpsocket socket, AsioRpcServer *server)
        : socket_(std::move(socket)), server_(server) {}

    void start() {
        auto self(shared_from_this());
        asio::async_read(
            socket_, asio::buffer(&request_header_, sizeof(Header)),
            [this, self](const asio::error_code &ec, std::size_t len) {
                if (ec || len != sizeof(Header)) {
                    LOG(ERROR) << "Failed to read header: " << ec.message()
                               << " (" << ec.value() << ")";
                    return;
                }
                if (request_header_.length == 0)
                    process();
                else
                    readContent();
            });
    }

    void readContent() {
        auto self(shared_from_this());
        request_data_.resize(request_header_.length);
        asio::async_read(
            socket_, asio::buffer(request_data_.data(), request_data_.size()),
            [this, self](const asio::error_code &ec, std::size_t len) {
                if (ec || len != request_data_.size()) {
                    LOG(ERROR) << "Failed to read content: " << ec.message()
                               << " (" << ec.value() << ")";
                    return;
                }
                process();
            });
    }

    void process() {
        Header response_header;
        response_header.func_id = request_header_.func_id;
        response_header.error_code = server_->process(
            request_header_.func_id, request_data_, response_data_);
        response_header.length = response_data_.size();
        response_data_.resize(response_header.length + sizeof(Header));
        if (response_header.length)
            memmove(&response_data_[sizeof(Header)], &response_data_[0],
                    response_header.length);
        memmove(&response_data_[0], &response_header, sizeof(Header));
        writeHeaderAndContent();
    }

    void writeHeaderAndContent() {
        auto self(shared_from_this());
        asio::async_write(
            socket_, asio::buffer(response_data_.data(), response_data_.size()),
            [this, self](const asio::error_code &ec, std::size_t len) {
                if (ec || len != response_data_.size()) {
                    LOG(ERROR) << "Failed to write header and content: "
                               << ec.message() << " (" << ec.value() << ")";
                    return;
                }
            });
    }

   private:
    tcpsocket socket_;
    Header request_header_;
    RpcRawData request_data_, response_data_;
    AsioRpcServer *server_;
};

AsioRpcServer::AsioRpcServer() {}

AsioRpcServer::~AsioRpcServer() { stop(); }

Status AsioRpcServer::registerFunction(int func_id, const Function &func) {
    func_map_mutex_.lock();
    func_map_[func_id] = func;
    func_map_mutex_.unlock();
    return Status::OK();
}

Status AsioRpcServer::start(uint16_t &port, bool ipv6) {
    const static uint16_t kStartPort = 15000;
    const static uint16_t kPortRange = 2000;
    const static int kMaxRetry = 10;

    if (running_)
        return Status::InvalidArgument("RPC server already started" LOC_MARK);

    for (int retry = 0; retry < kMaxRetry; ++retry) {
        try {
            if (port == 0)
                port = kStartPort + SimpleRandom::Get().next(kPortRange);

            acceptor_ = std::make_unique<asio::ip::tcp::acceptor>(
                io_context_,
                asio::ip::tcp::endpoint(
                    ipv6 ? asio::ip::tcp::v6() : asio::ip::tcp::v4(), port));

            running_ = true;
            io_context_.restart();  // <-- important
            doAccept();

            worker_ = std::thread([this]() {
                try {
                    io_context_.run();
                } catch (const std::exception &e) {
                    LOG(ERROR) << "RPC Server run exception: " << e.what();
                }
            });
            return Status::OK();
        } catch (const std::exception &e) {
            LOG(WARNING) << "Failed to start RPC server on port " << port
                         << ": " << e.what();
            port = 0;
            acceptor_.reset();
        }
    }

    return Status::RpcServiceError("Failed to bind any RPC port" LOC_MARK);
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
            if (!ec) {
                std::make_shared<Session>(std::move(socket), this)->start();
            }
            doAccept();
        });
}

int AsioRpcServer::process(int func_id, const RpcRawData &request,
                           RpcRawData &response) {
    if (!func_map_.count(func_id)) return ErrInvalidFunc;
    auto func = func_map_.at(func_id);
    func(request, response);
    return 0;
}

Status AsioRpcClient::Call(const std::string &server_addr, int func_id,
                           const RpcRawData &request, RpcRawData &response) {
    const static int maxRetries = 6;
    uint64_t backoff = 50;
    for (int retry = 0; retry < maxRetries; ++retry) {
        auto code = do_call(server_addr, func_id, request, response);
        if (code == OK) return Status::OK();
        if (code != ErrConnectFailed)
            return Status::RpcServiceError("RPC error code " +
                                           std::to_string(code));
        auto real_backoff = backoff + SimpleRandom::Get().next(backoff / 5);
        std::this_thread::sleep_for(std::chrono::milliseconds(real_backoff));
        backoff *= 2;
    }
    return Status::RpcServiceError("RPC connection failed");
}

RpcErrorCode AsioRpcClient::do_call(const std::string &server_addr, int func_id,
                                    const RpcRawData &request,
                                    RpcRawData &response) {
    try {
        asio::io_context local_context;
        asio::ip::tcp::resolver resolver(local_context);
        auto result = parseHostNameWithPort(server_addr, 0);
        if (result.second == 0) return ErrConnectFailed;

        auto endpoints =
            resolver.resolve(result.first, std::to_string(result.second));

        asio::ip::tcp::socket socket(local_context);
        asio::connect(socket, endpoints);

        Session::Header request_header{static_cast<uint16_t>(func_id), 0,
                                       static_cast<uint32_t>(request.size())};

        std::vector<asio::const_buffer> buffers;
        buffers.push_back(
            asio::buffer(&request_header, sizeof(request_header)));
        if (!request.empty()) {
            buffers.push_back(asio::buffer(request));
        }

        asio::write(socket, buffers);

        Session::Header response_header;
        asio::read(socket,
                   asio::buffer(&response_header, sizeof(response_header)));

        if (response_header.error_code)
            return static_cast<RpcErrorCode>(response_header.error_code);

        response.resize(response_header.length);
        if (!response.empty()) {
            asio::read(socket, asio::buffer(response));
        }

        return OK;
    } catch (const std::exception &e) {
        LOG(ERROR) << "RPC client exception: " << e.what();
        return ErrConnectFailed;
    }
}

}  // namespace v1
}  // namespace mooncake