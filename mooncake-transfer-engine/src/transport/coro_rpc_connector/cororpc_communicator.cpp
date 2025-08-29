#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <chrono>
#include <stdexcept>
#include <iostream>

using namespace async_simple::coro;

namespace mooncake {

// Impl类的处理函数实现
std::string CoroRPCCommunicator::Impl::handleDataTransfer(coro_rpc::context<std::string> context, std::string_view data) {
    // 简单回显数据，实际使用中可根据需要修改
    return std::string(data);
}

std::string CoroRPCCommunicator::Impl::handleTensorTransfer(coro_rpc::context<std::string> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();
    // 处理张量数据，这里简单返回接收到的大小信息
    return "received tensor size: " + std::to_string(attachment.size());
}

void CoroRPCCommunicator::Impl::handleDataTransferWithAttachment(coro_rpc::context<void> context, std::string_view data) {
    auto ctx_info = context.get_context_info();
    // 回显附件数据
    ctx_info->set_response_attachment(ctx_info->get_request_attachment());
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransferWithAttachment(coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    // 回显张量附件数据
    ctx_info->set_response_attachment(ctx_info->get_request_attachment());
    context.response_msg();
}

// CoroRPCCommunicator构造函数和析构函数
CoroRPCCommunicator::CoroRPCCommunicator() : impl_(std::make_unique<Impl>()) {}

CoroRPCCommunicator::~CoroRPCCommunicator() {
    stopServer();
}

bool CoroRPCCommunicator::initialize(const Config& config) {
    impl_->config = config;
    
    if (!config.listen_address.empty()) {
        // 初始化服务器
        impl_->server = std::make_unique<coro_rpc::coro_rpc_server>(
            config.thread_count, 
            config.listen_address,
            std::chrono::seconds(config.timeout_seconds)
        );
        
        // 注册处理函数
        impl_->server->register_handler<
            &CoroRPCCommunicator::Impl::handleDataTransfer,
            &CoroRPCCommunicator::Impl::handleTensorTransfer,
            &CoroRPCCommunicator::Impl::handleDataTransferWithAttachment,
            &CoroRPCCommunicator::Impl::handleTensorTransferWithAttachment
        >(impl_.get());
    }
    
    return true;
}

bool CoroRPCCommunicator::startServer() {
    if (!impl_->server) {
        return false;
    }
    
    auto ec = impl_->server->start();
    impl_->is_server_started = (ec.val() == 0);
    return impl_->is_server_started;
}

bool CoroRPCCommunicator::startServerAsync() {
    if (!impl_->server) {
        return false;
    }
    
    auto ec = impl_->server->async_start();
    impl_->is_server_started = !ec.hasResult();
    return impl_->is_server_started;
}

void CoroRPCCommunicator::stopServer() {
    if (impl_->server && impl_->is_server_started) {
        impl_->server.reset();
        impl_->is_server_started = false;
    }
}

int CoroRPCCommunicator::sendData(const std::string& target_address,
                                  const void* data,
                                  size_t data_size) {
    try {
        auto client = std::make_unique<coro_rpc::coro_rpc_client>();
        auto connect_result = syncAwait(client->connect(target_address));
        if (!connect_result) {
            return -1;
        }
        
        std::string_view data_view(static_cast<const char*>(data), data_size);
        auto result = syncAwait(client->call<&CoroRPCCommunicator::Impl::handleDataTransfer>(data_view));
        
        return result.has_value() ? 0 : -1;
    } catch (const std::exception& e) {
        return -1;
    }
}

Lazy<result> CoroRPCCommunicator::sendDataAsync(const std::string& target_address,
                                                 const void* data,
                                                 size_t data_size) {
    result res;
    
    try {
        auto client = std::make_unique<coro_rpc::coro_rpc_client>();
        auto connect_result = co_await client->connect(target_address);
        if (!connect_result) {
            res.code = -1;
            res.err_msg = "connection failed";
            co_return res;
        }
        
        std::string_view data_view(static_cast<const char*>(data), data_size);
        auto result = co_await client->call<&CoroRPCCommunicator::Impl::handleDataTransfer>(data_view);
        
        if (result.has_value()) {
            res.code = 0;
            res.data = result.value();
            res.data_size = res.data.size();
        } else {
            res.code = result.error().val();
            res.err_msg = result.error().msg;
        }
    } catch (const std::exception& e) {
        res.code = -1;
        res.err_msg = e.what();
    }
    
    co_return res;
}

int CoroRPCCommunicator::sendTensor(const std::string& target_address,
                                    const pybind11::object& tensor) {
    try {
        auto client = std::make_unique<coro_rpc::coro_rpc_client>();
        auto connect_result = syncAwait(client->connect(target_address));
        if (!connect_result) {
            return -1;
        }
        
        // 从PyTorch tensor获取数据指针和大小
        uintptr_t data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
        size_t numel = tensor.attr("numel")().cast<size_t>();
        size_t element_size = tensor.attr("element_size")().cast<size_t>();
        size_t tensor_size = numel * element_size;
        
        client->set_req_attachment(std::string_view(reinterpret_cast<char*>(data_ptr), tensor_size));
        auto result = syncAwait(client->call<&CoroRPCCommunicator::Impl::handleTensorTransferWithAttachment>());
        
        return result.has_value() ? 0 : -1;
    } catch (const std::exception& e) {
        return -1;
    }
}

std::future<int> CoroRPCCommunicator::sendTensorAsync(const std::string& target_address,
                                                       const TensorInfo& tensor) {
    auto promise = std::make_shared<std::promise<int>>();
    auto future = promise->get_future();
    
    auto task = [this, target_address, tensor, promise]() -> Lazy<void> {
        try {
            auto client = std::make_unique<coro_rpc::coro_rpc_client>();
            auto connect_result = co_await client->connect(target_address);
            if (!connect_result) {
                promise->set_value(-1);
                co_return;
            }
            
            std::string_view data_view(static_cast<const char*>(tensor.data_ptr), tensor.total_bytes);
            client->set_req_attachment(data_view);
            auto result = co_await client->call<&CoroRPCCommunicator::Impl::handleTensorTransferWithAttachment>();
            
            promise->set_value(result.has_value() ? 0 : -1);
        } catch (const std::exception& e) {
            promise->set_value(-1);
        }
    };
    
    task().start([](auto&&) {});
    
    return future;
}

int CoroRPCCommunicator::receiveData(const std::string& source_address,
                                     void* buffer,
                                     size_t buffer_size,
                                     int timeout_ms) {
    // 这是一个简化实现，实际中可能需要更复杂的接收逻辑
    // 由于coro_rpc主要是请求-响应模式，这里返回不支持
    return -1;
}

Lazy<std::string> CoroRPCCommunicator::receiveDataAsync(const std::string& source_address,
                                                        int timeout_ms) {
    // 这是一个简化实现，实际中可能需要更复杂的接收逻辑
    co_return "";
}

bool CoroRPCCommunicator::addRemoteConnection(const std::string& remote_address) {
    try {
        auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(remote_address);
        impl_->client_pools[remote_address] = pool;
        return true;
    } catch (const std::exception& e) {
        return false;
    }
}

void CoroRPCCommunicator::removeRemoteConnection(const std::string& remote_address) {
    impl_->client_pools.erase(remote_address);
}

bool CoroRPCCommunicator::isConnected(const std::string& remote_address) {
    return impl_->client_pools.find(remote_address) != impl_->client_pools.end();
}

std::string CoroRPCCommunicator::handleDataTransfer(coro_rpc::context<std::string> context, std::string_view data) {
    return impl_->handleDataTransfer(std::move(context), data);
}

std::string CoroRPCCommunicator::handleTensorTransfer(coro_rpc::context<std::string> context) {
    return impl_->handleTensorTransfer(std::move(context));
}

void CoroRPCCommunicator::handleDataTransferWithAttachment(coro_rpc::context<void> context, std::string_view data) {
    impl_->handleDataTransferWithAttachment(std::move(context), data);
}

void CoroRPCCommunicator::handleTensorTransferWithAttachment(coro_rpc::context<void> context) {
    impl_->handleTensorTransferWithAttachment(std::move(context));
}

std::unique_ptr<CoroRPCCommunicator> createClientPool(size_t pool_size, size_t timeout_seconds) {
    auto communicator = std::make_unique<CoroRPCCommunicator>();
    Config config;
    config.pool_size = pool_size;
    config.timeout_seconds = timeout_seconds;
    
    if (communicator->initialize(config)) {
        return communicator;
    }
    return nullptr;
}

std::unique_ptr<CoroRPCCommunicator> createServer(const std::string& listen_address, size_t thread_count) {
    auto communicator = std::make_unique<CoroRPCCommunicator>();
    Config config;
    config.listen_address = listen_address;
    config.thread_count = thread_count;
    
    if (communicator->initialize(config)) {
        return communicator;
    }
    return nullptr;
}

} // namespace mooncake