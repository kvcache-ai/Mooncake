#include "transport/coro_rpc_connector/cororpc_communicator.h"
#include <iostream>
#include <thread>
#include <functional>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_io/client_pool.hpp>
#include "async_simple/coro/SyncAwait.h"

namespace mooncake {

CoroRPCCommunicator::CoroRPCCommunicator() : impl_(std::make_shared<Impl>()) {}

CoroRPCCommunicator::~CoroRPCCommunicator() {
    stopServer();
}

void CoroRPCCommunicator::setDataReceiveCallback(std::function<void(const std::string&, const std::string&)> callback) {
    std::cout << "Setting data receive callback..." << std::endl;
    impl_->data_receive_callback = callback;
    std::cout << "Data receive callback set successfully" << std::endl;
}

bool CoroRPCCommunicator::initialize(const Config& config) {
    impl_->config = config;
    
    if (!config.listen_address.empty()) {
        std::cout << "Initializing server on " << config.listen_address << std::endl;
        
        impl_->server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            config.thread_count, 
            config.listen_address,
            std::chrono::seconds(config.timeout_seconds)
        );
        
        impl_->server_->register_handler<&CoroRPCCommunicator::Impl::handleDataTransfer,
                                        &CoroRPCCommunicator::Impl::handleTensorTransfer>(impl_.get());
    }
    
    std::cout << "Communicator initialized with client pool support" << std::endl;
    return true;
}

bool CoroRPCCommunicator::startServer() {
    if (!impl_->server_ || impl_->config.listen_address.empty()) return false;
    
    try {
        auto ec = impl_->server_->start();
        if (ec.val() == 0) {
            impl_->is_server_started = true;
            std::cout << "Server started on " << impl_->config.listen_address << std::endl;
            return true;
        } else {
            std::cerr << "Failed to start server: " << ec.message() << std::endl;
            return false;
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to start server: " << e.what() << std::endl;
        return false;
    }
}

bool CoroRPCCommunicator::startServerAsync() {
    if (!impl_->server_ || impl_->config.listen_address.empty()) return false;
    
    try {
        auto ec = impl_->server_->async_start();
        if (!ec.hasResult()) {
            impl_->is_server_started = true;
            std::cout << "Server started asynchronously on " << impl_->config.listen_address << std::endl;
            return true;
        } else {
            std::cerr << "Failed to start server asynchronously" << std::endl;
            return false;
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to start server asynchronously: " << e.what() << std::endl;
        return false;
    }
}

void CoroRPCCommunicator::stopServer() {
    if (impl_->is_server_started) {
        impl_->is_server_started = false;
        std::cout << "Server stopped" << std::endl;
    }
}

bool CoroRPCCommunicator::addRemoteConnection(const std::string& remote_address) {
    try {
        // Check if client pool already exists for this address
        auto it = impl_->client_pools_.find(remote_address);
        if (it != impl_->client_pools_.end()) {
            std::cout << "Client pool for " << remote_address << " already exists" << std::endl;
            return true;
        }
        
        // Create new client pool for this remote address
        auto client_pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(remote_address);
        
        if (!client_pool) {
            std::cerr << "Failed to create client pool for " << remote_address << std::endl;
            return false;
        }
        
        impl_->client_pools_[remote_address] = client_pool;
        std::cout << "Successfully created client pool for " << remote_address 
                  << " with pool size: " << impl_->config.pool_size << std::endl;
        return true;
        
    } catch (const std::exception& e) {
        std::cerr << "Exception while creating client pool for " << remote_address << ": " << e.what() << std::endl;
        return false;
    }
}

void CoroRPCCommunicator::removeRemoteConnection(const std::string& remote_address) {
    auto it = impl_->client_pools_.find(remote_address);
    if (it != impl_->client_pools_.end()) {
        impl_->client_pools_.erase(it);
        std::cout << "Removed client pool for " << remote_address << std::endl;
    } else {
        std::cout << "No client pool found for " << remote_address << std::endl;
    }
}

bool CoroRPCCommunicator::isConnected(const std::string& remote_address) {
    auto it = impl_->client_pools_.find(remote_address);
    if (it != impl_->client_pools_.end()) {
        // Client pool exists, assume it can provide connections
        return true;
    }
    return false;
}

int CoroRPCCommunicator::sendData(const std::string& target_address,
                                  const void* data,
                                  size_t data_size) {
    try {
        // Ensure client pool exists for this target
        if (impl_->client_pools_.find(target_address) == impl_->client_pools_.end()) {
            if (!addRemoteConnection(target_address)) {
                std::cerr << "Failed to create client pool for " << target_address << std::endl;
                return -1;
            }
        }
        
        auto& client_pool = impl_->client_pools_[target_address];
        
        // Use promise/future to convert async operation to sync
        auto promise = std::make_shared<std::promise<int>>();
        auto future = promise->get_future();
        
        client_pool->send_request(
            [data, data_size, promise](coro_rpc::coro_rpc_client &client) 
                -> async_simple::coro::Lazy<void> {
                
                std::string_view data_view(static_cast<const char*>(data), data_size);
                auto result = co_await client.call<&CoroRPCCommunicator::Impl::handleDataTransfer>(data_view);
                
                if (result.has_value()) {
                    promise->set_value(0);
                } else {
                    std::cerr << "RPC call failed: " << result.error().msg << std::endl;
                    promise->set_value(-1);
                }
            }
        ).start([](auto &&) {});
        
        int result = future.get();
        
        if (result == 0) {
            std::cout << "Successfully sent " << data_size << " bytes to " << target_address << std::endl;
        }
        
        return result;
    } catch (const std::exception& e) {
        std::cerr << "Send data error: " << e.what() << std::endl;
        return -1;
    }
}

std::future<result> CoroRPCCommunicator::sendDataAsync(const std::string& target_address,
                                                       const void* data,
                                                       size_t data_size) {
    auto promise = std::make_shared<std::promise<result>>();
    auto future = promise->get_future();
    
    // Ensure client pool exists for this target
    if (impl_->client_pools_.find(target_address) == impl_->client_pools_.end()) {
        if (!addRemoteConnection(target_address)) {
            result res;
            res.code = -1;
            res.err_msg = "Failed to create client pool for " + target_address;
            promise->set_value(res);
            return future;
        }
    }
    
    auto& client_pool = impl_->client_pools_[target_address];
    
    client_pool->send_request(
        [data, data_size, promise](coro_rpc::coro_rpc_client &client) 
            -> async_simple::coro::Lazy<void> {
            
            std::string_view data_view(static_cast<const char*>(data), data_size);
            auto rpc_result = co_await client.call<&CoroRPCCommunicator::Impl::handleDataTransfer>(data_view);
            
            result res;
            if (rpc_result.has_value()) {
                res.code = 0;
            } else {
                res.code = rpc_result.error().val();
                res.err_msg = rpc_result.error().msg;
            }
            
            promise->set_value(res);
        }
    ).start([](auto &&) {});
    
    return future;
}

int CoroRPCCommunicator::sendTensor(const std::string& target_address, const pybind11::object& tensor) {
    try {
        // Ensure client pool exists for this target
        if (impl_->client_pools_.find(target_address) == impl_->client_pools_.end()) {
            if (!addRemoteConnection(target_address)) {
                std::cerr << "Failed to create client pool for " << target_address << std::endl;
                return -1;
            }
        }
        
        auto& client_pool = impl_->client_pools_[target_address];
        
        // Use promise/future to convert async operation to sync
        auto promise = std::make_shared<std::promise<int>>();
        auto future = promise->get_future();
        
        uintptr_t data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
        size_t numel = tensor.attr("numel")().cast<size_t>();
        size_t element_size = tensor.attr("element_size")().cast<size_t>();
        size_t tensor_size = numel * element_size;
        
        client_pool->send_request(
            [data_ptr, tensor_size, promise](coro_rpc::coro_rpc_client &client) 
                -> async_simple::coro::Lazy<void> {
                
                client.set_req_attachment(std::string_view((char*)data_ptr, tensor_size));
                
                auto result = co_await client.call<&CoroRPCCommunicator::Impl::handleTensorTransfer>();
                
                if (result.has_value()) {
                    promise->set_value(0);
                } else {
                    std::cerr << "Tensor RPC call failed: " << result.error().msg << std::endl;
                    promise->set_value(-1);
                }
            }
        ).start([](auto &&) {});
        
        int result = future.get();
        
        if (result == 0) {
            std::cout << "Successfully sent tensor to " << target_address << std::endl;
        }
        
        return result;
    } catch (const std::exception& e) {
        std::cerr << "Send tensor error: " << e.what() << std::endl;
        return -1;
    }
}

std::future<int> CoroRPCCommunicator::sendTensorAsync(const std::string& target_address, const TensorInfo& tensor) {
    auto promise = std::make_shared<std::promise<int>>();
    auto future = promise->get_future();
    
    // Ensure client pool exists for this target
    if (impl_->client_pools_.find(target_address) == impl_->client_pools_.end()) {
        if (!addRemoteConnection(target_address)) {
            promise->set_value(-1);
            return future;
        }
    }
    
    auto& client_pool = impl_->client_pools_[target_address];
    
    client_pool->send_request(
        [tensor, promise](coro_rpc::coro_rpc_client &client) 
            -> async_simple::coro::Lazy<void> {
            
            client.set_req_attachment(std::string_view((char*)tensor.data_ptr, tensor.total_bytes));
            
            auto result = co_await client.call<&CoroRPCCommunicator::Impl::handleTensorTransfer>();
            
            if (result.has_value()) {
                promise->set_value(0);
            } else {
                std::cerr << "Async tensor RPC call failed: " << result.error().msg << std::endl;
                promise->set_value(-1);
            }
        }
    ).start([](auto &&) {});
    
    return future;
}

int CoroRPCCommunicator::receiveData(const std::string& source_address, void* buffer, size_t buffer_size, int timeout_ms) {
    return 0;
}

std::future<std::string> CoroRPCCommunicator::receiveDataAsync(const std::string& source_address, int timeout_ms) {
    auto promise = std::make_shared<std::promise<std::string>>();
    auto future = promise->get_future();
    
    std::thread([promise]() {
        promise->set_value(std::string());
    }).detach();
    
    return future;
}

void CoroRPCCommunicator::Impl::handleDataTransfer(coro_rpc::context<void> context, std::string_view data) {
    std::cout << "Handling data transfer: " << data.size() << " bytes" << std::endl;
    
    // Call the data receive callback if set
    if (data_receive_callback) {
        std::cout << "Calling data receive callback..." << std::endl;
        std::string source_address = "unknown"; // You may want to extract this from context
        std::string data_str(data);
        data_receive_callback(source_address, data_str);
    } else {
        std::cout << "No data receive callback set!" << std::endl;
    }
    
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransfer(coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();
    
    std::cout << "Handling tensor transfer: " << attachment.size() << " bytes" << std::endl;
    
    ctx_info->set_response_attachment(attachment);
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleDataTransferWithAttachment(coro_rpc::context<void> context, std::string_view data) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();
    
    std::cout << "Handling data transfer with attachment - Data: " << data.size() 
              << " bytes, Attachment: " << attachment.size() << " bytes" << std::endl;
    
    
    context.response_msg();
}

void CoroRPCCommunicator::Impl::handleTensorTransferWithAttachment(coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    auto attachment = ctx_info->get_request_attachment();
    
    std::cout << "Handling tensor transfer with attachment: " << attachment.size() << " bytes" << std::endl;
    
    ctx_info->set_response_attachment(attachment);
    context.response_msg();
}

std::unique_ptr<CoroRPCCommunicator> createClientPool(size_t pool_size, size_t timeout_seconds) {
    Config config;
    config.pool_size = pool_size;
    config.timeout_seconds = timeout_seconds;
    
    auto communicator = std::make_unique<CoroRPCCommunicator>();
    if (communicator->initialize(config)) {
        std::cout << "Created communicator with default pool size: " << pool_size << std::endl;
        return communicator;
    }
    return nullptr;
}

std::unique_ptr<CoroRPCCommunicator> createServer(const std::string& listen_address, size_t thread_count) {
    Config config;
    config.listen_address = listen_address;
    config.thread_count = thread_count;
    config.pool_size = 10; // Default pool size for server-side client pools
    
    auto communicator = std::make_unique<CoroRPCCommunicator>();
    if (communicator->initialize(config)) {
        std::cout << "Created server communicator with pool size: " << config.pool_size << std::endl;
        return communicator;
    }
    return nullptr;
}

} // namespace mooncake