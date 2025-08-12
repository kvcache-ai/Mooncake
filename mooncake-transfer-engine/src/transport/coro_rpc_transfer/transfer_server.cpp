#ifndef TRANSFER_SERVER_H
#define TRANSFER_SERVER_H

#include "transport/coro_rpc_transfer/transfer_server.h"

#include <chrono>
#include <cstdint>
#include <system_error>
#include <thread>
#include <iostream>
#include <string_view>

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/embed.h>
#include <ylt/easylog.hpp>

#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/Sleep.h"
#include "ylt/coro_io/client_pool.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/coro_rpc/impl/expected.hpp"
#include "ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp"

#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

using namespace coro_rpc;
using namespace async_simple::coro;
using namespace std::chrono_literals;

namespace py11 = pybind11;

// 定义echo函数
std::string echo(std::string_view data) {
    std::cout << "Echo function called with data: \"" << data << "\"" << std::endl;
    return std::string(data);
}
void echo_with_attachment() {
    auto ctx = coro_rpc::get_context();
    ctx->set_response_attachment(
        ctx->get_request_attachment()
    ); 
}

int add(int a, int b) {
    std::cout << "Add function called: " << a << " + " << b << " = " << (a + b) << std::endl;
    return a + b;
}

// HelloService类的定义（如果需要的话）
class HelloService {
public:
    std::string hello(const std::string& name) {
        std::string response = "Hello, " + name + "!";
        std::cout << "HelloService::hello called with: \"" << name << "\", response: \"" << response << "\"" << std::endl;
        return response;
    }
};

int main() {
    std::cout << "Starting RPC server..." << std::endl;
    
    // init rpc server
    coro_rpc_server server(/*thread=*/std::thread::hardware_concurrency(),
                           /*port=*/8801);

    coro_rpc_server server2{/*thread=*/1, /*port=*/8802};

    // regist normal function for rpc
    server.register_handler<echo_with_attachment, add>();

    // regist member function for rpc
    HelloService hello_service;
    server.register_handler<&HelloService::hello>(&hello_service);

    server2.register_handler<echo>();
    
    std::cout << "Registered handlers:" << std::endl;
    std::cout << "  - echo_with_attachment" << std::endl;
    std::cout << "  - add" << std::endl;
    std::cout << "  - HelloService::hello" << std::endl;
    std::cout << "  - echo (on server2)" << std::endl;
    
    // async start server
    auto res = server2.async_start();
    assert(!res.hasResult());

    // start an http server in same port(8801) of rpc server
    auto http_server = std::make_unique<coro_http::coro_http_server>(0, 0);
    http_server->set_http_handler<coro_http::GET>(
        "/", [](coro_http::coro_http_request& req,
                coro_http::coro_http_response& resp) {
            resp.set_status_and_content(coro_http::status_type::ok,
                                        R"(<!DOCTYPE html>
<html>
    <head>
        <title>RPC Server</title>
    </head>
    <body>
        <h1>RPC Server Running</h1>
        <p>Server is running on port 8801 (with HTTP) and 8802 (RPC only)</p>
        <p>Available RPC functions:</p>
        <ul>
            <li>echo_with_attachment</li>
            <li>add</li>
            <li>hello</li>
            <li>echo</li>
        </ul>
    </body>
</html>)");
        });
        
    std::function dispatcher = [](coro_io::socket_wrapper_t&& soc,
                                  std::string_view magic_number,
                                  coro_http::coro_http_server& server) {
        server.transfer_connection(std::move(soc), magic_number);
    };
    server.add_subserver(std::move(dispatcher), std::move(http_server));

    std::cout << "Server starting on ports 8801 (HTTP+RPC) and 8802 (RPC)" << std::endl;
    std::cout << "Visit http://localhost:8801/ to see the web interface" << std::endl;
    
    // sync start server & sync await server stop
    bool success = server.start();
    if (success) {
        std::cout << "Server started successfully" << std::endl;
    } else {
        std::cerr << "Failed to start server" << std::endl;
    }
    
    return !success;
}

#endif // TRANSFER_SERVER_H