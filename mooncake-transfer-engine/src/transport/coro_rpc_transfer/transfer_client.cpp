#ifndef TRANSFER_CLIENT_H
#define TRANSFER_CLIENT_H

#include "transport/coro_rpc_transfer/transfer_client.h"

#include <chrono>
#include <cstdint>
#include <system_error>
#include <thread>
#include <iostream>
#include <string_view>

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/embed.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
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

using namespace coro_rpc;
using namespace async_simple::coro;
using namespace std::chrono_literals;

namespace py11 = pybind11;

void echo_with_attachment(coro_rpc::context<void> ctx) { 
    try {
        if (!Py_IsInitialized()) {
            py11::initialize_interpreter();
        }
        coro_rpc_client client;
        
        auto tensor = py11::object(py11::module_::import("torch").attr("randn")(py11::make_tuple(2, 3)));
        
        tensor = tensor.attr("contiguous")();
        
        uintptr_t data_ptr = tensor.attr("data_ptr")().cast<uintptr_t>();
        size_t numel = tensor.attr("numel")().cast<size_t>();
        size_t element_size = tensor.attr("element_size")().cast<size_t>();
        size_t total_bytes = numel * element_size;
        
        std::string_view tensor_view(
            reinterpret_cast<const char*>(data_ptr), 
            total_bytes
        );
        std::string test_string = "Hello, World!";
        //client.set_req_attachment(tensor_view);
        client.set_req_attachment(test_string);
        ctx.get_context_info() -> set_response_attachment(ctx.get_context_info()->get_request_attachment());

        std::cout << "Successfully processed tensor with " << total_bytes << " bytes" << std::endl;
        
    } catch (const py11::error_already_set& e) {
        std::cerr << "Python error: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }
}

async_simple::coro::Lazy<void> simple_rpc_simulation() {
    try {
        coro_rpc_client client;
        co_await client.connect("127.0.0.1:8801");
        
        std::string test_data = "Hello World!";
        std::string_view test_data_view(test_data.data(), test_data.size());
        std::cout << "Test data: \"" << test_data << "\"" << std::endl;
        
        client.set_req_attachment(test_data_view);
        auto ret_void = co_await client.call<echo_with_attachment>();
        std::cout << "Request attachment (size: " << test_data.size() << " bytes)" << std::endl;
    
        auto resp_attachment =  client.get_resp_attachment();
        std::cout << "Response attachment received: \"" << resp_attachment << "\"" << std::endl;
        assert(resp_attachment == test_data);

        std::cout << "Simple RPC simulation completed" << std::endl;
    }
     catch (const std::exception& e) {
        std::cerr << "Simple RPC error: " << e.what() << std::endl;
    }
}

int main() {
    try {
        //coro_rpc::context<void> ctx;
        //echo_with_attachment(ctx);
        async_simple::coro::syncAwait(simple_rpc_simulation());
        
        if (Py_IsInitialized()) {
            py11::finalize_interpreter();
        }
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Main error: " << e.what() << std::endl;
        return 1;
    }
}

#endif // TRANSFER_CLIENT_H