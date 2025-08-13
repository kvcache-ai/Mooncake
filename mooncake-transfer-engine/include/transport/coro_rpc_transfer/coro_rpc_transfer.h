#ifndef CORO_RPC_TRANSFER_H
#define CORO_RPC_TRANSFER_H

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/buffer_info.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <array>
#include <functional>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <string_view>
#include <cstdint>

namespace py = pybind11;

extern py::module_ torch;

enum class TensorDtype : int32_t {
    FLOAT32 = 0, FLOAT64 = 1, INT8 = 2, UINT8 = 3, INT16 = 4, UINT16 = 5,
    INT32 = 6, UINT32 = 7, INT64 = 8, UINT64 = 9, BOOL = 10,
    FLOAT16 = 11, BFLOAT16 = 12, FLOAT8_E4M3 = 13, FLOAT8_E5M2 = 14, W8A8 = 15,
    UNKNOWN = -1
};

struct tensor_metadata {
    int32_t size;
    std::vector<size_t> shape;
    TensorDtype dtype;
};

using ArrayCreatorFunc = std::function<py::array(char *, size_t, size_t)>;

namespace detail {
    template <typename T>
    py::array create_typed_array(char *exported_data, size_t offset, size_t total_length);

    extern const std::array<ArrayCreatorFunc, 16> array_creators;
}

class py_rpc_context {
 public:
    void response_msg(py::buffer msg, py::handle done);
    struct ContextInfo {
        void set_response_attachment(std::string_view);
        void set_complete_handler(std::function<void(const std::error_code &, std::size_t)>);
        std::string get_request_attachment();
        std::shared_ptr<ContextInfo> get_context_info();
    };
    struct context {
        std::shared_ptr<ContextInfo> get_context_info();
        void response_msg();
    };
    context context_;
};

TensorDtype get_tensor_dtype(py::object dtype_obj);

class string_holder {
 public:
    string_holder(std::string val);
    py::object str_view(uint64_t data_size);
 private:
    std::string value;
};

struct rpc_result {
    int code;
    std::string err_msg;
    std::shared_ptr<string_holder> data_ptr;
    uint64_t data_size;
    py::object str_view();
};

class py_coro_rpc_client_pool {
 public:
    py_coro_rpc_client_pool(std::string url);
    pybind11::object async_send_msg_with_outbuf(py::handle loop, py::handle py_bytes, py::buffer out_buf);
    pybind11::object async_send_msg(py::handle loop, py::handle py_bytes);
    pybind11::object async_send_tensor(py::handle loop, py::handle tensor_handle);
 private:
    std::shared_ptr<void> pool_;
    void* client_;
};

class py_coro_rpc_server {
public:
    py_coro_rpc_server(size_t thd_num, std::string address, py::handle py_callback, size_t seconds);
    bool start();
    bool async_start();
private:
    void handle_msg(void* context, std::string_view msg);
    void reflect_tensor(void* context);
    void handle_tensor(void* context, tensor_metadata metadata);
    void* server_;
    py::handle py_callback_;
};

#endif // CORO_RPC_TRANSFER_H