#ifndef CORO_RPC_TRANSFER_H
#define CORO_RPC_TRANSFER_H

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>
#include <pybind11/buffer_info.h>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "async_simple/coro/SyncAwait.h"

namespace py = pybind11;

struct tensor_metadata {
    int32_t size;
    std::vector<size_t> shape;
    int32_t dtype;
};

class py_rpc_context {
 public:
  void response_msg(py::buffer msg, py::handle done) {
    py::buffer_info info = msg.request();
    const char *data = static_cast<char *>(info.ptr);
    context_.get_context_info()->set_response_attachment(
        std::string_view(data, info.size));
    done.inc_ref();
    context_.get_context_info()->set_complete_handler(
        [done](const std::error_code &ec, std::size_t) {
          py::gil_scoped_acquire acquire;
          done(!ec);
          done.dec_ref();
        });
    context_.response_msg();
  }

  coro_rpc::context<void> context_;
};

class py_coro_rpc_client_pool;
class py_coro_rpc_server {
 private:
  py::module_ torch = py::module_::import("torch");

  enum class TensorDtype : int32_t {
    FLOAT32 = 0,
    FLOAT64 = 1,
    INT8 = 2,
    UINT8 = 3,
    INT16 = 4,
    UINT16 = 5,
    INT32 = 6,
    UINT32 = 7,
    INT64 = 8,
    UINT64 = 9,
    BOOL = 10,
    FLOAT16 = 11,
    BFLOAT16 = 12,
    FLOAT8_E4M3 = 13,
    FLOAT8_E5M2 = 14,
    W8A8 = 15,
    UNKNOWN = -1
};

template <typename T>
py::array create_typed_array(char *exported_data, size_t offset,
                             size_t total_length) {
    py::capsule free_when_done(
        exported_data, [](void *p) { delete[] static_cast<char *>(p); });
    return py::array_t<T>({static_cast<ssize_t>(total_length / sizeof(T))},
                          (T *)(exported_data + offset), free_when_done);
}

using ArrayCreatorFunc = std::function<py::array(char *, size_t, size_t)>;

static const std::array<ArrayCreatorFunc, 16> array_creators = {{
    create_typed_array<float>,     // FLOAT32 = 0
    create_typed_array<double>,    // FLOAT64 = 1
    create_typed_array<int8_t>,    // INT8 = 2
    create_typed_array<uint8_t>,   // UINT8 = 3
    create_typed_array<int16_t>,   // INT16 = 4
    create_typed_array<uint16_t>,  // UINT16 = 5
    create_typed_array<int32_t>,   // INT32 = 6
    create_typed_array<uint32_t>,  // UINT32 = 7
    create_typed_array<int64_t>,   // INT64 = 8
    create_typed_array<uint64_t>,  // UINT64 = 9
    create_typed_array<bool>,      // BOOL = 10
    create_typed_array<uint16_t>,  // FLOAT16 = 11 (using uint16_t as storage)
    create_typed_array<uint16_t>,  // BFLOAT16 = 12 (using uint16_t as storage)
    create_typed_array<uint8_t>,  // FLOAT8_E4M3 = 13 (using uint8_t as storage)
    create_typed_array<uint8_t>,  // FLOAT8_E5M2 = 14 (using uint8_t as storage)
    create_typed_array<int8_t>    // W8A8 = 15 (using int8_t as storage)
}};

inline TensorDtype get_tensor_dtype(py::object dtype_obj) {
    if (dtype_obj.is_none()) {
        return TensorDtype::UNKNOWN;
    }

    if (dtype_obj.equal(torch.attr("float32"))) return TensorDtype::FLOAT32;
    if (dtype_obj.equal(torch.attr("float64"))) return TensorDtype::FLOAT64;
    if (dtype_obj.equal(torch.attr("int8"))) return TensorDtype::INT8;
    if (dtype_obj.equal(torch.attr("uint8"))) return TensorDtype::UINT8;
    if (dtype_obj.equal(torch.attr("int16"))) return TensorDtype::INT16;
    if (dtype_obj.equal(torch.attr("uint16"))) return TensorDtype::UINT16;
    if (dtype_obj.equal(torch.attr("int32"))) return TensorDtype::INT32;
    if (dtype_obj.equal(torch.attr("uint32"))) return TensorDtype::UINT32;
    if (dtype_obj.equal(torch.attr("int64"))) return TensorDtype::INT64;
    if (dtype_obj.equal(torch.attr("uint64"))) return TensorDtype::UINT64;
    if (dtype_obj.equal(torch.attr("bool"))) return TensorDtype::BOOL;
    if (dtype_obj.equal(torch.attr("float16"))) return TensorDtype::FLOAT16;
    if (dtype_obj.equal(torch.attr("bfloat16"))) return TensorDtype::BFLOAT16;
    if (dtype_obj.equal(torch.attr("float8_e4m3fn")))
        return TensorDtype::FLOAT8_E4M3;
    if (dtype_obj.equal(torch.attr("float8_e5m2")))
        return TensorDtype::FLOAT8_E5M2;
    if (dtype_obj.equal(torch.attr("w8a8"))) return TensorDtype::W8A8;

    return TensorDtype::UNKNOWN;
}
 public:
  py_coro_rpc_server(size_t thd_num, std::string address,
                     py::handle py_callback, size_t seconds)
      : server_(thd_num, address, std::chrono::seconds(seconds)),
        py_callback_(py_callback) {
    server_.register_handler<&py_coro_rpc_server::handle_msg,
                             &py_coro_rpc_server::handle_tensor,
                             &py_coro_rpc_server::reflect_tensor>(this);
  }

  bool start() {
    auto ec = server_.start();
    return ec.val() == 0;
  }

  bool async_start() {
    auto ec = server_.async_start();
    return !ec.hasResult();
  }

 private:
  friend class py_coro_rpc_client_pool;
  void handle_msg(coro_rpc::context<void> context, std::string_view msg) {
    py_rpc_context t{};
    t.context_ = std::move(context);
    py::gil_scoped_acquire acquire;
    auto view = py::memoryview::from_buffer(msg.data(), {msg.size()},
                                            {sizeof(uint8_t)});
    py_callback_(std::move(t), view);
  }

  void reflect_tensor(coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    ctx_info->set_response_attachment(ctx_info->get_request_attachment());
    context.response_msg();
  }

  void handle_tensor(coro_rpc::context<void> context, tensor_metadata metadata){
    auto ctx_info = context.get_context_info();
    auto data = ctx_info->get_request_attachment();
    
    auto shape = metadata.shape;
    auto dtype = metadata.dtype;
    auto size = metadata.size;

    try {
        if (data.size() != size) {
            std::string error_msg = "Data size mismatch: expected " + 
                                  std::to_string(size) + ", got " + 
                                  std::to_string(data.size());
            ctx_info->set_response_attachment(error_msg);
            context.response_msg();
            return;
        }

        py::tuple py_shape = py::cast(shape);

        int dtype_index = static_cast<int>(dtype);
         if (dtype_index >= 0){
            auto np_array = array_creators[dtype_index]((char*)data.data(), 0, data.size());
         } else {
            std::string error_msg = "Unsupported dtype: " + std::to_string(dtype);
            ctx_info->set_response_attachment(error_msg);
            context.response_msg();
            return;
         }
         np_array = np_array.attr("reshape")(py_shape);
         py::gil_scoped_acquire acquire_gil;
         pybind11::object tensor = torch.attr("from_numpy")(np_array);
         //tensor processing logic here, for example, call a python function
        std::string success_msg = "success";
        ctx_info->set_response_attachment(success_msg);
        context.response_msg();
        return;
        }
};

class string_holder {
 public:
  string_holder(std::string val) : value(std::move(val)) {}

  py::object str_view(uint64_t data_size) {
    auto view = py::memoryview::from_buffer(value.data(), {data_size},
                                            {sizeof(uint8_t)});
    return view;
  }

 private:
  std::string value;
};

struct rpc_result {
  int code;
  std::string err_msg;
  std::shared_ptr<string_holder> data_ptr;
  uint64_t data_size;
  py::object str_view() { return data_ptr->str_view(data_size); }
};

class py_coro_rpc_client_pool {
 public:
  py_coro_rpc_client_pool(std::string url)
      : pool_(coro_io::client_pool<coro_rpc::coro_rpc_client>::create(url)) {
    async_simple::coro::syncAwait(client_.connect(url));
  };

  pybind11::object async_send_msg_with_outbuf(py::handle loop,
                                              py::handle py_bytes,
                                              py::buffer out_buf) {
    auto local_future = loop.attr("create_future")();
    py::handle future = local_future;

    py::buffer_info info = out_buf.request(true);
    char *data = static_cast<char *>(info.ptr);
    std::span<char> buf(data, info.size);

    py_bytes.inc_ref();

    pool_
        ->send_request([py_bytes, loop, future,
                        buf](coro_rpc::coro_rpc_client &client)
                           -> async_simple::coro::Lazy<void> {
          char *data;
          ssize_t length;
          PyBytes_AsStringAndSize(py_bytes.ptr(), &data, &length);
          client.set_resp_attachment_buf(buf);
          auto result = co_await client.call<&py_coro_rpc_server::handle_msg>(
              std::string_view(data, length));
          py::gil_scoped_acquire acquire;
          loop.attr("call_soon_threadsafe")(
              future.attr("set_result"),
              py::make_tuple(result.has_value(),
                             client.get_resp_attachment().size()));
          py_bytes.dec_ref();
        })
        .start([](auto &&) {
        });

    return local_future;
  }

  pybind11::object async_send_msg(py::handle loop, py::handle py_bytes) {
    auto local_future = loop.attr("create_future")();
    py::handle future = local_future;

    py_bytes.inc_ref();

    pool_
        ->send_request([py_bytes, loop,
                        future](coro_rpc::coro_rpc_client &client)
                           -> async_simple::coro::Lazy<void> {
          char *data;
          ssize_t length;
          PyBytes_AsStringAndSize(py_bytes.ptr(), &data, &length);
          auto r = co_await client.call<&py_coro_rpc_server::handle_msg>(
              std::string_view(data, length));
          rpc_result result{};
          ELOG_INFO << "rpc result: " << client.get_resp_attachment();
          if (!r.has_value()) {
            ELOG_INFO << "rpc call failed: " << r.error().msg;
            result.code = r.error().val();
            result.err_msg = r.error().msg;
          }
          else {
            result.data_ptr = std::make_shared<string_holder>(
                std::move(client.release_resp_attachment()));
            result.data_size = client.get_resp_attachment().size();
          }

          py::gil_scoped_acquire acquire;
          loop.attr("call_soon_threadsafe")(future.attr("set_result"), result);
          py_bytes.dec_ref();
        })
        .start([](auto &&) {
        });

    return local_future;
  }

  pybind11::object async_send_tensor(py::handle loop,
                                     py::handle tensor_handle) {
    py::object local_future;
    py::handle future;

    {
      py::gil_scoped_acquire acquire;
      local_future = loop.attr("create_future")();
      future = local_future;
      tensor_handle.inc_ref();
    }

    pool_
        ->send_request([tensor_handle, loop,
                        future](coro_rpc::coro_rpc_client &client)
                           -> async_simple::coro::Lazy<void> {
          {
            py::gil_scoped_acquire acquire;
            uintptr_t data_ptr =
                tensor_handle.attr("data_ptr")().cast<uintptr_t>();
            size_t numel = tensor_handle.attr("numel")().cast<size_t>();
            size_t element_size =
                tensor_handle.attr("element_size")().cast<size_t>();
            size_t tensor_size = numel * element_size;
            client.set_req_attachment(
                std::string_view((char *)data_ptr, tensor_size));
          }

          auto r = co_await client.call<&py_coro_rpc_server::handle_tensor>();
          rpc_result result{};
          ELOG_INFO << "rpc result: " << client.get_resp_attachment();
          if (!r.has_value()) {
            ELOG_INFO << "rpc call failed: " << r.error().msg;
            result.code = r.error().val();
            result.err_msg = r.error().msg;
          }
          else {
            result.data_ptr = std::make_shared<string_holder>(
                std::move(client.release_resp_attachment()));
            result.data_size = client.get_resp_attachment().size();
          }

          py::gil_scoped_acquire acquire;
          loop.attr("call_soon_threadsafe")(future.attr("set_result"), result);
          tensor_handle.dec_ref();
        })
        .start([](auto &&) {
        });

    return local_future;
  }

 private:
  std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>> pool_;
  coro_rpc::coro_rpc_client client_;
};

PYBIND11_MODULE(py_coro_rpc, m) {
  m.def("hello", [] {
    return std::string("hello");
  });

  py::class_<py_rpc_context>(m, "py_rpc_context")
      .def(py::init<>())
      .def("response_msg", &py_rpc_context::response_msg);

  py::class_<py_coro_rpc_server>(m, "coro_rpc_server")
      .def(py::init<size_t, std::string, py::function, size_t>())
      .def("start", &py_coro_rpc_server::start)
      .def("async_start", &py_coro_rpc_server::async_start);

  py::class_<py_coro_rpc_client_pool>(m, "py_coro_rpc_client_pool")
      .def(py::init<std::string>())
      .def("async_send_msg", &py_coro_rpc_client_pool::async_send_msg)
      .def("async_send_tensor", &py_coro_rpc_client_pool::async_send_tensor,
           py::call_guard<py::gil_scoped_release>())
      .def("async_send_msg_with_outbuf",
           &py_coro_rpc_client_pool::async_send_msg_with_outbuf);

  py::class_<string_holder, std::shared_ptr<string_holder>>(m, "Holder")
      .def(py::init<std::string>())
      .def("str_view", &string_holder::str_view);

  py::class_<rpc_result>(m, "rpc_result")
      .def(py::init<>())
      .def_readonly("code", &rpc_result::code)
      .def_readonly("err_msg", &rpc_result::err_msg)
      .def_readwrite("data_ptr", &rpc_result::data_ptr)
      .def_readonly("data_size", &rpc_result::data_size)
      .def("str_view", &rpc_result::str_view);

  m.def("log", [](std::string str) {
    ELOG_INFO << str;
  });
}

#endif
