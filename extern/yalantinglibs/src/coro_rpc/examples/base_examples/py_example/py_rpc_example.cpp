#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "async_simple/coro/SyncAwait.h"

namespace py = pybind11;

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
 public:
  py_coro_rpc_server(size_t thd_num, std::string address,
                     py::handle py_callback, size_t seconds)
      : server_(thd_num, address, std::chrono::seconds(seconds)),
        py_callback_(py_callback) {
    server_.register_handler<&py_coro_rpc_server::handle_msg,
                             &py_coro_rpc_server::handle_tensor>(this);
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

  void handle_tensor(coro_rpc::context<void> context) {
    auto ctx_info = context.get_context_info();
    ctx_info->set_response_attachment(ctx_info->get_request_attachment());
    context.response_msg();
  }

  coro_rpc::coro_rpc_server server_;
  py::handle py_callback_;
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

  rpc_result sync_send_msg(py::handle py_bytes) {
    std::promise<rpc_result> p;
    auto future = p.get_future();
    pool_
        ->send_request([py_bytes, p = std::move(p)](
                           coro_rpc::coro_rpc_client &client) mutable
                       -> async_simple::coro::Lazy<void> {
          std::string_view send_msg;
          {
            char *data;
            ssize_t length;
            py::gil_scoped_acquire acquire;
            PyBytes_AsStringAndSize(py_bytes.ptr(), &data, &length);
            send_msg = std::string_view(data, length);
          }
          auto r =
              co_await client.call<&py_coro_rpc_server::handle_msg>(send_msg);
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

          p.set_value(result);
        })
        .start([](auto &&) {
        });

    return future.get();
  }

  rpc_result sync_send_msg1(py::handle py_bytes) {
    auto task = [this,
                 py_bytes]() mutable -> async_simple::coro::Lazy<rpc_result> {
      std::string_view send_msg;
      {
        char *data;
        ssize_t length;
        py::gil_scoped_acquire acquire;
        PyBytes_AsStringAndSize(py_bytes.ptr(), &data, &length);
        send_msg = std::string_view(data, length);
      }
      auto r = co_await client_.call<&py_coro_rpc_server::handle_msg>(send_msg);
      rpc_result result{};
      ELOG_INFO << "rpc result: " << client_.get_resp_attachment();
      if (!r.has_value()) {
        ELOG_INFO << "rpc call failed: " << r.error().msg;
        result.code = r.error().val();
        result.err_msg = r.error().msg;
      }
      else {
        result.data_ptr = std::make_shared<string_holder>(
            std::move(client_.release_resp_attachment()));
        result.data_size = client_.get_resp_attachment().size();
      }

      co_return result;
    };

    return async_simple::coro::syncAwait(task());
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
      .def("sync_send_msg", &py_coro_rpc_client_pool::sync_send_msg,
           py::call_guard<py::gil_scoped_release>())
      .def("sync_send_msg1", &py_coro_rpc_client_pool::sync_send_msg1,
           py::call_guard<py::gil_scoped_release>())
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