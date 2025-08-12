#ifndef CORO_RPC_TRANSFER_H
#define CORO_RPC_TRANSFER_H

##pragma once

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

// Forward declarations
class py_coro_rpc_server;
class py_coro_rpc_client_pool;

class py_rpc_context {
 public:
  void response_msg(py::buffer msg, py::handle done);

  coro_rpc::context<void> context_;
};

class py_coro_rpc_server {
 public:
  py_coro_rpc_server(size_t thd_num, std::string address, py::handle py_callback, size_t seconds);

  bool start();
  bool async_start();

 private:
  friend class py_coro_rpc_client_pool;
  void handle_msg(coro_rpc::context<void> context, std::string_view msg);
  void handle_tensor(coro_rpc::context<void> context);

  coro_rpc::coro_rpc_server server_;
  py::handle py_callback_;
};

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
  rpc_result sync_send_msg(py::handle py_bytes);
  rpc_result sync_send_msg1(py::handle py_bytes);

 private:
  std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>> pool_;
  coro_rpc::coro_rpc_client client_;
};

#endif // CORO_RPC_TRANSFER_H