/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <chrono>
#include <optional>
#include <thread>

#ifdef YLT_ENABLE_IBV
#include "ylt/coro_io/ibverbs/ib_socket.hpp"
#endif

#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/coro_rpc_server.hpp"
#include "ylt/coro_rpc/impl/context.hpp"
#include "ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp"

namespace coro_rpc {

struct config_base {
  bool is_enable_tcp_no_delay = true;
  uint16_t port = 9001;
  unsigned thread_num = std::thread::hardware_concurrency();
  std::chrono::steady_clock::duration conn_timeout_duration =
      std::chrono::seconds{0};
  std::string address = "0.0.0.0";
#ifdef YLT_ENABLE_SSL
  std::optional<ssl_configure> ssl_config = std::nullopt;
#ifdef YLT_ENABLE_NTLS
  std::optional<ssl_ntls_configure> ssl_ntls_config = std::nullopt;
#endif  // YLT_ENABLE_NTLS
#endif
#ifdef YLT_ENABLE_IBV
  std::optional<coro_io::ib_socket_t::config_t> ibv_config = std::nullopt;
#endif
};

struct config_t : public config_base {
  using rpc_protocol = coro_rpc::protocol::coro_rpc_protocol;
  using executor_pool_t = coro_io::io_context_pool;
};

using coro_rpc_server = coro_rpc_server_base<config_t>;
}  // namespace coro_rpc
