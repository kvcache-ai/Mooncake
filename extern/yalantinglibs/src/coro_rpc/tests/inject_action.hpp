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
#ifndef CORO_RPC_INJECT_ACTION_HPP
#define CORO_RPC_INJECT_ACTION_HPP
#include <atomic>
#include <cstdint>
namespace coro_rpc {
enum class inject_action : uint8_t {
  nothing = 0,
  close_socket_after_read_header,
  close_socket_after_send_length,
  server_send_bad_rpc_result,
  client_send_header_length_0,
  client_send_bad_header,
  client_send_bad_magic_num,
  client_close_socket_after_send_header,
  client_shutdown_socket_after_send_header,
  client_close_socket_after_send_partial_header,
  client_close_socket_after_send_payload,

  // TODO: we need to construct better case instead of forcing inject
  force_inject_server_accept_error,
  force_inject_connection_close_socket,
  force_inject_client_write_data_timeout,

};
inline std::atomic<inject_action> g_action = inject_action::nothing;
inline std::atomic<uint32_t> g_client_id = 1;
}  // namespace coro_rpc
#endif  // CORO_RPC_INJECT_ACTION_HPP
