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
#ifndef CORO_RPC_RPC_API_HPP
#define CORO_RPC_RPC_API_HPP

#include <string>
#include <string_view>
#include <ylt/coro_rpc/coro_rpc_context.hpp>

std::string_view echo(std::string_view data);
async_simple::coro::Lazy<std::string_view> async_echo_by_coroutine(
    std::string_view data);
void async_echo_by_callback(
    coro_rpc::context<std::string_view /*rpc response data here*/> conn,
    std::string_view /*rpc request data here*/ data);
void echo_with_attachment();
inline int add(int a, int b) { return a + b; }
async_simple::coro::Lazy<std::string_view> nested_echo(std::string_view sv);
void return_error_by_context(coro_rpc::context<void> conn);
void return_error_by_exception();
async_simple::coro::Lazy<void> get_ctx_info();
class HelloService {
 public:
  std::string_view hello();
};
async_simple::coro::Lazy<std::string> rpc_with_state_by_tag();
std::string_view rpc_with_complete_handler();
#endif  // CORO_RPC_RPC_API_HPP
