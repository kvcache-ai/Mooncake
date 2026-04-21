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

#include "config/rest_rpc_protocol.hpp"

std::string hello_world();
int add(int a, int b);
std::string echo(std::string s);
void hello_with_delay(coro_rpc::rest_rpc_context<std::string> conn,
                      std::string hello);

class HelloService {
 public:
  std::string hello();
  std::string echo(std::string s);
};

#endif  // CORO_RPC_RPC_API_HPP
