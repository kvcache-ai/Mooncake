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
#include "rpc_service.h"

#include <chrono>
#include <thread>
#include <ylt/easylog.hpp>

using namespace coro_rpc;

std::string hello_world() {
  ELOGV(INFO, "call helloworld");
  return "hello_world";
}

int add(int a, int b) {
  ELOGV(INFO, "call A+B");
  return a + b;
}

void hello_with_delay(coro_rpc::rest_rpc_context<std::string> conn,
                      std::string hello) {
  ELOGV(INFO, "call HelloServer hello_with_delay");
  // create a new thread
  std::thread([conn = std::move(conn), hello = std::move(hello)]() mutable {
    // do some heavy work in this thread that won't block the io-thread,
    std::cout << "running heavy work..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds{1});
    // Remember response before connection destruction! Or the connect will
    // be closed.
    conn.response_msg(hello);
  }).detach();
}

std::string echo(std::string s) { return s; }

std::string HelloService::hello() {
  ELOGV(INFO, "call HelloServer::hello");
  return "HelloService::hello";
}

std::string HelloService::echo(std::string s) { return s; }
