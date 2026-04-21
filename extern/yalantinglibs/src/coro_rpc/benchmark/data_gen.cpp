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
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog.hpp>

#include "server.hpp"
using namespace std::chrono_literals;

int main() {
  using namespace coro_rpc;
  coro_rpc::coro_rpc_server server(std::thread::hardware_concurrency(), 0);
  register_handlers(server);
  auto started = server.async_start();
  if (started.hasResult()) {
    ELOGV(ERROR, "server started failed");
    return -1;
  }

  std::promise<void> promise;
  std::thread thd([&promise] {
    promise.set_value();
    pool.run();
  });
  promise.get_future().wait();

  coro_rpc_client client;

  syncAwait(client.connect("localhost", std::to_string(server.port())));

  coro_rpc::benchmark_file_path = "./test_data/echo_test/";
  std::filesystem::create_directories(coro_rpc::benchmark_file_path);
  syncAwait(client.call<echo_4B>(std::string(4, 'A')));
  syncAwait(client.call<echo_100B>(std::string(100, 'A')));
  syncAwait(client.call<echo_500B>(std::string(500, 'A')));
  syncAwait(client.call<echo_1KB>(std::string(1000, 'A')));
  syncAwait(client.call<echo_5KB>(std::string(5000, 'A')));
  syncAwait(client.call<echo_10KB>(std::string(10000, 'A')));

  coro_rpc::benchmark_file_path = "./test_data/complex_test/";
  std::filesystem::create_directories(coro_rpc::benchmark_file_path);
  syncAwait(client.call<array_1K_int>(std::vector(1000, 42)));
  syncAwait(client.call<array_1K_str_4B>(
      std::vector(1000, std::string_view{"AAAA"})));
  syncAwait(client.call<array_1K_rect>(
      std::vector(1000, rect{.p1 = {1.2, 3.4}, .p2 = {2.5, 4.6}})));
  syncAwait(
      client
          .call<many_argument<int, int, int, int, int, int, int, int, int, int,
                              double, double, double, double, double, double>>(
              1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16));
  syncAwait(client.call<monsterFunc>(Monster{
      .pos = Vec3{3.1, 2.94, -1.4},
      .mana = 1341,
      .hp = 11451,
      .name = "Hello",
      .inventory = std::vector<uint8_t>(100, 100),
      .color = Red,
      .weapons =
          std::vector<Weapon>(10, Weapon{.name = "PINGPONG", .damage = 10}),
      .equipped =
          {
              .name = "noname",
              .damage = 17,
          },
      .path = std::vector<Vec3>(100, {2.0, 3.0, 4.0}),
  }));
  syncAwait(client.call<ValidateRequestFunc>(ValidateRequest{
      .msg = {.message_type = 101,
              .session_no = "PINGPONG",
              .tint_flag = false,
              .source_entity = 114514,
              .dest_entity = 114514,
              .client_ip = "127.0.0.1",
              .rc = ResponseCode{.retcode = 0, .error_message = std::nullopt},
              .version = 1024},
      .job_id = 114514,
      .query_keys = std::vector<std::string>(100, "PANGPPANG"),
      .clean = true,
  }));
  syncAwait(client.call<download_10KB>(42));

  coro_rpc::benchmark_file_path = "./test_data/long_tail_test/";
  std::filesystem::create_directories(coro_rpc::benchmark_file_path);
  syncAwait(client.call<long_tail_async_io>(42));
  syncAwait(client.call<long_tail_heavy_calculate>(42));
  syncAwait(client.call<long_tail_block_io>(42));

  coro_rpc::benchmark_file_path = "./test_data/slow_function_test/";
  std::filesystem::create_directories(coro_rpc::benchmark_file_path);
  syncAwait(client.call<async_io>(42));
  syncAwait(client.call<block_io>(42));
  syncAwait(client.call<heavy_calculate>(42));

  coro_rpc::benchmark_file_path = "./test_data/async_test/";
  std::filesystem::create_directories(coro_rpc::benchmark_file_path);
  syncAwait(client.call<callback_async_echo>("echo"));
  syncAwait(client.call<coroutine_async_echo>("echo"));

  server.stop();

  started.wait();

  pool.stop();
  thd.join();

  return 0;
};