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

#include <atomic>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdlib>
#include <locale>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "async_simple/coro/Collect.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/Mutex.h"
#include "async_simple/coro/SyncAwait.h"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/coro_rpc/impl/expected.hpp"
#include "ylt/easylog.hpp"
std::string echo(std::string_view sv);

constexpr auto thread_cnt = 96 * 20;
using namespace coro_rpc;
using namespace async_simple::coro;
using namespace std::string_view_literals;

auto finish_executor = coro_io::get_global_block_executor();
std::atomic<uint64_t> working_echo = 0;

std::vector<std::unique_ptr<coro_rpc_client>> clients;

std::atomic<uint64_t>& get_qps(int id) {
  static std::atomic<uint64_t> ar[thread_cnt * 8];
  return ar[id * 8];
}

Lazy<std::vector<std::chrono::microseconds>> send(int id, int cnt) {
  std::vector<std::chrono::microseconds> result;
  auto& cli = *clients[id];
  auto& qps = get_qps(id);
  result.reserve(cnt);
  ++working_echo;
  auto tp = std::chrono::steady_clock::now();
  for (int i = 0; i < cnt; ++i) {
    auto res_ = co_await cli.call<echo>("Hello world!");
    if (!res_.has_value()) {
      ELOG_ERROR << "coro_rpc err: \n" << res_.error().msg;
      continue;
    }
    qps.fetch_add(1, std::memory_order_relaxed);
    auto old_tp = tp;
    tp = std::chrono::steady_clock::now();
    result.push_back(
        std::chrono::duration_cast<std::chrono::microseconds>(tp - old_tp));
  }
  --working_echo;
  co_return std::move(result);
}

Lazy<void> qps_watcher() {
  using namespace std::chrono_literals;
  do {
    co_await coro_io::sleep_for(1s);
    uint64_t cnt = 0;
    for (int i = 0; i < thread_cnt; ++i) {
      auto& qps = get_qps(i);
      uint64_t tmp = qps.exchange(0);
      cnt += tmp;
    }
    std::cout << "QPS:"
              << cnt
              // << " free connection: " << clients.free_client_count()
              << " working echo:" << working_echo << std::endl;
    cnt = 0;
  } while (working_echo > 0);
}
std::vector<std::chrono::microseconds> result;
void latency_watcher() {
  std::sort(result.begin(), result.end());
  auto arr = {0.1, 0.3, 0.5, 0.7, 0.9, 0.95, 0.99, 0.999, 0.9999, 0.99999, 1.0};
  for (auto e : arr) {
    std::cout
        << (e * 100) << "% request finished in:"
        << result[std::max<std::size_t>(0, result.size() * e - 1)].count() /
               1000.0
        << "ms" << std::endl;
  }
}
int main() {
  for (int i = 0; i < thread_cnt; ++i) {
    clients.emplace_back(
        std::make_unique<coro_rpc_client>(coro_io::get_global_executor()));
    syncAwait(clients.back()->connect("localhost:8801"));
  }
  for (int i = 0, lim = thread_cnt; i < lim; ++i) {
    send(i, 100000).via(&clients[i]->get_executor()).start([](auto&& res) {
      finish_executor->schedule([res = std::move(res.value())] {
        result.insert(result.end(), res.begin(), res.end());
      });
    });
  }
  syncAwait(qps_watcher());
  latency_watcher();
  std::cout << "Done!" << std::endl;
  return 0;
}