#include <async_simple/coro/Collect.h>
#include <async_simple/coro/SyncAwait.h>
#include <doctest.h>

#include <asio/io_context.hpp>
#include <atomic>
#include <cassert>
#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <string_view>
#include <system_error>
#include <thread>
#include <ylt/coro_io/coro_file.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_io/load_balancer.hpp>

#include "async_simple/Executor.h"
#include "async_simple/Promise.h"
#include "async_simple/Signal.h"
#include "async_simple/Unit.h"
#include "async_simple/coro/ConditionVariable.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/Semaphore.h"
#include "async_simple/coro/Sleep.h"
#include "async_simple/coro/SpinLock.h"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp"
#include "ylt/coro_rpc/impl/expected.hpp"
#include "ylt/easylog.hpp"
#include "ylt/easylog/record.hpp"
using namespace std::chrono_literals;
using namespace async_simple::coro;

Lazy<bool> my_sleep(std::chrono::milliseconds ms, auto executor,
                    bool if_twice) {
  auto result = co_await coro_io::sleep_for(ms, executor);
  if (if_twice && result) {
    try {
      co_await async_simple::coro::sleep(ms);
    } catch (const async_simple::SignalException& e) {
      result = false;
    }
  }
  co_return result;
}
async_simple::coro::Lazy<void> test_cancel(coro_io::io_context_pool& io_pool,
                                           std::chrono::milliseconds ms,
                                           coro_io::ExecutorWrapper<>* executor,
                                           bool if_sleep_twice,
                                           bool is_same_sec = false,
                                           bool order = false) {
  std::vector<RescheduleLazy<bool>> tasks;
  for (int i = 1000; i >= 0; --i) {
    tasks.emplace_back(my_sleep(is_same_sec ? ms : ((i == 0) ? (0 * ms) : ms),
                                executor ? executor : io_pool.get_executor(),
                                if_sleep_twice)
                           .via(executor ? executor : io_pool.get_executor()));
  }
  if (order) {
    std::vector<RescheduleLazy<bool>> tasks2;
    while (tasks.size()) {
      tasks2.emplace_back(std::move(tasks.back()));
      tasks.pop_back();
    }
    tasks = std::move(tasks2);
  }
  auto result = co_await collectAll<async_simple::Terminate>(std::move(tasks));

  if (ms.count() && !is_same_sec) {
    if (order) {
      CHECK(result.front().value() == true);
      for (int i = 1; i <= 1000; ++i) {
        CHECK(result[i].value() == false);
      }
    }
    else {
      CHECK(result.back().value() == true);
      result.pop_back();
      for (auto& r : result) {
        CHECK(r.value() == false);
      }
    }
  }
}

async_simple::coro::Lazy<void> test_cancel2(coro_io::io_context_pool& io_pool,
                                            std::chrono::milliseconds ms,
                                            bool is_same_sec = false,
                                            bool order = false) {
  std::vector<Lazy<bool>> tasks;
  for (int i = 1000; i >= 0; --i) {
    tasks.emplace_back(my_sleep(is_same_sec ? ms : ((i == 0) ? (0 * ms) : ms),
                                io_pool.get_executor(), false));
  }
  if (order) {
    std::vector<Lazy<bool>> tasks2;
    while (tasks.size()) {
      tasks2.emplace_back(std::move(tasks.back()));
      tasks.pop_back();
    }
    tasks = std::move(tasks2);
  }
  auto result = co_await collectAll<async_simple::Terminate>(std::move(tasks));

  if (ms.count() && !is_same_sec) {
    if (order) {
      CHECK(result.front().value() == true);
      for (int i = 1; i <= 1000; ++i) {
        CHECK(result[i].value() == false);
      }
    }
    else {
      CHECK(result.back().value() == true);
      result.pop_back();
      for (auto& r : result) {
        CHECK(r.value() == false);
      }
    }
  }
}
TEST_CASE("test multithread cancellation") {
  coro_io::io_context_pool io_pool(101);
  ELOG_WARN << "test multithread cancellation";
  std::thread thd{[&]() {
    ELOG_WARN << "start thread pool";
    io_pool.run();
  }};
  auto test = [&](coro_io::ExecutorWrapper<>* e) {
    for (int sleep_twice = 0; sleep_twice <= 1; ++sleep_twice) {
      ELOG_WARN << "slow work canceled(reverse) test";
      for (int i = 0; i < 100; ++i) {
        async_simple::coro::syncAwait(
            test_cancel(io_pool, 10000s, e, sleep_twice, false, true));
      }
      ELOG_WARN << "slow work canceled test";
      for (int i = 0; i < 100; ++i) {
        async_simple::coro::syncAwait(
            test_cancel(io_pool, 10000s, e, sleep_twice));
      }
      ELOG_WARN << "work finished in 0s test";
      for (int i = 0; i < 100; ++i)
        async_simple::coro::syncAwait(test_cancel(io_pool, 0s, e, sleep_twice));
      ELOG_WARN << "work finished in same time test";
      for (int i = 0; i < 20; ++i)
        async_simple::coro::syncAwait(
            test_cancel(io_pool, 100ms, e, sleep_twice, true));
      ELOG_WARN << "work finished in same short time test";
      for (int i = 0; i < 100; ++i)
        async_simple::coro::syncAwait(
            test_cancel(io_pool, 1ms, e, sleep_twice, true));
    }
  };
  test(nullptr);
  test(coro_io::get_global_executor());

  ELOG_TRACE << "slow work canceled(reverse) test";
  for (int i = 0; i < 100; ++i)
    async_simple::coro::syncAwait(test_cancel2(io_pool, 10000s, false, true));
  ELOG_TRACE << "slow work canceled test";
  for (int i = 0; i < 100; ++i)
    async_simple::coro::syncAwait(test_cancel2(io_pool, 10000s));
  ELOG_TRACE << "work finished in 0s test";
  for (int i = 0; i < 100; ++i)
    async_simple::coro::syncAwait(test_cancel2(io_pool, 0s));
  ELOG_TRACE << "work finished in same time test";
  for (int i = 0; i < 20; ++i)
    async_simple::coro::syncAwait(test_cancel2(io_pool, 100ms, true));
  ELOG_TRACE << "work finished in same short time test";
  for (int i = 0; i < 100; ++i)
    async_simple::coro::syncAwait(test_cancel2(io_pool, 1ms, true));

  ELOG_TRACE << "stop thread pool";
  io_pool.stop();
  ELOG_TRACE << "join thread pool";
  thd.join();
  ELOG_DEBUG << "test multithread cancellation over.";
}