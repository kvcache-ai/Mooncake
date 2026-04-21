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
#include "async_simple/Unit.h"
#include "async_simple/coro/ConditionVariable.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/Semaphore.h"
#include "async_simple/coro/Sleep.h"
#include "async_simple/coro/SpinLock.h"
#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp"
#include "ylt/coro_rpc/impl/expected.hpp"
#include "ylt/struct_pack.hpp"
using namespace std::chrono_literals;
using namespace async_simple::coro;
template <typename T = coro_rpc::coro_rpc_client>
async_simple::coro::Lazy<bool> event(
    int lim, coro_io::client_pool<T> &pool, ConditionVariable<SpinLock> &cv,
    SpinLock &lock,
    std::function<void(coro_rpc::coro_rpc_client &client)> user_op =
        [](auto &client) {
        }) {
  std::vector<RescheduleLazy<bool>> works;
  int64_t cnt = 0;
  for (int i = 0; i < lim; ++i) {
    auto op = [&cnt, &lock, &cv, &lim,
               &user_op](T &client) -> async_simple::coro::Lazy<void> {
      user_op(client);
      auto l = co_await lock.coScopedLock();
      if (++cnt < lim) {
        co_await cv.wait(lock, [&cnt, &lim] {
          return cnt >= lim;
        });
      }
      else {
        l.unlock();
        cv.notifyAll();
      }
      co_return;
    };
    auto backer = [&cv, &lock, &cnt, &lim](
                      coro_io::client_pool<T> &pool,
                      auto op) -> async_simple::coro::Lazy<bool> {
      async_simple::Promise<bool> p;
      auto res = co_await pool.send_request(op);
      if (!res.has_value()) {
        {
          co_await lock.coScopedLock();
          cnt = lim;
        }
        cv.notifyAll();
        co_return false;
      }
      co_return true;
    };
    works.emplace_back(backer(pool, op).via(coro_io::get_global_executor()));
  }
  auto res = co_await collectAll(std::move(works));
  ELOG_INFO << "HI";
  for (auto &e : res) {
    if (!e.value()) {
      co_return false;
    }
  }
  co_return true;
};
TEST_CASE("test client pool") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    coro_rpc::coro_rpc_server server(1, 8801);
    auto is_started = server.async_start();
    REQUIRE(is_started.hasResult() == false);
    auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(
        "127.0.0.1:8801", {.max_connection = 100,
                           .idle_timeout = 700ms,
                           .short_connect_idle_timeout = 200ms});
    SpinLock lock;
    ConditionVariable<SpinLock> cv;
    auto res = co_await event(20, *pool, cv, lock);
    CHECK(res);
    CHECK(pool->free_client_count() == 20);
    res = co_await event(200, *pool, cv, lock);
    CHECK(res);
    auto sz = pool->free_client_count();
    CHECK(sz == 200);
    co_await coro_io::sleep_for(500ms);
    sz = pool->free_client_count();
    CHECK((sz >= 100 && sz <= 105));
    co_await coro_io::sleep_for(1000ms);
    CHECK(pool->free_client_count() == 0);
    server.stop();
  }());
  ELOG_DEBUG << "test client pool over.";
}

#if not defined(__APPLE__)  // disable for mac ci, will open it later
TEST_CASE("test idle timeout yield") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    coro_rpc::coro_rpc_server server(1, 8801);
    auto is_started = server.async_start();
    REQUIRE(!is_started.hasResult());
    auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(
        "127.0.0.1:8801", {.max_connection = 100,
                           .idle_queue_per_max_clear_count = 1,
                           .idle_timeout = 300ms});
    SpinLock lock;
    ConditionVariable<SpinLock> cv;
    auto res = co_await event(100, *pool, cv, lock);
    CHECK(res);
    CHECK(pool->free_client_count() == 100);
    co_await coro_io::sleep_for(700ms);
    CHECK(pool->free_client_count() == 0);
    server.stop();
  }());
  ELOG_DEBUG << "test idle timeout yield over.";
}
#endif
TEST_CASE("test reconnect") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(
        "127.0.0.1:8801",
        {.connect_retry_count = 3, .reconnect_wait_time = 500ms});
    SpinLock lock;
    ConditionVariable<SpinLock> cv;
    coro_rpc::coro_rpc_server server(2, 8801);
    async_simple::Promise<async_simple::Unit> p;
    coro_io::sleep_for(700ms).start([&server, &p](auto &&) {
      auto server_is_started = server.async_start();
      REQUIRE(!server_is_started.hasResult());
    });

    auto res = co_await event(100, *pool, cv, lock);
    CHECK(res);
    CHECK(pool->free_client_count() == 100);
    server.stop();
    co_return;
  }());
  ELOG_DEBUG << "test reconnect over.";
}

struct mock_client : public coro_rpc::coro_rpc_client {
  using coro_rpc::coro_rpc_client::coro_rpc_client;
  async_simple::coro::Lazy<coro_rpc::errc> connect(
      const std::string &hostname, std::vector<asio::ip::tcp::endpoint> *eps) {
    auto ec = co_await this->coro_rpc::coro_rpc_client::connect(hostname, eps);
    if (ec) {
      co_await coro_io::sleep_for(300ms);
    }
    co_return ec;
  }
};
TEST_CASE("test reconnect retry wait time exclude reconnect cost time") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto tp = std::chrono::steady_clock::now();
    auto pool = coro_io::client_pool<mock_client>::create(
        "127.0.0.1:8801",
        {.connect_retry_count = 3, .reconnect_wait_time = 500ms});
    SpinLock lock;
    ConditionVariable<SpinLock> cv;
    coro_rpc::coro_rpc_server server(2, 8801);
    async_simple::Promise<async_simple::Unit> p;
    coro_io::sleep_for(350ms).start([&server, &p](auto &&) {
      auto server_is_started = server.async_start();
      REQUIRE(!server_is_started.hasResult());
    });
    auto res = co_await event<mock_client>(100, *pool, cv, lock);
    CHECK(res);
    CHECK(pool->free_client_count() == 100);
    auto dur = std::chrono::steady_clock::now() - tp;
    ELOG_INFO << dur.count();
    CHECK((dur >= 500ms && dur <= 799ms));
    server.stop();
    co_return;
  }());
  ELOG_DEBUG
      << "test reconnect retry wait time exclude reconnect cost time over.";
}

TEST_CASE("test collect_free_client") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    coro_rpc::coro_rpc_server server(1, 8801);
    auto is_started = server.async_start();
    REQUIRE(!is_started.hasResult());
    auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(
        "127.0.0.1:8801", {.max_connection = 100, .idle_timeout = 300ms});

    SpinLock lock;
    ConditionVariable<SpinLock> cv;
    auto res = co_await event(50, *pool, cv, lock, [](auto &client) {
      client.close();
    });
    CHECK(res);
    CHECK(pool->free_client_count() == 0);
    server.stop();
    co_return;
  }());
  ELOG_DEBUG << "test collect_free_client over.";
}

TEST_CASE("test client pools parallel r/w") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto pool = coro_io::client_pools<coro_rpc::coro_rpc_client>{};
    for (int i = 0; i < 10000; ++i) {
      auto cli = pool[std::to_string(i)];
      CHECK(cli->get_host_name() == std::to_string(i));
    }
    auto rw = [&pool](int i) -> Lazy<void> {
      //      ELOG_DEBUG << "start to insert {" << i << "} to hash table.";
      auto cli = pool[std::to_string(i)];
      CHECK(cli->get_host_name() == std::to_string(i));
      //      ELOG_DEBUG << "end to insert {" << i << "} to hash table.";
      co_return;
    };

    std::vector<RescheduleLazy<void>> works;
    for (int i = 0; i < 20000; ++i) {
      works.emplace_back(rw(i).via(coro_io::get_global_executor()));
    }
    for (int i = 0; i < 10000; ++i) {
      works.emplace_back(rw(i).via(coro_io::get_global_executor()));
    }
    co_await collectAll(std::move(works));
  }());
  ELOG_DEBUG << "test client pools parallel r/w over.";
}

TEST_CASE("test client pools dns cache") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto pool = coro_io::client_pool<coro_http::coro_http_client>::create(
        "http://www.baidu.com",
        coro_io::client_pool<coro_http::coro_http_client>::pool_config{
            .dns_cache_update_duration = 600s});
    auto eps_init = pool->get_remote_endpoints();
    CHECK(eps_init->empty());
    co_await pool->send_request(
        [](coro_http::coro_http_client &cli) -> Lazy<void> {
          cli.close();
          co_return;
        });
    auto eps = pool->get_remote_endpoints();
    CHECK(!eps->empty());
    CHECK(eps.get() != eps_init.get());
    CHECK(eps->front().port() == 80);
    co_await pool->send_request(
        [](coro_http::coro_http_client &cli) -> Lazy<void> {
          cli.close();
          co_return;
        });
    auto eps2 = pool->get_remote_endpoints();
    CHECK(eps.get() == eps2.get());
  }());
}

TEST_CASE("test client pools dns refresh") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto pool = coro_io::client_pool<coro_http::coro_http_client>::create(
        "http://www.baidu.com",
        coro_io::client_pool<coro_http::coro_http_client>::pool_config{
            .dns_cache_update_duration = 0s});
    auto eps_init = pool->get_remote_endpoints();
    CHECK(eps_init->empty());
    co_await pool->send_request(
        [](coro_http::coro_http_client &cli) -> Lazy<void> {
          cli.close();
          co_return;
        });
    auto eps = pool->get_remote_endpoints();
    CHECK(!eps->empty());
    CHECK(eps.get() != eps_init.get());
    CHECK(eps->front().port() == 80);
    co_await pool->send_request(
        [](coro_http::coro_http_client &cli) -> Lazy<void> {
          co_return;
        });
    auto eps2 = pool->get_remote_endpoints();
    CHECK(eps.get() != eps2.get());
  }());
}

TEST_CASE("test client pools dns parallel refresh") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto pool = coro_io::client_pool<coro_http::coro_http_client>::create(
        "http://www.baidu.com",
        coro_io::client_pool<coro_http::coro_http_client>::pool_config{
            .dns_cache_update_duration = 0s});
    auto eps_init = pool->get_remote_endpoints();
    CHECK(eps_init->empty());
    std::vector<
        async_simple::coro::RescheduleLazy<ylt::expected<void, std::errc>>>
        results;
    std::atomic<int> err_cnt;
    for (int i = 0; i < 100; ++i) {
      results.push_back(
          pool->send_request(
                  [&](coro_http::coro_http_client &cli) -> Lazy<void> {
                    auto result = co_await cli.async_get("/");
                    if (result.net_err)
                      ++err_cnt;
                    co_return;
                  })
              .via(coro_io::get_global_executor()));
    }
    co_await collectAll(std::move(results));
    CHECK(err_cnt < 10);
    co_return;
  }());
}

TEST_CASE("test client pools dns don't refresh") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto pool = coro_io::client_pool<coro_http::coro_http_client>::create(
        "http://www.baidu.com",
        coro_io::client_pool<coro_http::coro_http_client>::pool_config{
            .dns_cache_update_duration = -1s});
    auto eps_init = pool->get_remote_endpoints();
    CHECK(eps_init->empty());
    co_await pool->send_request(
        [](coro_http::coro_http_client &cli) -> Lazy<void> {
          cli.close();
          co_return;
        });
    auto eps = pool->get_remote_endpoints();
    CHECK(eps.get() == eps_init.get());
    CHECK(eps->empty());
  }());
}

TEST_CASE("test client pools client pool") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto pool = coro_io::client_pool<coro_http::coro_http_client>::create(
        "http://www.baidu.com",
        coro_io::client_pool<coro_http::coro_http_client>::pool_config{
            .max_connection_life_time = 0s});
    co_await pool->send_request(
        [](coro_http::coro_http_client &cli) -> Lazy<void> {
          cli.close();
          co_return;
        });
    CHECK(pool->free_client_count() == 0);
  }());
}