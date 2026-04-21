#include <async_simple/coro/Collect.h>
#include <async_simple/coro/SyncAwait.h>
#include <doctest.h>

#include <asio/io_context.hpp>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string_view>
#include <system_error>
#include <thread>
#include <ylt/coro_io/coro_file.hpp>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_io/load_balancer.hpp>

#include "async_simple/coro/Lazy.h"
#include "ylt/coro_io/client_pool.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp"

TEST_CASE("test RR") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    coro_rpc::coro_rpc_server server(1, 8801);
    auto res = server.async_start();
    REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
    auto hosts =
        std::vector<std::string_view>{"127.0.0.1:8801", "localhost:8801"};
    auto load_balancer =
        coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(hosts);
    for (int i = 0; i < 100; ++i) {
      auto res = co_await load_balancer.send_request(
          [&i, &hosts](
              coro_rpc::coro_rpc_client &client,
              std::string_view host) -> async_simple::coro::Lazy<void> {
            CHECK(host == hosts[i % 2]);
            co_return;
          });
      CHECK(res.has_value());
    }
    server.stop();
  }());
}

TEST_CASE("test WRR") {
  SUBCASE(
      "exception tests: empty hosts, empty weights test or count not equal") {
    CHECK_THROWS_AS(
        coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(
            {}, {.lba = coro_io::load_balance_algorithm::WRR}, {2, 1}),
        std::invalid_argument);

    CHECK_THROWS_AS(coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(
                        {"127.0.0.1:8801", "127.0.0.1:8802"},
                        {.lba = coro_io::load_balance_algorithm::WRR}),
                    std::invalid_argument);

    CHECK_THROWS_AS(coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(
                        {"127.0.0.1:8801", "127.0.0.1:8802"},
                        {.lba = coro_io::load_balance_algorithm::WRR}, {1}),
                    std::invalid_argument);
  }

  coro_rpc::coro_rpc_server server1(1, 8801);
  auto res = server1.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc::coro_rpc_server server2(1, 8802);
  auto res2 = server2.async_start();
  REQUIRE_MESSAGE(!res2.hasResult(), "server start failed");

  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto hosts =
        std::vector<std::string_view>{"127.0.0.1:8801", "127.0.0.1:8802"};
    auto load_balancer =
        coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(
            hosts, {.lba = coro_io::load_balance_algorithm::WRR}, {2, 1});
    for (int i = 0; i < 6; ++i) {
      auto res = co_await load_balancer.send_request(
          [&i, &hosts](
              coro_rpc::coro_rpc_client &client,
              std::string_view host) -> async_simple::coro::Lazy<void> {
            if (i == 0 || i == 1) {
              CHECK(host == hosts[0]);
            }
            else if (i == 2 || i == 5) {
              CHECK(host == hosts[1]);
            }
            else if (i == 3 || i == 4) {
              CHECK(host == hosts[0]);
            }
            co_return;
          });
      CHECK(res.has_value());
    }
  }());

  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    auto hosts =
        std::vector<std::string_view>{"127.0.0.1:8801", "127.0.0.1:8802"};
    auto load_balancer =
        coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(
            hosts, {.lba = coro_io::load_balance_algorithm::WRR}, {0, 0});
    for (int i = 0; i < 6; ++i) {
      auto res = co_await load_balancer.send_request(
          [&i, &hosts](
              coro_rpc::coro_rpc_client &client,
              std::string_view host) -> async_simple::coro::Lazy<void> {
            if (i % 2 == 0) {
              CHECK(host == hosts[0]);
            }
            else {
              CHECK(host == hosts[1]);
            }
            co_return;
          });
      CHECK(res.has_value());
    }
  }());
}

TEST_CASE("test Random") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    coro_rpc::coro_rpc_server server(1, 8801);
    auto res = server.async_start();
    REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
    auto hosts =
        std::vector<std::string_view>{"127.0.0.1:8801", "localhost:8801"};
    auto load_balancer =
        coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(
            hosts, {.lba = coro_io::load_balance_algorithm::random});
    int host0_cnt = 0, hostRR_cnt = 0;
    for (int i = 0; i < 100; ++i) {
      auto res = co_await load_balancer.send_request(
          [&i, &hosts, &host0_cnt, &hostRR_cnt](
              coro_rpc::coro_rpc_client &client,
              std::string_view host) -> async_simple::coro::Lazy<void> {
            if (host == hosts[i % 2])
              ++hostRR_cnt;
            if (host == hosts[0])
              ++host0_cnt;
            co_return;
          });
      CHECK(res.has_value());
    }
    CHECK(host0_cnt < 100);
    CHECK(hostRR_cnt < 100);
    server.stop();
  }());
}

TEST_CASE("test single host") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    coro_rpc::coro_rpc_server server(1, 8801);
    auto res = server.async_start();
    REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
    auto hosts = std::vector<std::string_view>{"127.0.0.1:8801"};
    auto load_balancer =
        coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(hosts);
    for (int i = 0; i < 100; ++i) {
      auto res = co_await load_balancer.send_request(
          [&hosts](coro_rpc::coro_rpc_client &client,
                   std::string_view host) -> async_simple::coro::Lazy<void> {
            CHECK(host == hosts[0]);
            co_return;
          });
      CHECK(res.has_value());
    }
    server.stop();
  }());
}

TEST_CASE("test send_request config") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    coro_rpc::coro_rpc_server server(1, 9813);
    auto res = server.async_start();
    REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
    auto hosts = std::vector<std::string_view>{"127.0.0.1:9813"};
    auto load_balancer =
        coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(hosts);
    for (int i = 0; i < 100; ++i) {
      coro_rpc::coro_rpc_client::config config{};
      config.client_id = 114514;
      auto res = co_await load_balancer.send_request(
          [&i, &hosts](coro_rpc::coro_rpc_client &client, std::string_view host)
              -> async_simple::coro::Lazy<void> {
            CHECK(client.get_client_id() == 114514);
            co_return;
          },
          config);
      CHECK(res.has_value());
    }
    server.stop();
  }());
}

void hello() {}

TEST_CASE("test server down") {
  async_simple::coro::syncAwait([]() -> async_simple::coro::Lazy<void> {
    coro_rpc::coro_rpc_server server1(1, 0);
    server1.register_handler<hello>();
    auto res = server1.async_start();
    REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
    auto port1 = server1.port();
    coro_rpc::coro_rpc_server server2(1, 0);
    server2.register_handler<hello>();
    auto res2 = server2.async_start();
    REQUIRE_MESSAGE(!res2.hasResult(), "server start failed");

    auto port2 = server2.port();
    auto hosts = std::vector<std::string>{"127.0.0.1:" + std::to_string(port1),
                                          "127.0.0.1:" + std::to_string(port2)};
    auto host_view = std::vector<std::string_view>{hosts[0], hosts[1]};
    auto config = coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config{
        .connect_retry_count = 0,
        .reconnect_wait_time = std::chrono::milliseconds{0},
        .host_alive_detect_duration = std::chrono::milliseconds{500}};
    auto load_balancer =
        coro_io::load_balancer<coro_rpc::coro_rpc_client>::create(host_view,
                                                                  {config});

    for (int i = 0; i < 100; ++i) {
      auto res = co_await load_balancer.send_request(
          [&i, &hosts](
              coro_rpc::coro_rpc_client &client,
              std::string_view host) -> async_simple::coro::Lazy<void> {
            CHECK(host == hosts[i % 2]);
            co_return;
          });
      CHECK(res.has_value());
    }

    server1.stop();
    ELOG_INFO << "server1 stop";
    for (int i = 0; i < 100; ++i) {
      auto res = co_await load_balancer.send_request(
          [&i, &hosts](
              coro_rpc::coro_rpc_client &client,
              std::string_view host) -> async_simple::coro::Lazy<void> {
            auto res = co_await client.call<hello>();
            if (i > 0) {
              CHECK(res.has_value());
              CHECK(host == hosts[1]);
            }
            co_return;
          });
      if (i > 2)
        CHECK(res.has_value());
    }
    server2.stop();
    {
      {
        auto res = co_await load_balancer.send_request(
            [](coro_rpc::coro_rpc_client &client,
               std::string_view host) -> async_simple::coro::Lazy<void> {
              co_await client.call<hello>();
              co_return;
            });
        res = co_await load_balancer.send_request(
            [](coro_rpc::coro_rpc_client &client,
               std::string_view host) -> async_simple::coro::Lazy<void> {
              co_await client.call<hello>();
              co_return;
            });
        CHECK(!res.has_value());
      }
    }

    coro_rpc::coro_rpc_server server3(1, port1);
    server3.register_handler<hello>();
    auto res3 = server3.async_start();
    REQUIRE_MESSAGE(!res3.hasResult(), "server start failed");
    co_await coro_io::sleep_for(std::chrono::seconds{1});
    {
      for (int i = 0; i < 100; ++i) {
        auto res = co_await load_balancer.send_request(
            [&i, &hosts](
                coro_rpc::coro_rpc_client &client,
                std::string_view host) -> async_simple::coro::Lazy<void> {
              CHECK(host == hosts[0]);
              co_return;
            });
        CHECK(res.has_value());
      }
    }
    coro_rpc::coro_rpc_server server4(1, port2);
    server4.register_handler<hello>();
    auto res4 = server4.async_start();
    REQUIRE_MESSAGE(!res4.hasResult(), "server start failed");
    co_await coro_io::sleep_for(std::chrono::seconds{1});
    {
      int counter = 0;
      for (int i = 0; i < 100; ++i) {
        auto res = co_await load_balancer.send_request(
            [&i, &hosts, &counter](
                coro_rpc::coro_rpc_client &client,
                std::string_view host) -> async_simple::coro::Lazy<void> {
              if (host == hosts[1]) {
                ++counter;
              }
              co_return;
            });
        CHECK(res.has_value());
      }
      CHECK(counter == 50);
    }
  }());
}