#include <future>
#include <memory>
#include <vector>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "async_simple/coro/Semaphore.h"
#include "doctest.h"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/easylog.hpp"
#include "ylt/easylog/record.hpp"
using namespace async_simple::coro;
static std::string_view echo(std::string_view data) { return data; }
TEST_CASE("test parall coro_rpc call") {
  auto s = easylog::logger<>::instance().get_min_severity();
  easylog::logger<>::instance().set_min_severity(easylog::Severity::WARNING);
  int thread_cnt = 100;
  coro_rpc::coro_rpc_server server(thread_cnt, 9001);
  coro_io::io_context_pool pool(thread_cnt);
  std::thread thrd{[&] {
    pool.run();
  }};

  server.register_handler<echo>();
  server.async_start();
  int client_cnt = 200;
  int64_t work_cnt_tot = thread_cnt * client_cnt * 5;
  std::vector<std::unique_ptr<coro_rpc::coro_rpc_client>> clients;
  clients.resize(client_cnt);
  std::atomic<int> connected_cnt = 0;
  std::atomic<int> work_cnt = 0;
  std::promise<void> p, p2;
  for (auto& cli : clients) {
    cli = std::make_unique<coro_rpc::coro_rpc_client>();
    cli->connect("127.0.0.1", "9001", std::chrono::seconds{5})
        .via(pool.get_executor())
        .start([&, client_cnt](auto&&) {
          if (++connected_cnt == client_cnt) {
            p.set_value();
          }
        });
  }
  p.get_future().wait();
  for (int i = 0; i < work_cnt_tot; ++i) {
    [](coro_rpc::coro_rpc_client& cli) -> Lazy<void> {
      if (!cli.has_closed()) {
        auto future_resp = co_await cli.send_request<echo>("hello");
        auto result = co_await std::move(future_resp);
        if (result.has_value()) {
          CHECK(result.value().result() == "hello");
        }
      }
    }(*clients[i % client_cnt])
                                              .via(pool.get_executor())
                                              .start([&, work_cnt_tot](auto&&) {
                                                auto i = ++work_cnt;
                                                if (i == work_cnt_tot) {
                                                  p2.set_value();
                                                }
                                              });
  }
  p2.get_future().wait();
  pool.stop();
  thrd.join();
  easylog::logger<>::instance().set_min_severity(s);
};

TEST_CASE("test parall coro_rpc call2") {
  auto s = easylog::logger<>::instance().get_min_severity();
  easylog::logger<>::instance().set_min_severity(easylog::Severity::WARNING);
  int thread_cnt = 100;
  coro_rpc::coro_rpc_server server(thread_cnt, 9001);
  coro_io::io_context_pool pool(thread_cnt);
  std::thread thrd{[&] {
    pool.run();
  }};
  server.register_handler<echo>();
  server.async_start();
  int client_cnt = 200;
  std::vector<std::unique_ptr<coro_rpc::coro_rpc_client>> clients;
  clients.resize(client_cnt);
  std::atomic<int> work_cnt = 0;
  std::promise<void> p;
  for (auto& cli : clients) {
    cli = std::make_unique<coro_rpc::coro_rpc_client>();
    cli->connect("127.0.0.1", "9001", std::chrono::seconds{5})
        .via(pool.get_executor())
        .start([&](auto&&) {
          [](coro_rpc::coro_rpc_client& cli) -> Lazy<void> {
            for (int i = 0; i < 500; ++i)
              if (!cli.has_closed()) {
                auto result = co_await cli.call<echo>("hello");
                if (result.has_value()) {
                  CHECK(result.value() == "hello");
                }
              }
          }(*cli)
                                                    .start([&, client_cnt](
                                                               auto&&) {
                                                      auto i = ++work_cnt;
                                                      if (i == client_cnt) {
                                                        p.set_value();
                                                      }
                                                    });
        });
  }
  p.get_future().wait();
  pool.stop();
  thrd.join();
  easylog::logger<>::instance().set_min_severity(s);
};