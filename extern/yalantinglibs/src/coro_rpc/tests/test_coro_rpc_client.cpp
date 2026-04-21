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
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>

#include <asio/io_context.hpp>
#include <asio/ip/host_name.hpp>
#include <chrono>
#include <cstddef>
#include <memory>
#include <thread>
#include <variant>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/struct_pack.hpp>

#include "doctest.h"
#include "rpc_api.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/impl/coro_rpc_client.hpp"
#include "ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp"
#include "ylt/coro_rpc/impl/errno.h"
using namespace coro_rpc;
using namespace std::chrono_literals;
using namespace std::string_literals;
using namespace async_simple::coro;
using asio::ip::tcp;

template <typename Server>
class ClientTester {
  ClientTester(unsigned short port) : port_(std::to_string(port)) {}
  std::string port_;
};

static unsigned short coro_rpc_server_port = 8803;
Lazy<std::shared_ptr<coro_rpc_client>> create_client(
    coro_io::ExecutorWrapper<>* executor, std::string port) {
  auto client = std::make_shared<coro_rpc_client>(executor);
#ifdef YLT_ENABLE_SSL
  bool ok = client->init_ssl("../openssl_files", "server.crt");
  REQUIRE_MESSAGE(ok == true, "init ssl fail, please check ssl config");
#endif
  auto ec = co_await client->connect("127.0.0.1", port);
  REQUIRE(!ec);
  co_return client;
}

void show(auto& s) { return; }

TEST_CASE("testing client") {
  {
    coro_rpc::coro_rpc_client client;
    auto lazy_ret = client.connect("", "");
    auto ret = syncAwait(lazy_ret);
    CHECK(ret == coro_rpc::errc::not_connected);
  }
  {
    coro_rpc::coro_rpc_client client;
    auto ret = client.sync_connect("", "");
    CHECK(ret == coro_rpc::errc::not_connected);
  }
  g_action = {};
  std::string port = std::to_string(coro_rpc_server_port);
  asio::io_context io_context;
  coro_io::ExecutorWrapper<> executor = io_context.get_executor();
  auto executor_ptr = &executor;
  std::promise<void> promise;
  auto worker = std::make_unique<asio::io_context::work>(io_context);
  auto future = promise.get_future();
  std::thread thd([&io_context, &promise] {
    promise.set_value();
    io_context.run();
  });

  future.wait();
  coro_rpc_server server(2, coro_rpc_server_port);
#ifdef YLT_ENABLE_SSL
  server.init_ssl(
      ssl_configure{"../openssl_files", "server.crt", "server.key"});
#endif
  auto res = server.async_start();

  CHECK_MESSAGE(!res.hasResult(), "server start failed");

  SUBCASE("call rpc, function not registered") {
    g_action = {};
    auto f = [executor_ptr, &port]() -> Lazy<void> {
      auto client = co_await create_client(executor_ptr, port);
      auto ret = co_await client->template call<hello>();
      CHECK_MESSAGE(ret.error().code == coro_rpc::errc::function_not_registered,
                    ret.error().msg);
      co_return;
    };
    syncAwait(f());
  }
  SUBCASE("call rpc, function not registered 2") {
    g_action = {};
    auto f = [executor_ptr, &port]() -> Lazy<void> {
      auto client = co_await create_client(executor_ptr, port);
      auto ret = co_await client->template call<hi>();
      CHECK_MESSAGE(ret.error().code == coro_rpc::errc::function_not_registered,
                    ret.error().msg);
      co_return;
    };
    syncAwait(f());
  }

  server.stop();

  coro_rpc_server server2(2, coro_rpc_server_port);
#ifdef YLT_ENABLE_SSL
  server2.init_ssl(
      ssl_configure{"../openssl_files", "server.crt", "server.key"});
#endif
  server2.register_handler<hello_timeout>();
  server2.register_handler<hello>();
  server2.register_handler<large_arg_fun>();
  res = server2.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start failed");

  SUBCASE("call rpc timeout") {
    g_action = {};
    auto f = [executor_ptr, &port]() -> Lazy<void> {
      auto client = co_await create_client(executor_ptr, port);
      auto ret = co_await client->template call_for<hello_timeout>(10ms);
      CHECK_MESSAGE(ret.error().code == coro_rpc::errc::timed_out,
                    ret.error().msg);
      co_return;
    };
    syncAwait(f());
  }

  SUBCASE("call rpc success") {
    g_action = {};
    auto f = [executor_ptr, &port]() -> Lazy<void> {
      auto client = co_await create_client(executor_ptr, port);
      auto ret = co_await client->template call<hello>();
      CHECK(ret.value() == std::string("hello"));
      ret = co_await client->call_for<hello>(100ms);
      CHECK(ret.value() == std::string("hello"));
      co_return;
    };
    syncAwait(f());
  }

  SUBCASE("call with large buffer") {
    g_action = {};
    auto f = [executor_ptr, &port]() -> Lazy<void> {
      auto client = co_await create_client(executor_ptr, port);
      std::string arg;
      arg.resize(2048);
      auto ret = co_await client->template call<large_arg_fun>(arg);
      show(ret);
      CHECK(ret.value() == arg);
      co_return;
    };
    syncAwait(f());
  }

  server.stop();
  worker = nullptr;
  thd.join();
}

std::string get_first_local_ip() {
  using asio::ip::tcp;
  try {
    tcp::resolver resolver(coro_io::get_global_executor()->get_asio_executor());
    tcp::resolver::query query(asio::ip::host_name(), "");
    tcp::resolver::iterator iter = resolver.resolve(query);
    tcp::resolver::iterator end;  // End marker.
    while (iter != end) {
      tcp::endpoint ep = *iter++;
      auto addr = ep.address();
      if (addr.is_v4()) {
        return addr.to_string();
      }
    }
  } catch (std::exception& e) {
    ELOG_WARN << "get_first_local_ip error: " << e.what();
  }

  return "localhost";
}

void foo(const size_t& i) {}

TEST_CASE("testing const & args") {
  coro_rpc_server server(1, 8901);
  server.register_handler<foo>();
  auto res = server.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");

  coro_rpc_client client{};
  syncAwait(client.connect("127.0.0.1:8901"));
  const int& i = 0;
  auto ret = syncAwait(client.call<foo>(i));
  CHECK(ret.has_value());
}

TEST_CASE("testing client with local ip") {
  coro_rpc_server server(1, 8901);
  server.register_handler<hello>();
  auto res = server.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");

  std::string local_ip = get_first_local_ip();
  ELOG_INFO << "local ip: " << local_ip;
  coro_rpc_client client(local_ip);
  auto ec = client.sync_connect("127.0.0.1", "8901");
  REQUIRE_MESSAGE(!ec, ec.message());

  auto ret = client.sync_call<hello>();
  CHECK(ret.value() == "hello"s);

  coro_rpc_server server1(1, 8902, local_ip);
  server1.register_handler<hello>();
  res = server1.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  ec = client.sync_connect(local_ip, "8902");
  REQUIRE_MESSAGE(!ec, ec.message());

  ret = client.sync_call<hello>();
  CHECK(ret.value() == "hello"s);

  std::vector<asio::ip::tcp::endpoint> eps;
  eps.push_back(asio::ip::tcp::endpoint(
      asio::ip::address::from_string("192.0.2.1"), 8901));
  eps.push_back(asio::ip::tcp::endpoint(
      asio::ip::address::from_string("127.0.0.1"), 8901));

  auto local_ep =
      asio::ip::tcp::endpoint(asio::ip::address::from_string("127.0.0.1"), 0);
  asio::ip::tcp::socket socket(client.get_executor().get_asio_executor());
  socket.open(local_ep.protocol());
  socket.bind(local_ep);

  auto ec1 = async_simple::coro::syncAwait(coro_io::async_connect(socket, eps));
  CHECK(!ec1);
  CHECK(socket.local_endpoint().address().to_string() == "127.0.0.1");
}

TEST_CASE("testing client with inject server") {
  g_action = {};
  std::string port = std::to_string(coro_rpc_server_port);
  ELOGV(INFO, "inject server port: %d", port.data());
  asio::io_context io_context;
  coro_io::ExecutorWrapper<> executor = io_context.get_executor();
  auto executor_ptr = &executor;
  auto worker = std::make_unique<asio::io_context::work>(io_context);
  std::thread thd([&io_context] {
    io_context.run();
  });
  coro_rpc_server server(2, coro_rpc_server_port);
#ifdef YLT_ENABLE_SSL
  server.init_ssl(
      ssl_configure{"../openssl_files", "server.crt", "server.key"});
#endif
  server.register_handler<hello>();
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start failed");

  SUBCASE("server run ok") {
    g_action = {};
    auto f = [executor_ptr, &port]() -> Lazy<void> {
      auto client = co_await create_client(executor_ptr, port);
      auto ret = co_await client->template call<hello>();
      CHECK(ret.value() == std::string("hello"));
      co_return;
    };
    syncAwait(f());
  }

  SUBCASE("client read length error") {
    g_action = {};
    auto f = [executor_ptr, &port]() -> Lazy<void> {
      auto client = co_await create_client(executor_ptr, port);
      g_action = inject_action::close_socket_after_read_header;
      auto ret = co_await client->template call<hello>();
      REQUIRE_MESSAGE(ret.error().code == coro_rpc::errc::io_error,
                      ret.error().msg);
    };
    syncAwait(f());
  }
  SUBCASE("client read body error") {
    g_action = {};
    auto f = [executor_ptr, &port]() -> Lazy<void> {
      auto client = co_await create_client(executor_ptr, port);
      g_action = inject_action::close_socket_after_send_length;
      auto ret = co_await client->template call<hello>();
      show(ret);
      REQUIRE_MESSAGE(ret.error().code == coro_rpc::errc::io_error,
                      ret.error().msg);
    };
    syncAwait(f());
  }

  server.stop();
  worker = nullptr;
  thd.join();
  g_action = inject_action::nothing;
}
#ifdef YLT_ENABLE_SSL

enum class ssl_type { _, fake, no };

template <typename Server>
class SSLClientTester {
 public:
  SSLClientTester(std::string base_path, unsigned short port,
                  ssl_type client_crt, ssl_type server_crt, ssl_type server_key,
                  ssl_type dh)
      : port_(std::to_string(port)),
        server(2, port),
        base_path(base_path),
        client_crt(client_crt),
        server_crt(server_crt),
        server_key(server_key),
        dh(dh),
        executor_(io_context.get_executor()) {
    inject("client crt", client_crt_path, client_crt);
    inject("server crt", server_crt_path, server_crt);
    inject("server key", server_key_path, server_key);
    inject("dh", dh_path, dh);
    ssl_configure config{base_path, server_crt_path, server_key_path, dh_path};
    server.init_ssl(config);
    server.template register_handler<hi>();
    auto res = server.async_start();
    CHECK_MESSAGE(!res.hasResult(), "server start timeout");

    std::promise<void> promise;
    auto future = promise.get_future();
    worker = std::make_unique<asio::io_context::work>(io_context);
    thd = std::thread([this, &promise] {
      promise.set_value();
      io_context.run();
    });
    future.wait();
  }
  ~SSLClientTester() {
    worker = nullptr;
    thd.join();
  }
  void inject(std::string msg, std::string& path, ssl_type type) {
    switch (type) {
      case ssl_type::fake: {
        path = "fake_" + path;
        break;
      }
      case ssl_type::no: {
        path = "no_" + path;
        break;
      }
      default: {
      }
    }
    ELOGV(INFO, "%s %s", msg.data(), path.data());
  }
  void run() {
    auto client = std::make_shared<coro_rpc_client>(&executor_);
    bool ok = client->init_ssl(base_path, client_crt_path);
    if (client_crt == ssl_type::fake || client_crt == ssl_type::no) {
      REQUIRE(ok == false);
      auto ec = syncAwait(client->connect("127.0.0.1", port_));
      REQUIRE_MESSAGE(ec == coro_rpc::errc::not_connected, ec.message());
      auto ret = syncAwait(client->template call<hi>());
      REQUIRE_MESSAGE(ret.error().code == coro_rpc::errc::not_connected,
                      ret.error().msg);
    }
    else {
      REQUIRE_MESSAGE(ok == true, "init ssl fail, please check ssl config");
      auto f = [this, &client]() -> Lazy<void> {
        auto ec = co_await client->connect("127.0.0.1", port_);
        if (server_crt == ssl_type::_ && server_key == ssl_type::_) {
          if (ec) {
            ELOGV(INFO, "%s", gen_err().data());
          }
          REQUIRE_MESSAGE(!ec, ec.message());
          auto ret = co_await client->template call<hi>();
          CHECK(ret.has_value());
        }
        else {
          REQUIRE_MESSAGE(ec == coro_rpc::errc::not_connected, ec.message());
        }
      };
      syncAwait(f());
    }
  }

  std::string gen_err() {
    auto to_string = [](ssl_type t) -> std::string {
      switch (t) {
        case ssl_type::fake: {
          return "fake"s;
        }
        case ssl_type::no: {
          return "no"s;
        }
        default: {
          return "_"s;
        }
      }
    };
    std::stringstream buf;
    buf << "\n";
    buf << "client crt: " << client_crt_path << " __ " << to_string(client_crt)
        << "; ";
    buf << "server crt: " << server_crt_path << " __ " << to_string(server_crt)
        << "; ";
    buf << "server key: " << server_key_path << " __ " << to_string(server_key)
        << "; ";
    buf << "dh: " << dh_path << " __ " << to_string(dh) << "; ";
    return buf.str();
  }

  std::string port_;
  Server server;
  std::string base_path;
  std::string client_crt_path = "server.crt";
  std::string server_crt_path = "server.crt";
  std::string server_key_path = "server.key";
  std::string dh_path = "dhparam.pem";
  ssl_type client_crt;
  ssl_type server_crt;
  ssl_type server_key;
  ssl_type dh;
  asio::io_context io_context;
  coro_io::ExecutorWrapper<> executor_;
  std::thread thd;
  std::unique_ptr<asio::io_context::work> worker;
};

TEST_CASE("testing client with ssl server") {
  std::vector<ssl_type> type_list{ssl_type::fake, ssl_type::no, ssl_type::_};
  std::string base_path = "../openssl_files";
  unsigned short port = 8809;
  for (auto client_crt : type_list) {
    for (auto server_crt : type_list) {
      for (auto server_key : type_list) {
        for (auto dh : type_list) {
          SSLClientTester<coro_rpc_server>(base_path, port, client_crt,
                                           server_crt, server_key, dh)
              .run();
        }
      }
    }
  }
}
#endif
TEST_CASE("testing client with eof") {
  g_action = {};
  coro_rpc_server server(2, 8801);
  server.register_handler<hello, client_hello>();

  auto res = server.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  auto ec = client.sync_connect("127.0.0.1", "8801");
  REQUIRE_MESSAGE(!ec, ec.message());

  auto ret = client.sync_call<hello>();
  CHECK(ret.value() == "hello"s);

  ret = client.sync_call<client_hello>();
  CHECK(ret.value() == "client hello"s);

  g_action = inject_action::client_close_socket_after_send_partial_header;
  ret = client.sync_call<hello>();
  REQUIRE_MESSAGE(ret.error().code == coro_rpc::errc::io_error,
                  ret.error().msg);
}
TEST_CASE("testing client with attachment") {
  g_action = {};
  coro_rpc_server server(2, 8801);
  server.register_handler<echo_with_attachment>();

  auto res = server.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  auto ec = client.sync_connect("127.0.0.1", "8801");
  REQUIRE_MESSAGE(!ec, ec.message());

  auto ret = client.sync_call<echo_with_attachment>();
  CHECK(ret.has_value());
  CHECK(client.get_resp_attachment() == "");

  client.set_req_attachment("hellohi");
  ret = client.sync_call<echo_with_attachment>();
  CHECK(ret.has_value());
  CHECK(client.get_resp_attachment() == "hellohi");

  ret = client.sync_call<echo_with_attachment>();
  CHECK(ret.has_value());
  CHECK(client.get_resp_attachment() == "");

  char buf[100], short_buf[1];

  client.set_req_attachment("This is attachment.");
  client.set_resp_attachment_buf(buf);
  ret = client.sync_call<echo_with_attachment>();
  assert(client.get_resp_attachment() == "This is attachment.");
  assert(client.is_resp_attachment_in_external_buf());
  assert(client.get_resp_attachment().data() == buf);

  client.set_req_attachment("This is attachment.");
  ret = client.sync_call<echo_with_attachment>();
  assert(client.get_resp_attachment() == "This is attachment.");
  assert(!client.is_resp_attachment_in_external_buf());
  assert(client.get_resp_attachment().data() != buf);

  client.set_req_attachment("This is attachment.");
  client.set_resp_attachment_buf(short_buf);
  ret = client.sync_call<echo_with_attachment>();
  assert(client.get_resp_attachment() == "This is attachment.");
  assert(!client.is_resp_attachment_in_external_buf());
  assert(client.get_resp_attachment().data() != short_buf);
}

TEST_CASE("testing std::string_view") {
  g_action = {};
  coro_rpc_server server(1, 8801);
  server.register_handler<test_string_view>();

  auto res = server.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  auto ec = client.sync_connect("127.0.0.1", "8801");
  REQUIRE_MESSAGE(!ec, ec.message());

  auto ret = client.sync_call<test_string_view>("123");
  CHECK(ret.value() == "123OK");

  ret = client.sync_call<test_string_view>("1231232132123123");
  CHECK(ret.value() == "1231232132123123OK");

  ret = client.sync_call<test_string_view>("ABDD");
  CHECK(ret.value() == "ABDDOK");
}

TEST_CASE("testing client with context response user-defined error") {
  g_action = {};
  coro_rpc_server server(2, 8801);
  server.register_handler<error_with_context, hello>();
  auto res = server.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  auto ec = client.sync_connect("127.0.0.1", "8801");
  REQUIRE(!ec);
  auto ret = client.sync_call<error_with_context>();
  REQUIRE(!ret.has_value());
  CHECK(ret.error().code == coro_rpc::errc{1004});
  CHECK(ret.error().msg == "My Error.");
  CHECK(client.has_closed() == false);
  auto ret2 = client.sync_call<hello>();
  REQUIRE(ret2.has_value());
  CHECK(ret2.value() == "hello");
}

TEST_CASE("testing client with shutdown") {
  g_action = {};
  coro_rpc_server server(2, 8801);
  server.register_handler<hello, client_hello>();
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start timeout");
  coro_rpc_client client(coro_io::get_global_executor());
  auto ec = client.sync_connect("127.0.0.1", "8801");
  REQUIRE_MESSAGE(!ec, ec.message());

  g_action = inject_action::nothing;
  auto ret = client.sync_call<hello>();
  CHECK(ret.value() == "hello"s);

  ret = client.sync_call<client_hello>();
  CHECK(ret.value() == "client hello"s);

  g_action = inject_action::client_shutdown_socket_after_send_header;
  ret = client.sync_call<hello>();
  REQUIRE_MESSAGE(ret.error().code == coro_rpc::errc::io_error,
                  ret.error().msg);

  g_action = {};
}
TEST_CASE("testing client timeout") {
  coro_rpc_server server(2, 8801);
  server.register_handler<hello, client_hello>();
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start timeout");

  SUBCASE("connect, 0ms connect timeout") {
    coro_rpc_client client;
    coro_rpc_client::config config{};
    config.connect_timeout_duration = 0ms;
    bool r = client.init_config(config);
    CHECK(r);
    auto ret = client.connect("127.0.0.1", "8801");
    auto val = syncAwait(ret);
    if (val) {
      CHECK_MESSAGE(val == coro_rpc::errc::timed_out, val.message());
    }
  }
  SUBCASE("connect, -1ms never timeout") {
    coro_rpc_client client;
    coro_rpc_client::config config{};
    config.connect_timeout_duration = -1ms;  // less than 0, no timeout
                                             // checking.
    bool r = client.init_config(config);
    CHECK(r);
    auto ret = client.connect(
        "127.0.0.1",
        "8801");  // this arg won't update config connect timeout duration.
    auto val = syncAwait(ret);

    CHECK(!val);
  }

  SUBCASE("connect, 0ms request timeout") {
    coro_rpc_client client;
    coro_rpc_client::config config{};
    config.connect_timeout_duration = 1000ms;
    config.request_timeout_duration = 0ms;
    bool r = client.init_config(config);
    CHECK(r);
    auto ret = client.connect("127.0.0.1", "8801");
    auto val = syncAwait(ret);

    CHECK(!val);

    // TODO will remove via later for 0ms timeout
    auto result = syncAwait(client.call<hello>().via(&client.get_executor()));

    if (result.has_value()) {
      ELOG_INFO << result.value();
    }
    else {
      CHECK_MESSAGE(result.error().code == coro_rpc::errc::timed_out,
                    result.error().msg);
    }
  }
  SUBCASE("connect, -1ms never request timeout") {
    coro_rpc_client client;
    client.get_config().request_timeout_duration =
        -1ms;  // less than 0, never timeout.
    auto ret = client.connect("127.0.0.1", "8801");
    auto val = syncAwait(ret);

    CHECK(!val);
    auto result = syncAwait(client.call<hello>());

    CHECK(result.has_value());
  }
  SUBCASE("connect, ip timeout") {
    g_action = {};
    // https://stackoverflow.com/questions/100841/artificially-create-a-connection-timeout-error
    coro_rpc_client client(coro_io::get_global_executor());
    auto ret = client.connect("10.255.255.1", "8801", 5ms);
    auto val = syncAwait(ret);
    CHECK_MESSAGE(val == coro_rpc::errc::timed_out, val.message());
  }
}
TEST_CASE("testing client connect err") {
  coro_rpc_client client(coro_io::get_global_executor());
  auto val = syncAwait(client.connect("127.0.0.1", "8801"));
  CHECK_MESSAGE(val == coro_rpc::errc::not_connected, val.message());
}
#ifdef UNIT_TEST_INJECT
TEST_CASE("testing client sync connect, unit test inject only") {
  coro_rpc_client client(coro_io::get_global_executor());
  auto val = client.sync_connect("127.0.0.1", "8801");
  CHECK_MESSAGE(val == coro_rpc::errc::not_connected, val.message());
#ifdef YLT_ENABLE_SSL
  SUBCASE("client use ssl but server don't use ssl") {
    g_action = {};
    coro_rpc_server server(2, 8801);
    auto res = server.async_start();
    CHECK_MESSAGE(!res.hasResult(), "server start timeout");
    coro_rpc_client client2(coro_io::get_global_executor());
    bool ok = client2.init_ssl("../openssl_files", "server.crt");
    CHECK(ok == true);
    val = client2.sync_connect("127.0.0.1", "8801");
    CHECK_MESSAGE(val == coro_rpc::errc::not_connected, val.message());
  }
#endif
}
#endif
TEST_CASE("testing client call timeout") {
  g_action = {};
  SUBCASE("write timeout") {
    g_action = inject_action::force_inject_client_write_data_timeout;
    //    coro_rpc_server server(2, 8801);
    //    server.async_start().start([](auto&&) {
    //    });
    coro_rpc_client client(coro_io::get_global_executor());
    //    auto ec_lazy = client.connect("127.0.0.1", "8801", 5ms);
    //    auto ec = syncAwait(ec_lazy);
    //    assert(ec == std::errc{});
    auto ret = client.call<hi>();
    auto val = syncAwait(ret);
    CHECK_MESSAGE(val.error().code == coro_rpc::errc::timed_out,
                  val.error().msg);
    g_action = inject_action::nothing;
  }
#ifdef __GNUC__
  SUBCASE("read timeout") {
    g_action = {};
    coro_rpc_server server(2, 8801);

    server.register_handler<hello_timeout>();
    server.register_handler<hi>();
    auto res = server.async_start();
    CHECK_MESSAGE(!res.hasResult(), "server start timeout");
    coro_rpc_client client(coro_io::get_global_executor());
    auto ec_lazy = client.connect("127.0.0.1", "8801");
    auto ec = syncAwait(ec_lazy);
    REQUIRE(!ec);
    auto ret = client.call_for<hello_timeout>(10ms);
    auto val = syncAwait(ret);
    CHECK_MESSAGE(val.error().code == coro_rpc::errc::timed_out,
                  val.error().msg);
  }
#endif
  g_action = {};
}
static std::string_view echo(std::string_view data) { return data; }
TEST_CASE("testing client reconnect") {
  coro_rpc_server s1(1, 9002), s2(1, 9003);
  s1.async_start();
  s2.async_start();
  s2.register_handler<echo>();
  SUBCASE("test client reconnect") {
    coro_rpc_client cli;
    auto result = syncAwait(cli.connect("127.0.0.1", "9002"));
    CHECK_MESSAGE(!result, result.message());
    result = syncAwait(cli.connect("127.0.0.1", "9003"));
    CHECK_MESSAGE(!result, result.message());
    auto result2 = syncAwait(cli.call<echo>("hi"));
    REQUIRE_MESSAGE(result2.has_value(), result2.error().msg);
    CHECK_MESSAGE(result2.value() == "hi", result2.value());
  }
  SUBCASE("test client reconnect if client close") {
    coro_rpc_client cli;
    auto result = syncAwait(cli.connect("127.0.0.1", "9002"));
    CHECK_MESSAGE(!result, result.message());
    cli.close();
    result = syncAwait(cli.connect("127.0.0.1", "9003"));
    CHECK_MESSAGE(!result, result.message());
    auto result2 = syncAwait(cli.call<echo>("hi"));
    REQUIRE_MESSAGE(result2.has_value(), result2.error().msg);
    CHECK_MESSAGE(result2.value() == "hi", result2.value());
  }
  SUBCASE("test client reconnect if server close") {
    coro_rpc_client cli;
    auto result = syncAwait(cli.connect("127.0.0.1", "9002"));
    CHECK_MESSAGE(!result, result.message());
    s1.stop();
    result = syncAwait(cli.connect("127.0.0.1", "9003"));
    CHECK_MESSAGE(!result, result.message());
    auto result2 = syncAwait(cli.call<echo>("hi"));
    REQUIRE_MESSAGE(result2.has_value(), result2.error().msg);
    CHECK_MESSAGE(result2.value() == "hi", result2.value());
  }
}
std::errc init_acceptor(auto& acceptor_, auto port_) {
  using asio::ip::tcp;
  auto endpoint = tcp::endpoint(tcp::v4(), port_);
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(tcp::acceptor::reuse_address(true));

  asio::error_code ec;
  acceptor_.bind(endpoint, ec);
  if (ec) {
    ELOGV(ERROR, "bind error : %s", ec.message().data());
    acceptor_.cancel(ec);
    acceptor_.close(ec);
    return std::errc::address_in_use;
  }
  acceptor_.listen();

  ELOGV(INFO, "listen port %d successfully", port_);
  return std::errc{};
}
// TODO: will open after code refactor.
//  TEST_CASE("testing read body timeout") {
//    register_handler<hello>();
//    std::promise<void> server_p;
//    std::promise<void> p;
//    std::thread thd = std::thread([&server_p, &p]() {
//      asio::io_context io_context;
//      tcp::acceptor acceptor(io_context);
//      tcp::socket socket(io_context);
//      auto ec = init_acceptor(acceptor, 8801);
//      REQUIRE(ec == std::errc{});
//      easylog::info("server started");
//      server_p.set_value();
//      auto ec2 = accept(acceptor, socket);
//      REQUIRE(!ec2);
//      std::array<std::byte, RPC_HEAD_LEN> head;
//      read(socket, asio::buffer(head, RPC_HEAD_LEN));
//      req_header header;
//      auto errc = struct_pack::deserialize_to(header, head);
//      REQUIRE(errc == std::errc{});
//      std::vector<std::byte> body_;
//      body_.resize(header.length);
//      auto ret = read(socket, asio::buffer(body_.data(), header.length));
//      REQUIRE(!ret.first);
//      auto buf = struct_pack::serialize_with_offset(
//          /*offset = */ RESP_HEAD_LEN, std::monostate{});
//      *((uint32_t*)buf.data()) = buf.size() - RESP_HEAD_LEN;
//      write(socket, asio::buffer(buf.data(), RESP_HEAD_LEN));
//      std::this_thread::sleep_for(50ms);
//      write(socket, asio::buffer(buf.data() + RESP_HEAD_LEN,
//                                 buf.size() - RESP_HEAD_LEN));
//      p.set_value();
//    });
//    easylog::info("wait for server start");
//    server_p.get_future().wait();
//    easylog::info("custom server started");
//    coro_rpc_client client;
//    auto ec_lazy = client.connect("127.0.0.1", "8801"s, 5ms);
//    auto ec = syncAwait(ec_lazy);
//    REQUIRE(ec == std::errc{});
//    auto ret = client.call_for<hello>(50ms);
//    auto val = syncAwait(ret);
//    CHECK_MESSAGE(val.error().code == std::errc::timed_out, val.error().msg);
//    thd.join();
//    p.get_future().wait();
//  }
