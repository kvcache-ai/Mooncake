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
#include <async_simple/coro/Collect.h>
#include <async_simple/coro/SyncAwait.h>

#include <thread>
#include <variant>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "ServerTester.hpp"
#include "async_simple/coro/Lazy.h"
#include "cinatra/response_cv.hpp"
#include "doctest.h"
#include "rpc_api.hpp"
#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_http/coro_http_server.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/struct_pack.hpp"

async_simple::coro::Lazy<int> get_coro_value(int val) { co_return val; }

struct CoroServerTester : ServerTester {
  CoroServerTester(TesterConfig config)
      : ServerTester(config),
        server(2, config.port, config.address, config.conn_timeout_duration) {
#ifdef YLT_ENABLE_SSL
    if (use_ssl) {
      server.init_ssl(
          ssl_configure{"../openssl_files", "server.crt", "server.key"});
    }
#endif
#ifdef YLT_ENABLE_IBV
    if (use_rdma) {
      server.init_ibv();
    }
#endif
    auto res = server.async_start();
    CHECK_MESSAGE(!res.hasResult(), "server start timeout");
  }
  ~CoroServerTester() { server.stop(); }

  async_simple::coro::Lazy<int> get_value(int val) { co_return val; }

  void test_set_server_address() {
    {
      coro_rpc_server server(1, 9001);
      [[maybe_unused]] auto r = server.async_start();
      CHECK(!server.get_errc());
    }

    {
      coro_rpc_server server(1, 9001, "0.0.0.0");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(!server.get_errc());
    }

    {
      coro_rpc_server server(1, 9001, "127.0.0.1");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(!server.get_errc());
    }

    {
      coro_rpc_server server(1, 9001, "localhost");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(!server.get_errc());
    }

    {
      coro_rpc_server server(1, "0.0.0.0:9001");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(!server.get_errc());
    }

    {
      coro_rpc_server server(1, "127.0.0.1:9001");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(!server.get_errc());
    }

    {
      coro_rpc_server server(1, "localhost:9001");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(!server.get_errc());
    }

    {
      coro_rpc_server server(1, 9001, "x.x.x.x");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(server.get_errc() == coro_rpc::errc::bad_address);
    }

    {
      coro_rpc_server server(1, "x.x.x.x:9001");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(server.get_errc() == coro_rpc::errc::bad_address);
    }

    {
      coro_rpc_server server(1, "127.0.0.1:aaa");
      [[maybe_unused]] auto r = server.async_start();
      CHECK(server.get_errc() == coro_rpc::errc::bad_address);
    }
  }

  void test_all() override {
    g_action = {};
    ELOGV(INFO, "run %s", __func__);
    test_set_server_address();
    test_coro_handler();
    ServerTester::test_all();
    test_function_not_registered();
    test_start_new_server_with_same_port();
    test_server_send_bad_rpc_result();
    test_server_send_no_body();
    test_context_func();
    test_return_err_by_throw_exception();
    this->test_call_with_delay_func<coro_fun_with_delay_return_void>();
    this->test_call_with_delay_func<
        coro_fun_with_user_define_connection_type>();
    this->test_call_with_delay_func<coro_fun_with_delay_return_void_twice>();
    this->test_call_with_delay_func_client_read_length_error<
        coro_fun_with_delay_return_void>();
    this->test_call_with_delay_func_client_read_body_error<
        coro_fun_with_delay_return_void>();
    if (enable_heartbeat) {
      this->test_call_with_delay_func_server_timeout<
          coro_fun_with_delay_return_void_cost_long_time>();
    }
    this->test_call_with_delay_func<coro_fun_with_delay_return_string>();
    this->test_call_with_delay_func<coro_fun_with_delay_return_string_twice>();
  }
  void register_all_function() override {
    g_action = {};
    ELOGV(INFO, "run %s", __func__);
    server.register_handler<async_hi, large_arg_fun, client_hello>();
    server.register_handler<long_run_func>();
    server.register_handler<&ns_login::LoginService::login>(&login_service_);
    server.register_handler<&HelloService::hello>(&hello_service_);
    server.register_handler<hello>();
    server.register_handler<hi>();
    server.register_handler<test_context, test_lazy_context,
                            test_callback_context>();
    server.register_handler<test_response_error5, test_response_error6>();
    server.register_handler<coro_fun_with_user_define_connection_type>();
    server.register_handler<coro_fun_with_delay_return_void>();
    server.register_handler<coro_fun_with_delay_return_void_twice>();
    server.register_handler<coro_fun_with_delay_return_void_cost_long_time>();
    server.register_handler<coro_fun_with_delay_return_string>();
    server.register_handler<coro_fun_with_delay_return_string_twice>();
    server.register_handler<coro_func>();
    server.register_handler<coro_func_return_void>();
    server.register_handler<&HelloService::coro_func,
                            &HelloService::coro_func_return_void>(
        &hello_service_);
    server.register_handler<get_coro_value>();
    server.register_handler<&CoroServerTester::get_value>(this);
  }

  void test_context_func() {
    auto client = create_client();
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    client->set_req_attachment("1234567890987654321234567890");
    auto result = syncAwait(client->call<test_context>());
    CHECK(result);
    CHECK(client->get_resp_attachment() == "1234567890987654321234567890");

    client->set_req_attachment("12345678909876543212345678901");
    result = syncAwait(client->call<test_callback_context>());
    CHECK(result);
    CHECK(client->get_resp_attachment() == "12345678909876543212345678901");

    client->set_req_attachment("01234567890987654321234567890");
    result = syncAwait(client->call<test_lazy_context>());
    CHECK(result);
    CHECK(client->get_resp_attachment() == "01234567890987654321234567890");
  }
  void test_return_err_by_throw_exception() {
    {
      auto client = create_client();
      ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
      auto result = syncAwait(client->call<test_response_error5>());
      REQUIRE(!result);
      CHECK(result.error().code == coro_rpc::errc::address_in_used);
      CHECK(result.error().msg == "error with user-defined msg");
    }
    {
      auto client = create_client();
      ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
      auto result = syncAwait(client->call<test_response_error6>());
      REQUIRE(!result);
      CHECK(result.error().code == coro_rpc::errc::address_in_used);
      CHECK(result.error().msg == "error with user-defined msg");
    }
  }

  void test_function_not_registered() {
    g_action = {};
    auto client = create_client();
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = call<function_not_registered>(client);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::function_not_registered,
        std::to_string(client->get_client_id()).append(ret.error().msg));
    REQUIRE(client->has_closed() == true);
    ret = call<function_not_registered>(client);
    CHECK(client->has_closed() == true);
    ret = call<function_not_registered>(client);
    REQUIRE_MESSAGE(ret.error().code == coro_rpc::errc::io_error,
                    ret.error().msg);
    CHECK(client->has_closed() == true);
  }

  void test_server_start_again() {
    ELOGV(INFO, "run %s", __func__);

    auto ec = server.start();
    REQUIRE_MESSAGE(ec == coro_rpc::errc::io_error, ec.message());
  }

  void test_start_new_server_with_same_port() {
    ELOGV(INFO, "run %s", __func__);
    {
      auto new_server = coro_rpc_server(2, std::stoi(this->port_));
      auto ec = new_server.async_start();
      REQUIRE(ec.hasResult());
      REQUIRE_MESSAGE(ec.value() == coro_rpc::errc::address_in_used,
                      ec.value().message());
    }
    ELOGV(INFO, "OH NO");
  }
  void test_server_send_bad_rpc_result() {
    auto client = create_client(inject_action::server_send_bad_rpc_result);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = this->call<hi>(client);
    CHECK_MESSAGE(
        ret.error().code == coro_rpc::errc::invalid_rpc_result,
        std::to_string(client->get_client_id()).append(ret.error().msg));
    g_action = {};
  }

  void test_server_send_no_body() {
    auto client = create_client(inject_action::close_socket_after_send_length);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = this->template call<hello>(client);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
    g_action = {};
  }

  void test_coro_handler() {
    auto client = create_client(inject_action::nothing);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = this->template call<get_coro_value>(client, 42);
    CHECK(ret.value() == 42);

    auto ret1 = this->template call<&HelloService::coro_func>(client, 42);
    CHECK(ret1.value() == 42);

    auto ret2 = this->template call<&CoroServerTester::get_value>(client, 42);
    CHECK(ret2.value() == 42);

    auto ret3 = this->template call<coro_func>(client, 42);
    CHECK(ret3.value() == 42);

    auto ret4 = this->template call<coro_func_return_void>(client, 42);
    CHECK(ret4.has_value());

    auto ret5 =
        this->template call<&HelloService::coro_func_return_void>(client, 42);
    CHECK(ret5.has_value());
  }
  coro_rpc_server server;
  std::thread thd;
  HelloService hello_service_;
};
TEST_CASE("testing coro rpc server") {
  ELOGV(INFO, "run testing coro rpc server");
  unsigned short server_port = 8810;
  auto conn_timeout_duration = 500ms;
  std::vector<int> switch_list{0
#ifdef YLT_ENABLE_SSL
                               ,
                               1
#endif
#ifdef YLT_ENABLE_IBV
                               ,
                               2
#endif
  };
  for (auto enable_heartbeat : switch_list) {
    for (auto type : switch_list) {
      TesterConfig config;
      config.enable_heartbeat = enable_heartbeat;
      if (type == 0) {
        config.use_ssl = false;
        config.use_rdma = false;
      }
      else if (type == 1) {
        config.use_ssl = true;
        config.use_rdma = false;
      }
      else if (type == 2) {
        config.use_ssl = false;
        config.use_rdma = true;
      }
      config.sync_client = false;
      config.use_outer_io_context = false;
      config.port = server_port;
      if (enable_heartbeat) {
        config.conn_timeout_duration = conn_timeout_duration;
      }
      std::stringstream ss;
      ss << config;
      ELOGV(INFO, "config: %s", ss.str().data());
      CoroServerTester(config).run();
    }
    // }
  }
}

TEST_CASE("testing coro rpc server stop") {
  ELOGV(INFO, "run testing coro rpc server stop");
  coro_rpc_server server(2, 8810);
  auto res = server.async_start();
  REQUIRE_MESSAGE(!res.hasResult(), "server start failed");
  SUBCASE("stop twice") {
    server.stop();
    server.stop();
  }
  SUBCASE("stop in different thread") {
    std::thread thd1([&server]() {
      server.stop();
    });
    std::thread thd2([&server]() {
      server.stop();
    });
    thd1.join();
    thd2.join();
  }
}

TEST_CASE("test server accept error") {
  ELOGV(INFO, "run test server accept error");
  g_action = inject_action::force_inject_server_accept_error;
  coro_rpc_server server(2, 8810);
  server.register_handler<hi>();
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start timeout");
  coro_rpc_client client(coro_io::get_global_executor());
  ELOGV(INFO, "run test server accept error, client_id %d",
        client.get_client_id());
  auto ec = syncAwait(client.connect("127.0.0.1", "8810"));
  REQUIRE_MESSAGE(!ec,
                  std::to_string(client.get_client_id()).append(ec.message()));
  auto ret = syncAwait(client.call<hi>());
  REQUIRE_MESSAGE(ret.error().code == coro_rpc::errc::io_error,
                  ret.error().msg);
  REQUIRE(client.has_closed() == true);
  g_action = {};
}

TEST_CASE("test server write queue") {
  using coro_rpc_protocol = coro_rpc::protocol::coro_rpc_protocol;
  ELOGV(INFO, "run server write queue");
  g_action = {};
  coro_rpc_server server(2, 8810);
  server.register_handler<coro_fun_with_delay_return_void_cost_long_time>();
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start timeout");
  std::string buffer;
  buffer.reserve(coro_rpc_protocol::REQ_HEAD_LEN +
                 struct_pack::get_needed_size(std::monostate{}));
  buffer.resize(coro_rpc_protocol::REQ_HEAD_LEN);
  auto &header = *(coro_rpc_protocol::req_header *)buffer.data();
  header.magic = coro_rpc_protocol::magic_number;
  header.function_id =
      func_id<coro_fun_with_delay_return_void_cost_long_time>();
  header.seq_num = g_client_id++;
  header.length = struct_pack::get_needed_size(std::monostate{});
  ELOGV(INFO, "client_id %d begin to connect %d", header.seq_num, 8820);
  struct_pack::serialize_to(buffer, std::monostate{});
  asio::io_context io_context;
  std::thread thd([&io_context]() {
    asio::io_context::work work(io_context);
    io_context.run();
  });
  asio::ip::tcp::socket socket(io_context);
  auto ret = coro_io::connect(io_context, socket, "127.0.0.1", "8810");
  CHECK(!ret);
  ELOGV(INFO, "%s client_id %d call %s", "sync_client", header.seq_num,
        "coro_fun_with_delay_return_void_cost_long_time");
  for (int i = 0; i < 1; ++i) {
    auto err =
        coro_io::write(socket, asio::buffer(buffer.data(), buffer.size()));
    CHECK(err.second == buffer.size());
  }
  for (int i = 0; i < 1; ++i) {
    char buffer2[coro_rpc_protocol::RESP_HEAD_LEN];
    std::monostate r;
    auto buf = struct_pack::serialize<std::string>(r);
    std::string buffer_read;
    buffer_read.resize(buf.size());
    read(socket, asio::buffer(buffer2, coro_rpc_protocol::RESP_HEAD_LEN));
    [[maybe_unused]] auto resp_head =
        *(coro_rpc_protocol::resp_header *)buffer2;
    uint32_t body_len = header.length;
    CHECK(body_len == buf.size());
    read(socket, asio::buffer(buffer_read, body_len));
    std::monostate r2;
    std::size_t sz;
    auto ret =
        struct_pack::deserialize_to(r2, buffer_read.data(), body_len, sz);
    CHECK(!ret);
    CHECK(sz == body_len);
    CHECK(r2 == r);
  }

  ELOGV(INFO, "client_id %d close", header.seq_num);
  asio::error_code ignored_ec;
  socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignored_ec);
  socket.close(ignored_ec);
  io_context.stop();
  thd.join();
  server.stop();
}

TEST_CASE("testing coro rpc write error") {
  ELOGV(INFO, "run testing coro rpc write error");
  g_action = inject_action::force_inject_connection_close_socket;
  coro_rpc_server server(2, 8810);
  server.register_handler<hi>();
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  ELOGV(INFO, "run testing coro rpc write error, client_id %d",
        client.get_client_id());
  auto ec = syncAwait(client.connect("127.0.0.1", "8810"));
  REQUIRE_MESSAGE(!ec,
                  std::to_string(client.get_client_id()).append(ec.message()));
  auto ret = syncAwait(client.call<hi>());
  REQUIRE_MESSAGE(
      ret.error().code == coro_rpc::errc::io_error,
      std::to_string(client.get_client_id()).append(ret.error().msg));
  REQUIRE(client.has_closed() == true);
  g_action = inject_action::nothing;
}

TEST_CASE("testing ipv6") {
  using namespace coro_http;
  coro_rpc_server server(1, "[::1]:8811");
  server.register_handler<hi>();
  auto res = server.async_start();
  if (res.hasResult()) {
    return;
  }
  CHECK_MESSAGE(!res.hasResult(), "server start failed");

  coro_rpc_client client{};
  auto ec = syncAwait(client.connect("[::1]:8811"));
  REQUIRE_MESSAGE(!ec,
                  std::to_string(client.get_client_id()).append(ec.message()));
  auto ret = syncAwait(client.call<hi>());
  CHECK(ret);

  coro_rpc_client client1{};
  ec = syncAwait(client1.connect("::1:8811"));
  REQUIRE_MESSAGE(!ec,
                  std::to_string(client.get_client_id()).append(ec.message()));
  ret = syncAwait(client1.call<hi>());
  CHECK(ret);

  coro_http::coro_http_server http_server(1, 8812, "::1");
  http_server.set_http_handler<coro_http::GET>(
      "/test", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "ok");
      });
  http_server.async_start();
  coro_http::coro_http_client htttp_client{};
  auto r = syncAwait(htttp_client.connect("http://[::1]:8812"));
  CHECK(!r.net_err);
  auto result = htttp_client.get("/test");
  CHECK(result.status == 200);

  coro_http::coro_http_client htttp_client1{};
  result = htttp_client1.get("http://[::1]:8812/test");
  CHECK(result.status == 200);
}

TEST_CASE("testing coro rpc subserver") {
  ELOGV(INFO, "run testing coro rpc subserver");
  std::string http_body = R"(
<!DOCTYPE html>
<html>
    <head>
        <title>Example</title>
    </head>
    <body>
        <p>This is an example of a simple HTML page with one paragraph.</p>
    </body>
</html>)";
  coro_rpc_server server(2, 8810);
  server.register_handler<hi>();
  std::function dispatcher = [](coro_io::socket_wrapper_t &&soc,
                                std::string_view magic_number,
                                coro_http::coro_http_server &server) {
    CHECK(magic_number == "G");
    server.transfer_connection(std::move(soc), magic_number);
  };
  auto http_server = std::make_unique<coro_http::coro_http_server>(0, 0);
  http_server->set_http_handler<coro_http::GET>(
      "/index.html",
      [&](coro_http::coro_http_request &, coro_http::coro_http_response &resp) {
        resp.set_status_and_content(coro_http::status_type::ok, http_body);
      });
  server.add_subserver(std::move(dispatcher), std::move(http_server));
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  auto ec = syncAwait(client.connect("127.0.0.1", "8810"));
  REQUIRE_MESSAGE(!ec,
                  std::to_string(client.get_client_id()).append(ec.message()));
  auto ret = syncAwait(client.call<hi>());
  REQUIRE_MESSAGE(ret.has_value(), ret.error().msg);
  coro_http::coro_http_client cli;
  auto result = syncAwait(cli.connect("localhost:8810"));
  CHECK_MESSAGE(!result.net_err, result.net_err.message());
  result = syncAwait(cli.async_get("/index.html"));
  CHECK_MESSAGE(result.status == (int)coro_http::status_type::ok,
                result.status);
  CHECK_MESSAGE(result.resp_body == http_body, result.resp_body);
}
#ifdef YLT_ENABLE_SSL
TEST_CASE("testing coro rpc ssl subserver") {
  ELOGV(INFO, "run testing coro rpc ssl subserver");
  std::string http_body = R"(
<!DOCTYPE html>
<html>
    <head>
        <title>Example</title>
    </head>
    <body>
        <p>This is an example of a simple HTML page with one paragraph.</p>
    </body>
</html>)";
  coro_rpc_server server(2, 8810);
  server.init_ssl(
      ssl_configure{"../openssl_files", "server.crt", "server.key"});
  server.register_handler<hi>();
  std::function dispatcher = [](coro_io::socket_wrapper_t &&soc,
                                std::string_view magic_number,
                                coro_http::coro_http_server &server) {
    CHECK(magic_number == "G");
    server.transfer_connection(std::move(soc), magic_number);
  };
  auto http_server = std::make_unique<coro_http::coro_http_server>(0, 0);
  http_server->set_http_handler<coro_http::GET>(
      "/index.html",
      [&](coro_http::coro_http_request &, coro_http::coro_http_response &resp) {
        resp.set_status_and_content(coro_http::status_type::ok, http_body);
      });
  server.add_subserver(std::move(dispatcher), std::move(http_server));
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  CHECK(client.init_ssl("../openssl_files", "server.crt"));
  auto ec = syncAwait(client.connect("127.0.0.1", "8810"));
  REQUIRE_MESSAGE(!ec,
                  std::to_string(client.get_client_id()).append(ec.message()));
  auto ret = syncAwait(client.call<hi>());
  REQUIRE_MESSAGE(ret.has_value(), ret.error().msg);
  coro_http::coro_http_client cli;
  CHECK_MESSAGE(
      cli.init_ssl(asio::ssl::verify_peer, "../openssl_files/server.crt"),
      "init ssl failed");
  auto result = syncAwait(cli.connect("https://localhost:8810"));
  CHECK_MESSAGE(!result.net_err, result.net_err.message());
  result = syncAwait(cli.async_get("/index.html"));
  CHECK_MESSAGE(result.status == (int)coro_http::status_type::ok,
                result.net_err.message());
  CHECK_MESSAGE(result.resp_body == http_body, result.resp_body);
}
#ifdef YLT_ENABLE_IBV
TEST_CASE("testing coro rpc non-rdma client to rdma server") {
  ELOGV(INFO, "run testing coro rpc non-rdma client to rdma server");
  coro_rpc_server server(2, 8810);
  server.init_ibv();
  server.register_handler<hi>();
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  auto ec = syncAwait(client.connect("127.0.0.1", "8810"));
  REQUIRE_MESSAGE(!ec,
                  std::to_string(client.get_client_id()).append(ec.message()));
  auto ret = syncAwait(client.call<hi>());
  REQUIRE_MESSAGE(ret.has_value(), ret.error().msg);
}
TEST_CASE("testing coro rpc subserver with rdma") {
  ELOGV(INFO, "run testing coro rpc subserver with rdma");
  std::string http_body = R"(
<!DOCTYPE html>
<html>
    <head>
        <title>Example</title>
    </head>
    <body>
        <p>This is an example of a simple HTML page with one paragraph.</p>
    </body>
</html>)";
  coro_rpc_server server(2, 8810);
  server.init_ibv();
  server.register_handler<hi>();
  std::function dispatcher = [](coro_io::socket_wrapper_t &&soc,
                                std::string_view magic_number,
                                coro_http::coro_http_server &server) {
    CHECK(magic_number == "G");
    server.transfer_connection(std::move(soc), magic_number);
  };
  auto http_server = std::make_unique<coro_http::coro_http_server>(0, 0);
  http_server->set_http_handler<coro_http::GET>(
      "/index.html",
      [&](coro_http::coro_http_request &, coro_http::coro_http_response &resp) {
        resp.set_status_and_content(coro_http::status_type::ok, http_body);
      });
  server.add_subserver(std::move(dispatcher), std::move(http_server));
  auto res = server.async_start();
  CHECK_MESSAGE(!res.hasResult(), "server start failed");
  coro_rpc_client client(coro_io::get_global_executor());
  bool is_ok = client.init_ibv();
  REQUIRE_MESSAGE(is_ok, "init ibv failed");
  auto ec = syncAwait(client.connect("127.0.0.1", "8810"));
  REQUIRE_MESSAGE(!ec,
                  std::to_string(client.get_client_id()).append(ec.message()));
  auto ret = syncAwait(client.call<hi>());
  REQUIRE_MESSAGE(ret.has_value(), ret.error().msg);
  coro_http::coro_http_client cli;
  auto result = syncAwait(cli.connect("localhost:8810"));
  CHECK_MESSAGE(!result.net_err, result.net_err.message());
  result = syncAwait(cli.async_get("/index.html"));
  CHECK_MESSAGE(result.status == (int)coro_http::status_type::ok,
                result.status);
  CHECK_MESSAGE(result.resp_body == http_body, result.resp_body);
}
#ifdef YLT_ENABLE_SSL

// TODO: open it when whe support rdma client with ssl-encrypt connection

// TEST_CASE("testing coro rpc non-rdma ssl client to ssl rdma server") {
//   ELOGV(INFO, "run testing coro rpc non-rdma client to rdma server");
//   coro_rpc_server server(2, 8810);
//   server.init_ibv();
//   server.init_ssl(
//     ssl_configure{"../openssl_files", "server.crt", "server.key"});
//   server.register_handler<hi>();
//   auto res = server.async_start();
//   CHECK_MESSAGE(!res.hasResult(), "server start failed");
//   coro_rpc_client client(coro_io::get_global_executor());
//   CHECK(client.init_ssl("../openssl_files", "server.crt"));
//   auto ec = syncAwait(client.connect("127.0.0.1", "8810"));
//   REQUIRE_MESSAGE(!ec,
//                   std::to_string(client.get_client_id()).append(ec.message()));
//   auto ret = syncAwait(client.call<hi>());
//   REQUIRE_MESSAGE(ret.has_value(), ret.error().msg);
// }

// TEST_CASE("testing ssl coro rpc subserver with rdma") {
//   ELOGV(INFO, "run testing ssl coro rpc subserver with rdma");
//   std::string http_body = R"(
// <!DOCTYPE html>
// <html>
//     <head>
//         <title>Example</title>
//     </head>
//     <body>
//         <p>This is an example of a simple HTML page with one paragraph.</p>
//     </body>
// </html>)";
//   coro_rpc_server server(2, 8810);
//   server.init_ssl(
//     ssl_configure{"../openssl_files", "server.crt", "server.key"});
//   server.init_ibv();
//   server.register_handler<hi>();
//   std::function dispatcher = [](coro_io::socket_wrapper_t &&soc,
//                                 std::string_view magic_number,
//                                 coro_http::coro_http_server &server) {
//     CHECK(magic_number == "G");
//     server.transfer_connection(std::move(soc), magic_number);
//   };
//   auto http_server = std::make_unique<coro_http::coro_http_server>(0, 0);
//   http_server->set_http_handler<coro_http::GET>(
//       "/index.html",
//       [&](coro_http::coro_http_request &, coro_http::coro_http_response
//       &resp) {
//         resp.set_status_and_content(coro_http::status_type::ok, http_body);
//       });
//   server.add_subserver(std::move(dispatcher), std::move(http_server));
//   auto res = server.async_start();
//   CHECK_MESSAGE(!res.hasResult(), "server start failed");
//   coro_rpc_client client(coro_io::get_global_executor());
//   bool is_ok = client.init_ibv();
//   CHECK(client.init_ssl("../openssl_files", "server.crt"));
//   REQUIRE_MESSAGE(is_ok,"init ibv failed");
//   auto ec = syncAwait(client.connect("127.0.0.1", "8810"));
//   REQUIRE_MESSAGE(!ec,
//                   std::to_string(client.get_client_id()).append(ec.message()));
//   auto ret = syncAwait(client.call<hi>());
//   REQUIRE_MESSAGE(ret.has_value(), ret.error().msg);
//   coro_http::coro_http_client cli;
//   auto result = syncAwait(cli.connect("localhost:8810"));
//   CHECK_MESSAGE(!result.net_err, result.net_err.message());
//   result = syncAwait(cli.async_get("/index.html"));
//   CHECK_MESSAGE(result.status == (int)coro_http::status_type::ok,
//                 result.status);
//   CHECK_MESSAGE(result.resp_body == http_body, result.resp_body);
// }
#endif
#endif
#endif