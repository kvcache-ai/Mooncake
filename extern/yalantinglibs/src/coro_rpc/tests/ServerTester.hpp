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
#ifndef CORO_RPC_SERVERTESTER_HPP
#define CORO_RPC_SERVERTESTER_HPP
#include <async_simple/coro/Lazy.h>

#include <exception>
#include <future>
#include <ostream>
#include <string>
#include <thread>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/easylog.hpp>

#include "doctest.h"
#include "inject_action.hpp"
#include "rpc_api.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/coro_rpc/impl/errno.h"

#ifdef _MSC_VER
#define CORO_RPC_FUNCTION_SIGNATURE __FUNCSIG__
#else
#define CORO_RPC_FUNCTION_SIGNATURE __PRETTY_FUNCTION__
#endif

using namespace std::string_literals;
using namespace async_simple::coro;
using namespace coro_rpc;
using namespace std::chrono_literals;

struct TesterConfig {
  TesterConfig() = default;
  TesterConfig(TesterConfig &c) {
    enable_heartbeat = c.enable_heartbeat;
    use_ssl = c.use_ssl;
    use_rdma = c.use_rdma;
    sync_client = c.sync_client;
    use_outer_io_context = c.use_outer_io_context;
    port = c.port;
    conn_timeout_duration = c.conn_timeout_duration;
  }
  bool enable_heartbeat;
  bool use_ssl;
  bool use_rdma;
  bool sync_client;
  bool use_outer_io_context;
  unsigned short port;
  std::string address = "0.0.0.0";
  std::chrono::steady_clock::duration conn_timeout_duration =
      std::chrono::seconds(0);

  friend std::ostream &operator<<(std::ostream &os,
                                  const TesterConfig &config) {
    os << std::boolalpha;
    os << " enable_heartbeat: " << config.enable_heartbeat << ";"
       << " use_ssl: " << config.use_ssl << ";"
       << " use_rdma: " << config.use_rdma << ";"
       << " sync_client: " << config.sync_client << ";"
       << " use_outer_io_context: " << config.use_outer_io_context << ";"
       << " port: " << config.port << ";"
       << " address: " << config.address << ";";
    os << " conn_timeout_duration: ";
    auto val = std::chrono::duration_cast<std::chrono::milliseconds>(
                   config.conn_timeout_duration)
                   .count();
    os << val << "ms"
       << ";";
    return os;
  }
};

struct ServerTester : TesterConfig {
  ServerTester(TesterConfig config)
      : TesterConfig(config),
        executor_(io_context_.get_executor()),
        port_(std::to_string(config.port)) {
    if (use_outer_io_context) {
      std::promise<void> promise;
      auto future = promise.get_future();
      thd_ = std::thread([this, &promise]() {
        asio::io_context::work work(io_context_);
        promise.set_value();
        io_context_.run();
      });
      future.wait();
    }

    std::stringstream ss;
    ss << config;
    conf_str_ = ss.str();
  }
  ~ServerTester() {
    if (use_outer_io_context) {
      io_context_.stop();
      thd_.join();
    }
    g_action = coro_rpc::inject_action::nothing;
  }
  void run() {
    ELOGV(INFO, "run test: heartbeat=%d, ssl=%d", enable_heartbeat, use_ssl);
    register_all_function();
    test_all();
  }
  virtual void test_all() {
    test_connect_timeout();
    test_function_registered();
    if (sync_client) {
      // only sync_call implement inject behavior
      test_client_send_bad_header();
      test_client_send_bad_magic_num();
      test_client_send_header_length_is_0();
      test_client_close_socket_after_send_header();
      test_client_close_socket_after_send_partial_header();
      test_client_close_socket_after_send_payload();
    }
    test_heartbeat();
    test_call_function_with_long_response_time();
    test_call_with_large_buffer();
  }
  std::shared_ptr<coro_rpc_client> create_client(
      inject_action action = inject_action::nothing) {
    std::shared_ptr<coro_rpc_client> client;
    // sometimes, connect will take more than conn_timeout_duration(500ms), so
    // retry 3 times to make sure connect ok.
    coro_rpc::errc ec;
    int retry = 4;
    for (int i = 0; i < retry; i++) {
      if (use_outer_io_context) {
        client = std::make_shared<coro_rpc_client>(&executor_);
      }
      else {
        client =
            std::make_shared<coro_rpc_client>(coro_io::get_global_executor());
      }
#ifdef YLT_ENABLE_SSL
      if (use_ssl) {
        bool ok = client->init_ssl("../openssl_files", "server.crt");
        REQUIRE_MESSAGE(ok == true, "init ssl fail, please check ssl config");
      }
#endif
#ifdef YLT_ENABLE_IBV
      if (use_rdma) {
        bool ok = client->init_ibv();
        REQUIRE_MESSAGE(ok == true,
                        "init ibv fail, please check ibverbs config");
      }
#endif
      g_action = action;

      if (sync_client) {
        ec = client->sync_connect("127.0.0.1", port_);
      }
      else {
        ec = syncAwait(client->connect("127.0.0.1", port_));
      }

      if (!ec) {
        break;
      }

      ELOGV(INFO, "retry times %d", i);
    }

    REQUIRE_MESSAGE(!ec, std::to_string(client->get_client_id())
                             .append(" not connected ")
                             .append(conf_str_));
    return client;
  }
  template <auto func, typename... Args>
  decltype(auto) call(std::shared_ptr<coro_rpc_client> client, Args &&...args) {
    ELOGV(INFO, "%s client_id %d call %s",
          sync_client ? "sync_client" : "async_client", client->get_client_id(),
          coro_rpc::get_func_name<func>().data());
    if (sync_client) {
      return client->sync_call<func>(std::forward<Args>(args)...);
    }
    else {
      return syncAwait(
          client->template call<func>(std::forward<Args>(args)...));
    }
  }

  virtual void register_all_function() {}

  virtual void remove_all_rpc_function() {}

  void test_function_registered() {
    g_action = {};

    // because heartbeat timeout is 500ms, sometimes the client closed because
    // of timeout, so retry call.
    int retry = 4;
    for (int i = 0; i < retry; i++) {
      auto client = create_client();
      ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
      ELOGV(INFO, "retry call times %d", i);
      {
        auto ret = call<async_hi>(client);
        ELOGV(INFO, "client_id %d call async_hi", client->get_client_id());
        if (!ret) {
          ELOGV(ERROR, "client_id %d call async_hi error %s",
                client->get_client_id(), ret.error().msg.data());
          continue;
        }
        else {
          ELOGV(INFO, "client_id %d call async_hi ok, result %s",
                client->get_client_id(), ret.value().data());
        }
        CHECK(ret.value() == "async hi"s);
      }
      {
        auto ret = call<hello>(client);
        ELOGV(INFO, "client_id %d call hello", client->get_client_id());
        if (!ret) {
          ELOGV(ERROR, "client_id %d call hello error %s",
                client->get_client_id(), ret.error().msg.data());
          continue;
        }
        else {
          ELOGV(INFO, "client_id %d call hello ok, result %s",
                client->get_client_id(), ret.value().data());
        }
        CHECK(ret.value() == "hello"s);
      }
      {
        auto ret = call<&HelloService::hello>(client);
        ELOGV(INFO, "client_id %d call HelloService::hello",
              client->get_client_id());
        if (!ret) {
          ELOGV(ERROR, "client_id %d call HelloService::hello error %s",
                client->get_client_id(), ret.error().msg.data());
          continue;
        }
        else {
          ELOGV(INFO, "client_id %d call HelloService::hello ok, result %s",
                client->get_client_id(), ret.value().data());
        }
        CHECK(ret.value() == "hello"s);
      }
      {
#ifdef __GNUC__
        auto ret = call<&ns_login::LoginService::login>(client, "foo"s, "bar"s);
        if (!ret) {
          ELOGV(WARN, "%d %s", client->get_client_id(), ret.error().msg.data());
        }
        CHECK(ret.value() == true);
#endif
      }

      break;
    }
  }
  void test_client_send_bad_header() {
    g_action = {};
    auto client = create_client(inject_action::client_send_bad_header);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = call<client_hello>(client);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  }
  void test_client_send_bad_magic_num() {
    g_action = {};
    auto client = create_client(inject_action::client_send_bad_magic_num);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = call<client_hello>(client);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  }
  void test_client_send_header_length_is_0() {
    g_action = {};
    auto client = create_client(inject_action::client_send_header_length_0);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = call<client_hello>(client);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  }
  void test_client_close_socket_after_send_header() {
    g_action = {};
    auto client =
        create_client(inject_action::client_close_socket_after_send_header);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = call<client_hello>(client);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  }
  void test_client_close_socket_after_send_partial_header() {
    g_action = {};
    auto client = create_client(
        inject_action::client_close_socket_after_send_partial_header);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = call<client_hello>(client);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  }
  void test_client_close_socket_after_send_payload() {
    g_action = {};
    auto client =
        create_client(inject_action::client_close_socket_after_send_payload);
    auto ret = call<client_hello>(client);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  }

  void test_heartbeat() {
    // auto client = create_client(inject_action::nothing);
    // ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    // auto ret = call<async_hi>(client);
    // CHECK(ret.value() == "async hi"s);

    // std::this_thread::sleep_for(700ms);

    // ret = call<async_hi>(client);
    // if (enable_heartbeat) {
    //   REQUIRE_MESSAGE(
    //       ret.error().code == coro_rpc::errc::io_error,
    //       std::to_string(client->get_client_id()).append(ret.error().msg));
    // }
    // else {
    //   CHECK(ret.value() == "async hi"s);
    // }
    // ELOGV(INFO, "test heartbeat done");
  }
  void test_call_function_with_long_response_time() {
    auto client = create_client(inject_action::nothing);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = call<long_run_func>(client, 1);
    CHECK(ret.value() == 1);
  }
  void test_call_with_large_buffer() {
    auto client = create_client(inject_action::nothing);
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    std::string arg;
    arg.resize(2048);
    auto ret = call<large_arg_fun>(client, arg);
    CHECK(ret.value() == arg);
  }

  void test_connect_timeout() {
    g_action = {};
    if (sync_client) {
      return;
    }
    auto init_client = [this]() {
      std::shared_ptr<coro_rpc_client> client;
      if (use_outer_io_context) {
        client = std::make_shared<coro_rpc_client>(&executor_);
      }
      else {
        client =
            std::make_shared<coro_rpc_client>(coro_io::get_global_executor());
      }
#ifdef YLT_ENABLE_SSL
      if (use_ssl) {
        bool ok = client->init_ssl("../openssl_files", "server.crt");
        REQUIRE_MESSAGE(ok == true, "init ssl fail, please check ssl config");
      }
#endif
#ifdef YLT_ENABLE_IBV
      if (use_rdma) {
        bool ok = client->init_ibv();
        REQUIRE_MESSAGE(ok == true,
                        "init ibv fail, please check ibverbs config");
      }
#endif
      return client;
    };
    auto client = init_client();
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    coro_rpc::err_code ec;
    auto client2 = init_client();
    ec = syncAwait(client2->connect("10.255.255.1", port_, 5ms));
    CHECK_MESSAGE(ec,
                  std::to_string(client->get_client_id()).append(ec.message()));
  }

  template <auto func, typename... Args>
  void test_call_with_delay_func(Args... args) {
    g_action = {};
    auto client = create_client();
    ELOGV(INFO, "run %s, client_id %d", __func__, client->get_client_id());
    auto ret = call<func>(client, std::forward<Args>(args)...);
    CHECK(ret.has_value());
  }

  template <auto func, typename... Args>
  void test_call_with_delay_func_client_read_length_error(Args... args) {
    g_action = {};
    auto client = this->create_client();
    ELOGV(INFO, "run %s, client_id %d", CORO_RPC_FUNCTION_SIGNATURE,
          client->get_client_id());
    g_action = inject_action::close_socket_after_read_header;
    auto ret = this->template call<func>(client, std::forward<Args>(args)...);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  };

  template <auto func, typename... Args>
  void test_call_with_delay_func_client_read_body_error(Args... args) {
    g_action = {};
    auto client = this->create_client();
    ELOGV(INFO, "run %s, client_id %d", CORO_RPC_FUNCTION_SIGNATURE,
          client->get_client_id());
    g_action = inject_action::close_socket_after_send_length;
    auto ret = this->template call<func>(client, std::forward<Args>(args)...);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  };

  template <auto func, typename... Args>
  void test_call_with_delay_func_server_timeout(Args... args) {
    g_action = {};
    auto client = this->create_client();
    ELOGV(INFO, "run %s, client_id %d", CORO_RPC_FUNCTION_SIGNATURE,
          client->get_client_id());
    auto ret = this->template call<func>(client, std::forward<Args>(args)...);
    REQUIRE(ret);
    std::this_thread::sleep_for(700ms);
    ret = this->call<func>(client, std::forward<Args>(args)...);
    REQUIRE(!ret);
    REQUIRE_MESSAGE(
        ret.error().code == coro_rpc::errc::io_error,
        std::to_string(client->get_client_id()).append(ret.error().msg));
  };
  asio::io_context io_context_;
  coro_io::ExecutorWrapper<> executor_;
  std::string port_;
  std::thread thd_;
  ns_login::LoginService login_service_;
  HelloService hello_service_;
  std::string conf_str_;
};
#endif  // CORO_RPC_SERVERTESTER_HPP
