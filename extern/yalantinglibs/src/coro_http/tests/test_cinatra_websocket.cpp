#include <chrono>
#include <filesystem>
#include <future>
#include <memory>
#include <system_error>

#include "cinatra/error.hpp"
#include "doctest.h"
#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_http/coro_http_server.hpp"

using namespace std::chrono_literals;

using namespace coro_http;

#ifdef CINATRA_ENABLE_SSL
TEST_CASE("test wss client") {
  cinatra::coro_http_server server(1, 9001);
  server.init_ssl("../openssl_files/server.crt", "../openssl_files/server.key",
                  "test");
  server.set_http_handler<cinatra::GET>(
      "/",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            break;
          }
        }
      });
  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  bool ok =
      client.init_ssl(asio::ssl::verify_peer, "../openssl_files/server.crt");
  REQUIRE_MESSAGE(ok == true, "init ssl fail, please check ssl config");

  async_simple::coro::syncAwait(client.connect("wss://localhost:9001"));

  async_simple::coro::syncAwait(client.write_websocket("hello"));
  auto data = async_simple::coro::syncAwait(client.read_websocket());
  CHECK(data.resp_body == "hello");

  client.close();

  server.stop();
}
#endif

async_simple::coro::Lazy<void> test_websocket(coro_http_client &client) {
  auto r = co_await client.connect("ws://localhost:8090/ws");
  if (r.net_err) {
    co_return;
  }

  co_await client.write_websocket("hello websocket");
  auto data = co_await client.read_websocket();
  CHECK(data.resp_body == "hello websocket");
  co_await client.write_websocket("test again");
  data = co_await client.read_websocket();
  CHECK(data.resp_body == "test again");
  co_await client.write_websocket_close("ws close");
  data = co_await client.read_websocket();
  CHECK(data.resp_body == "ws close");
  CHECK(data.net_err == asio::error::eof);
}

#ifdef CINATRA_ENABLE_GZIP
async_simple::coro::Lazy<void> test_gzip_websocket(coro_http_client &client) {
  auto r = co_await client.connect("ws://localhost:8090/ws");
  if (r.net_err) {
    co_return;
  }

  std::string str = "hello websocket";
  co_await client.write_websocket(str.data(), str.size());
  auto data = co_await client.read_websocket();
  CHECK(data.resp_body == "hello websocket");

  co_await client.write_websocket_close("ws close");
  data = co_await client.read_websocket();
  CHECK(data.resp_body == "ws close");
  CHECK(data.net_err == asio::error::eof);
}
#endif

TEST_CASE("test websocket") {
  cinatra::coro_http_server server(1, 8090);
  server.set_http_handler<cinatra::GET>(
      "/ws",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            break;
          }
        }
      });
  server.set_http_handler<cinatra::GET>(
      "/test_client_timeout",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          std::this_thread::sleep_for(200ms);

          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            break;
          }
        }
      });
  server.async_start();

  auto client_timeout = []() -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    client.set_req_timeout(50ms);
    client.set_ws_sec_key("s//GYHa/XO7Hd2F2eOGfyA==");

    auto r = co_await client.connect("ws://localhost:8090/test_client_timeout");
    if (r.net_err) {
      co_return;
    }

    co_await client.write_websocket("hello websocket");
    auto data = co_await client.read_websocket();
    CINATRA_LOG_DEBUG << data.net_err.message();
    CHECK(data.net_err == http_errc::request_timeout);
  };

  async_simple::coro::syncAwait(client_timeout());

  coro_http_client client{};
  client.set_ws_sec_key("s//GYHa/XO7Hd2F2eOGfyA==");

  async_simple::coro::syncAwait(test_websocket(client));

#ifdef INJECT_FOR_HTTP_CLIENT_TEST
  {
    auto lazy1 = []() -> async_simple::coro::Lazy<void> {
      coro_http_client client{};
      co_await client.connect("ws://localhost:8090/ws");
      std::string send_str = "test";
      websocket ws{};
      // msg too long
      auto header = ws.encode_ws_header(9 * 1024 * 1024, opcode::text, true);
      co_await client.async_write_raw(header);
      co_await client.async_write_raw(send_str);
      auto data = co_await client.read_websocket();
      CHECK(data.status != 200);
      CINATRA_LOG_DEBUG << data.resp_body;
    };
    async_simple::coro::syncAwait(lazy1());
  }

  {
    auto lazy1 = []() -> async_simple::coro::Lazy<void> {
      coro_http_client client{};
      co_await client.connect("ws://localhost:8090/ws");
      std::string send_str = "test";
      websocket ws{};
      // error frame
      auto header = ws.encode_ws_header(send_str.size(), (opcode)15, true);
      co_await client.async_write_raw(header);
      co_await client.async_write_raw(send_str);
      auto data = co_await client.read_websocket();
      CHECK(data.status != 200);
    };
    async_simple::coro::syncAwait(lazy1());
  }
#endif

#ifdef CINATRA_ENABLE_GZIP
  coro_http_client client1{};
  client1.set_ws_deflate(true);
  async_simple::coro::syncAwait(test_gzip_websocket(client1));
#endif

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // client->async_close();
}

void test_websocket_content(size_t len) {
  cinatra::coro_http_server server(1, 8090);
  server.set_http_handler<cinatra::GET>(
      "/",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            break;
          }
        }
      });
  server.async_start();

  auto lazy = [len]() -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    co_await client.connect("ws://localhost:8090");
    std::string send_str(len, 'a');
    co_await client.write_websocket(std::string(send_str));
    auto data = co_await client.read_websocket();
    REQUIRE(data.resp_body.size() == send_str.size());
    CHECK(data.resp_body == send_str);
  };

  async_simple::coro::syncAwait(lazy());

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  server.stop();
}

TEST_CASE("test websocket content lt 126") {
  test_websocket_content(1);
  test_websocket_content(125);
}

TEST_CASE("test websocket content ge 126") {
  test_websocket_content(126);
  test_websocket_content(127);
}

TEST_CASE("test websocket content ge 65535") {
  test_websocket_content(65535);
  test_websocket_content(65536);
}

TEST_CASE("test send after server stop") {
  cinatra::coro_http_server server(1, 8090);
  server.async_start();

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  coro_http_client client{};
  async_simple::coro::syncAwait(client.connect("ws://127.0.0.1:8090"));

  server.stop();

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  async_simple::coro::syncAwait(client.write_websocket(""));
  auto data = async_simple::coro::syncAwait(client.read_websocket());
  CHECK(data.net_err);
}

TEST_CASE("test read write in different threads") {
  cinatra::coro_http_server server(1, 8090);
  size_t count = 0;
  std::promise<void> promise;
  server.set_http_handler<cinatra::GET>(
      "/",
      [&](coro_http_request &req,
          coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }
          count++;
          if (count == 100) {
            promise.set_value();
            break;
          }
          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            break;
          }
        }
      });
  server.async_start();

  auto client = std::make_shared<coro_http_client>();
  std::string send_str(100, 'a');
  std::weak_ptr weak = client;
  auto another_thread_lazy = [client,
                              send_str]() -> async_simple::coro::Lazy<void> {
    for (int i = 0; i < 100; i++) {
      auto data = co_await client->read_websocket();
      if (data.net_err) {
        co_return;
      }
      REQUIRE(data.resp_body.size() == send_str.size());
      CHECK(data.resp_body == send_str);
    }
  };
  another_thread_lazy().start([](auto &&) {
  });

  auto lazy = [client, weak, &send_str]() -> async_simple::coro::Lazy<void> {
    co_await client->connect("ws://localhost:8090");
    for (int i = 0; i < 100; i++) {
      auto data = co_await client->write_websocket(std::string(send_str));
      if (data.net_err) {
        co_return;
      }
    }
  };

  async_simple::coro::syncAwait(lazy().via(&client->get_executor()));

  promise.get_future().wait_for(std::chrono::seconds(2));

  server.stop();
}

async_simple::coro::Lazy<void> test_websocket() {
  coro_http_client client{};
  auto r = co_await client.connect("ws://127.0.0.1:8089/ws_echo");
  if (r.net_err) {
    co_return;
  }

  co_await client.write_websocket(std::string_view("test2fdsaf"),
                                  opcode::binary);
  auto data = co_await client.read_websocket();
  CHECK(data.resp_body == "test2fdsaf");

  co_await client.write_websocket_close("ws close");
  data = co_await client.read_websocket();
  CHECK(data.net_err == asio::error::eof);
  CHECK(data.resp_body == "ws close");
}

TEST_CASE("test client quit after send msg") {
  coro_http_server server(1, 8089);
  server.set_http_handler<cinatra::GET>(
      "/ws_echo",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        websocket_result result{};

        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            break;
          }

          co_await resp.get_conn()->write_websocket(result.data);
        }
      });
  server.async_start();

  async_simple::coro::syncAwait(test_websocket());
}

#ifdef CINATRA_ENABLE_GZIP
TEST_CASE("test websocket permessage defalte") {
  coro_http_server server(1, 8090);
  server.set_http_handler<cinatra::GET>(
      "/ws_extesion",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            CINATRA_LOG_DEBUG << "close frame\n";
            break;
          }

          if (result.type == ws_frame_type::WS_TEXT_FRAME ||
              result.type == ws_frame_type::WS_BINARY_FRAME) {
            CHECK(result.data == "test");
          }
          else if (result.type == ws_frame_type::WS_PING_FRAME ||
                   result.type == ws_frame_type::WS_PONG_FRAME) {
            // ping pong frame just need to continue, no need echo anything,
            // because framework has reply ping/pong msg to client
            // automatically.
            continue;
          }
          else {
            // error frame
            break;
          }

          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            break;
          }
        }
      });

  server.async_start();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  coro_http_client client{};
  client.set_ws_deflate(true);
  async_simple::coro::syncAwait(
      client.connect("ws://localhost:8090/ws_extesion"));

  std::string send_str("test");

  async_simple::coro::syncAwait(client.write_websocket(send_str));
  auto data = async_simple::coro::syncAwait(client.read_websocket());
  CHECK(data.resp_body == "test");

  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  server.stop();
  client.close();
}
#endif
