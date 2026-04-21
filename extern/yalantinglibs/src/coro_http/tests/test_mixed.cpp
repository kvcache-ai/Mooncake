#include "doctest.h"
#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_http/coro_http_server.hpp"

using namespace coro_http;

bool create_file2(std::string_view filename, size_t file_size = 1024) {
  std::ofstream out(filename.data(), std::ios::binary);
  if (!out.is_open()) {
    return false;
  }

  std::string str;
  for (int i = 0; i < file_size; ++i) {
    str.push_back(rand() % 26 + 'A');
  }
  out.write(str.data(), str.size());
  return true;
}

/*
loop ok/failed with outbuf/no outbuf
*/
TEST_CASE("test mixed async_request") {
  coro_http_server server(2, 9900);
  server.set_http_handler<GET, POST>("/", [](request& req, response& resp) {
    resp.set_status_and_content(status_type::ok, "ok");
  });
  server.set_http_handler<GET, POST>("/test", [](request& req, response& resp) {
    resp.set_status_and_content(status_type::ok, "test");
  });
  server.set_http_handler<POST, PUT>(
      "/upload", [](request& req, response& resp) {
        CHECK(req.get_body().size() == 20);
        resp.set_status_and_content(status_type::ok, "upload ok");
      });
  server.set_http_handler<cinatra::GET, cinatra::PUT>(
      "/chunked",
      [](coro_http_request& req,
         coro_http_response& resp) -> async_simple::coro::Lazy<void> {
        assert(req.get_content_type() == content_type::chunked);
        chunked_result result{};
        std::string content;

        while (true) {
          result = co_await req.get_conn()->read_chunked();
          if (result.ec) {
            co_return;
          }
          if (result.eof) {
            break;
          }

          content.append(result.data);
        }

        std::cout << "content size: " << content.size() << "\n";
        resp.set_format_type(format_type::chunked);
        resp.set_status_and_content(status_type::ok, "chunked ok");
      });

  server.async_start();
  std::string uri = "http://127.0.0.1:9900/";
  std::string test_uri = "http://127.0.0.1:9900/test";
  std::string failed_uri = "http://127.0.0.1:9900/failed";

  // loop ok-failed
  auto lazy = [&]() -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    req_context<> ctx{};
    for (size_t i = 0; i < 6; i++) {
      auto result = co_await client.async_request(uri, http_method::GET, ctx);
      CHECK(result.status == 200);
      result = co_await client.async_request(failed_uri, http_method::GET, ctx);
      CHECK(result.status != 200);
    }
  };
  async_simple::coro::syncAwait(lazy());

  // loop ok-failed with outbuf
  auto lazy1 = [&]() -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    req_context<> ctx{};
    std::unordered_map<std::string, std::string> headers;
    for (size_t i = 0; i < 10; i++) {
      std::vector<char> buffer(10, 'a');
      auto result = co_await client.async_request(uri, http_method::GET, ctx,
                                                  headers, {buffer});
      CHECK(result.status == 200);
      std::string_view body(buffer.data(), result.resp_body.size());
      CHECK(body == "ok");

      if (i % 2 == 0) {
        result = co_await client.async_request(failed_uri, http_method::GET,
                                               ctx, headers, {buffer});
      }
      else {
        result = co_await client.async_request(failed_uri, http_method::GET,
                                               ctx, headers);
      }

      CHECK(result.status != 200);

      result = co_await client.async_request(test_uri, http_method::GET, ctx,
                                             headers, {buffer});
      std::string_view body1(buffer.data(), result.resp_body.size());
      CHECK(body1 == "test");
      CHECK(result.status == 200);
    }
  };
  async_simple::coro::syncAwait(lazy1());

  std::string filename = "mixed_file.txt";
  create_file2(filename, 20);
  std::string upload_uri = "http://127.0.0.1:9900/upload";
  std::unordered_map<std::string, std::string> headers;

  // loop request and sendfile ok-failed
  auto lazy2 = [&]() -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    req_context<> ctx{};
    for (size_t i = 0; i < 6; i++) {
      auto result = co_await client.async_request(uri, http_method::GET, ctx);
      CHECK(result.status == 200);
      result =
          co_await client.async_upload(failed_uri, http_method::PUT, filename);
      CHECK(result.status != 200);
      result =
          co_await client.async_upload(upload_uri, http_method::PUT, filename);
      CHECK(result.status == 200);
    }
  };
  async_simple::coro::syncAwait(lazy2());

  // loop request and sendfile ok-failed with outbuf
  auto lazy3 = [&]() -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    req_context<> ctx{};
    for (size_t i = 0; i < 6; i++) {
      std::vector<char> buffer(10, 'a');
      auto result = co_await client.async_request(uri, http_method::GET, ctx);
      CHECK(result.status == 200);

      if (i % 2 == 0) {
        result = co_await client.async_request(uri, http_method::GET, ctx,
                                               headers, {buffer});
      }
      else {
        result =
            co_await client.async_request(uri, http_method::GET, ctx, headers);
      }
      CHECK(result.status == 200);

      if (i % 2 == 0) {
        result = co_await client.async_request(failed_uri, http_method::GET,
                                               ctx, headers, {buffer});
        CHECK(result.status != 200);
      }
      else {
        result =
            co_await client.async_request(uri, http_method::GET, ctx, headers);
        CHECK(result.status == 200);
      }

      result =
          co_await client.async_upload(failed_uri, http_method::PUT, filename);
      CHECK(result.status != 200);
      result =
          co_await client.async_upload(upload_uri, http_method::PUT, filename);
      CHECK(result.status == 200);
    }
  };
  async_simple::coro::syncAwait(lazy3());

  std::string chunked_uri = "http://127.0.0.1:9900/chunked";
  auto lazy4 = [&]() -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    req_context<> ctx{};
    for (size_t i = 0; i < 6; i++) {
      std::vector<char> buffer(10, 'a');
      auto result = co_await client.async_request(uri, http_method::GET, ctx);
      CHECK(result.status == 200);

      if (i % 2 == 0) {
        result = co_await client.async_request(uri, http_method::GET, ctx,
                                               headers, {buffer});
      }
      else {
        result =
            co_await client.async_request(uri, http_method::GET, ctx, headers);
      }
      CHECK(result.status == 200);

      if (i % 2 == 0) {
        result = co_await client.async_request(failed_uri, http_method::GET,
                                               ctx, headers, {buffer});
        CHECK(result.status != 200);
      }
      else {
        result =
            co_await client.async_request(uri, http_method::GET, ctx, headers);
        CHECK(result.status == 200);
      }

      result = co_await client.async_upload_chunked(failed_uri,
                                                    http_method::PUT, filename);
      CHECK(result.status != 200);
      client.reset();
      result =
          co_await client.async_request(uri, http_method::GET, ctx, headers);
      CHECK(result.status == 200);
      result = co_await client.async_upload_chunked(chunked_uri,
                                                    http_method::PUT, filename);
      CHECK(result.status == 200);
    }
  };
  async_simple::coro::syncAwait(lazy4());

  std::filesystem::remove(filename);
}

std::atomic<int> g_failed_count = 0;

TEST_CASE("test parallel http request") {
  size_t thd_num = 100;
  coro_http_server server(thd_num, 9002);
  std::atomic<int> total = 0;
  server.set_http_handler<GET>("/", [&](request& req, response& resp) {
    if (total.fetch_add(1) % 10000 == 0) {
      resp.set_delay(true);
      return;
    }
    resp.set_status_and_content(status_type::ok, "hello");
  });
  server.async_start();

  coro_io::io_context_pool pool(thd_num);
  std::thread thrd{[&] {
    pool.run();
  }};

  size_t client_count = 200;
  std::vector<std::unique_ptr<coro_http_client>> clients;
  clients.resize(client_count);
  std::atomic<int> work_count = 0;

  std::promise<void> p;
  for (auto& cli : clients) {
    cli = std::make_unique<coro_http_client>();
    cli->set_conn_timeout(std::chrono::seconds{1});
    cli->set_req_timeout(std::chrono::seconds{1});

    cli->connect("http://127.0.0.1:9002/")
        .via(pool.get_executor())
        .start([&](auto&&) {
          [](coro_http_client& cli) -> async_simple::coro::Lazy<void> {
            for (int i = 0; i < 500; ++i) {
              if (!cli.has_closed()) {
                auto result = co_await cli.async_get("/");
                if (result.status == 200) {
                  CHECK(result.resp_body == "hello");
                }
                else {
                  g_failed_count++;
                  CINATRA_LOG_INFO << result.status << ", "
                                   << result.net_err.value() << ", "
                                   << result.net_err.message();
                }
              }
            }
          }(*cli)
                                           .start([&](auto&&) {
                                             auto i = ++work_count;
                                             if (i == client_count) {
                                               p.set_value();
                                             }
                                           });
        });
  }

  p.get_future().wait();
  pool.stop();
  thrd.join();
  CINATRA_LOG_INFO << "failed request: " << g_failed_count;
  CHECK(g_failed_count >= 10);
}
