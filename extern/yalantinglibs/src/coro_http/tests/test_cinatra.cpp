#include <async_simple/coro/Collect.h>

#include <charconv>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <ios>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "cinatra/error.hpp"
#include "doctest.h"
#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_http/coro_http_server.hpp"

using namespace std::chrono_literals;

using namespace coro_http;

std::string_view get_header_value(auto &resp_headers, std::string_view key) {
  for (const auto &[k, v] : resp_headers) {
    if (k == key)
      return v;
  }
  return {};
}

#ifdef CINATRA_ENABLE_GZIP
TEST_CASE("test for gzip") {
  coro_http_server server(1, 8090);
  server.set_http_handler<GET, POST>(
      "/gzip", [](coro_http_request &req, coro_http_response &res) {
        CHECK(req.get_header_value("Content-Encoding") == "gzip");
        CHECK(req.get_encoding_type() == content_encoding::gzip);
        res.set_status_and_content(status_type::ok, "hello world",
                                   content_encoding::gzip);
      });
  server.set_http_handler<GET, POST>(
      "/deflate", [](coro_http_request &req, coro_http_response &res) {
        CHECK(req.get_header_value("Content-Encoding") == "deflate");
        CHECK(req.get_encoding_type() == content_encoding::deflate);
        res.set_status_and_content(status_type::ok, "hello world",
                                   content_encoding::deflate);
      });
  server.set_http_handler<GET, POST>(
      "/none", [](coro_http_request &req, coro_http_response &res) {
        CHECK(req.get_header_value("Content-Encoding") == "none");
        CHECK(req.get_encoding_type() == content_encoding::none);
        res.set_status_and_content(status_type::ok, "hello world",
                                   content_encoding::none);
      });
  server.async_start();

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:8090/gzip";
    client.add_header("Content-Encoding", "gzip");
    auto result = async_simple::coro::syncAwait(client.async_get(uri));
    // auto content = get_header_value(result.resp_headers, "Content-Encoding");
    CHECK(get_header_value(result.resp_headers, "Content-Encoding") == "gzip");
    CHECK(result.resp_body == "hello world");
  }

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:8090/deflate";
    client.add_header("Content-Encoding", "deflate");
    auto result = async_simple::coro::syncAwait(client.async_get(uri));
    // auto content = get_header_value(result.resp_headers, "Content-Encoding");
    CHECK(get_header_value(result.resp_headers, "Content-Encoding") ==
          "deflate");
    CHECK(result.resp_body == "hello world");
  }

  {
    coro_http_client client{};
    client.add_header("Content-Encoding", "none");
    client.set_conn_timeout(0ms);
    std::string uri = "http://127.0.0.1:8090/none";
    auto result = async_simple::coro::syncAwait(
        client.connect(uri).via(&client.get_executor()));
    if (result.net_err)
      CHECK(result.net_err == coro_http::http_errc::connect_timeout);

    client.set_conn_timeout(-1ms);
    client.set_req_timeout(0ms);
    result = async_simple::coro::syncAwait(
        client.connect(uri).via(&client.get_executor()));
    if (result.net_err)
      CHECK(!result.net_err);

    // TODO will remove via later for 0ms timeout
    result = async_simple::coro::syncAwait(
        client.async_get("/none").via(&client.get_executor()));
    if (result.net_err)
      CHECK(result.net_err == coro_http::http_errc::request_timeout);

    client.add_header("Content-Encoding", "none");
    client.set_req_timeout(-1ms);
    result = async_simple::coro::syncAwait(client.async_get(uri));
    CHECK(!result.net_err);
    client.add_header("Content-Encoding", "none");
    result = async_simple::coro::syncAwait(client.async_get(uri));
    CHECK(!result.net_err);

    client.add_header("Content-Encoding", "none");
    coro_http_client::config conf{};
    conf.req_timeout_duration = 0ms;
    client.init_config(conf);
    result = async_simple::coro::syncAwait(
        client.async_get(uri).via(&client.get_executor()));
    if (result.net_err)
      CHECK(result.net_err == coro_http::http_errc::request_timeout);
  }

  {
    coro_http_client client{};
    client.set_conn_timeout(100ms);
    auto start = std::chrono::steady_clock::now();
    auto result =
        async_simple::coro::syncAwait(client.async_get("192.168.31.96:7080"));
    auto end = std::chrono::steady_clock::now();
    auto dur =
        std::chrono::duration_cast<std::chrono::seconds>(end - start).count();
    CHECK(result.net_err);
    assert(dur < 5);
  }

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:8090/none";
    client.add_header("Content-Encoding", "none");
    auto result = async_simple::coro::syncAwait(client.async_get(uri));
    CHECK(get_header_value(result.resp_headers, "Content-Encoding").empty());
    CHECK(result.resp_body == "hello world");
  }
  server.stop();
}

TEST_CASE("test encoding type") {
  coro_http_server server(1, 9001);

  server.set_http_handler<GET, POST>("/get", [](coro_http_request &req,
                                                coro_http_response &resp) {
    auto encoding_type = req.get_encoding_type();

    if (encoding_type ==
        content_encoding::gzip) {  // only post request have this field
      std::string decode_str;
      gzip_codec::uncompress(req.get_body(), decode_str);
      CHECK(decode_str == "Hello World");
    }
    resp.set_status_and_content(status_type::ok, "ok", content_encoding::gzip,
                                req.get_accept_encoding());
    CHECK(resp.content() != "ok");
  });

  server.set_http_handler<GET>(
      "/coro",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok, "ok",
                                    content_encoding::deflate,
                                    req.get_accept_encoding());
        CHECK(resp.content() != "ok");
        co_return;
      });

  server.set_http_handler<GET>(
      "/only_gzip",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok, "ok",
                                    content_encoding::gzip,
                                    req.get_accept_encoding());
        // client4 accept-encoding not allow gzip, response content no
        // compression
        CHECK(resp.content() == "ok");
        co_return;
      });
  std::string_view content = "ok";
  server.set_http_handler<GET>(
      "/only_deflate_view",
      [content](coro_http_request &req,
                coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content_view(status_type::ok, content,
                                         content_encoding::deflate, true, "ok");
        co_return;
      });
  server.set_http_handler<GET>(
      "/only_deflate",
      [content](coro_http_request &req,
                coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok, "ok",
                                    content_encoding::deflate,
                                    req.get_accept_encoding());
        // client4 accept-encoding not allow gzip, response content no
        // compression
        CHECK(resp.content() == "ok");
        co_return;
      });

  server.async_start();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  coro_http_client client1{};
  client1.add_header("Accept-Encoding", "gzip, deflate");
  auto result = async_simple::coro::syncAwait(
      client1.async_get("http://127.0.0.1:9001/get"));
  CHECK(result.resp_body == "ok");

  coro_http_client client2{};
  client2.add_header("Accept-Encoding", "gzip, deflate");
  result = async_simple::coro::syncAwait(
      client2.async_get("http://127.0.0.1:9001/coro"));
  CHECK(result.resp_body == "ok");

  coro_http_client client3{};
  std::unordered_map<std::string, std::string> headers = {
      {"Content-Encoding", "gzip"},
  };
  std::string ziped_str;
  std::string_view data = "Hello World";
  gzip_codec::compress(data, ziped_str);
  result = async_simple::coro::syncAwait(client3.async_post(
      "http://127.0.0.1:9001/get", ziped_str, req_content_type::none, headers));
  CHECK(result.resp_body == "ok");

  coro_http_client client4{};
  client4.add_header("Accept-Encoding", "deflate");
  result = async_simple::coro::syncAwait(
      client4.async_get("http://127.0.0.1:9001/only_gzip"));
  CHECK(result.resp_body == "ok");

  coro_http_client client5{};
  result = async_simple::coro::syncAwait(
      client5.async_get("http://127.0.0.1:9001/only_deflate_view"));
  CHECK(result.resp_body == "ok");
  client5.add_header("Accept-Encoding", "gzip");
  result = async_simple::coro::syncAwait(
      client5.async_get("http://127.0.0.1:9001/only_deflate"));
  CHECK(result.resp_body == "ok");

  server.stop();
}
#endif

#ifdef CINATRA_ENABLE_BROTLI
TEST_CASE("test brotli type") {
  coro_http_server server(1, 9001);

  server.set_http_handler<GET, POST>(
      "/get", [](coro_http_request &req, coro_http_response &resp) {
        auto encoding_type = req.get_encoding_type();

        if (encoding_type == content_encoding::br) {
          std::string decode_str;
          bool r = br_codec::brotli_decompress(req.get_body(), decode_str);
          CHECK(decode_str == "Hello World");
        }
        resp.set_status_and_content(status_type::ok, "ok", content_encoding::br,
                                    req.get_accept_encoding());
      });

  server.async_start();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  coro_http_client client{};
  std::unordered_map<std::string, std::string> headers = {
      {"Content-Encoding", "br"},
  };
  std::string ziped_str;
  std::string_view data = "Hello World";
  bool r = br_codec::brotli_compress(data, ziped_str);

  auto result = async_simple::coro::syncAwait(client.async_post(
      "http://127.0.0.1:9001/get", ziped_str, req_content_type::none, headers));
  CHECK(result.resp_body == "ok");
  server.stop();
}
#endif

#ifdef CINATRA_ENABLE_SSL
TEST_CASE("test ssl client") {
  {
    coro_http_client client4{};
    client4.set_ssl_schema(true);
    auto result = client4.get("www.baidu.com");
    assert(result.status == 200);

    auto lazy = []() -> async_simple::coro::Lazy<void> {
      coro_http_client client5{};
      client5.set_ssl_schema(true);
      co_await client5.connect("www.baidu.com");
      auto result = co_await client5.async_get("/");
      assert(result.status == 200);
    };
    async_simple::coro::syncAwait(lazy());
  }
  {
    coro_http_client client{};
    auto result = client.get("https://www.bing.com");
    CHECK(result.status >= 200);
  }
  {
    coro_http_client client{};
    auto ret = client.get("https://baidu.com");
    client.reset();
    ret = client.get("http://cn.bing.com");
    CINATRA_LOG_DEBUG << ret.status;
    client.reset();
    ret = client.get("https://baidu.com");
    CINATRA_LOG_DEBUG << ret.status;
  }
  {
    coro_http_client client{};
    auto r =
        async_simple::coro::syncAwait(client.connect("https://www.baidu.com"));
    if (r.status == 200) {
      auto result = client.get("/");
      CHECK(result.status >= 200);
    }
  }

  {
    coro_http_client client{};
    auto result = client.get("http://www.bing.com");
    CHECK(result.status >= 200);
  }

  {
    coro_http_client client{};
    client.set_ssl_schema(true);
    auto result = client.get("www.bing.com");
    CHECK(result.status >= 200);
  }

  {
    coro_http_client client{};
    client.set_ssl_schema(false);
    auto result = client.get("https://www.bing.com");
    CHECK(result.status >= 200);
  }

  {
    coro_http_client client{};
    client.enable_auto_redirect(true);
    bool ok = client.init_ssl();
    client.reset();
    REQUIRE_MESSAGE(ok == true, "init ssl fail, please check ssl config");
    auto result = client.get("https://www.bing.com");
    CHECK(result.status >= 200);
  }

  {
    coro_http_client client{};
    client.set_req_timeout(8s);
    client.enable_auto_redirect(true);
    std::string uri = "http://www.bing.com";
    // Make sure the host and port are matching with your proxy server
    client.set_proxy("106.14.255.124", "80");
    resp_data result = async_simple::coro::syncAwait(client.async_get(uri));
    if (!result.net_err)
      CHECK(result.status >= 200);
  }

  {
    coro_http_client client{};
    bool ok = client.init_ssl();
    REQUIRE_MESSAGE(ok == true, "init ssl fail, please check ssl config");
    auto result = client.get("https://www.bing.com");
    CHECK(result.status >= 200);
  }
}

TEST_CASE("test ssl client") {
  coro_http_client client{};
  bool ok = client.init_ssl();
  REQUIRE_MESSAGE(ok == true, "init ssl fail, please check ssl config");
  // client.set_sni_hostname("https://www.bing.com");
  auto result = client.get("https://www.bing.com");
  CHECK(result.status >= 200);
}
#endif

TEST_CASE("test invalid http body size") {
  coro_http_server server(1, 9001);
  server.set_max_http_body_size(10);
  server.set_http_handler<GET, POST>(
      "/get", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok,
                                    "ok, it is a long test string!");
      });

  server.async_start();

  std::string uri = "http://127.0.0.1:9001/get";
  {
    coro_http_client client{};
    auto result =
        client.post(uri, "it is a long test string!", req_content_type::text);
    CHECK(result.status != 200);
  }

  {
    coro_http_client client{};
    client.set_max_http_body_size(10);
    auto result = client.post(uri, "test", req_content_type::text);
    CHECK(result.status != 200);
    CHECK(result.net_err == std::errc::invalid_argument);
  }
  {
    coro_http_client client{};
    auto result = client.post(uri, "test", req_content_type::text);
    CHECK(result.status == 200);
  }
}

TEST_CASE("test default handler, loweset priority") {
  coro_http_server server(2, 8443);
  server.set_http_handler<HEAD, POST, GET>(
      "/check/:id", [](request &req, response &resp) {
        resp.set_status_and_content(status_type::ok, "check ok");
      });
  server.set_http_handler<HEAD, POST, GET>(
      "/test", [](request &req, response &resp) {
        resp.set_status_and_content(status_type::ok, "test ok");
      });
  server.set_default_handler(
      [](request &req, response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok, "default");
        co_return;
      });
  server.async_start();

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:8443/check/12");
  CHECK(result.resp_body == "check ok");

  result = client.get("http://127.0.0.1:8443/test");
  CHECK(result.resp_body == "test ok");

  result = client.get("http://127.0.0.1:8443/dummy");
  CHECK(result.resp_body == "default");
}

bool create_file(std::string_view filename, size_t file_size = 1024) {
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

TEST_CASE("test cinatra::string with SSO") {
  std::string s = "HelloHi";
  auto oldlen = s.length();
  s.reserve(10);
  memset(s.data() + oldlen + 1, 'A', 3);
  cinatra::detail::resize(s, 10);
  CHECK(s[10] == '\0');
  memcpy(s.data() + oldlen, "233", 3);
  CHECK(strlen(s.data()) == 10);
  CHECK(s == "HelloHi233");
}

TEST_CASE("test parse query") {
  {
    http_parser parser{};
    parser.parse_query("=");
    parser.parse_query("&a");
    parser.parse_query("&b=");
    parser.parse_query("&c=&d");
    parser.parse_query("&e=&f=1");
    parser.parse_query("&g=1&h=1");
    auto map = parser.queries();
    CHECK(map["a"].empty());
    CHECK(map["b"].empty());
    CHECK(map["c"].empty());
    CHECK(map["d"].empty());
    CHECK(map["e"].empty());
    CHECK(map["f"] == "1");
    CHECK(map["g"] == "1");
    CHECK(map["h"] == "1");
  }
  {
    http_parser parser{};
    parser.parse_query("test");
    parser.parse_query("test1=");
    parser.parse_query("test2=&");
    parser.parse_query("test3&");
    parser.parse_query("test4&a");
    parser.parse_query("test5&b=2");
    parser.parse_query("test6=1&c=2");
    parser.parse_query("test7=1&d");
    parser.parse_query("test8=1&e=");
    parser.parse_query("test9=1&f");
    parser.parse_query("test10=1&g=10&h&i=3&j");
    auto map = parser.queries();
    CHECK(map["test"].empty());
    CHECK(map.size() == 21);
  }
}

TEST_CASE("test cinatra::string without SSO") {
  std::string s(1000, 'A');
  std::string s2(5000, 'B');
  std::string sum = s + s2;
  auto oldlen = s.length();
  s.reserve(6000);
  memset(s.data() + oldlen + 1, 'A', 5000);
  cinatra::detail::resize(s, 6000);
  CHECK(s[6000] == '\0');
  memcpy(s.data() + oldlen, s2.data(), s2.length());
  CHECK(strlen(s.data()) == 6000);
  CHECK(s == sum);
}

TEST_CASE("test cinatra::string SSO to no SSO") {
  std::string s(10, 'A');
  std::string s2(5000, 'B');
  std::string sum = s + s2;
  auto oldlen = s.length();
  s.reserve(5010);
  memset(s.data() + oldlen + 1, 'A', 5000);
  cinatra::detail::resize(s, 5010);
  CHECK(s[5010] == '\0');
  memcpy(s.data() + oldlen, s2.data(), s2.length());
  CHECK(strlen(s.data()) == 5010);
  CHECK(s == sum);
}

TEST_CASE("test config") {
  coro_http_client client{};
  coro_http_client::config conf{};
  conf.sec_key = "s//GYHa/XO7Hd2F2eOGfyA==";
  conf.proxy_host = "http://example.com";
  conf.proxy_host = "9090";
  conf.max_single_part_size = 1024 * 1024;
  conf.proxy_auth_username = "cinatra";
  conf.proxy_auth_token = "cinatra";
  conf.proxy_auth_passwd = "cinatra";
  conf.enable_tcp_no_delay = true;
  client.init_config(conf);

  std::unordered_map<std::string, std::string> req_headers{{"test", "ok"}};
  client.set_headers(req_headers);
  const auto &headers = client.get_headers();
  CHECK(req_headers == headers);

  auto &executor = client.get_executor();
  auto name = executor.name();
  CHECK(!name.empty());

  const auto &c = client.get_config();
  CHECK(c.enable_tcp_no_delay == conf.enable_tcp_no_delay);
  CHECK(c.max_single_part_size == 1024 * 1024);

  auto ret = async_simple::coro::syncAwait(client.connect("http://##test.com"));
  CHECK(ret.status != 200);
  CHECK(ret.net_err.value() == (int)std::errc::protocol_error);
}

#ifndef CINATRA_ENABLE_SSL
TEST_CASE("test request https without init_ssl") {
  coro_http_client client{};
  auto ret = client.get("https://baidu.com");
  CHECK(ret.status != 200);

  ret = async_simple::coro::syncAwait(client.connect("https://baidu.com"));
  CHECK(ret.status != 200);
}
#endif

TEST_CASE("test reconnect with different headers") {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>("/", [](request &req, response &resp) {
    auto value = req.get_header_value("hello");
    CHECK(value == "test");
    resp.set_status_and_content(status_type::ok, "ok");
  });

  server.set_http_handler<GET>("/test", [](request &req, response &resp) {
    auto value = req.get_header_value("hello");
    CHECK(value.empty());
    value = req.get_header_value("test");
    CHECK(value == "ok");
    resp.set_status_and_content(status_type::ok, "ok");
  });

  server.set_http_handler<GET>("/test1", [](request &req, response &resp) {
    auto value = req.get_header_value("hello");
    CHECK(value.empty());
    value = req.get_header_value("test");
    CHECK(value.empty());
    resp.set_status_and_content(status_type::ok, "ok");
  });
  server.async_start();

  coro_http_client client{};
  client.add_header("hello", "test");

  auto ret = client.get("http://127.0.0.1:9001/");
  CHECK(ret.status == 200);

  client.add_header("test", "ok");
  ret = client.get("http://127.0.0.1:9001/test");
  CHECK(ret.status == 200);

  ret = client.get("http://127.0.0.1:9001/test1");
  CHECK(ret.status == 200);

  ret = client.get("http://127.0.0.1:9001/test", {{"test", "ok"}});
  CHECK(ret.status == 200);

  client.reset();
  ret = client.get("http://127.0.0.1:9001/", {{"hello", "test"}});
  CHECK(ret.status == 200);
}

TEST_CASE("test bad address") {
  {
    coro_http_server server(1, 9001, "127.0.0.1");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, 9001, "localhost");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, 9001, "0.0.0.0");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, 9001);
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, "0.0.0.0:9001");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, "127.0.0.1:9001");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, "localhost:9001");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }

  {
    coro_http_server server(1, 9001, "x.x.x.x");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(ec);
  }
  {
    coro_http_server server(1, "localhost:aaa");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(ec);
  }
}

async_simple::coro::Lazy<void> test_collect_all() {
  asio::io_context ioc;
  std::thread thd([&] {
    ioc.run();
  });
  std::vector<std::shared_ptr<coro_http_client>> v;
  std::vector<async_simple::coro::Lazy<resp_data>> futures;
  for (int i = 0; i < 2; ++i) {
    auto client = std::make_shared<coro_http_client>();
    client->set_conn_timeout(2s);
    client->set_req_timeout(2s);
    v.push_back(client);
    futures.push_back(client->async_get("http://www.baidu.com/"));
  }

  auto out = co_await async_simple::coro::collectAll(std::move(futures));

  for (auto &item : out) {
    auto result = item.value();
    CHECK(result.status >= 200);
  }
  thd.join();
}

struct add_data {
  bool before(coro_http_request &req, coro_http_response &res) {
    req.set_aspect_data("hello world");
    auto val = std::make_shared<int>(42);
    req.set_user_data(val);
    return true;
  }
};

struct add_more_data {
  bool before(coro_http_request &req, coro_http_response &res) {
    req.set_aspect_data(std::vector<std::string>{"test", "aspect"});
    auto user_data = req.get_user_data();
    CHECK(user_data.has_value());
    auto val = std::any_cast<std::shared_ptr<int>>(user_data);
    CHECK(*val == 42);
    auto data = req.get_user_data();
    val = std::any_cast<std::shared_ptr<int>>(data);
    *val = 43;
    return true;
  }
};

std::vector<std::string> g_aspect_test_vec;
std::mutex g_vec_mtx;

struct auth_t {
  bool before(coro_http_request &req, coro_http_response &res) { return true; }
  bool after(coro_http_request &req, coro_http_response &res) {
    std::lock_guard lock(g_vec_mtx);
    g_aspect_test_vec.push_back("enter auth_t after");
    return false;
  }
};

struct dely_t {
  bool before(coro_http_request &req, coro_http_response &res) {
    auto user_data = req.get_user_data();
    CHECK(!user_data.has_value());
    res.set_status_and_content(status_type::unauthorized, "unauthorized");
    return false;
  }
  bool after(coro_http_request &req, coro_http_response &res) {
    std::lock_guard lock(g_vec_mtx);
    g_aspect_test_vec.push_back("enter delay_t after");
    return true;
  }
};

struct another_t {
  bool after(coro_http_request &req, coro_http_response &res) {
    // won't comming
    return true;
  }
};

TEST_CASE("test aspect") {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/get",
      [](coro_http_request &req, coro_http_response &resp) {
        auto val = req.get_aspect_data();
        CHECK(val[0] == "hello world");
        resp.set_status_and_content(status_type::ok, "ok");
      },
      add_data{});
  server.set_http_handler<GET>(
      "/get_more",
      [](coro_http_request &req, coro_http_response &resp) {
        auto val = req.get_aspect_data();
        CHECK(val[0] == "test");
        CHECK(val[1] == "aspect");
        CHECK(!req.is_upgrade());
        auto user_data = req.get_user_data();
        CHECK(user_data.has_value());
        auto val1 = std::any_cast<std::shared_ptr<int>>(user_data);
        CHECK(*val1 == 43);
        resp.set_status_and_content(status_type::ok, "ok");
      },
      add_data{}, add_more_data{});
  server.set_http_handler<GET>(
      "/auth",
      [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "ok");
      },
      dely_t{}, auth_t{}, another_t{});
  server.set_http_handler<GET>(
      "/exception", [](coro_http_request &req, coro_http_response &resp) {
        throw std::invalid_argument("invalid argument");
      });
  server.set_http_handler<GET>(
      "/throw", [](coro_http_request &req, coro_http_response &resp) {
        throw 9;
      });
  server.set_http_handler<GET>(
      "/coro_exception",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        throw std::invalid_argument("invalid argument");
        co_return;
      });
  server.set_http_handler<GET>(
      "/coro_throw",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        throw 9;
        co_return;
      });

  server.async_start();

  coro_http_client client{};
  auto result = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:9001/get"));
  CHECK(result.status == 200);
  result = async_simple::coro::syncAwait(client.async_get("/get_more"));
  CHECK(result.status == 200);
  result = async_simple::coro::syncAwait(client.async_get("/auth"));
  CHECK(result.status == 401);

  {
    std::lock_guard lock(g_vec_mtx);
    CHECK(g_aspect_test_vec.size() == 2);
  }

  CHECK(result.resp_body == "unauthorized");
  result = async_simple::coro::syncAwait(client.async_get("/exception"));
  CHECK(result.status == 503);
  result = async_simple::coro::syncAwait(client.async_get("/throw"));
  CHECK(result.status == 503);
  result = async_simple::coro::syncAwait(client.async_get("/coro_exception"));
  CHECK(result.status == 503);
  result = async_simple::coro::syncAwait(client.async_get("/coro_throw"));
  CHECK(result.status == 503);
}

TEST_CASE("test client pool") {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/", [&](coro_http_request &req, coro_http_response &resp) {
        // resp.get_conn()->tcp_socket().shutdown(asio::socket_base::shutdown_receive);
        resp.set_status_and_content(status_type::ok, "hello world");
      });
  server.set_http_handler<GET>("/test", [&](coro_http_request &req,
                                            coro_http_response &resp) {
    resp.get_conn()->tcp_socket().shutdown(asio::socket_base::shutdown_receive);
    resp.set_status_and_content(status_type::ok, "shutdown");
  });
  server.async_start();

  std::string url = "http://127.0.0.1:9001/";

  auto pool = coro_io::client_pool<coro_http_client>::create(url, {100});

  std::atomic<size_t> count = 0;
  std::promise<void> promise;
  for (size_t i = 0; i < 100; i++) {
    pool->send_request(
            [&](coro_http_client &client) -> async_simple::coro::Lazy<void> {
              auto data = co_await client.async_get(url);
              CHECK(data.resp_body == "hello world");
            })
        .start([&](auto &&) {
          if (++count == 100) {
            promise.set_value();
          }
        });
  }
  promise.get_future().wait();

  CHECK(pool->free_client_count() > 0);

  count = 0;
  std::atomic<size_t> failed_count = 0;
  url = "http://127.0.0.1:9001/test";
  std::promise<void> promise1;
  for (size_t i = 0; i < 100; i++) {
    pool->send_request(
            [&](coro_http_client &client) -> async_simple::coro::Lazy<void> {
              auto data = co_await client.async_get(url);
              if (data.status != 200) {
                failed_count++;
                CHECK(data.net_err);
              }
              else {
                CHECK(data.resp_body == "shutdown");
              }
              CINATRA_LOG_INFO << "test result: " << data.status << ", "
                               << data.resp_body << ", " << data.net_err.value()
                               << ", " << data.net_err.message();
            })
        .start([&](auto &&) {
          if (++count == 100) {
            promise1.set_value();
          }
        });
  }

  promise1.get_future().wait();
  CINATRA_LOG_INFO << "failed request: " << failed_count << ", "
                   << pool->free_client_count();
  CHECK(failed_count >= 0);
  CHECK(pool->free_client_count() <= 100 - failed_count);
}

TEST_CASE("test response") {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/get", [](coro_http_request &req, coro_http_response &resp) {
        resp.get_conn()->set_multi_buf(false);
        resp.set_status_and_content(status_type::ok, "ok");
        CHECK(resp.content_size() == 2);
        CHECK(resp.need_date());
      });
  server.set_http_handler<GET>(
      "/get2", [](coro_http_request &req, coro_http_response &resp) {
        resp.get_conn()->set_multi_buf(false);
        resp.set_status(status_type::ok);
      });
  std::array<http_header, 1> span{{"hello", "span"}};
  server.set_http_handler<GET>(
      "/get1", [&](coro_http_request &req, coro_http_response &resp) {
        resp.get_conn()->set_multi_buf(false);
        resp.need_date_head(false);
        CHECK(!resp.need_date());
        resp.set_keepalive(true);
        resp.add_header_span({span.data(), span.size()});

        resp.set_status_and_content(status_type::ok, "ok");
      });
  std::string sv = "hello view";
  server.set_http_handler<GET>(
      "/view", [&](coro_http_request &req, coro_http_response &resp) {
        resp.get_conn()->set_multi_buf(false);
        resp.need_date_head(false);
        resp.set_content_type<2>();
        CHECK(!resp.need_date());
        resp.add_header_span({span.data(), span.size()});

        resp.set_status_and_content_view(
            status_type::ok, std::string_view(sv.data(), sv.size()));
      });
  server.set_http_handler<GET>(
      "/empty", [&](coro_http_request &req, coro_http_response &resp) {
        resp.get_conn()->set_multi_buf(false);
        resp.need_date_head(false);
        resp.set_content_type<2>();
        CHECK(!resp.need_date());
        resp.add_header_span({span.data(), span.size()});

        resp.set_status_and_content_view(status_type::ok, "");
      });
  server.set_http_handler<GET>(
      "/empty1", [&](coro_http_request &req, coro_http_response &resp) {
        resp.set_content_type<2>();
        CHECK(resp.need_date());
        resp.add_header_span({span.data(), span.size()});

        resp.set_status_and_content_view(status_type::ok, "");
      });
  server.set_http_handler<GET>(
      "/empty2", [&](coro_http_request &req, coro_http_response &resp) {
        resp.set_content_type<2>();
        CHECK(resp.need_date());
        resp.add_header_span({span.data(), span.size()});

        resp.set_status_and_content(status_type::ok, "");
      });
  server.async_start();
  coro_http_client client{};
  auto result = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:9001/get"));
  CHECK(result.status == 200);
  result = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:9001/get1"));
  CHECK(result.status == 200);
  CHECK(get_header_value(result.resp_headers, "hello") == "span");
  result = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:9001/get2"));
  CHECK(result.status == 200);
  CHECK(result.resp_body == "200 OK");
  result = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:9001/view"));
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello view");
  result = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:9001/empty"));
  CHECK(result.status == 200);
  CHECK(result.resp_body.empty());
  result = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:9001/empty1"));
  CHECK(result.status == 200);
  CHECK(result.resp_body.empty());
  result = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:9001/empty2"));
  CHECK(result.status == 200);
  CHECK(result.resp_body.empty());
}

#ifdef INJECT_FOR_HTTP_CLIENT_TEST
TEST_CASE("test pipeline") {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET, POST>(
      "/test", [](coro_http_request &req, coro_http_response &res) {
        if (req.get_content_type() == content_type::multipart) {
          return;
        }
        res.set_status_and_content(status_type::ok, "hello world");
      });
  server.set_http_handler<GET, POST>(
      "/coro",
      [](coro_http_request &req,
         coro_http_response &res) -> async_simple::coro::Lazy<void> {
        res.set_status_and_content(status_type::ok, "hello coro");
        co_return;
      });
  server.set_http_handler<GET, POST>(
      "/test_available", [](coro_http_request &req, coro_http_response &res) {
        std::string str(1400, 'a');
        res.set_status_and_content(status_type::ok, std::move(str));
      });
  server.async_start();

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:9001";
    async_simple::coro::syncAwait(client.connect(uri));
    auto ec = async_simple::coro::syncAwait(client.async_write_raw(
        "GET /test HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\n"));
    CHECK(!ec);

    auto result =
        async_simple::coro::syncAwait(client.async_read_raw(http_method::GET));
    CHECK(!result.resp_body.empty());
    ec = async_simple::coro::syncAwait(client.async_write_raw(
        "GET /test HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\nGET /test "
        "HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\n"));
    CHECK(!ec);
    result = async_simple::coro::syncAwait(
        client.async_read_raw(http_method::GET, true));
    CHECK(!result.resp_body.empty());
    auto data = result.resp_body;
    http_parser parser{};
    int r = parser.parse_response(data.data(), data.size(), 0);
    if (r) {
      std::string_view body(data.data() + r, parser.body_len());
      CHECK(body == "hello world");
      CHECK(data.size() > parser.total_len());
    }
  }

  {
    http_parser p1{};
    std::string str = "GET /coro1 HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\n";
    int ret = p1.parse_request(str.data(), str.size(), 0);

    coro_http_client client{};
    std::string uri = "http://127.0.0.1:9001";
    async_simple::coro::syncAwait(client.connect(uri));
    auto ec = async_simple::coro::syncAwait(client.async_write_raw(
        "GET /coro HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\nGET /test "
        "HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\nGET /coro1 HTTP/1.1\r\nHost: "
        "127.0.0.1:8090\r\n\r\nGET /coro HTTP/1.1\r\nHost: "
        "127.0.0.1:8090\r\n\r\n"));
    CHECK(!ec);
    auto result = async_simple::coro::syncAwait(
        client.async_read_raw(http_method::GET, true));
    http_parser parser{};
    int r = parser.parse_response(result.resp_body.data(),
                                  result.resp_body.size(), 0);
    CHECK(parser.status() == 200);
  }

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:9001";
    async_simple::coro::syncAwait(client.connect(uri));
    auto ec = async_simple::coro::syncAwait(client.async_write_raw(
        "GET /test HTTP/1.1\r\nHost: 127.0.0.1:8090\r\nContent-Type: "
        "multipart/form-data\r\n\r\nGET /test HTTP/1.1\r\nHost: "
        "127.0.0.1:8090\r\nContent-Type: multipart/form-data\r\n\r\n"));
    CHECK(!ec);

    auto result =
        async_simple::coro::syncAwait(client.async_read_raw(http_method::GET));
    http_parser parser{};
    int r = parser.parse_response(result.resp_body.data(),
                                  result.resp_body.size(), 0);
    CHECK(parser.status() != 200);
  }

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:9001";
    async_simple::coro::syncAwait(client.connect(uri));
    auto ec = async_simple::coro::syncAwait(client.async_write_raw(
        "POST /test HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\nGET /test "
        "HTTP/1.1\r\nHost: "
        "127.0.0.1:8090\r\n\r\n"));
    CHECK(!ec);

    auto result =
        async_simple::coro::syncAwait(client.async_read_raw(http_method::POST));
    http_parser parser{};
    int r = parser.parse_response(result.resp_body.data(),
                                  result.resp_body.size(), 0);
    CHECK(parser.status() != 200);
  }

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:9001";
    async_simple::coro::syncAwait(client.connect(uri));
    auto ec = async_simple::coro::syncAwait(client.async_write_raw(
        "GET HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\nGET /test "
        "HTTP/1.1\r\nHost: "
        "127.0.0.1:8090\r\n\r\n"));
    CHECK(!ec);

    auto result =
        async_simple::coro::syncAwait(client.async_read_raw(http_method::GET));
    http_parser parser{};
    int r = parser.parse_response(result.resp_body.data(),
                                  result.resp_body.size(), 0);
    CHECK(parser.status() != 200);
  }

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:9001";
    async_simple::coro::syncAwait(client.connect(uri));
    auto ec = async_simple::coro::syncAwait(
        client.async_write_raw("GET /test HTTP/1.1\r\nHost: "
                               "127.0.0.1:8090\r\n\r\nGET HTTP/1.1\r\nHost: "
                               "127.0.0.1:8090\r\n\r\n"));
    CHECK(!ec);

    auto result =
        async_simple::coro::syncAwait(client.async_read_raw(http_method::GET));
    http_parser parser{};
    int r = parser.parse_response(result.resp_body.data(),
                                  result.resp_body.size(), 0);
    CHECK(parser.status() != 200);
  }

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:9001";
    async_simple::coro::syncAwait(client.connect(uri));
    auto ec = async_simple::coro::syncAwait(client.async_write_raw(
        "GET /test_available HTTP/1.1\r\nHost: 127.0.0.1:8090\r\n\r\n"));
    CHECK(!ec);

    auto result =
        async_simple::coro::syncAwait(client.async_read_raw(http_method::GET));
    auto sz = client.available();
    CHECK(sz > 0);
  }
}
#endif

enum class upload_type { send_file, chunked, multipart };

TEST_CASE("test out buffer and async upload ") {
  coro_http_server server(1, 9000);
  server.set_http_handler<GET, POST>(
      "/write_chunked",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_format_type(format_type::chunked);
        bool ok;
        if (ok = co_await resp.get_conn()->begin_chunked(); !ok) {
          co_return;
        }

        std::vector<std::string> vec{"hello", " world", " ok"};

        for (auto &str : vec) {
          if (ok = co_await resp.get_conn()->write_chunked(str); !ok) {
            co_return;
          }
        }

        ok = co_await resp.get_conn()->end_chunked();
      });
  server.set_http_handler<GET, POST>(
      "/normal", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "test");
      });
  server.set_http_handler<GET, POST>(
      "/more", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "test more");
      });

  server.async_start();

  auto lazy = [](upload_type flag) -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:9000/normal";
    std::vector<char> oubuf;
    oubuf.resize(10);
    req_context<> ctx{};
    auto result = co_await client.async_request(uri, http_method::GET,
                                                std::move(ctx), {}, oubuf);
    CINATRA_LOG_DEBUG << oubuf.data() << "\n";

    std::string_view out_view(oubuf.data(), result.resp_body.size());
    assert(out_view == "test");
    assert(out_view == result.resp_body);

    auto ss = std::make_shared<std::stringstream>();
    *ss << "hello world";
    client.add_header("Connection", "close");
    if (flag == upload_type::send_file) {
      result = co_await client.async_upload("http://127.0.0.1:9000/more"sv,
                                            http_method::POST, ss);
    }
    else if (flag == upload_type::chunked) {
      result = co_await client.async_upload_chunked(
          "http://127.0.0.1:9000/more"sv, http_method::POST, ss);
    }
    else if (flag == upload_type::multipart) {
      client.add_str_part("test_key", "test_value");
      result =
          co_await client.async_upload_multipart("http://127.0.0.1:9000/more");
    }

    CINATRA_LOG_DEBUG << (int)flag << oubuf.data() << "\n";
    CINATRA_LOG_DEBUG << result.resp_body << "\n";

    std::string_view out_view1(oubuf.data(), out_view.size());
    assert(out_view == out_view1);
    assert(result.resp_body != out_view1);
  };

  async_simple::coro::syncAwait(lazy(upload_type::send_file));
  async_simple::coro::syncAwait(lazy(upload_type::chunked));
  async_simple::coro::syncAwait(lazy(upload_type::multipart));
}

TEST_CASE("test multipart and chunked return error") {
  coro_http_server server(1, 8090);
  server.set_http_handler<cinatra::PUT, cinatra::POST>(
      "/multipart",
      [](request &req, response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::bad_request,
                                    "invalid headers");
        co_return;
      });
  server.set_http_handler<cinatra::PUT, cinatra::POST>(
      "/chunked",
      [](request &req, response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::bad_request,
                                    "invalid headers");
        co_return;
      });
  server.async_start();

  std::string filename = "small_test_file.txt";
  create_file(filename, 10);
  {
    coro_http_client client{};
    std::string uri1 = "http://127.0.0.1:8090/chunked";
    auto result = async_simple::coro::syncAwait(
        client.async_upload_chunked(uri1, http_method::PUT, filename));
    CHECK(result.status != 200);
    if (!result.resp_body.empty())
      CHECK(result.resp_body == "invalid headers");
  }

  {
    coro_http_client client{};
    std::string uri2 = "http://127.0.0.1:8090/multipart";
    client.add_str_part("test", "test value");
    auto result =
        async_simple::coro::syncAwait(client.async_upload_multipart(uri2));
    CHECK(result.status != 200);
    if (!result.resp_body.empty())
      CHECK(result.resp_body == "invalid headers");
  }

  {
    coro_http_client client{};
    std::string uri1 = "http://127.0.0.1:8090/no_such";
    auto result = async_simple::coro::syncAwait(
        client.async_upload_chunked(uri1, http_method::PUT, filename));
    CHECK(result.status != 200);
  }
  std::error_code ec;
  fs::remove(filename, ec);
}

async_simple::coro::Lazy<void> send_data(auto &ch, size_t count) {
  for (int i = 0; i < count; i++) {
    co_await coro_io::async_send(ch, i);
  }
}

async_simple::coro::Lazy<void> recieve_data(auto &ch, auto &vec, size_t count) {
  while (true) {
    if (vec.size() == count) {
      CINATRA_LOG_DEBUG << std::this_thread::get_id() << "\n";
      break;
    }

    auto [ec, i] = co_await coro_io::async_receive(ch);
    vec.push_back(i);
  }
}

TEST_CASE("test coro channel with multi thread") {
  size_t count = 10000;
  auto ch = coro_io::create_channel<int>(count);
  send_data(ch, count).via(ch.get_executor()).start([](auto &&) {
  });

  std::vector<int> vec;
  std::vector<std::thread> group;
  for (int i = 0; i < 10; i++) {
    group.emplace_back(std::thread([&]() {
      async_simple::coro::syncAwait(
          recieve_data(ch, vec, count).via(ch.get_executor()));
    }));
  }
  for (auto &thd : group) {
    thd.join();
  }

  for (int i = 0; i < count; i++) {
    CHECK(vec.at(i) == i);
  }
}

TEST_CASE("test coro channel") {
  {
    auto ch = coro_io::create_channel<std::string>(100);
    auto ec = async_simple::coro::syncAwait(
        coro_io::async_send(ch, std::string("test")));
    CHECK(!ec);

    std::string val;
    std::error_code err;
    std::tie(err, val) =
        async_simple::coro::syncAwait(coro_io::async_receive(ch));
    CHECK(!err);
    CHECK(val == "test");
  }
  auto ch = coro_io::create_channel<int>(1000);
  auto ec = async_simple::coro::syncAwait(coro_io::async_send(ch, 41));
  CHECK(!ec);
  ec = async_simple::coro::syncAwait(coro_io::async_send(ch, 42));
  CHECK(!ec);

  std::error_code err;
  int val;
  std::tie(err, val) =
      async_simple::coro::syncAwait(coro_io::async_receive(ch));
  CHECK(!err);
  CHECK(val == 41);

  std::tie(err, val) =
      async_simple::coro::syncAwait(coro_io::async_receive(ch));
  CHECK(!err);
  CHECK(val == 42);
}

TEST_CASE("test select coro channel") {
  using namespace coro_io;

  auto ch = coro_io::create_channel<int>(1000);

  async_simple::coro::syncAwait(coro_io::async_send(ch, 41));
  async_simple::coro::syncAwait(coro_io::async_send(ch, 42));

  std::error_code ec;
  int val;
  std::tie(ec, val) = async_simple::coro::syncAwait(coro_io::async_receive(ch));
  CHECK(val == 41);

  std::tie(ec, val) = async_simple::coro::syncAwait(coro_io::async_receive(ch));
  CHECK(val == 42);
}

TEST_CASE("test bad address") {
  {
    coro_http_server server(1, 9001, "127.0.0.1");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, 9001, "localhost");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, 9001, "0.0.0.0");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, 9001);
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, "0.0.0.0:9001");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, "127.0.0.1:9001");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }
  {
    coro_http_server server(1, "localhost:9001");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(!ec);
  }

  {
    coro_http_server server(1, 9001, "x.x.x.x");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(ec);
  }
  {
    coro_http_server server(1, "localhost:aaa");
    server.async_start();
    auto ec = server.get_errc();
    CHECK(ec);
  }
}

TEST_CASE("test default http handler") {
  coro_http_server server(1, 9001);
  server.set_default_handler(
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok,
                                    "It is from default handler");
        co_return;
      });
  server.set_http_handler<POST>(
      "/view",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_delay(true);
        resp.set_status_and_content_view(status_type::ok,
                                         req.get_body());  // no copy
        co_await resp.get_conn()->reply();
      });
  server.async_start();

  for (int i = 0; i < 5; i++) {
    coro_http_client client{};
    async_simple::coro::syncAwait(client.connect("http://127.0.0.1:9001"));
    auto data = client.get("/test");
    CHECK(data.resp_body == "It is from default handler");
    data = client.get("/test_again");
    CHECK(data.resp_body == "It is from default handler");
    data = client.get("/any");
    CHECK(data.resp_body == "It is from default handler");
    data = async_simple::coro::syncAwait(
        client.async_post("/view", "post string", req_content_type::string));
    CHECK(data.status == 200);
    CHECK(data.resp_body == "post string");
  }
}

TEST_CASE("test request with out buffer") {
  coro_http_server server(1, 8090);
  server.set_http_handler<GET>(
      "/test", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok,
                                    "it is a test string, more than 10 bytes");
      });
  server.set_http_handler<GET>(
      "/test1", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_format_type(format_type::chunked);
        resp.set_status_and_content(status_type::ok,
                                    "it is a test string, more than 10 bytes");
      });
  server.async_start();

  std::string str;
  str.resize(10);
  std::string url = "http://127.0.0.1:8090/test";
  std::string url1 = "http://127.0.0.1:8090/test1";

  {
    coro_http_client client;
    client.add_header("Host", "cinatra");
    auto ret = client.async_request(url, http_method::GET, req_context<>{}, {},
                                    std::span<char>{str.data(), str.size()});
    auto result = async_simple::coro::syncAwait(ret);
    CINATRA_LOG_DEBUG << result.status << "\n";
    CINATRA_LOG_DEBUG << result.net_err.message() << "\n";
    CINATRA_LOG_DEBUG << result.resp_body << "\n";
    CHECK(result.status == 200);
    CHECK(!client.is_body_in_out_buf());
  }

  {
    coro_http_client client;
    auto ret = client.async_request(url1, http_method::GET, req_context<>{}, {},
                                    std::span<char>{str.data(), str.size()});
    auto result = async_simple::coro::syncAwait(ret);
    CINATRA_LOG_DEBUG << result.status << "\n";
    CINATRA_LOG_DEBUG << result.net_err.message() << "\n";
    CINATRA_LOG_DEBUG << result.resp_body << "\n";
    CHECK(result.status == 200);
    CHECK(!client.is_body_in_out_buf());
    auto s = client.release_buf();
    CHECK(s == "it is a test string, more than 10 bytes");
  }

  {
    detail::resize(str, 1024);
    coro_http_client client;
    auto ret = client.async_request(url, http_method::GET, req_context<>{}, {},
                                    std::span<char>{str.data(), str.size()});
    auto result = async_simple::coro::syncAwait(ret);
    bool ok = result.status == 200 || result.status == 301;
    CHECK(ok);
    std::string_view sv(str.data(), result.resp_body.size());
    CHECK(result.resp_body == sv);
    CHECK(client.is_body_in_out_buf());
  }

  {
    detail::resize(str, 1024 * 64);
    coro_http_client client;
    client.set_conn_timeout(3s);
    client.set_req_timeout(5s);
    std::string dest = "http://www.baidu.com";
    auto ret = client.async_request(dest, http_method::GET, req_context<>{}, {},
                                    std::span<char>{str.data(), str.size()});
    auto result = async_simple::coro::syncAwait(ret);
    bool ok = result.status == 200 || result.status == 301;
    if (ok && result.resp_body.size() <= 1024 * 64) {
      std::string_view sv(str.data(), result.resp_body.size());
      CHECK(client.is_body_in_out_buf());
    }
  }
}

TEST_CASE("test response body greater than 2GB") {
  try {
    coro_http_server server(1, 8090);
    std::string resp_str;
    size_t large_size = 1024 * 1024 * 1024 + 10;
    detail::resize(resp_str, large_size);
    server.set_http_handler<GET>(
        "/test", [&](coro_http_request &req, coro_http_response &resp) {
          resp.set_status_and_content(status_type::ok, std::move(resp_str));
        });
    server.async_start();
    coro_http_client client;
    auto result = client.get("http://127.0.0.1:8090/test");
    CHECK(result.resp_body.length() == large_size);
  } catch (const std::bad_alloc &e) {
    std::cout << e.what() << std::endl;
  }
}

TEST_CASE("test pass path not entire uri") {
  coro_http_client client{};
  client.set_conn_timeout(2s);
  auto r =
      async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
  CINATRA_LOG_DEBUG << r.resp_body.size() << "\n";
  auto buf = client.release_buf();
  CINATRA_LOG_DEBUG << strlen(buf.data()) << "\n";
  CINATRA_LOG_DEBUG << buf << "\n";
  CHECK(r.status >= 200);

  r = async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
  CHECK(r.status >= 200);

  r = async_simple::coro::syncAwait(client.async_get("/"));
  CHECK(r.status >= 200);
}

TEST_CASE("test coro_http_client connect/request timeout") {
  {
#if !defined(_MSC_VER)
    coro_http_client client{};
    cinatra::coro_http_client::config conf{.conn_timeout_duration = 1ms};
    client.init_config(conf);
    auto r =
        async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
    CINATRA_LOG_DEBUG << r.net_err.value() << ", " << r.net_err.message()
                      << "\n";
    if (r.status != 200)
      CHECK(r.net_err != std::errc{});
#endif
  }

  {
    coro_http_client client{};
    cinatra::coro_http_client::config conf{.conn_timeout_duration = 10s,
                                           .req_timeout_duration = 1ms};
    client.init_config(conf);
    auto r =
        async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
    CINATRA_LOG_DEBUG << r.net_err.message() << "\n";
    CHECK(r.net_err != std::errc{});
  }
}

TEST_CASE("test out io_contex server") {
  auto executor = coro_io::get_global_executor()->get_asio_executor();
  coro_http_server server(executor.context(), "0.0.0.0:8002");
  server.set_no_delay(true);
  server.set_http_handler<GET>("/", [](request &req, response &res) {
    res.set_status_and_content(status_type::ok, "hello");
  });
  server.async_start();

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:8002/");
  CHECK(result.status == 200);
  server.stop();
}

TEST_CASE("test coro_http_client async_http_connect") {
  coro_http_client client{};
  cinatra::coro_http_client::config conf{.req_timeout_duration = 3s};
  client.init_config(conf);
  auto r = async_simple::coro::syncAwait(
      client.async_http_connect("http://www.baidu.com"));
  CHECK(r.status >= 200);
  for (auto [k, v] : r.resp_headers) {
    CINATRA_LOG_DEBUG << k << ", " << v << "\n";
  }

  coro_http_client client1{};
  client1.set_conn_timeout(2s);
  r = async_simple::coro::syncAwait(
      client1.async_http_connect("http//www.badurl.com"));
  CHECK(r.status != 200);

  r = async_simple::coro::syncAwait(client1.connect("http://cn.bing.com"));
  CHECK(client1.get_host() == "cn.bing.com");
  CHECK(client1.get_port() == "80");
  CHECK(r.status >= 200);

  r = async_simple::coro::syncAwait(client1.connect("http://www.baidu.com"));

  CHECK(r.status >= 200);
  r = async_simple::coro::syncAwait(client1.connect("http://cn.bing.com"));
  CHECK(r.status >= 200);
}

TEST_CASE("test collect all") {
  async_simple::coro::syncAwait(test_collect_all());
}

TEST_CASE("test head put and some other request") {
  coro_http_server server(1, 8090);
  server.set_http_handler<HEAD>(
      "/headers", [](coro_http_request &req, coro_http_response &resp) {
        resp.add_header("Content-Type", "application/json");
        resp.add_header("Content-Length", "117");
        resp.set_status_and_content(status_type::ok, "");
      });
  server.set_http_handler<cinatra::http_method::PATCH,
                          cinatra::http_method::TRACE>(
      "/", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status(status_type::method_not_allowed);
      });
  server.set_http_handler<cinatra::http_method::OPTIONS>(
      "/", [](coro_http_request &req, coro_http_response &resp) {
        resp.add_header("Allow", "HEAD, OPTIONS, GET, POST, PUT");
        resp.set_status_and_content(status_type::ok, "");
      });
  server.set_http_handler<cinatra::http_method::PUT>(
      "/put/json", [](coro_http_request &req, coro_http_response &resp) {
        auto json_str = req.get_body();
        std::ofstream file("json.txt", std::ios::binary);
        file.write(json_str.data(), json_str.size());
        file.close();
        resp.set_status_and_content(status_type::ok, "");
      });
  server.set_http_handler<cinatra::http_method::PUT>(
      "/delete/:name", [](coro_http_request &req, coro_http_response &resp) {
        auto &filename = req.params_["name"];
        std::error_code ec;
        fs::remove(filename, ec);
        std::string result = ec ? "delete failed" : "ok";
        resp.set_status_and_content(status_type::ok, result);
      });
  server.set_http_handler<cinatra::http_method::DEL>(
      "/delete/:name", [](coro_http_request &req, coro_http_response &resp) {
        auto &filename = req.params_["name"];
        std::error_code ec;
        fs::remove(filename, ec);
        std::string result = ec ? "delete failed" : "delete ok";
        resp.set_status_and_content(status_type::ok, result);
      });
  std::function<void(coro_http_request & req, coro_http_response & resp)> func =
      nullptr;
  server.set_http_handler<cinatra::http_method::DEL>("/delete1/:name", func);
  std::function<async_simple::coro::Lazy<void>(coro_http_request & req,
                                               coro_http_response & resp)>
      func1 = nullptr;
  server.set_http_handler<cinatra::http_method::DEL>("/delete2/:name", func1);

  server.async_start();
  std::this_thread::sleep_for(300ms);

  coro_http_client client{};

  auto result = async_simple::coro::syncAwait(
      client.async_head("http://127.0.0.1:8090/headers"));
  CHECK(result.status == 200);

  result = async_simple::coro::syncAwait(
      client.async_patch("http://127.0.0.1:8090/"));
  CHECK(result.status == 405);

  result = async_simple::coro::syncAwait(
      client.async_trace("http://127.0.0.1:8090/"));
  CHECK(result.status == 405);

  result = async_simple::coro::syncAwait(
      client.async_options("http://127.0.0.1:8090/"));
  CHECK(result.status == 200);

  std::string json = R"({
  "Id": 12345,
  "Customer": "John Smith",
  "Quantity": 1,
  "Price": 10.00
  })";

  coro_http_client client1{};
  result = async_simple::coro::syncAwait(client1.async_put(
      "http://127.0.0.1:8090/put/json", json, req_content_type::json));
  CHECK(result.status == 200);

  result = async_simple::coro::syncAwait(client1.async_post(
      "http://127.0.0.1:8090/delete/json.txt", json, req_content_type::json));

  CHECK(result.status == 404);

  result = async_simple::coro::syncAwait(client1.async_delete(
      "http://127.0.0.1:8090/delete/json.txt", json, req_content_type::json));

  CHECK(result.status == 200);

  result = async_simple::coro::syncAwait(client1.async_delete(
      "http://127.0.0.1:8090/delete1/json.txt", json, req_content_type::json));
  CHECK(result.status == 404);

  result = async_simple::coro::syncAwait(client1.async_delete(
      "http://127.0.0.1:8090/delete2/json.txt", json, req_content_type::json));
  CHECK(result.status == 404);
}

TEST_CASE("test coro_http_client connect/request timeout") {
  {
#if !defined(_MSC_VER)
    coro_http_client client{};
    cinatra::coro_http_client::config conf{.conn_timeout_duration = 1ms};
    client.init_config(conf);
    auto r =
        async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
    std::cout << r.net_err.value() << ", " << r.net_err.message() << "\n";
    if (r.status != 200)
      CHECK(r.net_err != std::errc{});
#endif
  }

  {
    coro_http_client client{};
    cinatra::coro_http_client::config conf{.conn_timeout_duration = 10s,
                                           .req_timeout_duration = 1ms};
    client.init_config(conf);
    auto r =
        async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
    std::cout << r.net_err.message() << "\n";
    CHECK(r.net_err != std::errc{});
  }
}

TEST_CASE("test upload file") {
  coro_http_server server(1, 8090);
  server.set_http_handler<cinatra::PUT, cinatra::POST>(
      "/multipart",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        assert(req.get_content_type() == content_type::multipart);
        auto boundary = req.get_boundary();
        multipart_reader_t multipart(req.get_conn());
        while (true) {
          auto part_head = co_await multipart.read_part_head(boundary);
          if (part_head.ec) {
            co_return;
          }

          CINATRA_LOG_DEBUG << part_head.name << "\n";
          CINATRA_LOG_DEBUG << part_head.filename << "\n";

          std::shared_ptr<coro_io::coro_file> file;
          std::string filename;
          if (!part_head.filename.empty()) {
            file = std::make_shared<coro_io::coro_file>();
            filename = std::to_string(
                std::chrono::system_clock::now().time_since_epoch().count());

            size_t pos = part_head.filename.rfind('.');
            if (pos != std::string::npos) {
              auto extent = part_head.filename.substr(pos);
              filename += extent;
            }

            CINATRA_LOG_DEBUG << filename << "\n";
            file->open(filename, std::ios::trunc | std::ios::out);
            if (!file->is_open()) {
              resp.set_status_and_content(status_type::internal_server_error,
                                          "file open failed");
              co_return;
            }
          }

          auto part_body = co_await multipart.read_part_body(boundary);
          if (part_body.ec) {
            co_return;
          }

          if (!filename.empty()) {
            auto [ec, sz] = co_await file->async_write(part_body.data);
            if (ec) {
              co_return;
            }

            file->close();
            CHECK(fs::file_size(filename) == 2 * 1024 * 1024);
          }
          else {
            CINATRA_LOG_DEBUG << part_body.data << "\n";
          }

          if (part_body.eof) {
            break;
          }
        }

        resp.set_status_and_content(status_type::ok, "multipart finished");
        co_return;
      });

  server.async_start();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  coro_http_client client{};
  std::string uri = "http://127.0.0.1:8090/multipart";
  resp_data result =
      async_simple::coro::syncAwait(client.async_upload_multipart(uri));
  CHECK(result.status == 404);

  client.add_str_part("hello", "world");
  client.add_str_part("key", "value");
  CHECK(!client.add_file_part("key", "value"));
  result = async_simple::coro::syncAwait(client.async_upload_multipart(uri));
  CHECK(!client.is_redirect(result));
  CHECK(result.resp_body == "multipart finished");

  client.add_str_part("hello", "world");
  result = async_simple::coro::syncAwait(
      client.async_upload_multipart("http//badurl.com"));
  CHECK(result.status == 404);

  client.set_max_single_part_size(1024);
  std::string test_file_name = "test1.txt";
  std::ofstream test_file;
  test_file.open(test_file_name,
                 std::ios::binary | std::ios::out | std::ios::trunc);
  std::vector<char> test_file_data(2 * 1024 * 1024, '0');
  test_file.write(test_file_data.data(), test_file_data.size());
  test_file.close();
  result = async_simple::coro::syncAwait(
      client.async_upload_multipart(uri, "test", test_file_name));

  CHECK(result.resp_body == "multipart finished");

  std::filesystem::remove(std::filesystem::path(test_file_name));

  std::string not_exist_file = "notexist.txt";
  result = async_simple::coro::syncAwait(client.async_upload_multipart(
      uri, "test_not_exist_file", not_exist_file));
  CHECK(result.status == 404);

  result = async_simple::coro::syncAwait(client.async_upload_multipart(
      "http//badurl.com", "test_not_exist_file", not_exist_file));
  CHECK(result.status == 404);

  client.close();

  server.stop();
}

TEST_CASE("test bad uri") {
  coro_http_client client{};
  CHECK(client.add_header("hello", "cinatra"));
  CHECK(client.add_header("hello", "cinatra"));
  CHECK(!client.add_header("", "cinatra"));
  client.add_str_part("hello", "world");
  auto result = async_simple::coro::syncAwait(
      client.async_upload_multipart("http://www.badurlrandom.org"));
  CHECK(result.status == 404);
}

TEST_CASE("test multiple ranges download") {
  coro_http_client client{};
  client.set_conn_timeout(2s);
  client.set_req_timeout(5s);
  std::string uri = "http://uniquegoodshiningmelody.neverssl.com/favicon.ico";

  std::string filename = "test1.txt";
  std::error_code ec{};
  std::filesystem::remove(filename, ec);
  resp_data result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "1-16"));
  if (result.status == 206) {
    CHECK(std::filesystem::file_size(filename) == 16);
  }
}

TEST_CASE("test ranges download") {
  create_file("test_range.txt", 64);
  coro_http_server server(1, 8090);
  server.set_static_res_dir("", "./");
  server.set_static_res_dir("", "./www");
  server.async_start();

  coro_http_client client{};
  client.set_req_timeout(std::chrono::seconds(8));
  std::string uri = "http://127.0.0.1:8090/test_range.txt";

  std::string filename = "test1.txt";
  std::error_code ec{};
  std::filesystem::remove(filename, ec);
  resp_data result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "1-10"));
  CHECK(result.status == 206);
  CHECK(std::filesystem::file_size(filename) == 10);

  filename = "test2.txt";
  std::filesystem::remove(filename, ec);
  result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "10-15"));
  CHECK(result.status == 206);
  CHECK(std::filesystem::file_size(filename) == 6);
}

TEST_CASE("test ranges download with a bad filename and multiple ranges") {
  create_file("test_multiple_range.txt", 64);
  coro_http_server server(1, 8090);
  server.set_static_res_dir("", "");
  server.async_start();

  coro_http_client client{};
  std::string uri = "http://127.0.0.1:8090/test_multiple_range.txt";

  std::string filename = "";
  std::error_code ec{};
  std::filesystem::remove(filename, ec);
  resp_data result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "1-10,11-16"));
  CHECK(result.status == 404);
  CHECK(result.net_err ==
        std::make_error_code(std::errc::no_such_file_or_directory));

  client.add_header("Range", "bytes=1-10,20-30");
  result = client.get(uri);
  CHECK(result.status == 206);
  CHECK(result.resp_body.size() == 21);

  filename = "test_ranges.txt";
  client.add_header("Range", "bytes=0-10,21-30");
  result = client.download(uri, filename);
  CHECK(result.status == 206);
  CHECK(fs::file_size(filename) == 21);
}

#ifdef INJECT_FOR_HTTP_SEVER_TEST
TEST_CASE("test inject") {
  {
    create_file("test_inject_range.txt", 64);
    coro_http_server server(1, 8090);
    server.set_static_res_dir("", "");
    server.set_write_failed_forever(true);
    server.async_start();

    {
      coro_http_client client{};
      std::string uri = "http://127.0.0.1:8090/test_inject_range.txt";
      std::string filename = "test_inject.txt";
      resp_data result = async_simple::coro::syncAwait(
          client.async_download(uri, filename, "1-10,11-16"));
      CHECK(result.status == 404);
    }

    {
      coro_http_client client{};
      std::string uri = "http://127.0.0.1:8090/test_inject_range.txt";
      std::string filename = "test_inject.txt";
      resp_data result = async_simple::coro::syncAwait(
          client.async_download(uri, filename, "0-60"));
      CHECK(result.status == 404);
    }
  }

  {
    create_file("test_inject_range.txt", 64);
    coro_http_server server(1, 8090);
    server.set_file_resp_format_type(file_resp_format_type::chunked);
    server.set_write_failed_forever(true);
    server.set_static_res_dir("", "");
    server.async_start();

    {
      coro_http_client client{};
      std::string uri = "http://127.0.0.1:8090/test_inject_range.txt";
      std::string filename = "test_inject.txt";
      resp_data result =
          async_simple::coro::syncAwait(client.async_download(uri, filename));
      CHECK(result.status == 404);
    }
  }

  {
    coro_http_server server(1, 8090);
    server.set_write_failed_forever(true);
    server.set_http_handler<GET>("/", [](request &req, response &resp) {
      resp.set_status_and_content(status_type::ok, "ok");
    });
    server.async_start();

    {
      coro_http_client client{};
      std::string uri = "http://127.0.0.1:8090/";
      resp_data result = client.get(uri);
      CHECK(result.status == 404);
    }
  }

  {
    coro_http_server server(1, 8090);
    server.set_read_failed_forever(true);
    server.set_http_handler<GET, POST>("/", [](request &req, response &resp) {
      resp.set_status_and_content(status_type::ok, "ok");
    });
    server.async_start();

    {
      coro_http_client client{};
      std::string uri = "http://127.0.0.1:8090/";
      std::string content(1024 * 2, 'a');
      resp_data result = client.post(uri, content, req_content_type::text);
      CHECK(result.status == 404);
    }
  }
}
#endif

TEST_CASE("test coro_http_client quit") {
  std::promise<bool> promise;
  [&] {
    { coro_http_client client{}; }
    promise.set_value(true);
  }();

  CHECK(promise.get_future().get());
}

TEST_CASE("test coro_http_client multipart upload") {
  coro_http_server server(1, 8090);
  server.set_http_handler<cinatra::PUT, cinatra::POST>(
      "/multipart_upload",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        assert(req.get_content_type() == content_type::multipart);
        auto boundary = req.get_boundary();
        multipart_reader_t multipart(req.get_conn());
        while (true) {
          auto part_head = co_await multipart.read_part_head(boundary);
          if (part_head.ec) {
            co_return;
          }

          CINATRA_LOG_DEBUG << part_head.name << "\n";
          CINATRA_LOG_DEBUG << part_head.filename << "\n";

          std::shared_ptr<coro_io::coro_file> file;
          std::string filename;
          if (!part_head.filename.empty()) {
            file = std::make_shared<coro_io::coro_file>();
            filename = std::to_string(
                std::chrono::system_clock::now().time_since_epoch().count());

            size_t pos = part_head.filename.rfind('.');
            if (pos != std::string::npos) {
              auto extent = part_head.filename.substr(pos);
              filename += extent;
            }

            CINATRA_LOG_DEBUG << filename << "\n";
            file->open(filename, std::ios::trunc | std::ios::out);
            if (!file->is_open()) {
              resp.set_status_and_content(status_type::internal_server_error,
                                          "file open failed");
              co_return;
            }
          }

          auto part_body = co_await multipart.read_part_body(boundary);
          if (part_body.ec) {
            co_return;
          }

          if (!filename.empty()) {
            auto [ec, sz] = co_await file->async_write(part_body.data);
            if (ec) {
              co_return;
            }

            file->close();
            CHECK(fs::file_size(filename) == 1024);
          }
          else {
            CINATRA_LOG_DEBUG << part_body.data << "\n";
          }

          if (part_body.eof) {
            break;
          }
        }

        resp.set_status_and_content(status_type::ok, "ok");
        co_return;
      });

  server.async_start();

  std::string filename = "test_1024.txt";
  create_file(filename);

  coro_http_client client{};
  std::string uri = "http://127.0.0.1:8090/multipart_upload";
  client.add_str_part("test", "test value");
  client.add_file_part("test file", filename);
  auto result =
      async_simple::coro::syncAwait(client.async_upload_multipart(uri));
  CHECK(result.status == 200);
}

#ifdef CINATRA_ENABLE_SSL
TEST_CASE("test ssl upload") {
  coro_http_server server(1, 8091);
  server.init_ssl("../openssl_files/server.crt", "../openssl_files/server.key",
                  "test");
  server.set_http_handler<cinatra::PUT>(
      "/upload",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        std::string_view filename = req.get_header_value("filename");
        uint64_t sz;
        auto oldpath = fs::current_path().append(filename);
        std::string newpath = fs::current_path()
                                  .append("server_" + std::string{filename})
                                  .string();
        std::ofstream file(newpath, std::ios::binary);
        CHECK(file.is_open());
        file.write(req.get_body().data(), req.get_body().size());
        file.flush();
        file.close();

        size_t offset = 0;
        std::string offset_s = std::string{req.get_header_value("offset")};
        if (!offset_s.empty()) {
          offset = stoull(offset_s);
        }

        std::string filesize = std::string{req.get_header_value("filesize")};

        if (!filesize.empty()) {
          sz = stoull(filesize);
        }
        else {
          sz = std::filesystem::file_size(oldpath);
          sz -= offset;
        }

        CHECK(!filename.empty());
        CHECK(sz == std::filesystem::file_size(newpath));
        std::ifstream ifs(oldpath);
        ifs.seekg(offset, std::ios::cur);
        std::string str;
        str.resize(sz);
        ifs.read(str.data(), sz);
        CHECK(str == req.get_body());
        resp.set_status_and_content(status_type::ok, std::string(filename));
        co_return;
      });
  server.async_start();

  std::string filename = "test_ssl_upload.txt";
  create_file(filename, 10);
  std::string uri = "https://127.0.0.1:8091/upload";

  {
    coro_http_client client{};
    bool r = client.init_ssl();
    CHECK(r);
    r = client.init_ssl();
    CHECK(r);
    client.add_header("filename", filename);
    auto lazy = client.async_upload(uri, http_method::PUT, filename);
    auto result = async_simple::coro::syncAwait(lazy);
    CHECK(result.status == 200);
  }

  {
    coro_http_client client{};
    client.add_header("filename", filename);
    auto lazy = client.async_upload(uri, http_method::PUT, filename);
    auto result = async_simple::coro::syncAwait(lazy);
    CHECK(result.status == 200);
  }

  cinatra::coro_http_server server1(1, 9002);
  server1.init_ssl("../openssl_files/server.crt", "../openssl_files/server.key",
                   "test");
  server1.set_http_handler<cinatra::GET, cinatra::PUT>(
      "/chunked",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
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

        CINATRA_LOG_DEBUG << "content size: " << content.size() << "\n";
        CINATRA_LOG_DEBUG << content << "\n";
        resp.set_format_type(format_type::chunked);
        resp.set_status_and_content(status_type::ok, "chunked ok");
      });
  server1.async_start();

  uri = "https://127.0.0.1:9002/chunked";
  {
    coro_http_client client{};
    bool r = client.init_ssl();
    CHECK(r);
    std::string_view file = "test_ssl_upload.txt";
    client.add_header("filename", filename);
    auto lazy = client.async_upload_chunked(uri, http_method::PUT, file);
    auto result = async_simple::coro::syncAwait(lazy);
    CHECK(result.status == 200);
  }

  {
    coro_http_client client{};
    client.enable_sni_hostname(true);
    bool r = client.init_ssl();
    CHECK(r);
    std::unordered_map<std::string, std::string> headers;
    headers.emplace("filename", filename);
    auto lazy = client.async_upload_chunked(uri, http_method::PUT, filename,
                                            req_content_type::none, headers);
    auto result = async_simple::coro::syncAwait(lazy);
    CHECK(result.status == 200);
  }

  {
    coro_http_client client{};
    client.write_failed_forever_ = true;
    bool r = client.init_ssl();
    CHECK(r);
    client.add_header("filename", filename);
    auto lazy = client.async_upload_chunked(uri, http_method::PUT, filename);
    auto result = async_simple::coro::syncAwait(lazy);
    CHECK(result.status != 200);
  }
}
#endif

TEST_CASE("test coro_http_client upload") {
  auto test_upload_by_file_path = [](std::string filename,
                                     std::size_t offset = 0,
                                     std::size_t r_size = SIZE_MAX,
                                     bool should_failed = false) {
    coro_http_client client{};
    client.add_header("filename", filename);
    client.add_header("offset", std::to_string(offset));
    if (r_size != SIZE_MAX)
      client.add_header("filesize", std::to_string(r_size));
    std::string uri = "http://127.0.0.1:8090/upload";
    cinatra::resp_data result;
    if (r_size != SIZE_MAX) {
      auto lazy =
          client.async_upload(uri, http_method::PUT, filename, offset, r_size);
      result = async_simple::coro::syncAwait(lazy);
    }
    else {
      auto lazy = client.async_upload(uri, http_method::PUT, filename, offset);
      result = async_simple::coro::syncAwait(lazy);
    }
    CHECK(((result.status == 200) ^ should_failed));
  };
  auto test_upload_by_stream = [](std::string filename, std::size_t offset = 0,
                                  std::size_t r_size = SIZE_MAX,
                                  bool should_failed = false) {
    coro_http_client client{};
    client.add_header("filename", filename);
    client.add_header("offset", std::to_string(offset));
    if (r_size != SIZE_MAX)
      client.add_header("filesize", std::to_string(r_size));
    std::string uri = "http://127.0.0.1:8090/upload";
    std::ifstream ifs(filename, std::ios::binary);
    cinatra::resp_data result;
    if (r_size != SIZE_MAX) {
      auto lazy =
          client.async_upload(uri, http_method::PUT, filename, offset, r_size);
      result = async_simple::coro::syncAwait(lazy);
    }
    else {
      auto lazy = client.async_upload(uri, http_method::PUT, filename, offset);
      result = async_simple::coro::syncAwait(lazy);
    }
    CHECK(((result.status == 200) ^ should_failed));
  };
  auto test_upload_by_coro = [](std::string filename,
                                std::size_t r_size = SIZE_MAX,
                                bool should_failed = false) {
    coro_http_client client{};
    client.add_header("filename", filename);
    client.add_header("offset", "0");
    if (r_size != SIZE_MAX)
      client.add_header("filesize", std::to_string(r_size));
    std::string uri = "http://127.0.0.1:8090/upload";
    coro_io::coro_file file;
    file.open(filename, std::ios::in);
    CHECK(file.is_open());
    std::string buf;
    buf.resize(1'000'000);
    auto async_read =
        [&file, &buf]() -> async_simple::coro::Lazy<cinatra::read_result> {
      auto [ec, size] = co_await file.async_read(buf.data(), buf.size());
      co_return read_result{{buf.data(), size}, file.eof(), ec};
    };
    cinatra::resp_data result;
    if (r_size == SIZE_MAX) {
      auto lazy = client.async_upload(uri, http_method::PUT, async_read);
      result = async_simple::coro::syncAwait(lazy);
      CHECK(result.status != 200);
    }
    else {
      auto lazy =
          client.async_upload(uri, http_method::PUT, async_read, 0, r_size);
      result = async_simple::coro::syncAwait(lazy);
      CHECK(((result.status == 200) ^ should_failed));
    }
  };
  coro_http_server server(1, 8090);
  server.set_http_handler<cinatra::PUT>(
      "/upload",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        std::string_view filename = req.get_header_value("filename");
        uint64_t sz;
        auto oldpath = fs::current_path().append(filename);
        std::string newpath = fs::current_path()
                                  .append("server_" + std::string{filename})
                                  .string();
        std::ofstream file(newpath, std::ios::binary);
        CHECK(file.is_open());
        file.write(req.get_body().data(), req.get_body().size());
        file.flush();
        file.close();

        size_t offset = 0;
        std::string offset_s = std::string{req.get_header_value("offset")};
        if (!offset_s.empty()) {
          offset = stoull(offset_s);
        }

        std::string filesize = std::string{req.get_header_value("filesize")};

        if (!filesize.empty()) {
          sz = stoull(filesize);
        }
        else {
          sz = std::filesystem::file_size(oldpath);
          sz -= offset;
        }

        CHECK(!filename.empty());
        CHECK(sz == std::filesystem::file_size(newpath));
        std::ifstream ifs(oldpath);
        ifs.seekg(offset, std::ios::cur);
        std::string str;
        str.resize(sz);
        ifs.read(str.data(), sz);
        CHECK(str == req.get_body());
        resp.set_status_and_content(status_type::ok, std::string(filename));
        co_return;
      });
  server.async_start();
  std::string filename = "test_upload.txt";
  // upload without size
  {
    auto sizes = {1024 * 1024, 2'000'000, 1024, 100, 0};
    for (auto size : sizes) {
      std::error_code ec{};
      fs::remove(filename, ec);
      if (ec) {
        CINATRA_LOG_DEBUG << ec << "\n";
      }
      bool r = create_file(filename, size);
      CHECK(r);
      test_upload_by_file_path(filename);
      test_upload_by_stream(filename);
      test_upload_by_coro(filename);
    }
  }
  // upload with size
  {
    auto sizes = {std::pair{1024 * 1024, 1'000'000},
                  std::pair{2'000'000, 1'999'999}, std::pair{200, 1},
                  std::pair{100, 0}, std::pair{0, 0}};
    for (auto [size, r_size] : sizes) {
      std::error_code ec{};
      fs::remove(filename, ec);
      if (ec) {
        CINATRA_LOG_DEBUG << ec << "\n";
      }
      bool r = create_file(filename, size);
      CHECK(r);
      test_upload_by_file_path(filename, 0, r_size);
      test_upload_by_stream(filename, 0, r_size);
      test_upload_by_coro(filename, r_size);
    }
  }
  // upload with too large size
  {
    auto sizes = {std::pair{1024 * 1024, 1024 * 1024 + 2},
                  std::pair{2'000'000, 2'000'001}, std::pair{200, 502},
                  std::pair{0, 1}};
    for (auto [size, r_size] : sizes) {
      std::error_code ec{};
      fs::remove(filename, ec);
      if (ec) {
        CINATRA_LOG_DEBUG << ec << "\n";
      }
      bool r = create_file(filename, size);
      CHECK(r);
      test_upload_by_file_path(filename, 0, r_size, true);
      test_upload_by_stream(filename, 0, r_size, true);
      test_upload_by_coro(filename, r_size, true);
    }
  }
  // upload with offset
  {
    auto sizes = {std::pair{1024 * 1024, 1'000'000},
                  std::pair{2'000'000, 1'999'999}, std::pair{200, 1},
                  std::pair{100, 0}, std::pair{0, 0}};
    for (auto [size, offset] : sizes) {
      std::error_code ec{};
      fs::remove(filename, ec);
      if (ec) {
        CINATRA_LOG_DEBUG << ec << "\n";
      }
      bool r = create_file(filename, size);
      CHECK(r);
      test_upload_by_file_path(filename, offset);
      test_upload_by_stream(filename, offset);
    }
  }
  // upload with size & offset
  {
    auto sizes = {std::tuple{1024 * 1024, 500'000, 500'000},
                  std::tuple{2'000'000, 1'999'999, 1}, std::tuple{200, 1, 199},
                  std::tuple{100, 100, 0}};
    for (auto [size, offset, r_size] : sizes) {
      std::error_code ec{};
      fs::remove(filename, ec);
      if (ec) {
        CINATRA_LOG_DEBUG << ec << "\n";
      }
      bool r = create_file(filename, size);
      CHECK(r);
      test_upload_by_file_path(filename, offset, r_size);
      test_upload_by_stream(filename, offset, r_size);
    }
  }
  // upload with too large size & offset
  {
    auto sizes = {std::tuple{1024 * 1024, 1'000'000, 50'000},
                  std::tuple{2'000'000, 1'999'999, 2}, std::tuple{200, 1, 200},
                  std::tuple{100, 100, 1}};
    for (auto [size, offset, r_size] : sizes) {
      std::error_code ec{};
      fs::remove(filename, ec);
      if (ec) {
        CINATRA_LOG_DEBUG << ec << "\n";
      }
      bool r = create_file(filename, size);
      CHECK(r);
      test_upload_by_file_path(filename, offset, r_size, true);
      test_upload_by_stream(filename, offset, r_size, true);
    }
  }
  {
    filename = "some_test_file.txt";
    bool r = create_file(filename, 10);
    CHECK(r);
    test_upload_by_file_path(filename, 20, SIZE_MAX, true);
    std::error_code ec{};
    fs::remove(filename, ec);
  }
}

TEST_CASE("test coro_http_client chunked upload and download") {
  {
    coro_http_server server(1, 8090);
    server.set_http_handler<cinatra::PUT, cinatra::POST>(
        "/chunked_upload",
        [](coro_http_request &req,
           coro_http_response &resp) -> async_simple::coro::Lazy<void> {
          assert(req.get_content_type() == content_type::chunked);
          chunked_result result{};
          std::string_view filename = req.get_header_value("filename");

          CHECK(!filename.empty());

          auto oldpath = fs::current_path().append(filename);
          std::string newpath = fs::current_path()
                                    .append("server_" + std::string{filename})
                                    .string();
          std::ofstream file(newpath, std::ios::binary);
          CHECK(file.is_open());

          while (true) {
            result = co_await req.get_conn()->read_chunked();
            if (result.ec) {
              co_return;
            }

            file.write(result.data.data(), result.data.size());

            if (result.eof) {
              break;
            }
          }
          file.flush();
          file.close();
          auto sz = std::filesystem::file_size(oldpath);
          CHECK(sz == std::filesystem::file_size(newpath));
          resp.set_status_and_content(status_type::ok, std::string(filename));
        });
    server.set_http_handler<cinatra::PUT>(
        "/upload_stream", [](coro_http_request &req, coro_http_response &resp) {
          std::cout << "body size: " << req.get_body().size() << "\n";
          resp.set_status_and_content(status_type::ok, "upload ok");
        });
    server.async_start();
    {
      coro_http_client client{};
      create_file("stream_file.txt", 20);
      auto stream_file =
          std::make_shared<std::ifstream>("stream_file.txt", std::ios::binary);
      std::string uri = "http://127.0.0.1:8090/upload_stream";
      std::unordered_map<std::string, std::string> headers;
      headers.emplace("filename", "stream_file.txt");
      auto result = async_simple::coro::syncAwait(
          client.async_upload(uri, http_method::PUT, stream_file, 0, -1,
                              req_content_type::text, std::move(headers)));
      CHECK(result.status == 200);
      stream_file =
          std::make_shared<std::ifstream>("stream_file.txt", std::ios::binary);
      result = async_simple::coro::syncAwait(
          client.async_upload(uri, http_method::PUT, stream_file, 0, 100));
      CHECK(result.status != 200);

      result = async_simple::coro::syncAwait(client.async_upload(
          uri, http_method::PUT, "stream_file.txt"sv, 0, 0));
      CHECK(result.status == 200);

      std::error_code ignore_ec;
      fs::remove("stream_file.txt", ignore_ec);
    }
    {
      coro_http_client client{};
      std::string uri = "http://###127.0.0.1:8090/chunked_upload";
      std::string filename = "test_chunked_upload.txt";
      auto lazy = client.async_upload(uri, http_method::PUT, filename);
      auto result = async_simple::coro::syncAwait(lazy);
      CHECK(result.status != 200);

      uri = "http://127.0.0.1:8090/chunked_upload";
      filename = "no_such.txt";
      auto lazy1 = client.async_upload(uri, http_method::PUT, filename);
      result = async_simple::coro::syncAwait(lazy1);
      CHECK(result.status != 200);

      std::shared_ptr<std::ifstream> file = nullptr;
      uri = "http://127.0.0.1:8090/chunked_upload";
      auto lazy2 = client.async_upload(uri, http_method::PUT, file);
      result = async_simple::coro::syncAwait(lazy2);
      CHECK(result.status != 200);

      auto code = async_simple::coro::syncAwait(client.handle_shake());
      CHECK(code);
    }
    {
      coro_http_client client{};
      std::string uri = "http://###127.0.0.1:8090/chunked_upload";
      std::string filename = "test_chunked_upload.txt";
      auto lazy = client.async_upload_chunked(uri, http_method::PUT, filename);
      auto result = async_simple::coro::syncAwait(lazy);
      CHECK(result.status != 200);

      uri = "http://127.0.0.1:8090/chunked_upload";
      filename = "no_such.txt";
      auto lazy1 = client.async_upload_chunked(uri, http_method::PUT, filename);
      result = async_simple::coro::syncAwait(lazy1);
      CHECK(result.status != 200);

      std::shared_ptr<std::ifstream> file = nullptr;
      uri = "http://127.0.0.1:8090/chunked_upload";
      auto lazy2 = client.async_upload_chunked(uri, http_method::PUT, file);
      result = async_simple::coro::syncAwait(lazy2);
      CHECK(result.status != 200);
      create_file("chunked_file.txt", 20);
      std::unordered_map<std::string, std::string> headers;
      headers.emplace("filename", "chunked_file.txt");
      result = async_simple::coro::syncAwait(client.async_upload_chunked(
          uri, http_method::PUT, "chunked_file.txt"sv, req_content_type::text,
          std::move(headers)));
      CHECK(result.status == 200);

      client.reset();
      client.set_conn_timeout(0ms);
      result = async_simple::coro::syncAwait(client.async_upload_chunked(
          uri, http_method::PUT, "chunked_file.txt"sv));
      CHECK(result.status != 200);
      CHECK(result.net_err == http_errc::connect_timeout);

      client.set_conn_timeout(500ms);
      client.set_req_timeout(0ms);
      client.add_header("filename", "chunked_file.txt");
      result = async_simple::coro::syncAwait(client.async_upload_chunked(
          uri, http_method::PUT, "chunked_file.txt"sv));
      CHECK(result.status != 200);
      CHECK(result.net_err == http_errc::request_timeout);

      std::error_code ignore_ec;
      fs::remove("chunked_file.txt", ignore_ec);
    }
    {
      std::string uri = "http://127.0.0.1:8090/upload_stream";
      std::string filename = "test_chunked_upload.txt";
      coro_http_client client{};
      client.set_conn_timeout(0ms);
      auto lazy3 = client.async_upload(uri, http_method::PUT, filename);
      auto result = async_simple::coro::syncAwait(lazy3);
      CHECK(result.status != 200);
      client.reset();
      client.set_conn_timeout(500ms);
      client.set_req_timeout(0ms);
      auto lazy4 = client.async_upload(uri, http_method::PUT, filename);
      result = async_simple::coro::syncAwait(lazy4);
      CHECK(result.status != 200);
    }
    auto sizes = {1024 * 1024, 2'000'000, 1024, 100, 0};
    for ([[maybe_unused]] auto size : sizes) {
      std::string filename = "test_chunked_upload.txt";
      std::error_code ec{};
      fs::remove(filename, ec);
      if (ec) {
        CINATRA_LOG_DEBUG << ec << "\n";
      }
      bool r = create_file(filename, 1024 * 1024 * 8);
      CHECK(r);
      coro_http_client client{};
      client.add_header("filename", filename);
      std::string uri = "http://127.0.0.1:8090/chunked_upload";
      auto lazy = client.async_upload_chunked(uri, http_method::PUT, filename);
      auto result = async_simple::coro::syncAwait(lazy);
      CHECK(result.status == 200);
    }
  }

  {
    // chunked download, not in cache
    create_file("test_102.txt", 102);
    create_file("test_static.txt", 1024);
    coro_http_server server(1, 8090);
    server.set_static_res_dir("download", "");
    server.set_max_size_of_cache_files(100);
    server.set_transfer_chunked_size(100);
    server.async_start();

    coro_http_client client{};

    std::string download_url = "http://127.0.0.1:8090/download/test_static.txt";
    std::string download_name = "test1.txt";
    auto r = client.download(download_url, download_name);
    CHECK(r.status == 200);
    CHECK(std::filesystem::file_size(download_name) == 1024);

    download_url = "http://127.0.0.1:8090/download/test_102.txt";
    download_name = "test2.txt";
    r = client.download(download_url, download_name);
    CHECK(r.status == 200);
    CHECK(std::filesystem::file_size(download_name) == 102);
  }
}

TEST_CASE("test coro_http_client get") {
  coro_http_client client{};
  client.set_conn_timeout(1s);
  auto r = client.get("http://www.baidu.com");
  if (!r.net_err)
    CHECK(r.status < 400);
}

TEST_CASE("test coro_http_client add header and url queries") {
  coro_http_client client{};
  client.add_header("Connection", "keep-alive");
  auto r =
      async_simple::coro::syncAwait(client.async_get("http://www.baidu.cn"));
  CHECK(!r.net_err);
  CHECK(r.status < 400);

  auto r2 = async_simple::coro::syncAwait(
      client.async_get("http://www.baidu.com?name='tom'&age=20"));
  CHECK(!r2.net_err);
  CHECK(r2.status < 400);
}

TEST_CASE("test coro_http_client not exist domain and bad uri") {
  {
    coro_http_client client{};
    auto r = async_simple::coro::syncAwait(
        client.async_get("http://www.notexistwebsit.com"));
    CHECK(r.net_err);
    CHECK(r.status != 200);
    CHECK(client.has_closed());
  }

  {
    coro_http_client client{};
    client.set_req_timeout(1s);
    auto r = async_simple::coro::syncAwait(
        client.async_get("http://www.baidu.com/><"));
    CHECK(r.net_err);
    CHECK(r.status != 200);
    CHECK(client.has_closed());
  }
}

TEST_CASE("test coro_http_client async_get") {
  coro_http_client client{};
  auto r =
      async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
  CHECK(!r.net_err);
  CHECK(r.status < 400);

  auto r1 =
      async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
  CHECK(!r1.net_err);
  CHECK(r1.status == 200);
}

TEST_CASE("test basic http request") {
  coro_http_server server(1, 8090);
  // Setting up GET and POST handlers
  server.set_http_handler<GET>(
      "/", [&server](coro_http_request &, coro_http_response &res) mutable {
        res.set_status_and_content(status_type::ok, "hello world");
      });
  server.set_http_handler<POST>(
      "/", [&server](coro_http_request &req, coro_http_response &res) mutable {
        std::string str(req.get_body());
        str.append(" reply from post");
        res.set_status_and_content(status_type::ok, std::move(str));
      });

  // Setting up PUT handler
  server.set_http_handler<PUT>(
      "/", [&server](coro_http_request &req, coro_http_response &res) mutable {
        std::string str(req.get_body());
        str.append(" put successfully");
        res.set_status_and_content(status_type::ok, std::move(str));
      });

  // Setting up DELETE handler
  server.set_http_handler<DEL>(
      "/", [&server](coro_http_request &, coro_http_response &res) mutable {
        res.set_status_and_content(status_type::ok, "data deleted");
      });

  server.async_start();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  coro_http_client client{};
  std::string uri = "http://127.0.0.1:8090";

  // Testing PUT method
  resp_data result = async_simple::coro::syncAwait(client.async_request(
      uri, http_method::PUT,
      req_context<std::string_view>{.content = "data for put"}));
  CHECK(result.resp_body == "data for put put successfully");

  // Testing DELETE method
  result = async_simple::coro::syncAwait(client.async_request(
      uri, http_method::DEL, req_context<std::string_view>{}));
  CHECK(result.resp_body == "data deleted");

  // Testing GET method again after DELETE
  result = async_simple::coro::syncAwait(client.async_get(uri));
  CHECK(result.resp_body == "hello world");

  size_t size = result.resp_body.size();
  auto buf = client.release_buf();
  CHECK(size == strlen(buf.data()));
  CHECK(buf == "hello world");

  // Rest of the POST tests
  result = async_simple::coro::syncAwait(client.async_post(
      uri, "async post hello coro_http_client", req_content_type::string));
  CHECK(result.resp_body ==
        "async post hello coro_http_client reply from post");

  result = client.post(uri, "sync post hello coro_http_client",
                       req_content_type::string);
  CHECK(result.resp_body == "sync post hello coro_http_client reply from post");

  std::string_view uri1 = "http://127.0.0.1:8090";
  std::string_view post_str = "post hello coro_http_client";

  result = async_simple::coro::syncAwait(
      client.async_request(uri, http_method::POST,
                           req_context<std::string_view>{.content = post_str}));
  CHECK(result.resp_body == "post hello coro_http_client reply from post");

  result = async_simple::coro::syncAwait(
      client.async_request(uri1, http_method::POST,
                           req_context<std::string_view>{.content = post_str}));
  CHECK(result.resp_body == "post hello coro_http_client reply from post");

  result = client.post(uri, "", req_content_type::string);
  CHECK(result.status == 200);

  server.stop();
}

TEST_CASE("test coro_http_client request timeout") {
  coro_http_client client{};
  cinatra::coro_http_client::config conf{.conn_timeout_duration = 10s,
                                         .req_timeout_duration = 1ms};
  client.init_config(conf);
  auto r =
      async_simple::coro::syncAwait(client.connect("http://www.baidu.com"));
  CINATRA_LOG_DEBUG << r.net_err.message() << "\n";
  if (!r.net_err) {
    r = async_simple::coro::syncAwait(client.async_get("/"));
    if (r.net_err) {
      CHECK(r.net_err == coro_http::http_errc::request_timeout);
    }
  }
}

#ifdef INJECT_FOR_HTTP_CLIENT_TEST
TEST_CASE("test inject failed") {
  coro_http_client client{};
  client.write_failed_forever_ = true;
  auto ret = client.get("http://baidu.com");
  CHECK(ret.status != 200);
  client.write_failed_forever_ = false;

  client.connect_timeout_forever_ = true;
  ret = async_simple::coro::syncAwait(client.connect("http://baidu.com"));
  CHECK(ret.status != 200);

  client.add_str_part("hello", "world");
  ret = async_simple::coro::syncAwait(
      client.async_upload_multipart("http://baidu.com"));
  CHECK(ret.status != 200);
  client.connect_timeout_forever_ = false;

  client.parse_failed_forever_ = true;
  ret = async_simple::coro::syncAwait(
      client.async_upload_multipart("http://baidu.com"));
  CHECK(ret.status != 200);
  client.parse_failed_forever_ = false;

  coro_http_server server(1, 8090);
  server.set_http_handler<GET, POST>(
      "/", [](coro_http_request &, coro_http_response &res) mutable {
        std::string str(1024, 'a');
        res.set_status_and_content(status_type::ok, std::move(str));
      });
  server.async_start();

  std::string uri = "http://127.0.0.1:8090";
  {
    coro_http_client client1{};
    client1.read_failed_forever_ = true;
    ret = client1.get(uri);
    CHECK(ret.status != 200);

    client1.close();
    std::string out;
    out.resize(2024);
    ret = async_simple::coro::syncAwait(
        client1.async_request(uri, http_method::GET, req_context<>{}, {},
                              std::span<char>{out.data(), out.size()}));
    CHECK(ret.status != 200);
    client1.read_failed_forever_ = false;
  }

  {
    coro_http_client client1{};
    client1.add_str_part("hello", "test");
    client1.write_failed_forever_ = true;
    client1.write_header_timeout_ = true;
    ret = async_simple::coro::syncAwait(
        client1.async_upload_multipart("http://baidu.com"));
    CHECK(ret.status != 200);
    client1.write_failed_forever_ = false;
    client1.write_header_timeout_ = false;
  }

  {
    coro_http_client client1{};
    client1.add_str_part("hello", "test");
    client1.write_failed_forever_ = true;
    client1.write_payload_timeout_ = true;
    ret = async_simple::coro::syncAwait(
        client1.async_upload_multipart("http://baidu.com"));
    CHECK(ret.status != 200);
  }

  {
    coro_http_client client1{};
    client1.add_str_part("hello", "test");
    client1.read_failed_forever_ = true;
    client1.read_timeout_ = true;
    ret = async_simple::coro::syncAwait(
        client1.async_upload_multipart("http://baidu.com"));
    CHECK(ret.status != 200);
  }

  {
    coro_http_client client1{};
    client1.write_failed_forever_ = true;
    ret = async_simple::coro::syncAwait(client1.connect("http://baidu.com"));
    if (!ret.net_err) {
      ret = async_simple::coro::syncAwait(client1.write_websocket("test"));
      CHECK(ret.status != 200);
    }
  }
}
#endif

TEST_CASE("test coro http proxy request") {
  coro_http_client client{};
  client.set_req_timeout(8s);
  std::string uri = "http://www.baidu.com";
  // Make sure the host and port are matching with your proxy server
  client.set_proxy("106.14.255.124", "80");
  resp_data result = async_simple::coro::syncAwait(client.async_get(uri));
  if (!result.net_err)
    CHECK(result.status >= 200);

  client.set_proxy("106.14.255.124", "80");
  result = async_simple::coro::syncAwait(client.async_get(uri));
  if (!result.net_err)
    CHECK(result.status >= 200);

  client.set_proxy("106.14.255.124", "80");
  uri = "http://www.baidu.com:443";
  result = async_simple::coro::syncAwait(client.async_get(uri));
  CHECK(result.status != 200);

  client.set_proxy("106.14.255.124", "80");
  uri = "http://www.baidu.com:12345";
  result = async_simple::coro::syncAwait(client.async_get(uri));
  CHECK(result.status != 200);
}

TEST_CASE("test coro http proxy request with port") {
  coro_http_client client{};
  client.set_req_timeout(8s);
  std::string uri = "http://www.baidu.com:80";
  // Make sure the host and port are matching with your proxy server
  client.set_proxy("106.14.255.124", "80");
  resp_data result = async_simple::coro::syncAwait(client.async_get(uri));
  if (!result.net_err)
    CHECK(result.status >= 200);  // maybe return 500 from that host.
}

// TEST_CASE("test coro http basic auth request") {
//   coro_http_client client{};
//   std::string uri = "http://www.purecpp.cn";
//   client.set_proxy_basic_auth("user", "pass");
//   resp_data result = async_simple::coro::syncAwait(client.async_get(uri));
//   CHECK(!result.net_err);
//   CHECK(result.status == 200);
// }

TEST_CASE("test coro http bearer token auth request") {
  coro_http_client client{};
  client.set_req_timeout(1s);
  std::string uri = "http://www.baidu.com";
  client.set_proxy_bearer_token_auth("password");
  resp_data result = async_simple::coro::syncAwait(client.async_get(uri));
  if (!result.net_err)
    CHECK(result.status < 400);
}

TEST_CASE("test coro http redirect request") {
  coro_http_client client{};
  client.set_req_timeout(8s);
  std::string uri = "http://httpbin.org/redirect-to?url=http://httpbin.org/get";
  resp_data result = async_simple::coro::syncAwait(client.async_get(uri));
  if (result.status != 404 && !result.net_err) {
    CHECK(!result.net_err);
    if (result.status < 500)
      CHECK((result.status == 302 || result.status == 301));

    if (client.is_redirect(result)) {
      std::string redirect_uri = client.get_redirect_uri();
      result = async_simple::coro::syncAwait(client.async_get(redirect_uri));
      if (result.status < 300)
        CHECK(result.status == 200);
    }

    client.enable_auto_redirect(true);
    result = async_simple::coro::syncAwait(client.async_get(uri));
    CHECK(result.status >= 200);
  }
}

TEST_CASE("test coro http request timeout") {
  coro_http_server server(1, 8090);
  server.set_http_handler<GET, POST>(
      "/", [&server](coro_http_request &, coro_http_response &res) mutable {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        res.set_status_and_content(status_type::ok, "hello world");
      });

  server.async_start();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  coro_http_client client{};
  std::string uri = "http://127.0.0.1:8090";

  resp_data result = async_simple::coro::syncAwait(client.async_get(uri));
  CHECK(result.status == 200);

  client.set_req_timeout(500ms);
  result = async_simple::coro::syncAwait(client.async_get(uri));
  CHECK(result.net_err == http_errc::request_timeout);  // TODO: use http_errc

  // after timeout, the socket in client has been closed, so use a new client
  // to test.
  coro_http_client client1{};
  result = async_simple::coro::syncAwait(client1.async_post(
      uri, "async post hello coro_http_client", req_content_type::string));
  CHECK(!result.net_err);

  server.stop();
}

TEST_CASE("test coro_http_client using external io_context") {
  asio::io_context io_context;
  std::promise<void> promise;
  auto future = promise.get_future();
  auto work = std::make_unique<asio::io_context::work>(io_context);
  std::thread io_thd([&io_context, &promise] {
    promise.set_value();
    io_context.run();
  });
  future.wait();

  coro_http_client client(io_context.get_executor());
  auto r =
      async_simple::coro::syncAwait(client.async_get("http://www.baidu.com"));
  CHECK(!r.net_err);
  CHECK(r.status < 400);
  work.reset();
  io_context.run();
  io_thd.join();
}

async_simple::coro::Lazy<resp_data> simulate_self_join() {
  coro_http_client client{};
  co_return co_await client.async_get("http://www.baidu.com");
}

TEST_CASE("test coro_http_client dealing with self join") {
  auto r = async_simple::coro::syncAwait(simulate_self_join());
  CHECK(!r.net_err);
  CHECK(r.status < 400);
}

TEST_CASE("test coro_http_client no scheme still send request check") {
  coro_http_server server(1, 8090);
  server.set_http_handler<GET, POST>(
      "/", [&server](coro_http_request &, coro_http_response &res) mutable {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        res.set_status_and_content(status_type::ok, "hello world");
      });

  server.async_start();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  std::string uri = "http://127.0.0.1:8090";

  coro_http_client client{};
  auto resp = async_simple::coro::syncAwait(client.async_get("127.0.0.1:8090"));
  CHECK(!resp.net_err);
  CHECK(resp.status == 200);
  resp = async_simple::coro::syncAwait(
      client.async_get("127.0.0.1:8090/ref='http://www.baidu.com'"));
  CHECK(resp.status == 404);

  server.stop();
}

#ifdef DSKIP_TIME_TEST
TEST_CASE("test conversion between unix time and gmt time, http format") {
  std::chrono::microseconds time_cost{0};
  std::ifstream file("../../tests/files_for_test_time_parse/http_times.txt");
  if (!file) {
    CINATRA_LOG_DEBUG << "open file failed";
  }
  std::string line;
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string time_to_parse;
    std::string timestamp;
    if (std::getline(iss, time_to_parse, '#') && std::getline(iss, timestamp)) {
      std::pair<bool, std::time_t> result;
      auto start = std::chrono::system_clock::now();
      for (int i = 0; i < 100; i++) {
        result = get_timestamp(time_to_parse);
      }
      auto end = std::chrono::system_clock::now();
      auto duration = duration_cast<std::chrono::microseconds>(end - start);
      time_cost += duration;
      if (result.first == true) {
        CHECK(timestamp != "invalid");
        if (timestamp != "invalid") {
          CHECK(result.second == std::stoll(timestamp));
        }
      }
      else {
        CHECK(timestamp == "invalid");
      }
    }
  }
  file.close();
  CINATRA_LOG_DEBUG << double(time_cost.count()) *
                           std::chrono::microseconds::period::num /
                           std::chrono::microseconds::period::den
                    << "s";
}

TEST_CASE("test conversion between unix time and gmt time, utc format") {
  std::chrono::microseconds time_cost{0};
  std::ifstream file("../../tests/files_for_test_time_parse/utc_times.txt");
  if (!file) {
    CINATRA_LOG_DEBUG << "open file failed";
  }
  std::string line;
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string time_to_parse;
    std::string timestamp;
    if (std::getline(iss, time_to_parse, '#') && std::getline(iss, timestamp)) {
      std::pair<bool, std::time_t> result;
      auto start = std::chrono::system_clock::now();
      for (int i = 0; i < 100; i++) {
        result = get_timestamp<time_format::utc_format>(time_to_parse);
      }
      auto end = std::chrono::system_clock::now();
      auto duration = duration_cast<std::chrono::microseconds>(end - start);
      time_cost += duration;
      if (result.first == true) {
        CHECK(timestamp != "invalid");
        if (timestamp != "invalid") {
          CHECK(result.second == std::stoll(timestamp));
        }
      }
      else {
        CHECK(timestamp == "invalid");
      }
    }
  }
  file.close();
  CINATRA_LOG_DEBUG << double(time_cost.count()) *
                           std::chrono::microseconds::period::num /
                           std::chrono::microseconds::period::den
                    << "s";
}

TEST_CASE(
    "test conversion between unix time and gmt time, utc without punctuation "
    "format") {
  std::chrono::microseconds time_cost{0};
  std::ifstream file(
      "../../tests/files_for_test_time_parse/"
      "utc_without_punctuation_times.txt");
  if (!file) {
    CINATRA_LOG_DEBUG << "open file failed";
  }
  std::string line;
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    std::string time_to_parse;
    std::string timestamp;
    if (std::getline(iss, time_to_parse, '#') && std::getline(iss, timestamp)) {
      std::pair<bool, std::time_t> result;
      auto start = std::chrono::system_clock::now();
      for (int i = 0; i < 100; i++) {
        result = get_timestamp<time_format::utc_without_punctuation_format>(
            time_to_parse);
      }
      auto end = std::chrono::system_clock::now();
      auto duration = duration_cast<std::chrono::microseconds>(end - start);
      time_cost += duration;
      if (result.first == true) {
        CHECK(timestamp != "invalid");
        if (timestamp != "invalid") {
          CHECK(result.second == std::stoll(timestamp));
        }
      }
      else {
        CHECK(timestamp == "invalid");
      }
    }
  }
  file.close();
  CINATRA_LOG_DEBUG << double(time_cost.count()) *
                           std::chrono::microseconds::period::num /
                           std::chrono::microseconds::period::den
                    << "s";
}
#endif

TEST_CASE("Testing get_content_type_str function") {
  SUBCASE("Test HTML content type") {
    CHECK(get_content_type_str(req_content_type::html) ==
          "text/html; charset=UTF-8");
  }

  SUBCASE("Test JSON content type") {
    CHECK(get_content_type_str(req_content_type::json) ==
          "application/json; charset=UTF-8");
  }

  SUBCASE("Test String content type") {
    CHECK(get_content_type_str(req_content_type::string) ==
          "text/html; charset=UTF-8");
  }

  SUBCASE("Test Multipart content type") {
    std::string result = get_content_type_str(req_content_type::multipart);
    std::string expectedPrefix = "multipart/form-data; boundary=";
    CHECK(result.find(expectedPrefix) ==
          0);  // Check if the result starts with the expected prefix

    // Check if there is something after the prefix,
    // this test failed.
    /*CHECK(result.length() > expectedPrefix.length());*/
  }

  SUBCASE("Test Octet Stream content type") {
    CHECK(get_content_type_str(req_content_type::octet_stream) ==
          "application/octet-stream");
  }

  SUBCASE("Test XML content type") {
    CHECK(get_content_type_str(req_content_type::xml) == "application/xml");
  }
}

TEST_CASE("test get_local_time_str with_month") {
  char buf[32];
  std::string_view format = "%Y-%m-%d %H:%M:%S";  // This format includes '%m'
  std::time_t t = std::time(nullptr);

  std::string_view result = cinatra::get_local_time_str(buf, t, format);
  CINATRA_LOG_DEBUG << "Local time with month: " << result << "\n";

  // Perform a basic check
  CHECK(!result.empty());
}

TEST_CASE("Testing base64_encode function") {
  SUBCASE("Base64 encoding of an empty string") {
    CHECK(base64_encode("") == "");
  }

  SUBCASE("Base64 encoding of 'Hello'") {
    CHECK(base64_encode("Hello") == "SGVsbG8=");
  }

  SUBCASE("Base64 encoding of a binary data") {
    std::string binaryData = "\x01\x02\x03";  // Example binary data
    CHECK(base64_encode(binaryData) == "AQID");
  }
}

TEST_CASE("Testing is_valid_utf8 function") {
  SUBCASE("Valid UTF-8 string") {
    auto validUtf8 = std::u8string(u8"Hello, 世界");
    std::string validUtf8Converted(validUtf8.begin(), validUtf8.end());
    CHECK(is_valid_utf8((unsigned char *)validUtf8.c_str(), validUtf8.size()) ==
          true);
  }

  SUBCASE("Invalid UTF-8 string with wrong continuation bytes") {
    std::string invalidUtf8 = "Hello, \x80\x80";  // wrong continuation bytes
    CHECK(is_valid_utf8((unsigned char *)invalidUtf8.c_str(),
                        invalidUtf8.size()) == false);
  }

  SUBCASE("Empty string") {
    std::string empty;
    CHECK(is_valid_utf8((unsigned char *)empty.c_str(), empty.size()) == true);
  }
}

TEST_CASE("test transfer cookie to string") {
  cookie cookie("name", "value");
  CHECK(cookie.get_name() == "name");
  CHECK(cookie.get_value() == "value");
  CHECK(cookie.to_string() == "name=value");
  cookie.set_path("/");
  CHECK(cookie.to_string() == "name=value; path=/");
  cookie.set_comment("comment");
  CHECK(cookie.to_string() == "name=value; path=/");
  cookie.set_domain("baidu.com");
  CHECK(cookie.to_string() == "name=value; domain=baidu.com; path=/");
  cookie.set_secure(true);
  CHECK(cookie.to_string() == "name=value; domain=baidu.com; path=/; secure");
  cookie.set_http_only(true);
  CHECK(cookie.to_string() ==
        "name=value; domain=baidu.com; path=/; secure; HttpOnly");
  cookie.set_priority("Low");
  CHECK(cookie.to_string() ==
        "name=value; domain=baidu.com; path=/; Priority=Low; secure; HttpOnly");
  cookie.set_priority("Medium");
  CHECK(cookie.to_string() ==
        "name=value; domain=baidu.com; path=/; Priority=Medium; secure; "
        "HttpOnly");
  cookie.set_priority("High");
  CHECK(
      cookie.to_string() ==
      "name=value; domain=baidu.com; path=/; Priority=High; secure; HttpOnly");
  cookie.set_priority("");
  cookie.set_http_only(false);

  cookie.set_version(1);
  CHECK(cookie.to_string() ==
        "name=\"value\"; Comment=\"comment\"; Domain=\"baidu.com\"; "
        "Path=\"/\"; secure; Version=\"1\"");

  cookie.set_secure(false);
  cookie.set_max_age(100);
  CHECK(cookie.to_string() ==
        "name=\"value\"; Comment=\"comment\"; Domain=\"baidu.com\"; "
        "Path=\"/\"; Max-Age=\"100\"; Version=\"1\"");

  cookie.set_http_only(true);
  CHECK(cookie.to_string() ==
        "name=\"value\"; Comment=\"comment\"; Domain=\"baidu.com\"; "
        "Path=\"/\"; Max-Age=\"100\"; HttpOnly; Version=\"1\"");

  cookie.set_priority("Low");
  CHECK(
      cookie.to_string() ==
      "name=\"value\"; Comment=\"comment\"; Domain=\"baidu.com\"; Path=\"/\"; "
      "Priority=\"Low\"; Max-Age=\"100\"; HttpOnly; Version=\"1\"");
  cookie.set_priority("Medium");
  CHECK(
      cookie.to_string() ==
      "name=\"value\"; Comment=\"comment\"; Domain=\"baidu.com\"; Path=\"/\"; "
      "Priority=\"Medium\"; Max-Age=\"100\"; HttpOnly; Version=\"1\"");
  cookie.set_priority("High");
  CHECK(
      cookie.to_string() ==
      "name=\"value\"; Comment=\"comment\"; Domain=\"baidu.com\"; Path=\"/\"; "
      "Priority=\"High\"; Max-Age=\"100\"; HttpOnly; Version=\"1\"");
}

std::vector<std::string_view> get_header_values(
    std::span<http_header> &resp_headers, std::string_view key) {
  std::vector<std::string_view> values{};
  for (const auto &p : resp_headers) {
    if (p.name == key)
      values.push_back(p.value);
  }
  return values;
}

std::string g_cookie_str1 = "";
std::string g_cookie_str2 = "";
std::mutex g_str_mtx;

TEST_CASE("test cookie") {
  coro_http_server server(5, 8090);
  server.set_http_handler<GET>(
      "/construct_cookies",
      [](coro_http_request &req, coro_http_response &res) {
        auto session = req.get_session();
        session->get_session_cookie().set_path("/");
        {
          std::lock_guard lock(g_str_mtx);
          g_cookie_str1 = session->get_session_cookie().to_string();
        }

        cookie another_cookie("test", "cookie");
        another_cookie.set_http_only(true);
        another_cookie.set_domain("baidu.com");
        res.add_cookie(another_cookie);
        {
          std::lock_guard lock(g_str_mtx);
          g_cookie_str2 = another_cookie.to_string();
        }

        res.set_status_and_content(status_type::ok, session->get_session_id());
      });

  server.set_http_handler<GET>(
      "/check_session_cookie",
      [](coro_http_request &req, coro_http_response &res) {
        auto session_id = req.get_header_value("Cookie");
        CHECK(session_id ==
              CSESSIONID + "=" + req.get_session()->get_session_id());
        res.set_status(status_type::ok);
      });

  server.async_start();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  coro_http_client client{};
  auto r1 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/construct_cookies"));
  auto cookie_strs = get_header_values(r1.resp_headers, "Set-Cookie");
  CHECK(cookie_strs.size() == 2);
  {
    std::lock_guard lock(g_str_mtx);
    bool check1 =
        (cookie_strs[0] == g_cookie_str1 && cookie_strs[1] == g_cookie_str2);
    bool check2 =
        (cookie_strs[1] == g_cookie_str1 && cookie_strs[0] == g_cookie_str2);
    CHECK((check1 || check2));
  }

  CHECK(r1.status == 200);

  std::string session_cookie =
      CSESSIONID + "=" + std::string(r1.resp_body.data(), r1.resp_body.size());

  client.add_header("Cookie", session_cookie);
  auto r2 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/check_session_cookie"));
  CHECK(r2.status == 200);

  server.stop();
}

std::string g_session_id_login = "";
std::string g_session_id_logout = "";
std::string g_session_id_check_login = "";
std::string g_session_id_check_logout = "";
std::mutex g_ss_mtx;

TEST_CASE("test session") {
  coro_http_server server(5, 8090);
  server.set_http_handler<GET>(
      "/login", [](coro_http_request &req, coro_http_response &res) {
        auto session = req.get_session();
        std::unique_lock lock(g_ss_mtx);
        g_session_id_login = session->get_session_id();
        lock.unlock();
        session->set_data("login", true);
        res.set_status(status_type::ok);
      });
  server.set_http_handler<GET>(
      "/logout", [](coro_http_request &req, coro_http_response &res) {
        auto session = req.get_session();
        std::unique_lock lock(g_ss_mtx);
        g_session_id_logout = session->get_session_id();
        lock.unlock();
        session->remove_data("login");
        res.set_status(status_type::ok);
      });
  server.set_http_handler<GET>(
      "/check_login", [](coro_http_request &req, coro_http_response &res) {
        auto session = req.get_session();
        std::unique_lock lock(g_ss_mtx);
        g_session_id_check_login = session->get_session_id();
        lock.unlock();
        bool login = session->get_data<bool>("login").value_or(false);
        CHECK(login == true);
        auto &all = session->get_all_data();
        CHECK(all.size() > 0);
        res.set_status(status_type::ok);
      });
  server.set_http_handler<GET>(
      "/check_logout", [](coro_http_request &req, coro_http_response &res) {
        auto session = req.get_session();
        std::unique_lock lock(g_ss_mtx);
        g_session_id_check_logout = session->get_session_id();
        lock.unlock();
        bool login = session->get_data<bool>("login").value_or(false);
        CHECK(login == false);
        res.set_status(status_type::ok);
      });
  server.async_start();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  coro_http_client client{};
  auto r1 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/check_logout"));
  CHECK(r1.status == 200);

  auto r2 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/login"));
  CHECK(r2.status == 200);
  {
    std::unique_lock lock(g_ss_mtx);
    CHECK(g_session_id_login != g_session_id_check_logout);
  }

  std::unique_lock lock(g_ss_mtx);
  std::string session_cookie = CSESSIONID + "=" + g_session_id_login;
  lock.unlock();

  client.add_header("Cookie", session_cookie);
  auto r3 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/check_login"));
  CHECK(r3.status == 200);
  {
    std::unique_lock lock(g_ss_mtx);
    CHECK(g_session_id_login == g_session_id_check_login);
  }

  client.add_header("Cookie", session_cookie);
  auto r4 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/logout"));
  CHECK(r4.status == 200);
  {
    std::unique_lock lock(g_ss_mtx);
    CHECK(g_session_id_login == g_session_id_logout);
  }

  client.add_header("Cookie", session_cookie);
  auto r5 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/check_logout"));
  CHECK(r5.status == 200);
  {
    std::unique_lock lock(g_ss_mtx);
    CHECK(g_session_id_login == g_session_id_check_logout);
  }

  server.stop();
}

std::string g_session_id = "";
std::mutex g_ss_mtx1;
TEST_CASE("test session timeout") {
  coro_http_server server(5, 8090);

  server.set_http_handler<GET>(
      "/construct_session",
      [](coro_http_request &req, coro_http_response &res) {
        auto session = req.get_session();
        std::unique_lock lock(g_ss_mtx1);
        g_session_id = session->get_session_id();
        session->set_session_timeout(1);
        res.set_status(status_type::ok);
      });

  server.set_http_handler<GET>("/no_sleep", [](coro_http_request &req,
                                               coro_http_response &res) {
    std::unique_lock lock(g_ss_mtx1);
    CHECK(session_manager::get().check_session_existence(g_session_id) == true);
    res.set_status(status_type::ok);
  });

  server.set_http_handler<GET>(
      "/after_sleep_2s", [](coro_http_request &req, coro_http_response &res) {
        std::unique_lock lock(g_ss_mtx1);
        CHECK(session_manager::get().check_session_existence(g_session_id) ==
              false);
        res.set_status(status_type::ok);
      });

  session_manager::get().set_check_session_duration(10ms);
  server.async_start();

  coro_http_client client{};
  auto r1 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/construct_session"));
  CHECK(r1.status == 200);

  auto r2 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/no_sleep"));
  CHECK(r2.status == 200);

  std::this_thread::sleep_for(2s);
  auto r3 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/after_sleep_2s"));
  CHECK(r3.status == 200);

  server.stop();
}

std::string g_session_id2 = "";
std::mutex g_ss_mtx2;
TEST_CASE("test session validate") {
  coro_http_server server(5, 8090);

  server.set_http_handler<GET>(
      "/construct_session",
      [](coro_http_request &req, coro_http_response &res) {
        auto session = req.get_session();
        std::unique_lock lock(g_ss_mtx2);
        g_session_id2 = session->get_session_id();
        res.set_status(status_type::ok);
      });

  server.set_http_handler<GET>(
      "/invalidate_session",
      [](coro_http_request &req, coro_http_response &res) {
        std::unique_lock lock(g_ss_mtx2);
        CHECK(session_manager::get().check_session_existence(g_session_id2) ==
              true);
        session_manager::get().get_session(g_session_id2)->invalidate();
        res.set_status(status_type::ok);
      });

  server.set_http_handler<GET>(
      "/after_sleep_2s", [](coro_http_request &req, coro_http_response &res) {
        std::unique_lock lock(g_ss_mtx2);
        CHECK(session_manager::get().check_session_existence(g_session_id2) ==
              false);
        res.set_status(status_type::ok);
      });

  session_manager::get().set_check_session_duration(10ms);
  server.async_start();

  coro_http_client client{};
  auto r1 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/construct_session"));
  CHECK(r1.status == 200);

  auto r2 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/invalidate_session"));
  CHECK(r2.status == 200);

  std::this_thread::sleep_for(2s);
  auto r3 = async_simple::coro::syncAwait(
      client.async_get("http://127.0.0.1:8090/after_sleep_2s"));
  CHECK(r3.status == 200);

  server.stop();
}