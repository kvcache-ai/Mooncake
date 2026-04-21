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
#include <iostream>

#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_http/coro_http_server.hpp"
#include "ylt/coro_io/coro_io.hpp"

using namespace std::chrono_literals;
using namespace coro_http;

void create_file(std::string filename, size_t file_size = 64) {
  std::ofstream file(filename, std::ios::binary);
  if (file) {
    std::string str(file_size, 'A');
    file.write(str.data(), str.size());
  }
}

async_simple::coro::Lazy<void> byte_ranges_download() {
  create_file("test_multiple_range.txt", 64);
  coro_http_server server(1, 8090);
  server.set_static_res_dir("", "");
  server.async_start();
  std::this_thread::sleep_for(200ms);

  std::string uri = "http://127.0.0.1:8090/test_multiple_range.txt";
  {
    std::string filename = "test1.txt";
    std::error_code ec{};
    std::filesystem::remove(filename, ec);

    coro_http_client client{};
    resp_data result = co_await client.async_download(uri, filename, "1-10");
    assert(result.status == 206);
    assert(std::filesystem::file_size(filename) == 10);

    filename = "test2.txt";
    std::filesystem::remove(filename, ec);
    result = co_await client.async_download(uri, filename, "10-15");
    assert(result.status == 206);
    assert(std::filesystem::file_size(filename) == 6);
  }

  {
    coro_http_client client{};
    std::string uri = "http://127.0.0.1:8090/test_multiple_range.txt";

    client.add_header("Range", "bytes=1-10,20-30");
    auto result = co_await client.async_get(uri);
    assert(result.status == 206);
    assert(result.resp_body.size() == 21);

    std::string filename = "test_ranges.txt";
    client.add_header("Range", "bytes=0-10,21-30");
    result = co_await client.async_download(uri, filename);
    assert(result.status == 206);
    assert(fs::file_size(filename) == 21);
  }
}

async_simple::coro::Lazy<resp_data> chunked_upload1(coro_http_client &client) {
  std::string filename = "test.txt";
  create_file(filename, 1010);

  coro_io::coro_file file{};
  file.open(filename, std::ios::in);

  std::string buf;
  detail::resize(buf, 100);

  auto fn = [&file, &buf]() -> async_simple::coro::Lazy<read_result> {
    auto [ec, size] = co_await file.async_read(buf.data(), buf.size());
    co_return read_result{{buf.data(), size}, file.eof(), ec};
  };

  auto result = co_await client.async_upload_chunked(
      "http://127.0.0.1:9001/chunked"sv, http_method::POST, std::move(fn));
  co_return result;
}

async_simple::coro::Lazy<void> chunked_upload_download() {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET, POST>(
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

        std::cout << "content size: " << content.size() << "\n";
        std::cout << content << "\n";
        resp.set_format_type(format_type::chunked);
        resp.set_status_and_content(status_type::ok, "chunked ok");
      });

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

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  auto r = co_await chunked_upload1(client);
  assert(r.status == 200);
  assert(r.resp_body == "chunked ok");

  auto ss = std::make_shared<std::stringstream>();
  *ss << "hello world";
  auto result = co_await client.async_upload_chunked(
      "http://127.0.0.1:9001/chunked"sv, http_method::POST, ss);
  assert(result.status == 200);
  assert(result.resp_body == "chunked ok");

  result = co_await client.async_get("http://127.0.0.1:9001/write_chunked");
  assert(result.status == 200);
  assert(result.resp_body == "hello world ok");
}

async_simple::coro::Lazy<void> use_websocket() {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/ws_echo",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        assert(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            std::cout << "close frame\n";
            break;
          }

          if (result.type == ws_frame_type::WS_TEXT_FRAME ||
              result.type == ws_frame_type::WS_BINARY_FRAME) {
            std::cout << result.data << "\n";
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
  std::this_thread::sleep_for(300ms);  // wait for server start

  coro_http_client client{};
  auto r = co_await client.connect("ws://127.0.0.1:9001/ws_echo");
  if (r.net_err) {
    co_return;
  }

  auto result = co_await client.write_websocket("hello websocket");
  assert(!result.net_err);
  auto data = co_await client.read_websocket();
  assert(data.resp_body == "hello websocket");
  result = co_await client.write_websocket("test again");
  assert(!result.net_err);
  data = co_await client.read_websocket();
  assert(data.resp_body == "test again");
}

async_simple::coro::Lazy<void> static_file_server() {
  std::string filename = "temp.txt";
  create_file(filename, 64);

  coro_http_server server(1, 9001);

  std::string virtual_path = "download";
  std::string files_root_path = "";  // current path
  server.set_static_res_dir(
      virtual_path,
      files_root_path);  // set this before server start, if you add new files,
                         // you need restart the server.
  server.async_start();
  std::this_thread::sleep_for(300ms);  // wait for server start

  coro_http_client client{};
  auto result =
      co_await client.async_get("http://127.0.0.1:9001/download/temp.txt");
  assert(result.status == 200);
  assert(result.resp_body.size() == 64);
}

struct log_t {
  bool before(coro_http_request &, coro_http_response &) {
    std::cout << "before log" << std::endl;
    return true;
  }

  bool after(coro_http_request &, coro_http_response &res) {
    std::cout << "after log" << std::endl;
    res.add_header("aaaa", "bbcc");
    return true;
  }
};

struct get_data {
  bool before(coro_http_request &req, coro_http_response &res) {
    req.set_aspect_data("hello world");
    return true;
  }
};

async_simple::coro::Lazy<void> use_aspects() {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/get",
      [](coro_http_request &req, coro_http_response &resp) {
        auto val = req.get_aspect_data();
        assert(val[0] == "hello world");
        resp.set_status_and_content(status_type::ok, "ok");
      },
      log_t{}, get_data{});

  server.async_start();
  std::this_thread::sleep_for(300ms);  // wait for server start

  coro_http_client client{};
  auto result = co_await client.async_get("http://127.0.0.1:9001/get");
  assert(result.status == 200);

  co_return;
}

struct person_t {
  void foo(coro_http_request &, coro_http_response &res) {
    res.set_status_and_content(status_type::ok, "ok");
  }
};

void url_queries() {
  {
    http_parser parser{};
    parser.parse_query("=");
    parser.parse_query("&a");
    parser.parse_query("&b=");
    parser.parse_query("&c=&d");
    parser.parse_query("&e=&f=1");
    parser.parse_query("&g=1&h=1");
    auto map = parser.queries();
    assert(map["a"].empty());
    assert(map["b"].empty());
    assert(map["c"].empty());
    assert(map["d"].empty());
    assert(map["e"].empty());
    assert(map["f"] == "1");
    assert(map["g"] == "1" && map["h"] == "1");
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
    assert(map["test"].empty());
    assert(map.size() == 21);
  }
}

async_simple::coro::Lazy<void> basic_usage() {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/get", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<GET>(
      "/queries", [](coro_http_request &req, coro_http_response &resp) {
        auto map = req.get_queries();
        assert(map["test"] == "");
        resp.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<GET>(
      "/queries2", [](coro_http_request &req, coro_http_response &resp) {
        auto map = req.get_queries();
        assert(map["test"] == "");
        assert(map["a"] == "42");
        resp.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<GET>(
      "/coro",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok, "ok");
        co_return;
      });

  server.set_http_handler<GET>(
      "/in_thread_pool",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        // will respose in another thread.
        co_await coro_io::post([&] {
          // do your heavy work here when finished work, response.
          resp.set_status_and_content(status_type::ok, "ok");
        });
      });

  server.set_http_handler<POST, PUT>(
      "/post", [](coro_http_request &req, coro_http_response &resp) {
        assert(resp.get_conn()->remote_address().find("127.0.0.1") !=
               std::string::npos);
        assert(resp.get_conn()->remote_address().find("127.0.0.1") !=
               std::string::npos);
        assert(resp.get_conn()->local_address() == "127.0.0.1:9001");
        auto req_body = req.get_body();
        resp.set_status_and_content(status_type::ok, std::string{req_body});
      });

  server.set_http_handler<GET>(
      "/headers", [](coro_http_request &req, coro_http_response &resp) {
        auto name = req.get_header_value("name");
        auto age = req.get_header_value("age");
        assert(name == "tom");
        assert(age == "20");
        resp.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<GET>(
      "/query", [](coro_http_request &req, coro_http_response &resp) {
        auto name = req.get_query_value("name");
        auto age = req.get_query_value("age");
        assert(name == "tom");
        assert(age == "20");
        resp.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<GET, POST>(
      "/users/:userid/subscriptions/:subid",
      [](coro_http_request &req, coro_http_response &response) {
        assert(req.params_["userid"] == "ultramarines");
        assert(req.params_["subid"] == "guilliman");
        response.set_status_and_content(status_type::ok, "ok");
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
  server.set_default_handler(
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok, "default response");
        co_return;
      });

  person_t person{};
  server.set_http_handler<GET>("/person", &person_t::foo, person);

  server.async_start();
  std::this_thread::sleep_for(300ms);  // wait for server start

  coro_http_client client{};
  auto result = co_await client.async_get("http://127.0.0.1:9001/get");
  assert(result.status == 200);
  assert(result.resp_body == "ok");
  for (auto [key, val] : result.resp_headers) {
    std::cout << key << ": " << val << "\n";
  }

  co_await client.async_get("/queries?test");
  co_await client.async_get("/queries2?test&a=42");

  result = co_await client.async_get("/coro");
  assert(result.status == 200);

  result = co_await client.async_get("/in_thread_pool");
  assert(result.status == 200);

  result = co_await client.async_post("/post", "post string",
                                      req_content_type::string);
  assert(result.status == 200);
  assert(result.resp_body == "post string");

  result = co_await client.async_post("/view", "post string",
                                      req_content_type::string);
  assert(result.status == 200);
  assert(result.resp_body == "post string");

  client.add_header("name", "tom");
  client.add_header("age", "20");
  result = co_await client.async_get("/headers");
  assert(result.status == 200);

  result = co_await client.async_get("/query?name=tom&age=20");
  assert(result.status == 200);

  result = co_await client.async_get(
      "http://127.0.0.1:9001/users/ultramarines/subscriptions/guilliman");
  assert(result.status == 200);

  result = co_await client.async_get("/not_exist");
  assert(result.status == 200);
  assert(result.resp_body == "default response");

  // make sure you have install openssl and enable CINATRA_ENABLE_SSL
#ifdef CINATRA_ENABLE_SSL
  coro_http_client client2{};
  result = co_await client2.async_get("https://www.baidu.com");
  assert(result.status == 200);

  coro_http_client client3{};
  co_await client3.connect("https://www.baidu.com");
  result = co_await client3.async_get("/");
  assert(result.status == 200);

  coro_http_client client4{};
  client4.set_ssl_schema(true);
  result = client4.get("www.baidu.com");
  assert(result.status == 200);

  coro_http_client client5{};
  client5.set_ssl_schema(true);
  co_await client5.connect("www.baidu.com");
  result = co_await client5.async_get("/");
  assert(result.status == 200);
#endif
}

#ifdef CINATRA_ENABLE_GZIP
std::string_view get_header_value(auto &resp_headers, std::string_view key) {
  for (const auto &[k, v] : resp_headers) {
    if (k == key)
      return v;
  }
  return {};
}

void test_gzip() {
  coro_http_server server(1, 8090);
  server.set_http_handler<GET, POST>(
      "/gzip", [](coro_http_request &req, coro_http_response &res) {
        assert(req.get_header_value("Content-Encoding") == "gzip");
        res.set_status_and_content(status_type::ok, "hello world",
                                   content_encoding::gzip);
      });
  server.async_start();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  coro_http_client client{};
  std::string uri = "http://127.0.0.1:8090/gzip";
  client.add_header("Content-Encoding", "gzip");
  auto result = async_simple::coro::syncAwait(client.async_get(uri));
  auto content = get_header_value(result.resp_headers, "Content-Encoding");
  assert(get_header_value(result.resp_headers, "Content-Encoding") == "gzip");
  std::string decompress_data;
  bool ret = gzip_codec::uncompress(result.resp_body, decompress_data);
  assert(ret == true);
  assert(decompress_data == "hello world");
  server.stop();
}
#endif

void http_proxy() {
  cinatra::coro_http_server web_one(1, 9001);

  web_one.set_http_handler<cinatra::GET, cinatra::POST>(
      "/",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          response.set_status_and_content(status_type::ok, "web1");
        });
      });

  web_one.async_start();

  cinatra::coro_http_server web_two(1, 9002);

  web_two.set_http_handler<cinatra::GET, cinatra::POST>(
      "/",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          response.set_status_and_content(status_type::ok, "web2");
        });
      });

  web_two.async_start();

  cinatra::coro_http_server web_three(1, 9003);

  web_three.set_http_handler<cinatra::GET, cinatra::POST>(
      "/", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "web3");
      });

  web_three.async_start();

  std::this_thread::sleep_for(200ms);

  coro_http_server proxy_wrr(2, 8090);
  proxy_wrr.set_http_proxy_handler<GET, POST>(
      "/", {"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"},
      coro_io::load_balance_algorithm::WRR, {10, 5, 5});

  coro_http_server proxy_rr(2, 8091);
  proxy_rr.set_http_proxy_handler<GET, POST>(
      "/", {"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"},
      coro_io::load_balance_algorithm::RR);

  coro_http_server proxy_random(2, 8092);
  proxy_random.set_http_proxy_handler<GET, POST>(
      "/", {"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"});

  coro_http_server proxy_all(2, 8093);
  proxy_all.set_http_proxy_handler(
      "/", {"127.0.0.1:9001", "127.0.0.1:9002", "127.0.0.1:9003"});

  proxy_wrr.async_start();
  proxy_rr.async_start();
  proxy_random.async_start();
  proxy_all.async_start();

  std::this_thread::sleep_for(200ms);

  coro_http_client client_rr;
  resp_data resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web1");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web2");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web3");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web1");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  assert(resp_rr.resp_body == "web2");
  resp_rr = client_rr.post("http://127.0.0.1:8091/", "test content",
                           req_content_type::text);
  assert(resp_rr.resp_body == "web3");

  coro_http_client client_wrr;
  resp_data resp = client_wrr.get("http://127.0.0.1:8090/");
  assert(resp.resp_body == "web1");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  assert(resp.resp_body == "web1");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  assert(resp.resp_body == "web2");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  assert(resp.resp_body == "web3");

  coro_http_client client_random;
  resp_data resp_random = client_random.get("http://127.0.0.1:8092/");
  std::cout << resp_random.resp_body << "\n";
  assert(!resp_random.resp_body.empty());

  coro_http_client client_all;
  resp_random = client_all.post("http://127.0.0.1:8093/", "test content",
                                req_content_type::text);
  std::cout << resp_random.resp_body << "\n";
  assert(!resp_random.resp_body.empty());
}

void coro_channel() {
  auto ch = coro_io::create_channel<int>(10000);
  auto ec = async_simple::coro::syncAwait(coro_io::async_send(ch, 41));
  assert(!ec);
  ec = async_simple::coro::syncAwait(coro_io::async_send(ch, 42));
  assert(!ec);

  std::error_code err;
  int val;
  std::tie(err, val) =
      async_simple::coro::syncAwait(coro_io::async_receive(ch));
  assert(!err);
  assert(val == 41);

  std::tie(err, val) =
      async_simple::coro::syncAwait(coro_io::async_receive(ch));
  assert(!err);
  assert(val == 42);
}

enum class upload_type { send_file, chunked, multipart };

void test_outbuf() {
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
    std::cout << oubuf.data() << "\n";

    std::string_view out_view(oubuf.data(), result.resp_body.size());
    assert(out_view == "test");
    assert(out_view == result.resp_body);

    auto ss = std::make_shared<std::stringstream>();
    *ss << "hello world";

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

    std::cout << oubuf.data() << "\n";

    std::string_view out_view1(oubuf.data(), out_view.size());
    assert(out_view == out_view1);
    assert(result.resp_body != out_view1);
  };
  async_simple::coro::syncAwait(lazy(upload_type::send_file));
  async_simple::coro::syncAwait(lazy(upload_type::chunked));
  async_simple::coro::syncAwait(lazy(upload_type::multipart));
}

int main() {
  test_outbuf();
  url_queries();
  async_simple::coro::syncAwait(basic_usage());
  async_simple::coro::syncAwait(use_aspects());
  async_simple::coro::syncAwait(static_file_server());
  async_simple::coro::syncAwait(use_websocket());
  async_simple::coro::syncAwait(chunked_upload_download());
  async_simple::coro::syncAwait(byte_ranges_download());
#ifdef CINATRA_ENABLE_GZIP
  test_gzip();
#endif
  http_proxy();
  coro_channel();
  return 0;
}