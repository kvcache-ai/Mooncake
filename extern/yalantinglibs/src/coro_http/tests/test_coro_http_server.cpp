#include <chrono>
#include <cstdio>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/SyncAwait.h"
#include "doctest.h"
#include "ylt/coro_http/coro_http_client.hpp"
#include "ylt/coro_http/coro_http_server.hpp"

using namespace coro_http;

using namespace std::chrono_literals;

TEST_CASE("test parse ranges") {
  bool is_valid = true;
  auto vec = parse_ranges("200-999", 10000, is_valid);
  CHECK(is_valid);
  CHECK(vec == std::vector<std::pair<int, int>>{{200, 999}});

  vec = parse_ranges("-", 10000, is_valid);
  CHECK(is_valid);
  CHECK(vec == std::vector<std::pair<int, int>>{{0, 9999}});

  vec = parse_ranges("-a", 10000, is_valid);
  CHECK(!is_valid);
  CHECK(vec.empty());

  vec = parse_ranges("--100", 10000, is_valid);
  CHECK(!is_valid);
  CHECK(vec.empty());

  vec = parse_ranges("abc", 10000, is_valid);
  CHECK(!is_valid);
  CHECK(vec.empty());

  is_valid = true;
  vec = parse_ranges("-900", 10000, is_valid);
  CHECK(is_valid);
  CHECK(vec == std::vector<std::pair<int, int>>{{9100, 9999}});

  vec = parse_ranges("900", 10000, is_valid);
  CHECK(is_valid);
  CHECK(vec == std::vector<std::pair<int, int>>{{900, 9999}});

  vec = parse_ranges("200-999, 2000-2499", 10000, is_valid);
  CHECK(is_valid);
  CHECK(vec == std::vector<std::pair<int, int>>{{200, 999}, {2000, 2499}});

  vec = parse_ranges("200-999, 2000-2499, 9500-", 10000, is_valid);
  CHECK(is_valid);
  CHECK(vec == std::vector<std::pair<int, int>>{
                   {200, 999}, {2000, 2499}, {9500, 9999}});

  vec = parse_ranges("", 10000, is_valid);
  CHECK(is_valid);
  CHECK(vec == std::vector<std::pair<int, int>>{{0, 9999}});
}

TEST_CASE("coro_io post") {
  auto t1 = async_simple::coro::syncAwait(coro_io::post([] {
  }));
  CHECK(!t1.hasError());
  auto t2 = async_simple::coro::syncAwait(coro_io::post([] {
    throw std::invalid_argument("e");
  }));
  CHECK(t2.hasError());

  auto t3 = async_simple::coro::syncAwait(coro_io::post([] {
    return 1;
  }));
  int r3 = t3.value();
  CHECK(r3 == 1);

  auto t4 = async_simple::coro::syncAwait(coro_io::post([] {
    throw std::invalid_argument("e");
    return 1;
  }));
  CHECK(t4.hasError());

  try {
    std::rethrow_exception(t4.getException());
  } catch (const std::exception &e) {
    CHECK(e.what() == std::string("e"));
    CINATRA_LOG_DEBUG << e.what() << "\n";
  }
}

TEST_CASE("coro_server example, will block") {
  return;  // remove this line when you run the coro server.
  cinatra::coro_http_server server(std::thread::hardware_concurrency(), 9001);
  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/", [](coro_http_request &req, coro_http_response &resp) {
        // response in io thread.
        std::this_thread::sleep_for(10ms);
        resp.set_status_and_content(cinatra::status_type::ok, "hello world");
      });

  server.set_http_handler<cinatra::GET>(
      "/coro",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          // coroutine in other thread.
          std::this_thread::sleep_for(10ms);
          resp.set_status_and_content(cinatra::status_type::ok, "hello world");
        });
        co_return;
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/echo", [](coro_http_request &req, coro_http_response &resp) {
        // response in io thread.
        resp.set_status_and_content(cinatra::status_type::ok, "hello world");
      });
  server.sync_start();
  CHECK(server.port() > 0);
}

template <typename View>
bool create_file(View filename, size_t file_size = 1024) {
  CINATRA_LOG_DEBUG << "begin to open file: " << filename << "\n";
  std::ofstream out(filename, std::ios::binary);
  if (!out.is_open()) {
    CINATRA_LOG_DEBUG << "open file: " << filename << " failed\n";
    return false;
  }
  CINATRA_LOG_DEBUG << "open file: " << filename << " ok\n";
  std::string str(file_size, 'A');
  out.write(str.data(), str.size());
  return true;
}

TEST_CASE("test redirect") {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/", [](coro_http_request &req, coro_http_response &resp) {
        resp.redirect("/test");
      });

  server.set_http_handler<GET>(
      "/test", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "redirect ok");
      });

  server.async_start();

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:9001/");
  CHECK(result.status == 302);
  for (auto [k, v] : result.resp_headers) {
    if (k == "Location") {
      auto r = client.get(std::string(v));
      CHECK(r.resp_body == "redirect ok");
      break;
    }
  }
}

TEST_CASE("test post") {
  cinatra::coro_http_server server(1, 9001);
  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/echo",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_status_and_content(status_type::ok,
                                    std::string(req.get_body()));
        co_return;
      });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  std::string str = "test";
  auto r =
      client.post("http://127.0.0.1:9001/echo", str, req_content_type::text);
  CHECK(r.status == 200);
  CHECK(r.resp_body == "test");

  r = client.post("/echo", "", req_content_type::text);
  CHECK(r.status == 200);
  CHECK(r.resp_body == "");
}

TEST_CASE("test multiple download") {
  coro_http_server server(1, 9001);
  server.set_http_handler<GET>(
      "/",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        // multipart_reader_t multipart(resp.get_conn());
        bool ok;
        if (ok = co_await resp.get_conn()->begin_multipart(); !ok) {
          co_return;
        }

        std::vector<std::string> vec{"hello", " world", " chunked"};

        for (auto &str : vec) {
          if (ok = co_await resp.get_conn()->write_multipart(str, "text/plain");
              !ok) {
            co_return;
          }
        }

        ok = co_await resp.get_conn()->end_multipart();
      });
  server.set_http_handler<GET>(
      "/multipart",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        bool ok;
        if (ok = co_await resp.get_conn()->begin_multipart(); !ok) {
          co_return;
        }

        std::vector<std::string> vec{"hello", " world", " multipart"};

        for (auto &str : vec) {
          if (ok = co_await resp.get_conn()->write_multipart(str, "text/plain");
              !ok) {
            co_return;
          }
        }

        ok = co_await resp.get_conn()->end_multipart();
      });

  server.async_start();

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:9001/");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello world chunked");
  result = client.get("http://127.0.0.1:9001/multipart");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello world multipart");
}

TEST_CASE("test range download") {
  create_file("range_test.txt", 64);
#ifdef ASIO_WINDOWS
#else
  create_file("中文测试.txt", 64);
  create_file(fs::path(u8"utf8中文.txt").string(), 64);
#endif
  CINATRA_LOG_DEBUG << fs::current_path() << "\n";
  coro_http_server server(1, 9001);
  server.set_static_res_dir("", "");
  server.set_file_resp_format_type(file_resp_format_type::range);
  server.async_start();
  std::this_thread::sleep_for(300ms);

#ifdef ASIO_WINDOWS
#else
  {
    // test Chinese file name
    coro_http_client client{};
    std::string local_filename = "temp.txt";

    std::string base_uri = "http://127.0.0.1:9001/";
    std::string path = code_utils::url_encode("中文测试.txt");
    auto result = client.download(base_uri + path, local_filename);
    CHECK(result.status == 200);
    CHECK(fs::file_size(local_filename) == 64);
  }

  {
    coro_http_client client{};
    std::string local_filename = "temp1.txt";
    std::string base_uri = "http://127.0.0.1:9001/";
    std::string path =
        code_utils::url_encode(fs::path(u8"utf8中文.txt").string());
    auto result = client.download(base_uri + path, local_filename);
    CHECK(result.status == 200);
    CHECK(fs::file_size(local_filename) == 64);
  }
#endif

  coro_http_client client{};
  std::string filename = "test1.txt";
  std::error_code ec{};
  std::filesystem::remove(filename, ec);

  std::string uri = "http://127.0.0.1:9001/range_test.txt";
  resp_data result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "1-16"));
  CHECK(result.status == 206);
  CHECK(fs::file_size(filename) == 16);

  filename = "test2.txt";
  result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "0-63"));
  CHECK(result.status == 200);
  CHECK(fs::file_size(filename) == 64);

  filename = "test2.txt";
  result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "-10"));
  CHECK(result.status == 206);
  CHECK(fs::file_size(filename) == 10);

  filename = "test2.txt";
  result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "0-200"));
  CHECK(result.status == 200);
  CHECK(fs::file_size(filename) == 64);

  filename = "test3.txt";
  result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "100-200"));
  CHECK(result.status == 416);

  result = async_simple::coro::syncAwait(
      client.async_download(uri, filename, "aaa-200"));
  CHECK(result.status == 416);
}

class my_object {
 public:
  void normal(coro_http_request &req, coro_http_response &response) {
    response.set_status_and_content(status_type::ok, "ok");
  }

  async_simple::coro::Lazy<void> lazy(coro_http_request &req,
                                      coro_http_response &response) {
    response.set_status_and_content(status_type::ok, "ok lazy");
    co_return;
  }
};

TEST_CASE("set http handler") {
  cinatra::coro_http_server server(1, 9001);
  auto &router = server.get_router();
  auto &handlers = router.get_handlers();

  server.set_http_handler<cinatra::GET>(
      "/", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "ok");
      });
  CHECK(handlers.size() == 1);
  server.set_http_handler<cinatra::GET>(
      "/", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "ok");
      });
  CHECK(handlers.size() == 1);
  server.set_http_handler<cinatra::GET>(
      "/aa", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "ok");
      });
  CHECK(handlers.size() == 2);

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/bb", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "ok");
      });
  CHECK(handlers.size() == 4);

  cinatra::coro_http_server server2(1, 9001);
  server2.set_http_handler<cinatra::GET>(
      "/", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "ok");
      });

  auto &handlers2 = server2.get_router().get_handlers();
  CHECK(handlers2.size() == 1);

  my_object o{};
  // member function
  server2.set_http_handler<GET>("/test", &my_object::normal, o);
  server2.set_http_handler<GET>("/test_lazy", &my_object::lazy, o);
  CHECK(handlers2.size() == 2);

  auto coro_func =
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
    response.set_status_and_content(status_type::ok, "ok");
    co_return;
  };

  auto &coro_handlers = router.get_coro_handlers();
  server.set_http_handler<cinatra::GET>("/", coro_func);
  CHECK(coro_handlers.size() == 1);
  server.set_http_handler<cinatra::GET>("/", coro_func);
  CHECK(coro_handlers.size() == 1);
  server.set_http_handler<cinatra::GET>("/aa", coro_func);
  CHECK(coro_handlers.size() == 2);

  server.set_http_handler<cinatra::GET, cinatra::POST>("/bb", coro_func);
  CHECK(coro_handlers.size() == 4);
}

TEST_CASE("test llm chunked stream api") {
  std::vector<std::string> llm_msgs = {
      R"(data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1732500000,"model":"qwen-max","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]})",
      R"(data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1732500000,"model":"qwen-max","choices":[{"index":0,"delta":{"content":"为什么程序员总是分不清万圣节和圣诞节？"},"finish_reason":null}]})",
      R"(data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1732500000,"model":"qwen-max","choices":[{"index":0,"delta":{"content":"因为 Oct 31 == Dec 25"}}, "finish_reason":null}]})",
      R"(data: {"id":"chatcmpl-abc123","object":"chat.completion.chunk","created":1732500000,"model":"qwen-max","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})",
      R"(data: [DONE])"};

  coro_http_server server(2, 9000);
  server.set_http_handler<POST>(
      "/llm_api",
      [&](coro_http_request &req,
          coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_format_type(format_type::chunked);
        resp.add_header("Content-Type", "text/event-stream");
        resp.add_header("Cache-Control", "no-cache");
        resp.add_header("Connection", "keep-alive");

        co_await resp.get_conn()->begin_chunked();

        for (auto &msg : llm_msgs) {
          co_await resp.get_conn()->write_chunked(msg);
        }
        co_await resp.get_conn()->end_chunked();
      });
  server.async_start();

  auto send_req = []() -> async_simple::coro::Lazy<void> {
    std::string uri = "http://127.0.0.1:9000/llm_api";
    std::string req_json =
        R"({"model":"qwen-max",  "messages": [{"role": "user", "content": "讲个笑话"}], "stream": true})";

    bool has_finished = false;
    coro_http_client client{};
    client.set_chunked_callback(
        [&](std::string_view data) -> async_simple::coro::Lazy<void> {
          auto json = data.substr(6);
          std::cout << json << "\n";
          if (json == "[DONE]") {
            std::cout << "all finished\n";
            has_finished = true;
          }
          co_return;
        });
    CHECK(client.has_chunked_callback());
    auto r = co_await client.async_post(uri, req_json, req_content_type::json);
    CHECK(r.status == 200);
    CHECK(has_finished);
  };

  async_simple::coro::syncAwait(send_req());
}

TEST_CASE("test server start and stop") {
  cinatra::coro_http_server server(1, 9000);
  auto future = server.async_start();

  cinatra::coro_http_server server2(1, 9000);
  auto future2 = server2.async_start();
  future2.wait();
  auto ec = future2.value();
  CHECK(ec == asio::error::address_in_use);
}

TEST_CASE("test server sync_start and stop") {
  cinatra::coro_http_server server(1, 0);

  std::promise<void> promise;
  std::error_code ec;
  std::thread thd([&] {
    promise.set_value();
    ec = server.sync_start();
  });
  promise.get_future().wait();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  server.stop();
  thd.join();
  CHECK(server.port() > 0);
  CHECK(ec == asio::error::operation_aborted);
}

TEST_CASE("get post") {
  cinatra::coro_http_server server(1, 9001);
  server.set_shrink_to_fit(true);
  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/test", [](coro_http_request &req, coro_http_response &resp) {
        auto value = req.get_header_value("connection");
        CHECK(!value.empty());

        auto value1 = req.get_header_value("connection1");
        CHECK(value1.empty());

        auto value2 = req.get_query_value("aa");
        CHECK(value2 == "1");

        auto value3 = req.get_query_value("bb");
        CHECK(value3 == "test");

        auto value4 = req.get_query_value("cc");
        CHECK(value4.empty());

        auto headers = req.get_headers();
        CHECK(!headers.empty());

        auto queries = req.get_queries();
        CHECK(!queries.empty());

        resp.set_keepalive(true);
        resp.set_status_and_content(cinatra::status_type::ok, "hello world");
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/test1", [](coro_http_request &req, coro_http_response &resp) {
        CHECK(req.get_method() == "POST");
        CHECK(req.get_url() == "/test1");
        CHECK(req.get_conn()->local_address() == "127.0.0.1:9001");
        CHECK(req.get_conn()->remote_address().find("127.0.0.1:") !=
              std::string::npos);
        CHECK(req.get_conn()->remote_address().find("127.0.0.1:") !=
              std::string::npos);
        resp.add_header("Host", "Cinatra");
        resp.set_status_and_content(cinatra::status_type::ok, "hello world");
      });

  server.set_http_handler<cinatra::GET>(
      "/test_coro",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&] {
          resp.set_status(cinatra::status_type::ok);
          resp.set_content("hello world in coro");
        });
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/empty", [](coro_http_request &req, coro_http_response &resp) {
        resp.add_header("Host", "Cinatra");
        resp.set_status_and_content(cinatra::status_type::ok, "");
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/close", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_keepalive(false);
        resp.set_status_and_content(cinatra::status_type::ok, "hello");
        resp.get_conn()->close();
        auto s = req.get_conn()->local_address();
        CHECK(s.empty());
      });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  resp_data result;
  result = client.get("http://127.0.0.1:9001/test?aa=1&bb=test");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello world");

  result =
      client.post("http://127.0.0.1:9001/test1", "", req_content_type::text);
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello world");

  result = client.get("http://127.0.0.1:9001/test_coro");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello world in coro");

  result = client.get("http://127.0.0.1:9001/not_exist");
  CHECK(result.status == 404);

  result = client.get("http://127.0.0.1:9001/empty");
  CHECK(result.status == 200);
  auto &headers = result.resp_headers;
  auto it =
      std::find_if(headers.begin(), headers.end(), [](http_header &header) {
        return header.name == "Host" && header.value == "Cinatra";
      });
  CHECK(it != headers.end());
  CHECK(result.resp_body.empty());

  client.add_header("Connection", "close");
  result = client.get("http://127.0.0.1:9001/close");
  CHECK(result.status != 200);

  server.stop();
}

TEST_CASE("test alias") {
  http_server server(1, 9001);
  server.set_http_handler<GET>("/", [](request &req, response &resp) {
    resp.set_status_and_content(status_type::ok, "ok");
  });
  server.async_start();
  std::this_thread::sleep_for(300ms);

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:9001/");
  CHECK(result.resp_body == "ok");
}

struct log_t {
  bool before(coro_http_request &, coro_http_response &) {
    CINATRA_LOG_DEBUG << "before log";
    return true;
  }

  bool after(coro_http_request &, coro_http_response &res) {
    CINATRA_LOG_DEBUG << "after log";
    res.add_header("aaaa", "bbcc");
    return true;
  }
};

struct check_t {
  bool before(coro_http_request &, coro_http_response &) {
    CINATRA_LOG_DEBUG << "check before";
    return true;
  }
};

struct check_t1 {
  bool before(coro_http_request &, coro_http_response &resp) {
    CINATRA_LOG_DEBUG << "check1 before";
    resp.set_status_and_content(status_type::bad_request, "check failed");
    return false;
  }
};

struct get_data {
  bool before(coro_http_request &req, coro_http_response &res) {
    req.set_aspect_data("hello", "world");
    return true;
  }
};

TEST_CASE("test aspects") {
  coro_http_server server(1, 9001);
  server.set_static_res_dir("", "");
  server.set_max_size_of_cache_files(100);
  create_file("test_aspect.txt", 64);  // in cache
  create_file("test_file.txt", 200);   // not in cache

  server.set_static_res_dir("", "", log_t{}, check_t{});
  server.set_http_handler<GET, POST>(
      "/",
      [](coro_http_request &req, coro_http_response &resp) {
        resp.add_header("aaaa", "bbcc");
        resp.set_status_and_content(status_type::ok, "ok");
      },
      log_t{}, check_t{});

  server.set_http_handler<GET, POST>(
      "/aspect",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        auto &val = req.get_aspect_data();
        CHECK(val[0] == "hello");
        CHECK(val[1] == "world");
        resp.set_status_and_content(status_type::ok, "ok");
        co_return;
      },
      get_data{});
  server.set_http_handler<GET, POST>(
      "/check1",
      [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "ok");
      },
      check_t1{}, log_t{});
  server.async_start();

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:9001/");

  auto check = [](auto &result) {
    bool has_str = false;
    for (auto [k, v] : result.resp_headers) {
      if (k == "aaaa") {
        if (v == "bbcc") {
          has_str = true;
        }
        break;
      }
    }
    CHECK(has_str);
  };

  check(result);

  result = client.get("http://127.0.0.1:9001/test_aspect.txt");
  CHECK(result.status == 200);

  result = client.get("http://127.0.0.1:9001/test_file.txt");
  CHECK(result.status == 200);

  result = client.get("http://127.0.0.1:9001/aspect");
  CHECK(result.status == 200);

  result = client.get("http://127.0.0.1:9001/check1");
  CHECK(result.status == 400);
}

TEST_CASE("use out context") {
  auto executor = coro_io::get_global_executor()->get_asio_executor();
  cinatra::coro_http_server server(executor.context(), 9001);
  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/out_ctx", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "use out ctx");
      });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  {
    coro_http_client client1{};
    auto result = client1.get("http://127.0.0.1:9001/out_ctx");
    CHECK(result.status == 200);
    CHECK(result.resp_body == "use out ctx");
  }

  server.stop();
}

TEST_CASE("delay reply, server stop, form-urlencode, qureies, throw") {
  cinatra::coro_http_server server(1, 9001);

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/delay2",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        resp.set_delay(true);
        std::this_thread::sleep_for(200ms);
        resp.set_status_and_content(status_type::ok, "delay reply in coro");
        co_await resp.get_conn()->reply();
      });

  server.set_http_handler<cinatra::POST>(
      "/form-urlencode", [](coro_http_request &req, coro_http_response &resp) {
        CHECK(req.get_body() == "theCityName=58367&aa=%22bbb%22");
        CHECK(req.get_query_value("theCityName") == "58367");
        CHECK(req.get_decode_query_value("aa") == "\"bbb\"");
        CHECK(req.get_decode_query_value("no_such-key").empty());
        CHECK(!req.is_upgrade());
        resp.set_status_and_content(status_type::ok, "form-urlencode");
      });

  server.set_http_handler<cinatra::GET>(
      "/throw", [](coro_http_request &req, coro_http_response &resp) {
        CHECK(req.get_boundary().empty());
        throw std::invalid_argument("invalid arguments");
        resp.set_status_and_content(status_type::ok, "ok");
      });
  server.set_http_handler<cinatra::GET>(
      "/coro_throw",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_boundary().empty());
        throw std::invalid_argument("invalid arguments");
        resp.set_status_and_content(status_type::ok, "ok");
        co_return;
      });
  server.set_http_handler<cinatra::GET>(
      "/throw1", [](coro_http_request &req, coro_http_response &resp) {
        CHECK(req.get_boundary().empty());
        throw 1;
        resp.set_status_and_content(status_type::ok, "ok");
      });
  server.set_http_handler<cinatra::GET>(
      "/coro_throw1",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_boundary().empty());
        throw 1;
        resp.set_status_and_content(status_type::ok, "ok");
        co_return;
      });

  server.async_start();

  resp_data result;
  coro_http_client client1{};
  result = client1.get("http://127.0.0.1:9001/delay2");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "delay reply in coro");

  result = client1.post("http://127.0.0.1:9001/form-urlencode",
                        "theCityName=58367&aa=%22bbb%22",
                        req_content_type::form_url_encode);
  CHECK(result.status == 200);
  CHECK(result.resp_body == "form-urlencode");

  result = client1.get("http://127.0.0.1:9001/throw");
  CHECK(result.status == 503);

  result = client1.get("http://127.0.0.1:9001/coro_throw");
  CHECK(result.status == 503);

  result = client1.get("http://127.0.0.1:9001/throw1");
  CHECK(result.status == 503);

  result = client1.get("http://127.0.0.1:9001/coro_throw1");
  CHECK(result.status == 503);

  server.stop();
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
    co_return read_result{{buf.data(), buf.size()}, file.eof(), ec};
  };

  auto result = co_await client.async_upload_chunked(
      "http://127.0.0.1:9001/chunked"sv, http_method::POST, std::move(fn));
  co_return result;
}

TEST_CASE("chunked request") {
  cinatra::coro_http_server server(1, 9001);
  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/chunked",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        assert(req.get_content_type() == content_type::chunked);
        chunked_result result{};
        std::string content;

        while (true) {
          result = co_await req.get_conn()->read_chunked();
          auto size = req.get_conn()->available();
          if (result.ec) {
            co_return;
          }
          if (result.eof) {
            CHECK(size == 0);
            break;
          }
          CHECK(size >= 0);
          content.append(result.data);
        }

        CINATRA_LOG_DEBUG << "content size: " << content.size() << "\n";
        CINATRA_LOG_DEBUG << content << "\n";
        resp.set_format_type(format_type::chunked);
        resp.set_status_and_content(status_type::ok, "chunked ok");
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
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
  auto r = async_simple::coro::syncAwait(chunked_upload1(client));
  CHECK(r.status == 200);
  CHECK(r.resp_body == "chunked ok");

  auto ss = std::make_shared<std::stringstream>();
  *ss << "hello world";
  auto result = async_simple::coro::syncAwait(client.async_upload_chunked(
      "http://127.0.0.1:9001/chunked"sv, http_method::POST, ss));
  CHECK(result.status == 200);
  CHECK(result.resp_body == "chunked ok");

  result = client.get("http://127.0.0.1:9001/write_chunked");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello world ok");
}

TEST_CASE("test websocket with chunked") {
  int ws_chunk_size = 100;
  cinatra::coro_http_server server(1, 9001);
  server.set_http_handler<cinatra::GET>(
      "/ws_source",
      [ws_chunk_size](coro_http_request &req, coro_http_response &resp)
          -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        std::string out_str;
        websocket_result result{};
        while (!result.eof) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            CINATRA_LOG_DEBUG << "close frame\n";
            CHECK(result.data.empty());
            break;
          }

          CINATRA_LOG_DEBUG << result.data.size() << "\n";

          if (result.data.size() < ws_chunk_size) {
            CHECK(result.data.size() == 24);
            CHECK(result.eof);
          }
          else {
            CHECK(result.data.size() == ws_chunk_size);
            CHECK(!result.eof);
          }
          out_str.append(result.data);

          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            continue;
          }
        }

        CHECK(out_str.size() == 1024);
        CINATRA_LOG_DEBUG << out_str << "\n";
      });
  server.async_start();

  coro_http_client client{};
  async_simple::coro::syncAwait(
      client.connect("ws://127.0.0.1:9001/ws_source"));

  std::string filename = "test.tmp";
  create_file(filename);
  std::ifstream in(filename, std::ios::binary);

  std::string str;
  str.resize(ws_chunk_size);

  auto source_fn = [&]() -> async_simple::coro::Lazy<read_result> {
    size_t size = in.read(str.data(), str.size()).gcount();
    bool eof = in.eof();
    co_return read_result{{str.data(), size}, eof};
  };

  async_simple::coro::syncAwait(
      client.write_websocket(std::move(source_fn), opcode::binary));

  auto data = async_simple::coro::syncAwait(client.read_websocket());
  if (data.net_err) {
    CINATRA_LOG_DEBUG << "ws_msg net error " << data.net_err.message() << "\n";
    return;
  }

  size_t msg_len = data.resp_body.size();

  CINATRA_LOG_DEBUG << "ws msg len: " << msg_len;
  CHECK(!data.resp_body.empty());
  std::this_thread::sleep_for(300ms);
  server.stop();
}

TEST_CASE("test websocket") {
  cinatra::coro_http_server server(1, 8003);
  server.set_http_handler<cinatra::GET>(
      "/ws_echo",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        std::ofstream out_file("test.temp", std::ios::binary);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            out_file.close();
            break;
          }

          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            CINATRA_LOG_DEBUG << "close frame\n";
            out_file.close();
            break;
          }

          if (result.type == ws_frame_type::WS_TEXT_FRAME ||
              result.type == ws_frame_type::WS_BINARY_FRAME) {
            CHECK(!result.data.empty());
            CINATRA_LOG_DEBUG << result.data << "\n";
            out_file << result.data;
          }
          else {
            CINATRA_LOG_DEBUG << result.data << "\n";
            if (result.type == ws_frame_type::WS_PING_FRAME ||
                result.type == ws_frame_type::WS_PONG_FRAME) {
              CINATRA_LOG_DEBUG << "ping or pong msg\n";
              // ping pong frame just need to continue, no need echo anything,
              // because framework has reply ping/pong to client automatically.
              continue;
            }
            else {
              // error frame
              break;
            }
          }

          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            break;
          }
        }
      });
  server.async_start();
  std::this_thread::sleep_for(200ms);  // wait for server handle all messages

  auto lazy = []() -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    auto ret = co_await client.connect("ws://127.0.0.1:8003/ws_echo");
    if (ret.status != 101) {
      CINATRA_LOG_DEBUG << ret.net_err.message() << "\n";
    }
    CHECK(ret.status == 101);
    co_await client.write_websocket(std::string_view("test2fdsaf"),
                                    opcode::binary);
    auto data = co_await client.read_websocket();
    CHECK(data.resp_body == "test2fdsaf");
    co_await client.write_websocket("test_ws");
    data = co_await client.read_websocket();
    CHECK(data.resp_body == "test_ws");
    co_await client.write_websocket("PING", opcode::ping);
    data = co_await client.read_websocket();
    CHECK(data.resp_body == "PING");
    co_await client.write_websocket("", opcode::ping);
    data = co_await client.read_websocket();
    CHECK(data.resp_body == "");
    co_await client.write_websocket("PONG", opcode::pong);
    data = co_await client.read_websocket();
    CHECK(data.resp_body == "PONG");
    co_await client.write_websocket_close("normal close");
    data = co_await client.read_websocket();
    CHECK(data.resp_body == "normal close");
    CHECK(data.net_err == asio::error::eof);
  };

  async_simple::coro::syncAwait(lazy());
}

TEST_CASE("check small ws file") {
  std::string filename = "test.temp";
  std::error_code ec;
  size_t file_size = std::filesystem::file_size(filename, ec);
  if (ec) {
    return;
  }
  std::ifstream file(filename, std::ios::binary);
  if (!file) {
    return;
  }
  std::string str;
  str.resize(file_size);

  file.read(str.data(), str.size());
  CHECK(str == "test2fdsaftest_ws");
  std::filesystem::remove(filename, ec);
}

TEST_CASE("test websocket binary data") {
  cinatra::coro_http_server server(1, 9001);
  server.set_http_handler<cinatra::GET>(
      "/short_binary",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            CINATRA_LOG_DEBUG << "close frame\n";
            CHECK(result.data.empty());
            break;
          }

          if (result.type == ws_frame_type::WS_BINARY_FRAME) {
            CHECK(result.data.size() == 127);
          }
        }
      });
  server.set_http_handler<cinatra::GET>(
      "/medium_binary",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            CINATRA_LOG_DEBUG << "close frame\n";
            CHECK(result.data.empty());
            break;
          }

          if (result.type == ws_frame_type::WS_BINARY_FRAME) {
            CHECK(result.data.size() == 65535);
          }
        }
      });
  server.set_http_handler<cinatra::GET>(
      "/long_binary",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        CHECK(req.get_content_type() == content_type::websocket);
        websocket_result result{};
        while (true) {
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          if (result.type == ws_frame_type::WS_CLOSE_FRAME) {
            CINATRA_LOG_DEBUG << "close frame\n";
            CHECK(result.data.empty());
            break;
          }

          if (result.type == ws_frame_type::WS_BINARY_FRAME) {
            CHECK(result.data.size() == 65536);
          }
        }
      });
  server.async_start();

  auto client1 = std::make_shared<coro_http_client>();
  async_simple::coro::syncAwait(
      client1->connect("ws://127.0.0.1:9001/short_binary"));

  std::string short_str(127, 'A');
  async_simple::coro::syncAwait(
      client1->write_websocket(std::move(short_str), opcode::binary));

  auto client2 = std::make_shared<coro_http_client>();
  async_simple::coro::syncAwait(
      client2->connect("ws://127.0.0.1:9001/medium_binary"));

  std::string medium_str(65535, 'A');
  async_simple::coro::syncAwait(
      client2->write_websocket(std::move(medium_str), opcode::binary));

  auto client3 = std::make_shared<coro_http_client>();
  async_simple::coro::syncAwait(
      client3->connect("ws://127.0.0.1:9001/long_binary"));

  std::string long_str(65536, 'A');
  async_simple::coro::syncAwait(
      client3->write_websocket(std::move(long_str), opcode::binary));

  async_simple::coro::syncAwait(client1->write_websocket_close());
  async_simple::coro::syncAwait(client2->write_websocket_close());
  async_simple::coro::syncAwait(client3->write_websocket_close());
}

TEST_CASE("check connecton timeout") {
  cinatra::coro_http_server server(1, 9001);
  server.set_check_duration(std::chrono::microseconds(600));
  server.set_timeout_duration(std::chrono::microseconds(500));
  server.set_http_handler<cinatra::GET>(
      "/", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "ok");
      });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client;
  client.get("http://127.0.0.1:9001/");

  // wait for timeout, the timeout connections will be removed by server.
  std::this_thread::sleep_for(std::chrono::seconds(1));
  CHECK(server.connection_count() == 0);
}

TEST_CASE("test websocket with different message size") {
  cinatra::coro_http_server server(1, 9008);
  server.set_http_handler<cinatra::GET>(
      "/ws_echo1",
      [](cinatra::coro_http_request &req,
         cinatra::coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        REQUIRE(req.get_content_type() == cinatra::content_type::websocket);
        cinatra::websocket_result result{};

        while (true) {
          req.get_conn()->set_ws_max_size(
              70000);  // default max size 8M, you can control the size.
          result = co_await req.get_conn()->read_websocket();
          if (result.ec) {
            break;
          }

          if (result.type == cinatra::ws_frame_type::WS_CLOSE_FRAME) {
            REQUIRE(result.data.empty());
            break;
          }

          auto ec = co_await req.get_conn()->write_websocket(result.data);
          if (ec) {
            break;
          }
        }
      });
  server.async_start();

  auto lazy = [](std::string str) -> async_simple::coro::Lazy<void> {
    coro_http_client client{};
    auto ret = co_await client.connect("ws://127.0.0.1:9008/ws_echo1");
    if (ret.status != 101) {
      CINATRA_LOG_DEBUG << ret.net_err.message() << "\n";
    }

    CHECK(ret.status == 101);
    co_await client.write_websocket(str);
    auto data = co_await client.read_websocket();
    CHECK(data.status == 200);
    CHECK(data.resp_body.size() == str.size());
    co_await client.write_websocket_close();
    data = co_await client.read_websocket();
    CHECK(data.resp_body.empty());
    CHECK(data.net_err == asio::error::eof);
  };

  SUBCASE("medium message - 16 bit length") {
    std::string medium_message(
        65535, 'x');  // 65,535 'x' characters for the medium message test.

    async_simple::coro::syncAwait(lazy(medium_message));
  }

  SUBCASE("large message - 64 bit length") {
    std::string large_message(
        70000, 'x');  // 70,000 'x' characters for the large message test.

    async_simple::coro::syncAwait(lazy(large_message));
  }

  server.stop();
  CINATRA_LOG_DEBUG << "server stop";
}

#ifdef CINATRA_ENABLE_SSL
TEST_CASE("test ssl server") {
  cinatra::coro_http_server server(1, 9001);
  CINATRA_LOG_DEBUG << std::filesystem::current_path() << "\n";
  server.init_ssl("../openssl_files/server.crt", "../openssl_files/server.key",
                  "test");
  server.set_http_handler<GET, POST>(
      "/ssl", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "ssl");
      });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  [[maybe_unused]] auto r = client.init_ssl();

  auto result = client.get("https://127.0.0.1:9001/ssl");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "ssl");
  CINATRA_LOG_DEBUG << "ssl ok\n";
}
#endif

TEST_CASE("test http download server") {
  cinatra::coro_http_server server(1, 9006);
  std::string filename = "test_download.txt";
  create_file(filename, 1010);

  // curl http://127.0.0.1:9006/download/test_download.txt will download
  // test_download.txt file
  server.set_transfer_chunked_size(100);
  server.set_static_res_dir("download", "");
  server.async_start();
  std::this_thread::sleep_for(200ms);

  {
    coro_http_client client{};
    auto result = async_simple::coro::syncAwait(client.async_download(
        "http://127.0.0.1:9006/download/test_download.txt", "download.txt"));

    CHECK(result.status == 200);
    std::string download_file = fs::absolute("download.txt").string();
    std::ifstream ifs(download_file, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));
    CHECK(content.size() == 1010);
    CHECK(content[0] == 'A');
  }

  {
    coro_http_client client{};
    auto result = async_simple::coro::syncAwait(client.async_download(
        "http://127.0.0.1:9006/download/test_download.txt", "download1.txt",
        "0-"));

    CHECK(result.status == 200);
    std::string download_file = fs::absolute("download1.txt").string();
    std::ifstream ifs(download_file, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));
    CHECK(content.size() == 1010);
    CHECK(content[0] == 'A');
  }
}

TEST_CASE("test restful api") {
  cinatra::coro_http_server server(1, 9001);

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/test2/{}/test3/{}",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          // coroutine in other thread.
          CHECK(req.matches_.str(1) == "name");
          CHECK(req.matches_.str(2) == "test");
          resp.set_status_and_content(cinatra::status_type::ok, "hello world");
        });
        co_return;
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      R"(/numbers/(\d+)/test/(\d+))",
      [](coro_http_request &req, coro_http_response &response) {
        CHECK(req.matches_.str(1) == "100");
        CHECK(req.matches_.str(2) == "200");
        response.set_status_and_content(status_type::ok, "number regex ok");
      });
  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/test4/{}", [](coro_http_request &req, coro_http_response &response) {
        CHECK(req.matches_.str(1) == "100");
        response.set_status_and_content(status_type::ok, "number regex ok");
      });

  server.async_start();

  coro_http_client client;
  auto result = client.get("http://127.0.0.1:9001/test2/name/test3/test");
  result = client.get("http://127.0.0.1:9001/numbers/100/test/200");
  result = client.get("http://127.0.0.1:9001/test4/100");
  CHECK(result.status == 200);
}

TEST_CASE("test response standalone") {
  coro_http_response resp(nullptr);
  resp.set_status_and_content(status_type::ok, "ok");
  CHECK(resp.content() == "ok");
  CHECK(resp.content_size() == 2);
  CHECK(resp.need_date());
  resp.need_date_head(false);
  CHECK(!resp.need_date());

  std::string str;
  resp.build_resp_str(str);
  CHECK(!str.empty());
  CHECK(str.find("200") != std::string::npos);
  resp.clear();
  str.clear();

  resp.set_status_and_content(status_type::ok, "");
  std::vector<http_header> v{{"hello", "world"}};
  resp.add_header_span(v);
  resp.build_resp_str(str);
  CHECK(str.find("200") != std::string::npos);
  resp.clear();
  str.clear();

  resp.set_keepalive(true);
  resp.build_resp_str(str);
  CHECK(str.find("501") != std::string::npos);
  resp.set_format_type(format_type::chunked);
  resp.build_resp_str(str);
  CHECK(str.find("501") != std::string::npos);
  resp.clear();
  str.clear();

  std::string_view out = "hello";
  resp.set_status_and_content_view(status_type::ok, out);
  resp.build_resp_str(str);
  CHECK(str.find("200") != std::string::npos);

  std::vector<asio::const_buffer> buffers;
  resp.set_content_type<4>();
  resp.build_resp_head(buffers);
  CHECK(buffers.size() == 13);
}

TEST_CASE("test radix tree restful api") {
  cinatra::coro_http_server server(1, 9001);

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/", [](coro_http_request &req, coro_http_response &response) {
        response.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/user/:id", [](coro_http_request &req, coro_http_response &response) {
        CHECK(req.params_["id"] == "cinatra");
        response.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/user/:id/subscriptions",
      [](coro_http_request &req, coro_http_response &response) {
        CHECK(req.params_["id"] == "subid");
        response.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/users/:userid/subscriptions/:subid",
      [](coro_http_request &req, coro_http_response &response) {
        CHECK(req.params_["userid"] == "ultramarines");
        CHECK(req.params_["subid"] == "guilliman");
        response.set_status_and_content(status_type::ok, "ok");
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/values/:x/:y/:z",
      [](coro_http_request &req, coro_http_response &response) {
        CHECK(req.params_["x"] == "guilliman");
        CHECK(req.params_["y"] == "cawl");
        CHECK(req.params_["z"] == "yvraine");
        response.set_status_and_content(status_type::ok, "ok");
      });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client;
  client.get("http://127.0.0.1:9001/user/cinatra");
  client.get("http://127.0.0.1:9001/user/subid/subscriptions");
  client.get("http://127.0.0.1:9001/user/ultramarines/subscriptions/guilliman");
  client.get("http://127.0.0.1:9001/value/guilliman/cawl/yvraine");

  client.post("http://127.0.0.1:9001/user/cinatra", "hello",
              req_content_type::string);
  client.post("http://127.0.0.1:9001/user/subid/subscriptions", "hello",
              req_content_type::string);
  client.post("http://127.0.0.1:9001/user/ultramarines/subscriptions/guilliman",
              "hello", req_content_type::string);
  client.post("http://127.0.0.1:9001/value/guilliman/cawl/yvraine", "hello",
              req_content_type::string);
}

TEST_CASE("test coro radix tree restful api") {
  cinatra::coro_http_server server(1, 9001);

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          response.set_status_and_content(status_type::ok, "ok");
        });
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/user/:id",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          CHECK(req.params_["id"] == "cinatra");
          response.set_status_and_content(status_type::ok, "ok");
        });
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/user/:id/subscriptions",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&] {
          CHECK(req.params_["id"] == "subid");
          response.set_status_and_content(status_type::ok, "ok");
        });
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/users/:userid/subscriptions/:subid",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&] {
          CHECK(req.params_["userid"] == "ultramarines");
          CHECK(req.params_["subid"] == "guilliman");
          response.set_status_and_content(status_type::ok, "ok");
        });
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/values/:x/:y/:z",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&] {
          CHECK(req.params_["x"] == "guilliman");
          CHECK(req.params_["y"] == "cawl");
          CHECK(req.params_["z"] == "yvraine");
          response.set_status_and_content(status_type::ok, "ok");
        });
      });

  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/ai/robot/:messages",
      [](coro_http_request &req,
         coro_http_response &response) -> async_simple::coro::Lazy<void> {
        co_await coro_io::post([&]() {
          CHECK(req.params_["messages"] == "android");
          response.set_status_and_content(status_type::ok, "ok");
        });
      });

  server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client;
  client.get("http://127.0.0.1:9001/user/cinatra");
  client.get("http://127.0.0.1:9001/user/subid/subscriptions");
  client.get("http://127.0.0.1:9001/user/ultramarines/subscriptions/guilliman");
  client.get("http://127.0.0.1:9001/value/guilliman/cawl/yvraine");

  client.post("http://127.0.0.1:9001/user/cinatra", "hello",
              req_content_type::string);
  client.post("http://127.0.0.1:9001/user/subid/subscriptions", "hello",
              req_content_type::string);
  client.post("http://127.0.0.1:9001/user/ultramarines/subscriptions/guilliman",
              "hello", req_content_type::string);
  client.post("http://127.0.0.1:9001/value/guilliman/cawl/yvraine", "hello",
              req_content_type::string);
  client.post("http://127.0.0.1:9001/ai/robot/android", "hello",
              req_content_type::string);
}

TEST_CASE("test reverse proxy") {
  SUBCASE(
      "exception tests: empty hosts, empty weights test or count not equal") {
    cinatra::coro_http_server server(1, 9002);
    CHECK_THROWS_AS(server.set_http_proxy_handler<cinatra::http_method::GET>(
                        "/", {}, coro_io::load_balance_algorithm::WRR, {2, 1}),
                    std::invalid_argument);

    CHECK_THROWS_AS(server.set_http_proxy_handler<cinatra::http_method::GET>(
                        "/", {"127.0.0.1:8801", "127.0.0.1:8802"},
                        coro_io::load_balance_algorithm::WRR),
                    std::invalid_argument);

    CHECK_THROWS_AS(server.set_http_proxy_handler<cinatra::http_method::GET>(
                        "/", {"127.0.0.1:8801", "127.0.0.1:8802"},
                        coro_io::load_balance_algorithm::WRR, {1}),
                    std::invalid_argument);

    CHECK_THROWS_AS(
        server.set_http_proxy_handler<cinatra::http_method::GET>("/", {}),
        std::invalid_argument);
  }

  cinatra::coro_http_server web_one(1, 9004);

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
      "/", {"127.0.0.1:9004", "127.0.0.1:9002", "127.0.0.1:9003"},
      coro_io::load_balance_algorithm::WRR, {10, 5, 5}, log_t{}, check_t{});

  coro_http_server proxy_rr(2, 8091);
  proxy_rr.set_http_proxy_handler<GET, POST>(
      "/", {"127.0.0.1:9004", "127.0.0.1:9002", "127.0.0.1:9003"},
      coro_io::load_balance_algorithm::RR, {}, log_t{});

  coro_http_server proxy_random(2, 8092);
  proxy_random.set_http_proxy_handler<GET, POST>(
      "/", {"127.0.0.1:9004", "127.0.0.1:9002", "127.0.0.1:9003"});

  coro_http_server proxy_all(2, 8093);
  proxy_all.set_http_proxy_handler(
      "/", {"127.0.0.1:9004", "127.0.0.1:9002", "127.0.0.1:9003"});

  proxy_wrr.async_start();
  proxy_rr.async_start();
  proxy_random.async_start();
  proxy_all.async_start();

  std::this_thread::sleep_for(200ms);

  coro_http_client client_rr;
  resp_data resp_rr = client_rr.get("http://127.0.0.1:8091/");
  CHECK(resp_rr.resp_body == "web1");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  CHECK(resp_rr.resp_body == "web2");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  CHECK(resp_rr.resp_body == "web3");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  CHECK(resp_rr.resp_body == "web1");
  resp_rr = client_rr.get("http://127.0.0.1:8091/");
  CHECK(resp_rr.resp_body == "web2");
  resp_rr = client_rr.post("http://127.0.0.1:8091/", "test content",
                           req_content_type::text);
  CHECK(resp_rr.resp_body == "web3");

  coro_http_client client_wrr;
  resp_data resp = client_wrr.get("http://127.0.0.1:8090/");
  CHECK(resp.resp_body == "web1");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  CHECK(resp.resp_body == "web1");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  CHECK(resp.resp_body == "web2");
  resp = client_wrr.get("http://127.0.0.1:8090/");
  CHECK(resp.resp_body == "web3");

  coro_http_client client_random;
  resp_data resp_random = client_random.get("http://127.0.0.1:8092/");
  CINATRA_LOG_DEBUG << resp_random.resp_body << "\n";
  CHECK(!resp_random.resp_body.empty());

  coro_http_client client_all;
  resp_random = client_all.post("http://127.0.0.1:8093/", "test content",
                                req_content_type::text);
  CINATRA_LOG_DEBUG << resp_random.resp_body << "\n";
  CHECK(!resp_random.resp_body.empty());
}

TEST_CASE("test reverse proxy download") {
  cinatra::coro_http_server server(1, 9001);
  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/test_chunked",
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
  server.set_http_handler<cinatra::GET, cinatra::POST>(
      "/test", [](coro_http_request &req, coro_http_response &resp) {
        resp.set_status_and_content(status_type::ok, "hello world");
      });
  server.set_http_handler<GET>(
      "/test_multipart",
      [](coro_http_request &req,
         coro_http_response &resp) -> async_simple::coro::Lazy<void> {
        bool ok;
        if (ok = co_await resp.get_conn()->begin_multipart(); !ok) {
          co_return;
        }

        std::vector<std::string> vec{"hello", " world", " multipart"};

        for (auto &str : vec) {
          if (ok = co_await resp.get_conn()->write_multipart(str, "text/plain");
              !ok) {
            co_return;
          }
        }

        ok = co_await resp.get_conn()->end_multipart();
      });
  server.async_start();

  coro_http_server proxy_rr(2, 8001);
  proxy_rr.set_http_proxy_handler<GET, POST>(
      "/([^]+)", {"127.0.0.1:9001"}, coro_io::load_balance_algorithm::RR);
  proxy_rr.async_start();

  coro_http_client client{};
  auto result = client.get("http://127.0.0.1:8001/test");
  CHECK(result.resp_body == "hello world");

  result = client.get("http://127.0.0.1:8001/test_chunked");
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello world ok");

  coro_http_client client1{};
  result = client1.get("http://127.0.0.1:8001/test_multipart");
  CINATRA_LOG_DEBUG << result.net_err.message();
  CHECK(result.status == 200);
  CHECK(result.resp_body == "hello world multipart");
}

TEST_CASE("test reverse proxy websocket") {
  {
    coro_http_server proxy_server(1, 9005);
    CHECK_THROWS_AS(proxy_server.set_websocket_proxy_handler("/ws_echo", {}),
                    std::invalid_argument);
  }
  coro_http_server server(1, 9005);
  server.set_http_handler<cinatra::GET>(
      "/ws_echo",
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

  coro_http_server proxy_server(1, 9002);
  proxy_server.set_websocket_proxy_handler("/ws_echo",
                                           {"ws://127.0.0.1:9005/ws_echo"});
  proxy_server.async_start();
  std::this_thread::sleep_for(200ms);

  coro_http_client client{};
  auto r = async_simple::coro::syncAwait(
      client.connect("ws://127.0.0.1:9002/ws_echo"));
  CHECK(!r.net_err);
  for (int i = 0; i < 10; i++) {
    async_simple::coro::syncAwait(client.write_websocket("test websocket"));
    auto data = async_simple::coro::syncAwait(client.read_websocket());
    CINATRA_LOG_DEBUG << data.resp_body << "\n";
    CHECK(data.resp_body == "test websocket");
  }
}
