#include <infiniband/verbs.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

#include "asio/ip/address.hpp"
#include "asio/ip/address_v4.hpp"
#include "asio/ip/tcp.hpp"
#include "async_simple/Signal.h"
#include "async_simple/coro/Collect.h"
#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/Sleep.h"
#include "async_simple/coro/SyncAwait.h"
#include "doctest.h"
#include "iguana/json_reader.hpp"
#include "iguana/json_writer.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/ibverbs/ib_buffer.hpp"
#include "ylt/coro_io/ibverbs/ib_io.hpp"
#include "ylt/coro_io/ibverbs/ib_socket.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/easylog.hpp"
#include "ylt/easylog/record.hpp"
#include "ylt/reflection/user_reflect_macro.hpp"
#include "ylt/standalone/iguana/prettify.hpp"
#include "ylt/struct_json/json_reader.h"
#include "ylt/struct_json/json_writer.h"
struct config_t {
  std::size_t buffer_size = 256 * 1024;
  std::size_t request_size = 20 * 1024 * 1024 + 1;
  int concurrency = 50;
  int test_type = 0;
  int enable_log = 0;
  bool enable_server = true;
  std::string enable_client = "127.0.0.1";
  int port = 58110;
  int test_time = 10;
};
YLT_REFL(config_t, buffer_size, request_size, concurrency, test_type,
         enable_log, enable_server, enable_client, port, test_time);

config_t config;
std::shared_ptr<coro_io::ib_device_t> g_dev;
std::atomic<uint64_t> cnt[2];

std::atomic<uint64_t> connect_cnt;

using namespace std::chrono_literals;
using namespace std::literals;
async_simple::coro::Lazy<std::error_code> echo_connect(
    coro_io::ib_socket_t soc) {
  std::string buffer;
  buffer.resize(100);
  ELOG_INFO << "start echo connect";
  ELOG_INFO << "start read from client";
  auto [ec, len] =
      co_await coro_io::async_read(soc, std::string_view{buffer.data(), 8});
  if (ec || len != 8) [[unlikely]] {
    ELOG_INFO << "err when read client:" << ec.message();
    co_return ec;
  }
  uint64_t sz = *(uint64_t *)buffer.data();
  buffer.resize(sz);
  std::tie(ec, len) =
      co_await coro_io::async_read(soc, std::string_view{buffer});
  if (ec) [[unlikely]] {
    co_return ec;
  }
  else if (len != sz) {
    co_return std::make_error_code(std::errc::protocol_error);
  }
  ELOG_INFO << "read data ok:" << len;
  char ch = 'A';
  auto s_view = std::string_view{&ch, 1};
  while (true) {
    ELOG_DEBUG << "start read from client" << &soc;
    auto [r, s] = co_await async_simple::coro::collectAll(
        coro_io::async_read(soc, std::string_view{buffer.data(), 8}),
        coro_io::async_write(soc, s_view));
    ELOG_DEBUG << "server waiting io for r/w from client over" << &soc;

    if (r.hasError() || s.hasError()) [[unlikely]] {
      co_return std::make_error_code(std::errc::io_error);
    }
    if (r.value().first) [[unlikely]] {
      co_return r.value().first;
    }
    if (s.value().first) [[unlikely]] {
      co_return s.value().first;
    }
    uint64_t sz = *(uint64_t *)buffer.data();
    buffer.resize(sz);
    auto [ec, len] =
        co_await coro_io::async_read(soc, std::string_view{buffer});
    if (ec) [[unlikely]] {
      co_return r.value().first;
    }
    else if (len != sz) {
      co_return std::make_error_code(std::errc::protocol_error);
    }
    cnt[0] += len;
    ELOG_DEBUG << "read data ok:" << len;
  }
  co_return std::error_code{};
}

ibv_sge make_sge(ibv_mr &mr) {
  ibv_sge sge{};
  sge.addr = (uint64_t)mr.addr;
  sge.length = mr.length;
  sge.lkey = mr.lkey;
  return sge;
}

async_simple::coro::Lazy<std::error_code> echo_connect_read_some(
    coro_io::ib_socket_t soc) {
  char *buffer = new char[config.buffer_size];
  ELOG_INFO << "start echo connect";
  auto ib = coro_io::ib_buffer_t::regist(*soc.get_device(), buffer,
                                         config.buffer_size);
  if (!ib) {
    co_return std::make_error_code(std::errc::no_buffer_space);
  }
  ELOG_INFO << "start read from client";
  auto [ec, len] = co_await coro_io::async_read_some(soc, make_sge(*ib));

  if (ec) [[unlikely]] {
    ELOG_INFO << "err when read client:" << ec.message();
    co_return ec;
  }
  ELOG_INFO << "read data ok:" << len;
  while (true) {
    auto r_view = make_sge(*ib);
    auto s_view = make_sge(*ib);
    s_view.length = 1;

    ELOG_DEBUG << "start read from client" << &soc;
    auto [r, s] = co_await async_simple::coro::collectAll(
        coro_io::async_read_some(soc, r_view),
        coro_io::async_write(soc, s_view));
    ELOG_DEBUG << "server waiting io for r/w from client over" << &soc;

    if (r.hasError() || s.hasError()) [[unlikely]] {
      co_return std::make_error_code(std::errc::io_error);
    }
    if (s.value().first) [[unlikely]] {
      co_return r.value().first;
    }
    if (s.value().first) [[unlikely]] {
      co_return s.value().first;
    }
    len = r.value().second;
    cnt[0] += len;
    ELOG_DEBUG << "read data ok:" << len;
  }
  delete[] buffer;
  co_return std::error_code{};
}

async_simple::coro::Lazy<std::error_code> echo_accept() {
  asio::ip::tcp::acceptor acceptor(
      coro_io::get_global_executor()->get_asio_executor());
  std::error_code ec;
  auto address = asio::ip::address_v4::from_string("0.0.0.0", ec);
  if (ec) [[unlikely]] {
    co_return ec;
  }
  auto endpoint = asio::ip::tcp::endpoint(address, config.port);
  acceptor.open(endpoint.protocol(), ec);
  if (ec) [[unlikely]] {
    co_return ec;
  }
  acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true), ec);
  if (ec) [[unlikely]] {
    co_return ec;
  }
  acceptor.bind(endpoint, ec);
  if (ec) [[unlikely]] {
    co_return ec;
  }
  acceptor.listen(asio::ip::tcp::socket::max_listen_connections, ec);
  if (ec) [[unlikely]] {
    co_return ec;
  }

  ELOG_INFO << "tcp listening";
  while (true) {
    coro_io::ib_socket_t soc(coro_io::get_global_executor(), {.device = g_dev});
    auto ec = co_await coro_io::async_accept(acceptor, soc);

    if (ec) [[unlikely]] {
      ELOG_INFO << "accept failed";
      co_return ec;
    }

    ELOG_INFO << "start new connection";
    auto executor = soc.get_executor();
    ++connect_cnt;
    if (config.test_type == 0) {
      echo_connect(std::move(soc)).start([](auto &&) {
        --connect_cnt;
      });
    }
    else {
      echo_connect_read_some(std::move(soc)).start([](auto &&) {
        --connect_cnt;
      });
    }
  }
  co_return ec;
}

async_simple::coro::Lazy<std::error_code> echo_client(
    coro_io::ib_socket_t &soc, std::string_view send_view) {
  std::string recv_buffer;
  recv_buffer.resize(1);
  std::string_view recv_view = std::string_view{recv_buffer};
  ELOG_INFO << "start echo";
  while (true) {
    auto [result, time_out] = co_await async_simple::coro::collectAll<
        async_simple::SignalType::Terminate>(
        async_simple::coro::collectAll(coro_io::async_read(soc, recv_view),
                                       coro_io::async_write(soc, send_view)),
        coro_io::sleep_for(10s, soc.get_coro_executor()));

    if (result.hasError() || time_out.hasError()) [[unlikely]] {
      co_return std::make_error_code(std::errc::io_error);
    }
    if (time_out.value()) [[unlikely]] {
      ELOG_WARN << "Time out!";
      co_return std::make_error_code(std::errc::timed_out);
    }
    auto &&[r, s] = result.value();
    if (r.hasError() || s.hasError()) [[unlikely]] {
      co_return std::make_error_code(std::errc::io_error);
    }
    if (s.value().first) [[unlikely]] {
      co_return r.value().first;
    }
    if (s.value().first) [[unlikely]] {
      co_return s.value().first;
    }
    if (s.value().second != send_view.size() || recv_view != "A") [[unlikely]] {
      ELOG_ERROR << "data err";
      co_return std::make_error_code(std::errc::protocol_error);
    }
    cnt[1] += send_view.size();
  }
  co_return std::error_code{};
}

async_simple::coro::Lazy<std::error_code> echo_client_read_some(
    coro_io::ib_socket_t &soc, std::string_view sv) {
  std::string buffer;
  buffer.resize(config.buffer_size);
  auto ib2 = coro_io::ib_buffer_t::regist(*soc.get_device(), buffer.data(),
                                          config.buffer_size);
  auto ib = coro_io::ib_buffer_t::regist(*soc.get_device(), (char *)sv.data(),
                                         sv.size());
  if (!ib || !ib2) {
    co_return std::make_error_code(std::errc::no_buffer_space);
  }
  ELOG_INFO << "start echo";
  while (true) {
    ibv_sge r_view = make_sge(*ib2);
    ibv_sge s_view = make_sge(*ib);

    auto [result, time_out] = co_await async_simple::coro::collectAll<
        async_simple::SignalType::Terminate>(
        async_simple::coro::collectAll(coro_io::async_read_some(soc, r_view),
                                       coro_io::async_write(soc, s_view)),
        coro_io::sleep_for(std::chrono::seconds{10}));

    if (result.hasError() || time_out.hasError()) [[unlikely]] {
      co_return std::make_error_code(std::errc::io_error);
    }
    if (time_out.value()) [[unlikely]] {
      ELOG_WARN << "Time out!";
      co_return std::make_error_code(std::errc::timed_out);
    }
    auto &&[r, s] = result.value();
    if (r.hasError() || s.hasError()) [[unlikely]] {
      co_return std::make_error_code(std::errc::io_error);
    }
    if (r.value().first) [[unlikely]] {
      co_return r.value().first;
    }
    if (s.value().first) [[unlikely]] {
      co_return s.value().first;
    }
    auto resp = std::string_view{(char *)r_view.addr, r.value().second};
    if (resp != "A" || s.value().second != sv.size()) [[unlikely]] {
      ELOG_ERROR << "data err";
      co_return std::make_error_code(std::errc::protocol_error);
    }
    else {
      cnt[1] += s_view.length;
    }
  }
  co_return std::error_code{};
}

async_simple::coro::Lazy<std::error_code> echo_connect() {
  coro_io::ib_socket_t soc(coro_io::get_global_executor(), {.device = g_dev});
  auto ec = co_await coro_io::async_connect(soc, config.enable_client,
                                            std::to_string(config.port));
  if (ec) [[unlikely]] {
    co_return ec;
  }
  ELOG_INFO << "connect over";
  std::string s;
  s.resize(config.request_size, 'A');
  uint64_t &sz = *(uint64_t *)s.data();
  sz = config.request_size - 8;
  ++connect_cnt;
  if (config.test_type == 0) {
    ec = co_await echo_client(soc, s);
  }
  else {
    ec = co_await echo_client_read_some(soc, s);
  }
  --connect_cnt;
  co_return ec;
}
TEST_CASE("ib socket pressure test") {
  try {
    struct_json::from_json_file(config, "ib_bench_config.json");
  } catch (...) {
    std::ofstream fs;
    fs.open("ib_bench_config.json");
    std::string s;
    struct_json::to_json(config, s);
    fs << iguana::prettify(s);
  }
  auto old_s = easylog::logger<>::instance().get_min_severity();
  g_dev = coro_io::ib_device_t::create(
      {.buffer_pool_config = {.buffer_size = config.buffer_size}});
  easylog::Severity s;
  if (config.enable_log) {
    s = easylog::Severity::TRACE;
  }
  else {
    s = easylog::Severity::WARN;
  }
  easylog::logger<>::instance().set_min_severity(s);

  ELOG_INFO << "start echo server & client";
  if (config.enable_server) {
    echo_accept().start([](auto &&ec) {
      ELOG_ERROR << ec.value().message();
    });
  }
  if (config.enable_client.size()) {
    for (int i = 0; i < config.concurrency; ++i)
      echo_connect().start([](auto &&ec) {
        ELOG_ERROR << ec.value().message();
      });
  }
  std::atomic<uint64_t> *cnt_p;
  if (config.enable_server) {
    cnt_p = &cnt[0];
  }
  else {
    cnt_p = &cnt[1];
  }
  for (int i = 0; i < config.test_time; ++i) {
    std::this_thread::sleep_for(std::chrono::seconds{1});
    auto c = cnt_p->exchange(0);
    std::cout << "Throughput:" << 8.0 * c / 1000'000'000
              << " Gb/s, alive connection:" << connect_cnt << std::endl;
  }
  // easylog::logger<>::instance().set_min_severity(old_s);
}