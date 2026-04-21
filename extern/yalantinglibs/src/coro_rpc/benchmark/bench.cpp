#include <atomic>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <system_error>
#include <vector>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/metric/summary.hpp>

#include "async_simple/Future.h"
#include "async_simple/Signal.h"
#include "async_simple/coro/Collect.h"
#include "async_simple/coro/Lazy.h"
#include "cmdline.h"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp"
#include "ylt/util/tl/expected.hpp"

struct bench_config {
  std::string url;
  uint32_t client_concurrency;
  size_t send_data_len;
  size_t max_request_count;
  unsigned short port;
  size_t resp_len;
  uint32_t buffer_size;
  int log_level;
  bool enable_ib;
  uint32_t duration;
  uint32_t min_recv_buf_count;
  uint32_t max_recv_buf_count;
  uint32_t send_buffer_cnt;
  bool use_client_pool;
  bool reuse_client_pool;
};

bench_config init_conf(const cmdline::parser& parser) {
  bench_config conf{};
  conf.url = parser.get<std::string>("url");
  conf.client_concurrency = parser.get<uint32_t>("client_concurrency");
  conf.max_request_count = parser.get<size_t>("max_request_count");
  conf.resp_len = parser.get<size_t>("resp_len");
  conf.send_data_len = parser.get<size_t>("send_data_len");
  conf.port = parser.get<unsigned short>("port");
  conf.buffer_size = parser.get<uint32_t>("buffer_size");
  conf.log_level = parser.get<int>("log_level");
  conf.enable_ib = parser.get<bool>("enable_ib");
  conf.duration = parser.get<uint32_t>("duration");
  conf.min_recv_buf_count = parser.get<uint32_t>("min_recv_buf_count");
  conf.max_recv_buf_count = parser.get<uint32_t>("max_recv_buf_count");
  conf.send_buffer_cnt = parser.get<uint32_t>("send_buffer_cnt");
  conf.use_client_pool = parser.get<bool>("use_client_pool");
  conf.reuse_client_pool = parser.get<bool>("reuse_client_pool");

  if (conf.client_concurrency == 0) {
    ELOG_WARN << "port: " << conf.port << ", "
              << "buffer_size: " << conf.buffer_size << ", "
              << "resp_len: " << conf.resp_len << ", "
              << "test duration: " << conf.duration << "s, "
              << "enable ibverbs: " << conf.enable_ib << ", "
              << "log level: " << conf.log_level << ", ";
  }
  else {
    ELOG_WARN << "url: " << conf.url << ", "
              << "reuse_client_pool: " << conf.reuse_client_pool << ", "
              << "use_client_pool: " << conf.use_client_pool << ", "
              << "buffer_size: " << conf.buffer_size << ", "
              << "client concurrency: " << conf.client_concurrency << ", "
              << "send data_len: " << conf.send_data_len << ", "
              << "max_request_count: " << conf.max_request_count << ", "
              << "enable ibverbs: " << conf.enable_ib << ", "
              << "log level: " << conf.log_level << ", ";
  }
  ELOG_WARN << "min_recv_buf_count: " << conf.min_recv_buf_count
            << ", max_recv_buf_count: " << conf.max_recv_buf_count
            << ", send_buffer_cnt: " << conf.send_buffer_cnt;
  return conf;
}

size_t g_resp_len = 0;
std::string_view g_resp_str;
std::atomic<size_t> g_throughput_count = 0;
std::atomic<size_t> g_qps_count = 0;

inline std::string_view echo() {
  auto str = coro_rpc::get_context()->get_request_attachment();
  if (g_resp_len == 0) {
    return str;
  }
  coro_rpc::get_context()->set_complete_handler(
      [sz = str.size()](std::error_code ec, std::size_t) {
        if (!ec) {
          g_throughput_count.fetch_add(sz, std::memory_order::relaxed);
          g_qps_count.fetch_add(1, std::memory_order_relaxed);
        }
      });
  return g_resp_str;
}

ylt::metric::summary_t g_latency{"Latency(us) of rpc call", "help",
                                 std::vector{0.5, 0.9, 0.95, 0.99},
                                 std::chrono::seconds{60}};

async_simple::coro::Lazy<void> watcher(const bench_config& conf) {
  size_t total = 0;
  size_t total_qps = 0;
  std::cout << std::fixed << std::setprecision(2);
  g_throughput_count = 0;
  auto test_time = conf.duration;
  if (conf.client_concurrency == 0) {
    test_time = UINT32_MAX;
  }
  for (uint32_t i = 0; i < test_time; i++) {
    auto start = std::chrono::system_clock::now();
    auto is_ok = co_await coro_io::sleep_for(std::chrono::seconds{1});
    if (!is_ok) {
      break;
    }
    auto thp = g_throughput_count.exchange(0);
    if (thp == 0) {
      continue;
    }
    total += thp;
    auto qps = g_qps_count.exchange(0);
    total_qps += qps;
    auto end = std::chrono::system_clock::now();
    auto dur = (end - start) / std::chrono::milliseconds(1);
    double thp_MB = (8.0 * thp * 1000) / (1000'000'000ll * dur);
    if (conf.client_concurrency != 0) {
      std::cout << "qps: " << qps << ",";
    }
    std::cout << "Throughput: " << thp_MB << " Gb/s ";
#ifdef YLT_ENABLE_IBV
    if (conf.enable_ib) {
      std::cout << "ibv mem usage: "
                << coro_io::get_global_ib_device()
                           ->get_buffer_pool()
                           ->memory_usage() /
                       (1.0 * 1024 * 1024)
                << "MB, max ibv mem usage: "
                << coro_io::get_global_ib_device()
                           ->get_buffer_pool()
                           ->max_recorded_memory_usage() /
                       (1.0 * 1024 * 1024)
                << "MB, free buffer cnt: "
                << coro_io::get_global_ib_device()
                       ->get_buffer_pool()
                       ->free_buffer_size();
    }
#endif
    std::cout << std::endl;
  }

  std::cout << "# Benchmark result \n";
  double avg_thp = (8.0 * total) / (1000'000'000ll * conf.duration);
  std::cout << "avg qps: " << total_qps / (conf.duration)
            << " and throughput: " << avg_thp << " Gb/s in duration "
            << conf.duration << "\n";
#ifdef YLT_ENABLE_IBV
  if (conf.enable_ib) {
    std::cout << "history max ibv memory usage:"
              << coro_io::get_global_ib_device()
                         ->get_buffer_pool()
                         ->max_recorded_memory_usage() /
                     (1.0 * 1024 * 1024)
              << "MB\n";
  }
#endif
  std::string str_rate;
  g_latency.serialize(str_rate);
  std::cout << str_rate << std::endl;
  co_return;
}

async_simple::coro::Lazy<std::error_code> request(const bench_config& conf) {
  ELOG_INFO << "bench_config buffer size " << conf.buffer_size;
  coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
  pool_conf.max_connection = 1024;
#ifdef YLT_ENABLE_IBV
  if (conf.enable_ib) {
    coro_io::ib_socket_t::config_t ib_conf{};
    ib_conf.send_buffer_cnt = conf.send_buffer_cnt;
    ib_conf.recv_buffer_cnt = conf.min_recv_buf_count;
    ib_conf.cap.max_recv_wr = conf.max_recv_buf_count;
    pool_conf.client_config.socket_config = ib_conf;
  }
#endif

  auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(
      conf.url, pool_conf);
  auto lazy = [pool, conf]() -> async_simple::coro::Lazy<void> {
    std::string send_str(conf.send_data_len, 'A');
    std::string_view send_str_view(send_str);
    for (size_t i = 0; i < conf.max_request_count; i++) {
      auto start = std::chrono::steady_clock::now();
      auto ec =
          co_await pool->send_request([&](coro_rpc::coro_rpc_client& client)
                                          -> async_simple::coro::Lazy<bool> {
            client.set_req_attachment(send_str_view);
            auto result = co_await client.call<echo>();
            if (!result.has_value()) {
              ELOG_WARN << result.error().msg;
              co_return false;
            }
            co_return true;
          });
      if (!ec.has_value()) {
        std::cout << "BREAK" << std::endl;
        break;
      }
      if (ec.value()) {
        auto now = std::chrono::steady_clock::now();
        g_latency.observe(
            std::chrono::duration_cast<std::chrono::microseconds>(now - start)
                .count());
        g_throughput_count.fetch_add(send_str_view.length(),
                                     std::memory_order_relaxed);
        g_qps_count.fetch_add(1, std::memory_order_relaxed);
      }
    }
  };
  std::vector<async_simple::coro::Lazy<void>> works;
  works.reserve(conf.client_concurrency);
  for (size_t i = 0; i < conf.client_concurrency; i++) {
    works.push_back(lazy());
  }
  co_await async_simple::coro::collectAll<async_simple::SignalType::Terminate>(
      async_simple::coro::collectAll(std::move(works)), watcher(conf));

  co_return std::error_code{};
}

async_simple::coro::Lazy<std::error_code> request_with_reuse(
    const bench_config& conf) {
  ELOG_INFO << "bench_config buffer size " << conf.buffer_size;
  coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
  pool_conf.max_connection = 1024;
#ifdef YLT_ENABLE_IBV
  if (conf.enable_ib) {
    coro_io::ib_socket_t::config_t ib_conf{};
    ib_conf.send_buffer_cnt = conf.send_buffer_cnt;
    ib_conf.recv_buffer_cnt = conf.min_recv_buf_count;
    ib_conf.cap.max_recv_wr = conf.max_recv_buf_count;
    pool_conf.client_config.socket_config = ib_conf;
  }
#endif

  auto pool = coro_io::client_pool<coro_rpc::coro_rpc_client>::create(
      conf.url, pool_conf);
  std::atomic<uint64_t> cnter;
  auto lazy = [pool, conf, &cnter]() -> async_simple::coro::Lazy<void> {
    std::string send_str(conf.send_data_len, 'A');
    std::string_view send_str_view(send_str);
    for (size_t i = 0; i < conf.max_request_count; i++) {
      auto start = std::chrono::steady_clock::now();
      auto ret = co_await pool->send_request(
          [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
            return client.send_request_with_attachment<echo>(send_str_view);
          });
      if (ret.has_value()) {
        auto result = co_await std::move(ret.value());
        if (result.has_value()) {
          auto now = std::chrono::steady_clock::now();
          g_latency.observe(
              std::chrono::duration_cast<std::chrono::microseconds>(now - start)
                  .count());
          g_throughput_count.fetch_add(send_str_view.length(),
                                       std::memory_order_relaxed);
          g_qps_count.fetch_add(1, std::memory_order_relaxed);
        }
      }
      else {
        break;
      }
    }
  };
  std::vector<async_simple::coro::Lazy<void>> works;
  works.reserve(conf.client_concurrency);
  for (size_t i = 0; i < conf.client_concurrency; i++) {
    works.push_back(lazy());
  }
  co_await async_simple::coro::collectAll<async_simple::SignalType::Terminate>(
      async_simple::coro::collectAll(std::move(works)), watcher(conf));

  co_return std::error_code{};
}

async_simple::coro::Lazy<std::error_code> request_no_pool(
    const bench_config& conf) {
  ELOG_INFO << "bench_config buffer size " << conf.buffer_size;

  std::vector<std::shared_ptr<coro_rpc::coro_rpc_client>> vec;
  for (size_t i = 0; i < conf.client_concurrency; i++) {
    auto client = std::make_shared<coro_rpc::coro_rpc_client>();
#ifdef YLT_ENABLE_IBV
    if (conf.enable_ib) {
      coro_io::ib_socket_t::config_t ib_conf{};
      ib_conf.send_buffer_cnt = conf.send_buffer_cnt;
      ib_conf.recv_buffer_cnt = conf.min_recv_buf_count;
      ib_conf.cap.max_recv_wr = conf.max_recv_buf_count;
      [[maybe_unused]] bool is_ok = client->init_ibv(ib_conf);
      assert(is_ok);
    }
#endif
    auto ec = co_await client->connect(conf.url);
    if (ec) {
      ELOG_ERROR << "connect failed";
      co_return std::make_error_code(std::errc::not_connected);
    }
    vec.push_back(std::move(client));
  }

  auto lazy = [&vec, conf](size_t i) -> async_simple::coro::Lazy<void> {
    std::string send_str(conf.send_data_len, 'A');
    std::string_view send_str_view(send_str);
    auto& client = *vec[i];
    for (size_t i = 0; i < conf.max_request_count; i++) {
      client.set_req_attachment(send_str_view);
      auto start = std::chrono::steady_clock::now();
      auto result = co_await client.call<echo>();
      if (!result.has_value()) {
        ELOG_WARN << result.error().msg;
        break;
      }
      auto now = std::chrono::steady_clock::now();
      g_latency.observe(
          std::chrono::duration_cast<std::chrono::microseconds>(now - start)
              .count());
      g_throughput_count.fetch_add(send_str_view.length(),
                                   std::memory_order_relaxed);
      g_qps_count.fetch_add(1, std::memory_order_relaxed);
    }
  };
  std::vector<async_simple::coro::Lazy<void>> works;
  works.reserve(conf.client_concurrency);
  for (size_t i = 0; i < conf.client_concurrency; i++) {
    works.push_back(lazy(i));
  }
  co_await async_simple::coro::collectAll<async_simple::SignalType::Terminate>(
      async_simple::coro::collectAll(std::move(works)), watcher(conf));

  co_return std::error_code{};
}

int main(int argc, char** argv) {
  cmdline::parser parser;

  parser.add<std::string>("url", 'u', "url", false, "0.0.0.0:9000");
  parser.add<size_t>("send_data_len", 's', "send data length", false,
                     1024 * 1024 * 8);  // 8MB
  parser.add<uint32_t>("client_concurrency", 'c',
                       "total number of http clients", false, 0);
  parser.add<size_t>("max_request_count", 'm', "max request count", false,
                     100000000);

  parser.add<unsigned short>("port", 'p', "server port", false, 9000);
  parser.add<size_t>("resp_len", 'r', "response data length", false, 13);
  parser.add<uint32_t>("buffer_size", 'b', "buffer size", false, 256 * 1024);
  parser.add<int>("log_level", 'o', "Severity::INFO 1 as default, WARN is 4",
                  false, 1);
#ifdef YLT_ENABLE_IBV
  parser.add<bool>("enable_ib", 'i', "enable ib", false, true);
#else
  parser.add<bool>("enable_ib", 'i', "enable ib", false, false);
#endif
  parser.add<uint32_t>("duration", 'd', "duration seconds", false, 100000);
  parser.add<uint32_t>("min_recv_buf_count", 'e', "min recieve buffer count",
                       false, 8);
  parser.add<uint32_t>("max_recv_buf_count", 'f', "max recieve buffer count",
                       false, 32);
  parser.add<bool>("use_client_pool", 'g', "use client pool", false, true);
  parser.add<bool>("reuse_client_pool", 'h', "reuse client pool", false, true);
  parser.add<uint32_t>("send_buffer_cnt", 'j', "send buffer max cnt", false, 4);

  parser.parse_check(argc, argv);
  auto conf = init_conf(parser);

  easylog::set_min_severity((easylog::Severity)conf.log_level);

  easylog::set_async(false);

#ifdef YLT_ENABLE_IBV
  if (conf.enable_ib) {
    coro_io::get_global_ib_device(
        {.buffer_pool_config = {.buffer_size = conf.buffer_size}});
  }
#endif

  if (conf.client_concurrency == 0) {
    std::cout << "start server\n";
    std::string resp_str;
    if (conf.resp_len != 0) {
      resp_str = std::string(conf.resp_len, 'A');
      g_resp_str = resp_str;
      g_resp_len = conf.resp_len;
    }
    coro_rpc::coro_rpc_server server(std::thread::hardware_concurrency(),
                                     conf.port, "0.0.0.0",
                                     std::chrono::seconds(10));
    server.register_handler<echo>();
#ifdef YLT_ENABLE_IBV
    if (conf.enable_ib) {
      coro_io::ib_socket_t::config_t ib_conf{};
      ib_conf.send_buffer_cnt = conf.send_buffer_cnt;
      ib_conf.recv_buffer_cnt = conf.min_recv_buf_count;
      ib_conf.cap.max_recv_wr = conf.max_recv_buf_count;
      server.init_ibv(ib_conf);
    }
#endif
    watcher(conf).start([](auto&&) {
    });
    [[maybe_unused]] auto ret = server.start();
  }
  else {
    std::cout << "will start client\n";
    if (conf.reuse_client_pool) {
      async_simple::coro::syncAwait(request_with_reuse(conf));
    }
    else if (conf.use_client_pool) {
      async_simple::coro::syncAwait(request(conf));
    }
    else {
      async_simple::coro::syncAwait(request_no_pool(conf));
    }
  }
}