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
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <vector>

#include "asio.hpp"
#include "asio/dispatch.hpp"
#include "asio/executor_work_guard.hpp"
#include "asio/io_context.hpp"

std::atomic<int64_t> qps, cli_connect_cnt, cli_finish_cnt;

std::atomic<bool> has_send_flag = false;

std::unique_ptr<std::promise<void>> has_end, has_start;

struct Config {
  std::chrono::seconds test_time{30}, warm_up_time{5};
  unsigned thread_cnt = std::thread::hardware_concurrency();
  unsigned client_cnt_per_thread = 20;
  std::string host = "127.0.0.1", port = "9000";
  std::filesystem::path input = "./test_data/echo_test";
  unsigned pipeline_size = 1;
  bool disable_check = true;
};

unsigned tot_client;
Config config;

class async_rpc_client {
 public:
  async_rpc_client(asio::io_context &io_context, std::string_view in_data,
                   std::string_view out_data)
      : io_context_(io_context),
        socket_(io_context),
        all_buf_(in_data),
        expected_buf_(out_data),
        timer_(io_context),
        resolver_(io_context) {}

  void connect(std::string_view host, std::string_view service,
               std::chrono::steady_clock::time_point time_point,
               std::deque<async_rpc_client> &clis) {
    timer_.expires_at(time_point);
    asio::post(io_context_, [=, &clis, this] {
      resolver_.async_resolve(
          host, service,
          [this, &clis](const asio::error_code &ec,
                        asio::ip::tcp::resolver::results_type endpoints) {
            asio::async_connect(
                socket_, endpoints,
                [this, &clis](const asio::error_code &ec,
                              const asio::ip::tcp::endpoint &endpoint) {
                  if (ec) {
                    if (!has_send_flag.exchange(true)) {
                      has_start->set_value();
                    }
                    close();
                    return;
                  }
                  auto tmp = cli_connect_cnt.fetch_add(1);

                  if (tmp == tot_client - 1) {
                    has_start->set_value();
                    for (auto &e : clis) {
                      e.start();
                    }
                  }
                });
          });
    });
  }

  void start() {
    asio::dispatch(io_context_, [this] {
      write_data();
      do_read();
      time_out();
    });
  }

  std::unordered_map<std::size_t, std::size_t> &get_latency_tables() {
    return latency_table;
  }

 private:
  void time_out() {
    if (is_closed)
      return;
    timer_.async_wait([this](const asio::error_code &err) {
      close();
    });
  }

  void write_data() {
    ++send_seq;
    if (send_seq % 1000 == 0) {
      times.emplace_back(std::chrono::steady_clock::now());
    }
    asio::async_write(socket_, asio::buffer(all_buf_),
                      [this](asio::error_code ec, std::size_t length) {
                        if (ec) {
                          if (!is_closed)
                            std::cout << ec.message() << std::endl;
                          close();
                          return;
                        }
                        if (send_seq - recv_seq < config.pipeline_size) {
                          write_data();
                        }
                        else {
                          send_flag = false;
                        }
                      });
  }

  void do_read() {
    socket_.async_read_some(
        asio::buffer(read_buf_),
        [this](asio::error_code ec, std::size_t length) mutable {
          if (!ec) {
            auto all_res = std::string_view{read_buf_, length};
            while (all_res.size()) {
              auto min_size = (std::min)(all_res.length(),
                                         expected_buf_.length() - read_pos);
              auto res = all_res.substr(0, min_size);
              auto expected_res = expected_buf_.substr(read_pos, min_size);
              if (config.disable_check || res == expected_res) {
                read_pos += min_size;
                if (read_pos == expected_buf_.length()) {
                  read_pos = 0;
                  ++qps;
                  ++recv_seq;
                  if (recv_seq % 1000 == 0) {
                    auto latency =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            std::chrono::steady_clock::now() - times.front());
                    latency_table[latency.count()] += 1;
                    times.pop_front();
                  }
                }
                all_res = all_res.substr(min_size);
              }
              else {
                std::cout << "Err Response!" << std::endl;
                if (!is_closed) {
                  close();
                }
                return;
              }
            }
            if (send_flag == false &&
                send_seq - recv_seq < config.pipeline_size) {
              send_flag = true;
              write_data();
            }
            do_read();
          }
          else if (!is_closed) {
            std::cout << ec.message() << std::endl;
            close();
          }
        });
  }

  void close() {
    if (is_closed) {
      return;
    }
    is_closed = true;
    asio::error_code ignored_ec;
    socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored_ec);
    socket_.close(ignored_ec);
    timer_.cancel();
    ++cli_finish_cnt;
  }

  char read_buf_[5000];
  std::deque<std::chrono::steady_clock::time_point> times;
  asio::io_context &io_context_;
  asio::ip::tcp::socket socket_;

  std::string_view all_buf_;
  std::string_view expected_buf_;
  std::size_t read_pos = 0;
  asio::steady_timer timer_;
  asio::ip::tcp::resolver resolver_;
  asio::ip::basic_resolver<asio::ip::tcp>::results_type endpoints;
  std::unordered_map<std::size_t, std::size_t> latency_table;
  bool is_closed = false, send_flag = true;
  size_t send_seq = 0, recv_seq = 0;
};

template <typename client>
void print_latency(std::deque<client> &clis) {
  std::map<std::size_t, std::size_t> latency_table;
  std::size_t sum = 0, now = 0;
  for (auto &cli : clis) {
    for (auto &e : cli.get_latency_tables()) {
      if (e.second) {
        latency_table[e.first] += e.second;
        sum += e.second;
      }
    }
  }
  static std::array table = {10.0, 30.0, 50.0, 70.0, 90.0, 99.0, 99.9, 99.99};
  size_t pos = 0;
  for (auto &e : latency_table) {
    now += e.second;
    if (pos < table.size() && (now * 100.0 / sum) >= table[pos]) {
      std::cout << table[pos] << "% request finished in " << e.first / 1000.0
                << " ms" << std::endl;
      pos += 1;
    }
  }
  if (latency_table.size())
    std::cout << "Max latency: " << (--latency_table.end())->first / 1000.0
              << " ms\n"
              << std::endl;
  else
    std::cout << "No latency info.\n" << std::endl;
}

template <typename client>
void watch(asio::steady_timer &timer, unsigned int &tot,
           std::deque<client> &clis,
           std::chrono::steady_clock::time_point per_time =
               std::chrono::steady_clock::now(),
           double max_qps = 0, double tot_qps = 0, int cnt = 0) {
  timer.expires_after(std::chrono::seconds(1));
  timer.async_wait([&, per_time, max_qps, tot_qps,
                    cnt](const asio::error_code &) mutable {
    if (cli_finish_cnt == tot) {
      printf("\r\033[k");
      if (cnt > config.warm_up_time.count()) {
        std::cout << "Avg QPS:"
                  << (int64_t)(tot_qps / (cnt - config.warm_up_time.count()))
                  << "              " << std::endl;
        std::cout << "Max QPS:" << (int64_t)max_qps << "              "
                  << std::endl;
      }
      print_latency(clis);
      has_end->set_value();
    }
    else {
      auto now = std::chrono::steady_clock::now();
      int64_t tmp = qps;
      auto r_qps = 1000 * tmp /
                   std::max<uint64_t>(
                       1, std::chrono::duration_cast<std::chrono::milliseconds>(
                              now - per_time)
                              .count());
      max_qps = std::max<double>(max_qps, r_qps);
      if (cnt >= config.warm_up_time.count())
        tot_qps += r_qps;
      cnt += 1;
      printf("\r\033[k");
      std::cout << "QPS: " << r_qps << "          ";
      std::cout.flush();
      qps -= tmp;
      watch(timer, tot, clis, now, max_qps, tot_qps, cnt);
    }
  });
}

template <typename client_type>
void task(std::string_view input, std::string_view output) {
  /*---init task ---*/
  qps = 0;
  cli_connect_cnt = 0;
  cli_finish_cnt = 0;
  has_send_flag = false;
  has_start = std::make_unique<std::promise<void>>();
  has_end = std::make_unique<std::promise<void>>();
  std::deque<client_type> clis;
  std::deque<asio::io_context> iocs(config.thread_cnt);
  std::deque<std::thread> thrds;
  auto stop_time = std::chrono::steady_clock::now() + config.test_time;
  for (size_t i = 0; i < config.thread_cnt; ++i) {
    for (size_t j = 0; j < config.client_cnt_per_thread; ++j) {
      clis.emplace_back(iocs[i], input, output);
    }
  }
  for (auto &e : clis) e.connect(config.host, config.port, stop_time, clis);

  /*---start task ---*/
  for (size_t i = 0; i < config.thread_cnt; ++i) {
    thrds.emplace_back([&iocs, i] {
      asio::io_context::work work(iocs[i]);
      iocs[i].run();
    });
  }
  has_start->get_future().wait();
  if (cli_connect_cnt != tot_client) {
    std::cout << "client connect failed!" << std::endl;
    exit(1);
  }
  asio::steady_timer timer(iocs[0]);
  asio::post(iocs[0], [&] {
    watch(timer, tot_client, clis);
  });
  has_end->get_future().wait();
  for (auto &e : iocs) e.stop();
  for (auto &e : thrds) e.join();
}

void init_config(int argc, char **argv) {
  if (argc >= 2) {
    config.thread_cnt = std::stoul(argv[1]);
    assert(config.thread_cnt >= 1);
    if (argc >= 3) {
      config.client_cnt_per_thread = std::stoul(argv[2]);
      assert(config.client_cnt_per_thread >= 1);
      if (argc >= 4) {
        config.pipeline_size = std::max<size_t>(1, std::stoul(argv[3]));
        if (argc >= 5) {
          config.host = argv[4];
        }
        if (argc >= 6) {
          config.port = argv[5];
        }
        if (argc >= 7) {
          config.input = argv[6];
          if (argc >= 8) {
            config.test_time = std::chrono::seconds(std::stoul(argv[7]));
            if (argc >= 9) {
              config.warm_up_time = std::chrono::seconds(std::stoul(argv[8]));
            }
          }
        }
      }
    }
  }
  tot_client = config.thread_cnt * config.client_cnt_per_thread;
  std::cout << "client thread_cnt: " << config.thread_cnt << '\n'
            << "client cnt per thread: " << config.client_cnt_per_thread << '\n'
            << "Total client: " << tot_client << '\n'
            << "Pipeline size: " << config.pipeline_size << '\n'
            << std::endl;
}

void read_file(const std::filesystem::directory_entry &input_file) {
  if (input_file.is_regular_file()) {
    auto path = input_file.path();
    if (path.has_extension() && path.extension() == ".in") {
      auto out_path = path;
      out_path.replace_extension(".out");
      auto out_file = std::filesystem::directory_entry{out_path};
      if (out_file.is_regular_file()) {
        std::ifstream ifs(input_file.path(),
                          std::ifstream::binary | std::ifstream::in);
        std::string input_data((std::istreambuf_iterator<char>(ifs)),
                               std::istreambuf_iterator<char>());
        std::ifstream ifs2(out_file.path(),
                           std::ifstream::binary | std::ifstream::in);
        std::string output_data((std::istreambuf_iterator<char>(ifs2)),
                                std::istreambuf_iterator<char>());
        auto task_name = out_path.filename().replace_extension().string();
        if (input_data.size() && output_data.size()) {
          std::cout << "task name: " << task_name << '\n' << std::endl;
          task<async_rpc_client>(input_data, output_data);
        }
      }
    }
  }
}

int main(int argc, char **argv) {
  init_config(argc, argv);
  /*-- get input data --*/
  std::error_code ec;
  std::filesystem::directory_entry dir(config.input, ec);
  if (ec) {
    std::cout << ec.message() << std::endl;
    return 0;
  }
  if (dir.is_regular_file()) {
    auto path = dir.path();
    if (path.has_extension() && path.extension() == ".out") {
      path.replace_extension(".in");
    }
    read_file(dir);
  }
  else {
    for (auto &input_file : std::filesystem::directory_iterator{dir}) {
      read_file(input_file);
    }
  }

  return 0;
}