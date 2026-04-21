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

#include <exception>
#include <system_error>
#ifdef HAVE_GLOG
#include <glog/logging.h>
#endif

#ifdef HAVE_SPDLOG
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/spdlog.h>
#endif

#include <filesystem>
#include <ylt/easylog.hpp>

using namespace easylog;

class ScopedTimer {
 public:
  ScopedTimer(const char *name)
      : m_name(name), m_beg(std::chrono::high_resolution_clock::now()) {}
  ScopedTimer(const char *name, uint64_t &ns) : ScopedTimer(name) {
    m_ns = &ns;
  }
  ~ScopedTimer() {
    auto end = std::chrono::high_resolution_clock::now();
    auto dur =
        std::chrono::duration_cast<std::chrono::nanoseconds>(end - m_beg);
    if (m_ns)
      *m_ns = dur.count();

    std::cout << m_name << " : " << dur.count() << " ns\n";
  }

 private:
  const char *m_name;
  std::chrono::time_point<std::chrono::high_resolution_clock> m_beg;
  uint64_t *m_ns = nullptr;
};

void test_glog() {
#ifdef HAVE_GLOG
  std::filesystem::remove("glog.txt");
  FLAGS_log_dir = ".";
  FLAGS_minloglevel = google::GLOG_INFO;
  google::SetLogDestination(google::INFO, "glog.txt");
  google::InitGoogleLogging("glog");

  {
    ScopedTimer timer("glog   ");
    for (int i = 0; i < 5000; i++)
      LOG(INFO) << "Hello, it is a long string test! " << 42 << 21 << 2.5;
  }
#endif
}

void test_easylog(std::string filename, int count, bool async) {
  std::error_code ec;
  std::filesystem::remove(filename, ec);
  if (ec) {
    std::cout << ec.message() << "\n";
  }
  easylog::init_log(Severity::DEBUG, filename, async, false, -1);
  for (int i = 0; i < 10; i++) {
    ScopedTimer timer("easylog");
    for (int i = 0; i < count; i++)
      ELOG(INFO) << "Hello logger: msg number " << i;
  }
}

#ifdef HAVE_SPDLOG
void bench(int howmany, std::shared_ptr<spdlog::logger> log) {
  spdlog::drop(log->name());

  using std::chrono::duration;
  using std::chrono::duration_cast;
  using std::chrono::high_resolution_clock;

  for (int i = 0; i < 10; i++) {
    ScopedTimer timer("spdlog ");
    for (auto i = 0; i < howmany; ++i) {
      SPDLOG_LOGGER_INFO(log, "Hello logger: msg number {}", i);
    }
  }
}

void bench_mt(int howmany, std::shared_ptr<spdlog::logger> log,
              size_t thread_count) {
  using std::chrono::duration;
  using std::chrono::duration_cast;
  using std::chrono::high_resolution_clock;
  spdlog::drop(log->name());

  std::vector<std::thread> threads;
  threads.reserve(thread_count);
  {
    ScopedTimer timer("spdlog ");
    for (size_t t = 0; t < thread_count; ++t) {
      threads.emplace_back([&]() {
        for (int j = 0; j < howmany / static_cast<int>(thread_count); j++) {
          SPDLOG_LOGGER_INFO(log, "Hello logger: msg number {}", j);
        }
      });
    }

    for (auto &t : threads) {
      t.join();
    };
  }
}
#endif

int main() {
  int count = 500000;
#ifdef HAVE_SPDLOG
  spdlog::set_pattern("%Y-%m-%d %H:%M:%S.%e %l [%t] [%@] %v");

  std::cout << "========test sync spdlog===========\n";
  auto basic_st = spdlog::basic_logger_st("basic_st", "basic_st.log", true);
  bench(count, std::move(basic_st));

  std::cout << "========test async spdlog===========\n";
  auto basic_mt = spdlog::basic_logger_st("basic_mt", "basic_mt.log", true);
  bench_mt(count, std::move(basic_mt), 4);
#endif

  test_glog();
  std::cout << "========test sync easylog===========\n";
  test_easylog("easylog.txt", count, /*async =*/false);
  std::cout << "========test async easylog===========\n";
  test_easylog("async_easylog.txt", count, /*async =*/true);
}
