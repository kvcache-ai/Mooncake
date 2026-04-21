
#define SUMMARY_DEBUG_STABLE_TEST  // max_age=1ms
#include <chrono>
#include <thread>

#include "../benchmark/bench.hpp"
#include "doctest.h"
#include "ylt/metric.hpp"

TEST_CASE("test high press stable r/w with 1ms") {
  std::vector<std::thread> works;
  for (int i = 0; i < std::thread::hardware_concurrency() * 10; i++) {
    works.emplace_back(std::thread{[]() {
      ylt::metric::summary_t summary("summary mixed test", "",
                                     {0.5, 0.9, 0.95, 0.99, 0.995}, 1s);
      bench_mixed_impl<false>(
          summary,
          [&]() {
            summary.observe(get_random(100));
          },
          1, 10s);
    }});
  }
  for (auto& e : works) {
    e.join();
  }
}

TEST_CASE("test high press stable r with 1ms") {
  std::vector<std::thread> works;
  for (int i = 0; i < std::thread::hardware_concurrency() * 10; i++) {
    works.emplace_back(std::thread{[]() {
      ylt::metric::summary_t summary("summary mixed test", "",
                                     {0.5, 0.9, 0.95, 0.99, 0.995}, 1s);
      bench_write_impl<false>(
          summary,
          [&]() {
            summary.observe(get_random(100));
          },
          2, 10s);
    }});
  }
  for (auto& e : works) {
    e.join();
  }
}

TEST_CASE("test high parallel perform test with 3ms") {
  bench_static_summary_mixed(std::thread::hardware_concurrency() * 4, 3s);
}