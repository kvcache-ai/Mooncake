#include <exception>
#include <random>

#include "ylt/metric.hpp"
#include "ylt/metric/counter.hpp"
#include "ylt/metric/summary.hpp"

using namespace std::chrono_literals;
using namespace ylt::metric;

inline auto get_random(size_t range = 10000) {
  thread_local std::default_random_engine gen(std::time(nullptr));
  std::uniform_int_distribution<> distr(1, range);
  return distr(gen);
}

struct bench_clock_t {
  bench_clock_t() : start_(std::chrono::steady_clock::now()) {}
  template <typename unit = std::chrono::milliseconds>
  unit duration() {
    auto now = std::chrono::steady_clock::now();
    auto ret = now - start_;
    return std::chrono::duration_cast<unit>(ret);
  }
  std::chrono::steady_clock::time_point start_;
};

template <bool need_latch = true, typename IMPL, typename WRITE_OP>
void bench_mixed_impl(IMPL& impl, WRITE_OP&& op, size_t thd_num,
                      std::chrono::seconds duration) {
  ylt::metric::summary_t lantency_summary(
      "write latency(ms)", "", {0.99, 0.999, 0.9999, 0.99999, 0.999999, 1.0});
  std::atomic<bool> stop = false;
  std::vector<std::thread> vec;
  std::array<std::string, 2> arr{"/test", "200"};
  bench_clock_t clock;
  std::string val(36, ' ');
  for (size_t i = 0; i < thd_num; i++) {
    vec.push_back(std::thread([&, i] {
      bench_clock_t clock_loop;
      auto dur = clock.duration<std::chrono::microseconds>();
      while (!stop && dur < duration + 1s) {
        op();
        if constexpr (need_latch) {
          auto new_dur = clock.duration<std::chrono::microseconds>();
          lantency_summary.observe((new_dur - dur).count() / 1000.0f);
          dur = new_dur;
        }
      }
    }));
  }
  std::string s;
  std::size_t tot = 0;
  bench_clock_t clock2;
  int64_t serialze_cnt = 0;
  do {
    s.clear();
    impl.serialize(s);
    tot += s.size();
    ++serialze_cnt;
  } while (clock2.duration() < duration);
  auto total_ms = clock.duration();
  stop = true;
  if constexpr (need_latch) {
    if constexpr (requires { impl.size(); }) {
      std::cout << "size:" << impl.size() << "\n";
    }
    std::cout << "serialize bytes total:" << tot << std::endl;
    std::cout << "run " << total_ms.count() << "ms\n";
    uint64_t cnt;
    double sum;
    auto result = lantency_summary.get_rates(sum, cnt);
    auto seconds = total_ms.count() / 1000.0;
    auto qps = 1.0 * cnt / seconds;
    std::cout << "write thd num: " << thd_num << ", write qps: " << (int64_t)qps
              << "\n";
    std::cout << "serialize qps:" << 1000.0 * serialze_cnt / total_ms.count()
              << ", str size=" << s.size() << "\n";
    s = "";
    lantency_summary.serialize(s);
    std::cout << s;
  }

  for (auto& thd : vec) {
    thd.join();
  }
}

inline void bench_static_summary_mixed(size_t thd_num,
                                       std::chrono::seconds duration,
                                       std::chrono::seconds age = 1s) {
  ylt::metric::summary_t summary("summary mixed test", "",
                                 {0.5, 0.9, 0.95, 0.99, 0.995}, age);
  bench_mixed_impl(
      summary,
      [&]() {
        summary.observe(get_random(100));
      },
      thd_num, duration);
}

inline void bench_static_counter_mixed(size_t thd_num,
                                       std::chrono::seconds duration) {
  ylt::metric::counter_t counter("counter mixed test", "");
  bench_mixed_impl(
      counter,
      [&]() {
        counter.inc(1);
      },
      thd_num, duration);
}

inline void bench_dynamic_summary_mixed(size_t thd_num,
                                        std::chrono::seconds duration,
                                        std::chrono::seconds age = 1s,
                                        int max_cnt = 1000000) {
  ylt::metric::dynamic_summary summary("dynamic summary mixed test", "",
                                       {0.5, 0.9, 0.95, 0.99, 0.995},
                                       {"a", "b"}, age);
  bench_mixed_impl(
      summary,
      [&]() mutable {
        summary.observe({"123e4567-e89b-12d3-a456-426614174000",
                         std::to_string(get_random(max_cnt))},
                        get_random(100));
      },
      thd_num, duration);
}

inline void bench_dynamic_counter_mixed_with_delete(
    size_t thd_num, std::chrono::seconds duration,
    std::chrono::seconds age = 1s, int max_cnt = 1000000) {
  ylt::metric::dynamic_counter_2d counter("dynamic summary mixed test", "",
                                          {"a", "b"});
  bench_mixed_impl(
      counter,
      [&]() mutable {
        std::array<std::string, 2> label = {
            "123e4567-e89b-12d3-a456-426614174000",
            std::to_string(get_random(max_cnt))};
        counter.inc(label, 1);
        counter.remove_label_value({{"a", label[0]}, {"b", label[1]}});
      },
      thd_num, duration);
}

inline void bench_dynamic_counter_mixed(size_t thd_num,
                                        std::chrono::seconds duration,
                                        std::chrono::seconds age = 1s,
                                        int max_cnt = 1000000) {
  ylt::metric::dynamic_counter_2d counter("dynamic summary mixed test", "",
                                          {"a", "b"});
  bench_mixed_impl(
      counter,
      [&]() mutable {
        counter.inc({"123e4567-e89b-12d3-a456-426614174000",
                     std::to_string(get_random(max_cnt))},
                    1);
      },
      thd_num, duration);
}

template <typename IMPL, typename OP>
void bench_serialize_impl(IMPL& impl, OP&& op, size_t COUNT, bool to_json) {
  for (size_t i = 0; i < COUNT; i++) {
    op(i);
  }
  std::string str;
  bench_clock_t clock;
  if (to_json) {
    if constexpr (requires { impl[0]; }) {
      str = manager_helper::serialize_to_json(impl);
    }
    else {
      impl.serialize_to_json(str);
    }
  }
  else {
    if constexpr (requires { impl[0]; }) {
      str = manager_helper::serialize(impl);
    }
    else {
      impl.serialize(str);
    }
  }
  std::cout << "COUNT:" << COUNT << ", string size: " << str.size() << ", "
            << clock.duration().count() << "ms\n";
  if (str.size() < 1000) {
    std::cout << str << std::endl;
  }
}

inline void bench_many_metric_serialize(size_t COUNT, size_t LABEL_COUNT,
                                        bool to_json = false) {
  std::vector<std::shared_ptr<metric_t>> vec;
  bench_serialize_impl(
      vec,
      [LABEL_COUNT, &vec](int) {
        auto counter = std::make_shared<dynamic_counter_t>(
            std::string("qps"), "", std::array<std::string, 2>{"url", "code"});
        for (size_t j = 0; j < LABEL_COUNT; j++) {
          counter->inc({"test_label_value", std::to_string(j)});
        }
        vec.push_back(counter);
      },
      COUNT, to_json);
}

inline void bench_dynamic_counter_serialize(size_t COUNT,
                                            bool to_json = false) {
  dynamic_counter_t counter("qps2", "", {"url", "code"});
  bench_serialize_impl(
      counter,
      [&](int i) {
        counter.inc(
            {"123e4567-e89b-12d3-a456-426614174000", std::to_string(i)});
      },
      COUNT, to_json);
}

inline void bench_dynamic_summary_serialize(size_t COUNT,
                                            bool to_json = false) {
  dynamic_summary_2 summary("qps2", "", {0.5, 0.9, 0.95, 0.995},
                            std::array<std::string, 2>{"method", "url"});
  bench_serialize_impl(
      summary,
      [&](int i) {
        summary.observe(
            {"123e4567-e89b-12d3-a456-426614174000", std::to_string(i)}, i);
      },
      COUNT, to_json);
}

template <bool need_output = true, typename IMPL, typename OP>
void bench_write_impl(IMPL& impl, OP&& op, size_t thd_num,
                      std::chrono::seconds duration) {
  std::atomic<bool> stop = false;
  std::vector<std::future<int64_t>> vec;
  std::array<std::string, 2> arr{"/test", "200"};
  thread_local_value<uint64_t> local_val(thd_num);
  bench_clock_t clock;
  for (size_t i = 0; i < thd_num; i++) {
    vec.push_back(std::async([&] {
      int64_t cnt = 0;
      while (!stop) {
        op();
        ++cnt;
      }
      return cnt;
    }));
  }
  std::this_thread::sleep_for(duration);
  stop = true;
  auto tm = clock.duration().count();
  double qps = 0;
  for (auto& thd : vec) {
    qps += thd.get();
  }
  if constexpr (need_output) {
    std::cout << "run " << clock.duration().count() << "ms\n";
    qps /= (clock.duration().count() / 1000.0);
    std::cout << "thd num: " << thd_num << ", qps: " << (int64_t)qps << "\n";
  }
}

inline void bench_static_counter_write(size_t thd_num,
                                       std::chrono::seconds duration) {
  counter_t counter("qps", "");
  bench_write_impl(
      counter,
      [&] {
        counter.inc(1);
      },
      thd_num, duration);
}

inline void bench_dynamic_counter_write(size_t thd_num,
                                        std::chrono::seconds duration) {
  dynamic_counter_t counter("qps2", "", {"url", "code"});
  bench_write_impl(
      counter,
      [&] {
        counter.inc({"/test", std::to_string(get_random())}, 1);
      },
      thd_num, duration);
}

inline void bench_dynamic_summary_write(size_t thd_num,
                                        std::chrono::seconds duration) {
  dynamic_summary_2 summary("qps2", "", {0.5, 0.9, 0.95, 0.99},
                            std::array<std::string, 2>{"method", "url"});
  bench_write_impl(
      summary,
      [&]() {
        summary.observe({"/test", std::to_string(get_random())},
                        get_random(100));
      },
      thd_num, duration);
}

inline void bench_static_summary_write(size_t thd_num,
                                       std::chrono::seconds duration) {
  ylt::metric::summary_t summary("qps2", "", {0.5, 0.9, 0.95, 0.99});
  bench_write_impl(
      summary,
      [&]() {
        summary.observe(get_random(100));
      },
      thd_num, duration);
}