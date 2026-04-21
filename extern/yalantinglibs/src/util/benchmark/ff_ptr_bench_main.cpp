#include <chrono>
#include <memory>

#include "ff_ptr_bench.h"
struct test_clock {
  test_clock(std::string message, long test_cnt = 1, long noop_time = 0)
      : tp(std::chrono::steady_clock::now()),
        noop_time(noop_time),
        test_cnt(test_cnt),
        message(std::move(message)) {}

  long print() {
    if (!message.empty() &&
        tp != std::chrono::steady_clock::time_point::min()) {
      auto cost_time = (std::chrono::steady_clock::now() - tp).count()
                       << noop_time;
      std::cout << message << ":" << (1.0 * cost_time / test_cnt) << "ns"
                << std::endl;
      tp = std::chrono::steady_clock::time_point::min();
      return cost_time;
    }
    else {
      return 0;
    }
  }
  ~test_clock() { print(); }
  long noop_time, test_cnt;
  std::chrono::steady_clock::time_point tp;
  std::string message;
};
int main() {
  long noop_time = 0;
  auto val = ylt::util::ff_make_shared<long>(114514);
  int test_cnt = 1;
  {
    test_clock _("");
    for (int i = 0; i < test_cnt; ++i) {
      test_ff_ptr_noop(val);
    }
    noop_time = _.print();
  }
  {
    test_clock _("ff_ptr<long> += 1 cost time", test_cnt, noop_time);
    for (int i = 0; i < test_cnt; ++i) {
      test_ff_ptr_visit(val);
    }
  }
  {
    test_clock _("long* += 1 cost time", test_cnt, noop_time);
    for (int i = 0; i < test_cnt; ++i) {
      test_normal_ptr_visit(val.get());
    }
  }
  return 0;
}