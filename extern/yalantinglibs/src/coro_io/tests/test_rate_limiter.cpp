#include <async_simple/coro/Collect.h>
#include <doctest.h>

#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_io/rate_limiter.hpp>

int64_t current_time_mills() {
  std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
  std::chrono::milliseconds ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          now.time_since_epoch());
  return ms.count();
}

TEST_CASE("test smooth_bursty_rate_limiter simple") {
  coro_io::smooth_bursty_rate_limiter rate_limiter(1);
  std::chrono::milliseconds wait_time =
      async_simple::coro::syncAwait(rate_limiter.acquire(1));
  CHECK_EQ(0, wait_time.count());
}

TEST_CASE("test smooth_bursty_rate_limiter acquire multi permits") {
  double permits_per_second = 10.0;
  int permits_to_acquire = 5;
  double expected_cost = (permits_to_acquire) * (1 / permits_per_second);
  double cost_diff = 0.1;

  coro_io::smooth_bursty_rate_limiter rate_limiter(permits_per_second);

  // acquire much enough, next acquire will wait
  async_simple::coro::syncAwait(rate_limiter.acquire(permits_to_acquire));

  int64_t start_mills = current_time_mills();
  async_simple::coro::syncAwait(rate_limiter.acquire(1));
  double cost = (current_time_mills() - start_mills) / 1000.0;
  CHECK(cost > expected_cost - cost_diff);
}

TEST_CASE("test smooth_bursty_rate_limiter single thread") {
  double permits_per_second = 10.0;
  int permits_to_acquire = 5;
  double expected_cost = (permits_to_acquire - 1) * (1 / permits_per_second);
  double cost_diff = 0.1;

  int64_t start_mills = current_time_mills();
  coro_io::smooth_bursty_rate_limiter rate_limiter(permits_per_second);
  for (int i = 0; i < permits_to_acquire; i++) {
    std::chrono::milliseconds wait_mills =
        async_simple::coro::syncAwait(rate_limiter.acquire(1));
    ELOG_INFO << "wait for " << wait_mills.count();
  }
  double cost = (current_time_mills() - start_mills) / 1000.0;

  CHECK(cost > expected_cost - cost_diff);
}

TEST_CASE("test smooth_bursty_rate_limiter multi coroutine") {
  double permits_per_second = 100.0;
  int num_of_coroutine = 5;
  int permits_to_acquire_every_coroutine = 5;
  double expected_cost =
      (num_of_coroutine * permits_to_acquire_every_coroutine - 1) *
      (1 / permits_per_second);
  double cost_diff = 0.1;

  coro_io::smooth_bursty_rate_limiter rate_limiter(permits_per_second);
  auto consumer = [&](int coroutine_num) -> async_simple::coro::Lazy<void> {
    for (int i = 0; i < permits_to_acquire_every_coroutine; i++) {
      co_await rate_limiter.acquire(1);
      ELOG_INFO << "coroutine " << coroutine_num << " acquired";
    }
  };

  auto consumer_list_lazy = [&]() -> async_simple::coro::Lazy<void> {
    std::vector<async_simple::coro::Lazy<void>> lazy_list;
    for (int i = 0; i < num_of_coroutine; i++) {
      lazy_list.push_back(consumer(i));
    }
    co_await collectAllPara(std::move(lazy_list));
  };

  int64_t start_mills = current_time_mills();
  syncAwait(consumer_list_lazy().via(
      coro_io::g_block_io_context_pool<coro_io::multithread_context_pool>(1)
          .get_executor()));
  double cost = (current_time_mills() - start_mills) / 1000.0;

  CHECK(cost > expected_cost - cost_diff);
}