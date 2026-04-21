#include <doctest.h>

#include <array>
#include <chrono>
#include <iostream>
#include <memory>
#include <system_error>
#include <ylt/coro_io/coro_io.hpp>

#include "async_simple/coro/Lazy.h"
using namespace std::chrono_literals;

#ifndef __clang__
#ifdef __GNUC__
#ifndef _WIN32
#include <features.h>
#if __GNUC_PREREQ(10, 3)  // If  gcc_version >= 10.3
#define IS_OK
#endif
#else
#define IS_OK
#endif
#else
#define IS_OK
#endif
#else
#define IS_OK
#endif

async_simple::coro::Lazy<void> test_channel() {
  auto ch = coro_io::create_channel<int>(1000);

  co_await coro_io::async_send(ch, 41);
  co_await coro_io::async_send(ch, 42);

  std::error_code ec;
  int val;
  std::tie(ec, val) = co_await coro_io::async_receive(ch);
  CHECK(val == 41);

  std::tie(ec, val) = co_await coro_io::async_receive(ch);
  CHECK(val == 42);
}

async_simple::coro::Lazy<std::pair<std::error_code, int>> async_receive_wrapper(
    std::shared_ptr<coro_io::channel<int>> ch) {
  co_return co_await async_receive(*ch);
}

async_simple::coro::Lazy<void> wait_wrapper(
    std::shared_ptr<coro_io::period_timer> t) {
  co_await t->async_await();
}
async_simple::coro::Lazy<void> test_select_channel() {
  using namespace coro_io;
  using namespace async_simple::coro;
  auto ch1 = coro_io::create_shared_channel<int>(1000);
  auto ch2 = coro_io::create_shared_channel<int>(1000);

  co_await async_send(*ch1, 41);
  co_await async_send(*ch2, 42);

  std::array<int, 2> arr{41, 42};

  auto result = co_await collectAny(async_receive_wrapper(ch1),
                                    async_receive_wrapper(ch2));
  int val = std::visit(
      [&val](auto& v) {
        return static_cast<int>(v.value().second);
      },
      result);

  CHECK(val == arr[result.index()]);

  co_await async_send(*ch1, 41);
  co_await async_send(*ch2, 42);

  std::vector<Lazy<std::pair<std::error_code, int>>> vec;
  vec.push_back(async_receive_wrapper(ch1));
  vec.push_back(async_receive_wrapper(ch2));

  auto result2 = co_await collectAny(std::move(vec));
  val = result2.value().second;
  CHECK(val == arr[result2.index()]);

  auto timer1 = std::make_shared<period_timer>(coro_io::get_global_executor());
  timer1->expires_after(100ms);
  auto timer2 = std::make_shared<period_timer>(coro_io::get_global_executor());
  timer2->expires_after(200ms);
  auto val1 = co_await collectAny(wait_wrapper(timer1), wait_wrapper(timer2));

  CHECK(val1.index() == 0);
}

void callback_lazy() {
#ifdef IS_OK
  using namespace async_simple::coro;
  auto test0 = []() mutable -> Lazy<int> {
    co_return 41;
  };

  auto test1 = []() mutable -> Lazy<int> {
    co_return 42;
  };

  std::vector<Lazy<int>> input;
  input.push_back(test1());

  syncAwait([&](std::vector<Lazy<int>> input) -> Lazy<void> {
    auto result = co_await collectAny(std::move(input));
    CHECK(result.index() == 0);
    CHECK(result.value() == 42);
    int r = co_await test0();
    int r2 = r + result.value();
    CHECK(r2 == 83);
    co_return;
  }(std::move(input)));

#endif
}

TEST_CASE("test channel send recieve, test select channel and coroutine") {
  async_simple::coro::syncAwait(test_channel());
  async_simple::coro::syncAwait(test_select_channel());
  callback_lazy();
}
