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
#pragma once

#include <async_simple/Executor.h>
#include <async_simple/Promise.h>
#include <async_simple/Try.h>
#include <async_simple/Unit.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/Sleep.h>
#include <async_simple/coro/SpinLock.h>

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <ylt/util/expected.hpp>

#include "async_simple/Future.h"
#include "async_simple/coro/Mutex.h"
#include "coro_io.hpp"
#include "detail/client_queue.hpp"
#include "io_context_pool.hpp"
#include "ylt/easylog.hpp"
#include "ylt/util/atomic_shared_ptr.hpp"
#ifdef YLT_ENABLE_IBV
#include "ylt/coro_io/ibverbs/ib_socket.hpp"
#endif
namespace coro_io {

struct client_reuse_hint {};

template <typename client_t, typename io_context_pool_t>
class client_pools;

template <typename, typename>
class load_balancer;

template <typename client_t,
          typename io_context_pool_t = coro_io::io_context_pool>
class client_pool : public std::enable_shared_from_this<
                        client_pool<client_t, io_context_pool_t>> {
  using client_pools_t = client_pools<client_t, io_context_pool_t>;
  static async_simple::coro::Lazy<void> collect_idle_timeout_client(
      std::weak_ptr<client_pool> self_weak,
      coro_io::detail::client_queue<std::unique_ptr<client_t>>& clients,
      std::chrono::milliseconds sleep_time, std::size_t clear_cnt) {
    std::shared_ptr<client_pool> self = self_weak.lock();
    if (self == nullptr) {
      co_return;
    }
    while (true) {
      clients.reselect();
      self = nullptr;
      co_await coro_io::sleep_for(sleep_time);
      if ((self = self_weak.lock()) == nullptr) {
        break;
      }
      while (true) {
        ELOG_TRACE << "start collect timeout client of pool{"
                   << self->host_name_
                   << "}, now client count: " << clients.size();
        auto [is_all_cleared, _] = clients.clear_old(clear_cnt);
        auto free_client_cnt = clients.size();
        ELOG_INFO
            << "finish collect timeout client of pool{" << self->host_name_
            << "}, now free client cnt: " << free_client_cnt
            << " total client cnt:"
            << self->inusing_client_cnt_.load(std::memory_order::relaxed) +
                   free_client_cnt
            << " parallel request cnt:"
            << self->parallel_request_cnt_.load(std::memory_order::relaxed);
        if (!is_all_cleared) [[unlikely]] {
          try {
            co_await async_simple::coro::Yield{};
          } catch (std::exception& e) {
            ELOG_ERROR << "unexcepted yield exception: " << e.what();
          }
        }
        else {
          break;
        }
      }
    }
    co_return;
  }

  static auto rand_time(std::chrono::milliseconds ms) {
    static thread_local std::default_random_engine r;
    std::uniform_real_distribution e(1.0f, 1.2f);
    return std::chrono::milliseconds{static_cast<int64_t>(e(r) * ms.count())};
  }

  static async_simple::coro::Lazy<std::pair<bool, std::chrono::milliseconds>>
  reconnect_impl(std::unique_ptr<client_t>& client,
                 std::shared_ptr<client_pool>& self) {
    auto pre_time_point = std::chrono::steady_clock::now();
    auto dns_cache_update_duration =
        self->pool_config_.dns_cache_update_duration;
    std::vector<asio::ip::tcp::endpoint>* eps_raw_ptr = nullptr;
    std::shared_ptr<std::vector<asio::ip::tcp::endpoint>> eps_ptr;
    std::vector<asio::ip::tcp::endpoint> eps;
    uint64_t old_tp;
    if (dns_cache_update_duration.count() >= 0) {
      eps_ptr = self->eps_.load(std::memory_order_acquire);
      eps_raw_ptr = eps_ptr.get();
      old_tp = self->timepoint_.load(std::memory_order_acquire);
      auto old_time_point = std::chrono::steady_clock::time_point{
          std::chrono::steady_clock::duration{old_tp}};

      // check if no dns cache, or cache outdated
      if (eps_raw_ptr->empty() || (pre_time_point - old_time_point) >
                                      dns_cache_update_duration) [[unlikely]] {
        // start resolve, store result to eps
        eps_raw_ptr = &eps;
      }
      // else, we can used cached eps
    }
    auto result = co_await client->connect(self->host_name_, eps_raw_ptr);

    bool ok = client_t::is_ok(result);
    if (dns_cache_update_duration.count() >= 0) {
      if ((!ok &&
           (eps_raw_ptr != &eps))  // use cache but request failed, clear cache
          || (ok &&
              (eps_raw_ptr == &eps)))  // don't have cache request ok, set cache
      {
        if (self->timepoint_.compare_exchange_strong(
                old_tp, pre_time_point.time_since_epoch().count(),
                std::memory_order_release)) {
          auto new_eps_ptr =
              std::make_shared<std::vector<asio::ip::tcp::endpoint>>(
                  std::move(eps));
          auto is_lock = self->dns_cache_update_mutex_.tryLock();
          // store should'nt need lock ,but tsan show a data race here,
          // so we add a try lock here
          if (is_lock) [[likely]] {
            self->eps_.store(std::move(new_eps_ptr), std::memory_order_release);
            self->dns_cache_update_mutex_.unlock();
          }
        }
      }
    }

    auto post_time_point = std::chrono::steady_clock::now();
    auto cost_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        post_time_point - pre_time_point);
    ELOG_TRACE << "reconnect client{" << client.get() << "}"
               << "is success:" << ok
               << ", cost time: " << cost_time / std::chrono::milliseconds{1}
               << "ms";
    co_return std::pair{ok, cost_time};
  }

  static async_simple::coro::Lazy<void> reconnect(
      std::unique_ptr<client_t>& client, std::weak_ptr<client_pool> watcher) {
    using namespace std::chrono_literals;
    std::shared_ptr<client_pool> self = watcher.lock();
    uint32_t i = UINT32_MAX;  // (at least connect once)
    do {
      ELOG_TRACE << "try to reconnect client{" << client.get() << "},host:{"
                 << client->get_host() << ":" << client->get_port()
                 << "}, try count:" << i + 1 << "max retry limit:"
                 << self->pool_config_.connect_retry_count;
      auto [ok, cost_time] = co_await reconnect_impl(client, self);
      if (ok) {
        ELOG_TRACE << "reconnect client{" << client.get() << "} success";
        co_return;
      }
      ELOG_TRACE << "reconnect client{" << client.get()
                 << "} failed. If client close:{" << client->has_closed()
                 << "}";
      auto wait_time = rand_time(
          (self->pool_config_.reconnect_wait_time - cost_time) / 1ms * 1ms);
      self = nullptr;
      if (wait_time.count() > 0)
        co_await coro_io::sleep_for(wait_time, &client->get_executor());
      self = watcher.lock();
      ++i;
    } while (self && i < self->pool_config_.connect_retry_count);
    ELOG_WARN << "reconnect client{" << client.get() << "},host:{"
              << client->get_host() << ":" << client->get_port()
              << "} out of max limit, stop retry. connect failed";
    alive_detect(client->get_config(), std::move(self)).start([](auto&&) {
    });
    client = nullptr;
  }

  static async_simple::coro::Lazy<void> alive_detect(
      const typename client_t::config& client_config,
      std::weak_ptr<client_pool> watcher) {
    std::shared_ptr<client_pool> self = watcher.lock();
    using namespace std::chrono_literals;
    if (self && self->pool_config_.host_alive_detect_duration.count() != 0 &&
        self->free_client_count() == 0) {
      bool expected = true;
      if (!self->is_alive_.compare_exchange_strong(
              expected, false)) {  // other alive detect coroutine is running.
        co_return;
      }
      if (self->free_client_count() > 0) {  // recheck for multi-thread
        self->is_alive_ = true;
        co_return;
      }
      auto executor = self->io_context_pool_.get_executor();
      auto client = std::make_unique<client_t>(executor);
      if (!client->init_config(client_config))
        AS_UNLIKELY {
          ELOG_ERROR << "Init client config failed in host alive detect. That "
                        "is not expected.";
          co_return;
        }
      while (true) {
        auto [ok, cost_time] = co_await reconnect_impl(client, self);
        if (ok) {
          ELOG_TRACE << "reconnect client{" << client.get()
                     << "} success. stop alive detect.";
          self->collect_free_client(std::move(client));
          self->is_alive_ =
              true; /*if client close(), we still mark it as alive*/
          co_return;
        }
        if (self->is_alive_) {
          ELOG_TRACE << "client pool is aliving, stop connect client {"
                     << client.get() << "} for alive detect";
          co_return;
        }
        ELOG_TRACE << "reconnect client{" << client.get()
                   << "} failed. continue alive detect.";
        auto wait_time = rand_time(
            (self->pool_config_.host_alive_detect_duration - cost_time) / 1ms *
            1ms);
        self = nullptr;
        if (wait_time.count() > 0) {
          co_await coro_io::sleep_for(wait_time, &client->get_executor());
        }
        self = watcher.lock();
        if (self->is_alive_) {
          ELOG_TRACE << "client pool is aliving, stop connect client {"
                     << client.get() << "} for alive detect";
          co_return;
        }
      }
    }
  }
  template <typename T, std::memory_order order>
  struct watcher {
    watcher(std::atomic<T>& atomic_value) : atomic_value(atomic_value) {
      atomic_value.fetch_add(1, order);
    }
    std::atomic<T>& atomic_value;
    ~watcher() { atomic_value.fetch_sub(1, order); }
  };
  template <typename T, typename U, std::memory_order order>
  struct watcher_weak {
    watcher_weak(std::atomic<T>& atomic_value, std::weak_ptr<U> w) noexcept
        : atomic_value(atomic_value), w(std::move(w)) {
      atomic_value.fetch_add(1, order);
    }
    watcher_weak(const watcher_weak&) = delete;
    watcher_weak(watcher_weak&&) noexcept = default;
    std::atomic<T>& atomic_value;
    std::weak_ptr<U> w;
    ~watcher_weak() {
      if (auto ptr = w.lock()) {
        atomic_value.fetch_sub(1, order);
      }
    }
  };

  async_simple::coro::Lazy<std::unique_ptr<client_t>> get_client(
      const typename client_t::config& client_config) {
    std::unique_ptr<client_t> client;
    free_clients_.try_dequeue(client);
    if (!client) {
      short_connect_clients_.try_dequeue(client);
    }
    if (client == nullptr) {
      auto executor = io_context_pool_.get_executor();
      client = std::make_unique<client_t>(executor);
      if (!client->init_config(client_config))
        AS_UNLIKELY {
          ELOG_ERROR << "init client config failed.";
          co_return nullptr;
        }
      co_await reconnect(client, this->weak_from_this());
    }
    else {
      ELOG_TRACE << "get free client{" << client.get() << "}. from queue";
    }
    co_return std::move(client);
  }

  void enqueue(
      coro_io::detail::client_queue<std::unique_ptr<client_t>>& clients,
      std::unique_ptr<client_t> client,
      std::chrono::milliseconds collect_time) {
    if (clients.enqueue(std::move(client)) == 1) {
      std::size_t expected = 0;
      if (clients.collecter_cnt_.compare_exchange_strong(expected, 1)) {
        ELOG_TRACE << "start timeout client collecter of client_pool{"
                   << host_name_ << "}";
        collect_idle_timeout_client(
            this->weak_from_this(), clients,
            (std::max)(collect_time, std::chrono::milliseconds{50}),
            pool_config_.idle_queue_per_max_clear_count)
            .directlyStart(
                [](auto&&) {
                },
                coro_io::get_global_executor());
      }
    }
  }

  void collect_free_client(std::unique_ptr<client_t> client) {
    if (pool_config_.max_connection_life_time < std::chrono::seconds::max()) {
      auto tp = client->get_create_time_point();
      if (std::chrono::steady_clock::now() - tp >
          pool_config_.max_connection_life_time) [[unlikely]] {
        ELOG_INFO << "client{" << client.get()
                  << "} live too long, we won't collect it";
        return;
      }
    }
    if (!client->has_closed()) {
      if (free_clients_.size() < pool_config_.max_connection) {
        ELOG_TRACE << "collect free client{" << client.get() << "} enqueue";
        enqueue(free_clients_, std::move(client), pool_config_.idle_timeout);
      }
      else {
        ELOG_TRACE << "out of max connection limit "
                   << pool_config_.max_connection << ", collect free client{"
                   << client.get() << "} enqueue short connect queue";
        enqueue(short_connect_clients_, std::move(client),
                pool_config_.short_connect_idle_timeout);
      }
      is_alive_ = true;
    }
    else {
      ELOG_TRACE << "client{" << client.get()
                 << "} is closed. we won't collect it";
    }

    return;
  };

  template <typename T>
  struct lazy_hacker {
    using type = void;
  };
  template <typename T>
  struct lazy_hacker<async_simple::coro::Lazy<T>> {
    using type = T;
  };
  template <typename T>
  using return_type =
      ylt::expected<typename lazy_hacker<util::function_return_type_t<T>>::type,
                    std::errc>;

 public:
  struct pool_config {
    uint32_t max_connection = 100;
    uint32_t connect_retry_count = 3;
    uint32_t idle_queue_per_max_clear_count = 1000;
    int32_t reuse_limit = -1;  // -1 means auto limit
    std::chrono::milliseconds reconnect_wait_time{1000};
    std::chrono::milliseconds idle_timeout{30000};
    std::chrono::milliseconds short_connect_idle_timeout{1000};
    std::chrono::milliseconds host_alive_detect_duration{
        30000}; /* zero means wont detect */
    typename client_t::config client_config;
    std::chrono::seconds dns_cache_update_duration{5 * 60};  // 5mins
    std::chrono::seconds max_connection_life_time = std::chrono::seconds::max();
  };

 private:
  struct private_construct_token {};

 public:
  static std::shared_ptr<client_pool> create(
      std::string_view host_name, const pool_config& pool_config = {},
      io_context_pool_t& io_context_pool = coro_io::g_io_context_pool()) {
    return std::make_shared<client_pool>(private_construct_token{}, host_name,
                                         pool_config, io_context_pool);
  }

  client_pool(private_construct_token t, std::string_view host_name,
              const pool_config& pool_config,
              io_context_pool_t& io_context_pool)
      : host_name_(host_name),
        pool_config_(pool_config),
        io_context_pool_(io_context_pool),
        free_clients_(pool_config.max_connection),
        eps_(std::make_shared<std::vector<asio::ip::tcp::endpoint>>()) {
    std::atomic_thread_fence(std::memory_order_seq_cst);
  };

  client_pool(private_construct_token t, client_pools_t* pools_manager_,
              std::string_view host_name, const pool_config& pool_config,
              io_context_pool_t& io_context_pool)
      : pools_manager_(pools_manager_),
        host_name_(host_name),
        pool_config_(pool_config),
        io_context_pool_(io_context_pool),
        free_clients_(pool_config.max_connection),
        eps_(std::make_shared<std::vector<asio::ip::tcp::endpoint>>()) {
    std::atomic_thread_fence(std::memory_order_seq_cst);
  };

  template <typename T>
  async_simple::coro::Lazy<return_type<T>> send_request(
      T op, typename client_t::config& client_config) {
    // return type: Lazy<expected<T::returnType,std::errc>>
    ELOG_TRACE << "try send request to " << host_name_;
    auto client = co_await get_client(client_config);
    watcher<uint64_t, std::memory_order_relaxed> w(inusing_client_cnt_);
    watcher<uint64_t, std::memory_order_release> w2(parallel_request_cnt_);
    if (!client) {
      ELOG_WARN << "send request to " << host_name_
                << " failed. connection refused.";
      co_return return_type<T>{ylt::unexpect, std::errc::connection_refused};
    }
    if constexpr (std::is_same_v<typename return_type<T>::value_type, void>) {
      co_await op(*client);
      collect_free_client(std::move(client));
      co_return return_type<T>{};
    }
    else {
      // enable reuse client limiter
      if constexpr (requires { op(client_reuse_hint{}, *client); }) {
        auto ret = co_await op(client_reuse_hint{}, *client);
        auto ret2 = co_await client_reuse_limiter(std::move(ret), *client);
        collect_free_client(std::move(client));
        co_return std::move(ret2);
      }
      else {
        auto ret = co_await op(*client);
        collect_free_client(std::move(client));
        co_return std::move(ret);
      }
    }
  }

  template <typename T>
  decltype(auto) send_request(T op) {
    return send_request(std::move(op), pool_config_.client_config);
  }

  /**
   * @brief approx free connection of client pools
   *
   * @return std::size_t
   */
  std::size_t free_client_count() const noexcept {
    return free_clients_.size() + short_connect_clients_.size();
  }
  /**
   * @brief approx connection of client pools
   *
   * @return std::size_t
   */
  std::size_t total_client_count() const noexcept {
    return free_client_count() +
           inusing_client_cnt_.load(std::memory_order::relaxed);
  }

 private:
  template <typename T>
  async_simple::coro::Lazy<T> make_ready_lazy(T t) {
    co_return std::move(t);
  }
  template <typename T>
  async_simple::coro::Lazy<T> client_reuse_limiter_impl(
      async_simple::coro::Lazy<T> lazy,
      watcher_weak<uint64_t, client_pool, std::memory_order_release>) {
    co_return co_await std::move(lazy);
  }
  template <typename T>
  async_simple::coro::Lazy<async_simple::coro::Lazy<T>> client_reuse_limiter(
      async_simple::coro::Lazy<T> lazy, client_t& cli) {
    auto limit = get_pool_config().reuse_limit;
    constexpr int rdma_reuse_limit = 24;
    constexpr int tcp_reuse_limit = 160;
    if (limit < 0) {
      // rdma
#ifdef YLT_ENABLE_IBV
      if (std::holds_alternative<coro_io::ib_socket_t::config_t>(
              cli.get_config().socket_config)) {
        limit = rdma_reuse_limit;
      }
#endif
      if (limit != rdma_reuse_limit) {
        limit = tcp_reuse_limit;
      }
    }
    if (limit < parallel_request_cnt_.load(std::memory_order_acquire)) {
      co_return client_reuse_limiter_impl(
          std::move(lazy), {parallel_request_cnt_, this->weak_from_this()});
    }
    else {
      co_return make_ready_lazy(co_await std::move(lazy));
    }
  }

 public:
  /**
   * @brief approx unfree connection of client pools
   *
   * @return std::size_t
   */
  std::size_t busy_client_count() const noexcept {
    return inusing_client_cnt_.load(std::memory_order::relaxed);
  }
  /**
   * @brief if host may not useable now.
   *
   * @return bool
   */
  bool is_alive() const noexcept { return is_alive_; }

  /**
   * @brief approx connection of client pools
   *
   * @return std::size_t
   */
  std::size_t size() const noexcept { return free_client_count(); }

  std::string_view get_host_name() const noexcept { return host_name_; }

  std::shared_ptr<std::vector<asio::ip::tcp::endpoint>> get_remote_endpoints()
      const noexcept {
    return eps_.load(std::memory_order_acquire);
  }

  const pool_config& get_pool_config() const noexcept { return pool_config_; }

 private:
  template <typename, typename>
  friend class client_pools;

  template <typename, typename>
  friend class load_balancer;

  template <typename T>
  async_simple::coro::Lazy<return_type<T>> send_request(
      T op, std::string_view endpoint,
      typename client_t::config& client_config) {
    // return type: Lazy<expected<T::returnType,std::errc>>
    ELOG_TRACE << "try send request to " << endpoint;
    auto client = co_await get_client(client_config);
    if (!client) {
      ELOG_WARN << "send request to " << endpoint
                << " failed. connection refused.";
      co_return return_type<T>{ylt::unexpect, std::errc::connection_refused};
    }
    if constexpr (std::is_same_v<typename return_type<T>::value_type, void>) {
      co_await op(*client, endpoint);
      collect_free_client(std::move(client));
      co_return return_type<T>{};
    }
    else {
      if constexpr (requires { op(client_reuse_hint{}, *client, endpoint); }) {
        auto ret = co_await op(client_reuse_hint{}, *client, endpoint);
        auto ret2 = co_await client_reuse_limiter(std::move(ret), *client);
        collect_free_client(std::move(client));
        co_return std::move(ret2);
      }
      else {
        auto ret = co_await op(*client, endpoint);
        collect_free_client(std::move(client));
        co_return std::move(ret);
      }
    }
  }

  template <typename T>
  decltype(auto) send_request(T op, std::string_view sv) {
    return send_request(std::move(op), sv, pool_config_.client_config);
  }

  coro_io::detail::client_queue<std::unique_ptr<client_t>> free_clients_;
  coro_io::detail::client_queue<std::unique_ptr<client_t>>
      short_connect_clients_;
  client_pools_t* pools_manager_ = nullptr;
  async_simple::Promise<async_simple::Unit> idle_timeout_waiter;
  std::atomic<uint64_t> inusing_client_cnt_, parallel_request_cnt_;
  std::string host_name_;
  pool_config pool_config_;
  io_context_pool_t& io_context_pool_;
  std::atomic<bool> is_alive_ = true;
  std::atomic<uint64_t> timepoint_;
  ylt::util::atomic_shared_ptr<std::vector<asio::ip::tcp::endpoint>> eps_;
  async_simple::coro::Mutex dns_cache_update_mutex_;
};

template <typename client_t,
          typename io_context_pool_t = coro_io::io_context_pool>
class client_pools {
  using client_pool_t = client_pool<client_t, io_context_pool_t>;

 public:
  client_pools(
      const typename client_pool_t::pool_config& pool_config = {},
      io_context_pool_t& io_context_pool = coro_io::g_io_context_pool())
      : io_context_pool_(io_context_pool), default_pool_config_(pool_config) {}
  auto send_request(std::string_view host_name, auto op)
      -> decltype(std::declval<client_pool_t>().send_request(std::move(op))) {
    auto pool = get_client_pool(host_name, default_pool_config_);
    auto ret = co_await pool->send_request(std::move(op));
    co_return ret;
  }
  auto send_request(std::string_view host_name,
                    typename client_pool_t::pool_config& pool_config, auto op)
      -> decltype(std::declval<client_pool_t>().send_request(std::move(op))) {
    auto pool = get_client_pool(host_name, pool_config);
    auto ret = co_await pool->send_request(std::move(op));
    co_return ret;
  }
  auto at(std::string_view host_name) {
    return get_client_pool(host_name, default_pool_config_);
  }
  auto at(std::string_view host_name,
          const typename client_pool_t::pool_config& pool_config) {
    return get_client_pool(host_name, pool_config);
  }
  auto operator[](std::string_view host_name) { return at(host_name); }
  auto get_io_context_pool() { return io_context_pool_; }

 private:
  std::shared_ptr<client_pool_t> get_client_pool(
      std::string_view host_name,
      const typename client_pool_t::pool_config& pool_config) {
    decltype(client_pool_manager_.end()) iter;
    bool has_inserted;
    {
#ifdef __cpp_lib_generic_unordered_lookup
      std::shared_lock shared_lock{mutex_};
      iter = client_pool_manager_.find(host_name);
#else
      std::string host_name_copy = std::string{host_name};
      std::shared_lock shared_lock{mutex_};
      iter = client_pool_manager_.find(host_name_copy);
#endif
      if (iter == client_pool_manager_.end()) {
        shared_lock.unlock();
        auto pool = std::make_shared<client_pool_t>(
            typename client_pool_t::private_construct_token{}, this, host_name,
            pool_config, io_context_pool_);
        {
          std::lock_guard lock{mutex_};
          std::tie(iter, has_inserted) =
              client_pool_manager_.emplace(host_name, nullptr);
          if (has_inserted) {
            iter->second = pool;
          }
        }
      }
      return iter->second;
    }
  }
  struct string_hash {
    using hash_type = std::hash<std::string_view>;
    using is_transparent = void;

    std::size_t operator()(std::string_view str) const {
      return hash_type{}(str);
    }
    std::size_t operator()(std::string const& str) const {
      return hash_type{}(str);
    }
  };
  typename client_pool_t::pool_config default_pool_config_{};
  std::unordered_map<std::string, std::shared_ptr<client_pool_t>, string_hash,
                     std::equal_to<>>
      client_pool_manager_;
  io_context_pool_t& io_context_pool_;
  std::shared_mutex mutex_;
};

template <typename client_t,
          typename io_context_pool_t = coro_io::io_context_pool>
inline client_pools<client_t, io_context_pool_t>& g_clients_pool(
    const typename client_pool<client_t, io_context_pool_t>::pool_config&
        pool_config = {},
    io_context_pool_t& io_context_pool = coro_io::g_io_context_pool()) {
  static client_pools<client_t, io_context_pool_t> _g_clients_pool(
      pool_config, io_context_pool);
  return _g_clients_pool;
}

}  // namespace coro_io
