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
#include <async_simple/coro/Lazy.h>

#include <atomic>
#include <memory>
#include <numeric>
#include <random>

#include "client_pool.hpp"
#include "io_context_pool.hpp"
namespace coro_io {

enum class load_balance_algorithm {
  RR = 0,  // round-robin
  WRR,     // weight round-robin
  random,
};

template <typename client_t, typename io_context_pool_t = io_context_pool>
class load_balancer {
  using client_pool_t = client_pool<client_t, io_context_pool_t>;
  using client_pools_t = client_pools<client_t, io_context_pool_t>;

 public:
  struct load_balancer_config {
    typename client_pool_t::pool_config pool_config;
    load_balance_algorithm lba = load_balance_algorithm::RR;
    ~load_balancer_config(){};
  };

 private:
  struct RRLoadbalancer {
    std::unique_ptr<std::atomic<uint32_t>> index =
        std::make_unique<std::atomic<uint32_t>>();
    async_simple::coro::Lazy<std::shared_ptr<client_pool_t>> operator()(
        const load_balancer& load_balancer) {
      auto i = index->fetch_add(1, std::memory_order_relaxed);
      co_return load_balancer
          .client_pools_[i % load_balancer.client_pools_.size()];
    }
  };

  /*
   Supposing that there is a server set ''S'' = {S0, S1, …, Sn-1};
   W(Si) indicates the weight of Si;
   ''i'' indicates the server selected last time, and ''i'' is initialized with
   -1;
   ''cw'' is the current weight in scheduling, and cw is initialized with zero;
   max(S) is the maximum weight of all the servers in S;
   gcd(S) is the greatest common divisor of all server weights in S;

   while (true) {
       i = (i + 1) mod n;
       if (i == 0) {
           cw = cw - gcd(S);
           if (cw <= 0) {
               cw = max(S);
               if (cw == 0)
               return NULL;
           }
       }
       if (W(Si) >= cw)
           return Si;
   }
  */
  struct WRRLoadbalancer {
    WRRLoadbalancer(const std::vector<int>& weights) : weights_(weights) {
      max_gcd_ = get_max_weight_gcd();
      max_weight_ = get_max_weight();
    }

    async_simple::coro::Lazy<std::shared_ptr<client_pool_t>> operator()(
        const load_balancer& load_balancer) {
      int selected = select_host_with_weight_round_robin();
      if (selected == -1) {
        selected = 0;
      }

      wrr_current_ = selected;
      co_return load_balancer
          .client_pools_[selected % load_balancer.client_pools_.size()];
    }

   private:
    int select_host_with_weight_round_robin() {
      while (true) {
        wrr_current_ = (wrr_current_ + 1) % weights_.size();
        if (wrr_current_ == 0) {
          weight_current_ = weight_current_ - max_gcd_;
          if (weight_current_ <= 0) {
            weight_current_ = max_weight_;
            if (weight_current_ == 0) {
              return -1;  // can't find max weight server
            }
          }
        }

        if (weights_[wrr_current_] >= weight_current_) {
          return wrr_current_;
        }
      }
    }

    int get_max_weight_gcd() {
      int res = weights_[0];
      int cur_max = 0, cur_min = 0;
      for (size_t i = 0; i < weights_.size(); i++) {
        cur_max = (std::max)(res, weights_[i]);
        cur_min = (std::min)(res, weights_[i]);
        res = std::gcd(cur_max, cur_min);
      }
      return res;
    }

    int get_max_weight() {
      return *std::max_element(weights_.begin(), weights_.end());
    }

    std::vector<int> weights_;
    int max_gcd_ = 0;
    int max_weight_ = 0;
    int wrr_current_ = -1;
    int weight_current_ = 0;
  };

  struct RandomLoadbalancer {
    async_simple::coro::Lazy<std::shared_ptr<client_pool_t>> operator()(
        const load_balancer& load_balancer) {
      static thread_local std::default_random_engine e(std::time(nullptr));
      std::uniform_int_distribution rnd{std::size_t{0},
                                        load_balancer.client_pools_.size() - 1};
      co_return load_balancer.client_pools_[rnd(e)];
    }
  };
  load_balancer() = default;

 public:
  load_balancer(load_balancer&& o)
      : config_(std::move(o.config_)),
        lb_worker(std::move(o.lb_worker)),
        client_pools_(std::move(o.client_pools_)){};
  load_balancer& operator=(load_balancer&& o) {
    this->config_ = std::move(o.config_);
    this->lb_worker = std::move(o.lb_worker);
    this->client_pools_ = std::move(o.client_pools_);
    return *this;
  }
  load_balancer(const load_balancer& o) = delete;
  load_balancer& operator=(const load_balancer& o) = delete;

  auto send_request(auto op, typename client_t::config& config)
      -> decltype(std::declval<client_pool_t>().send_request(std::move(op),
                                                             std::string_view{},
                                                             config)) {
    std::shared_ptr<client_pool_t> client_pool;
    if (client_pools_.size() > 1) {
      int cnt = 0;
      do {
        client_pool = co_await std::visit(
            [this](auto& worker) {
              return worker(*this);
            },
            lb_worker);
      } while (!client_pool->is_alive() && ++cnt <= size() * 2);
    }
    else {
      client_pool = client_pools_[0];
    }
    co_return co_await client_pool->send_request(
        std::move(op), client_pool->get_host_name(), config);
  }
  auto send_request(auto op) {
    return send_request(std::move(op), config_.pool_config.client_config);
  }

  static load_balancer create(
      const std::vector<std::string_view>& hosts,
      const load_balancer_config& config = {},
      const std::vector<int>& weights = {},
      client_pools_t& client_pools =
          g_clients_pool<client_t, io_context_pool_t>()) {
    load_balancer ch;
    ch.init(hosts, config, weights, client_pools);
    return ch;
  }

  /**
   * @brief return the load_balancer's hosts size.
   *
   * @return std::size_t
   */
  std::size_t size() const noexcept { return client_pools_.size(); }

 private:
  void init(const std::vector<std::string_view>& hosts,
            const load_balancer_config& config, const std::vector<int>& weights,
            client_pools_t& client_pools) {
    config_ = config;
    client_pools_.reserve(hosts.size());
    for (auto& host : hosts) {
      client_pools_.emplace_back(client_pools.at(host, config.pool_config));
    }
    switch (config_.lba) {
      case load_balance_algorithm::RR:
        lb_worker = RRLoadbalancer{};
        break;
      case load_balance_algorithm::WRR: {
        if (hosts.empty() || weights.empty()) {
          throw std::invalid_argument("host/weight list is empty!");
        }
        if (hosts.size() != weights.size()) {
          throw std::invalid_argument("hosts count is not equal with weights!");
        }
        lb_worker = WRRLoadbalancer(weights);
      } break;
      case load_balance_algorithm::random:
      default:
        lb_worker = RandomLoadbalancer{};
    }
    return;
  }
  load_balancer_config config_;
  std::variant<RRLoadbalancer, WRRLoadbalancer, RandomLoadbalancer> lb_worker;
  std::vector<std::shared_ptr<client_pool_t>> client_pools_;
};

}  // namespace coro_io
