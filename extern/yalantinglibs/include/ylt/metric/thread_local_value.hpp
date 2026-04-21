#pragma once
#include <atomic>
#include <chrono>
#include <cstddef>
#include <thread>
#include <vector>

#include "ylt/metric/metric.hpp"

namespace ylt::metric {
inline uint32_t get_round_index(uint32_t size) {
  static std::atomic<uint32_t> round = 0;
  static thread_local uint32_t index = round++;
  return index % size;
}

namespace detail {
template <typename value_type>
static value_type inc_impl(std::atomic<value_type> &obj, value_type value) {
  if constexpr (!requires {
                  std::atomic<value_type>{}.fetch_add(value_type{});
                }) {
    value_type v = obj.load(std::memory_order::relaxed);
    while (!std::atomic_compare_exchange_weak(&obj, &v, v + value))
      ;
    return v;
  }
  else {
    return obj.fetch_add(value, std::memory_order::relaxed);
  }
}
template <typename value_type>
static value_type dec_impl(std::atomic<value_type> &obj, value_type value) {
  if constexpr (!requires {
                  std::atomic<value_type>{}.fetch_add(value_type{});
                }) {
    value_type v = obj.load(std::memory_order::relaxed);
    while (!std::atomic_compare_exchange_weak(&obj, &v, v - value))
      ;
    return v;
  }
  else {
    return obj.fetch_sub(value, std::memory_order::relaxed);
  }
}
}  // namespace detail

template <typename value_type>
class thread_local_value {
  friend class metric_t;

 public:
  thread_local_value(uint32_t dupli_count = std::thread::hardware_concurrency())
      : duplicates_(dupli_count) {}

  ~thread_local_value() {
    for (auto &t : duplicates_) {
      if (t) {
        delete t.load();
      }
    }
  }

  thread_local_value(const thread_local_value &other)
      : duplicates_(other.duplicates_.size()) {
    for (size_t i = 0; i < other.duplicates_.size(); i++) {
      if (other.duplicates_[i]) {
        auto ptr =
            new std::atomic<value_type>(other.duplicates_[i].load()->load());
        duplicates_[i] = ptr;
      }
    }
  }

  thread_local_value &operator=(const thread_local_value &other) {
    for (size_t i = 0; i < other.duplicates_.size(); i++) {
      if (other.duplicates_[i]) {
        auto ptr =
            new std::atomic<value_type>(other.duplicates_[i].load()->load());
        duplicates_[i] = ptr;
      }
    }
    return *this;
  }

  thread_local_value(thread_local_value &&other) {
    duplicates_ = std::move(other.duplicates_);
  }

  thread_local_value &operator=(thread_local_value &&other) {
    duplicates_ = std::move(other.duplicates_);
    return *this;
  }

  void inc(value_type value = 1) { detail::inc_impl(local_value(), value); }

  void dec(value_type value = 1) { detail::dec_impl(local_value(), value); }

  value_type update(value_type value = 1) {
    value_type val = get_value(0).exchange(value, std::memory_order::relaxed);
    for (size_t i = 1; i < duplicates_.size(); i++) {
      if (duplicates_[i]) {
        val += duplicates_[i].load()->exchange(0, std::memory_order::relaxed);
      }
    }
    return val;
  }

  value_type reset() { return update(0); }

  auto &local_value() {
    auto index = get_round_index(duplicates_.size());
    return get_value(index);
  }

  auto &get_value(size_t index) {
    if (duplicates_[index] == nullptr) {
      auto ptr = new std::atomic<value_type>(0);

      std::atomic<value_type> *expected = nullptr;
      if (!duplicates_[index].compare_exchange_strong(expected, ptr)) {
        delete ptr;
      }
    }
    return *duplicates_[index];
  }

  value_type value() const {
    value_type val = 0;
    for (auto &t : duplicates_) {
      if (t) {
        val += t.load()->load();
      }
    }
    return val;
  }

 private:
  std::vector<std::atomic<std::atomic<value_type> *>> duplicates_;
  std::chrono::system_clock::time_point created_time_{};
};
}  // namespace ylt::metric