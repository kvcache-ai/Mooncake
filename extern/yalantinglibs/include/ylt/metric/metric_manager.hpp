#pragma once
#include <iostream>
#include <shared_mutex>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>

#include "async_simple/coro/Lazy.h"
#include "async_simple/coro/SyncAwait.h"
#if __has_include("ylt/coro_io/coro_io.hpp")
#include "ylt/coro_io/coro_io.hpp"
#else
#include "cinatra/ylt/coro_io/coro_io.hpp"
#endif
#include "metric.hpp"
#include "ylt/util/map_sharded.hpp"

namespace ylt::metric {
class manager_helper {
 public:
  static bool register_metric(auto& metric_map, auto metric) {
    if (metric::metric_t::g_user_metric_count > ylt_metric_capacity) {
      std::cerr << "metric count at capacity size: "
                << metric::metric_t::g_user_metric_count << std::endl;
      return false;
    }
    auto&& [it, r] = metric_map.try_emplace(metric->str_name(), metric);
    if (!r) {
      std::cerr << "duplicate registered metric name: " << metric->str_name()
                << std::endl;
      return false;
    }

    return true;
  }

  static std::string serialize(
      const std::vector<std::shared_ptr<metric_t>>& metrics) {
    std::string str;
    for (auto& m : metrics) {
      m->serialize(str);
    }
    return str;
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  static std::string serialize_to_json(
      const std::vector<std::shared_ptr<metric_t>>& metrics) {
    std::string str;
    str.append("[");
    for (auto& m : metrics) {
      size_t start = str.size();
      m->serialize_to_json(str);
      if (str.size() > start)
        str.append(",");
    }

    if (str.size() == 1) {
      str.append("]");
    }
    else {
      str.back() = ']';
    }

    str.back() = ']';
    return str;
  }
#endif

  static std::vector<std::shared_ptr<metric_t>> filter_metrics_by_name(
      auto& metrics, const std::regex& name_regex) {
    std::vector<std::shared_ptr<metric_t>> filtered_metrics;
    for (auto& m : metrics) {
      if (std::regex_match(m->str_name(), name_regex)) {
        filtered_metrics.push_back(m);
      }
    }
    return filtered_metrics;
  }

  static std::vector<std::shared_ptr<metric_t>> filter_metrics_by_label_name(
      auto& metrics, const std::regex& label_name_regex) {
    std::vector<std::shared_ptr<metric_t>> filtered_metrics;
    for (auto& m : metrics) {
      const auto& labels_name = m->labels_name();
      for (auto& label_name : labels_name) {
        if (std::regex_match(label_name, label_name_regex)) {
          filtered_metrics.push_back(m);
        }
      }
    }
    return filtered_metrics;
  }

  static std::vector<std::shared_ptr<metric_t>> filter_metrics_by_label_value(
      auto& metrics, const std::regex& label_value_regex) {
    std::vector<std::shared_ptr<metric_t>> filtered_metrics;
    for (auto& m : metrics) {
      if (m->has_label_value(label_value_regex)) {
        filtered_metrics.push_back(m);
      }
    }
    return filtered_metrics;
  }

  static std::vector<std::shared_ptr<metric_t>> filter_metrics(
      auto& metrics, const metric_filter_options& options) {
    if (!(options.name_regex || options.label_regex ||
          options.label_value_regex)) {
      return metrics;
    }

    std::vector<std::shared_ptr<metric_t>> filtered_metrics = metrics;
    if (options.name_regex) {
      filtered_metrics = filter_metrics_by_name(metrics, *options.name_regex);
      if (filtered_metrics.empty()) {
        return {};
      }
    }

    if (options.label_regex) {
      filtered_metrics =
          filter_metrics_by_label_name(filtered_metrics, *options.label_regex);
      if (filtered_metrics.empty()) {
        return {};
      }
    }

    if (options.label_value_regex) {
      filtered_metrics = filter_metrics_by_label_value(
          filtered_metrics, *options.label_value_regex);
      if (filtered_metrics.empty()) {
        return {};
      }
    }

    if (!options.is_white) {
      for (auto& m : filtered_metrics) {
        std::erase_if(metrics, [&](auto t) {
          return t == m;
        });
      }
      return metrics;
    }

    return filtered_metrics;
  }

  static void filter_by_label_name(
      std::vector<std::shared_ptr<metric_t>>& filtered_metrics,
      std::shared_ptr<metric_t> m, const metric_filter_options& options) {
    if (!options.label_regex) {
      return;
    }
    const auto& labels_name = m->labels_name();
    for (auto& label_name : labels_name) {
      if (std::regex_match(label_name, *options.label_regex)) {
        filtered_metrics.push_back(m);
      }
    }
  }
};

template <typename Tag>
class static_metric_manager {
 public:
  static_metric_manager(static_metric_manager const&) = delete;
  static_metric_manager(static_metric_manager&&) = delete;
  static_metric_manager& operator=(static_metric_manager const&) = delete;
  static_metric_manager& operator=(static_metric_manager&&) = delete;

  static std::shared_ptr<static_metric_manager<Tag>> instance() {
    static auto inst = std::shared_ptr<static_metric_manager<Tag>>(
        new static_metric_manager<Tag>());
    return inst;
  }

  template <typename T, typename... Args>
  std::pair<std::error_code, std::shared_ptr<T>> create_metric_static(
      const std::string& name, const std::string& help, Args&&... args) {
    auto m = std::make_shared<T>(name, help, std::forward<Args>(args)...);
    bool r = register_metric(m);
    if (!r) {
      return std::make_pair(std::make_error_code(std::errc::invalid_argument),
                            nullptr);
    }

    return std::make_pair(std::error_code{}, m);
  }

  bool register_metric(std::shared_ptr<static_metric> metric) {
    return manager_helper::register_metric(metric_map_, metric);
  }

  size_t metric_count() { return metric_map_.size(); }

  auto metric_map() { return metric_map_; }

  auto collect() {
    std::vector<std::shared_ptr<metric_t>> metrics;

    for (auto& pair : metric_map_) {
      metrics.push_back(pair.second);
    }

    return metrics;
  }

  std::string serialize(const std::vector<std::shared_ptr<metric_t>>& metrics) {
    return manager_helper::serialize(metrics);
  }

  std::string serialize_static() {
    return manager_helper::serialize(collect());
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string serialize_to_json_static() {
    return manager_helper::serialize_to_json(collect());
  }
#endif
  template <typename T>
  std::shared_ptr<T> get_metric_static(const std::string& name) {
    static_assert(std::is_base_of_v<static_metric, T>,
                  "must be dynamic metric");
    auto it = metric_map_.find(name);
    if (it == metric_map_.end()) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<T>(it->second);
  }

  std::shared_ptr<static_metric> get_metric_by_name(const std::string& name) {
    auto it = metric_map_.find(name);
    if (it == metric_map_.end()) {
      return nullptr;
    }

    return it->second;
  }

  std::vector<std::shared_ptr<static_metric>> get_metric_by_label(
      const std::map<std::string, std::string>& labels) {
    std::vector<std::shared_ptr<static_metric>> metrics;

    for (auto& [key, m] : metric_map_) {
      if (m->get_static_labels() == labels) {
        metrics.push_back(m);
      }
    }

    return metrics;
  }

  std::vector<std::shared_ptr<metric_t>> filter_metrics_static(
      const metric_filter_options& options) {
    auto metrics = collect();
    return manager_helper::filter_metrics(metrics, options);
  }

  std::vector<std::shared_ptr<metric_t>> filter_metrics_by_label_value(
      const std::regex& label_regex) {
    auto metrics = collect();
    return manager_helper::filter_metrics_by_label_value(metrics, label_regex);
  }

 private:
  static_metric_manager() = default;

  std::unordered_map<std::string, std::shared_ptr<static_metric>> metric_map_;
};

// using metric_manager_t = static_metric_manager;

template <typename Tag>
class dynamic_metric_manager {
 public:
  dynamic_metric_manager(dynamic_metric_manager const&) = delete;
  dynamic_metric_manager(dynamic_metric_manager&&) = delete;
  dynamic_metric_manager& operator=(dynamic_metric_manager const&) = delete;
  dynamic_metric_manager& operator=(dynamic_metric_manager&&) = delete;

  static std::shared_ptr<dynamic_metric_manager<Tag>> instance() {
    static auto inst = std::shared_ptr<dynamic_metric_manager<Tag>>(
        new dynamic_metric_manager<Tag>());
    if (ylt_label_max_age.count() > 0) {
      inst->clean_label_expired();
    }
    return inst;
  }

  ~dynamic_metric_manager() {
    asio::post(executor_->get_executor()->get_asio_executor(), [this] {
      std::error_code ec;
      timer_.cancel(ec);
      has_cancel_ = true;
    });

    executor_->stop();
  }

  template <typename T, typename... Args>
  std::pair<std::error_code, std::shared_ptr<T>> create_metric_dynamic(
      const std::string& name, const std::string& help, Args&&... args) {
    auto m = std::make_shared<T>(name, help, std::forward<Args>(args)...);
    bool r = register_metric(m);
    if (!r) {
      return std::make_pair(std::make_error_code(std::errc::invalid_argument),
                            nullptr);
    }

    return std::make_pair(std::error_code{}, m);
  }

  bool register_metric(std::shared_ptr<dynamic_metric> metric) {
    return manager_helper::register_metric(metric_map_, metric);
  }

  bool register_metric(std::vector<std::shared_ptr<dynamic_metric>> metrics) {
    bool r = false;
    for (auto& m : metrics) {
      r = register_metric(m);
      if (!r) {
        break;
      }
    }

    return r;
  }

  std::string serialize_dynamic() {
    return manager_helper::serialize(collect());
  }

  std::string serialize(const std::vector<std::shared_ptr<metric_t>>& metrics) {
    return manager_helper::serialize(metrics);
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  std::string serialize_to_json_dynamic() {
    return manager_helper::serialize_to_json(collect());
  }
#endif

  bool remove_metric(const std::string& name) {
    return metric_map_.erase(name);
  }

  bool remove_metric(std::shared_ptr<dynamic_metric> metric) {
    if (metric == nullptr) {
      return false;
    }
    return remove_metric(metric->str_name());
  }

  void remove_metric(const std::vector<std::string>& names) {
    if (names.empty()) {
      return;
    }
    for (auto& name : names) {
      remove_metric(name);
    }
  }

  void remove_metric(std::vector<std::shared_ptr<dynamic_metric>> metrics) {
    if (metrics.empty()) {
      return;
    }

    for (auto& metric : metrics) {
      remove_metric(metric);
    }
  }

  void remove_label_value(const std::map<std::string, std::string>& labels) {
    metric_map_.for_each([&](auto& m) {
      auto&& [_, metric] = m;
      metric->remove_label_value(labels);
    });
  }

  void remove_metric_by_label(
      const std::map<std::string, std::string>& labels) {
    metric_map_.erase_if([&](auto& metric) {
      auto&& [_, m] = metric;
      const auto& labels_name = m->labels_name();
      if (labels.size() > labels_name.size()) {
        return false;
      }

      if (labels.size() == labels_name.size()) {
        std::vector<std::string> label_value;
        for (auto& lb_name : labels_name) {
          if (auto i = labels.find(lb_name); i != labels.end()) {
            label_value.push_back(i->second);
          }
        }
        return m->has_label_value(label_value);
      }
      else {
        for (auto& label : labels) {
          if (auto i = std::find(labels_name.begin(), labels_name.end(),
                                 label.first);
              i != labels_name.end()) {
            if (!m->has_label_value(label.second)) {
              return false;
            }
          }
          else {
            return false;
          }
        }
        return true;
      }
    });
  }

  void remove_metric_by_label_name(
      const std::vector<std::string>& labels_name) {
    metric_map_.erase_one([&](auto& m) {
      auto&& [name, metric] = m;
      return metric->labels_name() == labels_name;
    });
  }

  void remove_metric_by_label_name(std::string_view labels_name) {
    metric_map_.erase_if([&](auto& m) {
      auto&& [_, metric] = m;
      auto& names = metric->labels_name();
      return std::find(names.begin(), names.end(), labels_name) != names.end();
    });
  }

  size_t metric_count() { return metric_map_.size(); }
  std::vector<std::shared_ptr<metric_t>> collect() const {
    return metric_map_.template copy<std::shared_ptr<metric_t>>();
  }

  template <typename T>
  std::shared_ptr<T> get_metric_dynamic(const std::string& name) {
    static_assert(std::is_base_of_v<dynamic_metric, T>,
                  "must be dynamic metric");
    return std::dynamic_pointer_cast<T>(metric_map_.find(name));
  }

  std::shared_ptr<dynamic_metric> get_metric_by_name(std::string_view name) {
    return metric_map_.find(name);
  }

  std::vector<std::shared_ptr<dynamic_metric>> get_metric_by_label(
      const std::vector<std::pair<std::string, std::string>>& labels) {
    std::vector<std::string> label_value;
    for (auto& [k, v] : labels) {
      label_value.push_back(v);
    }

    return get_metric_by_label_value(label_value);
  }

  std::vector<std::shared_ptr<dynamic_metric>> get_metric_by_label_name(
      const std::vector<std::string>& labels_name) {
    return metric_map_.template copy<std::shared_ptr<dynamic_metric>>(
        [&](auto& m) {
          return m->labels_name() == labels_name;
        });
  }

  std::vector<std::shared_ptr<metric_t>> filter_metrics_dynamic(
      const metric_filter_options& options) {
    auto metrics = collect();
    return manager_helper::filter_metrics(metrics, options);
  }

  std::vector<std::shared_ptr<metric_t>> filter_metrics_by_label_value(
      const std::regex& label_regex) {
    auto metrics = collect();
    return manager_helper::filter_metrics_by_label_value(metrics, label_regex);
  }

 private:
  void clean_label_expired() {
    check_label_expired(timer_)
        .via(executor_->get_executor())
        .start([](auto&&) {
        });
  }

  async_simple::coro::Lazy<void> check_label_expired(auto& timer) {
    while (true) {
      if (has_cancel_) {
        co_return;
      }
      timer.expires_after(ylt_label_check_expire_duration);
      bool r = co_await timer.async_await();
      if (!r) {
        co_return;
      }
      metric_map_.for_each([](auto& metric) {
        metric.second->clean_expired_label();
      });
    }
  }

  dynamic_metric_manager()
      : metric_map_(
            std::min<unsigned>(std::thread::hardware_concurrency(), 128u)),
        executor_(coro_io::create_io_context_pool(1)),
        timer_(executor_->get_executor()) {}

  std::vector<std::shared_ptr<dynamic_metric>> get_metric_by_label_value(
      const std::vector<std::string>& label_value) {
    return metric_map_.template copy<std::shared_ptr<dynamic_metric>>(
        [&label_value](auto& metric) {
          return metric->has_label_value(label_value);
        });
  }

  void remove_metric_by_label_value(
      const std::vector<std::string>& label_value) {
    metric_map_.erase_if([&](auto& metric) {
      return metric.second->has_label_value(label_value);
    });
  }

  template <size_t seed = 131>
  struct my_hash {
    using is_transparent = void;
    std::size_t operator()(std::string_view s) const noexcept {
      unsigned int hash = 0;
      for (auto ch : s) {
        hash = hash * seed + ch;
      }
      return hash;
    }
  };

  util::map_sharded_t<
      std::unordered_map<std::string, std::shared_ptr<dynamic_metric>>,
      my_hash<>>
      metric_map_;
  std::shared_ptr<coro_io::io_context_pool> executor_ = nullptr;
  bool has_cancel_ = false;
  coro_io::period_timer timer_;
  inline static std::once_flag once_;
};

struct ylt_default_metric_tag_t {};
using default_static_metric_manager =
    static_metric_manager<ylt_default_metric_tag_t>;
using default_dynamiv_metric_manager =
    dynamic_metric_manager<ylt_default_metric_tag_t>;

template <typename Tag>
struct metric_manager_t;

struct ylt_system_tag_t {};
using system_metric_manager = static_metric_manager<ylt_system_tag_t>;

template <typename... Args>
struct metric_collector_t {
  static std::string serialize() {
    auto vec = get_all_metrics();
    return manager_helper::serialize(vec);
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  static std::string serialize_to_json() {
    auto vec = get_all_metrics();
    return manager_helper::serialize_to_json(vec);
  }

  static std::string serialize_to_json(
      const std::vector<std::shared_ptr<metric_t>>& metrics) {
    return manager_helper::serialize_to_json(metrics);
  }
#endif

  static std::string serialize(
      const std::vector<std::shared_ptr<metric_t>>& metrics) {
    return manager_helper::serialize(metrics);
  }

  static std::vector<std::shared_ptr<metric_t>> get_all_metrics() {
    std::vector<std::shared_ptr<metric_t>> vec;
    (append_vector<Args>(vec), ...);
    return vec;
  }

  static std::vector<std::shared_ptr<metric_t>> filter_metrics(
      const metric_filter_options& options) {
    auto vec = get_all_metrics();
    return manager_helper::filter_metrics(vec, options);
  }

 private:
  template <typename T>
  static void append_vector(std::vector<std::shared_ptr<metric_t>>& vec) {
    auto v = T::instance()->collect();
    vec.insert(vec.end(), v.begin(), v.end());
  }
};
}  // namespace ylt::metric