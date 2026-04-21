#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>

#include "counter.hpp"
#include "dynamic_metric.hpp"
#include "summary_impl.hpp"
#if __has_include("ylt/util/concurrentqueue.h")
#include "ylt/util/concurrentqueue.h"
#else
#include "cinatra/ylt/util/concurrentqueue.h"
#endif

namespace ylt::metric {
#ifdef CINATRA_ENABLE_METRIC_JSON
struct json_summary_metric_t {
  std::vector<std::pair<std::string_view, std::string_view>> labels;
  std::vector<std::pair<float, float>> quantiles;
  uint64_t count;
  double sum;
};
YLT_REFL(json_summary_metric_t, labels, quantiles, count, sum);
struct json_summary_t {
  std::string_view name;
  std::string_view help;
  std::string_view type;
  std::vector<json_summary_metric_t> metrics;
};
YLT_REFL(json_summary_t, name, help, type, metrics);
#endif

namespace detail {
template <typename O, typename K, typename V>
inline void vector_combine(O& result, const K& vec1, const V& vec2) {
  assert(vec1.size() == vec2.size());
  result.reserve(vec1.size());
  for (std::size_t i = 0; i < vec1.size(); ++i) {
    result.emplace_back(vec1[i], vec2[i]);
  }
}
}  // namespace detail

class summary_t : public static_metric {
 public:
  summary_t(std::string name, std::string help, std::vector<double> quantiles,
            std::chrono::seconds max_age = std::chrono::seconds{0})
      : static_metric(MetricType::Summary, std::move(name), std::move(help)),
        quantiles_(std::move(quantiles)),
        impl_(quantiles_,
              std::chrono::duration_cast<std::chrono::seconds>(max_age)) {
    if (!std::is_sorted(quantiles_.begin(), quantiles_.end())) {
      std::sort(quantiles_.begin(), quantiles_.end());
    }
    auto iter = std::unique(quantiles_.begin(), quantiles_.end());
    quantiles_.erase(iter, quantiles_.end());
  }

  summary_t(std::string name, std::string help, std::vector<double> quantiles,
            std::map<std::string, std::string> static_labels,
            std::chrono::seconds max_age = std::chrono::seconds{0})
      : static_metric(MetricType::Summary, std::move(name), std::move(help),
                      std::move(static_labels)),
        quantiles_(std::move(quantiles)),
        impl_(quantiles_,
              std::chrono::duration_cast<std::chrono::seconds>(max_age)) {
    if (!std::is_sorted(quantiles_.begin(), quantiles_.end())) {
      std::sort(quantiles_.begin(), quantiles_.end());
    }
    auto iter = std::unique(quantiles_.begin(), quantiles_.end());
    quantiles_.erase(iter, quantiles_.end());
  }

  void observe(float value) {
    has_refreshed_.store(true, std::memory_order_relaxed);
    impl_.insert(value);
  }

  std::vector<float> get_rates() {
    uint64_t count;
    double sum;
    return get_rates(sum, count);
  }
  std::vector<float> get_rates(uint64_t& count) {
    double sum;
    return get_rates(sum, count);
  }
  std::vector<float> get_rates(double& sum) {
    uint64_t count;
    return get_rates(sum, count);
  }

  std::vector<float> get_rates(double& sum, uint64_t& count) {
    return impl_.stat(sum, count);
  }

  virtual void serialize(std::string& str) override {
    if (quantiles_.empty()) {
      return;
    }
    double sum = 0;
    uint64_t count = 0;
    auto rates = get_rates(sum, count);
    if (count == 0 && !has_refreshed_.load(std::memory_order_relaxed)) {
      return;
    }
    serialize_head(str);

    for (size_t i = 0; i < quantiles_.size(); i++) {
      str.append(name_);
      str.append("{");
      if (!labels_name_.empty()) {
        build_label_string(str, labels_name_, labels_value_);
        str.append(",");
      }

      str.append("quantile=\"");
      str.append(std::to_string(quantiles_[i])).append("\"} ");
      str.append(std::to_string(rates[i])).append("\n");
    }

    str.append(name_).append("_sum ").append(std::to_string(sum)).append("\n");
    str.append(name_)
        .append("_count ")
        .append(std::to_string((uint64_t)count))
        .append("\n");
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  virtual void serialize_to_json(std::string& str) override {
    if (quantiles_.empty()) {
      return;
    }

    json_summary_t summary{name_, help_, metric_name()};
    json_summary_metric_t metric;
    auto rates = get_rates(metric.sum, metric.count);
    detail::vector_combine(metric.quantiles, quantiles_, rates);
    if (metric.count == 0 && !has_refreshed_.load(std::memory_order_relaxed)) {
      return;
    }
    detail::vector_combine(metric.labels, labels_name(), labels_value_);
    summary.metrics.push_back(std::move(metric));
    iguana::to_json(summary, str);
  }
#endif

 private:
  std::atomic<bool> has_refreshed_;
  std::vector<double> quantiles_;
  ylt::metric::detail::summary_impl<uint64_t> impl_;
};

template <size_t N>
class basic_dynamic_summary
    : public dynamic_metric_impl<ylt::metric::detail::summary_impl<uint32_t>,
                                 N> {
 private:
  using Base =
      dynamic_metric_impl<ylt::metric::detail::summary_impl<uint32_t>, N>;

 public:
  basic_dynamic_summary(std::string name, std::string help,
                        std::vector<double> quantiles,
                        std::array<std::string, N> labels_name,
                        std::chrono::seconds max_age = std::chrono::seconds{0})
      : Base(MetricType::Summary, std::move(name), std::move(help),
             std::move(labels_name)),
        quantiles_(std::move(quantiles)),
        max_age_(max_age) {
    if (!std::is_sorted(quantiles_.begin(), quantiles_.end()))
      std::sort(quantiles_.begin(), quantiles_.end());
  }

  basic_dynamic_summary(std::string name, std::string help,
                        std::vector<double> quantiles,
                        std::map<std::string, std::string> static_labels,
                        std::array<std::string, N> labels_name,
                        std::chrono::seconds max_age = std::chrono::seconds{0})
      : Base(MetricType::Summary, std::move(name), std::move(help),
             std::move(labels_name)),
        quantiles_(std::move(quantiles)),
        max_age_(max_age),
        static_labels_(static_labels) {
    if (!std::is_sorted(quantiles_.begin(), quantiles_.end()))
      std::sort(quantiles_.begin(), quantiles_.end());
    Base::build_label_string(static_labels_str_, static_labels_);
  }

  void observe(const std::array<std::string, N>& labels_value, float value) {
    Base::try_emplace(labels_value, quantiles_, max_age_)
        .first->value.insert(value);
  }

  std::vector<float> get_rates(const std::array<std::string, N>& labels_value) {
    double sum;
    uint64_t count;
    return Base::try_emplace(labels_value, quantiles_, max_age_)
        .first->value.get_rates(sum, count);
  }

  std::vector<float> get_rates(const std::array<std::string, N>& labels_value,
                               uint64_t& count) {
    double sum;
    return Base::try_emplace(labels_value, quantiles_, max_age_)
        .first->value.get_rates(sum, count);
  }

  std::vector<float> get_rates(const std::array<std::string, N>& labels_value,
                               double& sum) {
    uint64_t count;
    return Base::try_emplace(labels_value, quantiles_, max_age_)
        .first->value.get_rates(sum, count);
  }

  std::vector<float> get_rates(const std::array<std::string, N>& labels_value,
                               double& sum, uint64_t& count) {
    return Base::try_emplace(labels_value, quantiles_, max_age_)
        .first->value.stat(sum, count);
  }

  virtual void serialize(std::string& str) override {
    double sum = 0;
    uint64_t count = 0;
    auto map = Base::copy();
    for (auto& e : map) {
      auto& labels_value = e->label;
      auto& summary_value = e->value;
      auto rates = summary_value.stat(sum, count);

      std::string dynamic_labels_str;
      Base::build_label_string(dynamic_labels_str, Base::labels_name_,
                               labels_value);

      for (size_t i = 0; i < quantiles_.size(); i++) {
        str.append(Base::name_);
        str.append("{");
        str.append(Base::join_string(static_labels_str_, dynamic_labels_str));
        str.append(",");
        str.append("quantile=\"");
        str.append(std::to_string(quantiles_[i])).append("\"} ");
        str.append(std::to_string(rates[i])).append("\n");
      }
      str.append(Base::name_).append("_sum ");
      str.append("{");
      str.append(Base::join_string(static_labels_str_, dynamic_labels_str));
      str.append("} ");
      str.append(std::to_string(sum)).append("\n");

      str.append(Base::name_).append("_count ");
      str.append("{");
      str.append(Base::join_string(static_labels_str_, dynamic_labels_str));
      str.append("} ");
      str.append(std::to_string((uint64_t)count)).append("\n");
    }
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  virtual void serialize_to_json(std::string& str) override {
    auto map = Base::copy();
    if (map.empty()) {
      return;
    }
    json_summary_t summary{Base::name_, Base::help_, Base::metric_name()};
    summary.metrics.reserve(map.size());
    for (size_t i = 0; i < map.size(); ++i) {
      auto& labels_value = map[i]->label;
      auto& summary_value = map[i]->value;
      double sum = 0;
      uint64_t count = 0;
      auto rates = summary_value.stat(sum, count);
      if (count == 0)
        continue;
      summary.metrics.emplace_back();
      json_summary_metric_t& metric = summary.metrics.back();
      metric.count = count;
      metric.sum = sum;
      detail::vector_combine(metric.quantiles, quantiles_, rates);
      for (auto& e : static_labels_) {
        metric.labels.emplace_back(e.first, e.second);
      }
      detail::vector_combine(metric.labels, Base::labels_name(), labels_value);
    }
    iguana::to_json(summary, str);
  }
#endif

 private:
  std::vector<double> quantiles_;
  std::chrono::seconds max_age_;
  std::string static_labels_str_;
  std::map<std::string, std::string> static_labels_;
};

using dynamic_summary_1 = basic_dynamic_summary<1>;
using dynamic_summary_2 = basic_dynamic_summary<2>;
using dynamic_summary = dynamic_summary_2;
using dynamic_summary_3 = basic_dynamic_summary<3>;
using dynamic_summary_4 = basic_dynamic_summary<4>;
using dynamic_summary_5 = basic_dynamic_summary<5>;
}  // namespace ylt::metric