#pragma once
#include <atomic>
#include <chrono>

#include "counter.hpp"
#include "ylt/metric/metric.hpp"

namespace ylt::metric {

template <typename value_type>
class basic_static_gauge : public basic_static_counter<value_type> {
  using metric_t::set_metric_type;
  using basic_static_counter<value_type>::default_label_value_;
  using metric_t::labels_value_;
  using basic_static_counter<value_type>::dupli_count_;
  using basic_static_counter<value_type>::has_change_;

 public:
  basic_static_gauge(std::string name, std::string help, size_t dupli_count = 2)
      : basic_static_counter<value_type>(std::move(name), std::move(help),
                                         dupli_count) {
    set_metric_type(MetricType::Gauge);
  }

  basic_static_gauge(std::string name, std::string help,
                     std::map<std::string, std::string> labels,
                     size_t dupli_count = 2)
      : basic_static_counter<value_type>(std::move(name), std::move(help),
                                         std::move(labels), dupli_count) {
    set_metric_type(MetricType::Gauge);
  }

  void dec(value_type value = 1) {
    if (!has_change_) [[unlikely]] {
      has_change_ = true;
    }
    default_label_value_.dec(value);
  }
};
using gauge_t = basic_static_gauge<int64_t>;
using gauge_d = basic_static_gauge<double>;

template <typename value_type, uint8_t N>
class basic_dynamic_gauge : public basic_dynamic_counter<value_type, N> {
  using metric_t::set_metric_type;
  using Base = basic_dynamic_counter<value_type, N>;

 public:
  basic_dynamic_gauge(std::string name, std::string help,
                      std::array<std::string, N> labels_name)
      : Base(std::move(name), std::move(help), std::move(labels_name)) {
    set_metric_type(MetricType::Gauge);
  }

  void dec(const std::array<std::string, N>& labels_value,
           value_type value = 1) {
    detail::dec_impl(Base::try_emplace(labels_value).first->value, value);
  }
};

using dynamic_gauge_1t = basic_dynamic_gauge<int64_t, 1>;
using dynamic_gauge_1d = basic_dynamic_gauge<double, 1>;

using dynamic_gauge_t = basic_dynamic_gauge<int64_t, 2>;
using dynamic_gauge_d = basic_dynamic_gauge<double, 2>;
using dynamic_gauge_2t = dynamic_gauge_t;
using dynamic_gauge_2d = dynamic_gauge_d;

using dynamic_gauge_3t = basic_dynamic_gauge<int64_t, 3>;
using dynamic_gauge_3d = basic_dynamic_gauge<double, 3>;

using dynamic_gauge_4t = basic_dynamic_gauge<int64_t, 4>;
using dynamic_gauge_4d = basic_dynamic_gauge<double, 4>;

using dynamic_gauge_5t = basic_dynamic_gauge<int64_t, 5>;
using dynamic_gauge_5d = basic_dynamic_gauge<double, 5>;
}  // namespace ylt::metric