#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <variant>

#include "dynamic_metric.hpp"
#include "thread_local_value.hpp"

namespace ylt::metric {
enum class op_type_t { INC, DEC, SET };

#ifdef CINATRA_ENABLE_METRIC_JSON
struct json_counter_metric_t {
  std::vector<std::pair<std::string_view, std::string_view>> labels;
  std::variant<int64_t, double> value;
};
YLT_REFL(json_counter_metric_t, labels, value);
struct json_counter_t {
  std::string_view name;
  std::string_view help;
  std::string_view type;
  std::vector<json_counter_metric_t> metrics;
};
YLT_REFL(json_counter_t, name, help, type, metrics);
#endif

template <typename value_type>
class basic_static_counter : public static_metric {
 public:
  // static counter, no labels, only contains an atomic value.
  basic_static_counter(std::string name, std::string help,
                       uint32_t dupli_count = (std::min)(
                           128u, std::thread::hardware_concurrency()))
      : static_metric(MetricType::Counter, std::move(name), std::move(help)),
        dupli_count_((std::max)(1u, dupli_count)),
        default_label_value_(dupli_count_) {}

  // static counter, contains a static labels with atomic value.
  basic_static_counter(std::string name, std::string help,
                       std::map<std::string, std::string> labels,
                       uint32_t dupli_count = (std::min)(
                           128u, std::thread::hardware_concurrency()))
      : static_metric(MetricType::Counter, std::move(name), std::move(help),
                      std::move(labels)),
        dupli_count_((std::max)(1u, dupli_count)),
        default_label_value_(dupli_count_) {}

  void inc(value_type val = 1) {
    if (val < 0) {
      return;
    }
    if (!has_change_.load(std::memory_order::relaxed)) [[unlikely]] {
      has_change_.store(true, std::memory_order::relaxed);
    }
    default_label_value_.inc(val);
  }

  value_type update(value_type value) {
    if (!has_change_.load(std::memory_order::relaxed)) [[unlikely]] {
      has_change_.store(true, std::memory_order::relaxed);
    }
    return default_label_value_.update(value);
  }

  value_type reset() { return default_label_value_.reset(); }

  value_type value() { return default_label_value_.value(); }

  void serialize(std::string &str) override {
    auto value = default_label_value_.value();
    if (value == 0 && !has_change_.load(std::memory_order::relaxed)) {
      return;
    }

    metric_t::serialize_head(str);
    serialize_default_label(str, value);
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  void serialize_to_json(std::string &str) override {
    auto value = default_label_value_.value();
    if (value == 0 && !has_change_.load(std::memory_order::relaxed)) {
      return;
    }

    json_counter_t counter{name_, help_, metric_name()};

    counter.metrics.resize(1);
    counter.metrics[0].labels.reserve(static_labels_.size());
    for (auto &[k, v] : static_labels_) {
      counter.metrics[0].labels.emplace_back(k, v);
    }
    counter.metrics[0].value = value;
    iguana::to_json(counter, str);
  }
#endif
 private:
 protected:
  void serialize_default_label(std::string &str, value_type value) {
    str.append(name_);
    if (labels_name_.empty()) {
      str.append(" ");
    }
    else {
      str.append("{");
      build_string(str, labels_name_, labels_value_);
      str.append("} ");
    }

    str.append(std::to_string(value));

    str.append("\n");
  }

  void build_string(std::string &str, const std::vector<std::string> &v1,
                    const std::vector<std::string> &v2) {
    for (size_t i = 0; i < v1.size(); i++) {
      str.append(v1[i]).append("=\"").append(v2[i]).append("\",");
    }
    str.pop_back();
  }

  std::atomic<bool> has_change_ = false;
  uint32_t dupli_count_;
  thread_local_value<value_type> default_label_value_;
};

using counter_t = basic_static_counter<int64_t>;
using counter_d = basic_static_counter<double>;

template <typename value_type, uint8_t N>
class basic_dynamic_counter
    : public dynamic_metric_impl<std::atomic<value_type>, N> {
  using Base = dynamic_metric_impl<std::atomic<value_type>, N>;

 public:
  // dynamic labels value
  basic_dynamic_counter(std::string name, std::string help,
                        std::array<std::string, N> labels_name)
      : Base(MetricType::Counter, std::move(name), std::move(help),
             std::move(labels_name)) {}

  // hybrid labels value
  basic_dynamic_counter(std::string name, std::string help,
                        std::map<std::string, std::string> static_labels,
                        std::array<std::string, N> labels_name)
      : Base(MetricType::Counter, std::move(name), std::move(help),
             std::move(labels_name)),
        static_labels_(static_labels) {
    Base::build_label_string(static_labels_str_, static_labels_);
  }

  using label_key_type = const std::array<std::string, N> &;
  void inc(label_key_type labels_value, value_type value = 1) {
    detail::inc_impl(Base::try_emplace(labels_value).first->value, value);
  }

  value_type update(label_key_type labels_value, value_type value) {
    return Base::try_emplace(labels_value)
        .first->value.exchange(value, std::memory_order::relaxed);
  }

  value_type value(label_key_type labels_value) {
    if (auto ptr = Base::find(labels_value); ptr != nullptr) {
      return ptr->value.load(std::memory_order::relaxed);
    }
    else {
      return value_type{};
    }
  }

  void remove_label_value(
      const std::map<std::string, std::string> &labels) override {
    if (Base::empty()) {
      return;
    }

    const auto &labels_name = this->labels_name();
    if (labels.size() > labels_name.size()) {
      return;
    }

    // if (labels.size() == labels_name.size()) {  // TODO: speed up for this
    // case

    // }
    // else {
    size_t count = 0;
    std::vector<std::string_view> vec;
    for (auto &lb_name : labels_name) {
      if (auto i = labels.find(lb_name); i != labels.end()) {
        vec.push_back(i->second);
      }
      else {
        vec.push_back("");
        count++;
      }
    }
    if (count == labels_name.size()) {
      return;
    }
    Base::erase_if([&](auto &pair) {
      auto &[arr, _] = pair;
      if constexpr (N > 0) {
        for (size_t i = 0; i < vec.size(); i++) {
          if (!vec[i].empty() && vec[i] != arr[i]) {
            return false;
          }
        }
      }
      return true;
    });
    //}
  }

  bool has_label_value(const std::string &value) override {
    auto map = Base::copy();
    for (auto &e : map) {
      auto &label_value = e->label;
      if (auto it = std::find(label_value.begin(), label_value.end(), value);
          it != label_value.end()) {
        return true;
      }
    }

    return false;
  }

  bool has_label_value(const std::regex &regex) override {
    auto map = Base::copy();
    for (auto &e : map) {
      auto &label_value = e->label;
      if (auto it = std::find_if(label_value.begin(), label_value.end(),
                                 [&](auto &val) {
                                   return std::regex_match(val, regex);
                                 });
          it != label_value.end()) {
        return true;
      }
    }

    return false;
  }

  bool has_label_value(const std::vector<std::string> &label_value) override {
    std::array<std::string, N> arr{};
    size_t size = (std::min)((size_t)N, label_value.size());
    if (label_value.size() > N) {
      return false;
    }

    for (size_t i = 0; i < size; i++) {
      arr[i] = label_value[i];
    }
    return Base::find(arr) != nullptr;
  }

  void serialize(std::string &str) override {
    auto map = Base::copy();
    if (map.empty()) {
      return;
    }

    std::string value_str;
    serialize_map(map, value_str);
    if (!value_str.empty()) {
      Base::serialize_head(str);
      str.append(value_str);
    }
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  void serialize_to_json(std::string &str) override {
    auto map = Base::copy();
    json_counter_t counter{Base::name_, Base::help_, Base::metric_name()};
    to_json(counter, map, str);
  }

  template <typename T>
  void to_json(json_counter_t &counter, T &map, std::string &str) {
    for (auto &e : map) {
      auto &k = e->label;
      auto &val = e->value;
      json_counter_metric_t metric;
      size_t index = 0;
      assert(Base::labels_name().size() == k.size());
      for (auto &[k, v] : static_labels_) {
        metric.labels.emplace_back(k, v);
      }
      for (auto &label_value : k) {
        metric.labels.emplace_back(Base::labels_name()[index++], label_value);
      }
      metric.value = val.load(std::memory_order::relaxed);
      counter.metrics.push_back(std::move(metric));
    }
    if (!counter.metrics.empty()) {
      iguana::to_json(counter, str);
    }
  }
#endif

 protected:
  template <typename T>
  void serialize_map(T &value_map, std::string &str) {
    for (auto &e : value_map) {
      auto &labels_value = e->label;
      auto val = e->value.load(std::memory_order::relaxed);
      str.append(Base::name_);
      if (Base::labels_name_.empty()) {
        str.append(" ");
      }
      else {
        str.append("{");
        build_string(str, Base::labels_name_, labels_value);
        str.append("} ");
      }

      str.append(std::to_string(val));

      str.append("\n");
    }
  }

  template <class T, std::size_t Size>
  bool equal(const std::vector<T> &v, const std::array<T, Size> &a) {
    if (v.size() != N)
      return false;

    return std::equal(v.begin(), v.end(), a.begin());
  }

  void build_string(std::string &str, const std::vector<std::string> &v1,
                    const auto &v2) {
    str.append(static_labels_str_);
    if (!static_labels_str_.empty() && !v1.empty()) {
      str.append(",");
    }
    for (size_t i = 0; i < v1.size(); i++) {
      str.append(v1[i]).append("=\"").append(v2[i]).append("\",");
    }
    str.pop_back();
  }

  std::string static_labels_str_;
  std::map<std::string, std::string> static_labels_;
};

using dynamic_counter_1t = basic_dynamic_counter<int64_t, 1>;
using dynamic_counter_1d = basic_dynamic_counter<double, 1>;

using dynamic_counter_2t = basic_dynamic_counter<int64_t, 2>;
using dynamic_counter_2d = basic_dynamic_counter<double, 2>;
using dynamic_counter_t = dynamic_counter_2t;
using dynamic_counter_d = dynamic_counter_2d;

using dynamic_counter_3t = basic_dynamic_counter<int64_t, 3>;
using dynamic_counter_3d = basic_dynamic_counter<double, 3>;

using dynamic_counter_4t = basic_dynamic_counter<int64_t, 4>;
using dynamic_counter_4d = basic_dynamic_counter<double, 4>;

using dynamic_counter_5t = basic_dynamic_counter<int64_t, 5>;
using dynamic_counter_5d = basic_dynamic_counter<double, 5>;
}  // namespace ylt::metric