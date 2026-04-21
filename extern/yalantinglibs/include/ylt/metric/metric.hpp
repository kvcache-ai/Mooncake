#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <map>
#include <optional>
#include <regex>
#include <span>
#include <string>
#include <vector>

#ifdef CINATRA_ENABLE_METRIC_JSON
namespace iguana {

template <typename T>
inline char* to_chars_float(T value, char* buffer) {
  return buffer + snprintf(buffer, 65, "%g", value);
}

}  // namespace iguana

#include <iguana/json_writer.hpp>
#endif

namespace ylt::metric {
enum class MetricType {
  Counter,
  Gauge,
  Histogram,
  Summary,
  Nil,
};

struct metric_filter_options {
  std::optional<std::regex> name_regex{};
  std::optional<std::regex> label_regex{};
  std::optional<std::regex> label_value_regex{};
  bool is_white = true;
};

class metric_t {
 public:
  static inline std::atomic<int64_t> g_user_metric_count = 0;

  metric_t() = default;
  metric_t(MetricType type, std::string name, std::string help)
      : type_(type),
        name_(std::move(name)),
        help_(std::move(help)),
        metric_created_time_(std::chrono::system_clock::now()) {
    g_user_metric_count.fetch_add(1, std::memory_order::relaxed);
  }

  template <size_t N>
  metric_t(MetricType type, std::string name, std::string help,
           std::array<std::string, N> labels_name)
      : metric_t(type, std::move(name), std::move(help)) {
    for (size_t i = 0; i < N; i++) {
      labels_name_.push_back(std::move(labels_name[i]));
    }
  }

  metric_t(MetricType type, std::string name, std::string help,
           std::map<std::string, std::string> static_labels)
      : metric_t(type, std::move(name), std::move(help)) {
    static_labels_ = std::move(static_labels);
    for (auto& [k, v] : static_labels_) {
      labels_name_.push_back(k);
      labels_value_.push_back(v);
    }
  }
  virtual ~metric_t() {
    g_user_metric_count.fetch_sub(1, std::memory_order::relaxed);
  }

  std::string_view name() { return name_; }

  const std::string& str_name() { return name_; }

  std::string_view help() { return help_; }

  MetricType metric_type() { return type_; }

  std::string_view metric_name() {
    switch (type_) {
      case MetricType::Counter:
        return "counter";
      case MetricType::Gauge:
        return "gauge";
      case MetricType::Histogram:
        return "histogram";
      case MetricType::Summary:
        return "summary";
      case MetricType::Nil:
      default:
        return "unknown";
    }
  }

  const std::vector<std::string>& labels_name() { return labels_name_; }

  const std::map<std::string, std::string>& get_static_labels() {
    return static_labels_;
  }

  virtual bool has_label_value(const std::string& label_value) {
    return std::find(labels_value_.begin(), labels_value_.end(), label_value) !=
           labels_value_.end();
  }

  virtual void clean_expired_label() {}

  virtual bool has_label_value(const std::vector<std::string>& label_value) {
    return labels_value_ == label_value;
  }
  virtual bool has_label_value(const std::regex& regex) {
    auto it = std::find_if(labels_value_.begin(), labels_value_.end(),
                           [&](auto& value) {
                             return std::regex_match(value, regex);
                           });

    return it != labels_value_.end();
  }
  bool has_label_name(const std::vector<std::string>& label_name) {
    return labels_name_ == label_name;
  }
  bool has_label_name(const std::string& label_name) {
    return std::find(labels_name_.begin(), labels_name_.end(), label_name) !=
           labels_name_.end();
  }

  virtual void remove_label_value(
      const std::map<std::string, std::string>& labels) {}

  virtual void serialize(std::string& str) {}

#ifdef CINATRA_ENABLE_METRIC_JSON
  virtual void serialize_to_json(std::string& str) {}
#endif

  template <typename T>
  T* as() {
    return dynamic_cast<T*>(this);
  }

 protected:
  void set_metric_type(MetricType type) { type_ = type; }
  void serialize_head(std::string& str) {
    str.append("# HELP ").append(name_).append(" ").append(help_).append("\n");
    str.append("# TYPE ")
        .append(name_)
        .append(" ")
        .append(metric_name())
        .append("\n");
  }

  void build_label_string(std::string& str,
                          const std::vector<std::string>& label_name,
                          const auto& label_value) {
    for (size_t i = 0; i < label_name.size(); i++) {
      str.append(label_name[i])
          .append("=\"")
          .append(label_value[i])
          .append("\"")
          .append(",");
    }
    str.pop_back();
  }

  void build_label_string(std::string& str,
                          const std::map<std::string, std::string>& labels) {
    for (auto& [k, v] : labels) {
      str.append(k).append("=\"").append(v).append("\",");
    }
    str.pop_back();
  }

  inline std::string join_string(const std::string& str1,
                                 const std::string& str2) {
    if (str1.empty())
      return str2;
    if (str2.empty())
      return str1;

    std::string result;
    result.reserve(str1.size() + str2.size() + 1);
    result.append(str1).append(",").append(str2);
    return result;
  }

  MetricType type_ = MetricType::Nil;
  std::string name_;
  std::string help_;
  std::map<std::string, std::string> static_labels_;
  std::vector<std::string> labels_name_;   // read only
  std::vector<std::string> labels_value_;  // read only
  std::chrono::system_clock::time_point metric_created_time_{};
};

class static_metric : public metric_t {
  using metric_t::metric_t;
};

inline std::chrono::seconds ylt_label_max_age{0};
inline std::chrono::seconds ylt_label_check_expire_duration{60};

inline std::atomic<int64_t> ylt_metric_capacity = 10000000;
inline void set_metric_capacity(int64_t max_count) {
  ylt_metric_capacity = max_count;
}

inline void set_label_max_age(
    std::chrono::seconds max_age,
    std::chrono::seconds check_duration = std::chrono::seconds{60}) {
  ylt_label_max_age = max_age;
  ylt_label_check_expire_duration = check_duration;
}
}  // namespace ylt::metric