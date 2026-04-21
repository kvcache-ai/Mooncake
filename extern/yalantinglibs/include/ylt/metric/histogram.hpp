
#pragma once
#include <algorithm>
#include <cstddef>
#include <memory>
#include <vector>

#include "counter.hpp"
#include "dynamic_metric.hpp"
#include "gauge.hpp"

namespace ylt::metric {
#ifdef CINATRA_ENABLE_METRIC_JSON
struct json_histogram_metric_t {
  std::map<std::string, std::string> labels;
  std::map<double, int64_t> quantiles;
  int64_t count;
  double sum;
};
YLT_REFL(json_histogram_metric_t, labels, quantiles, count, sum);
struct json_histogram_t {
  std::string name;
  std::string help;
  std::string type;
  std::vector<json_histogram_metric_t> metrics;
};
YLT_REFL(json_histogram_t, name, help, type, metrics);
#endif

template <typename value_type>
class basic_static_histogram : public static_metric {
 public:
  basic_static_histogram(std::string name, std::string help,
                         std::vector<double> buckets, size_t dupli_count = 2)
      : bucket_boundaries_(std::move(buckets)),
        static_metric(MetricType::Histogram, std::move(name), std::move(help)),
        sum_(std::make_shared<gauge_t>("", "", dupli_count)) {
    init_bucket_counter(dupli_count, bucket_boundaries_.size());
  }

  basic_static_histogram(std::string name, std::string help,
                         std::vector<double> buckets,
                         std::map<std::string, std::string> labels,
                         size_t dupli_count = 2)
      : bucket_boundaries_(std::move(buckets)),
        static_metric(MetricType::Histogram, name, help, labels),
        sum_(std::make_shared<gauge_t>("", "", dupli_count)) {
    init_bucket_counter(dupli_count, bucket_boundaries_.size());
  }

  void observe(value_type value) {
    const auto bucket_index = static_cast<std::size_t>(
        std::distance(bucket_boundaries_.begin(),
                      std::lower_bound(bucket_boundaries_.begin(),
                                       bucket_boundaries_.end(), value)));
    sum_->inc(value);
    bucket_counts_[bucket_index]->inc();
  }

  auto get_bucket_counts() { return bucket_counts_; }

  void serialize(std::string &str) override {
    auto val = sum_->value();

    if (val == 0) {
      return;
    }

    serialize_head(str);
    value_type count = 0;
    auto bucket_counts = get_bucket_counts();
    for (size_t i = 0; i < bucket_counts.size(); i++) {
      auto counter = bucket_counts[i];
      str.append(name_).append("_bucket{");

      if (!labels_name_.empty()) {
        build_label_string(str, labels_name_, labels_value_);
        str.append(",");
      }

      if (i == bucket_boundaries_.size()) {
        str.append("le=\"").append("+Inf").append("\"} ");
      }
      else {
        str.append("le=\"")
            .append(std::to_string(bucket_boundaries_[i]))
            .append("\"} ");
      }

      count += counter->value();
      str.append(std::to_string(count));
      str.append("\n");
    }

    str.append(name_).append("_sum ").append(std::to_string(val)).append("\n");

    str.append(name_)
        .append("_count ")
        .append(std::to_string(count))
        .append("\n");
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  void serialize_to_json(std::string &str) override {
    auto val = sum_->value();
    if (val == 0) {
      return;
    }

    json_histogram_t hist{name_, help_, std::string(metric_name())};

    value_type count = 0;
    auto bucket_counts = get_bucket_counts();
    json_histogram_metric_t metric{};
    for (size_t i = 0; i < bucket_counts.size(); i++) {
      auto counter = bucket_counts[i];

      count += counter->value();

      if (i == bucket_boundaries_.size()) {
        metric.quantiles.emplace(std::numeric_limits<int>::max(),
                                 (int64_t)count);
      }
      else {
        metric.quantiles.emplace(bucket_boundaries_[i],
                                 (int64_t)counter->value());
      }
    }
    metric.count = (int64_t)count;
    metric.sum = val;

    for (size_t i = 0; i < labels_value_.size(); i++) {
      metric.labels[labels_name_[i]] = labels_value_[i];
    }

    hist.metrics.push_back(std::move(metric));

    iguana::to_json(hist, str);
  }
#endif

 private:
  void init_bucket_counter(size_t dupli_count, size_t bucket_size) {
    for (size_t i = 0; i < bucket_size + 1; i++) {
      bucket_counts_.push_back(
          std::make_shared<counter_t>("", "", dupli_count));
    }
  }

  template <class ForwardIterator>
  bool is_strict_sorted(ForwardIterator first, ForwardIterator last) {
    return std::adjacent_find(first, last,
                              std::greater_equal<typename std::iterator_traits<
                                  ForwardIterator>::value_type>()) == last;
  }

  std::vector<double> bucket_boundaries_;
  std::vector<std::shared_ptr<counter_t>> bucket_counts_;  // readonly
  std::shared_ptr<gauge_t> sum_;
};
using histogram_t = basic_static_histogram<int64_t>;
using histogram_d = basic_static_histogram<double>;

template <typename value_type, uint8_t N>
class basic_dynamic_histogram : public dynamic_metric {
 public:
  basic_dynamic_histogram(std::string name, std::string help,
                          std::vector<double> buckets,
                          std::array<std::string, N> labels_name)
      : bucket_boundaries_(buckets),
        dynamic_metric(MetricType::Histogram, name, help, labels_name),
        sum_(std::make_shared<basic_dynamic_gauge<value_type, N>>(
            name, help, labels_name)) {
    for (size_t i = 0; i < buckets.size() + 1; i++) {
      bucket_counts_.push_back(
          std::make_shared<basic_dynamic_counter<value_type, N>>(name, help,
                                                                 labels_name));
    }
  }

  basic_dynamic_histogram(std::string name, std::string help,
                          std::vector<double> buckets,
                          std::map<std::string, std::string> static_labels,
                          std::array<std::string, N> labels_name)
      : bucket_boundaries_(buckets),
        dynamic_metric(MetricType::Histogram, name, help, labels_name),
        sum_(std::make_shared<basic_dynamic_gauge<value_type, N>>(name, help,
                                                                  labels_name)),
        static_labels_(static_labels) {
    for (size_t i = 0; i < buckets.size() + 1; i++) {
      bucket_counts_.push_back(
          std::make_shared<basic_dynamic_counter<value_type, N>>(name, help,
                                                                 labels_name));
    }
    build_label_string(static_labels_str_, static_labels_);
  }

  void observe(const std::array<std::string, N> &labels_value,
               value_type value) {
    const auto bucket_index = static_cast<std::size_t>(
        std::distance(bucket_boundaries_.begin(),
                      std::lower_bound(bucket_boundaries_.begin(),
                                       bucket_boundaries_.end(), value)));
    sum_->inc(labels_value, value);
    bucket_counts_[bucket_index]->inc(labels_value);
  }

  void clean_expired_label() override {
    sum_->clean_expired_label();
    for (auto &m : bucket_counts_) {
      m->clean_expired_label();
    }
  }

  auto get_bucket_counts() { return bucket_counts_; }

  bool has_label_value(const std::string &label_val) override {
    return sum_->has_label_value(label_val);
  }

  bool has_label_value(const std::regex &regex) override {
    return sum_->has_label_value(regex);
  }

  bool has_label_value(const std::vector<std::string> &label_value) override {
    return sum_->has_label_value(label_value);
  }

  size_t label_value_count() const { return sum_->label_value_count(); }

  void serialize(std::string &str) override {
    auto value_map = sum_->copy();
    if (value_map.empty()) {
      return;
    }

    serialize_head(str);

    std::string value_str;
    auto bucket_counts = get_bucket_counts();
    for (auto &e : value_map) {
      auto &labels_value = e->label;
      auto &value = e->value;
      if (value == 0) {
        continue;
      }

      value_type count = 0;
      for (size_t i = 0; i < bucket_counts.size(); i++) {
        auto counter = bucket_counts[i];
        value_str.append(name_).append("_bucket{");
        std::string dynamic_labels_str;
        if (!labels_name_.empty()) {
          build_label_string(dynamic_labels_str, labels_name_, labels_value);
          dynamic_labels_str.append(",");
        }
        value_str.append(join_string(static_labels_str_, dynamic_labels_str));

        if (i == bucket_boundaries_.size()) {
          value_str.append("le=\"").append("+Inf").append("\"} ");
        }
        else {
          value_str.append("le=\"")
              .append(std::to_string(bucket_boundaries_[i]))
              .append("\"} ");
        }

        count += counter->value(labels_value);
        value_str.append(std::to_string(count));
        value_str.append("\n");
      }

      str.append(value_str);

      std::string dynamic_labels_str;
      build_label_string(dynamic_labels_str, sum_->labels_name(), labels_value);
      str.append(name_);
      str.append("_sum{");
      str.append(join_string(static_labels_str_, dynamic_labels_str));
      str.append("} ");

      str.append(std::to_string(value));
      str.append("\n");

      str.append(name_).append("_count{");
      str.append(join_string(static_labels_str_, dynamic_labels_str));
      str.append("} ");
      str.append(std::to_string(count));
      str.append("\n");
    }
    if (value_str.empty()) {
      str.clear();
    }
  }

#ifdef CINATRA_ENABLE_METRIC_JSON
  void serialize_to_json(std::string &str) override {
    auto value_map = sum_->copy();
    if (value_map.empty()) {
      return;
    }

    json_histogram_t hist{name_, help_, std::string(metric_name())};
    auto bucket_counts = get_bucket_counts();

    for (auto &e : value_map) {
      auto &labels_value = e->label;
      auto &value = e->value;
      if (value == 0) {
        continue;
      }

      size_t count = 0;
      json_histogram_metric_t metric{};
      for (size_t i = 0; i < bucket_counts.size(); i++) {
        auto counter = bucket_counts[i];

        count += counter->value(labels_value);

        if (i == bucket_boundaries_.size()) {
          metric.quantiles.emplace(std::numeric_limits<int>::max(),
                                   (int64_t)count);
        }
        else {
          metric.quantiles.emplace(bucket_boundaries_[i],
                                   (int64_t)counter->value(labels_value));
        }
      }
      metric.count = (int64_t)count;
      metric.sum = sum_->value(labels_value);

      for (auto &e : static_labels_) {
        metric.labels[e.first] = e.second;
      }
      for (size_t i = 0; i < labels_value.size(); i++) {
        metric.labels[sum_->labels_name()[i]] = labels_value[i];
      }

      hist.metrics.push_back(std::move(metric));
    }

    if (!hist.metrics.empty()) {
      iguana::to_json(hist, str);
    }
  }
#endif

 private:
  template <class ForwardIterator>
  bool is_strict_sorted(ForwardIterator first, ForwardIterator last) {
    return std::adjacent_find(first, last,
                              std::greater_equal<typename std::iterator_traits<
                                  ForwardIterator>::value_type>()) == last;
  }

  std::vector<double> bucket_boundaries_;
  std::vector<std::shared_ptr<basic_dynamic_counter<value_type, N>>>
      bucket_counts_;  // readonly
  std::shared_ptr<basic_dynamic_gauge<value_type, N>> sum_;
  std::string static_labels_str_;
  std::map<std::string, std::string> static_labels_;
};

using dynamic_histogram_1t = basic_dynamic_histogram<int64_t, 1>;
using dynamic_histogram_1d = basic_dynamic_histogram<double, 1>;

using dynamic_histogram_2t = basic_dynamic_histogram<int64_t, 2>;
using dynamic_histogram_2d = basic_dynamic_histogram<double, 2>;
using dynamic_histogram_t = dynamic_histogram_2t;
using dynamic_histogram_d = dynamic_histogram_2d;

using dynamic_histogram_3t = basic_dynamic_histogram<int64_t, 3>;
using dynamic_histogram_3d = basic_dynamic_histogram<double, 3>;

using dynamic_histogram_4t = basic_dynamic_histogram<int64_t, 4>;
using dynamic_histogram_4d = basic_dynamic_histogram<double, 4>;

using dynamic_histogram_5t = basic_dynamic_histogram<int64_t, 5>;
using dynamic_histogram_5d = basic_dynamic_histogram<double, 5>;
}  // namespace ylt::metric