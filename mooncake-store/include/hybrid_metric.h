#pragma once
#include <array>
#include <variant>
#include <map>

#include <ylt/metric/counter.hpp>
#include <ylt/metric/histogram.hpp>

namespace ylt::metric {
template <typename value_type, uint8_t N>
class basic_hybrid_counter
    : public dynamic_metric_impl<std::atomic<value_type>, N> {
    using Base = dynamic_metric_impl<std::atomic<value_type>, N>;

   public:
    // hybrid labels value
    basic_hybrid_counter(std::string name, std::string help,
                         std::map<std::string, std::string> static_labels,
                         std::array<std::string, N> labels_name)
        : Base(MetricType::Counter, std::move(name), std::move(help),
               std::move(labels_name)),
          static_labels_(static_labels) {
        for (auto& [k, v] : static_labels) {
            static_labels_str_.append(k).append("=\"").append(v).append("\",");
        }
    }
    using label_key_type = const std::array<std::string, N>&;
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
        } else {
            return value_type{};
        }
    }

    void remove_label_value(
        const std::map<std::string, std::string>& labels) override {
        if (Base::empty()) {
            return;
        }

        const auto& labels_name = this->labels_name();
        if (labels.size() > labels_name.size()) {
            return;
        }

        size_t count = 0;
        std::vector<std::string_view> vec;
        for (auto& lb_name : labels_name) {
            if (auto i = labels.find(lb_name); i != labels.end()) {
                vec.push_back(i->second);
            } else {
                vec.push_back("");
                count++;
            }
        }
        if (count == labels_name.size()) {
            return;
        }
        Base::erase_if([&](auto& pair) {
            auto& [arr, _] = pair;
            if constexpr (N > 0) {
                for (size_t i = 0; i < vec.size(); i++) {
                    if (!vec[i].empty() && vec[i] != arr[i]) {
                        return false;
                    }
                }
            }
            return true;
        });
    }

    bool has_label_value(const std::string& value) override {
        auto map = Base::copy();
        for (auto& e : map) {
            auto& label_value = e->label;
            if (auto it =
                    std::find(label_value.begin(), label_value.end(), value);
                it != label_value.end()) {
                return true;
            }
        }

        return false;
    }

    bool has_label_value(const std::regex& regex) override {
        auto map = Base::copy();
        for (auto& e : map) {
            auto& label_value = e->label;
            if (auto it = std::find_if(
                    label_value.begin(), label_value.end(),
                    [&](auto& val) { return std::regex_match(val, regex); });
                it != label_value.end()) {
                return true;
            }
        }

        return false;
    }

    bool has_label_value(const std::vector<std::string>& label_value) override {
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

    void serialize(std::string& str) override {
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
    void serialize_to_json(std::string& str) override {
        auto map = Base::copy();
        json_counter_t counter{Base::name_, Base::help_, Base::metric_name()};
        to_json(counter, map, str);
    }

    template <typename T>
    void to_json(json_counter_t& counter, T& map, std::string& str) {
        for (auto& e : map) {
            auto& k = e->label;
            auto& val = e->value;
            json_counter_metric_t metric;
            size_t index = 0;
            assert(Base::labels_name().size() == k.size());
            for (auto& [k, v] : static_labels_) {
                metric.labels.emplace_back(k, v);
            }
            for (auto& label_value : k) {
                metric.labels.emplace_back(Base::labels_name()[index++],
                                           label_value);
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
    void serialize_map(T& value_map, std::string& str) {
        for (auto& e : value_map) {
            auto& labels_value = e->label;
            auto val = e->value.load(std::memory_order::relaxed);
            str.append(Base::name_);
            if (Base::labels_name_.empty()) {
                str.append(" ");
            } else {
                str.append("{");
                build_string_with_static(str, Base::labels_name_, labels_value);
                str.append("} ");
            }

            str.append(std::to_string(val));

            str.append("\n");
        }
    }

    template <class T, std::size_t Size>
    bool equal(const std::vector<T>& v, const std::array<T, Size>& a) {
        if (v.size() != N) return false;

        return std::equal(v.begin(), v.end(), a.begin());
    }

    void build_string_with_static(std::string& str,
                                  const std::vector<std::string>& v1,
                                  const auto& v2) {
        str.append(static_labels_str_);
        for (size_t i = 0; i < v1.size(); i++) {
            str.append(v1[i]).append("=\"").append(v2[i]).append("\"").append(
                ",");
        }
        str.pop_back();
    }

   private:
    std::string static_labels_str_;  // preformatted static labels string
    std::map<std::string, std::string> static_labels_;
};

using hybrid_counter_1t = basic_hybrid_counter<int64_t, 1>;
using hybrid_counter_1d = basic_hybrid_counter<double, 1>;

using hybrid_counter_2t = basic_hybrid_counter<int64_t, 2>;
using hybrid_counter_2d = basic_hybrid_counter<double, 2>;
using hybrid_counter_t = hybrid_counter_2t;
using hybrid_counter_d = hybrid_counter_2d;

using hybrid_counter_3t = basic_hybrid_counter<int64_t, 3>;
using hybrid_counter_3d = basic_hybrid_counter<double, 3>;

using hybrid_counter_4t = basic_hybrid_counter<int64_t, 4>;
using hybrid_counter_4d = basic_hybrid_counter<double, 4>;

using hybrid_counter_5t = basic_hybrid_counter<int64_t, 5>;
using hybrid_counter_5d = basic_hybrid_counter<double, 5>;

// ================ hybrid_histogram ================
template <typename value_type, uint8_t N>
class basic_hybrid_histogram : public dynamic_metric {
   public:
    basic_hybrid_histogram(std::string name, std::string help,
                           std::vector<double> buckets,
                           std::map<std::string, std::string> static_labels,
                           std::array<std::string, N> labels_name)
        : dynamic_metric(MetricType::Histogram, name, help, labels_name),
          static_labels_(static_labels),
          bucket_boundaries_(buckets),
          sum_(std::make_shared<basic_dynamic_gauge<value_type, N>>(
              name, help, labels_name)) {
        for (size_t i = 0; i < buckets.size() + 1; i++) {
            bucket_counts_.push_back(
                std::make_shared<basic_dynamic_counter<value_type, N>>(
                    name, help, labels_name));
        }
        for (auto& [k, v] : static_labels) {
            static_labels_str_.append(k).append("=\"").append(v).append("\",");
        }
    }

    void observe(const std::array<std::string, N>& labels_value,
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
        for (auto& m : bucket_counts_) {
            m->clean_expired_label();
        }
    }

    auto get_bucket_counts() { return bucket_counts_; }

    bool has_label_value(const std::string& label_val) override {
        return sum_->has_label_value(label_val);
    }

    bool has_label_value(const std::regex& regex) override {
        return sum_->has_label_value(regex);
    }

    bool has_label_value(const std::vector<std::string>& label_value) override {
        return sum_->has_label_value(label_value);
    }

    size_t label_value_count() const { return sum_->label_value_count(); }

    void serialize(std::string& str) override {
        auto value_map = sum_->copy();
        if (value_map.empty()) {
            return;
        }

        serialize_head(str);

        std::string value_str;
        auto bucket_counts = get_bucket_counts();
        for (auto& e : value_map) {
            auto& labels_value = e->label;
            auto& value = e->value;
            if (value == 0) {
                continue;
            }

            value_type count = 0;
            for (size_t i = 0; i < bucket_counts.size(); i++) {
                auto counter = bucket_counts[i];
                value_str.append(name_).append("_bucket{");
                if (!labels_name_.empty()) {
                    build_label_string_with_static(value_str, labels_name_,
                                                   labels_value);
                    value_str.append(",");
                }

                if (i == bucket_boundaries_.size()) {
                    value_str.append("le=\"").append("+Inf").append("\"} ");
                } else {
                    value_str.append("le=\"")
                        .append(std::to_string(bucket_boundaries_[i]))
                        .append("\"} ");
                }

                count += counter->value(labels_value);
                value_str.append(std::to_string(count));
                value_str.append("\n");
            }

            str.append(value_str);

            std::string labels_str;
            build_label_string_with_static(labels_str, sum_->labels_name(),
                                           labels_value);

            str.append(name_);
            str.append("_sum{");
            str.append(labels_str);
            str.append("} ");
            str.append(std::to_string(value));
            str.append("\n");
            str.append(name_).append("_count{");
            str.append(labels_str);
            str.append("} ");
            str.append(std::to_string(count));
            str.append("\n");
        }
        if (value_str.empty()) {
            str.clear();
        }
    }

    void build_label_string_with_static(
        std::string& str, const std::vector<std::string>& label_name,
        const auto& label_value) {
        str.append(static_labels_str_);
        for (size_t i = 0; i < label_name.size(); i++) {
            str.append(label_name[i])
                .append("=\"")
                .append(label_value[i])
                .append("\",");
        }
        str.pop_back();
    }

#ifdef CINATRA_ENABLE_METRIC_JSON
    void serialize_to_json(std::string& str) override {
        auto value_map = sum_->copy();
        if (value_map.empty()) {
            return;
        }

        json_histogram_t hist{name_, help_, std::string(metric_name())};
        auto bucket_counts = get_bucket_counts();

        for (auto& e : value_map) {
            auto& labels_value = e->label;
            auto& value = e->value;
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
                } else {
                    metric.quantiles.emplace(
                        bucket_boundaries_[i],
                        (int64_t)counter->value(labels_value));
                }
            }
            metric.count = (int64_t)count;
            metric.sum = sum_->value(labels_value);

            for (auto& [k, v] : static_labels_) {
                metric.labels[k] = v;
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
        return std::adjacent_find(
                   first, last,
                   std::greater_equal<typename std::iterator_traits<
                       ForwardIterator>::value_type>()) == last;
    }

    std::string static_labels_str_;  // preformatted static labels string
    std::map<std::string, std::string> static_labels_;
    std::vector<double> bucket_boundaries_;
    std::vector<std::shared_ptr<basic_dynamic_counter<value_type, N>>>
        bucket_counts_;  // readonly
    std::shared_ptr<basic_dynamic_gauge<value_type, N>> sum_;
};

using hybrid_histogram_1t = basic_hybrid_histogram<int64_t, 1>;
using hybrid_histogram_1d = basic_hybrid_histogram<double, 1>;

using hybrid_histogram_2t = basic_hybrid_histogram<int64_t, 2>;
using hybrid_histogram_2d = basic_hybrid_histogram<double, 2>;
using hybrid_histogram_t = hybrid_histogram_2t;
using hybrid_histogram_d = hybrid_histogram_2d;

using hybrid_histogram_3t = basic_hybrid_histogram<int64_t, 3>;
using hybrid_histogram_3d = basic_hybrid_histogram<double, 3>;

using hybrid_histogram_4t = basic_hybrid_histogram<int64_t, 4>;
using hybrid_histogram_4d = basic_hybrid_histogram<double, 4>;

using hybrid_histogram_5t = basic_hybrid_histogram<int64_t, 5>;
using hybrid_histogram_5d = basic_hybrid_histogram<double, 5>;
}  // namespace ylt::metric
