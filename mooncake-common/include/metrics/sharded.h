#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "prometheus/client_metric.h"
#include "prometheus/collectable.h"
#include "prometheus/metric_family.h"
#include "prometheus/metric_type.h"

namespace mooncake::metrics {

inline constexpr size_t kDefaultShards = 16;

namespace detail {

inline size_t ThreadShard(size_t shard_count) {
    static std::atomic<size_t> next_shard{0};
    static thread_local const size_t assigned_shard =
        next_shard.fetch_add(1, std::memory_order_relaxed);
    return assigned_shard % shard_count;
}

inline std::vector<std::string> LabelValues(
    const std::vector<std::string>& label_names,
    const std::map<std::string, std::string>& labels) {
    if (labels.size() != label_names.size()) {
        throw std::invalid_argument("label set does not match metric family");
    }
    std::vector<std::string> values;
    values.reserve(label_names.size());
    for (const auto& name : label_names) {
        const auto it = labels.find(name);
        if (it == labels.end()) {
            throw std::invalid_argument(
                "label set does not match metric family");
        }
        values.push_back(it->second);
    }
    return values;
}

}  // namespace detail

class ShardedCounterFamily final : public prometheus::Collectable {
   public:
    class Counter {
       public:
#ifdef MOONCAKE_METRICS_NOOP
        void Inc(uint64_t = 1) {}
#else
        void Inc(uint64_t value = 1) {
            shards_[detail::ThreadShard(shard_count_)].value.fetch_add(
                value, std::memory_order_relaxed);
        }
#endif

        uint64_t Value() const {
            uint64_t value = 0;
            for (size_t i = 0; i < shard_count_; ++i) {
                value += shards_[i].value.load(std::memory_order_relaxed);
            }
            return value;
        }

       private:
        friend class ShardedCounterFamily;

        struct alignas(64) Shard {
            std::atomic<uint64_t> value{0};
        };

        explicit Counter(size_t shard_count)
            : shard_count_(shard_count),
              shards_(std::make_unique<Shard[]>(shard_count)) {}

        size_t shard_count_;
        std::unique_ptr<Shard[]> shards_;
    };

    ShardedCounterFamily(std::string name, std::string help,
                         std::vector<std::string> label_names,
                         size_t shards = kDefaultShards)
        : name_(std::move(name)),
          help_(std::move(help)),
          label_names_(std::move(label_names)),
          shard_count_(shards) {
        if (shard_count_ == 0) {
            throw std::invalid_argument(
                "shard count must be greater than zero");
        }
    }

    Counter& Add(const std::map<std::string, std::string>& labels) {
        auto label_values = detail::LabelValues(label_names_, labels);
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = series_.find(label_values);
        if (it != series_.end()) {
            return *it->second;
        }
        auto counter = std::unique_ptr<Counter>(new Counter(shard_count_));
        Counter& result = *counter;
        series_.emplace(std::move(label_values), std::move(counter));
        return result;
    }

    bool Remove(Counter& counter) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = series_.begin(); it != series_.end(); ++it) {
            if (it->second.get() == &counter) {
                series_.erase(it);
                return true;
            }
        }
        return false;
    }

    std::vector<prometheus::MetricFamily> Collect() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<prometheus::MetricFamily> families(1);
        auto& family = families.front();
        family.name = name_;
        family.help = help_;
        family.type = prometheus::MetricType::Counter;
        family.metric.reserve(series_.size());
        for (const auto& [label_values, counter] : series_) {
            prometheus::ClientMetric metric;
            metric.counter.value = static_cast<double>(counter->Value());
            metric.label.reserve(label_names_.size());
            for (size_t i = 0; i < label_names_.size(); ++i) {
                metric.label.push_back({label_names_[i], label_values[i]});
            }
            family.metric.push_back(std::move(metric));
        }
        return families;
    }

   private:
    std::string name_;
    std::string help_;
    std::vector<std::string> label_names_;
    size_t shard_count_;
    mutable std::mutex mutex_;
    std::map<std::vector<std::string>, std::unique_ptr<Counter>> series_;
};

class ShardedHistogramFamily final : public prometheus::Collectable {
   public:
    class Histogram {
       public:
#ifdef MOONCAKE_METRICS_NOOP
        void Observe(double) {}
#else
        void Observe(double value) {
            // lower_bound finds the first bound >= value, matching Prometheus
            // bucket semantics for values exactly on a boundary. NaN only
            // counts toward +Inf, as in prometheus-cpp.
            const size_t bucket =
                std::isnan(value) ? bounds_.size()
                                  : static_cast<size_t>(
                                        std::lower_bound(bounds_.begin(),
                                                         bounds_.end(), value) -
                                        bounds_.begin());
            const size_t shard = detail::ThreadShard(shard_count_);
            Count(shard, bucket).fetch_add(1, std::memory_order_relaxed);
            sums_[shard].value.fetch_add(value, std::memory_order_relaxed);
        }
#endif

       private:
        friend class ShardedHistogramFamily;

        struct alignas(64) CacheLine {
            std::atomic<uint64_t> words[64 / sizeof(std::atomic<uint64_t>)];
        };
        struct alignas(64) PaddedSum {
            std::atomic<double> value{0.0};
        };
        static constexpr size_t kWordsPerLine =
            sizeof(CacheLine::words) / sizeof(CacheLine::words[0]);

        // All buckets of one shard are packed into whole cache lines, so
        // shards never share a line but buckets are not padded individually.
        Histogram(const std::vector<double>& bounds, size_t shard_count)
            : bounds_(bounds),
              bucket_count_(bounds.size() + 1),
              shard_count_(shard_count),
              lines_per_shard_((bucket_count_ + kWordsPerLine - 1) /
                               kWordsPerLine),
              counts_(std::make_unique<CacheLine[]>(shard_count_ *
                                                    lines_per_shard_)),
              sums_(std::make_unique<PaddedSum[]>(shard_count_)) {
            for (size_t i = 0; i < shard_count_ * lines_per_shard_; ++i) {
                for (auto& word : counts_[i].words) {
                    word.store(0, std::memory_order_relaxed);
                }
            }
        }

        std::atomic<uint64_t>& Count(size_t shard, size_t bucket) const {
            return counts_[shard * lines_per_shard_ + bucket / kWordsPerLine]
                .words[bucket % kWordsPerLine];
        }

        std::vector<double> bounds_;
        size_t bucket_count_;
        size_t shard_count_;
        size_t lines_per_shard_;
        std::unique_ptr<CacheLine[]> counts_;
        std::unique_ptr<PaddedSum[]> sums_;
    };

    ShardedHistogramFamily(std::string name, std::string help,
                           std::vector<std::string> label_names,
                           std::vector<double> bucket_bounds,
                           size_t shards = kDefaultShards)
        : name_(std::move(name)),
          help_(std::move(help)),
          label_names_(std::move(label_names)),
          bucket_bounds_(std::move(bucket_bounds)),
          bucket_count_(bucket_bounds_.size() + 1),
          shard_count_(shards) {
        if (bucket_bounds_.empty()) {
            throw std::invalid_argument("bucket bounds must not be empty");
        }
        if (shard_count_ == 0) {
            throw std::invalid_argument(
                "shard count must be greater than zero");
        }
        for (size_t i = 1; i < bucket_bounds_.size(); ++i) {
            if (!(bucket_bounds_[i - 1] < bucket_bounds_[i])) {
                throw std::invalid_argument(
                    "bucket bounds must be strictly increasing");
            }
        }
    }

    Histogram& Add(const std::map<std::string, std::string>& labels) {
        auto label_values = detail::LabelValues(label_names_, labels);
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = series_.find(label_values);
        if (it != series_.end()) {
            return *it->second;
        }
        auto histogram = std::unique_ptr<Histogram>(
            new Histogram(bucket_bounds_, shard_count_));
        Histogram& result = *histogram;
        series_.emplace(std::move(label_values), std::move(histogram));
        return result;
    }

    bool Remove(Histogram& histogram) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = series_.begin(); it != series_.end(); ++it) {
            if (it->second.get() == &histogram) {
                series_.erase(it);
                return true;
            }
        }
        return false;
    }

    // Read non-cumulative bucket counts, sum the shards, then accumulate. The
    // +Inf bucket therefore always matches sample_count. The independently
    // accumulated sum can be very slightly out of sync during a scrape.
    std::vector<prometheus::MetricFamily> Collect() const override {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<prometheus::MetricFamily> families(1);
        auto& family = families.front();
        family.name = name_;
        family.help = help_;
        family.type = prometheus::MetricType::Histogram;
        family.metric.reserve(series_.size());

        std::vector<uint64_t> bucket_totals(bucket_count_);
        for (const auto& [label_values, histogram] : series_) {
            std::fill(bucket_totals.begin(), bucket_totals.end(), 0);
            double sum = 0.0;
            for (size_t shard_index = 0; shard_index < shard_count_;
                 ++shard_index) {
                for (size_t i = 0; i < bucket_count_; ++i) {
                    bucket_totals[i] += histogram->Count(shard_index, i)
                                            .load(std::memory_order_relaxed);
                }
                sum += histogram->sums_[shard_index].value.load(
                    std::memory_order_relaxed);
            }

            prometheus::ClientMetric metric;
            metric.histogram.bucket.reserve(bucket_count_);
            uint64_t cumulative = 0;
            for (size_t i = 0; i < bucket_count_; ++i) {
                cumulative += bucket_totals[i];
                const double upper_bound =
                    i < histogram->bounds_.size()
                        ? histogram->bounds_[i]
                        : std::numeric_limits<double>::infinity();
                metric.histogram.bucket.push_back({cumulative, upper_bound});
            }
            metric.histogram.sample_count = cumulative;
            metric.histogram.sample_sum = sum;
            metric.label.reserve(label_names_.size());
            for (size_t i = 0; i < label_names_.size(); ++i) {
                metric.label.push_back({label_names_[i], label_values[i]});
            }
            family.metric.push_back(std::move(metric));
        }
        return families;
    }

   private:
    std::string name_;
    std::string help_;
    std::vector<std::string> label_names_;
    std::vector<double> bucket_bounds_;
    size_t bucket_count_;
    size_t shard_count_;
    mutable std::mutex mutex_;
    std::map<std::vector<std::string>, std::unique_ptr<Histogram>> series_;
};

}  // namespace mooncake::metrics
