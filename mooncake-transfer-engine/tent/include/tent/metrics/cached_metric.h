// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "ylt/metric.hpp"

namespace mooncake {
namespace tent {
namespace metrics {

// Movable acquire/release cell-pointer slot. std::atomic is not movable, so
// it cannot live in a resizeable vector; moves only happen while the vector
// is being built (before any recording).
class CellSlot {
   public:
    CellSlot() = default;
    CellSlot(const CellSlot&) = delete;
    CellSlot& operator=(const CellSlot&) = delete;
    CellSlot(CellSlot&& other) noexcept
        : cell_(other.cell_.load(std::memory_order_relaxed)) {}
    CellSlot& operator=(CellSlot&& other) noexcept {
        cell_.store(other.cell_.load(std::memory_order_relaxed),
                    std::memory_order_relaxed);
        return *this;
    }

    std::atomic<int64_t>* load() const {
        return cell_.load(std::memory_order_acquire);
    }
    void store(std::atomic<int64_t>* c) {
        cell_.store(c, std::memory_order_release);
    }

   private:
    std::atomic<std::atomic<int64_t>*> cell_{nullptr};
};

// Metrics primitives with pre-resolved label cells. ylt's dynamic counter
// inc() / histogram observe() take a mutex-protected map lookup per call, and
// because TENT labels come from small fixed enums, all updates for a label
// land on the same shard mutex. These primitives resolve the atomic cell
// backing a label once and cache it in a per-label-index slot, reducing the
// steady-state update to relaxed atomic adds.
//
// Labels are never erased by TentMetrics, so resolved cells stay valid for
// the process lifetime. Slot publication uses release/acquire, so
// concurrent first-use resolutions are race free.

template <uint8_t N>
class CachedDynamicCounter
    : public ylt::metric::basic_dynamic_counter<int64_t, N> {
   public:
    using Base = ylt::metric::basic_dynamic_counter<int64_t, N>;
    using Label = std::array<std::string, N>;

    // label_domain_size: number of distinct label values ever recorded; the
    // caller maps each label to a stable index < this size.
    CachedDynamicCounter(std::string name, std::string help,
                         std::array<std::string, N> labels_name,
                         size_t label_domain_size)
        : Base(std::move(name), std::move(help), std::move(labels_name)),
          slots_(label_domain_size) {}

    // Resolve (creating on first use) the atomic cell backing a label.
    std::atomic<int64_t>* resolve(const Label& label) {
        auto cell = Base::try_emplace(label).first;
        return &cell->value;
    }

    // Increment via the cached cell; make_label runs only on first use.
    template <typename LabelFn>
    void incCached(size_t label_index, LabelFn&& make_label,
                   int64_t value = 1) {
        std::atomic<int64_t>* cell = slots_[label_index].load();
        if (cell == nullptr) [[unlikely]] {
            cell = resolve(make_label());
            slots_[label_index].store(cell);
        }
        cell->fetch_add(value, std::memory_order_relaxed);
    }

   private:
    std::vector<CellSlot> slots_;
};

// Histogram composed of CachedDynamicCounter buckets and a sum counter.
// Same bucketing semantics as ylt's basic_dynamic_histogram (inclusive upper
// bounds, trailing +Inf bucket), but observe goes through cached cells.
template <uint8_t N>
class CachedDynamicHistogram {
   public:
    using Label = std::array<std::string, N>;
    using Counter = CachedDynamicCounter<N>;

    CachedDynamicHistogram(const std::string& name, const std::string& help,
                           const std::vector<double>& boundaries,
                           const std::array<std::string, N>& labels_name,
                           size_t label_domain_size)
        : name_(name),
          help_(help),
          labels_name_(labels_name),
          boundaries_(boundaries),
          sum_(std::make_unique<Counter>(name + "_sum", help, labels_name,
                                         label_domain_size)) {
        for (size_t i = 0; i < boundaries_.size() + 1; ++i) {
            buckets_.push_back(std::make_unique<Counter>(
                name, help, labels_name, label_domain_size));
        }
        bucket_slots_.resize(label_domain_size * buckets_.size());
        sum_slots_.resize(label_domain_size);
    }

    // Observation via cached bucket and sum cells.
    template <typename LabelFn>
    void observeCached(size_t label_index, LabelFn&& make_label,
                       int64_t value) {
        const size_t bucket = bucketIndex(value);
        const size_t slot = label_index * buckets_.size() + bucket;
        std::atomic<int64_t>* cell = bucket_slots_[slot].load();
        if (cell == nullptr) [[unlikely]] {
            cell = buckets_[bucket]->resolve(make_label());
            bucket_slots_[slot].store(cell);
        }
        cell->fetch_add(1, std::memory_order_relaxed);

        std::atomic<int64_t>* sum_cell = sum_slots_[label_index].load();
        if (sum_cell == nullptr) [[unlikely]] {
            sum_cell = sum_->resolve(make_label());
            sum_slots_[label_index].store(sum_cell);
        }
        sum_cell->fetch_add(value, std::memory_order_relaxed);
    }

    // Scrape-time accessors, typed as the ylt base for copy()/value().
    std::vector<ylt::metric::basic_dynamic_counter<int64_t, N>*>
    bucketCounters() const {
        std::vector<ylt::metric::basic_dynamic_counter<int64_t, N>*> counters;
        counters.reserve(buckets_.size());
        for (auto& bucket : buckets_) {
            counters.push_back(bucket.get());
        }
        return counters;
    }

    ylt::metric::basic_dynamic_counter<int64_t, N>* sumCounter() {
        return sum_.get();
    }

    const std::string& name() const { return name_; }
    const std::string& help() const { return help_; }
    const std::array<std::string, N>& labelsName() const {
        return labels_name_;
    }
    const std::vector<double>& boundaries() const { return boundaries_; }

   private:
    // Index of the first boundary >= value (ylt bucketing); values above the
    // last boundary land in the trailing +Inf bucket.
    size_t bucketIndex(int64_t value) const {
        return static_cast<size_t>(std::distance(
            boundaries_.begin(),
            std::lower_bound(boundaries_.begin(), boundaries_.end(), value)));
    }

    std::string name_;
    std::string help_;
    std::array<std::string, N> labels_name_;
    std::vector<double> boundaries_;
    std::vector<std::unique_ptr<Counter>> buckets_;
    std::unique_ptr<Counter> sum_;
    std::vector<CellSlot> bucket_slots_;
    std::vector<CellSlot> sum_slots_;
};

}  // namespace metrics
}  // namespace tent
}  // namespace mooncake
