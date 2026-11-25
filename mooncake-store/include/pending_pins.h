#pragma once

// PendingPins: soft pins an external controller asked for before their keys
// were written (BatchUpdateSoftPin with `pending`). Each key is recorded here
// first; the write that publishes the key takes the pin and applies it. A pin
// whose key never shows up runs out at its deadline and is dropped. The table
// is bounded: past `capacity`, the oldest recorded pin is dropped. It is not
// replicated; a pin is a hint.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace mooncake {

class PendingPins {
   public:
    using Clock = std::chrono::system_clock;

    struct Pin {
        Clock::time_point deadline;
        uint32_t priority{0};
    };

    /// What became of pins recorded here; read with `counters()`.
    struct Counters {
        uint64_t recorded{0};
        uint64_t applied{0};
        uint64_t expired{0};
        uint64_t dropped_full{0};
    };

    explicit PendingPins(size_t capacity) : capacity_(capacity) {}

    Counters counters() const {
        return {recorded_.load(std::memory_order_relaxed),
                applied_.load(std::memory_order_relaxed),
                expired_.load(std::memory_order_relaxed),
                dropped_full_.load(std::memory_order_relaxed)};
    }

    // Records a pin for `key` until `deadline`. A key already recorded keeps
    // the later deadline and the higher priority.
    void Record(std::string_view tenant, std::string_view key,
                Clock::time_point deadline, uint32_t priority,
                Clock::time_point now) {
        std::lock_guard<std::mutex> lock(mutex_);
        const std::string id = Id(tenant, key);
        if (auto it = index_.find(id); it != index_.end()) {
            Entry& entry = *it->second;
            entry.pin.deadline = std::max(entry.pin.deadline, deadline);
            entry.pin.priority = std::max(entry.pin.priority, priority);
            order_.splice(order_.end(), order_, it->second);
            return;
        }
        while (!order_.empty() && (order_.size() >= capacity_ ||
                                   order_.front().pin.deadline <= now)) {
            (order_.front().pin.deadline <= now ? expired_ : dropped_full_)
                .fetch_add(1, std::memory_order_relaxed);
            index_.erase(order_.front().id);
            order_.pop_front();
        }
        if (capacity_ != 0) {
            order_.push_back(Entry{id, Pin{deadline, priority}});
            index_.emplace(id, std::prev(order_.end()));
            recorded_.fetch_add(1, std::memory_order_relaxed);
        } else {
            dropped_full_.fetch_add(1, std::memory_order_relaxed);
        }
        size_.store(order_.size(), std::memory_order_relaxed);
    }

    // Removes and returns the pin recorded for `key`, if one is still live.
    // Every write asks, so an empty table answers without the lock. A pin
    // recorded before the write's key lock was taken is seen: Record and the
    // writer's key lock order it.
    std::optional<Pin> Take(std::string_view tenant, std::string_view key,
                            Clock::time_point now) {
        if (size_.load(std::memory_order_relaxed) == 0) {
            return std::nullopt;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = index_.find(Id(tenant, key));
        if (it == index_.end()) {
            return std::nullopt;
        }
        const Pin pin = it->second->pin;
        order_.erase(it->second);
        index_.erase(it);
        size_.store(order_.size(), std::memory_order_relaxed);
        if (pin.deadline <= now) {
            expired_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }
        applied_.fetch_add(1, std::memory_order_relaxed);
        return pin;
    }

    size_t Size() const { return size_.load(std::memory_order_relaxed); }

   private:
    struct Entry {
        std::string id;
        Pin pin;
    };

    static std::string Id(std::string_view tenant, std::string_view key) {
        std::string id;
        id.reserve(tenant.size() + 1 + key.size());
        id.append(tenant).push_back('\0');
        id.append(key);
        return id;
    }

    const size_t capacity_;
    std::mutex mutex_;
    std::atomic<size_t> size_{0};
    std::atomic<uint64_t> recorded_{0};
    std::atomic<uint64_t> applied_{0};
    std::atomic<uint64_t> expired_{0};
    std::atomic<uint64_t> dropped_full_{0};
    // Oldest recorded first.
    std::list<Entry> order_;
    std::unordered_map<std::string, std::list<Entry>::iterator> index_;
};

}  // namespace mooncake
