#pragma once

#include <cstddef>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

namespace mooncake {

/**
 * @class BoundedDedupQueue
 * @brief Fixed-capacity FIFO queue with per-key de-duplication.
 *
 * A push is dropped (returns false) when the key is already queued or the queue
 * is full. This bounds memory under high QPS and never blocks the
 * read path; events that are dropped are recovered by the periodic evict loop.
 *
 * The de-dup key is stored alongside the event so the queue never needs to
 * inspect the event's fields, and the de-dup marker is cleared exactly when the
 * event is popped (so the same key can be re-queued afterwards). Thread-safe.
 */
template <class Event>
class BoundedDedupQueue {
   public:
    explicit BoundedDedupQueue(size_t capacity) : capacity_(capacity) {}

    // Already queued / full => returns false (event dropped).
    bool TryPush(const std::string& key, Event ev) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= capacity_) {
            return false;
        }
        if (!in_queue_.insert(key).second) {
            return false;  // key already in the queue
        }
        queue_.emplace_back(key, std::move(ev));
        return true;
    }

    // FIFO pop; clears the de-dup marker so the key can be re-queued.
    bool TryPop(Event& out) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty()) {
            return false;
        }
        out = std::move(queue_.front().second);
        in_queue_.erase(queue_.front().first);
        queue_.pop_front();
        return true;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    size_t capacity() const { return capacity_; }

   private:
    mutable std::mutex mutex_;
    std::deque<std::pair<std::string, Event>> queue_;  // FIFO
    std::unordered_set<std::string> in_queue_;
    size_t capacity_;
};

}  // namespace mooncake
