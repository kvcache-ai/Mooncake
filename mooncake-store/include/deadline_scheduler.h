#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {

template <typename Id>
class DeadlineScheduler {
   public:
    using Clock = std::chrono::steady_clock;
    using Callback = std::function<void(const Id& id)>;

   private:
    struct Record;
    using Records = std::multimap<Clock::time_point, std::shared_ptr<Record>>;
    struct Record {
        explicit Record(Id value) : id(std::move(value)) {}
        Id id;
        typename Records::iterator position;
        bool pending{false};
    };
    struct State {
        explicit State(Callback fn) : callback(std::move(fn)) {}
        Callback callback;
        std::mutex mutex;
        std::condition_variable cv;
        Records records;
        bool stopping{false};
    };

   public:
    class Handle {
       public:
        Handle() = default;

        // Explicit and idempotent. Dropping a handle does not cancel its
        // record; cancellation after scheduler destruction is a no-op.
        bool Cancel() const {
            auto state = state_.lock();
            auto record = record_.lock();
            if (!state || !record) return false;
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                if (state->stopping || !record->pending) return false;
                record->pending = false;
                state->records.erase(record->position);
            }
            state->cv.notify_one();
            return true;
        }

       private:
        friend class DeadlineScheduler;
        Handle(const std::shared_ptr<State>& state,
               const std::shared_ptr<Record>& record)
            : state_(state), record_(record) {}
        std::weak_ptr<State> state_;
        std::weak_ptr<Record> record_;
    };

    explicit DeadlineScheduler(Callback callback)
        : state_(std::make_shared<State>(std::move(callback))) {}

    ~DeadlineScheduler() { Stop(); }

    Handle Schedule(Id id, Clock::time_point deadline) {
        std::shared_ptr<Record> record;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            if (state_->stopping) return {};
            if (!timer_thread_.joinable()) {
                // TimerLoop takes the same mutex before observing records.
                // Failed thread construction publishes no startup state.
                timer_thread_ =
                    std::thread([state = state_] { TimerLoop(state); });
            }
            record = std::make_shared<Record>(std::move(id));
            record->position = state_->records.emplace(deadline, record);
            record->pending = true;
        }
        state_->cv.notify_one();
        return Handle(state_, record);
    }

    template <typename Predicate>
    void RemoveIf(Predicate predicate) {
        Records discarded;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            for (auto it = state_->records.begin();
                 it != state_->records.end();) {
                if (predicate(it->second->id)) {
                    it->second->pending = false;
                    auto current = it++;
                    discarded.insert(state_->records.extract(current));
                } else {
                    ++it;
                }
            }
        }
        state_->cv.notify_one();
    }

    void Stop() {
        Records discarded;
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->stopping = true;
            discarded.swap(state_->records);
        }
        state_->cv.notify_all();
        if (timer_thread_.joinable()) timer_thread_.join();
    }

   private:
    static void TimerLoop(const std::shared_ptr<State>& state) {
        while (true) {
            std::vector<std::shared_ptr<Record>> expired;
            {
                std::unique_lock<std::mutex> lock(state->mutex);
                if (state->stopping) return;
                if (state->records.empty()) {
                    state->cv.wait(lock, [&] {
                        return state->stopping || !state->records.empty();
                    });
                    continue;
                }
                auto now = Clock::now();
                const auto next = state->records.begin()->first;
                if (next > now) {
                    // Bound each sleep: older standard libraries convert to
                    // system_clock internally, which can overflow near max().
                    const auto delay = std::min(
                        next - now, Clock::duration(std::chrono::hours(24)));
                    state->cv.wait_for(lock, delay, [&] {
                        return state->stopping || state->records.empty() ||
                               state->records.begin()->first != next;
                    });
                    continue;
                }
                while (!state->records.empty() &&
                       state->records.begin()->first <= now) {
                    auto first = state->records.begin();
                    expired.push_back(first->second);
                    first->second->pending = false;
                    state->records.erase(first);
                }
            }
            // Neither callbacks nor record payload destruction hold mutex.
            for (auto& record : expired) state->callback(record->id);
        }
    }

    std::shared_ptr<State> state_;
    std::thread timer_thread_;
};

}  // namespace mooncake
