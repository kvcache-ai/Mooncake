#pragma once

// Range-parallel eviction executor.
//
// A wave submits only one task per worker. Each worker claims disjoint ranges
// of the cursor, so no two workers visit the same entry, and reclaims the
// candidates it finds in its own ranges. A wave ends when the goal is met, the
// caller cancels, or the cursor view is claimed out; the next wave then widens
// and rescans. Progress is cumulative: a partial wave narrows what the next
// wave still has to find instead of resetting to the original goal.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <future>
#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

#include "thread_pool.h"

namespace mooncake::eviction {

struct RangeResult {
    uint64_t progress = 0;  // Successful visit progress, not entries seen.
    uint64_t waves = 0;     // Widening passes run, including the first.
};

// Given to the visitor so it can end the run early, for example when its policy
// refuses to reclaim anything more. Workers stop claiming entries as soon as
// the visits already in flight return.
class RangeStop {
   public:
    void Stop() { stopped_.store(true, std::memory_order_relaxed); }

    bool Stopped() const { return stopped_.load(std::memory_order_relaxed); }

   private:
    std::atomic<bool> stopped_{false};
};

namespace detail {

// One run. The wave loop, the claim loop and the worker fan-out share the
// cursor, goal and cumulative progress, so they live together.
template <typename Cursor, typename Visit>
class RangeExecutor {
   public:
    RangeExecutor(Cursor& cursor, uint64_t goal, uint64_t range_slots,
                  Visit& visit)
        : cursor_(cursor),
          goal_(goal),
          range_slots_(range_slots),
          visit_(visit) {
        if (range_slots_ == 0) {
            throw std::invalid_argument("invalid range executor size");
        }
    }

    // Runs every wave on `pool` (or inline when `pool` is null), one task per
    // worker thread it owns.
    RangeResult Run(ThreadPool* pool) {
        RangeResult result;
        do {
            ++result.waves;
            StartWave();
            RunWorkers(pool);
            result.progress = Progress();
        } while (WantsAnotherWave() && cursor_.Widen());
        return result;
    }

   private:
    // Starts a new wave: claims the view from the start again, and snapshots
    // what it holds. Entries reclaimed by an earlier wave simply report no
    // progress the second time.
    void StartWave() {
        next_range_.store(0, std::memory_order_relaxed);
        wave_stop_.store(false, std::memory_order_relaxed);
        limit_ = cursor_.Limit();
    }

    // Runs the wave's workers. A single worker is no fan-out at all, so it runs
    // here on the calling thread. Otherwise the wave is over only when every
    // admitted task has finished.
    void RunWorkers(ThreadPool* pool) {
        if (pool == nullptr) {
            RunWorker(0);
            return;
        }
        const auto workers = static_cast<uint32_t>(pool->size());
        if (workers == 0) {
            throw std::invalid_argument("invalid range executor size");
        }
        if (workers == 1) {
            RunWorker(0);
            return;
        }

        // Reserve up front so appending an admitted future cannot reallocate.
        std::vector<std::future<void>> pending;
        pending.reserve(workers);
        for (uint32_t worker = 0; worker < workers; ++worker) {
            auto task = std::make_shared<std::packaged_task<void()>>(
                [this, worker] { RunWorker(worker); });
            auto future = task->get_future();
            pool->enqueue([task] { (*task)(); });
            pending.push_back(std::move(future));
        }

        // Join every task first: get() rethrows, and that must not skip the
        // joins of the tasks still running.
        for (auto& future : pending) {
            future.wait();
        }
        for (auto& future : pending) {
            future.get();
        }
    }

    // Claims ranges of `range_slots_` slots from `next_range_` until `limit_`
    // is passed or a stop is requested.
    void RunWorker(uint32_t worker) {
        while (!Stopped()) {
            const uint64_t begin =
                next_range_.fetch_add(range_slots_, std::memory_order_relaxed);
            if (begin >= limit_) {
                return;
            }
            const uint64_t end = std::min(limit_, begin + range_slots_);
            cursor_.VisitRange(begin, end, [&](const auto& entry) {
                if (Stopped()) {
                    return false;
                }
                return VisitEntry(worker, entry);
            });
        }
    }

    // Visits one entry as `worker` and reports whether the scan may continue.
    template <typename Entry>
    bool VisitEntry(uint32_t worker, const Entry& entry) {
        const uint64_t delta = visit_(worker, entry, run_stop_);
        if (delta == 0) {
            return true;
        }

        // Reaching the goal raises the wave's stop flag so peers stop admitting
        // entries. Entries already in flight are never interrupted, so progress
        // can overshoot the goal by at most one entry per worker.
        const uint64_t done =
            progress_.fetch_add(delta, std::memory_order_relaxed) + delta;
        if (done < goal_) {
            return true;
        }
        wave_stop_.store(true, std::memory_order_relaxed);
        return false;
    }

    bool Stopped() const {
        return wave_stop_.load(std::memory_order_relaxed) ||
               run_stop_.Stopped();
    }

    bool WantsAnotherWave() const {
        return Progress() < goal_ && !run_stop_.Stopped();
    }

    uint64_t Progress() const {
        return progress_.load(std::memory_order_relaxed);
    }

    Cursor& cursor_;
    const uint64_t goal_;
    const uint64_t range_slots_;
    Visit& visit_;
    RangeStop run_stop_;                   // Whole run, set by the visitor.
    uint64_t limit_ = 0;                   // Size of the view, per wave.
    std::atomic<uint64_t> progress_{0};    // Cumulative over waves.
    std::atomic<uint64_t> next_range_{0};  // Next unclaimed range, per wave.
    std::atomic<bool> wave_stop_{false};   // Set once the current wave ends.
};

}  // namespace detail

// Visits (worker, entry, stop) for disjoint candidates and returns the progress
// those visits made, not the number of candidates inspected. A group is one
// visit, so stopping is checked between entries and never inside one group. The
// visitor calls stop.Stop() to end the run early. Every admitted task is joined
// before returning or propagating an exception.
//
// `goal` bounds the run's total progress. Waves repeat while the goal is unmet
// and cursor.Widen() keeps reaching more of the index. `pool` must outlive the
// run and stay running until it returns.
template <typename Cursor, typename Visit>
RangeResult RunRanges(Cursor& cursor, ThreadPool* pool, uint64_t goal,
                      Visit&& visit, uint64_t range_slots = 16 * 1024) {
    if (goal == 0) {
        return {};
    }
    detail::RangeExecutor<Cursor, Visit> executor(cursor, goal, range_slots,
                                                  visit);
    return executor.Run(pool);
}

template <typename Cursor, typename Visit>
RangeResult RunRanges(Cursor& cursor, ThreadPool& pool, uint64_t goal,
                      Visit&& visit, uint64_t range_slots = 16 * 1024) {
    return RunRanges(cursor, &pool, goal, std::forward<Visit>(visit),
                     range_slots);
}

}  // namespace mooncake::eviction
