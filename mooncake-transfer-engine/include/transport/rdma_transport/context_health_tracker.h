// Copyright 2024 KVCache.AI
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

#ifndef CONTEXT_HEALTH_TRACKER_H
#define CONTEXT_HEALTH_TRACKER_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

namespace mooncake {

// Context-level circuit-breaker state for the local RNIC. A streak of
// consecutive all-rails-failed submit batches trips the breaker ONLY when the
// streak spans at least `min_peers` distinct peer servers: a store-client
// batch targets a single peer, so a streak of failures confined to one peer
// indicates a dead/restarting peer, not a dead local NIC, and must never
// deactivate the whole context. After a trip, tryReactivate() implements
// half-open recovery: the trip clears once a TTL has elapsed and the streak
// restarts from zero, so a genuinely dead local NIC re-trips after another
// threshold's worth of failed batches while a false positive self-heals.
//
// The clock is injected so trip/TTL logic is unit-testable without RDMA
// hardware or real sleeps. All methods are thread-safe: recordFailure runs on
// submitter threads, recordSuccess on transfer-worker threads, and
// tryReactivate/reset on the monitor thread.
//
// State-transition methods accept a callback executed UNDER the tracker
// mutex, so an external flag (the RdmaContext active flag) changes atomically
// with the tracker's tripped state. Without this, a submitter's trip
// (tripped_ = true, then set_active(false)) could interleave with the monitor
// thread's physical-event recovery (set_active(true), then reset()) such that
// the context ends inactive while the tracker is untripped -- and with no
// armed trip, TTL reactivation never runs, leaving the context skipped
// forever. Callbacks must be cheap and lock-free (a plain flag store).
class ContextHealthTracker {
   public:
    using Clock = std::function<uint64_t()>;  // nanosecond timestamp source

    explicit ContextHealthTracker(Clock clock) : clock_(std::move(clock)) {}

    struct FailureRecord {
        bool tripped_now = false;   // exactly one call per trip returns true
        int streak = 0;             // consecutive all-rails-failed batches
        size_t distinct_peers = 0;  // distinct peers spanned by the streak
        std::string peer_sample;    // bounded comma-joined names (trip only)
    };

    // Record one submit batch whose slices ALL failed with no available rail.
    // `peers` holds the distinct peer server names the batch targeted. Trips
    // (once) when the streak reaches `failure_threshold` AND spans at least
    // `min_peers` distinct peers; `on_trip` runs under the tracker mutex at
    // that moment (deactivate the context here so the flag and the trip are
    // one atomic transition).
    template <typename OnTrip>
    FailureRecord recordFailure(const std::unordered_set<std::string> &peers,
                                int failure_threshold, int min_peers,
                                OnTrip &&on_trip) {
        std::lock_guard<std::mutex> lock(mu_);
        FailureRecord rec;
        if (tripped_.load(std::memory_order_relaxed)) {
            // In-flight straggler batches after the trip must not re-trip,
            // double-log, or move the trip timestamp.
            rec.streak = streak_.load(std::memory_order_relaxed);
            rec.distinct_peers = streak_peers_.size();
            return rec;
        }
        rec.streak = streak_.fetch_add(1, std::memory_order_relaxed) + 1;
        for (const auto &peer : peers) {
            if (streak_peers_.size() >= kMaxTrackedPeers) break;
            streak_peers_.insert(peer);
        }
        rec.distinct_peers = streak_peers_.size();
        if (rec.streak >= failure_threshold &&
            rec.distinct_peers >= static_cast<size_t>(min_peers)) {
            tripped_.store(true, std::memory_order_release);
            trip_time_ns_ = clock_();
            rec.tripped_now = true;
            size_t emitted = 0;
            for (const auto &peer : streak_peers_) {
                if (emitted == kPeerSampleCount) break;
                if (emitted++) rec.peer_sample += ", ";
                rec.peer_sample += peer;
            }
            on_trip();
        }
        return rec;
    }

    FailureRecord recordFailure(const std::unordered_set<std::string> &peers,
                                int failure_threshold, int min_peers) {
        return recordFailure(peers, failure_threshold, min_peers, [] {});
    }

    // A processed CQE batch ends the streak. Lock-free fast path when the
    // streak is already zero (the hot path: called once per poll pass).
    // Deliberately does NOT clear an armed trip -- reactivation is owned by
    // the monitor thread via tryReactivate()/reset(), keeping set_active(true)
    // on a single thread. The fast path may read a stale zero while a
    // concurrent recordFailure increments; that skipped clear is benign (the
    // next successful poll clears it, and a genuine streak has no interleaved
    // successes by definition).
    void recordSuccess() {
        if (streak_.load(std::memory_order_relaxed) == 0) return;
        std::lock_guard<std::mutex> lock(mu_);
        if (tripped_.load(std::memory_order_relaxed)) return;
        streak_.store(0, std::memory_order_relaxed);
        streak_peers_.clear();
    }

    // True while the breaker holds the context inactive. Lock-free.
    bool tripped() const { return tripped_.load(std::memory_order_acquire); }

    // If tripped and `ttl_ns` has elapsed since the trip, clear the trip and
    // the streak (half-open) and return true -- once per trip. ttl_ns == 0
    // means "never auto-reactivate" (legacy latch) and always returns false.
    // `on_reactivate` runs under the tracker mutex when clearing (reactivate
    // the context here so the flag and the clear are one atomic transition).
    template <typename OnReactivate>
    bool tryReactivate(uint64_t ttl_ns, OnReactivate &&on_reactivate) {
        if (ttl_ns == 0) return false;
        std::lock_guard<std::mutex> lock(mu_);
        if (!tripped_.load(std::memory_order_relaxed)) return false;
        if (clock_() < trip_time_ns_ + ttl_ns) return false;
        clearLocked();
        on_reactivate();
        return true;
    }

    bool tryReactivate(uint64_t ttl_ns) {
        return tryReactivate(ttl_ns, [] {});
    }

    // Full reset: successful physical-event recovery, or a fatal async event
    // taking
    // ownership of the context's inactive state (cancels any pending
    // reactivation so an event-deactivated context is never resurrected by
    // the breaker's TTL). `under_lock` runs under the tracker mutex: pass the
    // event's context-flag update (true after successful delayed recovery,
    // false for fatal events) so it cannot interleave with a concurrent
    // submitter trip's flag update.
    template <typename UnderLock>
    void reset(UnderLock &&under_lock) {
        std::lock_guard<std::mutex> lock(mu_);
        clearLocked();
        under_lock();
    }

    void reset() {
        reset([] {});
    }

    // For tests / diagnostics.
    int streak() {
        std::lock_guard<std::mutex> lock(mu_);
        return streak_.load(std::memory_order_relaxed);
    }
    size_t distinctPeerCount() {
        std::lock_guard<std::mutex> lock(mu_);
        return streak_peers_.size();
    }
    uint64_t tripTimeNs() {
        std::lock_guard<std::mutex> lock(mu_);
        return trip_time_ns_;
    }

   private:
    void clearLocked() {
        tripped_.store(false, std::memory_order_release);
        streak_.store(0, std::memory_order_relaxed);
        streak_peers_.clear();
        trip_time_ns_ = 0;
    }

    // Saturation cap for the streak's peer set (== the config maximum for
    // min_peers), bounding memory during long failure storms.
    static constexpr size_t kMaxTrackedPeers = 64;
    // Peer names included in the trip log line.
    static constexpr size_t kPeerSampleCount = 4;

    const Clock clock_;
    std::mutex mu_;
    std::atomic<bool> tripped_{false};  // lock-free tripped() reads
    std::atomic<int> streak_{0};        // lock-free recordSuccess fast path
    std::unordered_set<std::string> streak_peers_;  // guarded by mu_
    uint64_t trip_time_ns_ = 0;                     // guarded by mu_
};

}  // namespace mooncake

#endif  // CONTEXT_HEALTH_TRACKER_H
