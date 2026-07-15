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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_set>
#include <utility>

namespace mooncake {

// Context-level circuit-breaker state for the local RNIC. Submit-side
// all-rails-unavailable failures and local completion failures accumulate in
// separate evidence channels because they have different strength and peer
// qualification. Either channel may trip the one shared breaker, but neither
// can preload the other's streak. A submit streak trips ONLY when it spans at
// least `min_peers` distinct peer servers: a store-client batch targets a
// single peer, so failures confined to one peer indicate a dead/restarting
// peer, not a dead local NIC. After a trip, tryReactivate() implements
// half-open recovery: the shared trip and both evidence channels clear once a
// TTL has elapsed, so a genuinely dead local NIC can build fresh evidence and
// re-trip while a false positive self-heals.
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

    enum class FailureSource : uint8_t {
        kAllRailsUnavailable = 0,
        kLocalCompletion = 1,
    };

    explicit ContextHealthTracker(Clock clock) : clock_(std::move(clock)) {}

    struct FailureRecord {
        bool tripped_now = false;   // exactly one call per trip returns true
        int streak = 0;             // consecutive batches for this source
        size_t distinct_peers = 0;  // peers spanned by this source's streak
        std::string peer_sample;    // bounded comma-joined names (trip only)
    };

    // Record one failed batch in the selected evidence channel. `peers` holds
    // the distinct peer server names the batch targeted. Trips the shared
    // breaker (once) when this source's streak reaches `failure_threshold` AND
    // spans at least `min_peers` distinct peers; `on_trip` runs under the
    // tracker mutex at that moment (deactivate the context here so the flag and
    // the trip are one atomic transition).
    template <typename OnTrip>
    FailureRecord recordFailure(FailureSource source,
                                const std::unordered_set<std::string> &peers,
                                int failure_threshold, int min_peers,
                                OnTrip &&on_trip) {
        std::lock_guard<std::mutex> lock(mu_);
        FailureRecord rec;
        auto &state = failure_states_[sourceIndex(source)];
        if (tripped_.load(std::memory_order_relaxed)) {
            // In-flight straggler batches after the trip must not re-trip,
            // double-log, or move the trip timestamp.
            rec.streak = state.streak;
            rec.distinct_peers = state.peers.size();
            return rec;
        }
        rec.streak = ++state.streak;
        has_untripped_failures_.store(true, std::memory_order_relaxed);
        for (const auto &peer : peers) {
            if (state.peers.size() >= kMaxTrackedPeers) break;
            state.peers.insert(peer);
        }
        rec.distinct_peers = state.peers.size();
        if (rec.streak >= failure_threshold &&
            rec.distinct_peers >= static_cast<size_t>(min_peers)) {
            tripped_.store(true, std::memory_order_release);
            trip_time_ns_ = clock_();
            rec.tripped_now = true;
            size_t emitted = 0;
            for (const auto &peer : state.peers) {
                if (emitted == kPeerSampleCount) break;
                if (emitted++) rec.peer_sample += ", ";
                rec.peer_sample += peer;
            }
            on_trip();
        }
        return rec;
    }

    FailureRecord recordFailure(FailureSource source,
                                const std::unordered_set<std::string> &peers,
                                int failure_threshold, int min_peers) {
        return recordFailure(source, peers, failure_threshold, min_peers,
                             [] {});
    }

    // A processed CQE batch proves local data movement and ends both evidence
    // streaks. Lock-free fast path when both channels are already empty (the
    // hot path: called once per poll pass).
    // Deliberately does NOT clear an armed trip -- reactivation is owned by
    // the monitor thread via tryReactivate()/reset(), keeping set_active(true)
    // on a single thread. The fast path may read a stale zero while a
    // concurrent recordFailure adds evidence; that skipped clear is benign
    // (the next successful poll clears it, and a genuine streak has no
    // interleaved successes by definition).
    void recordSuccess() {
        if (!has_untripped_failures_.load(std::memory_order_relaxed)) return;
        std::lock_guard<std::mutex> lock(mu_);
        if (tripped_.load(std::memory_order_relaxed)) return;
        clearFailureStatesLocked();
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
    int streak(FailureSource source) {
        std::lock_guard<std::mutex> lock(mu_);
        return failure_states_[sourceIndex(source)].streak;
    }
    size_t distinctPeerCount(FailureSource source) {
        std::lock_guard<std::mutex> lock(mu_);
        return failure_states_[sourceIndex(source)].peers.size();
    }
    uint64_t tripTimeNs() {
        std::lock_guard<std::mutex> lock(mu_);
        return trip_time_ns_;
    }

   private:
    struct FailureState {
        int streak = 0;
        std::unordered_set<std::string> peers;
    };

    static constexpr size_t kFailureSourceCount = 2;

    static constexpr size_t sourceIndex(FailureSource source) {
        return static_cast<size_t>(source);
    }

    void clearFailureStatesLocked() {
        for (auto &state : failure_states_) {
            state.streak = 0;
            state.peers.clear();
        }
        has_untripped_failures_.store(false, std::memory_order_relaxed);
    }

    void clearLocked() {
        tripped_.store(false, std::memory_order_release);
        clearFailureStatesLocked();
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
    // Lets recordSuccess avoid locking when neither channel has evidence.
    std::atomic<bool> has_untripped_failures_{false};
    std::array<FailureState, kFailureSourceCount> failure_states_;  // mu_
    uint64_t trip_time_ns_ = 0;                                     // mu_
};

}  // namespace mooncake

#endif  // CONTEXT_HEALTH_TRACKER_H
