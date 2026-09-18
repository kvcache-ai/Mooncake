// Throttle state for best-effort SSD prefetch (SSD -> DRAM promotion
// triggered out-of-band, e.g. by exist probes).
//
// Two mechanisms:
//   1. Memory-pressure cooldown: when promotion fails because DRAM is
//      saturated (NO_AVAILABLE_HANDLE), open a short cooldown window during
//      which prefetch is a no-op, so prefetch (which *adds* to DRAM) stops
//      competing with eviction/offload (which *frees* DRAM) on the holder.
//   2. Per-key dedup / rate-limit: suppress duplicate prefetch triggers for
//      the same key within a TTL window, cutting the RPC/thread storm that
//      concurrent exist/get probes would otherwise cause.
//
// Held via shared_ptr so async prefetch jobs can use it safely without
// capturing a raw client pointer that may outlive the work.
//
// Concurrency: the per-key table is sharded (kNumShards independent
// mutex+map pairs) so probes on hot keys do not serialize globally. Expiry
// is lazy: lookups check the per-entry deadline, and each shard sweeps its
// expired entries at most once per dedup TTL (amortized), instead of
// scanning the whole table on every reserve() call.
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace mooncake {

class PrefetchThrottle {
   public:
    enum class State : uint8_t {
        kTriggered = 0,        // reserved, async job not started yet
        kInFlight = 1,         // promotion task registered, executing
        kCompleted = 2,        // promotion committed on the master
        kFailed = 3,           // promotion failed; retryable after backoff
        kAlreadyResident = 4,  // query showed a MEMORY replica; no promotion
        kDelegated = 5,        // handed to a remote holder via RPC; the holder
                               // tracks in-flight state in its own process
    };

    struct Entry {
        int64_t trigger_ms{-1};
        int64_t completed_ms{-1};
        State state{State::kTriggered};
        // Set when the register + promote path actually ran for this key.
        bool promote_attempted{false};
    };

    static int64_t NowMs() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    // Reconfigure (values come in as seconds, stored as ms). Negative values
    // keep the current setting. A 0 cooldown disables the memory-pressure
    // backoff; a 0 dedup TTL disables per-key rate-limiting.
    void configure(int64_t cooldown_sec, int64_t dedup_ttl_sec) {
        if (cooldown_sec >= 0) {
            cooldown_ms_.store(cooldown_sec * 1000, std::memory_order_relaxed);
        }
        if (dedup_ttl_sec >= 0) {
            dedup_ttl_ms_.store(dedup_ttl_sec * 1000,
                                std::memory_order_relaxed);
        }
    }

    bool inCooldown() const {
        return NowMs() < cooldown_until_ms_.load(std::memory_order_relaxed);
    }

    // Open the memory-pressure backoff window. No-op if cooldown is disabled.
    void enterCooldown() {
        const int64_t cooldown_ms =
            cooldown_ms_.load(std::memory_order_relaxed);
        if (cooldown_ms <= 0) {
            return;
        }
        cooldown_until_ms_.store(NowMs() + cooldown_ms,
                                 std::memory_order_relaxed);
    }

    // Returns the subset of keys not seen within their effective window,
    // registering them as kTriggered atomically with the check (so
    // concurrent probes cannot both win the same key).
    //
    // Effective window per state: kFailed entries only block for the
    // (usually much shorter) cooldown window so transient failures retry
    // quickly; all other states block for the full dedup TTL.
    std::vector<std::string> reserve(const std::vector<std::string>& keys) {
        const int64_t ttl_ms = dedup_ttl_ms_.load(std::memory_order_relaxed);
        const int64_t now = NowMs();
        std::vector<std::string> out;
        out.reserve(keys.size());
        for (const auto& key : keys) {
            Shard& shard = ShardFor(key);
            std::lock_guard<std::mutex> lock(shard.mutex);
            MaybeSweep(shard, now, ttl_ms);
            auto it = shard.entries.find(key);
            if (it != shard.entries.end() && !EntryExpired(it->second, now)) {
                continue;
            }
            shard.entries[key] =
                Entry{.trigger_ms = now,
                      .completed_ms = -1,
                      .state = State::kTriggered};
            out.push_back(key);
        }
        return out;
    }

    void markInFlight(const std::string& key) {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        if (it == shard.entries.end()) {
            return;
        }
        it->second.state = State::kInFlight;
        it->second.promote_attempted = true;
    }

    void markCompleted(const std::string& key) {
        const int64_t now = NowMs();
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        if (it == shard.entries.end()) {
            shard.entries[key] = Entry{.trigger_ms = now,
                                       .completed_ms = now,
                                       .state = State::kCompleted};
            return;
        }
        it->second.state = State::kCompleted;
        it->second.completed_ms = now;
    }

    void markFailed(const std::string& key) {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        if (it == shard.entries.end()) {
            return;
        }
        it->second.state = State::kFailed;
    }

    // Async classification decided the key is not SSD-only (e.g. a MEMORY
    // replica is already present). Clears in-flight semantics without
    // treating it as a completed promotion.
    void markAlreadyResident(const std::string& key) {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        if (it == shard.entries.end()) {
            return;
        }
        it->second.state = State::kAlreadyResident;
        it->second.promote_attempted = false;
    }

    // The key was delegated to a remote holder via prefetch_offload_object.
    // Unlike kFailed, a delegated key blocks re-triggering for the full
    // dedup TTL (the holder is working on it; re-delegating every backoff
    // window would spam the holder RPC), and it must never be waited on
    // locally (the holder's throttle tracks execution, not ours).
    void markDelegated(const std::string& key) {
        Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        if (it == shard.entries.end()) {
            return;
        }
        it->second.state = State::kDelegated;
        it->second.promote_attempted = false;
    }

    bool promoteAttempted(const std::string& key) const {
        const Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        return it != shard.entries.end() && it->second.promote_attempted;
    }

    int64_t triggeredAt(const std::string& key) const {
        const Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        return it == shard.entries.end() ? -1 : it->second.trigger_ms;
    }

    int64_t completedAt(const std::string& key) const {
        const Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        if (it == shard.entries.end() || it->second.state != State::kCompleted) {
            return -1;
        }
        return it->second.completed_ms;
    }

    State stateOf(const std::string& key) const {
        const Shard& shard = ShardFor(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.entries.find(key);
        return it == shard.entries.end() ? State::kFailed : it->second.state;
    }

    // Poll until promotion completes or the budget expires. Returns true
    // only when the key reaches kCompleted before the deadline. Terminal
    // non-success states (kFailed, kAlreadyResident, kDelegated) return
    // immediately instead of burning the budget.
    bool waitForCompletion(const std::string& key, int64_t max_wait_ms,
                           int64_t poll_ms = 1) const {
        if (max_wait_ms <= 0) {
            return false;
        }
        const int64_t deadline = NowMs() + max_wait_ms;
        const int64_t step_ms = std::max<int64_t>(poll_ms, 1);
        while (NowMs() < deadline) {
            {
                const State state = stateOf(key);
                if (state == State::kCompleted) {
                    return true;
                }
                if (state == State::kFailed ||
                    state == State::kAlreadyResident ||
                    state == State::kDelegated) {
                    return false;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
        }
        return stateOf(key) == State::kCompleted;
    }

   private:
    static constexpr size_t kNumShards = 16;

    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<std::string, Entry> entries;
        int64_t last_sweep_ms{0};
    };

    Shard& ShardFor(const std::string& key) {
        return shards_[std::hash<std::string>{}(key) % kNumShards];
    }
    const Shard& ShardFor(const std::string& key) const {
        return shards_[std::hash<std::string>{}(key) % kNumShards];
    }

    // An entry blocks re-triggering until its effective window expires:
    // dedup TTL from the last activity for healthy states, cooldown-length
    // backoff from the trigger for failed ones.
    bool EntryExpired(const Entry& entry, int64_t now) const {
        const int64_t ttl_ms = dedup_ttl_ms_.load(std::memory_order_relaxed);
        if (ttl_ms <= 0) {
            return true;
        }
        if (entry.state == State::kFailed) {
            const int64_t backoff_ms =
                cooldown_ms_.load(std::memory_order_relaxed);
            // cooldown == 0 disables the backoff: retry immediately.
            if (backoff_ms <= 0) {
                return true;
            }
            return now - entry.trigger_ms > backoff_ms;
        }
        const int64_t last_ms = entry.completed_ms >= 0 ? entry.completed_ms
                                                        : entry.trigger_ms;
        return now - last_ms > ttl_ms;
    }

    // Amortized cleanup: sweep a shard at most once per TTL so reserve()
    // stays O(batch size) on the hot path instead of O(table size).
    void MaybeSweep(Shard& shard, int64_t now, int64_t ttl_ms) {
        if (ttl_ms <= 0 || now - shard.last_sweep_ms <= ttl_ms) {
            return;
        }
        shard.last_sweep_ms = now;
        for (auto it = shard.entries.begin(); it != shard.entries.end();) {
            if (EntryExpired(it->second, now)) {
                it = shard.entries.erase(it);
            } else {
                ++it;
            }
        }
    }

    std::atomic<int64_t> cooldown_until_ms_{0};
    std::atomic<int64_t> cooldown_ms_{DEFAULT_SSD_PREFETCH_COOLDOWN_SEC *
                                     1000};
    std::atomic<int64_t> dedup_ttl_ms_{DEFAULT_SSD_PREFETCH_DEDUP_TTL_SEC *
                                       1000};
    std::array<Shard, kNumShards> shards_;
};

}  // namespace mooncake
