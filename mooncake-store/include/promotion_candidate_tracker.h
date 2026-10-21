#pragma once

// Promotion retry candidates: the keys whose LOCAL_DISK -> MEMORY promotion a
// transient gate turned away, kept so the retry loop can try them again.
//
// The tracker owns the index of keys carrying a candidate, ordered by when
// each is due for another try, the global count held against the candidate
// limit, and the retry rules. It owns no object: the candidate of a key lives
// on route::ObjectState, guarded by the lock of the metadata shard holding
// the key, and the *Locked methods apply the rules to a state whose shard
// lock the caller already holds. Every change to a candidate goes through
// them, so the index files each candidate at its retry_after, and a retry
// round reads only the candidates that are due instead of walking objects.
//
// Lock order: shard lock -> index lock. The index lock is a leaf.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "metadata/object_state.h"
#include "object_runtime_state.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

namespace test {
class MasterServiceTestPeer;
}  // namespace test

class PromotionCandidateTracker {
   public:
    struct Key {
        TenantId tenant_id;
        std::string key;

        friend bool operator==(const Key&, const Key&) = default;
    };

    PromotionCandidateTracker() = default;

    PromotionCandidateTracker(const PromotionCandidateTracker&) = delete;
    PromotionCandidateTracker& operator=(const PromotionCandidateTracker&) =
        delete;

    // Whether no candidate is held, so a retry round has nothing to do.
    bool Empty() const;

    // Whether a rejection is worth retrying: the gate it hit clears on its own.
    static bool IsTransient(PromotionQueueResult result);

    // Up to `limit` keys whose candidate is due by `now`, the longest overdue
    // first, as a snapshot to resolve again under each key's shard lock.
    std::vector<Key> Due(std::chrono::steady_clock::time_point now,
                         size_t limit) const;

    // --- Per-object candidate; the caller holds the shard lock -------------

    // Records a candidate on the object, or refreshes the one it carries with
    // new demand: the retry budget starts over, the execution-failure history
    // does not. A new candidate is dropped once the global limit is reached.
    void RecordLocked(const TenantId& tenant_id, const std::string& key,
                      route::ObjectState& state, uint8_t sketch_score,
                      PromotionCandidateReason reason, ErrorCode last_error,
                      uint32_t execution_failures = 0);
    // Drops the object's candidate, if it carries one.
    void EraseLocked(const TenantId& tenant_id, const std::string& key,
                     route::ObjectState& state);
    // Drops the candidate as its promotion is admitted, returning the execution
    // failures its chain carries into the task: 0 for a fresh chain.
    uint32_t ConsumeLocked(const TenantId& tenant_id, const std::string& key,
                           route::ObjectState& state);
    // Whether the object carries a candidate due for another try. One whose TTL
    // or retry budget ran out is dropped instead.
    bool DueLocked(const TenantId& tenant_id, const std::string& key,
                   route::ObjectState& state,
                   std::chrono::steady_clock::time_point now);
    // Counts one more retry the transient gate `result` rejected, and backs the
    // candidate off, or drops it once its TTL or retry budget ran out.
    void BackoffLocked(const TenantId& tenant_id, const std::string& key,
                       route::ObjectState& state, PromotionQueueResult result);
    // Drops the index entry of a key that no longer carries a candidate,
    // because its object went away without EraseLocked.
    void ForgetLocked(const TenantId& tenant_id, const std::string& key);

    // Forgets every candidate, for a metadata reload that already dropped the
    // objects carrying them.
    void Reset();

   private:
    friend class test::MasterServiceTestPeer;

    struct KeyHash {
        size_t operator()(const Key& k) const noexcept {
            return TenantIdHash{}(k.tenant_id) ^
                   (std::hash<std::string>{}(k.key) * 1099511628211ULL);
        }
    };
    using TimePoint = std::chrono::steady_clock::time_point;

    static constexpr size_t kLimit = 50000;
    // Retry budget is sized to the condition it waits on: the watermark /
    // queue-cap / push-failure gates clear on the client's offload heartbeat
    // (10s-scale), not in milliseconds. The old budget (8 retries ≈ 2.3s)
    // expired candidates long before their condition could clear, silently
    // killing promotions whose only trigger was a one-off read. 64 retries
    // with a 5s backoff cap spans ~5 minutes (≈ 30 heartbeat ticks); the TTL
    // bounds how long an unread key can keep a slot.
    static constexpr uint32_t kMaxRetries = 64;
    static constexpr std::chrono::milliseconds kTtl{300000};
    static constexpr std::chrono::milliseconds kInitialBackoff{10};
    static constexpr std::chrono::milliseconds kMaxBackoff{5000};

    static std::chrono::milliseconds Backoff(uint32_t retry_count);
    static bool Stale(const PromotionCandidate& candidate, TimePoint now);
    // Files the key at `due`, moving it if it is filed already. Returns
    // whether it was new to the index.
    bool File(const TenantId& tenant_id, const std::string& key, TimePoint due);
    // Returns whether the key was filed.
    bool Unfile(const TenantId& tenant_id, const std::string& key);
    void DecrementCount();

    // Advisory: a relaxed count against kLimit, not a barrier.
    std::atomic<uint64_t> count_{0};

    mutable std::mutex mutex_;
    // When each filed key is due, and the same keys in due order. The order
    // points at the keys of `due_`, whose nodes never move.
    std::unordered_map<Key, TimePoint, KeyHash> due_;
    std::set<std::pair<TimePoint, const Key*>> order_;
};

}  // namespace mooncake
