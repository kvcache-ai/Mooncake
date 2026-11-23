#pragma once

// Promotion retry candidates: the keys whose LOCAL_DISK -> MEMORY promotion a
// transient gate turned away, kept so the retry loop can try them again.
//
// Fully self-contained and thread-safe: the tracker owns both each candidate's
// state and its due-time index across cache-line-aligned shards, guarded
// exclusively by its own internal shard locks.

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/concurrent_sharded_map.h"
#include "object_runtime_state.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

namespace test {
class MasterServiceTestPeer;
}  // namespace test

class PromotionCandidateTracker {
   public:
    using Key = ScopedObjectKey;
    using TimePoint = std::chrono::steady_clock::time_point;

    PromotionCandidateTracker() = default;

    PromotionCandidateTracker(const PromotionCandidateTracker&) = delete;
    PromotionCandidateTracker& operator=(const PromotionCandidateTracker&) =
        delete;

    bool Empty() const;
    static bool IsTransient(PromotionQueueResult result);

    void Record(const TenantId& tenant_id, std::string_view key,
                uint8_t sketch_score, PromotionCandidateReason reason,
                ErrorCode last_error, uint32_t execution_failures = 0);

    void Erase(const TenantId& tenant_id, std::string_view key);

    uint32_t Consume(const TenantId& tenant_id, std::string_view key);

    std::vector<Key> TakeDue(TimePoint now, size_t limit);

    void Backoff(const TenantId& tenant_id, std::string_view key,
                 PromotionQueueResult result);

    std::optional<PromotionCandidate> Find(const TenantId& tenant_id,
                                           std::string_view key) const;

    size_t CountByTenant(const TenantId& tenant_id) const;

   private:
    friend class test::MasterServiceTestPeer;

    using EntryMap = std::unordered_map<Key, PromotionCandidate,
                                        ScopedObjectKeyHash, ScopedObjectKeyEq>;

    static constexpr size_t kShardCount = 256;
    static constexpr size_t kLimit = 50000;
    static constexpr uint32_t kMaxRetries = 64;
    static constexpr std::chrono::milliseconds kTtl{300000};
    static constexpr std::chrono::milliseconds kInitialBackoff{10};
    static constexpr std::chrono::milliseconds kMaxBackoff{5000};
    static constexpr int64_t kNoDueNs = std::numeric_limits<int64_t>::max();

    struct alignas(64) Shard {
        std::atomic<size_t> size{0};
        std::atomic<int64_t> min_due_ns{kNoDueNs};
        ShardPresenceFilter filter;
        mutable std::mutex mutex;
        // Node-based map so `order` pointers to `const Key*` stay stable.
        EntryMap entries;
        std::set<std::pair<TimePoint, const Key*>> order;
    };

    static int64_t ToNs(TimePoint tp) noexcept {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   tp.time_since_epoch())
            .count();
    }
    static void RefreshMinDueLocked(Shard& shard) noexcept;
    static std::chrono::milliseconds BackoffDelay(uint32_t retry_count);
    static bool Stale(const PromotionCandidate& candidate, TimePoint now);
    Shard& ShardForHash(uint64_t mixed_hash) noexcept {
        return shards_[mixed_hash & (kShardCount - 1)];
    }
    const Shard& ShardForHash(uint64_t mixed_hash) const noexcept {
        return shards_[mixed_hash & (kShardCount - 1)];
    }
    void EraseEntryLocked(Shard& shard, EntryMap::iterator it,
                          uint64_t mixed_hash);
    void DecrementCount();
    void ResetBackoffs();

    std::atomic<uint64_t> count_{0};
    std::array<Shard, kShardCount> shards_;
};

}  // namespace mooncake
