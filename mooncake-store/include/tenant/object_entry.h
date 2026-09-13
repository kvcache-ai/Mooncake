#pragma once

// ObjectEntry: the per-object runtime shell. The ObjectMetadata envelope is
// the single source of identity (user_key, group_id) and group-lease wiring;
// the entry adds the per-key task state and the per-object mutation
// boundary.

#include <chrono>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>

#include "lease.h"
#include "object_metadata.h"
#include "tenant/object_entry_types.h"

namespace mooncake {
namespace metadata {

class ObjectEntry {
   public:
    // Takes ownership of a non-null metadata envelope; the envelope carries
    // the object's identity (user_key, group_id).
    explicit ObjectEntry(std::unique_ptr<ObjectMetadata> metadata)
        : metadata_(std::move(metadata)) {}

    // Not copyable/movable: it owns per-object state and a per-object lock.
    ObjectEntry(const ObjectEntry&) = delete;
    ObjectEntry& operator=(const ObjectEntry&) = delete;
    ObjectEntry(ObjectEntry&&) = delete;
    ObjectEntry& operator=(ObjectEntry&&) = delete;

    // Identity lives in the envelope; these are pass-through views.
    const std::string& key() const { return metadata_->user_key; }
    const std::string& group_id() const { return metadata_->group_id; }

    // Monotonic generation assigned by ObjectIndex at route publication
    // (0 = never published). Lets a pinned handle distinguish itself from a
    // later replacement of the same key; see ObjectIndex::IsCurrent.
    uint64_t generation() const { return generation_; }

    // Per-key task state; at most one in-flight task per entry, driven by
    // MasterService. Guarded by `mutex`.
    bool is_processing{false};
    // Teardown-once claim (set under `mutex` by EraseMetadata): a second
    // eraser of the same entry bails out instead of double-releasing
    // refcounts, quota charges and KV removal events.
    bool is_torn_down{false};
    std::optional<ReplicationTask> replication_task;
    std::optional<OffloadingTask> offloading_task;
    std::optional<PromotionTask> promotion_task;
    std::optional<PromotionCandidate> promotion_candidate;
    std::optional<DynamicReplicaPending> dynamic_replication_pending;
    std::chrono::steady_clock::time_point dynamic_replication_cooldown{};

    // Per-object mutation boundary; see the lock-order note below. The
    // returned lock may be released and reacquired midway through a compound
    // operation. Lock order: entry mutex → route lock → metadata spin lock;
    // never the reverse for any pair.
    std::unique_lock<std::shared_mutex> LockUnique() const {
        return std::unique_lock<std::shared_mutex>(mutex);
    }
    std::shared_lock<std::shared_mutex> LockShared() const {
        return std::shared_lock<std::shared_mutex>(mutex);
    }
    // Non-blocking probe: an owning lock when the mutex was free, an empty
    // one when it was already held.
    [[nodiscard]] std::unique_lock<std::shared_mutex> TryLockUnique() const {
        return std::unique_lock<std::shared_mutex>(mutex, std::try_to_lock);
    }

    // Owned envelope; never null (enforced by the constructor). Read under
    // `mutex` when the entry may be mid-mutation.
    ObjectMetadata& metadata() const { return *metadata_; }

    // Callback-scoped access: runs `fn(metadata())` while the per-object
    // mutex is held; the reference must not escape the callback.
    template <typename Fn>
    void WithMetadata(Fn&& fn) const {
        std::unique_lock<std::shared_mutex> lock(mutex);
        std::forward<Fn>(fn)(*metadata_);
    }

   private:
    friend class ObjectIndex;  // assigns generation_ at route publication

    std::unique_ptr<ObjectMetadata> metadata_;
    uint64_t generation_{0};
    mutable std::shared_mutex mutex;
};

}  // namespace metadata
}  // namespace mooncake
