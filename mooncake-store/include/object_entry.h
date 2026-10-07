#pragma once

// ObjectEntry: the per-object runtime shell. The ObjectMetadata envelope is
// the single source of identity (user_key, group_id) and group-lease wiring;
// the entry adds the per-key runtime state, the flag marking the one
// publication it stands for, and the lock guarding all of it.
//
// The lock never leaves the class: everything below the identity is reachable
// only through WithExclusiveAccess or WithSharedAccess, which hold it for the
// callback, so a caller cannot act on half of a compound operation.

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>

#include "object_metadata.h"
#include "object_runtime_state.h"

namespace mooncake {

class ObjectEntry {
   public:
    // What the entry adds to the envelope: the per-key task state, at most one
    // in-flight task per entry, and the lifecycle claims over it.
    struct State {
        // A primary write or a background task is in flight for this key.
        bool is_processing{false};
        // Teardown-once claim: a second eraser of the same entry bails out
        // instead of double-releasing its refcounts, quota charges and KV
        // removal events.
        bool is_torn_down{false};
        std::optional<ReplicationTask> replication_task;
        std::optional<OffloadingTask> offloading_task;
        std::optional<PromotionTask> promotion_task;
        std::optional<PromotionCandidate> promotion_candidate;
        std::optional<DynamicReplicaPending> dynamic_replication_pending;
        std::chrono::steady_clock::time_point dynamic_replication_cooldown{};
    };

    // Takes ownership of a non-null metadata envelope; the envelope carries the
    // object's identity (user_key, group_id).
    explicit ObjectEntry(std::unique_ptr<ObjectMetadata> metadata)
        : metadata_(std::move(metadata)) {}

    // Not copyable/movable: it owns per-object state and a per-object lock.
    ObjectEntry(const ObjectEntry&) = delete;
    ObjectEntry& operator=(const ObjectEntry&) = delete;
    ObjectEntry(ObjectEntry&&) = delete;
    ObjectEntry& operator=(ObjectEntry&&) = delete;

    // Identity lives in the envelope, which holds both as const members, so
    // these read without taking `mutex_`.
    const std::string& key() const noexcept { return metadata_->user_key; }
    const std::string& group_id() const noexcept { return metadata_->group_id; }

    // True once the route has published this entry. An entry instance stands
    // for exactly one publication — `ObjectIndex::Insert` asserts that it is
    // published once — so a handle names one publication and no more.
    [[nodiscard]] bool IsPublished() const noexcept {
        return published_.load(std::memory_order_relaxed);
    }

    // Runs `fn(envelope, state)` with the entry held exclusively and returns
    // whatever `fn` returns. Lock order: entry → route → metadata spin lock,
    // never the reverse for any pair. Both references last only for the call.
    template <typename Fn>
    decltype(auto) WithExclusiveAccess(Fn&& fn) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return std::forward<Fn>(fn)(*metadata_, state_);
    }

    // The same for readers: `fn` sees both halves but may not mutate them.
    template <typename Fn>
    decltype(auto) WithSharedAccess(Fn&& fn) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return std::forward<Fn>(fn)(std::as_const(*metadata_),
                                    std::as_const(state_));
    }

   private:
    friend class ObjectIndex;  // claims the entry at route publication

    std::unique_ptr<ObjectMetadata> metadata_;
    // Atomic because publication claims it under the route lock while a holder
    // reads it without any lock. Relaxed: the flag carries no other state.
    std::atomic<bool> published_{false};
    // Mutable so a const entry can still be read under the shared lock.
    mutable std::shared_mutex mutex_;
    State state_ GUARDED_BY(mutex_);
};

}  // namespace mooncake
