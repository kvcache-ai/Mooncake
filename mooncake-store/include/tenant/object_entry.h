#pragma once

// ObjectEntry: the per-object unit that consolidates the per-key runtime task
// state that previously lived as N separate MasterService TenantState maps
// keyed by the same string. The host wires the ObjectMetadata envelope at
// construction, so a published entry is always fully materialized; the
// per-object mutex is the mutation boundary.

#include <chrono>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <utility>

#include "lease.h"
#include "object_metadata.h"
#include "replica.h"
#include "tenant/object_entry_types.h"
#include "types.h"

namespace mooncake {
namespace tenant {

class ObjectEntry {
   public:
    // Takes ownership of a non-null metadata envelope; `key`/`group_id` must
    // agree with `metadata`'s.
    ObjectEntry(std::string key, std::string group_id,
                std::unique_ptr<ObjectMetadata> metadata)
        : key_(std::move(key)),
          group_id_(std::move(group_id)),
          metadata_(std::move(metadata)) {}

    // Not copyable/movable: it owns per-object state and a per-object lock.
    ObjectEntry(const ObjectEntry&) = delete;
    ObjectEntry& operator=(const ObjectEntry&) = delete;
    ObjectEntry(ObjectEntry&&) = delete;
    ObjectEntry& operator=(ObjectEntry&&) = delete;

    const std::string& key() const { return key_; }
    const std::string& group_id() const { return group_id_; }

    // Authoritative lease: a singleton never sets one (lease() stays null); a
    // grouped member points at the group's shared Lease so the read path can
    // extend the group TTL without touching the group table. Written only
    // before publish or under `mutex` (restore re-wiring); readers hold
    // `mutex`.
    std::shared_ptr<Lease> lease() const { return lease_; }
    void set_lease(std::shared_ptr<Lease> lease) { lease_ = std::move(lease); }

    // Per-key runtime task state. By convention at most one in-flight task
    // (replication / offload / promotion / dynamic-replication pending) is
    // wired per entry; the transitions are driven by MasterService.
    bool is_processing{false};
    // Set under `mutex` by EraseMetadata so a second eraser that pinned the
    // entry before the route erase bails out instead of double-releasing
    // refcounts, quota charges and KV removal events.
    bool is_torn_down{false};
    std::optional<ReplicationTask> replication_task;
    std::optional<OffloadingTask> offloading_task;
    std::optional<PromotionTask> promotion_task;
    std::optional<PromotionCandidate> promotion_candidate;
    std::optional<DynamicReplicaPending> dynamic_replication_pending;
    std::chrono::steady_clock::time_point dynamic_replication_cooldown{};

    // Per-object mutation boundary: the narrowest lock a point operation may
    // hold after pinning this entry. Deliberately public: compound operations
    // must release it midway (entry mutex and route_lock_ are never held
    // together), which a closure API cannot express. Lock order: this mutex
    // first, then ObjectMetadata's own SpinLock, never the reverse.
    mutable std::shared_mutex mutex;

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
    const std::string key_;
    const std::string group_id_;
    std::shared_ptr<Lease> lease_;
    std::unique_ptr<ObjectMetadata> metadata_;
};

}  // namespace tenant
}  // namespace mooncake
