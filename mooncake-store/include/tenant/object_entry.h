#pragma once

// ObjectEntry: the tenant-scoped, per-object unit that consolidates the per-key
// runtime task state that today lives as N separate TenantState maps keyed by the
// same string. MasterService's redundant per-key maps (replication_tasks,
// offloading_tasks, promotion_tasks, promotion_candidates,
// dynamic_replication_pending/leases/cooldowns, processing_keys) collapse into
// this single record.
//
// It is deliberately pointer/value friendly and self-contained: the host still
// owns ObjectMetadata (the cache/data envelope) alongside it; the metadata
// envelope is wired in by the caller. The per-object mutex is the mutation
// boundary.
//
// NOTE: reuse of the shared task types (mooncake::ReplicationTask, ...) means
// this type and MasterService now share a single authoritative definition of the
// per-key state, rather than duplicating it.

#include <chrono>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/functional/hash.hpp>

#include "lease.h"
#include "object_entry_types.h"
#include "object_metadata.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {
namespace tenant {

class ObjectEntry {
 public:
    ObjectEntry(std::string key, std::string group_id)
        : key_(std::move(key)), group_id_(std::move(group_id)) {}
    ~ObjectEntry() = default;

    // Not copyable/movable: it owns per-object state and a per-object lock.
    ObjectEntry(const ObjectEntry&) = delete;
    ObjectEntry& operator=(const ObjectEntry&) = delete;
    ObjectEntry(ObjectEntry&&) = delete;
    ObjectEntry& operator=(ObjectEntry&&) = delete;

    const std::string& key() const { return key_; }
    const std::string& group_id() const { return group_id_; }

    // Authoritative lease. A singleton owns one Lease; a grouped member points
    // at the group's shared Lease so the read path can extend the group TTL
    // without touching the group table. Never null after being wired.
    std::shared_ptr<Lease> lease() const { return lease_; }
    void set_lease(std::shared_ptr<Lease> lease) {
        lease_ = std::move(lease);
    }
    bool IsGrouped() const { return !group_id_.empty(); }

    // ---- per-key runtime task state (consolidated) ----
    bool is_processing{false};
    std::optional<ReplicationTask> replication_task;
    std::optional<OffloadingTask> offloading_task;
    std::optional<PromotionTask> promotion_task;
    std::optional<PromotionCandidate> promotion_candidate;
    std::optional<DynamicReplicaPending> dynamic_replication_pending;
    std::unordered_map<UUID, ReplicaActionLease, boost::hash<UUID>>
        dynamic_replication_leases;
    std::chrono::steady_clock::time_point dynamic_replication_cooldown{};

    // Per-object mutation boundary: the narrowest lock a point operation may
    // hold after pinning this entry.
    mutable std::shared_mutex mutex;

    // ---- metadata envelope (cache/data) ----
    // ObjectMetadata is NON-movable / NON-copyable and self-locking, so it is
    // owned through a pointer here. A live routed object has metadata wired in;
    // an entry that has not yet materialized metadata (e.g. during teardown, or
    // a module-level unit test that only exercises task state) has a null
    // metadata_ and must not be handed out for object work.
    ObjectMetadata* metadata() const { return metadata_.get(); }
    bool has_metadata() const { return metadata_ != nullptr; }

    // Attach/construct the metadata envelope. Takes ownership. Returns the
    // prior metadata (if any) to the caller so it can tear down accounting
    // without double-destroying.
    std::unique_ptr<ObjectMetadata> SetMetadata(
        std::unique_ptr<ObjectMetadata> metadata) {
        return std::exchange(metadata_, std::move(metadata));
    }
    std::unique_ptr<ObjectMetadata> TakeMetadata() {
        return std::move(metadata_);
    }

    // Callback-scoped access to the metadata. Runs `fn(*metadata())` while the
    // per-object mutex is held; the metadata reference must not escape the
    // callback scope. No-op when metadata is not yet wired.
    template <typename Fn>
    void WithMetadata(Fn&& fn) const {
        std::unique_lock<std::shared_mutex> lock(mutex);
        if (metadata_) {
            std::forward<Fn>(fn)(*metadata_);
        }
    }

 private:
    const std::string key_;
    const std::string group_id_;
    std::shared_ptr<Lease> lease_;
    std::unique_ptr<ObjectMetadata> metadata_;  // owned; null until wired
};

}  // namespace tenant
}  // namespace mooncake
