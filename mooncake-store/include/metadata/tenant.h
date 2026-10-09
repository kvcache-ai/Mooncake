#pragma once

// Tenant: one tenant's object route and the lifecycle of its groups, plus the
// charges against the quota account it was built with. Replica-action leases
// and promotion candidates belong to their own subsystems, which validate what
// they hold against the entry the route publishes before acting on it.
//
// Identity is the entry handle: an entry stands for exactly one publication, so
// `TearDownObject` recognises an object by the handle a caller holds and drops
// its group membership with the slot. Reaching a published object goes through
// `WithPublishedObject` or `ReadPublishedObject`, which re-check that identity
// under the entry lock.
//
// Every method synchronizes internally. Lock order: entry lock, route lock,
// then the group table. Only a teardown holds the route lock across the
// group table, and nothing takes the route lock while holding a group stripe.
// An in-flight stripe is taken under the entry lock alone and nests nothing.

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include "common/intrusive_list.h"
#include "common/transparent_string_hash.h"
#include "group_index.h"
#include "object_index.h"
#include "tenant_quota.h"
#include "types.h"

namespace mooncake {
namespace metadata {

class Tenant {
   public:
    // An unmetered tenant, for quotas off: its objects charge nothing.
    Tenant() = default;

    // A metered tenant, bound to its account in the quota table. The binding
    // never changes: the table keeps one stable account per tenant id and a
    // policy recompute updates it in place.
    explicit Tenant(TenantQuotaAccount& quota_account)
        : quota_account_(&quota_account) {}

    // Publishes `entry` on this tenant's route. The route slot and the group
    // lease are wired under the entry's own lock, so a reader that reaches the
    // entry through the route cannot observe a grouped object before its group
    // lease is in place; an entry published with work in flight joins the
    // in-flight list under that same lock. False when the key is already
    // routed, and then nothing was registered.
    [[nodiscard]] bool InsertObject(std::shared_ptr<ObjectEntry> entry) {
        const std::string group_id = entry->group_id();
        bool inserted = false;
        entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                inserted = object_index_.Insert(entry);
                if (inserted && !group_id.empty()) {
                    // AddMember returns null only for an empty group_id, which
                    // the guard above already excluded.
                    metadata.lease_ =
                        group_index_.AddMember(group_id, entry->key());
                    assert(metadata.lease_ != nullptr);
                }
                if (inserted) {
                    TrackInFlight(*entry, state);
                }
            });
        return inserted;
    }

    // Null when the key is absent.
    [[nodiscard]] std::shared_ptr<ObjectEntry> Get(std::string_view key) const {
        return object_index_.Get(key);
    }

    // Ends the publication `entry` stands for, at most once: claims the
    // teardown, runs `release()`, then drops the route slot and the group
    // membership. `release` gives back what hangs off the object (refcounts,
    // quota charges, KV removal events) while the slot is still held, so a
    // newer publication of the same key cannot land before it. The caller holds
    // the entry's own lock and passes the `state` it guards; since the claim
    // and the removal share that hold, a routed entry is never torn down, and
    // the accessors below need only check the route. The entry leaves the
    // in-flight list with its slot, whatever work it still carries. False,
    // running nothing, when the entry was already torn down.
    template <typename Release>
    [[nodiscard]] bool TearDownObject(const std::shared_ptr<ObjectEntry>& entry,
                                      ObjectEntry::State& state,
                                      Release&& release) {
        if (state.is_torn_down) {
            return false;
        }
        state.is_torn_down = true;
        std::forward<Release>(release)();
        UntrackInFlight(*entry);
        RemoveObject(entry);
        return true;
    }

    // Runs `fn(metadata, state)` on the entry the route currently publishes for
    // `key`, under that entry's own lock, and only once it has re-checked under
    // that lock that the slot still publishes this entry. False without running
    // `fn` otherwise, so a caller that kept a handle from before resolves the
    // key again instead of acting on it.
    //
    // The callback runs inside the entry's lock, which is not recursive: it
    // must not reach the same entry through any accessor here again.
    template <typename Fn>
    [[nodiscard]] bool WithPublishedObject(std::string_view key, Fn&& fn) {
        const auto entry = object_index_.Get(key);
        return entry != nullptr &&
               WithPublishedObject(entry, std::forward<Fn>(fn));
    }

    // The same for a handle the caller already holds: `fn` runs only while the
    // route still publishes this very entry, never on a newer publication of
    // its key.
    template <typename Fn>
    [[nodiscard]] bool WithPublishedObject(
        const std::shared_ptr<ObjectEntry>& entry, Fn&& fn) {
        return entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                if (!object_index_.IsCurrent(entry->key(), entry)) {
                    return false;
                }
                std::forward<Fn>(fn)(metadata, state);
                return true;
            });
    }

    // The reader's form, under the entry's shared lock.
    template <typename Fn>
    [[nodiscard]] bool ReadPublishedObject(
        const std::shared_ptr<ObjectEntry>& entry, Fn&& fn) const {
        return entry->WithSharedAccess([&](const ObjectMetadata& metadata,
                                           const ObjectEntry::State& state) {
            if (!object_index_.IsCurrent(entry->key(), entry)) {
                return false;
            }
            std::forward<Fn>(fn)(metadata, state);
            return true;
        });
    }

    [[nodiscard]] bool ContainsObject(std::string_view key) const {
        return object_index_.Contains(key);
    }

    // True while the route publishes this very entry. A caller that holds the
    // entry through ObjectEntry's scoped holds asks this under the hold, which
    // is the same re-check `WithPublishedObject` makes under its lock.
    [[nodiscard]] bool IsPublishedObject(
        const std::shared_ptr<ObjectEntry>& entry) const {
        return entry != nullptr && object_index_.IsCurrent(entry->key(), entry);
    }

    [[nodiscard]] size_t ObjectCount() const {
        return object_index_.ObjectCount();
    }

    // Strong handles to what is routed right now, for a scan that acts on them
    // after it ends: resolve each key again before mutating anything, since an
    // entry can be replaced under the same key in between.
    [[nodiscard]] std::vector<std::shared_ptr<ObjectEntry>> SnapshotObjects()
        const {
        return object_index_.SnapshotObjects();
    }

    // True when the tenant holds no object and no group membership.
    [[nodiscard]] bool Empty() const {
        return object_index_.Empty() && group_index_.Empty();
    }

    // Drops a grouped entry's membership, without touching the route slot;
    // `RemoveObject` calls it as one step of a teardown, and rebuilding
    // membership state calls it on its own. The group and the key
    // come from the entry. Callers must have established that this entry is the
    // one the route publishes, which is what keeps a teardown from dropping the
    // membership a newer publication of the same key registered.
    void UnregisterGroupMember(const std::shared_ptr<ObjectEntry>& entry) {
        const std::string& group_id = entry->group_id();
        if (group_id.empty()) {
            return;
        }
        (void)group_index_.RemoveMember(group_id, entry->key());
    }

    // The member keys of one group, as a snapshot to re-resolve: a group can be
    // dropped and rebuilt while a caller walks the list, so a key in it may
    // resolve to a different object than the one that registered it, or to
    // none. Callers that act on the members, eviction among them, read each key
    // again and check what they find rather than treating the list and the
    // handles as one consistent view.
    [[nodiscard]] std::vector<std::string> GroupMembers(
        std::string_view group_id) const {
        return group_index_.Members(group_id);
    }

    // Rebuilds group membership and the group leases from object metadata, for
    // the snapshot and standby restore paths. The first pass takes the maximum
    // restored deadline per group, so a grouped object is not left on a
    // zero-deadline lease that post-restore cleanup would drop; the second
    // registers membership and points every grouped entry at the group's shared
    // lease. Ungrouped objects keep the lease they were constructed with.
    void RebuildGroupState() {
        std::unordered_map<std::string, std::chrono::system_clock::time_point>
            max_deadline_by_group;
        const auto objects = object_index_.SnapshotObjects();
        for (const auto& entry : objects) {
            entry->WithSharedAccess(
                [&](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                    if (!metadata.IsGrouped()) {
                        return;
                    }
                    const auto deadline = metadata.EvictionDeadline();
                    auto [it, inserted] = max_deadline_by_group.try_emplace(
                        metadata.group_id, deadline);
                    if (!inserted) {
                        it->second = std::max(it->second, deadline);
                    }
                });
        }
        for (const auto& entry : objects) {
            // `group_id` is a const member of the envelope, so it reads without
            // the entry lock, unlike the metadata write below.
            const std::string group_id = entry->group_id();
            if (group_id.empty()) {
                continue;
            }
            auto lease = group_index_.AddMember(group_id, entry->key());
            // A non-empty group_id always yields a lease, as in InsertObject.
            assert(lease != nullptr);
            const auto it = max_deadline_by_group.find(group_id);
            if (it != max_deadline_by_group.end()) {
                lease->ExtendTo(it->second);
            }
            entry->WithExclusiveAccess(
                [&](ObjectMetadata& metadata, ObjectEntry::State&) {
                    metadata.lease_ = std::move(lease);
                });
        }
    }

    // --- Work in flight ------------------------------------------------------
    //
    // The tenant lists the entries that carry work in flight (see
    // ObjectEntry::State::HasInFlightWork), so a sweep for expired work walks
    // those instead of every object. The list may still hold an entry whose
    // work has finished, until something sees it idle and takes it off; it
    // never misses one with work, because whoever starts work on a published
    // entry tracks it under the same hold, and InsertObject tracks an entry
    // published with work already on it. Every call below is made under the
    // entry's exclusive lock, with the `state` that lock guards.

    // Puts a published entry that carries work on the list. A no-op for an
    // entry already on it, one with no work, one not yet published (whose
    // InsertObject tracks it) and one already torn down.
    void TrackInFlight(ObjectEntry& entry, const ObjectEntry::State& state) {
        if (!entry.IsPublished() || state.is_torn_down ||
            !state.HasInFlightWork()) {
            return;
        }
        InFlightStripe& stripe = InFlightStripeOf(entry.key());
        std::lock_guard<std::mutex> lock(stripe.lock);
        if (!InFlightList::IsLinked(entry)) {
            stripe.entries.PushBack(entry);
        }
    }

    // Takes the entry off the list once it carries no work.
    void UntrackIfIdle(ObjectEntry& entry, const ObjectEntry::State& state) {
        if (!state.HasInFlightWork()) {
            UntrackInFlight(entry);
        }
    }

    // The keys of the listed entries, as a snapshot to resolve again: an entry
    // can finish, be torn down or be replaced under its key once the stripe
    // lock is released.
    [[nodiscard]] std::vector<std::string> InFlightKeys() const {
        std::vector<std::string> keys;
        for (const InFlightStripe& stripe : in_flight_) {
            std::lock_guard<std::mutex> lock(stripe.lock);
            for (const ObjectEntry& entry : stripe.entries) {
                keys.push_back(entry.key());
            }
        }
        return keys;
    }

    // --- Quota ---------------------------------------------------------------
    //
    // The tenant charges and releases its own account; an unmetered tenant has
    // none, so every charge below succeeds and every release is a no-op. How
    // large the account is belongs to TenantQuotaManager.

    // A charge held for an operation still in flight. It is given back when
    // the reservation goes out of scope, unless Commit() handed it on to what
    // accounts for it from then on: a task's pending bytes or an object's
    // ledger.
    class QuotaReservation {
       public:
        QuotaReservation(QuotaReservation&& other) noexcept
            : tenant_(other.tenant_), bytes_(std::exchange(other.bytes_, 0)) {}
        QuotaReservation(const QuotaReservation&) = delete;
        QuotaReservation& operator=(const QuotaReservation&) = delete;
        QuotaReservation& operator=(QuotaReservation&&) = delete;
        ~QuotaReservation() { tenant_->ReleaseQuota(bytes_); }

        [[nodiscard]] uint64_t bytes() const { return bytes_; }
        // The bytes, now owed by the caller instead of this reservation.
        [[nodiscard]] uint64_t Commit() { return std::exchange(bytes_, 0); }

       private:
        friend class Tenant;
        QuotaReservation(Tenant& tenant, uint64_t bytes)
            : tenant_(&tenant), bytes_(bytes) {}

        Tenant* tenant_;
        uint64_t bytes_;
    };

    // Charges `bytes` to the account. Zero bytes charges nothing and only
    // checks that the tenant still admits writes.
    [[nodiscard]] tl::expected<void, ErrorCode> ChargeQuota(uint64_t bytes) {
        if (quota_account_ == nullptr) {
            return {};
        }
        auto result = quota_account_->TryCharge(bytes);
        if (result) {
            return {};
        }
        switch (result.error().error) {
            case TenantQuotaError::kTenantNotRegistered:
                return tl::make_unexpected(ErrorCode::TENANT_NOT_REGISTERED);
            case TenantQuotaError::kQuotaExceeded:
                return tl::make_unexpected(ErrorCode::TENANT_QUOTA_EXCEEDED);
            case TenantQuotaError::kInvalidArgument:
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            default:
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    // ChargeQuota, held by a reservation the caller commits or drops.
    [[nodiscard]] tl::expected<QuotaReservation, ErrorCode> ReserveQuota(
        uint64_t bytes) {
        auto charged = ChargeQuota(bytes);
        if (!charged) {
            return tl::make_unexpected(charged.error());
        }
        return QuotaReservation(*this, bytes);
    }

    void ReleaseQuota(uint64_t bytes) {
        if (quota_account_ == nullptr || bytes == 0) {
            return;
        }
        if (!quota_account_->Release(bytes)) {
            LOG(ERROR) << "tenant quota release mismatch bytes=" << bytes;
        }
    }

    // Null only for an unmetered tenant, which the quota ledger takes as
    // nothing to charge.
    [[nodiscard]] TenantQuotaHandle QuotaAccount() const {
        return quota_account_;
    }

    // What an object charges once written: its size for every completed
    // MEMORY replica, saturating at the 64-bit range.
    [[nodiscard]] static uint64_t MemoryQuotaCharge(
        const ObjectMetadata& metadata) {
        const auto completed_replicas =
            metadata.CountReplicas([](const Replica& replica) {
                return replica.is_memory_replica() && replica.is_completed();
            });
        const unsigned __int128 charge =
            static_cast<unsigned __int128>(metadata.size) * completed_replicas;
        return charge > std::numeric_limits<uint64_t>::max()
                   ? std::numeric_limits<uint64_t>::max()
                   : static_cast<uint64_t>(charge);
    }

    // What all routed objects charge together, for a restore that rebuilds the
    // account. Throws std::overflow_error past the accounting range.
    [[nodiscard]] uint64_t QuotaUsage() const {
        uint64_t charged_bytes = 0;
        for (const auto& entry : object_index_.SnapshotObjects()) {
            entry->WithSharedAccess(
                [&](const ObjectMetadata& metadata, const ObjectEntry::State&) {
                    const uint64_t charge = MemoryQuotaCharge(metadata);
                    if (charge > TenantQuotaAccount::kMaxChargedBytes ||
                        charged_bytes >
                            TenantQuotaAccount::kMaxChargedBytes - charge) {
                        throw std::overflow_error(
                            "rebuilt tenant quota exceeds 2^63 - 1 bytes");
                    }
                    charged_bytes += charge;
                });
        }
        return charged_bytes;
    }

    // Resets every routed object's ledger to what its replicas charge. Only
    // while no charge or release is in flight; the account itself is rebuilt
    // by the quota table. Throws std::runtime_error when a ledger rejects it.
    void RebuildQuotaLedgers(const TenantId& tenant_id) {
        for (const auto& entry : object_index_.SnapshotObjects()) {
            entry->WithExclusiveAccess(
                [&](ObjectMetadata& metadata, ObjectEntry::State&) {
                    auto rebuild_result = metadata.quota_ledger.Rebuild(
                        quota_account_, MemoryQuotaCharge(metadata));
                    if (!rebuild_result) {
                        throw std::runtime_error(
                            "failed to rebuild object tenant quota ledger "
                            "for " +
                            tenant_id.value() + "/" + entry->key());
                    }
                });
        }
    }

   private:
    // Drops the route slot and the group membership, only while the slot still
    // holds `entry`. Both go under one hold of the route lock, because
    // membership is keyed by the object key: once the slot is released, a
    // newer publication of the same key can register the membership this
    // teardown would drop.
    void RemoveObject(const std::shared_ptr<ObjectEntry>& entry) {
        object_index_.WithExclusiveRoute(entry->key(), [&](auto& route) {
            const auto it = route.find(entry->key());
            if (it == route.end() || it->second != entry) {
                return;
            }
            route.erase(it);
            UnregisterGroupMember(entry);
        });
    }

    using InFlightList = IntrusiveList<ObjectEntry, InFlightListTag>;

    // One stripe of the in-flight list. Starting and finishing a write each
    // touch the list, so it is striped by key, as the route is, rather than
    // put under one lock for the whole tenant.
    struct InFlightStripe {
        mutable std::mutex lock;
        InFlightList entries;
    };

    static constexpr size_t kInFlightStripeCount = ObjectIndex::kStripeCount;

    InFlightStripe& InFlightStripeOf(std::string_view key) {
        return in_flight_[TransparentStringHash{}(key) % kInFlightStripeCount];
    }

    void UntrackInFlight(ObjectEntry& entry) {
        InFlightStripe& stripe = InFlightStripeOf(entry.key());
        std::lock_guard<std::mutex> lock(stripe.lock);
        if (InFlightList::IsLinked(entry)) {
            stripe.entries.Erase(entry);
        }
    }

    // Primary object route: object key -> strong ObjectEntry handle, with the
    // per-object mutation boundary inside the entry.
    ObjectIndex object_index_;

    // Group membership and the one shared Lease per group.
    GroupIndex group_index_;

    // The tenant's quota account, fixed at construction.
    const TenantQuotaHandle quota_account_{nullptr};

    // The entries with work in flight. Declared after the route so it is
    // destroyed first: the route's handles keep every listed entry alive
    // until the list has let go of its hook.
    std::array<InFlightStripe, kInFlightStripeCount> in_flight_;
};

}  // namespace metadata
}  // namespace mooncake
