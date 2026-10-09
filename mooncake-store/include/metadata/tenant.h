#pragma once

// Tenant: one tenant's object route and the lifecycle of its groups, plus the
// charges against the quota account it was built with. Replica-action leases
// and promotion candidates belong to their own subsystems, which validate what
// they hold against the entry the route publishes before acting on it.
//
// Identity is the entry handle: an entry stands for exactly one publication, so
// `TearDownObject` recognises an object by the handle a caller holds and drops
// its group membership with the slot. An object is reached through `ReadHold`
// or `WriteHold`, which re-check that identity under the entry lock; whether
// an entry is still published is the tenant's to decide, never the caller's.
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
#include <optional>
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
    // in-flight list as that lock is released. `on_published(hold)` then runs
    // under the same hold, for what the publisher does first on the object it
    // just made, before anyone else can reach it. False when the key is
    // already routed, and then nothing was registered and nothing ran.
    template <typename OnPublished>
    [[nodiscard]] bool InsertObject(std::shared_ptr<ObjectEntry> entry,
                                    OnPublished&& on_published) {
        const std::string group_id = entry->group_id();
        ObjectEntry::WriteHold hold(std::move(entry), &Tenant::SettleInFlight,
                                    this);
        if (!object_index_.Insert(hold.handle())) {
            return false;
        }
        if (!group_id.empty()) {
            // AddMember returns null only for an empty group_id, which the
            // guard above already excluded.
            hold.metadata().lease_ =
                group_index_.AddMember(group_id, hold.key());
            assert(hold.metadata().lease_ != nullptr);
        }
        std::forward<OnPublished>(on_published)(hold);
        return true;
    }
    [[nodiscard]] bool InsertObject(std::shared_ptr<ObjectEntry> entry) {
        return InsertObject(std::move(entry),
                            [](const ObjectEntry::WriteHold&) {});
    }

    // Null when the key is absent.
    [[nodiscard]] std::shared_ptr<ObjectEntry> Get(std::string_view key) const {
        return object_index_.Get(key);
    }

    // Ends the publication the hold names, at most once: claims the teardown,
    // runs `release()`, then drops the route slot and the group membership.
    // `release` gives back what hangs off the object (refcounts, quota
    // charges, KV removal events) while the slot is still held, so a newer
    // publication of the same key cannot land before it. Since the claim and
    // the removal share the hold, a routed entry is never torn down. The entry
    // leaves the in-flight list with its slot, whatever work it still carries.
    // False, running nothing, when it was already torn down under this hold.
    template <typename Release>
    [[nodiscard]] bool TearDownObject(const ObjectEntry::WriteHold& hold,
                                      Release&& release) {
        ObjectEntry::State& state = hold.state();
        if (state.is_torn_down) {
            return false;
        }
        state.is_torn_down = true;
        std::forward<Release>(release)();
        SyncInFlight(hold);
        RemoveObject(hold.handle());
        return true;
    }

    // Holds the entry the route currently publishes for `key` under its read
    // or write lock, for the caller's scope. The hold is made only once it has
    // re-checked under that lock that the route still publishes this entry;
    // nullopt otherwise, so a caller never acts on an entry torn down or
    // replaced in between.
    //
    // The entry lock is not recursive: while a hold lives, the caller must not
    // reach the same entry through any accessor here again.
    //
    // Work started or finished under a write hold is put on or taken off the
    // tenant's in-flight list as the hold is released, so the caller has
    // nothing to track.
    [[nodiscard]] std::optional<ObjectEntry::ReadHold> ReadHold(
        std::string_view key) const {
        return HoldIfPublished<LockMode::kRead>(object_index_.Get(key),
                                                nullptr);
    }
    [[nodiscard]] std::optional<ObjectEntry::WriteHold> WriteHold(
        std::string_view key) {
        return HoldIfPublished<LockMode::kWrite>(object_index_.Get(key), this);
    }

    // The same for a handle the caller already has, from this tenant: the
    // hold is made only while the route still publishes this very entry,
    // never a newer publication of its key.
    [[nodiscard]] std::optional<ObjectEntry::ReadHold> ReadHold(
        std::shared_ptr<ObjectEntry> entry) const {
        return HoldIfPublished<LockMode::kRead>(std::move(entry), nullptr);
    }
    [[nodiscard]] std::optional<ObjectEntry::WriteHold> WriteHold(
        std::shared_ptr<ObjectEntry> entry) {
        return HoldIfPublished<LockMode::kWrite>(std::move(entry), this);
    }

    [[nodiscard]] bool ContainsObject(std::string_view key) const {
        return object_index_.Contains(key);
    }

    [[nodiscard]] size_t ObjectCount() const {
        return object_index_.ObjectCount();
    }

    // A cursor over every published object, each under its own read or write
    // lock; see ObjectIndex::ReadCursor for what the loop body may do. An
    // object it visits is the current publication of its key, so the body
    // needs no route check; one that acts after the cursor keeps `handle()`
    // and goes through `WriteHold` like any other handle.
    [[nodiscard]] ObjectIndex::Cursor<LockMode::kRead> ReadCursor() const {
        return object_index_.ReadCursor();
    }
    [[nodiscard]] ObjectIndex::Cursor<LockMode::kWrite> WriteCursor() {
        return object_index_.WriteCursor();
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
        for (auto object : object_index_.ReadCursor()) {
            const ObjectMetadata& metadata = object.metadata();
            if (!metadata.IsGrouped()) {
                continue;
            }
            const auto deadline = metadata.EvictionDeadline();
            auto [it, inserted] =
                max_deadline_by_group.try_emplace(metadata.group_id, deadline);
            if (!inserted) {
                it->second = std::max(it->second, deadline);
            }
        }
        // The group index is below the route in the lock order, so the cursor's
        // loop body registers membership itself.
        for (auto object : object_index_.WriteCursor()) {
            const std::string& group_id = object.metadata().group_id;
            if (group_id.empty()) {
                continue;
            }
            auto lease = group_index_.AddMember(group_id, object.key());
            // A non-empty group_id always yields a lease, as in InsertObject.
            assert(lease != nullptr);
            const auto it = max_deadline_by_group.find(group_id);
            if (it != max_deadline_by_group.end()) {
                lease->ExtendTo(it->second);
            }
            object.metadata().lease_ = std::move(lease);
        }
    }

    // --- Work in flight ------------------------------------------------------
    //
    // The tenant lists the entries that carry work in flight (see
    // ObjectEntry::State::HasInFlightWork), so a sweep for expired work walks
    // those instead of every object. Work only starts or finishes under a write
    // hold, and every write hold the tenant hands out settles the entry's place
    // on the list as it is released: an entry is listed exactly while it is
    // published, not torn down and carries work as of the last write hold on
    // it. A write cursor neither starts nor finishes such work.

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
        for (auto object : object_index_.ReadCursor()) {
            const uint64_t charge = MemoryQuotaCharge(object.metadata());
            if (charge > TenantQuotaAccount::kMaxChargedBytes ||
                charged_bytes > TenantQuotaAccount::kMaxChargedBytes - charge) {
                throw std::overflow_error(
                    "rebuilt tenant quota exceeds 2^63 - 1 bytes");
            }
            charged_bytes += charge;
        }
        return charged_bytes;
    }

    // Resets every routed object's ledger to what its replicas charge. Only
    // while no charge or release is in flight; the account itself is rebuilt
    // by the quota table. Throws std::runtime_error when a ledger rejects it.
    void RebuildQuotaLedgers(const TenantId& tenant_id) {
        for (auto object : object_index_.WriteCursor()) {
            ObjectMetadata& metadata = object.metadata();
            auto rebuild_result = metadata.quota_ledger.Rebuild(
                quota_account_, MemoryQuotaCharge(metadata));
            if (!rebuild_result) {
                throw std::runtime_error(
                    "failed to rebuild object tenant quota ledger for " +
                    tenant_id.value() + "/" + object.key());
            }
        }
    }

   private:
    // Locks `entry` and keeps the hold only while the route still publishes
    // it. No route lookup is needed for that under the entry's lock: only
    // TearDownObject drops a route slot, and it claims the entry under that
    // same lock first, so an entry published and not yet torn down is still
    // the one its slot holds. A write hold settles the in-flight list on
    // release through `owner`.
    template <LockMode kMode>
    [[nodiscard]] static std::optional<ObjectEntry::Hold<kMode>>
    HoldIfPublished(std::shared_ptr<ObjectEntry> entry,
                    [[maybe_unused]] Tenant* owner) {
        if (entry == nullptr) {
            return std::nullopt;
        }
        ObjectEntry::Hold<kMode> hold(std::move(entry));
        if (!hold.handle()->IsPublished() || hold.state().is_torn_down) {
            return std::nullopt;
        }
        if constexpr (kMode == LockMode::kWrite) {
            hold.on_release_ = &Tenant::SettleInFlight;
            hold.hook_context_ = owner;
        }
        return std::optional<ObjectEntry::Hold<kMode>>(std::move(hold));
    }

    // The release hook of the write holds this tenant hands out.
    static void SettleInFlight(void* tenant,
                               const ObjectEntry::WriteHold& hold) {
        static_cast<Tenant*>(tenant)->SyncInFlight(hold);
    }

    // Puts the held entry on the in-flight list or takes it off, to match
    // whether it is published, not torn down and carries work. The stripe
    // lock is taken only when that changed.
    void SyncInFlight(const ObjectEntry::WriteHold& hold) {
        ObjectEntry& entry = *hold.handle();
        ObjectEntry::State& state = hold.state();
        const bool listed = entry.IsPublished() && !state.is_torn_down &&
                            state.HasInFlightWork();
        if (listed == state.in_flight_listed) {
            return;
        }
        InFlightStripe& stripe = InFlightStripeOf(entry.key());
        std::lock_guard<std::mutex> lock(stripe.lock);
        if (listed) {
            stripe.entries.PushBack(entry);
        } else {
            stripe.entries.Erase(entry);
        }
        state.in_flight_listed = listed;
    }

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
