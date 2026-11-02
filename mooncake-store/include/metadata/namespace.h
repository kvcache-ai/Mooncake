#pragma once

// Namespace: one tenant's object route, the list of its objects with work in
// flight, the index eviction reads, and the namespace policy over it, if any.
// Replica-action leases and promotion candidates belong to their own
// subsystems, which hold their own state and validate it against the object the
// route holds before acting.
//
// The route (route::ObjectRoute) owns the keys, their locks, the objects and
// the group index; the namespace keeps the in-flight list and the eviction
// index in step with every write guard the route releases, and reports each
// release to the policy. Read releases go to the eviction index alone, and
// only when it observes reads.

#include <array>
#include <cassert>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "common/transparent_string_hash.h"
#include "metadata/eviction_index.h"
#include "metadata/lease_eviction_index.h"
#include "metadata/namespace_policy.h"
#include "metadata/object_route.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace metadata {

class Namespace final : private route::RouteObserver {
   public:
    // `policy`, when set, outlives the namespace; `attachment` is what it hung
    // on this namespace when it was created. Without an `eviction_index` the
    // namespace ranks its objects by lease (LeaseEvictionIndex).
    explicit Namespace(TenantId id = {}, NamespacePolicy* policy = nullptr,
                       std::unique_ptr<PolicyAttachment> attachment = nullptr,
                       std::unique_ptr<EvictionIndex> eviction_index = nullptr)
        // The route is built first, so it reads the index before it moves.
        : objects(this, eviction_index != nullptr &&
                            eviction_index->observes_reads()),
          id_(std::move(id)),
          policy_(policy),
          attachment_(std::move(attachment)),
          eviction_index_(eviction_index != nullptr
                              ? std::move(eviction_index)
                              : std::make_unique<LeaseEvictionIndex>()) {}
    Namespace(const Namespace&) = delete;
    Namespace& operator=(const Namespace&) = delete;

    route::ObjectRoute objects;

    const TenantId& id() const { return id_; }

    // The policy's say on the object `guard` holds taking `bytes` more memory,
    // asked before the memory is allocated; zero bytes asks only whether the
    // namespace admits writes. Always granted without a policy.
    [[nodiscard]] tl::expected<void, ErrorCode> Grow(
        const route::WriteGuard& guard, uint64_t bytes) const {
        if (policy_ == nullptr) {
            return {};
        }
        return policy_->OnGrow(Context(guard), bytes);
    }

    // Tell the policy the object `guard` holds is about to be replaced under
    // this same lock, or torn down for good; see NamespacePolicy::OnReplace
    // and OnTearDown.
    void BeforeReplace(const route::WriteGuard& guard) const {
        if (policy_ != nullptr) {
            policy_->OnReplace(Context(guard));
        }
    }
    void BeforeTearDown(const route::WriteGuard& guard) const {
        if (policy_ != nullptr) {
            policy_->OnTearDown(Context(guard));
        }
    }

    // True when the namespace holds no object and no group membership.
    [[nodiscard]] bool Empty() const { return objects.Empty(); }

    // The order eviction visits this namespace's objects in.
    [[nodiscard]] const EvictionIndex& eviction_index() const {
        return *eviction_index_;
    }

    // --- Work in flight ------------------------------------------------------
    //
    // The namespace lists the keys whose object carries work in flight (see
    // route::ObjectState::HasWorkInFlight), so a sweep for expired work walks
    // those instead of every object. Work only starts or finishes under a write
    // guard, and the route reports every write guard to the namespace as it is
    // released: a key is listed exactly while it holds an object that carries
    // work as of the last write guard on it. A write cursor neither starts nor
    // finishes such work.

    // The listed keys, as a snapshot to resolve again: an object can finish,
    // be torn down or be replaced once the stripe lock is released.
    [[nodiscard]] std::vector<std::string> InFlightKeys() const {
        std::vector<std::string> keys;
        for (const InFlightStripe& stripe : in_flight_) {
            std::lock_guard<std::mutex> lock(stripe.lock);
            for (const route::KeySlot& slot : stripe.slots) {
                keys.push_back(slot.key());
            }
        }
        return keys;
    }

   private:
    PolicyContext Context(const route::WriteGuard& guard) const {
        return PolicyContext{id_, attachment_.get(), guard};
    }

    void OnWriteRelease(const route::WriteGuard& guard) override {
        SyncInFlight(guard);
        eviction_index_->OnWrite(guard);
        if (policy_ != nullptr) {
            policy_->OnWriteRelease(Context(guard));
        }
        // A key with no object may be dropped once this guard lets go, or
        // published again, so whatever is kept on it must be back to its
        // initial value by now.
        assert(guard.has_object() || IsInitial(guard.owner_state()));
    }

    static bool IsInitial(const route::SlotOwnerState& owner) {
        return !owner.in_flight_listed && !owner.eviction_filed &&
               owner.policy_words == decltype(owner.policy_words){};
    }

    void OnReadRelease(const route::ReadGuard& guard) override {
        eviction_index_->OnRead(guard);
    }

    // Puts the held key on the in-flight list or takes it off, to match
    // whether it holds an object that carries work. The stripe lock is taken
    // only when that changed.
    void SyncInFlight(const route::WriteGuard& guard) {
        route::SlotOwnerState& owner = guard.owner_state();
        const bool listed =
            guard.has_object() && guard.state().HasWorkInFlight();
        if (listed == owner.in_flight_listed) {
            return;
        }
        InFlightStripe& stripe = InFlightStripeOf(guard.key());
        std::lock_guard<std::mutex> lock(stripe.lock);
        if (listed) {
            stripe.slots.push_back(guard.slot());
        } else {
            stripe.slots.erase(stripe.slots.iterator_to(guard.slot()));
        }
        owner.in_flight_listed = listed;
    }

    using InFlightList = route::KeySlot::InFlightList;

    // One stripe of the in-flight list. Starting and finishing a write each
    // touch the list, so it is striped by key rather than put under one lock
    // for the whole namespace. Only writers take these locks, and only to
    // link or unlink, so it needs far fewer stripes than the route.
    struct InFlightStripe {
        mutable std::mutex lock;
        InFlightList slots;
    };

    static constexpr size_t kInFlightStripeCount = 64;

    InFlightStripe& InFlightStripeOf(std::string_view key) {
        return in_flight_[TransparentStringHash{}(key) % kInFlightStripeCount];
    }

    const TenantId id_;
    NamespacePolicy* const policy_;
    const std::unique_ptr<PolicyAttachment> attachment_;

    // The keys with work in flight, and the keys eviction may visit.
    // Declared after the route so they are destroyed first: the route's slots
    // must outlive the hooks the lists hold.
    std::array<InFlightStripe, kInFlightStripeCount> in_flight_;
    const std::unique_ptr<EvictionIndex> eviction_index_;
};

}  // namespace metadata
}  // namespace mooncake
