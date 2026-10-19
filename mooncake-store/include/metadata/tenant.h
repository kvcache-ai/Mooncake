#pragma once

// Tenant: one tenant's object route, which holds its objects, the lock of each
// key and the lifecycle of its groups, plus the quota account it was built
// with. Replica-action leases and promotion candidates belong to their own
// subsystems, which validate what they hold against the publication the route
// holds before acting on it.
//
// Identity is the publication: every object published on the route gets a
// generation, so code that lets go of a key and comes back later names the
// object it judged with a route::ObjectRef and acts only while that
// publication is still there. See route::ObjectRoute for the locking rules.

#include "metadata/object_route.h"
#include "tenant/quota.h"

namespace mooncake {
namespace metadata {

class Tenant {
   public:
    // `quota_account` is the tenant's account in the quota table, or null when
    // quotas are off. The registry's factory resolves it before building the
    // tenant, and it never changes afterwards: the quota table keeps one stable
    // account per tenant id and a policy recompute updates it in place.
    explicit Tenant(TenantQuotaHandle quota_account = nullptr)
        : quota_account_(quota_account) {}

    Tenant(const Tenant&) = delete;
    Tenant& operator=(const Tenant&) = delete;

    // A guard or cursor taken on the route must not outlive the tenant, so
    // whoever takes one keeps its handle to the tenant for as long; a removal
    // from the registry drops only the registry's handle.
    [[nodiscard]] route::ObjectRoute& Route() { return route_; }
    [[nodiscard]] const route::ObjectRoute& Route() const { return route_; }

    // True when the tenant holds no object and no group membership.
    [[nodiscard]] bool Empty() const { return route_.Empty(); }

    // --- Quota account -------------------------------------------------------

    [[nodiscard]] TenantQuotaHandle QuotaAccount() const {
        return quota_account_;
    }

   private:
    // Object key -> the key's slot, its lock and, while it holds an object,
    // that object's metadata and state; with the group index beside it.
    route::ObjectRoute route_;

    // The tenant's quota account, fixed at construction.
    const TenantQuotaHandle quota_account_;
};

}  // namespace metadata
}  // namespace mooncake
