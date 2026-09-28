#pragma once

// TenantRegistry: tenant id -> a strong handle to that tenant's metadata, and
// the lifecycle rules MasterService resolves tenants through. A tenant is
// created atomically, published fully initialized, and stays reachable, so an
// in-flight reader never finds its tenant reclaimed.
//
// Every object operation resolves its tenant while tenants are created and
// removed rarely, so the table is one map under a reader-writer lock: a lookup
// takes the lock shared, finds the tenant and copies its handle out, and a
// create or remove takes it exclusively. The handle, not the lock, keeps a
// tenant alive, so a remove cannot invalidate a tenant a reader already
// resolved. The registry locks only its own map: each tenant synchronizes its
// own containers.
//
// The factory is bound once at construction, so every tenant of one registry is
// built the same way and a caller only names the tenant it wants; it is where a
// tenant's quota account is resolved and handed to the tenant. It runs before
// the registry lock is taken, so it may take other locks (the quota table's)
// without nesting them under this one, and a handle never escapes before the
// tenant it names is fully built. Racing creators of one id may each run it,
// and only one result is published, so it must have no effect that cannot be
// repeated, and it must not re-enter this registry.
//
// Alive is not current: `Remove` drops the tenant from the registry while a
// handle a caller already holds stays valid, and a later get-or-create for the
// same id builds a new tenant. A caller that must act on the reachable instance
// resolves it through `Lookup` rather than reusing an old handle.

#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "metadata/tenant.h"
#include "tenant_id.h"

namespace mooncake {
namespace metadata {

class TenantRegistry {
   public:
    // One tenant id in, one initialized tenant out.
    using TenantFactory =
        std::function<std::shared_ptr<Tenant>(const TenantId&)>;

    explicit TenantRegistry(TenantFactory factory)
        : factory_(std::move(factory)) {}

    // Null when the tenant is absent.
    [[nodiscard]] std::shared_ptr<Tenant> Lookup(
        const TenantId& tenant_id) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto it = tenants_.find(tenant_id);
        return it == tenants_.end() ? nullptr : it->second;
    }

    // Atomic get-or-create: concurrent callers for the same tenant all observe
    // the one Tenant the winner published; a loser's build is dropped. The
    // shared lookup covers the common case, so the factory runs and the
    // exclusive lock is taken on a miss only.
    [[nodiscard]] std::shared_ptr<Tenant> GetOrCreateTenant(
        const TenantId& tenant_id) {
        if (auto tenant = Lookup(tenant_id)) {
            return tenant;
        }
        auto candidate = factory_(tenant_id);
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return tenants_.try_emplace(tenant_id, std::move(candidate))
            .first->second;
    }

    // Drops the tenant from the registry. A handle a caller already holds stays
    // valid, so an in-flight operation keeps working on the tenant it resolved.
    void Remove(const TenantId& tenant_id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        tenants_.erase(tenant_id);
    }

    // Walks the tenants present when the walk starts. Their handles are
    // collected under the shared lock and `fn` runs after it is released, so
    // `fn` may take the tenant's own locks and may create or remove tenants. A
    // change made from `fn` (or from any other thread) shows in the next walk,
    // never in this one: a tenant created from the callback joins the next
    // walk, and a tenant removed during the walk is still visited.
    template <typename Fn>
    void Visit(Fn&& fn) const {
        std::vector<std::pair<TenantId, std::shared_ptr<Tenant>>> tenants;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            tenants.assign(tenants_.begin(), tenants_.end());
        }
        for (const auto& entry : tenants) {
            fn(entry.first, entry.second);
        }
    }

   private:
    const TenantFactory factory_;
    // Shared by lookups and walks, exclusive for the rare creates and removes.
    mutable std::shared_mutex mutex_;
    std::unordered_map<TenantId, std::shared_ptr<Tenant>, TenantIdHash>
        tenants_;
};

}  // namespace metadata
}  // namespace mooncake
