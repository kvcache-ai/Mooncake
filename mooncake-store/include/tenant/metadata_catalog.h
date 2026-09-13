#pragma once

// MetadataCatalog: the authoritative boundary between MasterService and the
// tenant metadata module. Owns the tenant registry (a lock-free COW directory
// of TenantCatalog aggregates) and the lifecycle rules callers must not
// reconstruct from raw map operations:
//   - atomic tenant get-or-create;
//   - tenants are published fully initialized and stay reachable (no eager
//     reclamation);
//   - object publication and teardown go through the ObjectIndex
//     identity-checked operations (EraseIf / is_torn_down).
//
// RAII read/write access to an object runs through MasterService's
// MetadataAccessorRO/RW (pin + per-object lock).

#include <functional>
#include <memory>

#include "tenant/tenant_catalog.h"
#include "tenant/tenant_directory.h"
#include "tenant/tenant_id.h"

namespace mooncake {
namespace metadata {

class MetadataCatalog {
   public:
    MetadataCatalog() = default;

    // Atomic tenant get-or-create: concurrent first writers for the same
    // tenant all observe the one winning TenantCatalog. The factory stays a
    // caller parameter because publication requires service-side
    // initialization (quota binding) before the handle escapes.
    std::shared_ptr<TenantCatalog> GetOrCreateTenant(
        const TenantId& tenant_id,
        std::function<std::shared_ptr<TenantCatalog>()> factory) {
        return tenants_.GetOrCreate(tenant_id, std::move(factory));
    }

    // Null handle when the tenant is absent; lock-free.
    std::shared_ptr<TenantCatalog> Lookup(const TenantId& tenant_id) const {
        return tenants_.Lookup(tenant_id);
    }

    // Snapshot-consistent visit of every tenant. The visitor must not mutate
    // the registry (no GetOrCreateTenant/RemoveTenant).
    template <typename Fn>
    void Visit(Fn&& fn) const {
        tenants_.Visit(std::forward<Fn>(fn));
    }

    // Explicit tenant removal (admin paths only; there is no eager
    // empty-tenant reclamation — see the retention note in MasterService).
    void Remove(const TenantId& tenant_id) { tenants_.Remove(tenant_id); }

    // Rebuild group membership and shared-lease deadlines from object metadata
    // in every tenant (snapshot / standby restore path).
    void RebuildGroupState() {
        tenants_.Visit(
            [](const TenantId&, const std::shared_ptr<TenantCatalog>& handle) {
                handle->RebuildGroupState();
            });
    }

   private:
    TenantDirectory<std::shared_ptr<TenantCatalog>> tenants_;
};

}  // namespace metadata
}  // namespace mooncake
