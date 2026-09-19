#pragma once

// MetadataCatalog: the tenant registry MasterService resolves tenants through.
// It owns one TenantCatalog per tenant and keeps the lifecycle rules out of its
// callers: a tenant is created atomically, published fully initialized, and
// stays reachable, so an in-flight reader never finds its tenant reclaimed.
//
// The factory is bound once at construction, so every tenant of one catalog is
// built the same way and a caller only names the tenant it wants.
//
// Alive is not current: `Remove` drops the tenant from the registry while a
// handle a caller already holds stays valid, and a later get-or-create for the
// same id builds a new tenant. A caller that must act on the reachable instance
// resolves it through `Lookup` rather than reusing an old handle.

#include <functional>
#include <memory>
#include <utility>

#include "tenant/tenant_catalog.h"
#include "tenant/tenant_directory.h"

namespace mooncake {
namespace metadata {

class MetadataCatalog {
   public:
    using TenantFactory =
        std::function<std::shared_ptr<TenantCatalog>(const TenantId&)>;

    // `factory` runs inside the atomic get-or-create, so a tenant is fully
    // initialized (its quota account bound) before its handle is published.
    explicit MetadataCatalog(TenantFactory factory)
        : factory_(std::move(factory)) {}

    // Atomic get-or-create: concurrent callers for the same tenant all observe
    // the one TenantCatalog the winner published.
    [[nodiscard]] std::shared_ptr<TenantCatalog> GetOrCreateTenant(
        const TenantId& tenant_id) {
        return tenants_.GetOrCreate(
            tenant_id, [this, tenant_id] { return factory_(tenant_id); });
    }

    // Null when the tenant is absent.
    [[nodiscard]] std::shared_ptr<TenantCatalog> Lookup(
        const TenantId& tenant_id) const {
        return tenants_.Lookup(tenant_id);
    }

    // Walks every tenant of one snapshot; a tenant created from the callback
    // joins the next walk rather than this one.
    template <typename Fn>
    void Visit(Fn&& fn) const {
        tenants_.Visit(std::forward<Fn>(fn));
    }

    // Drops the tenant from the registry. A handle a caller already holds stays
    // valid, so an in-flight operation keeps working on the tenant it resolved.
    void Remove(const TenantId& tenant_id) { tenants_.Remove(tenant_id); }

    // Rebuilds group membership and lease deadlines in every tenant, for the
    // snapshot and standby restore paths.
    void RebuildGroupState() {
        tenants_.Visit(
            [](const TenantId&, const std::shared_ptr<TenantCatalog>& tenant) {
                tenant->RebuildGroupState();
            });
    }

   private:
    TenantFactory factory_;
    TenantDirectory<std::shared_ptr<TenantCatalog>> tenants_;
};

}  // namespace metadata
}  // namespace mooncake
