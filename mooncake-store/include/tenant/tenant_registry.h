#pragma once

// TenantRegistry: the registry MasterService resolves tenants through. It owns
// one TenantMetadata per tenant and keeps the lifecycle rules out of its
// callers: a tenant is created atomically, published fully initialized, and
// stays reachable, so an in-flight reader never finds its tenant reclaimed.
//
// The factory is bound once at construction, so every tenant of one registry is
// built the same way and a caller only names the tenant it wants. It runs under
// the write lock of the get-or-create, before the tenant is published, so a
// handle never escapes before the tenant it names is fully initialized.
//
// Alive is not current: `Remove` drops the tenant from the registry while a
// handle a caller already holds stays valid, and a later get-or-create for the
// same id builds a new tenant. A caller that must act on the reachable instance
// resolves it through `Lookup` rather than reusing an old handle.
//
// The registry adds no synchronization of its own: the directory publishes
// frames atomically and each tenant synchronizes its own containers.

#include <functional>
#include <memory>
#include <utility>

#include "tenant/tenant_directory.h"
#include "tenant/tenant_metadata.h"

namespace mooncake {
namespace metadata {

class TenantRegistry {
   public:
    // One tenant id in, one initialized tenant out.
    using TenantFactory =
        std::function<std::shared_ptr<TenantMetadata>(const TenantId&)>;

    explicit TenantRegistry(TenantFactory factory)
        : factory_(std::move(factory)) {}

    // Atomic get-or-create: concurrent callers for the same tenant all observe
    // the one TenantMetadata the winner published, and the factory runs for
    // that winner only.
    [[nodiscard]] std::shared_ptr<TenantMetadata> GetOrCreateTenant(
        const TenantId& tenant_id) {
        return tenants_.GetOrCreate(
            tenant_id, [this, &tenant_id] { return factory_(tenant_id); });
    }

    // Null when the tenant is absent.
    [[nodiscard]] std::shared_ptr<TenantMetadata> Lookup(
        const TenantId& tenant_id) const {
        return tenants_.Lookup(tenant_id);
    }

    // Walks every tenant of one snapshot; a tenant created from the callback
    // joins the next walk rather than this one. The handles are const, so the
    // walk itself cannot replace entries of the snapshot it is reading.
    template <typename Fn>
    void Visit(Fn&& fn) const {
        tenants_.Visit(std::forward<Fn>(fn));
    }

    // Drops the tenant from the registry. A handle a caller already holds stays
    // valid, so an in-flight operation keeps working on the tenant it resolved.
    void Remove(const TenantId& tenant_id) { tenants_.Remove(tenant_id); }

   private:
    const TenantFactory factory_;
    TenantDirectory<std::shared_ptr<TenantMetadata>> tenants_;
};

}  // namespace metadata
}  // namespace mooncake
