#pragma once

// NamespaceTable: tenant id -> the namespace holding that tenant's objects, and
// the lifecycle rules MasterService resolves namespaces through. A namespace is
// created atomically, published fully initialized, and from then on stays
// where it is for as long as the service serves: a reference a caller resolved
// never dangles, so it needs no ownership of its own.
//
// Every object operation resolves its namespace while namespaces are created
// rarely, so the table is one map under a reader-writer lock: a lookup takes
// the lock shared and a create takes it exclusively. The default namespace,
// which every request resolves when multi-tenancy is off, is also kept in an
// atomic once created, so resolving it takes no lock and writes to no shared
// cache line. The table locks only its own map: each namespace synchronizes
// its own containers.
//
// The factory is bound once at construction, so every namespace of one table
// is built the same way and a caller only names the one it wants; it is where
// whatever a policy hangs on a namespace is resolved. It runs before the table
// lock is taken, so it may take other locks without nesting them under this
// one, and a namespace is never reachable before it is fully built. Racing
// creators of one id may each run it, and only one result is published, so it
// must have no effect that cannot be repeated, and it must not re-enter this
// table.

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "metadata/namespace.h"
#include "tenant_id.h"

namespace mooncake {
namespace metadata {

class NamespaceTable {
   public:
    // One tenant id in, one initialized namespace out.
    using Factory = std::function<std::unique_ptr<Namespace>(const TenantId&)>;

    explicit NamespaceTable(Factory factory) : factory_(std::move(factory)) {}

    // Null when the namespace is absent.
    [[nodiscard]] Namespace* Lookup(const TenantId& tenant_id) const {
        if (tenant_id.IsDefault()) {
            return default_.load(std::memory_order_acquire);
        }
        std::shared_lock<std::shared_mutex> lock(mutex_);
        const auto it = namespaces_.find(tenant_id);
        return it == namespaces_.end() ? nullptr : it->second.get();
    }

    // Atomic get-or-create: concurrent callers for the same id all observe the
    // one namespace the winner published; a loser's build is dropped. The
    // lookup covers the common case, so the factory runs and the exclusive
    // lock is taken on a miss only.
    [[nodiscard]] Namespace& GetOrCreate(const TenantId& tenant_id) {
        if (Namespace* ns = Lookup(tenant_id)) {
            return *ns;
        }
        auto candidate = factory_(tenant_id);
        std::unique_lock<std::shared_mutex> lock(mutex_);
        Namespace& ns =
            *namespaces_.try_emplace(tenant_id, std::move(candidate))
                 .first->second;
        if (tenant_id.IsDefault()) {
            default_.store(&ns, std::memory_order_release);
        }
        return ns;
    }

    // Walks the namespaces present when the walk starts. They are collected
    // under the shared lock and `fn(tenant_id, ns)` runs after it is released,
    // so `fn` may take the namespace's own locks and may create namespaces; one
    // created during the walk joins the next one.
    template <typename Fn>
    void Visit(Fn&& fn) const {
        std::vector<std::pair<const TenantId*, Namespace*>> namespaces;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            namespaces.reserve(namespaces_.size());
            for (const auto& [tenant_id, ns] : namespaces_) {
                namespaces.emplace_back(&tenant_id, ns.get());
            }
        }
        for (const auto& [tenant_id, ns] : namespaces) {
            fn(*tenant_id, *ns);
        }
    }

   private:
    const Factory factory_;
    // Shared by lookups and walks, exclusive for the rare creates.
    mutable std::shared_mutex mutex_;
    // A node map, so a namespace and its id stay put while others are created.
    std::unordered_map<TenantId, std::unique_ptr<Namespace>, TenantIdHash>
        namespaces_;
    // The default namespace once created: a lookup of it skips the lock.
    std::atomic<Namespace*> default_{nullptr};
};

}  // namespace metadata
}  // namespace mooncake
