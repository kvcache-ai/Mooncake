#pragma once

// TenantRegistry: tenant id -> a strong handle to that tenant's metadata, and
// the lifecycle rules MasterService resolves tenants through. A tenant is
// created atomically, published fully initialized, and stays reachable, so an
// in-flight reader never finds its tenant reclaimed.
//
// Every object operation resolves its tenant while tenants are created and
// removed rarely, so the table is copy-on-write: a lookup is one atomic acquire
// load of an immutable frame plus one map find, and a write copies the frame,
// applies its change and publishes the copy. A reader that already loaded a
// frame keeps it alive through the handle it holds, so a concurrent publish
// cannot invalidate an in-flight lookup. The registry adds no locking beyond
// the write lock of the creates and removes: each tenant synchronizes its own
// containers.
//
// The factory is bound once at construction, so every tenant of one registry is
// built the same way and a caller only names the tenant it wants. It runs under
// the write lock of the get-or-create, before the tenant is published, so a
// handle never escapes before the tenant it names is fully initialized. It must
// be short, and it must not re-enter this registry.
//
// Alive is not current: `Remove` drops the tenant from the registry while a
// handle a caller already holds stays valid, and a later get-or-create for the
// same id builds a new tenant. A caller that must act on the reachable instance
// resolves it through `Lookup` rather than reusing an old handle.

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

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
        const auto frame = Snapshot();
        const auto it = frame->find(tenant_id);
        return it == frame->end() ? nullptr : it->second;
    }

    // Atomic get-or-create: concurrent callers for the same tenant all observe
    // the one Tenant the winner published, and the factory runs for that winner
    // only. The lookup above covers the common case, so the write lock is taken
    // on a miss only.
    [[nodiscard]] std::shared_ptr<Tenant> GetOrCreateTenant(
        const TenantId& tenant_id) {
        if (auto tenant = Lookup(tenant_id)) {
            return tenant;
        }
        std::lock_guard<std::mutex> lock(write_mutex_);
        const auto frame = Snapshot();
        const auto it = frame->find(tenant_id);
        if (it != frame->end()) {
            return it->second;
        }
        auto tenant = factory_(tenant_id);
        auto next = std::make_shared<Frame>(*frame);
        (*next)[tenant_id] = tenant;
        Publish(std::move(next));
        return tenant;
    }

    // Drops the tenant from the registry. A handle a caller already holds stays
    // valid, so an in-flight operation keeps working on the tenant it resolved.
    void Remove(const TenantId& tenant_id) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        const auto frame = Snapshot();
        if (frame->find(tenant_id) == frame->end()) {
            return;
        }
        auto next = std::make_shared<Frame>(*frame);
        next->erase(tenant_id);
        Publish(std::move(next));
    }

    // Walks every tenant of one frame, with that frame held for the walk. `fn`
    // may dereference its handle and take the tenant's own locks; the handles
    // are const, so it cannot replace an entry of an already-published frame. A
    // publish from `fn` (or from any other thread) becomes visible to the next
    // walk, never to this one, so a tenant created from the callback joins the
    // next walk rather than this one.
    template <typename Fn>
    void Visit(Fn&& fn) const {
        const auto frame = Snapshot();
        for (const auto& entry : *frame) {
            fn(entry.first, entry.second);
        }
    }

   private:
    // One immutable mapping, handed out whole: a reader keeps the frame it
    // loaded, a writer copies it and publishes the copy.
    using Frame =
        std::unordered_map<TenantId, std::shared_ptr<Tenant>, TenantIdHash>;

    // GCC 11's libstdc++ has no std::atomic<std::shared_ptr<T>>, so the
    // portable free functions operate on a plain shared_ptr.
    std::shared_ptr<Frame> Snapshot() const {
        return std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
    }

    void Publish(std::shared_ptr<Frame> frame) {
        std::atomic_store_explicit(&snapshot_, std::move(frame),
                                   std::memory_order_release);
    }

    const TenantFactory factory_;
    std::shared_ptr<Frame> snapshot_{std::make_shared<Frame>()};
    // Serializes the rare creates and removes; a lookup never takes it.
    mutable std::mutex write_mutex_;
};

}  // namespace metadata
}  // namespace mooncake
