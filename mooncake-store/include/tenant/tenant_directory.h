#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "tenant/tenant_id.h"

namespace mooncake {
namespace tenant {

// Read-optimized, copy-on-write registry mapping TenantId -> a strong handle to
// that tenant's store.
//
// WHY COW: tenants are read-many / write-few. Every object operation resolves
// its tenant on the hot read path, so a read must be lock-free: one atomic load
// of an immutable snapshot, then one unordered map find. Writes (tenant
// create/remove) are rare — they serialize on a private mutex, copy the
// snapshot, apply the delta, and publish a new snapshot atomically. In-flight
// readers hold a strong shared_ptr to the snapshot they loaded, so a concurrent
// publish never invalidates them.
//
// `Handle` is expected to behave like std::shared_ptr (owning, default-
// constructible to "absent", async-callback-safe). The directory never owns or
// mutates the targets themselves.
template <class Handle>
class TenantDirectory {
   public:
    using Frame = std::unordered_map<TenantId, Handle, TenantIdHash>;

    // Returns a null Handle when the tenant is absent. Lock-free on the read
    // path: one atomic load + one immutable map find.
    Handle Lookup(const TenantId& tenant_id) const {
        auto frame =
            std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
        const auto it = frame->find(tenant_id);
        return it == frame->end() ? Handle{} : it->second;
    }

    void Upsert(const TenantId& tenant_id, Handle handle) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        const auto frame =
            std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
        // Copy-on-write: never mutate a snapshot that concurrent readers may
        // still hold.
        auto next = std::make_shared<Frame>(*frame);
        (*next)[tenant_id] = std::move(handle);
        std::atomic_store_explicit(&snapshot_, std::move(next),
                                   std::memory_order_release);
    }

    void Remove(const TenantId& tenant_id) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        const auto frame =
            std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
        if (frame->find(tenant_id) == frame->end()) {
            return;
        }
        auto next = std::make_shared<Frame>(*frame);
        next->erase(tenant_id);
        std::atomic_store_explicit(&snapshot_, std::move(next),
                                   std::memory_order_release);
    }

    // Snapshot-consistent visit of every (tenant, handle). The visitor runs
    // while a strong snapshot is held, so a concurrent COW publish cannot
    // invalidate it; it must NOT call Upsert/Remove on this directory. Callers
    // dereference `handle` (a strong owning handle) and lock the target's own
    // mutex before mutating it.
    template <typename Fn>
    void Visit(Fn&& fn) const {
        auto frame =
            std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
        for (auto& kv : *frame) {
            std::forward<Fn>(fn)(kv.first, kv.second);
        }
    }

   private:
    // GCC 11's libstdc++ lacks the C++20 std::atomic<std::shared_ptr<T>>
    // specialization, so use the portable free functions on a plain
    // std::shared_ptr. The read path stays lock-free.
    std::shared_ptr<Frame> snapshot_{std::make_shared<Frame>()};
    // Serializes the rare create/remove writes; never taken on the read path.
    mutable std::mutex write_mutex_;
};

}  // namespace tenant
}  // namespace mooncake
