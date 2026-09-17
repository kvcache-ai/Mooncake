#pragma once

// TenantDirectory: tenant id -> a strong handle to that tenant's state.
//
// Every object operation resolves its tenant, while tenants are created and
// removed rarely, so the table is copy-on-write: a lookup is one atomic
// acquire load of an immutable snapshot plus one map find, and a write copies
// the snapshot, applies its change and publishes the copy. A reader that
// already loaded a snapshot keeps it alive, so a concurrent publish cannot
// invalidate an in-flight lookup.

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "tenant_id.h"

namespace mooncake {
namespace metadata {

// `Handle` behaves like std::shared_ptr: owning, default-constructible to
// "absent", safe to copy into a callback. The directory never touches the
// targets themselves.
template <class Handle>
class TenantDirectory {
   public:
    // The tenant's handle, or a null one when it is absent.
    [[nodiscard]] Handle Lookup(const TenantId& tenant_id) const {
        const auto frame = Snapshot();
        const auto it = frame->find(tenant_id);
        return it == frame->end() ? Handle{} : it->second;
    }

    void Upsert(const TenantId& tenant_id, Handle handle) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        auto next = std::make_shared<Frame>(*Snapshot());
        (*next)[tenant_id] = std::move(handle);
        Publish(std::move(next));
    }

    // Get-or-create. The lookup above covers the common case, and the write
    // lock is taken only on a miss: a racer that finds the tenant already
    // published there returns it without running `factory`, so exactly one
    // caller builds each tenant. `factory` runs under the write lock: it must
    // be short, and it must not re-enter this directory.
    template <typename Factory>
    Handle GetOrCreate(const TenantId& tenant_id, Factory&& factory) {
        if (auto handle = Lookup(tenant_id)) {
            return handle;
        }
        std::lock_guard<std::mutex> lock(write_mutex_);
        const auto frame = Snapshot();
        const auto it = frame->find(tenant_id);
        if (it != frame->end()) {
            return it->second;
        }
        Handle handle = factory();
        auto next = std::make_shared<Frame>(*frame);
        (*next)[tenant_id] = handle;
        Publish(std::move(next));
        return handle;
    }

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
    // may dereference its handle and take the target's own locks; the handles
    // are const, so it cannot replace an entry of an already-published frame. A
    // publish from `fn` (or from any other thread) becomes visible to the next
    // walk, never to this one.
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
    using Frame = std::unordered_map<TenantId, Handle, TenantIdHash>;

    // GCC 11's libstdc++ has no std::atomic<std::shared_ptr<T>>, so the
    // portable free functions operate on a plain shared_ptr.
    std::shared_ptr<Frame> Snapshot() const {
        return std::atomic_load_explicit(&snapshot_, std::memory_order_acquire);
    }

    void Publish(std::shared_ptr<Frame> frame) {
        std::atomic_store_explicit(&snapshot_, std::move(frame),
                                   std::memory_order_release);
    }

    std::shared_ptr<Frame> snapshot_{std::make_shared<Frame>()};
    // Serializes the rare creates and removes; a lookup never takes it.
    mutable std::mutex write_mutex_;
};

}  // namespace metadata
}  // namespace mooncake
