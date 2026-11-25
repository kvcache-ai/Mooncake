#pragma once

// NamespacePolicy: the decisions the metadata core leaves to a policy layered
// on top of it. The core routes and locks the objects of each namespace; a
// policy decides whether a namespace admits a write, whether an object may
// grow, what an object's footprint costs and when a namespace must shed memory
// (tenant quotas are the policy it was built for), and keeps its own state.
//
// The core calls each method at a fixed point and under fixed locks, written
// on the method. One that runs under a key's lock may take only leaf locks: it
// must not reach the core again, take master_state_mutex_ or client_mutex_,
// or lock another key.

#include <cstdint>
#include <functional>
#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "metadata/object_route.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {

// What a policy hangs on a namespace when the core creates it. The core keeps
// it with the namespace and hands it back on every object-level call, so the
// policy needs no lookup of its own.
class PolicyAttachment {
   public:
    virtual ~PolicyAttachment() = default;
};

// A write admitted by NamespacePolicy::AdmitWrite, held by the core until the
// request that asked for it ends.
class AdmissionToken {
   public:
    virtual ~AdmissionToken() = default;
};

// What an object-level call sees: the namespace, the policy's attachment to
// it, and the key's write lock. `guard.owner_state().policy_words` are what the
// policy keeps on the key; they survive the objects published under the key
// and must be zero once the key holds none.
struct PolicyContext {
    const TenantId& tenant_id;
    PolicyAttachment* attachment;
    const route::WriteGuard& guard;
};

// What the core offers a policy, and what its background pass may ask of it,
// outside any object lock. The policy is handed one when it is built.
class StoreControl {
   public:
    virtual ~StoreControl() = default;

    // Runs `fn(tenant_id, route)` for every namespace. The route may be walked
    // with a cursor; while restoring, a write cursor may also reset each key's
    // policy words.
    virtual void VisitNamespaces(
        const std::function<void(const TenantId&, route::ObjectRoute&)>&
            fn) = 0;
    // Objects the namespace holds right now; zero when it does not exist.
    virtual size_t ObjectCount(const TenantId& tenant_id) const = 0;
    // The memory every namespace's share is carved from.
    virtual uint64_t AllocatableMemoryBytes() const = 0;

    struct EvictionResult {
        uint64_t freed_bytes{0};
        uint64_t evicted_objects{0};
    };
    // Evicts memory replicas of `tenant_id`'s objects, oldest lease first,
    // until `target_bytes` are freed or nothing more qualifies.
    virtual EvictionResult EvictNamespaceMemory(const TenantId& tenant_id,
                                                uint64_t target_bytes) = 0;
};

class NamespacePolicy {
   public:
    virtual ~NamespacePolicy() = default;

    // A namespace is being created; runs once per namespace before any of its
    // objects exists, outside every core lock. Racing creators of one
    // namespace may each call it, and only one result is kept.
    virtual std::unique_ptr<PolicyAttachment> OnNamespaceCreated(
        const TenantId& tenant_id) {
        (void)tenant_id;
        return nullptr;
    }

    // A request is about to write to `tenant_id`; runs before the core takes
    // any lock. The token is held until the request ends, so it may carry a
    // lock that orders the write against the policy's own control plane.
    virtual tl::expected<std::unique_ptr<AdmissionToken>, ErrorCode> AdmitWrite(
        const TenantId& tenant_id) {
        (void)tenant_id;
        return std::unique_ptr<AdmissionToken>{};
    }

    // The object under `ctx.guard` is about to take `bytes` more memory (a new
    // write, a replica copy, a promotion); runs under the key's write lock
    // before the core allocates. An error refuses the growth. Zero bytes asks
    // only whether the namespace admits writes.
    virtual tl::expected<void, ErrorCode> OnGrow(const PolicyContext& ctx,
                                                 uint64_t bytes) {
        (void)ctx;
        (void)bytes;
        return {};
    }

    // The object under `ctx.guard` is about to give up its replicas and be
    // replaced, under this same lock, by a new write of the key (an upsert
    // that reallocates). Runs while the old replicas are still attached.
    virtual void OnReplace(const PolicyContext& ctx) { (void)ctx; }

    // The object under `ctx.guard` is about to be torn down for good, not
    // replaced. Runs before the teardown.
    virtual void OnTearDown(const PolicyContext& ctx) { (void)ctx; }

    // A write lock on a key is being released, with the lock still held; the
    // key may hold an object or none. Cannot fail.
    virtual void OnWriteRelease(const PolicyContext& ctx) { (void)ctx; }

    // Metadata was restored (a standby taking over as primary) and no write
    // is in flight yet.
    virtual void OnRestored() {}

    // The allocatable memory changed (a segment mounted or went away); runs
    // outside every core lock.
    virtual void OnCapacityChanged() {}

    // One round of the eviction thread, outside every core lock.
    virtual void OnMaintenance() {}
};

}  // namespace mooncake
