#pragma once

// OffloadingReclaimer: a Reclaimer that keeps an object's data on local disk
// before its memory goes. Layered over the reclaimer that does the releasing:
//
// - An object that already has a completed local-disk replica loses nothing,
//   so its memory goes through the inner reclaimer as it is. One being
//   removed does not count: once its removal is durable it is gone.
// - An object with an offload already in flight keeps its data through the
//   replica that offload pinned, which no plan names while pinned, so its
//   memory goes through the inner reclaimer too. Another offload would pin a
//   replica nothing unpins: an object tracks one offload at a time.
// - Otherwise one memory replica the plan names is pinned and queued for
//   offload, and the rest are released now: the data survives through the
//   pinned one, which a later run frees once the copy lands.
// - When no offload can be queued, the object waits for a later run, unless
//   force_evict lets it go without one. With force_evict, a run that has
//   queued `cap` offloads lets the rest go without one too.

#include <atomic>
#include <cstdint>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "eviction/reclaimer.h"
#include "replica.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace eviction {

// Where offloads are tracked and queued.
class OffloadQueue {
   public:
    // True when `(tenant_id, key)` already has an offload task in flight.
    virtual bool HasInFlight(const TenantId& tenant_id,
                             const std::string& key) const = 0;

    // Queues `source`, a memory replica of the object `key` names, for a copy
    // to its host's local disk, pins `source` via ReplicaPin before making the
    // task visible, and records the active offload task. True when the offload
    // task was started (leaving `source` pinned until completion/cancellation).
    virtual bool StartOffload(const TenantId& tenant_id, const std::string& key,
                              Replica& source) = 0;

   protected:
    ~OffloadQueue() = default;
};

class OffloadingReclaimer final : public Reclaimer {
   public:
    // Stop scanning in non-forced mode after this many consecutive offload
    // rejections without any reclaimed or queued object, preventing an O(N)
    // full-table scan across saturated queues.
    static constexpr uint64_t kMaxConsecutiveRejections = 256;

    struct Options {
        // How many offloads one run may queue (0 = unlimited when
        // force_evict=false; 0 = drop immediately when force_evict=true).
        long cap = 0;
        // Let an object go without its offload rather than keep it.
        bool force_evict = false;
    };

    // `inner` and `queue` outlive the reclaimer.
    OffloadingReclaimer(Reclaimer& inner, OffloadQueue& queue, Options options)
        : inner_(inner), queue_(queue), options_(options) {}

    bool SupportsParallel() const override { return inner_.SupportsParallel(); }

    void OnRunBegin() override {
        run_offloads_.store(0, std::memory_order_relaxed);
        consecutive_rejections_.store(0, std::memory_order_relaxed);
    }

    tl::expected<Effect, ErrorCode> Apply(metadata::Namespace& ns,
                                          const route::WriteGuard& hold,
                                          const Plan& plan,
                                          const Report& so_far) override;
    void TearDown(metadata::Namespace& ns,
                  const route::WriteGuard& hold) override {
        inner_.TearDown(ns, hold);
    }

   private:
    // Atomically reserves an offload slot up to `cap`. Returns the 1-based
    // slot number if reserved, or 0 if the cap is already reached.
    uint64_t TryReserveOffloadSlot(const Report& so_far);

    // The plan carried out by the inner reclaimer, without an offload.
    tl::expected<Effect, ErrorCode> WithoutOffload(
        metadata::Namespace& ns, const route::WriteGuard& hold,
        const Plan& plan, const Report& so_far, DropReason reason);

    Reclaimer& inner_;
    OffloadQueue& queue_;
    const Options options_;
    std::atomic<uint64_t> run_offloads_{0};
    std::atomic<uint64_t> consecutive_rejections_{0};
};

}  // namespace eviction
}  // namespace mooncake
