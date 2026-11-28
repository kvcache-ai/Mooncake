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
// - Async (--offload_on_evict_async): no per-run cap, since each owner's
//   room in the queue bounds the offloads in flight, and force_evict lets an
//   object go without its offload only while the caller says memory is under
//   pressure (SetPressure), never on a plain watermark run. An object kept
//   because its destination is full keeps its rank; one kept for another
//   reason is not tried again for kRetryBackoff (ObjectState::
//   offload_retry_after), unless the round may drop it. Once every
//   destination is full the run ends (Effect::exhausted), except under
//   pressure, when objects already on disk can still go.

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "eviction/reclaimer.h"
#include "replica.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace eviction {

// Where offloads are queued.
class OffloadQueue {
   public:
    struct Pushed {
        enum class Result : uint8_t {
            kQueued,
            // The destination already has as many offloads in flight as it
            // may.
            kOwnerFull,
            kRejected,
        };
        Result result = Result::kRejected;
        // Held by the offload's task, and given back when it goes; empty
        // unless the queue keeps a ledger (see OffloadLedger).
        std::shared_ptr<void> credit;

        [[nodiscard]] bool queued() const { return result == Result::kQueued; }
    };

    // Queues `source`, a memory replica of the object `key` names, for a copy
    // to its host's local disk. When it was queued, the destinations are
    // appended to `mirror_clients`.
    virtual Pushed Push(const TenantId& tenant_id, const std::string& key,
                        Replica& source, std::vector<UUID>* mirror_clients) = 0;
    // Whether every destination is full, so no push can succeed now.
    [[nodiscard]] virtual bool AllFull() const { return false; }

   protected:
    ~OffloadQueue() = default;
};

class OffloadingReclaimer final : public Reclaimer {
   public:
    // How long an object whose offload a destination refused (other than
    // for being full) waits before eviction asks again.
    static constexpr Clock::duration kRetryBackoff = std::chrono::seconds(1);

    struct Options {
        // How many offloads one run may queue; enforced with force_evict.
        long cap = 0;
        // Let an object go without its offload rather than keep it.
        bool force_evict = false;
        // Async offload-on-evict: no cap, and force_evict only under
        // pressure.
        bool async = false;
    };

    // `inner` and `queue` outlive the reclaimer.
    OffloadingReclaimer(Reclaimer& inner, OffloadQueue& queue, Options options)
        : inner_(inner), queue_(queue), options_(options) {}

    tl::expected<Effect, ErrorCode> Apply(metadata::Namespace& ns,
                                          const route::WriteGuard& hold,
                                          const Plan& plan,
                                          const Report& so_far) override;
    void TearDown(metadata::Namespace& ns,
                  const route::WriteGuard& hold) override {
        inner_.TearDown(ns, hold);
    }

    // Async: whether the runs from now on free memory a write is waiting for
    // (an allocation failed), the only time force_evict drops an object it
    // cannot queue. Set by the one thread that runs memory eviction, around
    // its run.
    void SetPressure(bool pressure) {
        pressure_.store(pressure, std::memory_order_relaxed);
    }
    // Async: whether the runs from now on may queue new offloads; without,
    // only objects already on disk (or with a copy in flight) give up
    // memory, as while eviction waits for the copies in flight to land.
    void SetQueueing(bool queueing) {
        queueing_.store(queueing, std::memory_order_relaxed);
    }

    // Sets pressure and queueing for one run and puts the defaults back
    // when it ends.
    class RunScope {
       public:
        RunScope(OffloadingReclaimer& reclaimer, bool pressure, bool queueing)
            : reclaimer_(reclaimer) {
            reclaimer_.SetPressure(pressure);
            reclaimer_.SetQueueing(queueing);
        }
        RunScope(const RunScope&) = delete;
        RunScope& operator=(const RunScope&) = delete;
        ~RunScope() {
            reclaimer_.SetPressure(false);
            reclaimer_.SetQueueing(true);
        }

       private:
        OffloadingReclaimer& reclaimer_;
    };

   private:
    // The plan carried out by the inner reclaimer, without an offload.
    tl::expected<Effect, ErrorCode> WithoutOffload(
        metadata::Namespace& ns, const route::WriteGuard& hold,
        const Plan& plan, const Report& so_far, DropReason reason);

    Reclaimer& inner_;
    OffloadQueue& queue_;
    const Options options_;
    std::atomic<bool> pressure_{false};
    std::atomic<bool> queueing_{true};
};

}  // namespace eviction
}  // namespace mooncake
