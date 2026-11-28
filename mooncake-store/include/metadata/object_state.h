#pragma once

#include <chrono>
#include <cstdint>
#include <limits>

#include "common/heap_optional.h"
#include "object_runtime_state.h"

namespace mooncake {
namespace route {

// The per-object runtime state kept next to the object's metadata: the task
// in flight for the key, at most one of each kind, and the promotion retry
// candidate. It lives and dies with the object, under the same lock as its
// metadata.
//
// Every object carries one, and almost every object carries neither a task nor
// a candidate, so each lives on the heap and costs a pointer while absent.
struct ObjectState {
    // A primary write or a background task is in flight for this key.
    bool is_processing{false};
    // KV event state fits in the padding after `is_processing`, so it adds
    // zero bytes to KeySlot. Announcements may outlive availability while a
    // removed replica waits for durable finalization.
    uint8_t announced_kv_media{0};
    uint8_t live_kv_media{0};
    // An in-place overwrite invalidates the old contents even if a pending
    // REMOVED replica still retains their tier. Consumed on write release.
    bool kv_content_invalidated{false};
    // Async offload-on-evict: eviction does not try to queue this object's
    // offload again before this second of the system clock (0: no wait), set
    // when its copy failed or could not be queued for a reason other than a
    // full destination. Unlike postponing the key in the eviction index, it
    // leaves the key's rank alone, and a round that may drop (force_evict
    // under pressure) ignores it. Fits in the padding after the fields above.
    uint32_t offload_retry_after{0};
    HeapOptional<ReplicationTask> replication_task;
    HeapOptional<OffloadingTask> offloading_task;
    HeapOptional<PromotionTask> promotion_task;
    // The dynamic replica proposal admitted for this key and not yet started
    // by its CopyStart, and how long the key waits before the next one. Owned
    // by DynamicReplicationController; neither is work in flight.
    HeapOptional<DynamicReplicaPending> dynamic_replication_pending;
    std::chrono::steady_clock::time_point dynamic_replication_cooldown{};
    // A promotion a transient gate turned away, waiting for a retry. It is
    // not work in flight: no task holds anything for it.
    HeapOptional<PromotionCandidate> promotion_candidate;

    bool HasWorkInFlight() const {
        return is_processing || replication_task || offloading_task ||
               promotion_task;
    }

    // Whether eviction may try to queue this object's offload at `now`.
    bool OffloadRetryDue(std::chrono::system_clock::time_point now) const {
        return offload_retry_after == 0 ||
               SecondsOf(now) >= offload_retry_after;
    }
    // Holds eviction's next offload attempt back until at least `until`.
    void DelayOffloadRetry(std::chrono::system_clock::time_point until) {
        offload_retry_after = SecondsOf(until) + 1;
    }

   private:
    static uint32_t SecondsOf(std::chrono::system_clock::time_point at) {
        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(
                                 at.time_since_epoch())
                                 .count();
        return seconds <= 0 ? 0
               : seconds >= std::numeric_limits<uint32_t>::max() - 1
                   ? std::numeric_limits<uint32_t>::max() - 1
                   : static_cast<uint32_t>(seconds);
    }
};

}  // namespace route
}  // namespace mooncake
