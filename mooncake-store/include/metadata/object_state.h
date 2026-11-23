#pragma once

#include <chrono>
#include <cstdint>

#include "common/heap_optional.h"
#include "object_runtime_state.h"

namespace mooncake {
namespace route {

// The per-object runtime state kept next to the object's metadata: primary
// write and replication tasks in flight for the key, plus dynamic replication
// pending/cooldown state. Offload and promotion tasks/candidates are owned
// inside OffloadManager and PromotionManager under their own sharded locks.
//
// Every object carries one, and almost every object carries no task, so each
// task lives on the heap and costs a pointer while absent.
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
    HeapOptional<ReplicationTask> replication_task;
    // The dynamic replica proposal admitted for this key and not yet started
    // by its CopyStart, and how long the key waits before the next one. Owned
    // by DynamicReplicationController; neither is work in flight.
    HeapOptional<DynamicReplicaPending> dynamic_replication_pending;
    std::chrono::steady_clock::time_point dynamic_replication_cooldown{};

    bool HasWorkInFlight() const { return is_processing || replication_task; }
};

}  // namespace route
}  // namespace mooncake
