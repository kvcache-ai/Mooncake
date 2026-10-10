#pragma once

#include "common/heap_optional.h"
#include "object_runtime_state.h"

namespace mooncake {
namespace route {

// The per-object runtime state kept next to the object's metadata: the task
// in flight for the key, at most one of each kind. It lives and dies with the
// object, under the same lock as its metadata.
//
// Every object carries one, and almost every object carries no task, so each
// task lives on the heap and costs a pointer while absent.
struct ObjectState {
    // A primary write or a background task is in flight for this key.
    bool is_processing{false};
    HeapOptional<ReplicationTask> replication_task;
    HeapOptional<OffloadingTask> offloading_task;
    HeapOptional<PromotionTask> promotion_task;
};

}  // namespace route
}  // namespace mooncake
