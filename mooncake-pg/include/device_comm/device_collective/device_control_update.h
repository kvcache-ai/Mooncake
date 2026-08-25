#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_H

#include <cstddef>
#include <cstdint>

#include "device_comm/device_collective/device_collective_types.cuh"
#include "error_types.h"

namespace mooncake {

// Device control updates carry control-plane changes, such as a Ring Plan,
// protocol-state reset, or same-device active-ranks mirror copy, without
// launching CUDA work from a control or recovery thread.
//
// The host builds one complete update in ordinary memory and publishes it to a
// mapped single-slot mailbox using only CPU stores. A collective kernel claims
// and executes a Published update at startup. After a failure, the already
// parked CTA claims the corresponding Pinned update before it exits.
//
// This indirection avoids a recovery deadlock. A failed collective leaves its
// final CTA resident while it waits for the host recovery worker to advance
// ready_generation. If that host worker enqueues Plan resets or mirror copies
// through the CUDA runtime and synchronizes the update stream, those operations
// may be unable to complete before the parked invocation releases its device or
// stream-ordering resources. The host worker then waits for the CUDA update,
// while the CTA waits for the worker: neither can make progress.
//
// CPU publication followed by execution in the parked CTA breaks that cycle.
// The same mechanism makes ordinary GroupView updates visible at the next
// collective boundary without CUDA runtime calls on the update path.

// Builds one complete update in ordinary host-local storage. Construction does
// not touch the mapped slot.
class ControlUpdateBuilder {
   public:
    PGResult<void> copyBytes(void* destination, const void* source,
                             size_t size);
    PGResult<void> fillBytes(void* destination, uint8_t value, size_t count);
    PGResult<void> fillU64(uint64_t* destination, uint64_t value,
                           size_t count);

    [[nodiscard]] bool empty() const noexcept {
        return update_.operation_count == 0;
    }

    [[nodiscard]] const ControlUpdate& update() const noexcept {
        return update_;
    }

   private:
    PGResult<void> append(ControlUpdateOp operation);

    ControlUpdate update_;
};

// Briefly acquires the mapped slot in Writing, copies an already complete
// host-local update, then makes it visible as Published or Pinned with one
// release store. This is the only path that owns Writing.
void publishControlUpdate(DeviceControlUpdateSlot& slot,
                          const ControlUpdate& update,
                          bool pinned = false);

// Pin an already published update for a parked failed collective. Once pinned,
// ordinary collective startups leave it untouched; only the failure
// tail may apply that update.
PGResult<void> pinPublishedControlUpdate(DeviceControlUpdateSlot& slot);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_DEVICE_CONTROL_UPDATE_H
