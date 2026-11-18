#pragma once

// WriteHook: what a component hanging on a namespace hears about a write, and
// when. A write lock on a key is reported twice:
//
//   OnWriteBegin    under the key's lock, before the first change, and may
//                   fail. Nothing has changed yet, so a failure leaves the
//                   object as it was and the write does not happen.
//   OnWriteRelease  under the key's lock, with the lock still held, and cannot
//                   fail. It sees the post-state the write produced, and runs
//                   before the owner settles the key's accounting and indexes,
//                   so a hook that finishes the write is accounted for.
//
// A hook hears of a write only when the write changed the object: a guard
// starts out cancelled, Publish and TearDown commit it, and a caller that
// changes metadata or state itself calls Commit. A guard released without a
// commit is a write that did not happen, and nothing is recorded for it.
//
// The order is why a hook that must record a change before it becomes visible
// reserves in begin: a reservation taken before the key's lock could put two
// writes of one key in the wrong order, while one taken under it keeps the
// order the lock enforces.
//
// A hook runs under the key's lock, so it may take only leaf locks: it must
// not reach the route again, take master_state_mutex_ or client_mutex_, lock
// another key, or block on I/O.

#include <memory>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "metadata/lock_mode.h"
#include "types.h"

namespace mooncake {

namespace route {
template <LockMode kMode>
class Guard;
}  // namespace route

namespace metadata {

// The key's write lock, as the route hands it out.
using WriteGuard = route::Guard<LockMode::kWrite>;

// The kind of change a write guard makes to the object under its key. It is
// decided when the guard is taken and settled when the guard is released, so a
// hook picks what to record from the guard itself rather than from what it
// observes.
enum class WriteChange {
    // The guard changes neither the publication nor the set of replicas: it
    // reads state, takes a cursor step, or refreshes metadata in place.
    kNone,
    // The key will hold a new publication, or the held one with replicas
    // added or completed.
    kNewWrite,
    // Replicas of the held object will be marked removed, and the object
    // itself may be torn down if none is left.
    kReplicasRemoved,
    // The held object will be torn down.
    kKeyRemoved,
};

// What a hook holds for one write, from begin to release. The guard owns it: a
// guard released without a commit drops it, which gives back whatever begin
// reserved.
class WriteReservation {
   public:
    virtual ~WriteReservation() = default;
};

// The hooks one namespace chains. They run in the order they were registered,
// each with the guard and its own reservation.
class WriteHook {
   public:
    virtual ~WriteHook() = default;

    // Runs under the key's lock before the first change. Fills `reservation`
    // when it must keep something until the release; a returned error stops
    // the write before anything changed.
    [[nodiscard]] virtual tl::expected<void, ErrorCode> OnWriteBegin(
        const WriteGuard& guard, WriteChange change,
        std::unique_ptr<WriteReservation>& reservation) = 0;

    // Runs under the key's lock with the lock still held, and cannot fail.
    // `reservation` is what this hook's OnWriteBegin filled, if it ran.
    virtual void OnWriteRelease(
        const WriteGuard& guard, WriteChange change,
        std::unique_ptr<WriteReservation>& reservation) = 0;
};

// One reservation per hook of a namespace, in registration order.
using WriteReservations = std::vector<std::unique_ptr<WriteReservation>>;

}  // namespace metadata
}  // namespace mooncake
