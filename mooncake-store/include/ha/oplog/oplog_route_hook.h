#pragma once

// OpLogRouteHook: the OpLog as one more hook on a namespace's write chain,
// registered only where the OpLog is enabled. It is what RFC #4955 asks the
// write paths to become: the hook reserves under the key's lock in begin, so
// the order the lock enforces is the order the records take, and commits at
// release from the state the write actually left.
//
// The reservation lives in the guard, which owns it: a write that is refused
// in begin never changed anything, and a guard released without a commit
// hands its reservation back unused. What remains for the commit to refuse is
// a writer that stopped accepting or turned terminal between the reserve and
// the commit; the hook logs that and moves on, since a release cannot fail.

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <glog/logging.h>

#include "ha/oplog/ordered_oplog_writer.h"
#include "metadata/namespace.h"
#include "types.h"

namespace mooncake {

class OpLogRouteHook : public metadata::WriteHook {
   public:
    // Serializes the state a committed write left, under the key's lock.
    // Returning nullopt records a REMOVE: the key is left with nothing worth
    // reading. A string records a PUT_END carrying it.
    using Serializer =
        std::function<std::optional<std::string>(const metadata::WriteGuard&)>;

    // `writer` must outlive every write the namespace this hook is registered
    // on serves; `tenant_id` is the namespace's own, so the entries name it
    // without a lookup under the key's lock.
    OpLogRouteHook(const TenantId& tenant_id, OrderedOpLogWriter* writer,
                   Serializer serializer)
        : tenant_id_(tenant_id.value()),
          writer_(writer),
          serializer_(std::move(serializer)) {}

    // Validates what the record will name and holds a slot for it, before the
    // write changes anything. A key too long for the OpLog is refused here,
    // which the master itself does not check, along with an invalid tenant or
    // an unknown operation; backpressure and a stopped writer are refused the
    // same way, so the write fails before it began.
    [[nodiscard]] tl::expected<void, ErrorCode> OnWriteBegin(
        const metadata::WriteGuard& guard, metadata::WriteChange change,
        std::unique_ptr<metadata::WriteReservation>& reservation) override {
        OpLogEntry entry;
        entry.op_type = change == metadata::WriteChange::kKeyRemoved
                            ? OpType::REMOVE
                            : OpType::PUT_END;
        entry.tenant_id = tenant_id_;
        entry.object_key = guard.key();
        auto reserved = writer_->ReserveFor(entry);
        if (!reserved) {
            return tl::make_unexpected(reserved.error());
        }
        reservation =
            std::make_unique<Reservation>(writer_, std::move(*reserved));
        return {};
    }

    // Records what the write left, into the slot its begin reserved. Only a
    // committed write gets here, and the reservation it held is this hook's
    // own, so the entry is committed in the order the key's lock allowed the
    // writes to begin.
    void OnWriteRelease(
        const metadata::WriteGuard& guard, metadata::WriteChange change,
        std::unique_ptr<metadata::WriteReservation>& reservation) override {
        auto* held = static_cast<Reservation*>(reservation.get());
        if (held == nullptr || held->writer_ != writer_) {
            return;
        }
        // The kind of change decides the record, not the post-state: a
        // removal commits as REMOVE while its replicas are still there, which
        // is what lets a deferred removal keep them until the record is
        // durable. The serializer never runs for one.
        std::optional<std::string> payload;
        if (change != metadata::WriteChange::kKeyRemoved) {
            payload = serializer_(guard);
        }
        OpLogEntry entry;
        entry.op_type = change == metadata::WriteChange::kKeyRemoved
                            ? OpType::REMOVE
                            : OpType::PUT_END;
        entry.tenant_id = tenant_id_;
        entry.object_key = guard.key();
        entry.payload = std::move(payload).value_or(std::string{});
        auto committed = writer_->Commit(std::move(held->reservation_),
                                         std::move(entry), nullptr);
        if (!committed) {
            LOG(WARNING) << "OpLog hook: commit failed for key=" << guard.key()
                         << ", err=" << static_cast<int>(committed.error());
        }
    }

   private:
    // The reservation the guard carries for this hook. Given back unused when
    // the guard dies without a commit, which aborts the slot.
    struct Reservation final : metadata::WriteReservation {
        Reservation(OrderedOpLogWriter* writer,
                    OrderedOpLogWriter::Reservation&& reservation)
            : writer_(writer), reservation_(std::move(reservation)) {}
        ~Reservation() override = default;

        OrderedOpLogWriter* writer_;
        OrderedOpLogWriter::Reservation reservation_;
    };

    const std::string tenant_id_;
    OrderedOpLogWriter* const writer_;
    const Serializer serializer_;
};

}  // namespace mooncake
