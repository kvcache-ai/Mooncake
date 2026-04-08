// mooncake-store/include/oplog_change_notifier.h
#pragma once

#include <cstdint>
#include <functional>

#include "ha/oplog/oplog_manager.h"
#include "types.h"

namespace mooncake {

// Abstract interface for watching OpLog changes.
//
// Delivery semantics:
//   The notifier provides **at-most-once delivery**. Its internal cursor
//   tracks *fetch progress* (where to read from next), NOT consumer
//   processing progress. The cursor advances unconditionally after entries
//   are dispatched to EntryCallback, regardless of whether the callback
//   processed them successfully.
//
//   Consumers (e.g. OpLogApplier) must maintain their own cursor
//   (expected_sequence_id_) and handle gaps, duplicates, and late arrivals
//   independently.
//
// Implementations: EtcdOpLogChangeNotifier (push via Watch),
//                  PollingOpLogChangeNotifier (poll via ReadOpLogSince)
class OpLogChangeNotifier {
   public:
    virtual ~OpLogChangeNotifier() = default;

    using EntryCallback = std::function<void(const OpLogEntry& entry)>;
    using ErrorCallback = std::function<void(ErrorCode error)>;

    virtual ErrorCode Start(uint64_t start_sequence_id, EntryCallback on_entry,
                            ErrorCallback on_error) = 0;
    virtual void Stop() = 0;
    virtual bool IsHealthy() const = 0;
};

}  // namespace mooncake
