// mooncake-store/include/oplog_change_notifier.h
#pragma once

#include <cstdint>
#include <functional>

#include "oplog_manager.h"
#include "types.h"

namespace mooncake {

// Abstract interface for watching OpLog changes.
// Implementations: EtcdOpLogChangeNotifier (push via Watch),
//                  (future) PollingOpLogChangeNotifier (poll via
//                  ReadOpLogSince)
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
