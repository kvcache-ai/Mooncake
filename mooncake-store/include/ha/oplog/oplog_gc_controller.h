#pragma once

#include <memory>

#include "ha/ha_types.h"
#include "types.h"

namespace mooncake {
namespace ha {

class OpLogStore;
class StandbyProgressStore;

class OpLogGcController final {
   public:
    OpLogGcController(std::shared_ptr<OpLogStore> oplog_store,
                      std::shared_ptr<StandbyProgressStore> progress_store);

    ErrorCode CleanupAfterSnapshot(const SnapshotDescriptor& descriptor) const;

   private:
    std::shared_ptr<OpLogStore> oplog_store_;
    std::shared_ptr<StandbyProgressStore> progress_store_;
};

}  // namespace ha
}  // namespace mooncake
