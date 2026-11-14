#pragma once

#include <functional>
#include <string>

#include "types.h"

namespace mooncake {

class HaKvBackend;
class SnapshotObjectStore;
class SnapshotMaintenanceLease;

// Validates snapshots and publishes the reader-visible safe deletion floor
// under the maintenance lease. Physical deletion is handled asynchronously.
class BatchOpLogPruningCoordinator {
   public:
    BatchOpLogPruningCoordinator(HaKvBackend& backend,
                                 SnapshotObjectStore& object_store,
                                 std::string cluster_id,
                                 std::string snapshot_root);

    ErrorCode Run(const SnapshotMaintenanceLease& lease,
                  const std::function<bool()>& cancelled = {});

   private:
    HaKvBackend& backend_;
    SnapshotObjectStore& object_store_;
    std::string cluster_id_;
    std::string snapshot_root_;
};

}  // namespace mooncake
