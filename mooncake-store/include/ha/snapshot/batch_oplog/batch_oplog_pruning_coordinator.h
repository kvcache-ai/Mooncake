#pragma once

#include <functional>
#include <string>

#include "types.h"

namespace mooncake {

class HaKvBackend;
class SnapshotObjectStore;
class SnapshotMaintenanceLease;

// Runs only after snapshot publication, under the same maintenance lease.
// The reader-visible floor always precedes batch deletion. Errors retain data
// and must not change the outcome of the already successful publication.
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
