#pragma once
#include <string>
#include "types.h"
namespace mooncake {
class HaKvBackend;
class SnapshotObjectStore;
class SnapshotMaintenanceLease;
class BatchOpLogSnapshotGc {
   public:
    BatchOpLogSnapshotGc(HaKvBackend&, SnapshotObjectStore&, std::string,
                         std::string);
    ErrorCode Run(const SnapshotMaintenanceLease&);

   private:
    HaKvBackend& backend_;
    SnapshotObjectStore& store_;
    std::string cluster_, root_;
};
}  // namespace mooncake
