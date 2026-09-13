#pragma once
#include <string>
#include <string_view>
#include "types.h"
namespace mooncake {
class HaKvBackend;
class SnapshotObjectStore;
class SnapshotMaintenanceLease;
class BatchOpLogSnapshotGc {
   public:
    BatchOpLogSnapshotGc(HaKvBackend&, SnapshotObjectStore&, std::string,
                         std::string);
    ErrorCode Run(const SnapshotMaintenanceLease&, std::string_view published);

   private:
    HaKvBackend& backend_;
    SnapshotObjectStore& store_;
    std::string cluster_, root_;
};
}  // namespace mooncake
