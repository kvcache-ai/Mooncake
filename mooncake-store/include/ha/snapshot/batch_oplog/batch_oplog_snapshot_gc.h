#pragma once
#include <string>
#include <string_view>
#include <optional>
#include "types.h"
namespace mooncake {
class HaKvBackend;
class SnapshotObjectStore;
class SnapshotMaintenanceLease;
class BatchOpLogSnapshotGc {
   public:
    BatchOpLogSnapshotGc(HaKvBackend&, SnapshotObjectStore&, std::string,
                         std::string);
    ErrorCode Run(const SnapshotMaintenanceLease&, std::string_view published,
                  const std::optional<std::string>& expected_fallback);

   private:
    HaKvBackend& backend_;
    SnapshotObjectStore& store_;
    std::string cluster_, root_;
};
}  // namespace mooncake
