#pragma once
#include <string>
#include <string_view>
#include <optional>
#include <functional>
#include "common/types.h"
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
    ErrorCode Run(const SnapshotMaintenanceLease&, std::string_view,
                  const std::optional<std::string>&,
                  const std::function<bool()>& cancelled);

   private:
    HaKvBackend& backend_;
    SnapshotObjectStore& store_;
    std::string cluster_, root_;
};
}  // namespace mooncake
