#pragma once

#include <string>
#include <string_view>
#include <optional>

#include "types.h"

namespace mooncake {

class HaKvBackend;
class SnapshotMaintenanceLease;

class BatchOpLogSnapshotPublisher {
   public:
    BatchOpLogSnapshotPublisher(HaKvBackend& backend, std::string cluster_id);

    ErrorCode Publish(const SnapshotMaintenanceLease& lease,
                      std::string_view descriptor_json,
                      std::optional<std::string>* expected_fallback = nullptr);

   private:
    ErrorCode PublishImpl(std::string_view owner_token,
                          std::string_view descriptor_json,
                          const SnapshotMaintenanceLease& lease,
                          std::optional<std::string>* expected_fallback);

    HaKvBackend& backend_;
    std::string cluster_id_;
};

}  // namespace mooncake
