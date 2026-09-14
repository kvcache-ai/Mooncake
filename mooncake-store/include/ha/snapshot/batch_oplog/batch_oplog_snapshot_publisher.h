#pragma once

#include <string>
#include <string_view>

#include "types.h"

namespace mooncake {

class HaKvBackend;
class SnapshotMaintenanceLease;

class BatchOpLogSnapshotPublisher {
   public:
    BatchOpLogSnapshotPublisher(HaKvBackend& backend, std::string cluster_id);

    ErrorCode Publish(const SnapshotMaintenanceLease& lease,
                      std::string_view descriptor_json);

   private:
    ErrorCode PublishImpl(std::string_view owner_token,
                          std::string_view descriptor_json,
                          const SnapshotMaintenanceLease& lease);

    HaKvBackend& backend_;
    std::string cluster_id_;
};

}  // namespace mooncake
