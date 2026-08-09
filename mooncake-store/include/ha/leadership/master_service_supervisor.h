#pragma once

#include "ha/ha_types.h"
#include "master_config.h"

namespace mooncake {
namespace ha {

WrappedMasterServiceConfig BuildServingMasterServiceConfig(
    const MasterServiceSupervisorConfig& config,
    const LeadershipSession& leadership_session);

class MasterServiceSupervisor {
   public:
    explicit MasterServiceSupervisor(
        const MasterServiceSupervisorConfig& config);

    int Start();

   private:
    MasterServiceSupervisorConfig config_;
};

}  // namespace ha
}  // namespace mooncake
