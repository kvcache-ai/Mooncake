#pragma once

#include <memory>

#include "ha/ha_types.h"
#include "master_config.h"

namespace mooncake {

class MasterAdminServer;

namespace ha {

class LeaderCoordinator;
class StandbyController;

class MasterServiceSupervisor {
   public:
    explicit MasterServiceSupervisor(
        const MasterServiceSupervisorConfig& config);

    int Start();

   private:
    MasterServiceSupervisorConfig config_;
};

// Runs the production supervisor loop with injected HA components.
int RunSupervisorLoopForTesting(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config,
    MasterAdminServer& admin_server,
    std::unique_ptr<LeaderCoordinator> coordinator,
    std::unique_ptr<StandbyController> standby_controller);

}  // namespace ha
}  // namespace mooncake
