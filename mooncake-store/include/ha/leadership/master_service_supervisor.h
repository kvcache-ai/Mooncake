#pragma once

#include "master_config.h"

namespace mooncake {
namespace ha {

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
