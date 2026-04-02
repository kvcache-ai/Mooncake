#pragma once

#include <functional>
#include <memory>
#include <optional>

#include "ha/ha_types.h"
#include "master_config.h"
#include "types.h"

namespace mooncake {
namespace ha {

class StandbyController {
   public:
    using RuntimeStateCallback = std::function<void(MasterRuntimeState)>;

    virtual ~StandbyController() = default;

    virtual ErrorCode StartStandby(
        const std::optional<MasterView>& observed_leader) = 0;

    virtual void StopStandby() = 0;

    virtual ErrorCode PromoteStandby() = 0;

    virtual void UpdateObservedLeader(
        const std::optional<MasterView>& observed_leader) = 0;

    virtual MasterRuntimeState GetStandbyRuntimeState() const = 0;

    virtual void SetStandbyRuntimeStateCallback(
        RuntimeStateCallback callback) = 0;
};

std::unique_ptr<StandbyController> CreateStandbyController(
    const HABackendSpec& spec, const MasterServiceSupervisorConfig& config);

}  // namespace ha
}  // namespace mooncake
