#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"
#include "master_config.h"
#include "metadata_store.h"
#include "types.h"

namespace mooncake {
namespace ha {

/**
 * Context exported from standby at promotion time.
 * Contains everything needed to restore primary's state.
 */
struct PromotionContext {
    uint64_t applied_seq_id{0};
    std::vector<StandbyObjectEntry> objects;
    std::vector<StandbySegmentInfo> segments;
};

class StandbyController {
   public:
    using RuntimeStateCallback = std::function<void(MasterRuntimeState)>;

    virtual ~StandbyController() = default;

    virtual ErrorCode StartStandby(
        const std::optional<MasterView>& observed_leader) = 0;

    virtual void StopStandby() = 0;

    virtual ErrorCode PromoteStandby() = 0;

    /**
     * Promote standby and export complete context for new primary.
     * This replaces the old PromoteStandby() for HA scenarios.
     *
     * @return PromotionContext on success, error code on failure
     */
    virtual tl::expected<PromotionContext, ErrorCode>
    PromoteStandbyAndExport() = 0;

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
