#pragma once

#include <functional>
#include <memory>
#include <optional>

#include "ha/standby_controller.h"
#include "ha/ha_types.h"

namespace mooncake {

class P2PHotStandbyService;

namespace ha {

/**
 * @brief P2P deployment-mode StandbyController. Wraps P2PHotStandbyService to
 *        follow the centralized leadership infrastructure (master_service_supervisor)
 *        while preserving the P2P oplog hot-standby + metadata-export promote
 *        semantics. On PromoteStandbyAndExport it returns a PromotionContext
 *        whose `p2p_promotion_data` holds a P2PPromotionData payload for the
 *        new primary's RestoreFromStandbyMetadata.
 *
 *        Snapshot bootstrap is currently disabled (snapshot_service_port=0);
 *        only oplog following is wired. Snapshot sources/port can be exposed
 *        in MasterServiceSupervisorConfig later.
 */
class P2PStandbyController final : public StandbyController {
   public:
    P2PStandbyController(const HABackendSpec& spec,
                         const MasterServiceSupervisorConfig& config);
    ~P2PStandbyController() override;

    P2PStandbyController(const P2PStandbyController&) = delete;
    P2PStandbyController& operator=(const P2PStandbyController&) = delete;

    ErrorCode StartStandby(
        const std::optional<MasterView>& observed_leader) override;
    void StopStandby() override;
    ErrorCode PromoteStandby() override;
    tl::expected<PromotionContext, ErrorCode> PromoteStandbyAndExport()
        override;
    void UpdateObservedLeader(const std::optional<MasterView>& observed_leader)
        override;
    MasterRuntimeState GetStandbyRuntimeState() const override;
    void SetStandbyRuntimeStateCallback(RuntimeStateCallback callback) override;

   private:
    void ReportRuntimeState();

    std::unique_ptr<P2PHotStandbyService> standby_;
    RuntimeStateCallback callback_;
    mutable std::mutex mutex_;
};

}  // namespace ha
}  // namespace mooncake
