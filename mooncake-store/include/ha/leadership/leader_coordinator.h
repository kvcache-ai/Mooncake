#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

class LeadershipMonitorHandle {
   public:
    virtual ~LeadershipMonitorHandle() = default;
    virtual void Stop() = 0;
};

class LeaderCoordinator {
   public:
    virtual ~LeaderCoordinator() = default;

    virtual tl::expected<std::optional<MasterView>, ErrorCode>
    ReadCurrentView() = 0;

    virtual tl::expected<AcquireLeadershipResult, ErrorCode>
    TryAcquireLeadership(const std::string& leader_address) = 0;

    virtual tl::expected<bool, ErrorCode> RenewLeadership(
        const LeadershipSession& session) = 0;

    virtual tl::expected<ViewChangeResult, ErrorCode> WaitForViewChange(
        std::optional<ViewVersionId> known_version,
        std::chrono::milliseconds timeout) = 0;

    virtual tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
    StartLeadershipMonitor(const LeadershipSession& session,
                           LeadershipLostCallback on_leadership_lost) = 0;

    virtual ErrorCode ReleaseLeadership(const LeadershipSession& session) = 0;
};

}  // namespace ha
}  // namespace mooncake
