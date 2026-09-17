#pragma once

#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "master/ha/ha_types.h"
#include "master/ha/leadership/leader_coordinator.h"

namespace mooncake {
namespace ha {

tl::expected<std::unique_ptr<LeaderCoordinator>, ErrorCode>
CreateLeaderCoordinator(const HABackendSpec& spec);

}  // namespace ha
}  // namespace mooncake
