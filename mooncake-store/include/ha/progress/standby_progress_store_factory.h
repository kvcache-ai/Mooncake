#pragma once

#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "ha/progress/standby_progress_store.h"

namespace mooncake {
namespace ha {

tl::expected<std::shared_ptr<StandbyProgressStore>, ErrorCode>
CreateStandbyProgressStore(const HABackendSpec& spec);

}  // namespace ha
}  // namespace mooncake
