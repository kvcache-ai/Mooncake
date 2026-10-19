#pragma once

#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"
#include "ha/kv/ha_kv_backend.h"

namespace mooncake {

// Opens the OpLog KV backend selected by spec.type. etcd keeps the process-wide
// client. Redis opens a dedicated connection that is not shared with leader
// election.
tl::expected<std::shared_ptr<HaKvBackend>, ErrorCode> CreateHaKvBackend(
    const ha::HABackendSpec& spec);

}  // namespace mooncake
