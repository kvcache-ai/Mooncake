#pragma once

#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"
#include "ha/kv/ha_kv_backend.h"

namespace mooncake {

// Opens the OpLog KV backend selected by spec.type. etcd keeps the process-wide
// client. Redis opens a dedicated connection that is not shared with leader
// election.
// Redis currently uses a fixed one-replica policy for both detailed and legacy
// writes. Only a primary with exactly one replica is supported by this factory;
// missing/excess ACKs return unknown (ETCD_OPERATION_ERROR for legacy writes).
// HABackendSpec does not yet supply a policy, and membership is not validated.
tl::expected<std::shared_ptr<HaKvBackend>, ErrorCode> CreateHaKvBackend(
    const ha::HABackendSpec& spec);

}  // namespace mooncake
