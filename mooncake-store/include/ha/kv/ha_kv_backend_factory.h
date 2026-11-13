#pragma once

#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"
#include "ha/kv/ha_kv_backend.h"

namespace mooncake {

// Opens the OpLog KV backend selected by spec.type. etcd keeps the process-wide
// client. Redis opens a dedicated connection that is not shared with leader
// election.
// Redis uses spec.ha_redis_expected_replicas_per_shard for detailed and legacy
// writes. Cluster mode validates membership; missing/excess ACKs remain
// unknown.
tl::expected<std::shared_ptr<HaKvBackend>, ErrorCode> CreateHaKvBackend(
    const ha::HABackendSpec& spec);

}  // namespace mooncake
