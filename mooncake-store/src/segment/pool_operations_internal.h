#pragma once

#include <optional>

#include "pool_transaction_internal.h"

namespace mooncake {

struct SegmentPool::UnmountState {
    Segment segment;
    UUID client_id;
    std::optional<RegionUnmountTxn> transaction;
    bool released{false};
};

}  // namespace mooncake
