#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "ha/ha_types.h"

namespace mooncake {
namespace ha {

class SnapshotStore {
   public:
    virtual ~SnapshotStore() = default;

    virtual ErrorCode Publish(const SnapshotDescriptor& snapshot) = 0;

    virtual tl::expected<std::optional<SnapshotDescriptor>, ErrorCode>
    GetLatest() = 0;

    virtual tl::expected<std::vector<SnapshotDescriptor>, ErrorCode> List(
        size_t limit) = 0;

    virtual ErrorCode Delete(const SnapshotId& snapshot_id) = 0;
};

}  // namespace ha
}  // namespace mooncake
