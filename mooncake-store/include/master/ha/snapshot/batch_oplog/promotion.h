#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "master/ha/oplog/oplog_batch_storage.h"
#include "master/ha/standby_metadata_store.h"
#include "master/metadata_store.h"
#include "common/types.h"

namespace mooncake {

inline constexpr size_t kDefaultBatchOpLogPromotionChunkObjects = 1'000'000;

struct BatchOpLogPromotionHandoff {
    std::unique_ptr<StandbyMetadataStore> metadata_store;
    std::vector<StandbySegmentInfo> segments;
    DurablePrefix applied_cursor;
    ViewVersionId producer_view_version{0};
    ReplicaID max_replica_id{0};
};

}  // namespace mooncake
