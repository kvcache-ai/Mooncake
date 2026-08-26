#pragma once

#include <cstdint>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "local_ssd/persisted_state.h"
#include "types.h"

namespace mooncake {
class SegmentPool;
struct SerializationError;
}  // namespace mooncake

namespace mooncake::ha {

// Codec for the existing ma/an/ms/cs/ld store-resource snapshot payload.
// It composes catalog, driver resource, placement and LocalSSD views without
// making any of those runtime components depend on serialization.
class StoreResourceSnapshotCodec final {
   public:
    static tl::expected<std::vector<uint8_t>, SerializationError> Encode(
        const SegmentPool& segment_pool,
        const LocalSsdPersistedState& local_ssd_state);
    static tl::expected<LocalSsdPersistedState, SerializationError> Decode(
        SegmentPool& segment_pool, const std::vector<uint8_t>& data,
        bool account_capacity_metrics);
};

}  // namespace mooncake::ha
