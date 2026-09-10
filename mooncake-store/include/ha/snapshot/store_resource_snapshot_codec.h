#pragma once

#include <cstdint>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "local_ssd/persisted_state.h"
#include "segment/snapshot.h"
#include "types.h"

namespace mooncake {
struct SerializationError;
}  // namespace mooncake

namespace mooncake::ha {

struct StoreResourceSnapshot {
    SegmentPoolSnapshot segment_pool;
    LocalSsdPersistedState local_ssd;
};

// Codec for the existing ma/an/ms/cs/ld store-resource snapshot payload.
// Runtime components remain unaware of serialization.
class StoreResourceSnapshotCodec final {
   public:
    // Reads only owned snapshot data, independent of the source pool's
    // lifetime. Preserves SegmentSerializer's wire format (OFFSET host memory,
    // not CXL).
    static tl::expected<std::vector<uint8_t>, SerializationError> Encode(
        const SegmentPoolSnapshot& snapshot,
        const LocalSsdPersistedState& local_ssd_state);

    // Decodes detached state without publishing resources or updating metrics.
    static tl::expected<StoreResourceSnapshot, SerializationError> Decode(
        const std::vector<uint8_t>& data);
};

}  // namespace mooncake::ha
