#pragma once

#include <msgpack.hpp>
#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "serialize/serializer.h"

namespace mooncake::ha {

// Owns the snapshot representation of concrete physical allocators. Driver and
// SegmentPool lifecycle code remain unaware of serialization details.
class AllocatorSnapshotCodec final {
   public:
    static tl::expected<void, SerializationError> Encode(
        const OffsetBufferAllocatorSnapshot& snapshot, MsgpackPacker& packer);

    static tl::expected<OffsetBufferAllocatorSnapshot, SerializationError>
    Decode(const msgpack::object& object);
};

}  // namespace mooncake::ha
