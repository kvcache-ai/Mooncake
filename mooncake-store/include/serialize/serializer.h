#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>
#include <msgpack.hpp>

namespace mooncake::offset_allocator {
class __Allocator;
class OffsetAllocator;
class OffsetAllocationHandle;
}  // namespace mooncake::offset_allocator

namespace mooncake {
class AllocatedBuffer;
class Replica;
class MasterService;
class BufferAllocatorBase;
class SegmentView;
enum class ErrorCode;
struct SerializationError;
class MountedSegment;
class OffsetBufferAllocator;

using MsgpackPacker = msgpack::packer<msgpack::sbuffer>;

// Generic serialization interface
template <typename T>
class Serializer;

// Serializer specialization for __Allocator
template <>
class Serializer<offset_allocator::__Allocator> {
   public:
    using PointerType = std::unique_ptr<offset_allocator::__Allocator>;

    // What a serialized layout describes: the byte size of its occupied regions
    // and how many allocations they hold. Node sizes are stored rounded up to
    // their bin, so used_bytes is an upper bound for the requested byte count
    // persisted next to the layout.
    struct LayoutUsage {
        uint64_t used_bytes;
        uint64_t used_nodes;
    };

    static tl::expected<void, SerializationError> serialize(
        const offset_allocator::__Allocator &allocator, MsgpackPacker &packer);

    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj);

    // Verifies the invariants a persisted layout must satisfy before it is
    // installed: in-range node indices, an exact used / free-region / spare
    // partition, bin and free-stack bookkeeping, node geometry that covers
    // [0, m_size) contiguously, and a matching m_freeStorage. Returns nullopt
    // for an inconsistent layout. `multiplier_bits` is the unit shift of the
    // layout and is only used to report the usage in bytes.
    static std::optional<LayoutUsage> ValidateLayout(
        const offset_allocator::__Allocator &allocator,
        uint64_t multiplier_bits);

   private:
    enum class NodeState : uint8_t { kUnclassified, kUsed, kFree, kSpare };

    // Each stage consumes the node classification from the previous stage.
    // Keep these as members to use the serializer's access to allocator state.
    static std::optional<uint32_t> ValidateBins(
        const offset_allocator::__Allocator &allocator,
        std::vector<NodeState> &state);
    static bool ValidateFreeStack(
        const offset_allocator::__Allocator &allocator,
        std::vector<NodeState> &state, uint32_t live_nodes);
    static bool ValidateNeighborChain(
        const offset_allocator::__Allocator &allocator,
        const std::vector<NodeState> &state, uint32_t live_nodes);
};

// Serializer specialization for OffsetAllocator
template <>
class Serializer<offset_allocator::OffsetAllocator> {
   public:
    using PointerType = std::shared_ptr<offset_allocator::OffsetAllocator>;

    static tl::expected<void, SerializationError> serialize(
        const offset_allocator::OffsetAllocator &allocator,
        MsgpackPacker &packer);

    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj);
};

// Serializer specialization for OffsetAllocationHandle
template <>
class Serializer<offset_allocator::OffsetAllocationHandle> {
   public:
    using PointerType =
        std::shared_ptr<offset_allocator::OffsetAllocationHandle>;

    static tl::expected<void, SerializationError> serialize(
        const offset_allocator::OffsetAllocationHandle &handle,
        MsgpackPacker &packer);

    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj,
        const std::shared_ptr<offset_allocator::OffsetAllocator> &allocator);
};

// Serializer specialization for AllocatedBuffer
template <>
class Serializer<AllocatedBuffer> {
   public:
    using PointerType = std::unique_ptr<AllocatedBuffer>;

    static tl::expected<void, SerializationError> serialize(
        const AllocatedBuffer &buffer, const SegmentView &segment_view,
        MsgpackPacker &packer);

    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj, const SegmentView &segment_view);
};

// Serializer specialization for Replica (interface declaration)
template <>
class Serializer<Replica> {
   public:
    using PointerType = std::shared_ptr<Replica>;

    static tl::expected<void, SerializationError> serialize(
        const Replica &replica, const SegmentView &segment_view,
        MsgpackPacker &packer);

    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj, const SegmentView &segment_view);
};

template <>
class Serializer<MountedSegment> {
   public:
    static tl::expected<void, SerializationError> serialize(
        const MountedSegment &mounted_segment, MsgpackPacker &packer);

    static tl::expected<MountedSegment, SerializationError> deserialize(
        const msgpack::object &obj);
};

// Serializer specialization for OffsetBufferAllocator (interface declaration)
template <>
class Serializer<OffsetBufferAllocator> {
   public:
    using PointerType = std::shared_ptr<OffsetBufferAllocator>;

    static tl::expected<void, SerializationError> serialize(
        const OffsetBufferAllocator &allocator, MsgpackPacker &packer);

    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj);
};

// Generic serialization helper class
class SerializationHelper {
   public:
    // Serialize uint32_t (little-endian)
    static void serializeUint32(uint32_t value, std::vector<uint8_t> &out) {
        out.push_back(static_cast<uint8_t>(value & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
    }

    // Deserialize uint32_t (little-endian)
    static uint32_t deserializeUint32(const uint8_t *data) {
        return static_cast<uint32_t>(data[0]) |
               (static_cast<uint32_t>(data[1]) << 8) |
               (static_cast<uint32_t>(data[2]) << 16) |
               (static_cast<uint32_t>(data[3]) << 24);
    }
};

}  // namespace mooncake
