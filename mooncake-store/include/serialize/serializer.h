#pragma once

#include <cstdint>
#include <vector>

#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>
#include <msgpack.hpp>

#include "segment/pool.h"

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
enum class ErrorCode;
struct SerializationError;

using MsgpackPacker = msgpack::packer<msgpack::sbuffer>;

// Generic serialization interface
template <typename T>
class Serializer;

// Serializer specialization for __Allocator
template <>
class Serializer<offset_allocator::__Allocator> {
   public:
    using PointerType = std::unique_ptr<offset_allocator::__Allocator>;

    // Layout validation lives with the snapshot so every restore path shares
    // one checker (OffsetAllocatorSnapshot::Validate); this codec only bounds
    // the scalar capacities before decompressing node payloads.
    static tl::expected<void, SerializationError> serialize(
        const offset_allocator::__Allocator &allocator, MsgpackPacker &packer);

    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj);
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

// Encoding with SegmentPool requires a forked child or external quiescence;
// the ReadAccess overload is for callers already holding a runtime read lock.
template <>
class Serializer<AllocatedBuffer> {
   public:
    using PointerType = std::unique_ptr<AllocatedBuffer>;

    static tl::expected<void, SerializationError> serialize(
        const AllocatedBuffer& buffer, const SegmentPool& segment_pool,
        MsgpackPacker& packer);
    static tl::expected<void, SerializationError> serialize(
        const AllocatedBuffer& buffer,
        const SegmentPool::ReadAccess& segment_view, MsgpackPacker& packer);
    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object& obj,
        const SegmentPool::ReadAccess& segment_view);

   private:
    static tl::expected<void, SerializationError> SerializeWithRegionId(
        const AllocatedBuffer& buffer, const UUID& region_id,
        MsgpackPacker& packer);
};

template <>
class Serializer<Replica> {
   public:
    using PointerType = std::shared_ptr<Replica>;

    static tl::expected<void, SerializationError> serialize(
        const Replica& replica, const SegmentPool& segment_pool,
        MsgpackPacker& packer);
    static tl::expected<void, SerializationError> serialize(
        const Replica& replica, const SegmentPool::ReadAccess& segment_view,
        MsgpackPacker& packer);
    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object& obj,
        const SegmentPool::ReadAccess& segment_view);

   private:
    template <typename SegmentAccess>
    static tl::expected<void, SerializationError> SerializeImpl(
        const Replica& replica, const SegmentAccess& segment_access,
        MsgpackPacker& packer);
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
