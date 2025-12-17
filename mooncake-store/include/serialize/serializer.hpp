#pragma once

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

// 通用序列化接口
template <typename T>
class Serializer;

// 为 __Allocator 特化 Serializer
template <>
class Serializer<offset_allocator::__Allocator> {
   public:
    using PointerType = std::unique_ptr<offset_allocator::__Allocator>;

    static tl::expected<void, SerializationError> serialize(
        const offset_allocator::__Allocator &allocator, MsgpackPacker &packer);


    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj);
};

// 为 OffsetAllocator 特化 Serializer
template <>
class Serializer<offset_allocator::OffsetAllocator> {
   public:
    using PointerType = std::shared_ptr<offset_allocator::OffsetAllocator>;


    static tl::expected<void, SerializationError> serialize(
        const offset_allocator::OffsetAllocator &allocator, MsgpackPacker &packer);


    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj);
};

// 为 OffsetAllocationHandle 特化 Serializer
template <>
class Serializer<offset_allocator::OffsetAllocationHandle> {
   public:
    using PointerType = std::shared_ptr<offset_allocator::OffsetAllocationHandle>;


    static tl::expected<void, SerializationError> serialize(
        const offset_allocator::OffsetAllocationHandle &handle, MsgpackPacker &packer);


    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj,
        const std::shared_ptr<offset_allocator::OffsetAllocator> &allocator);

};

// 为 AllocatedBuffer 特化 Serializer
template <>
class Serializer<AllocatedBuffer> {
   public:
    using PointerType = std::unique_ptr<AllocatedBuffer>;

    static tl::expected<void, SerializationError> serialize(const AllocatedBuffer &buffer,
                                                                    const SegmentView &segment_view,
                                                                    MsgpackPacker &packer);

    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj, const SegmentView &segment_view);
};

// 为 Replica 特化 Serializer（接口声明）
template <>
class Serializer<Replica> {
   public:
    using PointerType = std::shared_ptr<Replica>;


    static tl::expected<void, SerializationError> serialize(const Replica &replica,
                                                                    const SegmentView &segment_view,
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

// OffsetBufferAllocator 特化 Serializer（接口声明）
template <>
class Serializer<OffsetBufferAllocator> {
   public:
    using PointerType = std::shared_ptr<OffsetBufferAllocator>;


    static tl::expected<void, SerializationError> serialize(
        const OffsetBufferAllocator &allocator, MsgpackPacker &packer);


    static tl::expected<PointerType, SerializationError> deserialize(
        const msgpack::object &obj);
};

// 通用的序列化助手类
class SerializationHelper {
   public:
    // 序列化 uint32_t（小端序）
    static void serializeUint32(uint32_t value, std::vector<uint8_t> &out) {
        out.push_back(static_cast<uint8_t>(value & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
        out.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
    }

    // 反序列化 uint32_t（小端序）
    static uint32_t deserializeUint32(const uint8_t *data) {
        return static_cast<uint32_t>(data[0]) | (static_cast<uint32_t>(data[1]) << 8) |
               (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
    }

};

}  // namespace mooncake