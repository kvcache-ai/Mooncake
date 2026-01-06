#include <iostream>
#include <vector>

#include "serialize/serializer.hpp"
#include "offset_allocator/offset_allocator.hpp"
#include "types.h"
#include "master_service.h"
#include "utils/zstd_util.h"

namespace mooncake {

// __Allocator serialize_msgpack
tl::expected<void, SerializationError> Serializer<offset_allocator::__Allocator>::serialize(
    const offset_allocator::__Allocator &allocator, MsgpackPacker &packer) {
    // 使用数组而不是map来存储数据，更紧凑
    // 数组顺序与deserialize_msgpack中的一致
    packer.pack_array(10);

    // 基本属性 (按顺序打包)
    packer.pack(allocator.m_size);
    packer.pack(allocator.m_current_capacity);
    packer.pack(allocator.m_max_capacity);
    packer.pack(allocator.m_freeStorage);
    packer.pack(allocator.m_usedBinsTop);

    // usedBins 数组
    packer.pack_array(offset_allocator::NUM_TOP_BINS);
    for (unsigned char m_usedBin : allocator.m_usedBins) {
        packer.pack(m_usedBin);
    }

    // binIndex 数组
    packer.pack_array(offset_allocator::NUM_LEAF_BINS);
    for (unsigned int m_binIndex : allocator.m_binIndices) {
        packer.pack(m_binIndex);
    }

    // nodes 数据序列化和压缩
    std::vector<uint8_t> serialized_nodes;
    serialized_nodes.reserve(allocator.m_max_capacity * 25);

    for (uint32_t i = 0; i < allocator.m_max_capacity; i++) {
        const auto &node = allocator.m_nodes[i];
        serialized_nodes.push_back(node.used ? 1 : 0);
        SerializationHelper::serializeUint32(node.dataOffset, serialized_nodes);
        SerializationHelper::serializeUint32(node.dataSize, serialized_nodes);
        SerializationHelper::serializeUint32(node.binListPrev, serialized_nodes);
        SerializationHelper::serializeUint32(node.binListNext, serialized_nodes);
        SerializationHelper::serializeUint32(node.neighborPrev, serialized_nodes);
        SerializationHelper::serializeUint32(node.neighborNext, serialized_nodes);
    }

    try {
        std::vector<uint8_t> compressed_nodes = zstd_compress(serialized_nodes, 3);
        packer.pack(compressed_nodes);

        // freeNodes 数据序列化和压缩
        std::vector<uint8_t> serialized_free_nodes;
        serialized_free_nodes.reserve(allocator.m_current_capacity * 4);

        for (uint32_t i = 0; i < allocator.m_current_capacity; i++) {
            SerializationHelper::serializeUint32(allocator.m_freeNodes[i], serialized_free_nodes);
        }

        std::vector<uint8_t> compressed_free_nodes = zstd_compress(serialized_free_nodes, 3);
        packer.pack(compressed_free_nodes);
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL,
            std::string("offset_allocator::__Allocator, error compressing nodes: ") + e.what()));
    }

    // freeOffset
    packer.pack(allocator.m_freeOffset);

    return {};
}

// __Allocator deserialize_msgpack
tl::expected<std::unique_ptr<offset_allocator::__Allocator>, SerializationError>
Serializer<offset_allocator::__Allocator>::deserialize(const msgpack::object &obj) {
    // 检查对象类型是否为数组（与serialize_msgpack保持一致）
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize offset_allocator::__Allocator "
                                                 "invalid msgpack data, expected array"));
    }

    // 验证数组大小是否正确
    if (obj.via.array.size != 10) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize offset_allocator::__Allocator invalid "
                                           "array size: expected 9, got {}",
                                           obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;
    size_t index = 0;

    // 反序列化基本属性
    uint32_t size = array_items[index++].as<uint32_t>();
    uint32_t current_capacity = array_items[index++].as<uint32_t>();
    uint32_t max_capacity = array_items[index++].as<uint32_t>();

    // 创建 allocator 对象
    auto allocator = std::make_unique<offset_allocator::__Allocator>(size, current_capacity,max_capacity);

    allocator->m_freeStorage = array_items[index++].as<uint32_t>();
    allocator->m_usedBinsTop = array_items[index++].as<uint32_t>();

    // 反序列化 usedBins 数组
    const auto &used_bins_array = array_items[index++];
    if (used_bins_array.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize offset_allocator::__Allocator "
                                                 "usedBins is not an array"));
    }

    if (used_bins_array.via.array.size != offset_allocator::NUM_TOP_BINS) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator usedBins "
                        "invalid size: expected {}, got {}",
                        offset_allocator::NUM_TOP_BINS, used_bins_array.via.array.size)));
    }

    for (uint32_t i = 0; i < used_bins_array.via.array.size && i < offset_allocator::NUM_TOP_BINS;
         i++) {
        allocator->m_usedBins[i] = used_bins_array.via.array.ptr[i].as<uint8_t>();
    }

    // 反序列化 binIndices 数组
    const auto &bin_indices_array = array_items[index++];
    if (bin_indices_array.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize offset_allocator::__Allocator "
                                                 "binIndices is not an array"));
    }

    if (bin_indices_array.via.array.size != offset_allocator::NUM_LEAF_BINS) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator binIndices "
                        "invalid size: expected {}, got {}",
                        offset_allocator::NUM_LEAF_BINS, bin_indices_array.via.array.size)));
    }

    for (uint32_t i = 0;
         i < bin_indices_array.via.array.size && i < offset_allocator::NUM_LEAF_BINS; i++) {
        allocator->m_binIndices[i] = bin_indices_array.via.array.ptr[i].as<uint32_t>();
    }

    try {
        // 反序列化压缩的 nodes 数据
        const auto &nodes_bin = array_items[index++];
        if (nodes_bin.type != msgpack::type::BIN) {
            return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                     "deserialize offset_allocator::__Allocator "
                                                     "nodes data is not binary"));
        }

        // 创建压缩数据的副本
        std::vector<uint8_t> compressed_data(
            reinterpret_cast<const uint8_t *>(nodes_bin.via.bin.ptr),
            reinterpret_cast<const uint8_t *>(nodes_bin.via.bin.ptr) + nodes_bin.via.bin.size);

        // 解压数据
        std::vector<uint8_t> serialized_nodes = zstd_decompress(compressed_data);

        // 验证解压后的数据大小是否合理
        if (serialized_nodes.size() % 25 != 0) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   fmt::format("deserialize offset_allocator::__Allocator invalid "
                                               "serialized nodes data size: expected multiple of "
                                               "25, actual size: {}",
                                               serialized_nodes.size())));
        }

        if (serialized_nodes.size() / 25 != max_capacity) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   fmt::format("deserialize offset_allocator::__Allocator invalid "
                                               "serialized nodes data size: expected quotient of "
                                               "{}, actual quotient: {}",
                                               max_capacity, serialized_nodes.size() / 25)));
        }

        // 按标准化格式反序列化 nodes 数组
        size_t offset = 0;
        for (uint32_t i = 0; i < max_capacity; i++) {
            if (offset + 25 > serialized_nodes.size()) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       fmt::format("deserialize offset_allocator::__Allocator "
                                                   "incomplete serialized nodes data at index {}",
                                                   i)));
            }

            // 反序列化 bool 字段
            allocator->m_nodes[i].used = (serialized_nodes[offset++] != 0);

            // 反序列化 uint32_t 字段
            allocator->m_nodes[i].dataOffset =
                SerializationHelper::deserializeUint32(&serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].dataSize =
                SerializationHelper::deserializeUint32(&serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].binListPrev =
                SerializationHelper::deserializeUint32(&serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].binListNext =
                SerializationHelper::deserializeUint32(&serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].neighborPrev =
                SerializationHelper::deserializeUint32(&serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].neighborNext =
                SerializationHelper::deserializeUint32(&serialized_nodes[offset]);
            offset += 4;
        }
    } catch (const std::exception &e) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize offset_allocator::__Allocator error "
                                           "decompressing nodes: {}",
                                           e.what())));
    }

    try {
        // 反序列化压缩的 freeNodes 数据
        const auto &free_nodes_bin = array_items[index++];
        if (free_nodes_bin.type != msgpack::type::BIN) {
            return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                     "deserialize offset_allocator::__Allocator "
                                                     "freeNodes data is not binary"));
        }

        // 创建压缩数据的副本
        std::vector<uint8_t> compressed_data(
            reinterpret_cast<const uint8_t *>(free_nodes_bin.via.bin.ptr),
            reinterpret_cast<const uint8_t *>(free_nodes_bin.via.bin.ptr) +
                free_nodes_bin.via.bin.size);

        // 解压数据
        std::vector<uint8_t> serialized_free_nodes = zstd_decompress(compressed_data);

        // 验证解压后的数据大小是否合理
        if (serialized_free_nodes.size() != current_capacity * 4) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("deserialize offset_allocator::__Allocator invalid "
                            "serialized free nodes data size: expected {}, actual {}",
                            current_capacity * 4, serialized_free_nodes.size())));
        }

        // 按标准化格式反序列化 freeNodes 数组
        size_t offset = 0;
        for (uint32_t i = 0; i < current_capacity; i++) {
            if (offset + 4 > serialized_free_nodes.size()) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("deserialize offset_allocator::__Allocator incomplete "
                                "serialized free nodes data at index {}",
                                i)));
            }
            allocator->m_freeNodes[i] =
                SerializationHelper::deserializeUint32(&serialized_free_nodes[offset]);
            offset += 4;
        }
    } catch (const std::exception &e) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize offset_allocator::__Allocator error "
                                           "processing free nodes: {}",
                                           e.what())));
    }

    // 反序列化 freeOffset
    allocator->m_freeOffset = array_items[index++].as<uint32_t>();

    return allocator;
}

// serialize_msgpack
tl::expected<void, SerializationError>
Serializer<offset_allocator::OffsetAllocator>::serialize(
    const offset_allocator::OffsetAllocator &allocator, MsgpackPacker &packer) {
    packer.pack_array(6);

    // 序列化基础成员（按顺序打包）
    packer.pack(allocator.m_base);
    packer.pack(allocator.m_multiplier_bits);
    packer.pack(allocator.m_capacity);

    packer.pack(allocator.m_allocated_size);
    packer.pack(allocator.m_allocated_num);

    // 序列化 __Allocator
    auto allocator_result =
        Serializer<offset_allocator::__Allocator>::serialize(
            *allocator.m_allocator, packer);
    if (!allocator_result) {
        return tl::unexpected(allocator_result.error());
    }

    return {};
}

// deserialize_msgpack
auto Serializer<offset_allocator::OffsetAllocator>::deserialize(const msgpack::object &obj)
    -> tl::expected<PointerType, SerializationError> {
    // 检查对象类型是否为数组（与serialize_msgpack保持一致）
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize offset_allocator::OffsetAllocator "
                                                 "invalid msgpack data, expected array"));
    }

    // 验证数组大小是否正确 (应该有3个元素，与serialize_msgpack中的一致)
    if (obj.via.array.size != 6) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize offset_allocator::OffsetAllocator invalid "
                                           "array size: expected 6, got {}",
                                           obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;
    size_t index = 0;

    // 反序列化基本属性
    uint64_t base = array_items[index++].as<uint64_t>();
    uint64_t multiplier_bits = array_items[index++].as<uint64_t>();
    uint64_t capacity = array_items[index++].as<uint64_t>();

    uint64_t allocated_size= array_items[index++].as<uint64_t>();
    uint64_t allocated_num= array_items[index++].as<uint64_t>();

    // 反序列化 __Allocator
    // 直接将数组中的第三个元素传递给 __Allocator 的反序列化函数
    auto allocator_result =
        Serializer<offset_allocator::__Allocator>::deserialize(array_items[index++]);
    if (!allocator_result) {
        return tl::unexpected(allocator_result.error());
    }

    // 创建 OffsetAllocator 实例
    auto offset_allocator = std::shared_ptr<offset_allocator::OffsetAllocator>(
        new offset_allocator::OffsetAllocator(
            base, capacity, multiplier_bits,
            std::move(allocator_result.value())));
    offset_allocator->m_allocated_size = allocated_size;
    offset_allocator->m_allocated_num = allocated_num;

    return offset_allocator;
}

tl::expected<void, SerializationError>
Serializer<offset_allocator::OffsetAllocationHandle>::serialize(
    const offset_allocator::OffsetAllocationHandle &handle, MsgpackPacker &packer) {
    packer.pack_array(3);

    // 序列化基本字段
    packer.pack(handle.real_base);
    packer.pack(handle.requested_size);

    // 序列化 allocation 结构体中的两个字段
    packer.pack_array(2);
    packer.pack(handle.m_allocation.offset);
    packer.pack(handle.m_allocation.metadata);
    return {};
}


auto Serializer<offset_allocator::OffsetAllocationHandle>::deserialize(
    const msgpack::object &obj, const std::shared_ptr<offset_allocator::OffsetAllocator> &allocator)
    -> tl::expected<PointerType, SerializationError> {
    // 检查对象类型是否为数组（与serialize_msgpack保持一致）
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize offset_allocator::OffsetAllocationHandle invalid "
                               "msgpack data, expected array"));
    }

    // 验证数组大小是否正确 (应该有3个元素，与serialize_msgpack中的一致)
    if (obj.via.array.size != 3) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize offset_allocator::OffsetAllocationHandle "
                                           "invalid array size: expected 3, got {}",
                                           obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;

    // 反序列化基本属性
    uint64_t real_base = array_items[0].as<uint64_t>();
    uint64_t requested_size = array_items[1].as<uint64_t>();

    // 反序列化 allocation 结构
    const auto &allocation_array = array_items[2];
    if (allocation_array.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize offset_allocator::OffsetAllocationHandle allocation "
                               "is not an array"));
    }

    if (allocation_array.via.array.size != 2) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize offset_allocator::OffsetAllocationHandle "
                                           "allocation invalid size: expected 2, got {}",
                                           allocation_array.via.array.size)));
    }

    auto offset = allocation_array.via.array.ptr[0].as<uint32_t>();
    auto metadata = allocation_array.via.array.ptr[1].as<uint32_t>();
    offset_allocator::OffsetAllocation allocation(offset, metadata);

    // 创建一个新的 OffsetAllocationHandle 对象
    auto handle = std::make_shared<offset_allocator::OffsetAllocationHandle>(
        allocator, allocation, real_base, requested_size);

    return handle;
}


tl::expected<void, SerializationError> Serializer<AllocatedBuffer>::serialize(
    const AllocatedBuffer &buffer, const SegmentView &segment_view, MsgpackPacker &packer) {
    packer.pack_array(5);

    // 序列化基本属性
    //packer.pack(buffer.segment_name_);
    packer.pack(static_cast<uint64_t>(buffer.size_));
    packer.pack(reinterpret_cast<uint64_t>(buffer.buffer_ptr_));
    //packer.pack(static_cast<int32_t>(buffer.status));

    if (buffer.allocator_.expired()) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL,
            fmt::format("buffer.allocator_.expired,buffer_ptr:{}", buffer.buffer_ptr_)));
    }

    // 获取 segment 信息
    const auto &allocator = buffer.allocator_.lock();
    if (!allocator) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL, fmt::format("serialize AllocatedBuffer "
                                                    "buffer.allocator_.lock() fail,buffer_ptr:{}",
                                                    buffer.buffer_ptr_)));
    }

    Segment segment;
    ErrorCode ret = segment_view.GetSegment(allocator, segment);
    if (ret != ErrorCode::OK) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL, fmt::format("serialize AllocatedBuffer "
                                                   "segment_view.GetSegment() fail ret={}",
                                                   static_cast<int32_t>(ret))));
    }

    packer.pack(UuidToString(segment.id));

    // 序列化 offset_handle_ (如果存在)
    if (buffer.offset_handle_.has_value()) {
        // 标记存在 offset_handle
        packer.pack(true);
        auto handle_result =
            Serializer<offset_allocator::OffsetAllocationHandle>::serialize(
                buffer.offset_handle_.value(), packer);
        if (!handle_result) {
            return tl::unexpected(handle_result.error());
        }
    } else {
        // 标记不存在 offset_handle
        packer.pack(false);
        packer.pack_nil();
    }

    return {};
}


auto Serializer<AllocatedBuffer>::deserialize(const msgpack::object &obj,
                                                      const SegmentView &segment_view)
    -> tl::expected<PointerType, SerializationError> {
    // 检查对象类型是否为数组（与serialize_msgpack保持一致）
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize_msgpack AllocatedBuffer invalid "
                                                 "msgpack data, expected array"));
    }

    // 验证数组大小是否正确 (应该有5个元素：size, buffer_ptr, segment_id, has_offset_handle, offset_handle)
    if (obj.via.array.size != 5) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize_msgpack AllocatedBuffer invalid array "
                                           "size: expected 5, got {}",
                                           obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;

    // 反序列化基本属性
    //std::string segment_name = array_items[0].as<std::string>();
    auto size = static_cast<size_t>(array_items[0].as<uint64_t>());
    void *buffer_ptr = reinterpret_cast<void *>(array_items[1].as<uint64_t>());
    //auto status = static_cast<BufStatus>(array_items[3].as<int32_t>());

    // 获取 segment_id 并查找对应的 allocator
    std::string segment_id = array_items[2].as<std::string>();
    UUID segment_uuid;
    bool success = StringToUuid(segment_id, segment_uuid);
    if (!success) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize_msgpack AllocatedBuffer invalid segment "
                                           "ID format: {}",
                                           segment_id)));
    }

    MountedSegment mountedSegment;
    ErrorCode ret = segment_view.GetMountedSegment(segment_uuid, mountedSegment);
    if (ret != ErrorCode::OK) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize_msgpack AllocatedBuffer "
                        "segment_view.GetMountedSegment() fail ret={},segment_id={}",
                        static_cast<int32_t>(ret), segment_id)));
    }

    if (mountedSegment.status != SegmentStatus::OK) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize_msgpack AllocatedBuffer "
                        "mountedSegment.status!=OK status={} segment_id={}",
                        static_cast<int32_t>(mountedSegment.status), segment_id)));
    }

    std::shared_ptr<BufferAllocatorBase> allocator = mountedSegment.buf_allocator;
    // 检查 allocator 是否有效
    if (!allocator) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize_msgpack AllocatedBuffer invalid allocator "
                                           "for segment {}",
                                           segment_id)));
    }

    // 反序列化 offset_handle_ (如果存在)
    std::optional<offset_allocator::OffsetAllocationHandle> offsetHandle = std::nullopt;

    bool has_offset_handle = array_items[3].as<bool>();
    if (has_offset_handle) {
        auto offset_allocator = std::dynamic_pointer_cast<OffsetBufferAllocator>(allocator);
        if (offset_allocator) {
            // 使用 OffsetBufferAllocator 的 offset_allocator_ 创建
            // OffsetAllocationHandle
            auto handle_result =
                Serializer<offset_allocator::OffsetAllocationHandle>::deserialize(
                    array_items[4], offset_allocator->getOffsetAllocator());
            if (!handle_result) {
                return tl::unexpected(handle_result.error());
            }
            offsetHandle = std::move(*handle_result.value());
        }
    }

    // 创建 AllocatedBuffer 对象
    auto buffer = std::make_unique<AllocatedBuffer>(allocator, buffer_ptr, size,
                                                    std::move(offsetHandle));
    //buffer->status = status;

    return buffer;
}


tl::expected<void, SerializationError> Serializer<Replica>::serialize(
    const Replica &replica, const SegmentView &segment_view, MsgpackPacker &packer) {
    packer.pack_array(3);

    // 序列化 status_ 成员变量
    packer.pack(static_cast<int16_t>(replica.status_));

    // 序列化 buffers_ 成员变量
    if (const auto *mem_data = std::get_if<MemoryReplicaData>(&replica.data_)) {
        auto buffer_ptr = mem_data->buffer.get();
        if (!buffer_ptr) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   fmt::format("serialize_msgpack Replica "
                                               "buffer_ptr is nullptr")));
        }
        packer.pack(static_cast<int8_t>(ReplicaType::MEMORY));
        auto result = Serializer<AllocatedBuffer>::serialize(
            *buffer_ptr, segment_view, packer);
        if (!result) {
            return tl::unexpected(result.error());
        }
    } else {
        // 其它类型暂不支持
        packer.pack(255);
        packer.pack_nil();
    }

    return {};
}


auto Serializer<Replica>::deserialize(const msgpack::object &obj,
                                              const SegmentView &segment_view)
    -> tl::expected<PointerType, SerializationError> {
    // 检查对象类型是否为数组（与serialize保持一致）
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize_msgpack Replica invalid msgpack "
                                                 "data, expected array"));
    }

    // 验证数组大小是否正确 (应该有3个元素，与serialize中的一致)
    if (obj.via.array.size != 3) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize_msgpack Replica invalid array size: "
                                           "expected 2, got {}",
                                           obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;

    // 反序列化 status_ 成员变量
    auto status = static_cast<ReplicaStatus>(array_items[0].as<int16_t>());

    auto replica_type=array_items[1].as<int8_t>();
    if (replica_type != static_cast<int8_t>(ReplicaType::MEMORY)) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize Replica invalid replica type: {}", replica_type)));
    }
    // 反序列化 buffers_ 成员变量
    auto buffer_result = Serializer<AllocatedBuffer>::deserialize(
        array_items[2], segment_view);
    if (!buffer_result) {
        return tl::unexpected(buffer_result.error());
    }

    // 使用带参构造函数创建 Replica 对象
    auto replica = std::make_shared<Replica>(std::move(buffer_result.value()), status);
    return replica;
}


tl::expected<void, SerializationError> Serializer<MountedSegment>::serialize(
    const MountedSegment &mounted_segment, MsgpackPacker &packer) {
    // 使用数组结构打包，提高效率
    // 格式: [segment_id, segment_name, segment_base, segment_size, te_endpoint, status,
    // has_buffer_allocator, buffer_allocator_data...]

    packer.pack_array(8);

    // 序列化 Segment 信息
    packer.pack(UuidToString(mounted_segment.segment.id));
    packer.pack(mounted_segment.segment.name);
    packer.pack(static_cast<uint64_t>(mounted_segment.segment.base));
    packer.pack(static_cast<uint64_t>(mounted_segment.segment.size));
    packer.pack(mounted_segment.segment.te_endpoint);

    // 序列化 SegmentStatus
    packer.pack(static_cast<int16_t>(mounted_segment.status));

    // 序列化 BufferAllocator
    if (mounted_segment.buf_allocator) {
        auto offsetAllocator =
            std::dynamic_pointer_cast<OffsetBufferAllocator>(mounted_segment.buf_allocator);
        if (offsetAllocator) {
            packer.pack(true);  // 标记存在buffer allocator
            auto result =
                Serializer<OffsetBufferAllocator>::serialize(*offsetAllocator, packer);
            if (!result) {
                return tl::unexpected(result.error());
            }
            return {};
        }
    }

    packer.pack(false);  // 标记不存在有效的buffer allocator
    packer.pack_nil();
    return {};
}


tl::expected<MountedSegment, SerializationError> Serializer<MountedSegment>::deserialize(
    const msgpack::object &obj) {
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize MountedSegment invalid serialized "
                                                 "state: not a msgpack array"));
    }

    if (obj.via.array.size < 8) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize MountedSegment invalid array size"));
    }

    MountedSegment mounted_segment;
    msgpack::object *array = obj.via.array.ptr;

    try {
        // 反序列化 Segment 信息
        std::string segment_id_str = array[0].as<std::string>();
        UUID segment_uuid;
        if (!StringToUuid(segment_id_str, segment_uuid)) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("deserialize MountedSegment invalid UUID {}", segment_id_str)));
        }

        mounted_segment.segment.id = segment_uuid;
        mounted_segment.segment.name = array[1].as<std::string>();
        mounted_segment.segment.base = static_cast<uintptr_t>(array[2].as<uint64_t>());
        mounted_segment.segment.size = static_cast<size_t>(array[3].as<uint64_t>());
        mounted_segment.segment.te_endpoint = array[4].as<std::string>();

        // 反序列化 SegmentStatus
        mounted_segment.status = static_cast<SegmentStatus>(array[5].as<int16_t>());

        // 反序列化 BufferAllocator
        bool has_buffer_allocator = array[6].as<bool>();
        if (has_buffer_allocator) {
            auto allocatorResult = Serializer<OffsetBufferAllocator>::deserialize(array[7]);
            if (allocatorResult) {
                mounted_segment.buf_allocator = std::move(allocatorResult.value());
            } else {
                return tl::unexpected(allocatorResult.error());
            }
        }
    } catch (const std::exception &e) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               fmt::format("deserialize MountedSegment failed: {}", e.what())));
    }

    return mounted_segment;
}


tl::expected<void, SerializationError> Serializer<OffsetBufferAllocator>::serialize(
    const OffsetBufferAllocator &allocator, MsgpackPacker &packer) {
    // 使用数组结构打包OffsetBufferAllocator
    // 格式: [segment_name, base, total_size, current_size, transport_endpoint, offset_allocator]

    packer.pack_array(6);

    // 序列化基本属性
    packer.pack(allocator.segment_name_);
    packer.pack(static_cast<uint64_t>(allocator.base_));
    packer.pack(static_cast<uint64_t>(allocator.total_size_));
    packer.pack(static_cast<uint64_t>(allocator.cur_size_.load()));
    packer.pack(allocator.transport_endpoint_);

    // 序列化 offset_allocator
    auto result = Serializer<mooncake::offset_allocator::OffsetAllocator>::serialize(
        *allocator.getOffsetAllocator(), packer);
    if (!result) {
        return tl::unexpected(result.error());
    }

    return {};
}

auto Serializer<OffsetBufferAllocator>::deserialize(const msgpack::object &obj)
    -> tl::expected<PointerType, SerializationError> {
    // 验证输入状态
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                                 "deserialize OffsetBufferAllocator invalid "
                                                 "serialized state: not a msgpack array"));
    }

    if (obj.via.array.size != 6) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "deserialize OffsetBufferAllocator invalid array size"));
    }

    try {
        msgpack::object *array = obj.via.array.ptr;

        // 反序列化基本属性
        std::string segment_name = array[0].as<std::string>();
        auto base = static_cast<size_t>(array[1].as<uint64_t>());
        auto total_size = static_cast<size_t>(array[2].as<uint64_t>());
        auto cur_size = static_cast<size_t>(array[3].as<uint64_t>());
        std::string transport_endpoint = array[4].as<std::string>();

        // 反序列化 offset_allocator
        auto offset_allocator_result =
            Serializer<mooncake::offset_allocator::OffsetAllocator>::deserialize(array[5]);
        if (!offset_allocator_result) {
            return tl::unexpected(offset_allocator_result.error());
        }

        // 创建 OffsetBufferAllocator 实例
        auto allocator = std::make_shared<OffsetBufferAllocator>(
            segment_name, base, total_size, transport_endpoint);

        // 设置内部成员变量值
        allocator->offset_allocator_ = offset_allocator_result.value();
        allocator->cur_size_ = cur_size;

        return allocator;
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize OffsetBufferAllocator failed: {}", e.what())));
    }
}

}  // namespace mooncake
