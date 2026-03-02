#include <iostream>
#include <vector>

#include "serialize/serializer.hpp"
#include "offset_allocator/offset_allocator.hpp"
#include "types.h"
#include "master_service.h"
#include "utils/zstd_util.h"

namespace mooncake {

// Node serialization size constants for offset_allocator::__Allocator
constexpr size_t OFFSET_ALLOCATOR_NODE_BOOL_SIZE = 1;
constexpr size_t OFFSET_ALLOCATOR_NODE_UINT32_COUNT = 6;
constexpr size_t OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE =
    OFFSET_ALLOCATOR_NODE_BOOL_SIZE +
    OFFSET_ALLOCATOR_NODE_UINT32_COUNT * sizeof(uint32_t);

// __Allocator serialize_msgpack
tl::expected<void, SerializationError>
Serializer<offset_allocator::__Allocator>::serialize(
    const offset_allocator::__Allocator &allocator, MsgpackPacker &packer) {
    // Use array instead of map for more compact storage
    // Array order is consistent with deserialize_msgpack
    packer.pack_array(10);

    // Basic properties (packed in order)
    packer.pack(allocator.m_size);
    packer.pack(allocator.m_current_capacity);
    packer.pack(allocator.m_max_capacity);
    packer.pack(allocator.m_freeStorage);
    packer.pack(allocator.m_usedBinsTop);

    // usedBins array
    packer.pack_array(offset_allocator::NUM_TOP_BINS);
    for (unsigned char m_usedBin : allocator.m_usedBins) {
        packer.pack(m_usedBin);
    }

    // binIndex array
    packer.pack_array(offset_allocator::NUM_LEAF_BINS);
    for (unsigned int m_binIndex : allocator.m_binIndices) {
        packer.pack(m_binIndex);
    }

    // nodes data serialization and compression
    // Each node serializes to: 1 byte (used flag) + 6 * sizeof(uint32_t)
    // (fields)
    std::vector<uint8_t> serialized_nodes;
    serialized_nodes.reserve(allocator.m_max_capacity *
                             OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE);

    for (uint32_t i = 0; i < allocator.m_current_capacity; i++) {
        const auto &node = allocator.m_nodes[i];
        serialized_nodes.push_back(node.used ? 1 : 0);
        SerializationHelper::serializeUint32(node.dataOffset, serialized_nodes);
        SerializationHelper::serializeUint32(node.dataSize, serialized_nodes);
        SerializationHelper::serializeUint32(node.binListPrev,
                                             serialized_nodes);
        SerializationHelper::serializeUint32(node.binListNext,
                                             serialized_nodes);
        SerializationHelper::serializeUint32(node.neighborPrev,
                                             serialized_nodes);
        SerializationHelper::serializeUint32(node.neighborNext,
                                             serialized_nodes);
    }

    try {
        std::vector<uint8_t> compressed_nodes =
            zstd_compress(serialized_nodes, 3);
        packer.pack(compressed_nodes);

        // freeNodes data serialization and compression
        std::vector<uint8_t> serialized_free_nodes;
        serialized_free_nodes.reserve(allocator.m_current_capacity * 4);

        for (uint32_t i = 0; i < allocator.m_current_capacity; i++) {
            SerializationHelper::serializeUint32(allocator.m_freeNodes[i],
                                                 serialized_free_nodes);
        }

        std::vector<uint8_t> compressed_free_nodes =
            zstd_compress(serialized_free_nodes, 3);
        packer.pack(compressed_free_nodes);
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL,
            std::string(
                "offset_allocator::__Allocator, error compressing nodes: ") +
                e.what()));
    }

    // freeOffset
    packer.pack(allocator.m_freeOffset);

    return {};
}

// __Allocator deserialize_msgpack
tl::expected<std::unique_ptr<offset_allocator::__Allocator>, SerializationError>
Serializer<offset_allocator::__Allocator>::deserialize(
    const msgpack::object &obj) {
    // Check if object type is array (consistent with serialize_msgpack)
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize offset_allocator::__Allocator "
                               "invalid msgpack data, expected array"));
    }

    // Verify array size is correct
    if (obj.via.array.size != 10) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator invalid "
                        "array size: expected 10, got {}",
                        obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;
    size_t index = 0;

    // Deserialize basic properties
    uint32_t size = array_items[index++].as<uint32_t>();
    uint32_t current_capacity = array_items[index++].as<uint32_t>();
    uint32_t max_capacity = array_items[index++].as<uint32_t>();

    // Create allocator object
    auto allocator = std::make_unique<offset_allocator::__Allocator>(
        size, current_capacity, max_capacity);

    allocator->m_freeStorage = array_items[index++].as<uint32_t>();
    allocator->m_usedBinsTop = array_items[index++].as<uint32_t>();

    // Deserialize usedBins array
    const auto &used_bins_array = array_items[index++];
    if (used_bins_array.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize offset_allocator::__Allocator "
                               "usedBins is not an array"));
    }

    if (used_bins_array.via.array.size != offset_allocator::NUM_TOP_BINS) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator usedBins "
                        "invalid size: expected {}, got {}",
                        offset_allocator::NUM_TOP_BINS,
                        used_bins_array.via.array.size)));
    }

    for (uint32_t i = 0; i < used_bins_array.via.array.size &&
                         i < offset_allocator::NUM_TOP_BINS;
         i++) {
        allocator->m_usedBins[i] =
            used_bins_array.via.array.ptr[i].as<uint8_t>();
    }

    // Deserialize binIndices array
    const auto &bin_indices_array = array_items[index++];
    if (bin_indices_array.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize offset_allocator::__Allocator "
                               "binIndices is not an array"));
    }

    if (bin_indices_array.via.array.size != offset_allocator::NUM_LEAF_BINS) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator binIndices "
                        "invalid size: expected {}, got {}",
                        offset_allocator::NUM_LEAF_BINS,
                        bin_indices_array.via.array.size)));
    }

    for (uint32_t i = 0; i < bin_indices_array.via.array.size &&
                         i < offset_allocator::NUM_LEAF_BINS;
         i++) {
        allocator->m_binIndices[i] =
            bin_indices_array.via.array.ptr[i].as<uint32_t>();
    }

    try {
        // Deserialize compressed nodes data
        const auto &nodes_bin = array_items[index++];
        if (nodes_bin.type != msgpack::type::BIN) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize offset_allocator::__Allocator "
                                   "nodes data is not binary"));
        }

        // Create copy of compressed data
        std::vector<uint8_t> compressed_data(
            reinterpret_cast<const uint8_t *>(nodes_bin.via.bin.ptr),
            reinterpret_cast<const uint8_t *>(nodes_bin.via.bin.ptr) +
                nodes_bin.via.bin.size);

        // Decompress data
        std::vector<uint8_t> serialized_nodes =
            zstd_decompress(compressed_data);

        // Verify decompressed data size is reasonable
        if (serialized_nodes.size() % OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE !=
            0) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("deserialize offset_allocator::__Allocator invalid "
                            "serialized nodes data size: expected multiple of "
                            "{}, actual size: {}",
                            OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE,
                            serialized_nodes.size())));
        }

        if (serialized_nodes.size() / OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE !=
            current_capacity) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("deserialize offset_allocator::__Allocator invalid "
                            "serialized nodes data size: expected quotient of "
                            "{}, actual quotient: {}",
                            current_capacity,
                            serialized_nodes.size() /
                                OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE)));
        }

        // Deserialize nodes array in standardized format
        size_t offset = 0;
        for (uint32_t i = 0; i < current_capacity; i++) {
            if (offset + OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE >
                serialized_nodes.size()) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("deserialize offset_allocator::__Allocator "
                                "incomplete serialized nodes data at index {}",
                                i)));
            }

            // Deserialize bool field
            allocator->m_nodes[i].used = (serialized_nodes[offset++] != 0);

            // Deserialize uint32_t field
            allocator->m_nodes[i].dataOffset =
                SerializationHelper::deserializeUint32(
                    &serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].dataSize =
                SerializationHelper::deserializeUint32(
                    &serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].binListPrev =
                SerializationHelper::deserializeUint32(
                    &serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].binListNext =
                SerializationHelper::deserializeUint32(
                    &serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].neighborPrev =
                SerializationHelper::deserializeUint32(
                    &serialized_nodes[offset]);
            offset += 4;
            allocator->m_nodes[i].neighborNext =
                SerializationHelper::deserializeUint32(
                    &serialized_nodes[offset]);
            offset += 4;
        }
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator error "
                        "decompressing nodes: {}",
                        e.what())));
    }

    try {
        // Deserialize compressed freeNodes data
        const auto &free_nodes_bin = array_items[index++];
        if (free_nodes_bin.type != msgpack::type::BIN) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize offset_allocator::__Allocator "
                                   "freeNodes data is not binary"));
        }

        // Create copy of compressed data
        std::vector<uint8_t> compressed_data(
            reinterpret_cast<const uint8_t *>(free_nodes_bin.via.bin.ptr),
            reinterpret_cast<const uint8_t *>(free_nodes_bin.via.bin.ptr) +
                free_nodes_bin.via.bin.size);

        // Decompress data
        std::vector<uint8_t> serialized_free_nodes =
            zstd_decompress(compressed_data);

        // Verify decompressed data size is reasonable
        if (serialized_free_nodes.size() != current_capacity * 4) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format(
                    "deserialize offset_allocator::__Allocator invalid "
                    "serialized free nodes data size: expected {}, actual {}",
                    current_capacity * 4, serialized_free_nodes.size())));
        }

        // Deserialize freeNodes array in standardized format
        size_t offset = 0;
        for (uint32_t i = 0; i < current_capacity; i++) {
            if (offset + 4 > serialized_free_nodes.size()) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format(
                        "deserialize offset_allocator::__Allocator incomplete "
                        "serialized free nodes data at index {}",
                        i)));
            }
            allocator->m_freeNodes[i] = SerializationHelper::deserializeUint32(
                &serialized_free_nodes[offset]);
            offset += 4;
        }
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator error "
                        "processing free nodes: {}",
                        e.what())));
    }

    // Deserialize freeOffset
    allocator->m_freeOffset = array_items[index++].as<uint32_t>();

    return allocator;
}

// serialize_msgpack
tl::expected<void, SerializationError>
Serializer<offset_allocator::OffsetAllocator>::serialize(
    const offset_allocator::OffsetAllocator &allocator, MsgpackPacker &packer) {
    packer.pack_array(6);

    // Serialize basic members (packed in order)
    packer.pack(allocator.m_base);
    packer.pack(allocator.m_multiplier_bits);
    packer.pack(allocator.m_capacity);

    packer.pack(allocator.m_allocated_size);
    packer.pack(allocator.m_allocated_num);

    // Serialize __Allocator
    auto allocator_result =
        Serializer<offset_allocator::__Allocator>::serialize(
            *allocator.m_allocator, packer);
    if (!allocator_result) {
        return tl::unexpected(allocator_result.error());
    }

    return {};
}

// deserialize_msgpack
auto Serializer<offset_allocator::OffsetAllocator>::deserialize(
    const msgpack::object &obj)
    -> tl::expected<PointerType, SerializationError> {
    // Check if object type is array (consistent with serialize_msgpack)
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize offset_allocator::OffsetAllocator "
                               "invalid msgpack data, expected array"));
    }

    // Verify array size is correct (should have 6 elements, consistent with
    // serialize_msgpack)
    if (obj.via.array.size != 6) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::OffsetAllocator invalid "
                        "array size: expected 6, got {}",
                        obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;
    size_t index = 0;

    // Deserialize basic properties
    uint64_t base = array_items[index++].as<uint64_t>();
    uint64_t multiplier_bits = array_items[index++].as<uint64_t>();
    uint64_t capacity = array_items[index++].as<uint64_t>();

    uint64_t allocated_size = array_items[index++].as<uint64_t>();
    uint64_t allocated_num = array_items[index++].as<uint64_t>();

    // Deserialize __Allocator
    // Pass the third element of the array directly to __Allocator's deserialize
    // function
    auto allocator_result =
        Serializer<offset_allocator::__Allocator>::deserialize(
            array_items[index++]);
    if (!allocator_result) {
        return tl::unexpected(allocator_result.error());
    }

    // Create OffsetAllocator instance
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
    const offset_allocator::OffsetAllocationHandle &handle,
    MsgpackPacker &packer) {
    packer.pack_array(3);

    // Serialize basic fields
    packer.pack(handle.real_base);
    packer.pack(handle.requested_size);

    // Serialize allocation struct fields
    packer.pack_array(2);
    packer.pack(handle.m_allocation.offset);
    packer.pack(handle.m_allocation.metadata);
    return {};
}

auto Serializer<offset_allocator::OffsetAllocationHandle>::deserialize(
    const msgpack::object &obj,
    const std::shared_ptr<offset_allocator::OffsetAllocator> &allocator)
    -> tl::expected<PointerType, SerializationError> {
    // Check if object type is array (consistent with serialize_msgpack)
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize offset_allocator::OffsetAllocationHandle invalid "
            "msgpack data, expected array"));
    }

    // Verify array size is correct (should have 3 elements, consistent with
    // serialize_msgpack)
    if (obj.via.array.size != 3) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::OffsetAllocationHandle "
                        "invalid array size: expected 3, got {}",
                        obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;

    // Deserialize basic properties
    uint64_t real_base = array_items[0].as<uint64_t>();
    uint64_t requested_size = array_items[1].as<uint64_t>();

    // Deserialize allocation struct
    const auto &allocation_array = array_items[2];
    if (allocation_array.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize offset_allocator::OffsetAllocationHandle allocation "
            "is not an array"));
    }

    if (allocation_array.via.array.size != 2) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::OffsetAllocationHandle "
                        "allocation invalid size: expected 2, got {}",
                        allocation_array.via.array.size)));
    }

    auto offset = allocation_array.via.array.ptr[0].as<uint32_t>();
    auto metadata = allocation_array.via.array.ptr[1].as<uint32_t>();
    offset_allocator::OffsetAllocation allocation(offset, metadata);

    // Create a new OffsetAllocationHandle object
    auto handle = std::make_shared<offset_allocator::OffsetAllocationHandle>(
        allocator, allocation, real_base, requested_size);

    return handle;
}

tl::expected<void, SerializationError> Serializer<AllocatedBuffer>::serialize(
    const AllocatedBuffer &buffer, const SegmentView &segment_view,
    MsgpackPacker &packer) {
    packer.pack_array(5);

    // Serialize basic properties
    // packer.pack(buffer.segment_name_);
    packer.pack(static_cast<uint64_t>(buffer.size_));
    packer.pack(reinterpret_cast<uint64_t>(buffer.buffer_ptr_));
    // packer.pack(static_cast<int32_t>(buffer.status));

    if (buffer.allocator_.expired()) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL,
            fmt::format("buffer.allocator_.expired,buffer_ptr:{}",
                        buffer.buffer_ptr_)));
    }

    // Get segment info
    const auto &allocator = buffer.allocator_.lock();
    if (!allocator) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL,
            fmt::format("serialize AllocatedBuffer "
                        "buffer.allocator_.lock() fail,buffer_ptr:{}",
                        buffer.buffer_ptr_)));
    }

    Segment segment;
    ErrorCode ret = segment_view.GetSegment(allocator, segment);
    if (ret != ErrorCode::OK) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL,
            fmt::format("serialize AllocatedBuffer "
                        "segment_view.GetSegment() fail ret={}",
                        static_cast<int32_t>(ret))));
    }

    packer.pack(UuidToString(segment.id));

    // Serialize offset_handle_ (if exists)
    if (buffer.offset_handle_.has_value()) {
        // Mark offset_handle exists
        packer.pack(true);
        auto handle_result =
            Serializer<offset_allocator::OffsetAllocationHandle>::serialize(
                buffer.offset_handle_.value(), packer);
        if (!handle_result) {
            return tl::unexpected(handle_result.error());
        }
    } else {
        // Mark offset_handle does not exist
        packer.pack(false);
        packer.pack_nil();
    }

    return {};
}

auto Serializer<AllocatedBuffer>::deserialize(const msgpack::object &obj,
                                              const SegmentView &segment_view)
    -> tl::expected<PointerType, SerializationError> {
    // Check if object type is array (consistent with serialize_msgpack)
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize_msgpack AllocatedBuffer invalid "
                               "msgpack data, expected array"));
    }

    // Verify array size is correct (should have 5 elements: size, buffer_ptr,
    // segment_id, has_offset_handle, offset_handle)
    if (obj.via.array.size != 5) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize_msgpack AllocatedBuffer invalid array "
                        "size: expected 5, got {}",
                        obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;

    // Deserialize basic properties
    // std::string segment_name = array_items[0].as<std::string>();
    auto size = static_cast<size_t>(array_items[0].as<uint64_t>());
    void *buffer_ptr = reinterpret_cast<void *>(array_items[1].as<uint64_t>());
    // auto status = static_cast<BufStatus>(array_items[3].as<int32_t>());

    // Get segment_id and find corresponding allocator
    std::string segment_id = array_items[2].as<std::string>();
    UUID segment_uuid;
    bool success = StringToUuid(segment_id, segment_uuid);
    if (!success) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize_msgpack AllocatedBuffer invalid segment "
                        "ID format: {}",
                        segment_id)));
    }

    MountedSegment mountedSegment;
    ErrorCode ret =
        segment_view.GetMountedSegment(segment_uuid, mountedSegment);
    if (ret != ErrorCode::OK) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format(
                "deserialize_msgpack AllocatedBuffer "
                "segment_view.GetMountedSegment() fail ret={},segment_id={}",
                static_cast<int32_t>(ret), segment_id)));
    }

    if (mountedSegment.status != SegmentStatus::OK) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize_msgpack AllocatedBuffer "
                        "mountedSegment.status!=OK status={} segment_id={}",
                        static_cast<int32_t>(mountedSegment.status),
                        segment_id)));
    }

    std::shared_ptr<BufferAllocatorBase> allocator =
        mountedSegment.buf_allocator;
    // Check if allocator is valid
    if (!allocator) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize_msgpack AllocatedBuffer invalid allocator "
                        "for segment {}",
                        segment_id)));
    }

    // Deserialize offset_handle_ (if exists)
    std::optional<offset_allocator::OffsetAllocationHandle> offsetHandle =
        std::nullopt;

    bool has_offset_handle = array_items[3].as<bool>();
    if (has_offset_handle) {
        auto offset_allocator =
            std::dynamic_pointer_cast<OffsetBufferAllocator>(allocator);
        if (offset_allocator) {
            // Use OffsetBufferAllocator's offset_allocator_ to create
            // OffsetAllocationHandle
            auto handle_result =
                Serializer<offset_allocator::OffsetAllocationHandle>::
                    deserialize(array_items[4],
                                offset_allocator->getOffsetAllocator());
            if (!handle_result) {
                return tl::unexpected(handle_result.error());
            }
            offsetHandle = std::move(*handle_result.value());
        }
    }

    // Create AllocatedBuffer object
    auto buffer = std::make_unique<AllocatedBuffer>(allocator, buffer_ptr, size,
                                                    std::move(offsetHandle));
    // buffer->status = status;

    return buffer;
}

tl::expected<void, SerializationError> Serializer<Replica>::serialize(
    const Replica &replica, const SegmentView &segment_view,
    MsgpackPacker &packer) {
    // Use unified array structure to pack Replica
    // Format: [id(uint64), status(int16), replica_type(int8), payload]
    packer.pack_array(4);

    // 1. Serialize id_ member variable
    packer.pack(static_cast<uint64_t>(replica.id_));

    // 2. Serialize status_ member variable
    packer.pack(static_cast<int16_t>(replica.status_));

    // 3. Serialize replica type
    auto replica_type = replica.type();
    packer.pack(static_cast<int8_t>(replica_type));

    // 4. Serialize specific data by type
    switch (replica_type) {
        case ReplicaType::MEMORY: {
            const auto *mem_data =
                std::get_if<MemoryReplicaData>(&replica.data_);
            if (!mem_data || !mem_data->buffer) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("serialize_msgpack Replica memory buffer_ptr "
                                "is nullptr")));
            }
            auto result = Serializer<AllocatedBuffer>::serialize(
                *mem_data->buffer, segment_view, packer);
            if (!result) {
                return tl::unexpected(result.error());
            }
            break;
        }
        case ReplicaType::DISK: {
            const auto *disk_data =
                std::get_if<DiskReplicaData>(&replica.data_);
            if (!disk_data) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    "serialize_msgpack Replica missing DiskReplicaData"));
            }
            // Format: [file_path, object_size]
            packer.pack_array(2);
            packer.pack(disk_data->file_path);
            packer.pack(static_cast<uint64_t>(disk_data->object_size));
            break;
        }
        case ReplicaType::LOCAL_DISK: {
            const auto *local_data =
                std::get_if<LocalDiskReplicaData>(&replica.data_);
            if (!local_data) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    "serialize_msgpack Replica missing LocalDiskReplicaData"));
            }
            // Format: [client_id_str, object_size, transport_endpoint]
            packer.pack_array(3);
            packer.pack(UuidToString(local_data->client_id));
            packer.pack(static_cast<uint64_t>(local_data->object_size));
            packer.pack(local_data->transport_endpoint);
            break;
        }
        default:
            // Unsupported replica type
            packer.pack(static_cast<int8_t>(255));
            packer.pack_nil();
            break;
    }

    return {};
}

auto Serializer<Replica>::deserialize(const msgpack::object &obj,
                                      const SegmentView &segment_view)
    -> tl::expected<PointerType, SerializationError> {
    // Check if object type is array (consistent with serialize)
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize_msgpack Replica invalid msgpack "
                               "data, expected array"));
    }

    // Verify array size is correct (should have 4 elements: id, status,
    // replica_type, payload)
    if (obj.via.array.size != 4) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize_msgpack Replica invalid array size: "
                        "expected 4, got {}",
                        obj.via.array.size)));
    }

    auto *array_items = obj.via.array.ptr;

    // 1. Deserialize id_ member variable
    auto id = static_cast<ReplicaID>(array_items[0].as<uint64_t>());

    // 2. Deserialize status_ member variable
    auto status = static_cast<ReplicaStatus>(array_items[1].as<int16_t>());

    // 3. Deserialize replica_type
    auto replica_type_code = array_items[2].as<int8_t>();

    // 4. Parse payload by type
    std::shared_ptr<Replica> replica;
    switch (replica_type_code) {
        case static_cast<int8_t>(ReplicaType::MEMORY): {
            // MEMORY: payload is AllocatedBuffer
            auto buffer_result = Serializer<AllocatedBuffer>::deserialize(
                array_items[3], segment_view);
            if (!buffer_result) {
                return tl::unexpected(buffer_result.error());
            }
            replica = std::make_shared<Replica>(
                std::move(buffer_result.value()), status);
            break;
        }
        case static_cast<int8_t>(ReplicaType::DISK): {
            const auto &payload = array_items[3];
            if (payload.type != msgpack::type::ARRAY ||
                payload.via.array.size != 2) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "deserialize_msgpack Replica DISK "
                                       "payload is not valid array[2]"));
            }
            auto *payload_items = payload.via.array.ptr;
            std::string file_path = payload_items[0].as<std::string>();
            uint64_t object_size = payload_items[1].as<uint64_t>();

            replica = std::make_shared<Replica>(std::move(file_path),
                                                object_size, status);
            break;
        }
        case static_cast<int8_t>(ReplicaType::LOCAL_DISK): {
            const auto &payload = array_items[3];
            if (payload.type != msgpack::type::ARRAY ||
                payload.via.array.size != 3) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "deserialize_msgpack Replica LOCAL_DISK "
                                       "payload is not valid array[3]"));
            }
            auto *payload_items = payload.via.array.ptr;
            std::string client_id_str = payload_items[0].as<std::string>();
            uint64_t object_size = payload_items[1].as<uint64_t>();
            std::string transport_endpoint = payload_items[2].as<std::string>();

            UUID client_id;
            if (!StringToUuid(client_id_str, client_id)) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("deserialize_msgpack Replica invalid client_id "
                                "UUID: {}",
                                client_id_str)));
            }

            replica = std::make_shared<Replica>(
                client_id, object_size, std::move(transport_endpoint), status);
            break;
        }
        default:
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("deserialize Replica invalid replica type: {}",
                            replica_type_code)));
    }

    // Restore the original id (overwrite the auto-generated one)
    // Note: refcnt_ is not restored, it remains 0 (default value)
    replica->id_ = id;

    return replica;
}

tl::expected<void, SerializationError> Serializer<MountedSegment>::serialize(
    const MountedSegment &mounted_segment, MsgpackPacker &packer) {
    // Use array structure for packing, more efficient
    // Format: [segment_id, segment_name, segment_base, segment_size,
    // te_endpoint, status, has_buffer_allocator, buffer_allocator_data...]

    packer.pack_array(8);

    // Serialize Segment info
    packer.pack(UuidToString(mounted_segment.segment.id));
    packer.pack(mounted_segment.segment.name);
    packer.pack(static_cast<uint64_t>(mounted_segment.segment.base));
    packer.pack(static_cast<uint64_t>(mounted_segment.segment.size));
    packer.pack(mounted_segment.segment.te_endpoint);

    // Serialize SegmentStatus
    packer.pack(static_cast<int16_t>(mounted_segment.status));

    // Serialize BufferAllocator
    if (mounted_segment.buf_allocator) {
        auto offsetAllocator = std::dynamic_pointer_cast<OffsetBufferAllocator>(
            mounted_segment.buf_allocator);
        if (offsetAllocator) {
            packer.pack(true);  // Mark buffer allocator exists
            auto result = Serializer<OffsetBufferAllocator>::serialize(
                *offsetAllocator, packer);
            if (!result) {
                return tl::unexpected(result.error());
            }
            return {};
        }
    }

    packer.pack(false);  // Mark no valid buffer allocator exists
    packer.pack_nil();
    return {};
}

tl::expected<MountedSegment, SerializationError>
Serializer<MountedSegment>::deserialize(const msgpack::object &obj) {
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize MountedSegment invalid serialized "
                               "state: not a msgpack array"));
    }

    if (obj.via.array.size < 8) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize MountedSegment invalid array size"));
    }

    MountedSegment mounted_segment;
    msgpack::object *array = obj.via.array.ptr;

    try {
        // Deserialize Segment info
        std::string segment_id_str = array[0].as<std::string>();
        UUID segment_uuid;
        if (!StringToUuid(segment_id_str, segment_uuid)) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("deserialize MountedSegment invalid UUID {}",
                            segment_id_str)));
        }

        mounted_segment.segment.id = segment_uuid;
        mounted_segment.segment.name = array[1].as<std::string>();
        mounted_segment.segment.base =
            static_cast<uintptr_t>(array[2].as<uint64_t>());
        mounted_segment.segment.size =
            static_cast<size_t>(array[3].as<uint64_t>());
        mounted_segment.segment.te_endpoint = array[4].as<std::string>();

        // Deserialize SegmentStatus
        mounted_segment.status =
            static_cast<SegmentStatus>(array[5].as<int16_t>());

        // Deserialize BufferAllocator
        bool has_buffer_allocator = array[6].as<bool>();
        if (has_buffer_allocator) {
            auto allocatorResult =
                Serializer<OffsetBufferAllocator>::deserialize(array[7]);
            if (allocatorResult) {
                mounted_segment.buf_allocator =
                    std::move(allocatorResult.value());
            } else {
                return tl::unexpected(allocatorResult.error());
            }
        }
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize MountedSegment failed: {}", e.what())));
    }

    return mounted_segment;
}

tl::expected<void, SerializationError>
Serializer<OffsetBufferAllocator>::serialize(
    const OffsetBufferAllocator &allocator, MsgpackPacker &packer) {
    // Use array structure to pack OffsetBufferAllocator
    // Format: [segment_name, base, total_size, current_size,
    // transport_endpoint, offset_allocator]

    packer.pack_array(6);

    // Serialize basic properties
    packer.pack(allocator.segment_name_);
    packer.pack(static_cast<uint64_t>(allocator.base_));
    packer.pack(static_cast<uint64_t>(allocator.total_size_));
    packer.pack(static_cast<uint64_t>(allocator.cur_size_.load()));
    packer.pack(allocator.transport_endpoint_);

    // Serialize offset_allocator
    auto result =
        Serializer<mooncake::offset_allocator::OffsetAllocator>::serialize(
            *allocator.getOffsetAllocator(), packer);
    if (!result) {
        return tl::unexpected(result.error());
    }

    return {};
}

auto Serializer<OffsetBufferAllocator>::deserialize(const msgpack::object &obj)
    -> tl::expected<PointerType, SerializationError> {
    // Validate input state
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize OffsetBufferAllocator invalid "
                               "serialized state: not a msgpack array"));
    }

    if (obj.via.array.size != 6) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize OffsetBufferAllocator invalid array size"));
    }

    try {
        msgpack::object *array = obj.via.array.ptr;

        // Deserialize basic properties
        std::string segment_name = array[0].as<std::string>();
        auto base = static_cast<size_t>(array[1].as<uint64_t>());
        auto total_size = static_cast<size_t>(array[2].as<uint64_t>());
        auto cur_size = static_cast<size_t>(array[3].as<uint64_t>());
        std::string transport_endpoint = array[4].as<std::string>();

        // Deserialize offset_allocator
        auto offset_allocator_result = Serializer<
            mooncake::offset_allocator::OffsetAllocator>::deserialize(array[5]);
        if (!offset_allocator_result) {
            return tl::unexpected(offset_allocator_result.error());
        }

        // Create OffsetBufferAllocator instance
        auto allocator = std::make_shared<OffsetBufferAllocator>(
            segment_name, base, total_size, transport_endpoint);

        // Set internal member variable values
        allocator->offset_allocator_ = offset_allocator_result.value();
        allocator->cur_size_ = cur_size;

        return allocator;
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize OffsetBufferAllocator failed: {}",
                        e.what())));
    }
}

}  // namespace mooncake
