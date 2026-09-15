#include <array>
#include <iostream>
#include <optional>
#include <vector>

#include "serialize/serializer.h"
#include "ha/snapshot/allocator_snapshot_codec.h"
#include "offset_allocator/offset_allocator.h"
#include "types.h"
#include "replica.h"
#include "segment/pool_read_access.h"
#include "common/zstd_util.h"

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

    // Parse every scalar before materializing or decompressing anything: the
    // allocator constructor reserves max_capacity nodes, so corrupt capacities
    // and a corrupt freeOffset must be rejected against the node bound before a
    // payload can request large storage.
    uint32_t size = 0;
    uint32_t current_capacity = 0;
    uint32_t max_capacity = 0;
    uint32_t free_storage = 0;
    uint32_t used_bins_top = 0;
    uint32_t free_offset = 0;
    try {
        size = array_items[index++].as<uint32_t>();
        current_capacity = array_items[index++].as<uint32_t>();
        max_capacity = array_items[index++].as<uint32_t>();
        free_storage = array_items[index++].as<uint32_t>();
        used_bins_top = array_items[index++].as<uint32_t>();
        // freeOffset is packed last; reading it here keeps the whole scalar
        // header validated before any node payload is touched.
        free_offset = array_items[9].as<uint32_t>();
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator invalid "
                        "scalar field: {}",
                        e.what())));
    }

    if (auto valid =
            offset_allocator::OffsetAllocatorSnapshot::ValidateNodeCapacity(
                size, current_capacity, max_capacity, free_offset);
        !valid) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator {}",
                        valid.error())));
    }

    std::array<uint8_t, offset_allocator::NUM_TOP_BINS> used_bins{};
    std::array<offset_allocator::NodeIndex, offset_allocator::NUM_LEAF_BINS>
        bin_indices{};
    try {
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
                fmt::format(
                    "deserialize offset_allocator::__Allocator usedBins "
                    "invalid size: expected {}, got {}",
                    offset_allocator::NUM_TOP_BINS,
                    used_bins_array.via.array.size)));
        }

        for (uint32_t i = 0; i < offset_allocator::NUM_TOP_BINS; i++) {
            used_bins[i] = used_bins_array.via.array.ptr[i].as<uint8_t>();
        }

        // Deserialize binIndices array
        const auto &bin_indices_array = array_items[index++];
        if (bin_indices_array.type != msgpack::type::ARRAY) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize offset_allocator::__Allocator "
                                   "binIndices is not an array"));
        }

        if (bin_indices_array.via.array.size !=
            offset_allocator::NUM_LEAF_BINS) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format(
                    "deserialize offset_allocator::__Allocator binIndices "
                    "invalid size: expected {}, got {}",
                    offset_allocator::NUM_LEAF_BINS,
                    bin_indices_array.via.array.size)));
        }

        for (uint32_t i = 0; i < offset_allocator::NUM_LEAF_BINS; i++) {
            const uint32_t bin_index =
                bin_indices_array.via.array.ptr[i].as<uint32_t>();
            if (bin_index != offset_allocator::__Allocator::Node::unused &&
                bin_index >= current_capacity) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("deserialize offset_allocator::__Allocator bin "
                                "index {} is out of range for capacity {}",
                                bin_index, current_capacity)));
            }
            bin_indices[i] = bin_index;
        }
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator invalid "
                        "bitmap field: {}",
                        e.what())));
    }

    std::vector<uint8_t> serialized_nodes;
    try {
        // Deserialize compressed nodes data
        const auto &nodes_bin = array_items[index++];
        if (nodes_bin.type != msgpack::type::BIN) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize offset_allocator::__Allocator "
                                   "nodes data is not binary"));
        }

        // The payload must describe exactly the nodes it will populate, so the
        // decompression is bounded by that size: a frame header can then never
        // request a far larger buffer.
        const size_t expected_size = static_cast<size_t>(current_capacity) *
                                     OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE;
        serialized_nodes = zstd_decompress(
            reinterpret_cast<const uint8_t *>(nodes_bin.via.bin.ptr),
            nodes_bin.via.bin.size, expected_size);
        if (serialized_nodes.size() != expected_size) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("deserialize offset_allocator::__Allocator invalid "
                            "serialized nodes data size: expected {}, actual "
                            "size: {}",
                            expected_size, serialized_nodes.size())));
        }
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator error "
                        "decompressing nodes: {}",
                        e.what())));
    }

    std::vector<uint32_t> free_nodes;
    try {
        // Deserialize compressed freeNodes data
        const auto &free_nodes_bin = array_items[index++];
        if (free_nodes_bin.type != msgpack::type::BIN) {
            return tl::unexpected(
                SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                   "deserialize offset_allocator::__Allocator "
                                   "freeNodes data is not binary"));
        }

        // Bounded like the node payload, then required to be exact.
        const size_t expected_size = static_cast<size_t>(current_capacity) * 4;
        const std::vector<uint8_t> serialized_free_nodes = zstd_decompress(
            reinterpret_cast<const uint8_t *>(free_nodes_bin.via.bin.ptr),
            free_nodes_bin.via.bin.size, expected_size);
        if (serialized_free_nodes.size() != expected_size) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format(
                    "deserialize offset_allocator::__Allocator invalid "
                    "serialized free nodes data size: expected {}, actual {}",
                    expected_size, serialized_free_nodes.size())));
        }

        // Deserialize freeNodes array in standardized format
        free_nodes.resize(current_capacity);
        for (uint32_t i = 0; i < current_capacity; i++) {
            free_nodes[i] = SerializationHelper::deserializeUint32(
                &serialized_free_nodes[static_cast<size_t>(i) * 4]);
        }
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator error "
                        "processing free nodes: {}",
                        e.what())));
    }

    // Capacities and payload sizes are consistent: both the reservation and
    // populated node storage are bounded, and every copy stays in range.
    try {
        auto allocator = std::make_unique<offset_allocator::__Allocator>(
            size, current_capacity, max_capacity);
        allocator->m_freeStorage = free_storage;
        allocator->m_usedBinsTop = used_bins_top;
        allocator->m_freeOffset = free_offset;
        for (uint32_t i = 0; i < offset_allocator::NUM_TOP_BINS; i++) {
            allocator->m_usedBins[i] = used_bins[i];
        }
        for (uint32_t i = 0; i < offset_allocator::NUM_LEAF_BINS; i++) {
            allocator->m_binIndices[i] = bin_indices[i];
        }

        // Deserialize nodes array in standardized format
        size_t offset = 0;
        for (uint32_t i = 0; i < current_capacity; i++) {
            // Deserialize bool field
            allocator->m_nodes[i].used = (serialized_nodes[offset++] != 0);

            // Deserialize uint32_t fields
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
        for (uint32_t i = 0; i < current_capacity; i++) {
            allocator->m_freeNodes[i] = free_nodes[i];
        }

        return allocator;
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator error "
                        "constructing layout: {}",
                        e.what())));
    }
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

    try {
        // Deserialize basic properties
        uint64_t base = array_items[index++].as<uint64_t>();
        uint64_t multiplier_bits = array_items[index++].as<uint64_t>();
        uint64_t capacity = array_items[index++].as<uint64_t>();

        uint64_t allocated_size = array_items[index++].as<uint64_t>();
        uint64_t allocated_num = array_items[index++].as<uint64_t>();

        // Deserialize __Allocator
        auto allocator_result =
            Serializer<offset_allocator::__Allocator>::deserialize(
                array_items[index++]);
        if (!allocator_result) {
            return tl::unexpected(allocator_result.error());
        }

        // Install through the same factory that snapshot restore uses: it runs
        // the full layout validation exactly once and returns the detailed
        // reason.
        offset_allocator::OffsetAllocatorSnapshot snapshot{
            .base = base,
            .multiplier_bits = multiplier_bits,
            .capacity = capacity,
            .allocated_size = allocated_size,
            .allocated_num = allocated_num,
            .layout = std::move(allocator_result.value()),
        };
        auto offset_allocator =
            offset_allocator::OffsetAllocator::Restore(std::move(snapshot));
        if (!offset_allocator) {
            return tl::unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format(
                    "deserialize offset_allocator::OffsetAllocator rejected "
                    "its state: {}",
                    offset_allocator.error())));
        }

        return *offset_allocator;
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format(
                "deserialize offset_allocator::OffsetAllocator failed: {}",
                e.what())));
    }
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
    if (!allocator || requested_size == 0) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "invalid offset allocation handle"));
    }
    // A raw constructor would let a corrupt node index reach the allocator's
    // free path. Use the recovery API to validate ownership and bounds first.
    auto restored =
        allocator->createHandleAtNode(metadata, real_base, requested_size);
    if (!restored || restored->m_allocation.getOffset() != offset) {
        // This is only a borrowed reconstruction of an existing allocation;
        // rejecting its redundant offset must not free that allocation.
        if (restored) restored->m_allocator.reset();
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "offset allocation handle does not match restored allocator"));
    }
    return std::make_shared<offset_allocator::OffsetAllocationHandle>(
        std::move(*restored));
}

tl::expected<void, SerializationError> Serializer<AllocatedBuffer>::serialize(
    const AllocatedBuffer& buffer, const SegmentPool& segment_pool,
    MsgpackPacker& packer) {
    const auto allocator = buffer.getAllocator();
    // Snapshot encoding runs in a forked child. Never acquire the inherited
    // pool mutex here: a vanished parent thread may have held its write lock.
    const MountedRegion* region = nullptr;
    for (const auto& mounted : segment_pool.catalog_.Regions()) {
        const auto* resource = segment_pool.GetResource(mounted);
        if (resource && resource->allocator() == allocator) {
            region = &mounted;
            break;
        }
    }
    if (!region || region->kind != RegionKind::HOST_MEMORY) {
        return tl::unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL,
            "serialize AllocatedBuffer has no snapshot-compatible region"));
    }
    return SerializeWithRegionId(buffer, region->segment.id, packer);
}

tl::expected<void, SerializationError> Serializer<AllocatedBuffer>::serialize(
    const AllocatedBuffer& buffer, const SegmentPool::ReadAccess& segment_view,
    MsgpackPacker& packer) {
    const auto allocator = buffer.getAllocator();
    for (const auto& mounted : segment_view.Catalog().Regions()) {
        if (mounted.kind == RegionKind::HOST_MEMORY && allocator &&
            segment_view.GetAllocator(mounted.segment.id) == allocator) {
            return SerializeWithRegionId(buffer, mounted.segment.id, packer);
        }
    }
    return tl::unexpected(SerializationError(
        ErrorCode::SERIALIZE_FAIL,
        "serialize AllocatedBuffer has no snapshot-compatible region"));
}

tl::expected<void, SerializationError>
Serializer<AllocatedBuffer>::SerializeWithRegionId(
    const AllocatedBuffer& buffer, const UUID& region_id,
    MsgpackPacker& packer) {
    packer.pack_array(5);
    packer.pack(static_cast<uint64_t>(buffer.size_));
    packer.pack(reinterpret_cast<uint64_t>(buffer.buffer_ptr_));
    packer.pack(UuidToString(region_id));

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

auto Serializer<AllocatedBuffer>::deserialize(
    const msgpack::object& obj, const SegmentPool::ReadAccess& segment_view)
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

    auto* array_items = obj.via.array.ptr;

    // Deserialize basic properties
    // std::string segment_name = array_items[0].as<std::string>();
    auto size = static_cast<size_t>(array_items[0].as<uint64_t>());
    void* buffer_ptr = reinterpret_cast<void*>(array_items[1].as<uint64_t>());
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

    const auto* mounted_region = segment_view.Catalog().Find(segment_uuid);
    if (!mounted_region) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize AllocatedBuffer unknown segment {}",
                        segment_id)));
    }
    // Draining/gracefully-unmounting regions retain readable allocations.
    // Discarded replicas can also refer to an immediate unmount in progress.
    std::shared_ptr<BufferAllocatorBase> allocator =
        segment_view.GetAllocator(segment_uuid);
    // Check if allocator is valid
    if (!allocator) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize_msgpack AllocatedBuffer invalid allocator "
                        "for segment {}",
                        segment_id)));
    }

    const auto address = reinterpret_cast<uintptr_t>(buffer_ptr);
    const auto& segment = mounted_region->segment;
    if (size == 0 || address < segment.base ||
        address - segment.base >= segment.size ||
        size > segment.size - (address - segment.base)) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "allocated buffer lies outside its mounted region"));
    }

    auto offset_allocator =
        std::dynamic_pointer_cast<OffsetBufferAllocator>(allocator);
    const auto& handle_object = array_items[4];
    if (!offset_allocator || !array_items[3].as<bool>() ||
        handle_object.type != msgpack::type::ARRAY ||
        handle_object.via.array.size != 3 ||
        handle_object.via.array.ptr[0].as<uint64_t>() != address ||
        handle_object.via.array.ptr[1].as<uint64_t>() != size) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "allocated buffer has a missing or inconsistent offset handle"));
    }
    auto handle_result =
        Serializer<offset_allocator::OffsetAllocationHandle>::deserialize(
            handle_object, offset_allocator->getOffsetAllocator());
    if (!handle_result) return tl::unexpected(handle_result.error());
    std::optional<offset_allocator::OffsetAllocationHandle> offsetHandle(
        std::move(**handle_result));

    // Create AllocatedBuffer object
    auto buffer = std::make_unique<AllocatedBuffer>(allocator, buffer_ptr, size,
                                                    std::move(offsetHandle));
    if (!segment_view.BindBufferToSegment(segment_uuid, *buffer)) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize AllocatedBuffer cannot bind to its region"));
    }
    // buffer->status = status;

    return buffer;
}

template <typename SegmentAccess>
tl::expected<void, SerializationError> Serializer<Replica>::SerializeImpl(
    const Replica& replica, const SegmentAccess& segment_access,
    MsgpackPacker& packer) {
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
            const auto* mem_data =
                std::get_if<MemoryReplicaData>(&replica.data_);
            if (!mem_data || !mem_data->buffer) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("serialize_msgpack Replica memory buffer_ptr "
                                "is nullptr")));
            }
            auto result = Serializer<AllocatedBuffer>::serialize(
                *mem_data->buffer, segment_access, packer);
            if (!result) {
                return tl::unexpected(result.error());
            }
            break;
        }
        case ReplicaType::DISK: {
            const auto* disk_data =
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
            const auto* local_data =
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
        case ReplicaType::DFS: {
            const auto* dfs_data = std::get_if<DfsReplicaData>(&replica.data_);
            if (!dfs_data) {
                return tl::unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    "serialize_msgpack Replica missing DfsReplicaData"));
            }
            // Format: [file_path, offset, object_size, aligned_size, shard_idx]
            packer.pack_array(5);
            packer.pack(dfs_data->descriptor.file_path);
            packer.pack(static_cast<uint64_t>(dfs_data->descriptor.offset));
            packer.pack(
                static_cast<uint64_t>(dfs_data->descriptor.object_size));
            packer.pack(
                static_cast<uint64_t>(dfs_data->descriptor.aligned_size));
            packer.pack(static_cast<int32_t>(dfs_data->descriptor.shard_idx));
            break;
        }
        default:
            return tl::unexpected(SerializationError(
                ErrorCode::SERIALIZE_UNSUPPORTED,
                "snapshot does not support this replica type"));
    }

    return {};
}

tl::expected<void, SerializationError> Serializer<Replica>::serialize(
    const Replica& replica, const SegmentPool& segment_pool,
    MsgpackPacker& packer) {
    return SerializeImpl(replica, segment_pool, packer);
}

tl::expected<void, SerializationError> Serializer<Replica>::serialize(
    const Replica& replica, const SegmentPool::ReadAccess& segment_view,
    MsgpackPacker& packer) {
    return SerializeImpl(replica, segment_view, packer);
}

auto Serializer<Replica>::deserialize(
    const msgpack::object& obj, const SegmentPool::ReadAccess& segment_view)
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
        case static_cast<int8_t>(ReplicaType::DFS): {
            const auto &payload = array_items[3];
            if (payload.type != msgpack::type::ARRAY ||
                payload.via.array.size != 5) {
                return tl::unexpected(
                    SerializationError(ErrorCode::DESERIALIZE_FAIL,
                                       "deserialize_msgpack Replica DFS "
                                       "payload is not valid array[5]"));
            }
            auto *payload_items = payload.via.array.ptr;
            DistributedFSDescriptor descriptor;
            descriptor.file_path = payload_items[0].as<std::string>();
            descriptor.offset = payload_items[1].as<uint64_t>();
            descriptor.object_size = payload_items[2].as<uint64_t>();
            descriptor.aligned_size = payload_items[3].as<uint64_t>();
            descriptor.shard_idx = payload_items[4].as<int32_t>();

            replica = std::make_shared<Replica>(std::move(descriptor), status);
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

}  // namespace mooncake
