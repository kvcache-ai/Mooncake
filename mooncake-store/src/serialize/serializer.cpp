#include <array>
#include <bit>
#include <iostream>
#include <limits>
#include <optional>
#include <vector>

#include "serialize/serializer.h"
#include "ha/snapshot/allocator_snapshot_codec.h"
#include "offset_allocator/offset_allocator.h"
#include "types.h"
#include "master_service.h"
#include "common/zstd_util.h"

namespace mooncake {

// Node serialization size constants for offset_allocator::__Allocator
constexpr size_t OFFSET_ALLOCATOR_NODE_BOOL_SIZE = 1;
constexpr size_t OFFSET_ALLOCATOR_NODE_UINT32_COUNT = 6;
constexpr size_t OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE =
    OFFSET_ALLOCATOR_NODE_BOOL_SIZE +
    OFFSET_ALLOCATOR_NODE_UINT32_COUNT * sizeof(uint32_t);

// Node capacity accepted from serialized data must stay bounded: the allocator
// materializes max_capacity nodes, so a corrupt-but-parseable payload could
// otherwise request billions of nodes and trigger the OOM killer before
// bad_alloc is thrown. 1<<24 (16.7M) stays above every legitimate
// configuration (the storage backend clamps node capacity to about 9.6M) and
// matches the bound the byte serializer applies.
constexpr uint32_t kMaxSerializedAllocatorNodes = 1u << 24;

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

    // Parse every scalar before materializing the allocator: its constructor
    // reserves max_capacity nodes, so corrupt capacities must be rejected here
    // instead of allocating storage for them, and the payloads below are
    // validated against current_capacity before they are copied into it.
    const uint32_t size = array_items[index++].as<uint32_t>();
    const uint32_t current_capacity = array_items[index++].as<uint32_t>();
    const uint32_t max_capacity = array_items[index++].as<uint32_t>();
    const uint32_t free_storage = array_items[index++].as<uint32_t>();
    const uint32_t used_bins_top = array_items[index++].as<uint32_t>();
    if (size == 0 || current_capacity == 0 || current_capacity > max_capacity ||
        max_capacity > kMaxSerializedAllocatorNodes) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator invalid "
                        "capacity fields: size {}, current_capacity {}, "
                        "max_capacity {}",
                        size, current_capacity, max_capacity)));
    }

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

    std::array<uint8_t, offset_allocator::NUM_TOP_BINS> used_bins{};
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

    if (bin_indices_array.via.array.size != offset_allocator::NUM_LEAF_BINS) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator binIndices "
                        "invalid size: expected {}, got {}",
                        offset_allocator::NUM_LEAF_BINS,
                        bin_indices_array.via.array.size)));
    }

    std::array<offset_allocator::NodeIndex, offset_allocator::NUM_LEAF_BINS>
        bin_indices{};
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

        // Create copy of compressed data
        std::vector<uint8_t> compressed_data(
            reinterpret_cast<const uint8_t *>(nodes_bin.via.bin.ptr),
            reinterpret_cast<const uint8_t *>(nodes_bin.via.bin.ptr) +
                nodes_bin.via.bin.size);

        // Decompress data
        serialized_nodes = zstd_decompress(compressed_data);

        // The payload must describe exactly the nodes it will populate.
        const size_t expected_size = static_cast<size_t>(current_capacity) *
                                     OFFSET_ALLOCATOR_NODE_SERIALIZED_SIZE;
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

        // Create copy of compressed data
        std::vector<uint8_t> compressed_data(
            reinterpret_cast<const uint8_t *>(free_nodes_bin.via.bin.ptr),
            reinterpret_cast<const uint8_t *>(free_nodes_bin.via.bin.ptr) +
                free_nodes_bin.via.bin.size);

        // Decompress data
        const std::vector<uint8_t> serialized_free_nodes =
            zstd_decompress(compressed_data);

        // Verify decompressed data size is reasonable
        const size_t expected_size = static_cast<size_t>(current_capacity) * 4;
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
        size_t offset = 0;
        for (uint32_t i = 0; i < current_capacity; i++) {
            free_nodes[i] = SerializationHelper::deserializeUint32(
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

    // Capacities and payload sizes are consistent, so materializing
    // max_capacity nodes is bounded and every copy below stays in range.
    auto allocator = std::make_unique<offset_allocator::__Allocator>(
        size, current_capacity, max_capacity);
    allocator->m_freeStorage = free_storage;
    allocator->m_usedBinsTop = used_bins_top;
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
    for (uint32_t i = 0; i < current_capacity; i++) {
        allocator->m_freeNodes[i] = free_nodes[i];
    }

    // Deserialize freeOffset
    const uint32_t free_offset = array_items[index++].as<uint32_t>();
    if (free_offset > current_capacity) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize offset_allocator::__Allocator freeOffset "
                        "{} is out of range for capacity {}",
                        free_offset, current_capacity)));
    }
    allocator->m_freeOffset = free_offset;

    return allocator;
}

// Mirrors __Allocator's small-float bin encoding
// (SmallFloat::uintToFloatRoundDown in offset_allocator.cpp). The vendored
// mapping is not exposed through its header, and a persisted layout must be
// checked against the same size class the live allocator derives when it
// inserts or removes a bin node, so the few lines are duplicated here on
// purpose.
static uint32_t allocatorBinForUnits(uint32_t size) {
    constexpr uint32_t kMantissaBits = 3;
    constexpr uint32_t kMantissaValue = 1u << kMantissaBits;
    constexpr uint32_t kMantissaMask = kMantissaValue - 1;
    if (size < kMantissaValue) {
        return size;
    }
    const uint32_t highest_set_bit =
        static_cast<uint32_t>(std::bit_width(size)) - 1;
    const uint32_t mantissa_start_bit = highest_set_bit - kMantissaBits;
    const uint32_t exponent = mantissa_start_bit + 1;
    return (exponent << kMantissaBits) |
           ((size >> mantissa_start_bit) & kMantissaMask);
}

std::optional<Serializer<offset_allocator::__Allocator>::LayoutUsage>
Serializer<offset_allocator::__Allocator>::ValidateLayout(
    const offset_allocator::__Allocator &allocator, uint64_t multiplier_bits) {
    const uint32_t capacity = allocator.m_current_capacity;
    if (allocator.m_size == 0 || capacity == 0 ||
        capacity > allocator.m_max_capacity ||
        allocator.m_nodes.size() < capacity ||
        allocator.m_freeNodes.size() < capacity ||
        allocator.m_freeOffset > capacity) {
        return std::nullopt;
    }

    std::vector<NodeState> state(capacity, NodeState::kUnclassified);
    uint64_t used_nodes = 0;
    uint64_t used_bytes = 0;
    for (uint32_t i = 0; i < capacity; ++i) {
        if (!allocator.m_nodes[i].used) {
            continue;
        }
        state[i] = NodeState::kUsed;
        ++used_nodes;
        used_bytes += static_cast<uint64_t>(allocator.m_nodes[i].dataSize)
                      << multiplier_bits;
    }

    const auto bin_nodes = ValidateBins(allocator, state);
    if (!bin_nodes) {
        return std::nullopt;
    }
    const uint32_t live_nodes = static_cast<uint32_t>(used_nodes) + *bin_nodes;
    if (!ValidateFreeStack(allocator, state, live_nodes) ||
        !ValidateNeighborChain(allocator, state, live_nodes)) {
        return std::nullopt;
    }
    return LayoutUsage{used_bytes, used_nodes};
}

std::optional<uint32_t> Serializer<offset_allocator::__Allocator>::ValidateBins(
    const offset_allocator::__Allocator &allocator,
    std::vector<NodeState> &state) {
    const uint32_t capacity = allocator.m_current_capacity;
    // Bin lists must be acyclic, hold only free nodes of their own size class,
    // and be linked consistently in both directions.
    uint8_t expected_used_bins[offset_allocator::NUM_TOP_BINS] = {};
    uint32_t expected_used_bins_top = 0;
    uint32_t bin_nodes = 0;
    uint32_t free_storage = 0;
    for (uint32_t bin = 0; bin < offset_allocator::NUM_LEAF_BINS; ++bin) {
        uint32_t node_index = allocator.m_binIndices[bin];
        if (node_index == offset_allocator::__Allocator::Node::unused) {
            continue;
        }
        const uint32_t top_bin = bin >> offset_allocator::TOP_BINS_INDEX_SHIFT;
        expected_used_bins[top_bin] |=
            uint8_t{1} << (bin & offset_allocator::LEAF_BINS_INDEX_MASK);
        expected_used_bins_top |= uint32_t{1} << top_bin;
        uint32_t previous = offset_allocator::__Allocator::Node::unused;
        while (node_index != offset_allocator::__Allocator::Node::unused) {
            if (node_index >= capacity ||
                state[node_index] != NodeState::kUnclassified) {
                return std::nullopt;
            }
            const auto &node = allocator.m_nodes[node_index];
            if (node.used || allocatorBinForUnits(node.dataSize) != bin ||
                node.binListPrev != previous ||
                free_storage >
                    std::numeric_limits<uint32_t>::max() - node.dataSize) {
                return std::nullopt;
            }
            state[node_index] = NodeState::kFree;
            ++bin_nodes;
            free_storage += node.dataSize;
            previous = node_index;
            node_index = node.binListNext;
        }
    }
    if (allocator.m_freeStorage != free_storage ||
        allocator.m_usedBinsTop != expected_used_bins_top) {
        return std::nullopt;
    }
    for (uint32_t top = 0; top < offset_allocator::NUM_TOP_BINS; ++top) {
        if (allocator.m_usedBins[top] != expected_used_bins[top]) {
            return std::nullopt;
        }
    }

    return bin_nodes;
}

bool Serializer<offset_allocator::__Allocator>::ValidateFreeStack(
    const offset_allocator::__Allocator &allocator,
    std::vector<NodeState> &state, uint32_t live_nodes) {
    const uint32_t capacity = allocator.m_current_capacity;
    // The free stack holds exactly the nodes that are neither used nor in a
    // bin, and its position matches the used/bin accounting.
    for (uint32_t i = allocator.m_freeOffset; i < capacity; ++i) {
        const uint32_t node_index = allocator.m_freeNodes[i];
        if (node_index >= capacity ||
            state[node_index] != NodeState::kUnclassified) {
            return false;
        }
        state[node_index] = NodeState::kSpare;
    }
    if (allocator.m_freeOffset != live_nodes) {
        return false;
    }
    for (uint32_t i = 0; i < capacity; ++i) {
        if (state[i] == NodeState::kUnclassified) {
            return false;
        }
    }
    return true;
}

bool Serializer<offset_allocator::__Allocator>::ValidateNeighborChain(
    const offset_allocator::__Allocator &allocator,
    const std::vector<NodeState> &state, uint32_t live_nodes) {
    const uint32_t capacity = allocator.m_current_capacity;
    // Live nodes form one neighbor chain ordered by offset that covers
    // [0, m_size) exactly once.
    const auto is_live = [&state](uint32_t node_index) {
        return state[node_index] == NodeState::kUsed ||
               state[node_index] == NodeState::kFree;
    };
    constexpr uint32_t kUnused = offset_allocator::__Allocator::Node::unused;
    uint32_t head = kUnused;
    for (uint32_t i = 0; i < capacity; ++i) {
        if (!is_live(i) || allocator.m_nodes[i].neighborPrev != kUnused) {
            continue;
        }
        if (head != kUnused) {
            return false;
        }
        head = i;
    }
    std::vector<uint8_t> walked(capacity, 0);
    uint32_t chain_nodes = 0;
    uint32_t offset = 0;
    for (uint32_t node_index = head; node_index != kUnused;
         node_index = allocator.m_nodes[node_index].neighborNext) {
        if (node_index >= capacity || !is_live(node_index) ||
            walked[node_index]) {
            return false;
        }
        walked[node_index] = 1;
        const auto &node = allocator.m_nodes[node_index];
        if (node.dataOffset != offset || node.dataSize == 0 ||
            node.dataSize > allocator.m_size - offset) {
            return false;
        }
        offset += node.dataSize;
        ++chain_nodes;
        if (node.neighborNext != kUnused) {
            const uint32_t next = node.neighborNext;
            if (next >= capacity || !is_live(next) ||
                allocator.m_nodes[next].neighborPrev != node_index) {
                return false;
            }
        }
        if (node.neighborPrev != kUnused) {
            const uint32_t previous = node.neighborPrev;
            if (previous >= capacity || !is_live(previous) ||
                allocator.m_nodes[previous].neighborNext != node_index) {
                return false;
            }
        }
    }
    return offset == allocator.m_size && chain_nodes == live_nodes;
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
    auto allocator_result =
        Serializer<offset_allocator::__Allocator>::deserialize(
            array_items[index++]);
    if (!allocator_result) {
        return tl::unexpected(allocator_result.error());
    }

    // The persisted counters must describe the persisted layout before it is
    // installed, the same gate OffsetBufferAllocator::Restore applies.
    const auto usage =
        Serializer<offset_allocator::__Allocator>::ValidateLayout(
            *allocator_result.value(), multiplier_bits);
    if (!usage || usage->used_nodes != allocated_num ||
        usage->used_bytes < allocated_size) {
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize offset_allocator::OffsetAllocator "
                               "rejected its state"));
    }

    // Install through the same factory that snapshot restore uses, so the
    // capacity relationship is re-checked as well.
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
        return tl::unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "deserialize offset_allocator::OffsetAllocator "
                               "rejected its state"));
    }

    return *offset_allocator;
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
        case ReplicaType::DFS: {
            const auto *dfs_data = std::get_if<DfsReplicaData>(&replica.data_);
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

tl::expected<void, SerializationError> Serializer<MountedSegment>::serialize(
    const MountedSegment &mounted_segment, MsgpackPacker &packer) {
    // Use array structure for packing, more efficient
    // Format: [segment_id, segment_name, segment_base, segment_size,
    // te_endpoint, status, has_buffer_allocator, buffer_allocator_data,
    // host_id]

    packer.pack_array(9);

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
            packer.pack(mounted_segment.segment.host_id);
            return {};
        }
    }

    packer.pack(false);  // Mark no valid buffer allocator exists
    packer.pack_nil();
    packer.pack(mounted_segment.segment.host_id);
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
        if (obj.via.array.size >= 9) {
            mounted_segment.segment.host_id = array[8].as<std::string>();
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
    packer.pack(allocator.getSegmentName());
    packer.pack(static_cast<uint64_t>(allocator.base()));
    packer.pack(static_cast<uint64_t>(allocator.capacity()));
    packer.pack(static_cast<uint64_t>(allocator.size()));
    packer.pack(allocator.getTransportEndpoint());

    // Serialize offset_allocator
    return Serializer<mooncake::offset_allocator::OffsetAllocator>::serialize(
        *allocator.getOffsetAllocator(), packer);
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
        auto snapshot = ha::AllocatorSnapshotCodec::Decode(obj);
        if (!snapshot) return tl::unexpected(snapshot.error());
        auto allocator = OffsetBufferAllocator::Restore(std::move(*snapshot));
        if (!allocator) {
            return tl::unexpected(SerializationError(
                allocator.error(),
                "deserialize OffsetBufferAllocator rejected its state"));
        }
        return std::move(*allocator);
    } catch (const std::exception &e) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("deserialize OffsetBufferAllocator failed: {}",
                        e.what())));
    }
}

}  // namespace mooncake
