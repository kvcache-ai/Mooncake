#include "ha/snapshot/allocator_snapshot_codec.h"

#include <fmt/format.h>

#include <exception>
#include <limits>

namespace mooncake::ha {

tl::expected<void, SerializationError> AllocatorSnapshotCodec::Encode(
    const OffsetBufferAllocatorSnapshot& snapshot, MsgpackPacker& packer) {
    if (!snapshot.allocation_state.layout) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL, "snapshot allocator has no layout"));
    }
    // Preserve the legacy OffsetBufferAllocator and OffsetAllocator arrays.
    packer.pack_array(6);
    packer.pack(snapshot.segment_name);
    packer.pack(static_cast<uint64_t>(snapshot.base));
    packer.pack(static_cast<uint64_t>(snapshot.capacity));
    packer.pack(static_cast<uint64_t>(snapshot.used_bytes));
    packer.pack(snapshot.transport_endpoint);
    const auto& layout = snapshot.allocation_state;
    packer.pack_array(6);
    packer.pack(layout.base);
    packer.pack(layout.multiplier_bits);
    packer.pack(layout.capacity);
    packer.pack(layout.allocated_size);
    packer.pack(layout.allocated_num);
    return Serializer<offset_allocator::__Allocator>::serialize(*layout.layout,
                                                                packer);
}

tl::expected<OffsetBufferAllocatorSnapshot, SerializationError>
AllocatorSnapshotCodec::Decode(const msgpack::object& object) {
    if (object.type != msgpack::type::ARRAY || object.via.array.size != 6) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "snapshot OffsetBufferAllocator is not an array[6]"));
    }

    try {
        const auto* array = object.via.array.ptr;
        std::string segment_name = array[0].as<std::string>();
        auto base = static_cast<size_t>(array[1].as<uint64_t>());
        auto total_size = static_cast<size_t>(array[2].as<uint64_t>());
        auto current_size = static_cast<size_t>(array[3].as<uint64_t>());
        std::string transport_endpoint = array[4].as<std::string>();
        // Usage is restored into signed metrics and must describe bytes within
        // this allocator, not wrap the gauge or exceed its capacity.
        if (current_size > total_size ||
            current_size >
                static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "snapshot OffsetBufferAllocator has invalid usage"));
        }

        if (array[5].type != msgpack::type::ARRAY ||
            array[5].via.array.size != 6) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "snapshot OffsetAllocator is not an array[6]"));
        }
        const auto* state = array[5].via.array.ptr;
        auto layout =
            Serializer<offset_allocator::__Allocator>::deserialize(state[5]);
        if (!layout) {
            return tl::make_unexpected(layout.error());
        }
        offset_allocator::OffsetAllocatorSnapshot allocation_state{
            state[0].as<uint64_t>(), state[1].as<uint64_t>(),
            state[2].as<uint64_t>(), state[3].as<uint64_t>(),
            state[4].as<uint64_t>(), std::move(*layout)};
        if (allocation_state.base != base ||
            allocation_state.capacity != total_size) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "snapshot allocator bounds are inconsistent"));
        }
        return OffsetBufferAllocatorSnapshot{std::move(segment_name),
                                             base,
                                             total_size,
                                             current_size,
                                             std::move(transport_endpoint),
                                             std::move(allocation_state)};
    } catch (const std::exception& error) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("decode OffsetBufferAllocator failed: {}",
                        error.what())));
    }
}

}  // namespace mooncake::ha
