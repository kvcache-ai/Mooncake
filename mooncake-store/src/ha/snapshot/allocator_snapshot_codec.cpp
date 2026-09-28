#include "ha/snapshot/allocator_snapshot_codec.h"

#include <fmt/format.h>

#include <exception>

namespace mooncake::ha {

tl::expected<void, SerializationError> AllocatorSnapshotCodec::Encode(
    const OffsetBufferAllocatorSnapshot& snapshot, MsgpackPacker& packer) {
    if (!snapshot.allocation_state.layout) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::SERIALIZE_FAIL, "snapshot allocator has no layout"));
    }
    // Extend the legacy array with storage_level while accepting the old
    // six-element representation during decode.
    packer.pack_array(7);
    packer.pack(snapshot.segment_name);
    packer.pack(static_cast<uint64_t>(snapshot.base));
    packer.pack(static_cast<uint64_t>(snapshot.capacity));
    packer.pack(static_cast<uint64_t>(snapshot.used_bytes));
    packer.pack(snapshot.transport_endpoint);
    packer.pack(static_cast<int8_t>(snapshot.storage_level));
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
    if (object.type != msgpack::type::ARRAY ||
        (object.via.array.size != 6 && object.via.array.size != 7)) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "snapshot OffsetBufferAllocator is not an array[6] or array[7]"));
    }

    try {
        const auto* array = object.via.array.ptr;
        std::string segment_name = array[0].as<std::string>();
        auto base = static_cast<size_t>(array[1].as<uint64_t>());
        auto total_size = static_cast<size_t>(array[2].as<uint64_t>());
        auto used_bytes = static_cast<size_t>(array[3].as<uint64_t>());
        std::string transport_endpoint = array[4].as<std::string>();
        StorageLevel storage_level = StorageLevel::RAM;
        size_t allocation_state_index = 5;
        if (object.via.array.size == 7) {
            storage_level = static_cast<StorageLevel>(array[5].as<int8_t>());
            if (storage_level < StorageLevel::RAM ||
                storage_level >= StorageLevel::NUM_STORAGE_LEVELS) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    "snapshot OffsetBufferAllocator has invalid storage level"));
            }
            allocation_state_index = 6;
        }

        if (array[allocation_state_index].type != msgpack::type::ARRAY ||
            array[allocation_state_index].via.array.size != 6) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "snapshot OffsetAllocator is not an array[6]"));
        }
        const auto* state = array[allocation_state_index].via.array.ptr;
        auto layout =
            Serializer<offset_allocator::__Allocator>::deserialize(state[5]);
        if (!layout) {
            return tl::make_unexpected(layout.error());
        }

        OffsetBufferAllocatorSnapshot snapshot{
            .segment_name = std::move(segment_name),
            .base = base,
            .capacity = total_size,
            .used_bytes = used_bytes,
            .transport_endpoint = std::move(transport_endpoint),
            .allocation_state =
                offset_allocator::OffsetAllocatorSnapshot{
                    .base = state[0].as<uint64_t>(),
                    .multiplier_bits = state[1].as<uint64_t>(),
                    .capacity = state[2].as<uint64_t>(),
                    .allocated_size = state[3].as<uint64_t>(),
                    .allocated_num = state[4].as<uint64_t>(),
                    .layout = std::move(*layout),
                },
            .storage_level = storage_level,
        };
        // The snapshot owns both the outer metadata and the inner layout, so
        // one validation pass covers the bounds/base/usage consistency and the
        // allocator topology. Decode propagates diagnostics to its caller;
        // direct restore factories also log validation failures.
        if (auto valid = snapshot.Validate(); !valid) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("snapshot OffsetBufferAllocator {}",
                            valid.error())));
        }
        return snapshot;
    } catch (const std::exception& error) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("decode OffsetBufferAllocator failed: {}",
                        error.what())));
    }
}

}  // namespace mooncake::ha
