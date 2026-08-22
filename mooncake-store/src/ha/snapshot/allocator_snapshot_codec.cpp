#include "ha/snapshot/allocator_snapshot_codec.h"

#include <fmt/format.h>

#include "master_metric_manager.h"

namespace mooncake::ha {

tl::expected<void, SerializationError> AllocatorSnapshotCodec::Encode(
    const BufferAllocatorBase& allocator, MsgpackPacker& packer) {
    const auto* offset = dynamic_cast<const OffsetBufferAllocator*>(&allocator);
    if (!offset) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::SERIALIZE_UNSUPPORTED,
            "snapshot allocator is not OffsetBufferAllocator"));
    }

    // Preserved wire shape: [segment_name, base, total_size, current_size,
    // transport_endpoint, offset_allocator].
    packer.pack_array(6);
    packer.pack(offset->segment_name_);
    packer.pack(static_cast<uint64_t>(offset->base_));
    packer.pack(static_cast<uint64_t>(offset->total_size_));
    packer.pack(static_cast<uint64_t>(offset->cur_size_.load()));
    packer.pack(offset->transport_endpoint_);
    return Serializer<offset_allocator::OffsetAllocator>::serialize(
        *offset->offset_allocator_, packer);
}

tl::expected<std::shared_ptr<BufferAllocatorBase>, SerializationError>
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

        auto offset_allocator =
            Serializer<offset_allocator::OffsetAllocator>::deserialize(
                array[5]);
        if (!offset_allocator) {
            return tl::make_unexpected(offset_allocator.error());
        }

        auto allocator = std::make_shared<OffsetBufferAllocator>(
            segment_name, base, total_size, transport_endpoint);
        allocator->offset_allocator_ = std::move(*offset_allocator);
        allocator->cur_size_ = current_size;

        // Restore bypasses allocate(), while destruction still decrements the
        // current size. Keep metric accounting symmetric for live and temporary
        // snapshot readers.
        MasterMetricManager::instance().inc_allocated_mem_size(
            segment_name, static_cast<int64_t>(current_size));
        return std::shared_ptr<BufferAllocatorBase>(std::move(allocator));
    } catch (const std::exception& error) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            fmt::format("decode OffsetBufferAllocator failed: {}",
                        error.what())));
    }
}

}  // namespace mooncake::ha
