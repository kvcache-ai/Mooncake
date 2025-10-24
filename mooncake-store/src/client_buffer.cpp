#include "client_buffer.hpp"

#include <algorithm>
#include <cstdlib>
#include <vector>
#include "utils.h"

namespace mooncake {

ClientBufferAllocator::ClientBufferAllocator(size_t size,
                                             const std::string& protocol)
    : protocol(protocol), buffer_size_(size) {
    // Align to 64 bytes(cache line size) for better cache performance
    constexpr size_t alignment = 64;
    buffer_ = allocate_buffer_allocator_memory(size, protocol, alignment);
    if (!buffer_) {
        throw std::bad_alloc();
    }

    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_), size);
}

ClientBufferAllocator::~ClientBufferAllocator() {
    // Free the aligned allocated memory
    if (buffer_) {
        free_memory(protocol, buffer_);
    }
}

std::optional<BufferHandle> ClientBufferAllocator::allocate(size_t size) {
    auto handle = allocator_->allocate(size);
    if (!handle) {
        return std::nullopt;
    }

    return std::make_optional<BufferHandle>(shared_from_this(),
                                            std::move(*handle));
}

BufferHandle::BufferHandle(
    std::shared_ptr<ClientBufferAllocator> allocator,
    mooncake::offset_allocator::OffsetAllocationHandle handle)
    : allocator_(std::move(allocator)), handle_(std::move(handle)) {}

BufferHandle::~BufferHandle() {
    // The OffsetAllocationHandle destructor will automatically deallocate
    // No need to manually call deallocate
}

void* BufferHandle::ptr() const { return handle_.ptr(); }

size_t BufferHandle::size() const { return handle_.size(); }

// Utility functions for buffer and slice management
std::vector<Slice> split_into_slices(BufferHandle& handle) {
    std::vector<Slice> slices;
    auto base = static_cast<uint8_t*>(handle.ptr());
    size_t offset = 0;

    while (offset < handle.size()) {
        size_t chunk_size = std::min(handle.size() - offset, kMaxSliceSize);
        slices.push_back({base + offset, chunk_size});
        offset += chunk_size;
    }
    return slices;
}

uint64_t calculate_total_size(const Replica::Descriptor& replica) {
    uint64_t total_length = 0;
    if (replica.is_memory_replica() == false) {
        auto& disk_descriptor = replica.get_disk_descriptor();
        total_length = disk_descriptor.object_size;
    } else {
        for (auto& handle :
             replica.get_memory_descriptor().buffer_descriptors) {
            total_length += handle.size_;
        }
    }
    return total_length;
}

int allocateSlices(std::vector<Slice>& slices,
                   const Replica::Descriptor& replica,
                   BufferHandle& buffer_handle) {
    uint64_t offset = 0;
    if (replica.is_memory_replica() == false) {
        // For disk-based replica, split into slices based on file size
        uint64_t total_length = replica.get_disk_descriptor().object_size;
        while (offset < total_length) {
            auto chunk_size = std::min(total_length - offset, kMaxSliceSize);
            void* chunk_ptr = static_cast<char*>(buffer_handle.ptr()) + offset;
            slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }
    } else {
        // For memory-based replica, split into slices based on buffer
        // descriptors
        for (auto& handle :
             replica.get_memory_descriptor().buffer_descriptors) {
            void* chunk_ptr = static_cast<char*>(buffer_handle.ptr()) + offset;
            slices.emplace_back(Slice{chunk_ptr, handle.size_});
            offset += handle.size_;
        }
    }
    return 0;
}

}  // namespace mooncake