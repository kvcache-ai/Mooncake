#include "client_buffer.hpp"

#include <cstdlib>
#include <stdexcept>

namespace mooncake {

ClientBufferAllocator::ClientBufferAllocator(size_t size) : buffer_size_(size) {
    // Align to 64 bytes(cache line size) for better cache performance
    constexpr size_t alignment = 64;
    buffer_ = std::aligned_alloc(alignment, size);
    if (!buffer_) {
        throw std::bad_alloc();
    }

    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_), size);
}

ClientBufferAllocator::~ClientBufferAllocator() {
    // Free the aligned allocated memory
    if (buffer_) {
        std::free(buffer_);
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

}  // namespace mooncake