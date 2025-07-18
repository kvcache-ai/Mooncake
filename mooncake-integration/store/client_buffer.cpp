#include "client_buffer.hpp"

namespace mooncake {

ClientBufferAllocator::ClientBufferAllocator(size_t size)
    : buffer_(std::make_unique<char[]>(size)) {
    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_.get()), size);
}

ClientBufferAllocator::~ClientBufferAllocator() {
    // allocator_ and buffer_ will be automatically cleaned up by the
    // OffsetAllocator destructor
}

std::optional<BufferHandle> ClientBufferAllocator::allocate(size_t size) {
    auto handle = allocator_->allocate(size);
    if (!handle) {
        return std::nullopt;
    }

    void* ptr = reinterpret_cast<void*>(handle->address());
    return std::make_optional<BufferHandle>(shared_from_this(),
                                            std::move(*handle), ptr, size);
}

BufferHandle::BufferHandle(
    std::shared_ptr<ClientBufferAllocator> allocator,
    mooncake::offset_allocator::OffsetAllocationHandle handle, void* ptr,
    size_t size)
    : allocator_(std::move(allocator)),
      handle_(std::move(handle)),
      ptr_(ptr),
      size_(size) {}

BufferHandle::~BufferHandle() {
    // The OffsetAllocationHandle destructor will automatically deallocate
    // No need to manually call deallocate
}

void* BufferHandle::ptr() const { return ptr_; }

size_t BufferHandle::size() const { return size_; }

}  // namespace mooncake