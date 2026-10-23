#include "client_buffer.hpp"

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <vector>
#include <sys/mman.h>  // For shm_open, mmap, munmap
#include <sys/stat.h>  // For S_IRUSR, S_IWUSR
#include <fcntl.h>     // For O_CREAT, O_RDWR
#include <unistd.h>    // For ftruncate, close, shm_unlink

#include "utils.h"

namespace mooncake {

std::shared_ptr<ClientBufferAllocator> ClientBufferAllocator::create(
    size_t size, const std::string& protocol, bool use_hugepage) {
    return std::shared_ptr<ClientBufferAllocator>(
        new ClientBufferAllocator(size, protocol, use_hugepage));
}

tl::expected<std::shared_ptr<ClientBufferAllocator>, ErrorCode>
ClientBufferAllocator::create(void* addr, size_t size,
                             const std::string& protocol) {
    try {
        return std::shared_ptr<ClientBufferAllocator>(
            new ClientBufferAllocator(addr, size, protocol));
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to create external buffer allocator: " << e.what();
        return tl::unexpected(ErrorCode::INTERNAL_ERROR);
    }
}

ClientBufferAllocator::ClientBufferAllocator(size_t size,
                                             const std::string& protocol,
                                             bool use_hugepage)
    : protocol(protocol), buffer_size_(size), use_hugepage_(use_hugepage) {
    if (size == 0) {
        buffer_ = nullptr;
        allocator_ = nullptr;
        return;
    }
    // Align to 64 bytes(cache line size) for better cache performance
    constexpr size_t alignment = 64;
    if (use_hugepage_) {
        buffer_ = allocate_buffer_mmap_memory(size, alignment);
    } else {
        buffer_ = allocate_buffer_allocator_memory(size, protocol, alignment);
    }
    if (!buffer_) {
        throw std::bad_alloc();
    }

    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_), size);
}

ClientBufferAllocator::ClientBufferAllocator(void* addr, size_t size,
                                             const std::string& protocol)
    : protocol(protocol), buffer_size_(size) {
    buffer_ = addr;
    is_external_memory_ = true;
    allocator_ = mooncake::offset_allocator::OffsetAllocator::create(
        reinterpret_cast<uint64_t>(buffer_), size);
}

ClientBufferAllocator::~ClientBufferAllocator() {
    // Free the aligned allocated memory or unmap shared memory
    if (!is_external_memory_ && buffer_) {
        if (use_hugepage_) {
            free_buffer_mmap_memory(buffer_, buffer_size_);
        } else {
            free_memory(protocol, buffer_);
        }
    }
}

std::optional<BufferHandle> ClientBufferAllocator::allocate(size_t size) {
    if (allocator_ == nullptr) {
        return std::nullopt;
    }
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
size_t CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

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

}  // namespace mooncake