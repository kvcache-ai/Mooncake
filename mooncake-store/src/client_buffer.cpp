#include "client_buffer.hpp"

#include <algorithm>
#include <cstdlib>
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

std::shared_ptr<ClientBufferAllocator> ClientBufferAllocator::create(
    void* addr, size_t size, const std::string& protocol) {
    return std::shared_ptr<ClientBufferAllocator>(
        new ClientBufferAllocator(addr, size, protocol));
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
std::vector<Slice> split_into_slices(BufferHandle& handle) {
    auto base = static_cast<uint8_t*>(handle.ptr());
    auto length = handle.size();
    return split_into_slices(base, length);
}

std::vector<Slice> split_into_slices(void* buffer, size_t length) {
    std::vector<Slice> slices;
    auto base = static_cast<uint8_t*>(buffer);
    size_t offset = 0;

    while (offset < length) {
        size_t chunk_size = std::min(length - offset, kMaxSliceSize);
        slices.push_back({base + offset, chunk_size});
        offset += chunk_size;
    }
    return slices;
}

uint64_t calculate_total_size(const Replica::Descriptor& replica) {
    uint64_t total_length = 0;
    if (replica.is_disk_replica()) {
        auto& disk_descriptor = replica.get_disk_descriptor();
        total_length = disk_descriptor.object_size;
    } else if (replica.is_local_disk_replica()) {
        total_length = replica.get_local_disk_descriptor().object_size;
    } else {
        total_length = replica.get_memory_descriptor().buffer_descriptor.size_;
    }
    return total_length;
}

int allocateSlices(std::vector<Slice>& slices,
                   const Replica::Descriptor& replica, void* buffer_ptr) {
    if (replica.is_disk_replica()) {
        // For disk-based replica, split into slices based on file size
        uint64_t offset = 0;
        uint64_t total_length = replica.get_disk_descriptor().object_size;
        while (offset < total_length) {
            auto chunk_size = std::min(total_length - offset, kMaxSliceSize);
            void* chunk_ptr = static_cast<char*>(buffer_ptr) + offset;
            slices.emplace_back(Slice{chunk_ptr, chunk_size});
            offset += chunk_size;
        }
    } else if (replica.is_local_disk_replica()) {
        slices.emplace_back(
            Slice{buffer_ptr, replica.get_local_disk_descriptor().object_size});
    } else {
        // For memory-based replica, split into slices based on buffer
        // descriptors
        auto& handle = replica.get_memory_descriptor().buffer_descriptor;
        void* chunk_ptr = buffer_ptr;
        slices.emplace_back(Slice{chunk_ptr, handle.size_});
    }
    return 0;
}

}  // namespace mooncake
