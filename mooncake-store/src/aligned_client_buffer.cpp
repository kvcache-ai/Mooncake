#include "aligned_client_buffer.hpp"

#include <glog/logging.h>
#include <sys/mman.h>
#include <cstdlib>
#include <cstring>
#include <string_view>

#include "utils.h"

namespace mooncake {
namespace {
constexpr std::string_view kAscendProtocol = "ascend";
constexpr std::string_view kUbshmemProtocol = "ubshmem";

bool UseProtocolAllocator(const std::string& protocol) {
    return protocol == kAscendProtocol || protocol == kUbshmemProtocol;
}

void FreeAlignedBuffer(void* buffer, size_t size, const std::string& protocol,
                       bool use_hugepage) {
    if (use_hugepage) {
        free_buffer_mmap_memory(buffer, size);
    } else if (UseProtocolAllocator(protocol)) {
        free_memory(protocol, buffer);
    } else {
        free(buffer);
    }
}

void* AllocateProtocolAlignedBuffer(size_t aligned_size,
                                    const std::string& protocol) {
    void* aligned_buffer = allocate_buffer_allocator_memory(
        aligned_size, protocol,
        AlignedClientBufferAllocator::kDirectIOAlignment);
    if (!aligned_buffer) {
        LOG(ERROR) << "AlignedClientBufferAllocator: failed to allocate "
                   << "protocol-aware memory of size " << aligned_size
                   << " for protocol " << protocol;
    }
    return aligned_buffer;
}

void* AllocateHugepageAlignedBuffer(size_t aligned_size) {
    void* aligned_buffer = allocate_buffer_mmap_memory(
        aligned_size, AlignedClientBufferAllocator::kDirectIOAlignment);
    if (!aligned_buffer) {
        LOG(ERROR) << "AlignedClientBufferAllocator: failed to allocate "
                   << "hugepage memory of size " << aligned_size;
    }
    return aligned_buffer;
}

void* AllocatePosixAlignedBuffer(size_t aligned_size) {
    void* aligned_buffer = nullptr;
    int ret = posix_memalign(&aligned_buffer,
                             AlignedClientBufferAllocator::kDirectIOAlignment,
                             aligned_size);
    if (ret != 0) {
        LOG(ERROR) << "AlignedClientBufferAllocator: posix_memalign failed "
                   << "with error " << ret << " (" << strerror(ret) << ")";
        return nullptr;
    }
    memset(aligned_buffer, 0, aligned_size);
    return aligned_buffer;
}

void* AllocateAlignedBuffer(size_t aligned_size, const std::string& protocol,
                            bool use_mmap_hugepage) {
    if (UseProtocolAllocator(protocol)) {
        return AllocateProtocolAlignedBuffer(aligned_size, protocol);
    }
    if (use_mmap_hugepage) {
        return AllocateHugepageAlignedBuffer(aligned_size);
    }
    return AllocatePosixAlignedBuffer(aligned_size);
}
}  // namespace

std::shared_ptr<AlignedClientBufferAllocator>
AlignedClientBufferAllocator::create(size_t size, const std::string& protocol,
                                     bool use_hugepage) {
    if (size == 0) {
        LOG(ERROR)
            << "AlignedClientBufferAllocator: size must be greater than 0";
        return nullptr;
    }

    // Align size up to kDirectIOAlignment
    size_t aligned_size = align_up(size, kDirectIOAlignment);

    const bool use_protocol_allocator = UseProtocolAllocator(protocol);
    const bool use_mmap_hugepage = use_hugepage && !use_protocol_allocator;
    void* aligned_buffer =
        AllocateAlignedBuffer(aligned_size, protocol, use_mmap_hugepage);
    if (!aligned_buffer) {
        return nullptr;
    }

    // Verify alignment
    if (reinterpret_cast<uintptr_t>(aligned_buffer) % kDirectIOAlignment != 0) {
        LOG(ERROR) << "AlignedClientBufferAllocator: allocated buffer is not "
                   << "aligned to " << kDirectIOAlignment << " bytes";
        FreeAlignedBuffer(aligned_buffer, aligned_size, protocol,
                          use_mmap_hugepage);
        return nullptr;
    }

    LOG(INFO) << "AlignedClientBufferAllocator: allocated " << aligned_size
              << " bytes at address " << aligned_buffer << " (aligned to "
              << kDirectIOAlignment << " bytes)";

    // Use custom deleter to properly free the aligned memory
    return std::shared_ptr<AlignedClientBufferAllocator>(
        new AlignedClientBufferAllocator(aligned_buffer, aligned_size, protocol,
                                         use_mmap_hugepage));
}

AlignedClientBufferAllocator::AlignedClientBufferAllocator(
    void* aligned_buffer, size_t size, const std::string& protocol,
    bool use_hugepage)
    : ClientBufferAllocator(aligned_buffer, size, protocol),
      owns_memory_(true),
      allocated_size_(size),
      protocol_(protocol) {
    // Store hugepage flag in parent class's use_hugepage_ member
    // We need this for proper cleanup
    use_hugepage_ = use_hugepage;
}

AlignedClientBufferAllocator::~AlignedClientBufferAllocator() {
    // Free the aligned memory we allocated
    // The parent class destructor will not free it because is_external_memory_
    // is set to true
    if (owns_memory_ && buffer_) {
        if (use_hugepage_) {
            LOG(INFO)
                << "AlignedClientBufferAllocator: freeing hugepage memory "
                << "at " << buffer_ << " (" << allocated_size_ << " bytes)";
            FreeAlignedBuffer(buffer_, allocated_size_, protocol_,
                              use_hugepage_);
        } else {
            LOG(INFO) << "AlignedClientBufferAllocator: freeing aligned memory "
                      << "at " << buffer_ << " (" << allocated_size_
                      << " bytes)";
            FreeAlignedBuffer(buffer_, allocated_size_, protocol_,
                              use_hugepage_);
        }
        buffer_ = nullptr;
    }
}

}  // namespace mooncake
