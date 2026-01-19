#pragma once

#include <optional>
#include <vector>
#include <string>

#include "offset_allocator/offset_allocator.hpp"
#include "types.h"
#include "replica.h"

namespace mooncake {

class BufferHandle;

/**
 * ClientBufferAllocator manages a contiguous memory buffer using an
 * offset-based allocation strategy. It provides thread-safe allocation and
 * automatic deallocation through RAII handles.
 *
 * This allocator is designed for client-side memory management where you need:
 * - Efficient sub-allocation within a larger buffer
 * - Automatic memory cleanup via RAII
 * - Thread-safe allocation operations
 * - Support for shared memory allocation
 */
class ClientBufferAllocator
    : public std::enable_shared_from_this<ClientBufferAllocator> {
   public:
    // Create for heap-allocated memory
    static std::shared_ptr<ClientBufferAllocator> create(
        size_t size, const std::string& protocol = "",
        bool use_hugepage = false);

    // Create for shared memory
    static std::shared_ptr<ClientBufferAllocator> create(
        void* addr, size_t size, const std::string& protocol = "");

    ~ClientBufferAllocator();

    // Disable copy constructor and copy assignment operator
    ClientBufferAllocator(const ClientBufferAllocator&) = delete;
    ClientBufferAllocator& operator=(const ClientBufferAllocator&) = delete;

    // Disable move constructor and move assignment operator
    ClientBufferAllocator(ClientBufferAllocator&&) = delete;
    ClientBufferAllocator& operator=(ClientBufferAllocator&&) = delete;

    [[nodiscard]] void* getBase() const { return buffer_; }

    [[nodiscard]] size_t size() const { return buffer_size_; }

    [[nodiscard]] std::optional<BufferHandle> allocate(size_t size);

   private:
    // Private constructors for different memory types
    ClientBufferAllocator(size_t size, const std::string& protocol,
                          bool use_hugepage);
    ClientBufferAllocator(void* addr, size_t size, const std::string& protocol);

    std::shared_ptr<offset_allocator::OffsetAllocator> allocator_;

    std::string protocol;
    void* buffer_;
    size_t buffer_size_;
    bool is_external_memory_ = false;
    bool use_hugepage_ = false;
};

/**
 * BufferHandle provides RAII management for allocated memory regions within
 * a ClientBufferAllocator. It automatically deallocates the memory when
 * destroyed and provides access to the allocated memory pointer and size.
 *
 * This class is move-only to ensure proper ownership semantics and prevent
 * accidental double-free errors.
 */
class BufferHandle {
   public:
    BufferHandle(std::shared_ptr<ClientBufferAllocator> allocator,
                 offset_allocator::OffsetAllocationHandle handle);
    ~BufferHandle();

    BufferHandle(BufferHandle&&) = default;  // Allow move operations
    BufferHandle& operator=(BufferHandle&&) = default;

    // Disable copy constructor and copy assignment operator
    BufferHandle(const BufferHandle&) = delete;
    BufferHandle& operator=(const BufferHandle&) = delete;

    [[nodiscard]] void* ptr() const;
    [[nodiscard]] size_t size() const;

   private:
    std::shared_ptr<ClientBufferAllocator> allocator_;
    offset_allocator::OffsetAllocationHandle handle_;
};

// Utility functions for buffer and slice management
/**
 * @brief Split a BufferHandle into slices of maximum size kMaxSliceSize
 * @param handle The buffer handle to split
 * @return Vector of slices covering the entire buffer
 */
std::vector<Slice> split_into_slices(BufferHandle& handle);

/**
 * @brief Calculate the total size of a replica descriptor
 * @param replica The replica descriptor to calculate size for
 * @return Total size in bytes
 */
uint64_t calculate_total_size(const Replica::Descriptor& replica);

/**
 * @brief Allocate slices from a buffer handle based on replica descriptor
 * @param slices Output vector to store the allocated slices
 * @param replica The replica descriptor defining the slice structure
 * @param buffer_ptr The buffer pointer to allocate slices from
 * @return 0 on success, non-zero on error
 */
int allocateSlices(std::vector<Slice>& slices,
                   const Replica::Descriptor& replica,
                   void* buffer_ptr);

}  // namespace mooncake