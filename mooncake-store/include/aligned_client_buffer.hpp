#pragma once

#include "client_buffer.hpp"

namespace mooncake {

/**
 * AlignedClientBufferAllocator extends ClientBufferAllocator to provide
 * 4096-byte aligned memory allocation, which is required for O_DIRECT I/O
 * operations with io_uring.
 *
 * This allocator ensures:
 * - Base memory address is aligned to 4096 bytes (page size)
 * - Can be registered with UringFile for zero-copy I/O
 * - Maintains all features of the parent ClientBufferAllocator
 */
class AlignedClientBufferAllocator : public ClientBufferAllocator {
   public:
    // Alignment requirement for O_DIRECT I/O
    static constexpr size_t kDirectIOAlignment = 4096;

    /**
     * Create an AlignedClientBufferAllocator with aligned memory
     * @param size Total size of the buffer to allocate
     * @param protocol Optional protocol string (unused, for compatibility)
     * @param use_hugepage Whether to use huge pages for allocation
     * @return Shared pointer to the allocator, or nullptr on failure
     */
    static std::shared_ptr<AlignedClientBufferAllocator> create(
        size_t size, const std::string& protocol = "",
        bool use_hugepage = false);

    /**
     * Get the base pointer of the aligned buffer
     * @return Base address of the allocated buffer
     */
    [[nodiscard]] void* get_base_pointer() const { return getBase(); }

    /**
     * Get the total size of the aligned buffer
     * @return Total size in bytes
     */
    [[nodiscard]] size_t get_total_size() const { return size(); }

    /**
     * Destructor - properly frees the aligned memory
     */
    ~AlignedClientBufferAllocator();

   private:
    // Private constructor - use create() factory method
    AlignedClientBufferAllocator(void* aligned_buffer, size_t size,
                                  const std::string& protocol,
                                  bool use_hugepage);

    // Store whether we own the memory (for cleanup)
    bool owns_memory_;
    size_t allocated_size_;  // Store the actual allocated size for cleanup
};

}  // namespace mooncake
