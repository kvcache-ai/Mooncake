#pragma once

#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <vector>
#include <string>

#include "offset_allocator/offset_allocator.h"
#include "types.h"
#include "replica.h"
#include "nof/dma_buffer_allocator.h"

namespace mooncake {

class BufferHandle;

/**
 * Signalled whenever an allocation owned by a ClientBufferAllocator returns
 * capacity to that allocator. Waiters park on this object, so it must outlive
 * both the waiter and the handles; the allocator and every handle hold it via
 * shared_ptr and it never points back at a caller that may already be gone.
 */
struct ClientBufferReleaseSignal {
    std::mutex mu;
    std::condition_variable cv;
    uint64_t generation = 0;

    // Bump the generation and wake every waiter. Capacity has actually been
    // returned at this point; calling this earlier lets a waiter sleep through
    // space that is already free.
    void NotifyReleased() {
        {
            std::lock_guard<std::mutex> lk(mu);
            ++generation;
        }
        cv.notify_all();
    }
};

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
        bool use_hugepage = false,
        std::shared_ptr<DmaBufferAllocator> dma_allocator = nullptr);

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

    // Install the shared release signal waiters park on. Only the allocator a
    // waiter actually waits against needs one; others leave it null.
    void SetReleaseSignal(std::shared_ptr<ClientBufferReleaseSignal> signal) {
        release_signal_ = std::move(signal);
    }

    [[nodiscard]] std::shared_ptr<ClientBufferReleaseSignal> GetReleaseSignal()
        const {
        return release_signal_;
    }

    // Called once an allocation has returned to this allocator. Notifies only
    // when a signal is installed, so pinned/one-off allocators stay silent.
    void NotifyReleased() {
        if (release_signal_) {
            release_signal_->NotifyReleased();
        }
    }

   protected:
    // Constructors accessible to derived classes
    ClientBufferAllocator(void* addr, size_t size, const std::string& protocol);

    void* buffer_;
    size_t buffer_size_;
    bool use_hugepage_ = false;

   private:
    ClientBufferAllocator(size_t size, const std::string& protocol,
                          bool use_hugepage,
                          std::shared_ptr<DmaBufferAllocator> dma_allocator);

    std::shared_ptr<offset_allocator::OffsetAllocator> allocator_;

    std::string protocol;
    bool is_external_memory_ = false;
    std::shared_ptr<DmaBufferAllocator> dma_allocator_;  // was use_spdk_dma_
    // Size of the THP mapping backing an RDMA buffer (see
    // allocate_buffer_thp()); 0 when the buffer is not THP backed.
    size_t thp_mapped_size_ = 0;
    std::shared_ptr<ClientBufferReleaseSignal> release_signal_;
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
    // Owning mode: backed by allocator
    BufferHandle(std::shared_ptr<ClientBufferAllocator> allocator,
                 offset_allocator::OffsetAllocationHandle handle);

    // View mode: non-owning, calls release_fn on destruction
    BufferHandle(void* ptr, size_t size, std::function<void()> release_fn);

    ~BufferHandle();

    BufferHandle(BufferHandle&&) = default;  // Allow move operations
    BufferHandle& operator=(BufferHandle&& other) noexcept;

    // Disable copy constructor and copy assignment operator
    BufferHandle(const BufferHandle&) = delete;
    BufferHandle& operator=(const BufferHandle&) = delete;

    [[nodiscard]] void* ptr() const;
    [[nodiscard]] size_t size() const;

   private:
    // Returns whatever this handle holds. An owned allocation is freed and,
    // when it backed a signal-equipped allocator, wakes arena waiters once the
    // capacity is back. A view handle only drops its alias through release_fn;
    // the owner that still holds the memory is what signals the real return.
    void Release() noexcept;

    // Owning mode members
    std::shared_ptr<ClientBufferAllocator> allocator_;
    offset_allocator::OffsetAllocationHandle handle_;

    // View mode members
    void* view_ptr_ = nullptr;
    size_t view_size_ = 0;
    std::function<void()> release_fn_;
};

// Utility functions for buffer and slice management
/**
 * @brief Split a BufferHandle into slices of maximum size kMaxSliceSize
 * @param handle The buffer handle to split
 * @return Vector of slices covering the entire buffer
 */
std::vector<Slice> split_into_slices(BufferHandle& handle);

/**
 * @brief Split a buffer into slices of maximum kMaxSliceSize
 * @param buffer The buffer buffer to split
 * @param length The length of the buffer to split
 * @return Vector of slices covering the entire buffer
 */
std::vector<Slice> split_into_slices(void* buffer, size_t length);

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
                   const Replica::Descriptor& replica, void* buffer_ptr);

}  // namespace mooncake
