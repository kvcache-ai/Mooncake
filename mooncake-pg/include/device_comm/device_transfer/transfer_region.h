#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_REGION_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_REGION_H

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <utility>

#include "error_types.h"

namespace mooncake {

class DeviceTransferRegion;

// A stable subrange of one DeviceTransferRegion. Moving the slice transfers
// ownership; destroying it returns the range without freeing backing memory.
class RegionSlice {
   public:
    ~RegionSlice() noexcept;

    RegionSlice(const RegionSlice&) = delete;
    RegionSlice& operator=(const RegionSlice&) = delete;
    RegionSlice(RegionSlice&& other) noexcept;
    RegionSlice& operator=(RegionSlice&& other) noexcept;

    [[nodiscard]] void* addr() const noexcept;
    [[nodiscard]] uint64_t offset() const noexcept;
    [[nodiscard]] uint64_t size() const noexcept;

   private:
    friend class DeviceTransferRegion;

    RegionSlice(DeviceTransferRegion& owner, uint64_t offset,
                uint64_t size) noexcept;
    void release() noexcept;
    void moveFrom(RegionSlice&& other) noexcept;

    DeviceTransferRegion* owner_ = nullptr;
    uint64_t offset_ = 0;
    uint64_t size_ = 0;
};

// Owns one stable device-memory allocation and sub-allocates slices from it.
// The region must outlive its slices.
class DeviceTransferRegion {
   public:
    static PGResult<DeviceTransferRegion> create(int device_index, size_t size);

    // Allocator is a byte-oriented allocation policy providing allocate(bytes)
    // and deallocate(ptr, bytes).
    template <typename Allocator>
    static PGResult<DeviceTransferRegion> create(int device_index, size_t size,
                                                 Allocator allocator) {
        auto state = std::make_shared<Allocator>(std::move(allocator));
        return createWithAllocator(
            device_index, size,
            [state](size_t bytes) { return state->allocate(bytes); },
            [state](void* ptr, size_t bytes) {
                state->deallocate(ptr, bytes);
            });
    }

    ~DeviceTransferRegion() noexcept;

    DeviceTransferRegion(const DeviceTransferRegion&) = delete;
    DeviceTransferRegion& operator=(const DeviceTransferRegion&) = delete;
    DeviceTransferRegion& operator=(DeviceTransferRegion&&) = delete;
    DeviceTransferRegion(DeviceTransferRegion&& other) noexcept;

    PGResult<RegionSlice> allocate(size_t size, size_t alignment);
    PGResult<void> release();

    [[nodiscard]] void* addr() const noexcept;
    [[nodiscard]] size_t size() const noexcept;

   private:
    friend class RegionSlice;

    using Allocate = std::function<void*(size_t)>;
    using Deallocate = std::function<void(void*, size_t)>;

    static PGResult<DeviceTransferRegion> createWithAllocator(
        int device_index, size_t size, Allocate allocate,
        Deallocate deallocate);

    DeviceTransferRegion(int device_index, size_t size) noexcept;

    void releaseSlice(uint64_t offset) noexcept;

    int device_index_ = -1;
    void* addr_ = nullptr;
    size_t size_ = 0;
    Deallocate deallocate_;

    mutable std::mutex mutex_;
    std::map<uint64_t, uint64_t> free_ranges_;
    std::map<uint64_t, uint64_t> allocations_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_TRANSFER_TRANSFER_REGION_H
