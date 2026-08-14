#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_ARENA_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_ARENA_H

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>

#include "error_types.h"

namespace mooncake {

// Default size of the transfer service's registered device-memory region.
inline constexpr size_t kDefaultDeviceArenaSize = 256ull << 20;  // 256 MiB

class DeviceArena;

// A stable subrange of one DeviceArena. Moving the slice transfers ownership;
// destroying it returns the range to the arena without freeing device memory.
class DeviceArenaSlice {
   public:
    ~DeviceArenaSlice() noexcept;

    DeviceArenaSlice(const DeviceArenaSlice&) = delete;
    DeviceArenaSlice& operator=(const DeviceArenaSlice&) = delete;
    DeviceArenaSlice(DeviceArenaSlice&& other) noexcept;
    DeviceArenaSlice& operator=(DeviceArenaSlice&& other) noexcept;

    [[nodiscard]] void* addr() const noexcept;
    [[nodiscard]] uint64_t offset() const noexcept;
    [[nodiscard]] uint64_t size() const noexcept;

   private:
    friend class DeviceArena;

    DeviceArenaSlice(DeviceArena& owner, uint64_t offset,
                     uint64_t size) noexcept;
    void release() noexcept;
    void moveFrom(DeviceArenaSlice&& other) noexcept;

    DeviceArena* owner_ = nullptr;
    uint64_t offset_ = 0;
    uint64_t size_ = 0;
};

// Non-owning sub-allocation over one stable device-memory region. The region
// owner must outlive the arena and all of its slices.
class DeviceArena {
   public:
    static std::unique_ptr<DeviceArena> create(int device_index, void* base,
                                               size_t arena_size);

    ~DeviceArena() noexcept;

    DeviceArena(const DeviceArena&) = delete;
    DeviceArena& operator=(const DeviceArena&) = delete;

    PGResult<DeviceArenaSlice> allocate(size_t size, size_t alignment);

    [[nodiscard]] int deviceIndex() const noexcept;

   private:
    friend class DeviceArenaSlice;

    DeviceArena(int device_index, void* base, size_t arena_size) noexcept;

    void release(uint64_t offset) noexcept;
    int device_index_ = -1;
    size_t arena_size_ = 0;
    void* base_ = nullptr;

    mutable std::mutex mutex_;
    std::map<uint64_t, uint64_t> free_ranges_;
    std::map<uint64_t, uint64_t> allocations_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_ARENA_H
