#include "device_comm/device_arena.h"

#include <iterator>
#include <optional>
#include <utility>

#include <glog/logging.h>

#include "pg_utils.h"

namespace mooncake {
namespace {

std::optional<uint64_t> alignUp(uint64_t value, uint64_t alignment) {
    if (alignment == 0) return std::nullopt;
    const auto increment = alignmentPadding(value, alignment);
    if (addOverflows(value, increment)) return std::nullopt;
    return value + increment;
}

}  // namespace

DeviceArenaSlice::DeviceArenaSlice(DeviceArena& owner, uint64_t offset,
                                   uint64_t size) noexcept
    : owner_(&owner), offset_(offset), size_(size) {}

DeviceArenaSlice::~DeviceArenaSlice() noexcept { release(); }

DeviceArenaSlice::DeviceArenaSlice(DeviceArenaSlice&& other) noexcept {
    moveFrom(std::move(other));
}

DeviceArenaSlice& DeviceArenaSlice::operator=(
    DeviceArenaSlice&& other) noexcept {
    if (this != &other) {
        release();
        moveFrom(std::move(other));
    }
    return *this;
}

void* DeviceArenaSlice::addr() const noexcept {
    return static_cast<char*>(owner_->base_) + offset_;
}

uint64_t DeviceArenaSlice::offset() const noexcept { return offset_; }

uint64_t DeviceArenaSlice::size() const noexcept { return size_; }

void DeviceArenaSlice::release() noexcept {
    if (owner_) owner_->release(offset_);
    owner_ = nullptr;
    offset_ = 0;
    size_ = 0;
}

void DeviceArenaSlice::moveFrom(DeviceArenaSlice&& other) noexcept {
    owner_ = std::exchange(other.owner_, nullptr);
    offset_ = std::exchange(other.offset_, 0);
    size_ = std::exchange(other.size_, 0);
}

std::unique_ptr<DeviceArena> DeviceArena::create(int device_index, void* base,
                                                 size_t arena_size) {
    PG_ASSERT(device_index >= 0, "invalid DeviceArena CUDA device");
    PG_ASSERT(base, "DeviceArena base address is null");
    PG_ASSERT(arena_size != 0, "DeviceArena size must be positive");
    PG_ASSERT(!addOverflows(reinterpret_cast<uintptr_t>(base), arena_size),
              "DeviceArena address range overflows");

    auto arena = std::unique_ptr<DeviceArena>(
        new DeviceArena(device_index, base, arena_size));
    return arena;
}

DeviceArena::DeviceArena(int device_index, void* base,
                         size_t arena_size) noexcept
    : device_index_(device_index), arena_size_(arena_size), base_(base) {
    free_ranges_.emplace(0, arena_size);
}

DeviceArena::~DeviceArena() noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!allocations_.empty()) {
        // A slice stores its owning Arena address. Continuing destruction here
        // would turn every remaining slice into a dangling owner pointer.
        LOG(ERROR) << "DeviceArena destroyed with live slices";
    }
}

PGResult<DeviceArenaSlice> DeviceArena::allocate(size_t size,
                                                 size_t alignment) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(base_, "DeviceArena is closed");
    PG_VALIDATE_ARG(size != 0, "DeviceArena slice is empty");
    PG_VALIDATE_ARG(alignment != 0, "DeviceArena alignment is zero");

    const uint64_t base_address = reinterpret_cast<uintptr_t>(base_);

    for (auto current = free_ranges_.begin(); current != free_ranges_.end();
         ++current) {
        const uint64_t range_begin_offset = current->first;
        const uint64_t range_size = current->second;

        // Align the actual device address. Relative offsets may consequently
        // differ between ranks with different base addresses.
        const auto allocation_address =
            alignUp(base_address + range_begin_offset, alignment);
        if (!allocation_address) continue;
        const uint64_t allocation_begin_offset =
            *allocation_address - base_address;
        const uint64_t prefix = allocation_begin_offset - range_begin_offset;
        if (prefix > range_size || size > range_size - prefix) continue;

        const uint64_t allocation_end_offset =
            allocation_begin_offset + static_cast<uint64_t>(size);
        const uint64_t range_end_offset = range_begin_offset + range_size;
        free_ranges_.erase(current);
        if (prefix != 0) {
            free_ranges_.emplace(range_begin_offset, prefix);
        }
        if (allocation_end_offset != range_end_offset) {
            free_ranges_.emplace(allocation_end_offset,
                                 range_end_offset - allocation_end_offset);
        }
        allocations_.emplace(allocation_begin_offset, size);
        return DeviceArenaSlice(*this, allocation_begin_offset, size);
    }
    return makePGError(PGErrorCode::ResourceBusy,
                       "DeviceArena has no sufficiently large free range");
}

int DeviceArena::deviceIndex() const noexcept { return device_index_; }

void DeviceArena::release(uint64_t offset) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto allocation = allocations_.find(offset);
    if (allocation == allocations_.end()) {
        LOG(ERROR) << "DeviceArena received an unknown slice offset " << offset;
        return;
    }

    uint64_t begin = allocation->first;
    uint64_t size = allocation->second;
    allocations_.erase(allocation);

    auto next = free_ranges_.lower_bound(begin);
    if (next != free_ranges_.begin()) {
        auto previous = std::prev(next);
        if (previous->first + previous->second == begin) {
            begin = previous->first;
            size += previous->second;
            free_ranges_.erase(previous);
        }
    }
    next = free_ranges_.lower_bound(begin);
    if (next != free_ranges_.end() && begin + size == next->first) {
        size += next->second;
        free_ranges_.erase(next);
    }
    free_ranges_.emplace(begin, size);
}

}  // namespace mooncake
