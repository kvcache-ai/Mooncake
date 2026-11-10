#include "device_comm/device_transfer/transfer_region.h"

#include <iterator>
#include <limits>
#include <optional>
#include <utility>

#include <glog/logging.h>

#include "gpu_runtime.h"
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

RegionSlice::RegionSlice(DeviceTransferRegion& owner, uint64_t offset,
                         uint64_t size) noexcept
    : owner_(&owner), offset_(offset), size_(size) {}

RegionSlice::~RegionSlice() noexcept { release(); }

RegionSlice::RegionSlice(RegionSlice&& other) noexcept {
    moveFrom(std::move(other));
}

RegionSlice& RegionSlice::operator=(RegionSlice&& other) noexcept {
    if (this != &other) {
        release();
        moveFrom(std::move(other));
    }
    return *this;
}

void* RegionSlice::addr() const noexcept {
    return static_cast<char*>(owner_->addr()) + offset_;
}

uint64_t RegionSlice::offset() const noexcept { return offset_; }

uint64_t RegionSlice::size() const noexcept { return size_; }

void RegionSlice::release() noexcept {
    if (owner_) owner_->releaseSlice(offset_);
    owner_ = nullptr;
    offset_ = 0;
    size_ = 0;
}

void RegionSlice::moveFrom(RegionSlice&& other) noexcept {
    owner_ = std::exchange(other.owner_, nullptr);
    offset_ = std::exchange(other.offset_, 0);
    size_ = std::exchange(other.size_, 0);
}

PGResult<DeviceTransferRegion> DeviceTransferRegion::create(int device_index,
                                                            size_t size) {
    PG_VALIDATE_ARG(size != 0, "device transfer region is empty");
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index));
    DeviceTransferRegion region(device_index);
    PG_TRY(region.allocateBacking(size));
    PG_ASSERT(
        !addOverflows(reinterpret_cast<uintptr_t>(region.addr_), region.size_),
        "DeviceTransferRegion address range overflows");
    region.free_ranges_.emplace(0, region.size_);
    return region;
}

DeviceTransferRegion::DeviceTransferRegion(int device_index) noexcept
    : device_index_(device_index) {}

DeviceTransferRegion::~DeviceTransferRegion() noexcept {
    auto result = release();
    if (!result.has_value()) {
        LOG(ERROR) << "Failed to release device transfer region: "
                   << result.error().message;
    }
}

DeviceTransferRegion::DeviceTransferRegion(
    DeviceTransferRegion&& other) noexcept
    : device_index_(std::exchange(other.device_index_, -1)),
      addr_(std::exchange(other.addr_, nullptr)),
      size_(std::exchange(other.size_, 0)),
      handle_(std::exchange(other.handle_, 0)),
      mapped_(std::exchange(other.mapped_, false)),
      free_ranges_(std::move(other.free_ranges_)),
      allocations_(std::move(other.allocations_)) {
    PG_ASSERT(allocations_.empty(),
              "cannot move DeviceTransferRegion with live slices");
}

PGResult<void> DeviceTransferRegion::allocateBacking(size_t bytes) {
#if CUDA_VERSION >= 12030
    // FABRIC VMM lets TE P2P and NCCL share the same allocation.
    int fabric_supported = 0;
    cuDeviceGetAttribute(&fabric_supported,
                         CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
                         device_index_);
    if (fabric_supported) {
        CUmemAllocationProp prop{};
        prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
        prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        prop.location.id = device_index_;
        prop.requestedHandleTypes = static_cast<CUmemAllocationHandleType>(
            CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR |
            CU_MEM_HANDLE_TYPE_FABRIC);
        int rdma_supported = 0;
        PG_TRY_CU(cuDeviceGetAttribute(
            &rdma_supported,
            CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
            device_index_));
        prop.allocFlags.gpuDirectRDMACapable = rdma_supported != 0;

        // Match NCCL's user-buffer requirements without depending on libnccl.
        size_t granularity = 0;
        PG_TRY_CU(cuMemGetAllocationGranularity(
            &granularity, &prop, CU_MEM_ALLOC_GRANULARITY_RECOMMENDED));
        PG_VALIDATE_STATE(granularity != 0,
                          "VMM allocation granularity is zero");
        const size_t padding =
            (granularity - bytes % granularity) % granularity;
        PG_VALIDATE_ARG(bytes <= std::numeric_limits<size_t>::max() - padding,
                        "device region size overflows VMM alignment");
        size_ = bytes + padding;

        const auto result = cuMemCreate(&handle_, size_, &prop, 0);
        if (result != CUDA_ERROR_NOT_PERMITTED &&
            result != CUDA_ERROR_NOT_SUPPORTED) {
            PG_TRY_CU(result);
            CUdeviceptr address = 0;
            PG_TRY_CU(cuMemAddressReserve(&address, size_, granularity, 0, 0));
            addr_ = reinterpret_cast<void*>(address);
            PG_TRY_CU(cuMemMap(address, size_, 0, handle_, 0));
            mapped_ = true;
            CUmemAccessDesc access{};
            access.location = prop.location;
            access.flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
            PG_TRY_CU(cuMemSetAccess(address, size_, &access, 1));
            return {};
        }
    }
#endif
    // Without FABRIC, TE P2P needs legacy CUDA IPC: POSIX FD import is not
    // implemented. Prefer keeping P2P available when both routes cannot share
    // one allocation.
    PG_TRY_CUDA(cudaMalloc(&addr_, bytes));
    size_ = bytes;
    return {};
}

PGResult<RegionSlice> DeviceTransferRegion::allocate(size_t size,
                                                     size_t alignment) {
    std::lock_guard<std::mutex> lock(mutex_);
    PG_VALIDATE_STATE(addr(), "DeviceTransferRegion is closed");
    PG_VALIDATE_ARG(size != 0, "DeviceTransferRegion slice is empty");
    PG_VALIDATE_ARG(alignment != 0, "DeviceTransferRegion alignment is zero");

    const uint64_t base_address = reinterpret_cast<uintptr_t>(addr());

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
        return RegionSlice(*this, allocation_begin_offset, size);
    }
    return makePGError(
        PGErrorCode::ResourceBusy,
        "DeviceTransferRegion has no sufficiently large free range");
}

PGResult<void> DeviceTransferRegion::release() {
    if (!addr_ && !handle_) return {};
    PG_TRY(auto device_guard, GpuDeviceGuard::create(device_index_));

    {
        std::lock_guard<std::mutex> lock(mutex_);
        // A slice stores its owning region address. Freeing the backing
        // allocation while a slice is alive would leave it dangling.
        PG_VALIDATE_STATE(allocations_.empty(),
                          "DeviceTransferRegion still has live slices");
        free_ranges_.clear();
    }

    if (!handle_) {
        PG_TRY_CUDA(cudaFree(addr_));
        addr_ = nullptr;
    } else {
        if (mapped_) {
            PG_TRY_CU(cuMemUnmap(reinterpret_cast<CUdeviceptr>(addr_), size_));
            mapped_ = false;
        }
        if (addr_) {
            PG_TRY_CU(
                cuMemAddressFree(reinterpret_cast<CUdeviceptr>(addr_), size_));
            addr_ = nullptr;
        }
        PG_TRY_CU(cuMemRelease(handle_));
        handle_ = 0;
    }
    size_ = 0;
    return {};
}

void* DeviceTransferRegion::addr() const noexcept { return addr_; }

size_t DeviceTransferRegion::size() const noexcept { return size_; }

void DeviceTransferRegion::releaseSlice(uint64_t offset) noexcept {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto allocation = allocations_.find(offset);
    if (allocation == allocations_.end()) {
        LOG(ERROR) << "DeviceTransferRegion received an unknown slice offset "
                   << offset;
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
