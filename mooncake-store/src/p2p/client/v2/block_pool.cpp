#include "p2p/client/v2/block_pool.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <mutex>
#include <new>
#include <unordered_map>
#include <utility>

#include <fcntl.h>
#include <glog/logging.h>
#include <numa.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>

#include "offset_allocator/offset_allocator.hpp"
#include "transfer_engine.h"
#include "utils.h"

namespace mooncake::v2 {
namespace {

constexpr const char* kWildcardLocation = "*";

bool IsPowerOfTwo(size_t value) {
    return value != 0 && (value & (value - 1)) == 0;
}

size_t AlignUp(size_t value, size_t alignment) {
    return (value + alignment - 1) & ~(alignment - 1);
}

/**
 * @class DramBlockDataHandle
 * @brief One DRAM extent. The offset-allocator handle it owns is the RAII
 *        token for the space, so destroying the handle returns it.
 */
class DramBlockDataHandle final : public BlockDataHandle {
   public:
    DramBlockDataHandle(offset_allocator::OffsetAllocationHandle handle,
                        char* aligned_base, size_t usable_size,
                        size_t charged_size, bool te_registered)
        : handle_(std::move(handle)),
          base_(aligned_base),
          size_(usable_size),
          charged_size_(charged_size),
          te_registered_(te_registered) {}

    bool TeRegistered() const override { return te_registered_; }

    size_t Size() const override { return size_; }

    /**
     * @brief Bytes this block took from the arena, including the alignment
     *        padding. Free must credit back exactly this, not a figure
     *        recomputed from Size() and some other alignment, or the usage
     *        counter drifts once per allocate/free cycle.
     */
    size_t ChargedSize() const { return charged_size_; }

    tl::expected<void, ErrorCode> Read(
        size_t offset, std::span<std::byte> dst) const override {
        auto checked = CheckRange(offset, dst.size());
        if (!checked) return checked;
        if (!dst.empty()) {
            std::memcpy(dst.data(), base_ + offset, dst.size());
        }
        return {};
    }

    tl::expected<void, ErrorCode> Write(
        size_t offset, std::span<const std::byte> src) override {
        auto checked = CheckRange(offset, src.size());
        if (!checked) return checked;
        if (!src.empty()) {
            std::memcpy(base_ + offset, src.data(), src.size());
        }
        return {};
    }

    /** DRAM is already durable for our purposes. */
    tl::expected<void, ErrorCode> Commit() override { return {}; }

    std::optional<TransferAddress> GetTransferAddress() const override {
        // DRAM always has one. Whether the arena was registered with a
        // TransferEngine is a deployment fact reported by the pool's
        // capabilities and warned about at Init, not a per-block property:
        // routing a write away from DRAM because the process happens to lack
        // an engine would silently change where data lands.
        TransferAddress address;
        address.addr = reinterpret_cast<uintptr_t>(base_);
        address.size = size_;
        return address;
    }

   private:
    tl::expected<void, ErrorCode> CheckRange(size_t offset,
                                             size_t length) const {
        // Overflow first: offset + length can wrap before the bound check.
        if (offset > size_ || length > size_ - offset) {
            LOG(ERROR) << "DRAM block access out of range, offset=" << offset
                       << ", length=" << length << ", size=" << size_;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return {};
    }

    offset_allocator::OffsetAllocationHandle handle_;
    char* base_ = nullptr;
    size_t size_ = 0;
    size_t charged_size_ = 0;
    bool te_registered_ = false;
};

/**
 * @struct DramArena
 * @brief One contiguous region with its own allocator and handle table.
 *        Sharding the handle table per arena means Allocate/Free on different
 *        arenas never contend.
 */
struct DramArena {
    std::optional<int> numa_node;
    size_t capacity = 0;
    size_t alignment = 64;
    std::unique_ptr<char[], std::function<void(char*)>> memory;
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator;
    bool te_registered = false;

    mutable std::mutex mu;
    std::unordered_map<uint64_t, std::unique_ptr<DramBlockDataHandle>> handles;
    uint64_t next_local_id = 1;
    size_t used_bytes = 0;  // padded bytes actually taken from the allocator
};

/**
 * @class DramBlockPoolState
 * @brief Owns every arena and every handle. Shared by the pool facade and by
 *        each outstanding BlockAllocation, so an allocation handed out before
 *        shutdown stays valid until its last holder drops it.
 */
class DramBlockPoolState final : public BlockPoolState {
   public:
    DramBlockPoolState(UUID pool_id,
                       std::shared_ptr<TransferEngine> transfer_engine)
        : pool_id_(pool_id), transfer_engine_(std::move(transfer_engine)) {}

    ~DramBlockPoolState() override {
        for (auto& arena : arenas_) {
            if (arena->te_registered && transfer_engine_) {
                transfer_engine_->unregisterLocalMemory(arena->memory.get());
            }
        }
    }

    tl::expected<void, ErrorCode> Init(const DramBlockPoolConfig& config) {
        if (config.arenas.empty()) {
            LOG(ERROR) << "DramBlockPool requires at least one arena";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        for (const auto& arena_config : config.arenas) {
            auto arena = CreateArena(arena_config);
            if (!arena) return tl::make_unexpected(arena.error());
            arenas_.push_back(std::move(arena.value()));
        }
        // Arenas may differ; the pool reports the strictest of them, which is
        // the only alignment it can promise for an allocation it may place on
        // any arena.
        minimum_alignment_ = 1;
        for (const auto& arena : arenas_) {
            minimum_alignment_ = std::max(minimum_alignment_, arena->alignment);
        }
        return {};
    }

    tl::expected<BlockAllocation, ErrorCode> Allocate(
        size_t size, size_t alignment,
        const std::shared_ptr<BlockPoolState>& self) {
        if (size == 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        if (alignment == 0) alignment = minimum_alignment_;
        if (!IsPowerOfTwo(alignment)) {
            LOG(ERROR) << "DramBlockPool alignment must be a power of two, got "
                       << alignment;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        // Pad so the usable region can start on an aligned address. The
        // allocator hands out arbitrary offsets, so this is the only way to
        // honour the requested alignment without a second round trip. The
        // waste is bounded by alignment-1 and is dwarfed by the allocator's
        // own bin rounding for realistic block sizes.
        if (size > std::numeric_limits<size_t>::max() - (alignment - 1)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const size_t padded = size + alignment - 1;

        // Round-robin start so concurrent allocations spread across arenas,
        // then fall through the rest: this is the pool's own placement and
        // failover, invisible above.
        const size_t arena_count = arenas_.size();
        const size_t start =
            next_arena_.fetch_add(1, std::memory_order_relaxed) % arena_count;
        for (size_t i = 0; i < arena_count; ++i) {
            const size_t index = (start + i) % arena_count;
            auto allocated = AllocateFrom(index, size, padded, alignment, self);
            if (allocated) return allocated;
            if (allocated.error() != ErrorCode::NO_AVAILABLE_HANDLE) {
                return allocated;
            }
        }
        // Out of space is the only legal answer; reclaiming is not this
        // layer's decision (section 5.6).
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    tl::expected<void, ErrorCode> Free(PhysicalBlockId id) override {
        if (id.pool_id != pool_id_ || id.target_index >= arenas_.size()) {
            LOG(ERROR) << "DramBlockPool::Free with a foreign physical id";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto& arena = *arenas_[id.target_index];

        // Move the handle out under the lock and destroy it after unlocking:
        // the destructor returns space to the offset allocator, which takes
        // its own lock.
        std::unique_ptr<DramBlockDataHandle> handle;
        {
            std::lock_guard<std::mutex> lock(arena.mu);
            auto it = arena.handles.find(id.local_id);
            if (it == arena.handles.end()) {
                LOG(ERROR) << "DramBlockPool::Free on an unknown or already "
                              "freed block, local_id="
                           << id.local_id;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            handle = std::move(it->second);
            arena.handles.erase(it);
            arena.used_bytes -=
                std::min(arena.used_bytes, handle->ChargedSize());
        }
        return {};
    }

    BlockDataHandle* Get(PhysicalBlockId id) const override {
        if (id.pool_id != pool_id_ || id.target_index >= arenas_.size()) {
            return nullptr;
        }
        auto& arena = *arenas_[id.target_index];
        std::lock_guard<std::mutex> lock(arena.mu);
        auto it = arena.handles.find(id.local_id);
        return it == arena.handles.end() ? nullptr : it->second.get();
    }

    size_t Capacity() const override {
        size_t total = 0;
        for (const auto& arena : arenas_) total += arena->capacity;
        return total;
    }

    size_t Usage() const override {
        size_t total = 0;
        for (const auto& arena : arenas_) {
            std::lock_guard<std::mutex> lock(arena->mu);
            total += arena->used_bytes;
        }
        return total;
    }

    BlockPoolCapabilities Capabilities() const override {
        BlockPoolCapabilities caps;
        caps.direct_cpu_access = true;
        // A medium property: DRAM blocks expose an address a TransferEngine
        // can use. `te_registered_` below records whether this process
        // actually registered them, which is a configuration question.
        caps.te_addressable = true;
        caps.persistent = false;
        caps.minimum_alignment = minimum_alignment_;
        caps.preferred_io_size = 4096;
        return caps;
    }

    UUID PoolId() const { return pool_id_; }

   private:
    tl::expected<BlockAllocation, ErrorCode> AllocateFrom(
        size_t index, size_t usable_size, size_t padded, size_t alignment,
        const std::shared_ptr<BlockPoolState>& self) {
        auto& arena = *arenas_[index];
        auto raw = arena.allocator->allocate(padded);
        if (!raw.has_value()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        char* base = static_cast<char*>(raw->ptr());
        char* aligned = reinterpret_cast<char*>(
            AlignUp(reinterpret_cast<size_t>(base), alignment));

        PhysicalBlockId id;
        id.pool_id = pool_id_;
        id.target_index = static_cast<uint32_t>(index);
        // DRAM ids are never reused, so generation stays at 1. It exists for
        // pools with a fixed slot table, which must bump it on reuse so an
        // ABA on the slot is detectable.
        id.generation = 1;

        DramBlockDataHandle* borrowed = nullptr;
        {
            std::lock_guard<std::mutex> lock(arena.mu);
            id.local_id = arena.next_local_id++;
            auto handle = std::make_unique<DramBlockDataHandle>(
                std::move(*raw), aligned, usable_size, padded,
                arena.te_registered);
            borrowed = handle.get();
            arena.handles.emplace(id.local_id, std::move(handle));
            arena.used_bytes += padded;
        }
        return BlockAllocation::MakeForPool(id, usable_size, borrowed, self);
    }

    tl::expected<std::unique_ptr<DramArena>, ErrorCode> CreateArena(
        const DramArenaConfig& config) {
        if (config.capacity_bytes == 0) {
            LOG(ERROR) << "DRAM arena capacity must be greater than zero";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!IsPowerOfTwo(config.alignment)) {
            LOG(ERROR) << "DRAM arena alignment must be a power of two, got "
                       << config.alignment;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        auto arena = std::make_unique<DramArena>();
        arena->numa_node = config.numa_node;
        arena->capacity = config.capacity_bytes;
        arena->alignment = config.alignment;

        std::string location(kWildcardLocation);
        if (config.numa_node.has_value()) {
            if (numa_available() < 0) {
                LOG(ERROR) << "NUMA requested but not available";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            const int node = *config.numa_node;
            if (node < 0 || node > numa_max_node()) {
                LOG(ERROR) << "Invalid NUMA node " << node;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            char* memory = static_cast<char*>(
                numa_alloc_onnode(config.capacity_bytes, node));
            if (memory == nullptr) {
                LOG(ERROR) << "Failed to allocate " << config.capacity_bytes
                           << " bytes on NUMA node " << node;
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
            const size_t capacity = config.capacity_bytes;
            arena->memory = std::unique_ptr<char[], std::function<void(char*)>>(
                memory, [capacity](char* p) { numa_free(p, capacity); });
            location = "cpu:" + std::to_string(node);
        } else {
            char* memory = nullptr;
            try {
                memory = new char[config.capacity_bytes];
            } catch (const std::bad_alloc&) {
                LOG(ERROR) << "Failed to allocate " << config.capacity_bytes
                           << " bytes for a DRAM arena";
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
            arena->memory = std::unique_ptr<char[], std::function<void(char*)>>(
                memory, [](char* p) { delete[] p; });
        }

        arena->allocator = offset_allocator::OffsetAllocator::create(
            reinterpret_cast<uint64_t>(arena->memory.get()),
            config.capacity_bytes);
        if (!arena->allocator) {
            LOG(ERROR) << "Failed to create the offset allocator for a DRAM "
                          "arena";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        if (transfer_engine_) {
            const int rc = transfer_engine_->registerLocalMemory(
                arena->memory.get(), config.capacity_bytes, location);
            if (rc != 0) {
                LOG(ERROR) << "Failed to register a DRAM arena with the "
                              "TransferEngine, rc="
                           << rc;
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }
            arena->te_registered = true;
            te_registered_ = true;
        }
        return arena;
    }

    UUID pool_id_;
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::vector<std::unique_ptr<DramArena>> arenas_;
    std::atomic<size_t> next_arena_{0};
    size_t minimum_alignment_ = 1;
    bool te_registered_ = false;

   public:
    bool TeRegistered() const { return te_registered_; }
};

/**
 * @class SSDBlockDataHandle
 * @brief One extent inside a device file.
 *
 * It deliberately exposes no address. That is the whole point of the medium
 * split: a slow tier participates through Read/Write only, so nothing above
 * can accidentally publish a pointer to storage that is not memory. V1's
 * StorageBuffer instead returned 0 from data() once flushed, which is what let
 * an addr == 0 descriptor reach a peer.
 */
class SSDBlockDataHandle final : public BlockDataHandle {
   public:
    SSDBlockDataHandle(offset_allocator::OffsetAllocationHandle handle, int fd,
                       uint64_t file_offset, size_t usable_size,
                       size_t charged_size, bool fsync_on_commit)
        : handle_(std::move(handle)),
          fd_(fd),
          file_offset_(file_offset),
          size_(usable_size),
          charged_size_(charged_size),
          fsync_on_commit_(fsync_on_commit) {}

    size_t Size() const override { return size_; }
    size_t ChargedSize() const { return charged_size_; }

    tl::expected<void, ErrorCode> Read(
        size_t offset, std::span<std::byte> dst) const override {
        auto checked = CheckRange(offset, dst.size());
        if (!checked) return checked;

        size_t done = 0;
        while (done < dst.size()) {
            const ssize_t n =
                ::pread(fd_, dst.data() + done, dst.size() - done,
                        static_cast<off_t>(file_offset_ + offset + done));
            if (n < 0) {
                // A signal can interrupt a blocking pread at any point; that
                // is not an IO error.
                if (errno == EINTR) continue;
                LOG(ERROR) << "pread failed at offset " << file_offset_ + offset
                           << ": " << std::strerror(errno);
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            if (n == 0) {
                // Short of the requested length with no error: the extent does
                // not hold what the index says it does.
                LOG(ERROR) << "pread hit EOF after " << done << " of "
                           << dst.size() << " bytes";
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            done += static_cast<size_t>(n);
        }
        return {};
    }

    tl::expected<void, ErrorCode> Write(
        size_t offset, std::span<const std::byte> src) override {
        auto checked = CheckRange(offset, src.size());
        if (!checked) return checked;

        size_t done = 0;
        while (done < src.size()) {
            const ssize_t n =
                ::pwrite(fd_, src.data() + done, src.size() - done,
                         static_cast<off_t>(file_offset_ + offset + done));
            if (n < 0) {
                if (errno == EINTR) continue;
                LOG(ERROR) << "pwrite failed at offset "
                           << file_offset_ + offset << ": "
                           << std::strerror(errno);
                return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
            }
            done += static_cast<size_t>(n);
        }
        return {};
    }

    tl::expected<void, ErrorCode> Commit() override {
        if (!fsync_on_commit_) return {};
        // fdatasync, not fsync: the file's length and identity never change
        // after Init, so only the data needs to reach the device.
        while (::fdatasync(fd_) != 0) {
            if (errno == EINTR) continue;
            LOG(ERROR) << "fdatasync failed: " << std::strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_WRITE_FAIL);
        }
        return {};
    }

    std::optional<TransferAddress> GetTransferAddress() const override {
        return std::nullopt;
    }

   private:
    tl::expected<void, ErrorCode> CheckRange(size_t offset,
                                             size_t length) const {
        if (offset > size_ || length > size_ - offset) {
            LOG(ERROR) << "SSD block access out of range, offset=" << offset
                       << ", length=" << length << ", size=" << size_;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return {};
    }

    offset_allocator::OffsetAllocationHandle handle_;
    int fd_ = -1;
    uint64_t file_offset_ = 0;
    size_t size_ = 0;
    size_t charged_size_ = 0;
    bool fsync_on_commit_ = false;
};

/**
 * @struct SSDDevice
 * @brief One backing file with its own extent allocator and handle table.
 */
struct SSDDevice {
    std::string file_path;
    int fd = -1;
    size_t capacity = 0;
    size_t alignment = 4096;
    bool fsync_on_commit = false;
    std::shared_ptr<offset_allocator::OffsetAllocator> allocator;

    mutable std::mutex mu;
    std::unordered_map<uint64_t, std::unique_ptr<SSDBlockDataHandle>> handles;
    uint64_t next_local_id = 1;
    size_t used_bytes = 0;

    ~SSDDevice() {
        if (fd >= 0) ::close(fd);
    }
};

/**
 * @class SSDBlockPoolState
 * @brief A pure extent allocator over one or more device files.
 *
 * Out of space returns NO_AVAILABLE_HANDLE and nothing else: no whole-bucket
 * eviction, no background GC, no key-oriented file layout, and no upward
 * notification channel. V1's StorageTier did all of those and told the
 * metadata layer afterwards via TieredBackend::NotifyBucketEviction; V2
 * reclaims slow-tier space only through the eviction engine. Because the
 * request path never allocates here, exhaustion can at worst postpone an
 * offload.
 */
class SSDBlockPoolState final : public BlockPoolState {
   public:
    explicit SSDBlockPoolState(UUID pool_id) : pool_id_(pool_id) {}

    tl::expected<void, ErrorCode> Init(const SSDBlockPoolConfig& config) {
        if (config.devices.empty()) {
            LOG(ERROR) << "SSDBlockPool requires at least one device";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        for (const auto& device_config : config.devices) {
            auto device = CreateDevice(device_config);
            if (!device) return tl::make_unexpected(device.error());
            devices_.push_back(std::move(device.value()));
        }
        minimum_alignment_ = 1;
        for (const auto& device : devices_) {
            minimum_alignment_ =
                std::max(minimum_alignment_, device->alignment);
        }
        return {};
    }

    tl::expected<BlockAllocation, ErrorCode> Allocate(
        size_t size, size_t alignment,
        const std::shared_ptr<BlockPoolState>& self) {
        if (size == 0) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        if (alignment == 0) alignment = minimum_alignment_;
        if (!IsPowerOfTwo(alignment)) {
            LOG(ERROR) << "SSDBlockPool alignment must be a power of two, got "
                       << alignment;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (size > std::numeric_limits<size_t>::max() - (alignment - 1)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        const size_t padded = size + alignment - 1;

        const size_t device_count = devices_.size();
        const size_t start =
            next_device_.fetch_add(1, std::memory_order_relaxed) % device_count;
        for (size_t i = 0; i < device_count; ++i) {
            const size_t index = (start + i) % device_count;
            auto allocated = AllocateFrom(index, size, padded, alignment, self);
            if (allocated) return allocated;
            if (allocated.error() != ErrorCode::NO_AVAILABLE_HANDLE) {
                return allocated;
            }
        }
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }

    tl::expected<void, ErrorCode> Free(PhysicalBlockId id) override {
        if (id.pool_id != pool_id_ || id.target_index >= devices_.size()) {
            LOG(ERROR) << "SSDBlockPool::Free with a foreign physical id";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        auto& device = *devices_[id.target_index];

        std::unique_ptr<SSDBlockDataHandle> handle;
        {
            std::lock_guard<std::mutex> lock(device.mu);
            auto it = device.handles.find(id.local_id);
            if (it == device.handles.end()) {
                LOG(ERROR) << "SSDBlockPool::Free on an unknown or already "
                              "freed block, local_id="
                           << id.local_id;
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            handle = std::move(it->second);
            device.handles.erase(it);
            device.used_bytes -=
                std::min(device.used_bytes, handle->ChargedSize());
        }
        // Only the extent goes back. The pool has no idea what key lived here
        // and does not tell anyone it is gone.
        return {};
    }

    BlockDataHandle* Get(PhysicalBlockId id) const override {
        if (id.pool_id != pool_id_ || id.target_index >= devices_.size()) {
            return nullptr;
        }
        auto& device = *devices_[id.target_index];
        std::lock_guard<std::mutex> lock(device.mu);
        auto it = device.handles.find(id.local_id);
        return it == device.handles.end() ? nullptr : it->second.get();
    }

    size_t Capacity() const override {
        size_t total = 0;
        for (const auto& device : devices_) total += device->capacity;
        return total;
    }

    size_t Usage() const override {
        size_t total = 0;
        for (const auto& device : devices_) {
            std::lock_guard<std::mutex> lock(device->mu);
            total += device->used_bytes;
        }
        return total;
    }

    BlockPoolCapabilities Capabilities() const override {
        BlockPoolCapabilities caps;
        // No address, ever: that is what keeps a slow tier out of the request
        // path and out of any descriptor published to a peer.
        caps.direct_cpu_access = false;
        caps.te_addressable = false;
        caps.persistent = true;
        caps.minimum_alignment = minimum_alignment_;
        caps.preferred_io_size = 1ULL * 1024 * 1024;
        return caps;
    }

    UUID PoolId() const { return pool_id_; }

   private:
    tl::expected<BlockAllocation, ErrorCode> AllocateFrom(
        size_t index, size_t usable_size, size_t padded, size_t alignment,
        const std::shared_ptr<BlockPoolState>& self) {
        auto& device = *devices_[index];
        auto raw = device.allocator->allocate(padded);
        if (!raw.has_value()) {
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        const uint64_t aligned_offset =
            AlignUp(static_cast<size_t>(raw->address()), alignment);

        PhysicalBlockId id;
        id.pool_id = pool_id_;
        id.target_index = static_cast<uint32_t>(index);
        id.generation = 1;

        SSDBlockDataHandle* borrowed = nullptr;
        {
            std::lock_guard<std::mutex> lock(device.mu);
            id.local_id = device.next_local_id++;
            auto handle = std::make_unique<SSDBlockDataHandle>(
                std::move(*raw), device.fd, aligned_offset, usable_size, padded,
                device.fsync_on_commit);
            borrowed = handle.get();
            device.handles.emplace(id.local_id, std::move(handle));
            device.used_bytes += padded;
        }
        return BlockAllocation::MakeForPool(id, usable_size, borrowed, self);
    }

    tl::expected<std::unique_ptr<SSDDevice>, ErrorCode> CreateDevice(
        const SSDDeviceConfig& config) {
        if (config.capacity_bytes == 0) {
            LOG(ERROR) << "SSD device capacity must be greater than zero";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (!IsPowerOfTwo(config.alignment)) {
            LOG(ERROR) << "SSD device alignment must be a power of two, got "
                       << config.alignment;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (config.file_path.empty()) {
            LOG(ERROR) << "SSD device needs a file path";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        std::error_code ec;
        const std::filesystem::path path(config.file_path);
        if (path.has_parent_path()) {
            std::filesystem::create_directories(path.parent_path(), ec);
            if (ec) {
                LOG(ERROR) << "Failed to create the directory for "
                           << config.file_path << ": " << ec.message();
                return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
            }
        }

        auto device = std::make_unique<SSDDevice>();
        device->file_path = config.file_path;
        device->capacity = config.capacity_bytes;
        device->alignment = config.alignment;
        device->fsync_on_commit = config.fsync_on_commit;

        device->fd = ::open(config.file_path.c_str(), O_RDWR | O_CREAT, 0644);
        if (device->fd < 0) {
            LOG(ERROR) << "Failed to open " << config.file_path << ": "
                       << std::strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }
        // Size the file up front so an allocation never fails mid-write on a
        // full filesystem, and so the extent allocator's offsets are always
        // inside the file. Stale bytes from a previous run are harmless: the
        // index starts empty, so no extent is ever read before it is written.
        if (::ftruncate(device->fd,
                        static_cast<off_t>(config.capacity_bytes)) != 0) {
            LOG(ERROR) << "Failed to size " << config.file_path << " to "
                       << config.capacity_bytes
                       << " bytes: " << std::strerror(errno);
            return tl::make_unexpected(ErrorCode::FILE_OPEN_FAIL);
        }

        device->allocator = offset_allocator::OffsetAllocator::create(
            /*base=*/0, config.capacity_bytes);
        if (!device->allocator) {
            LOG(ERROR) << "Failed to create the extent allocator for "
                       << config.file_path;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
        LOG(INFO) << "SSD device ready: " << config.file_path
                  << ", capacity=" << config.capacity_bytes
                  << ", alignment=" << config.alignment
                  << ", fsync_on_commit=" << config.fsync_on_commit;
        return device;
    }

    UUID pool_id_;
    std::vector<std::unique_ptr<SSDDevice>> devices_;
    std::atomic<size_t> next_device_{0};
    size_t minimum_alignment_ = 1;
};

}  // namespace

// ---------------------------------------------------------------------------
// DramBlockPool
// ---------------------------------------------------------------------------

DramBlockPool::DramBlockPool(const DramBlockPoolConfig& config,
                             std::shared_ptr<TransferEngine> transfer_engine)
    : state_(std::make_shared<DramBlockPoolState>(generate_uuid(),
                                                  std::move(transfer_engine))),
      config_(config) {}

DramBlockPool::~DramBlockPool() = default;

tl::expected<void, ErrorCode> DramBlockPool::Init() {
    auto* state = static_cast<DramBlockPoolState*>(state_.get());
    auto initialized = state->Init(config_);
    if (!initialized) return initialized;
    if (!state->TeRegistered()) {
        // Loud, because it is almost always a misconfiguration: local Put/Get
        // still work, but no peer can read or write this memory. Silently
        // routing around it would move data somewhere the operator did not
        // ask for.
        LOG(WARNING) << "DRAM pool " << state->PoolId()
                     << " was built without a TransferEngine: its memory is "
                        "not registered, so remote transfers to it will fail";
    }
    return {};
}

tl::expected<BlockAllocation, ErrorCode> DramBlockPool::Allocate(
    size_t size, size_t alignment) {
    return static_cast<DramBlockPoolState*>(state_.get())
        ->Allocate(size, alignment, state_);
}

tl::expected<void, ErrorCode> DramBlockPool::Free(PhysicalBlockId id) {
    return state_->Free(id);
}

BlockDataHandle* DramBlockPool::Get(PhysicalBlockId id) {
    return state_->Get(id);
}

size_t DramBlockPool::Capacity() const { return state_->Capacity(); }

size_t DramBlockPool::Usage() const { return state_->Usage(); }

BlockPoolCapabilities DramBlockPool::Capabilities() const {
    return state_->Capabilities();
}

UUID DramBlockPool::Id() const {
    return static_cast<DramBlockPoolState*>(state_.get())->PoolId();
}

// ---------------------------------------------------------------------------
// SSDBlockPool
// ---------------------------------------------------------------------------

SSDBlockPool::SSDBlockPool(const SSDBlockPoolConfig& config)
    : state_(std::make_shared<SSDBlockPoolState>(generate_uuid())),
      config_(config) {}

SSDBlockPool::~SSDBlockPool() = default;

tl::expected<void, ErrorCode> SSDBlockPool::Init() {
    return static_cast<SSDBlockPoolState*>(state_.get())->Init(config_);
}

tl::expected<BlockAllocation, ErrorCode> SSDBlockPool::Allocate(
    size_t size, size_t alignment) {
    return static_cast<SSDBlockPoolState*>(state_.get())
        ->Allocate(size, alignment, state_);
}

tl::expected<void, ErrorCode> SSDBlockPool::Free(PhysicalBlockId id) {
    return state_->Free(id);
}

BlockDataHandle* SSDBlockPool::Get(PhysicalBlockId id) {
    return state_->Get(id);
}

size_t SSDBlockPool::Capacity() const { return state_->Capacity(); }

size_t SSDBlockPool::Usage() const { return state_->Usage(); }

BlockPoolCapabilities SSDBlockPool::Capabilities() const {
    return state_->Capabilities();
}

UUID SSDBlockPool::Id() const {
    return static_cast<SSDBlockPoolState*>(state_.get())->PoolId();
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

tl::expected<std::shared_ptr<BlockPool>, ErrorCode> CreateBlockPool(
    const BlockPoolConfig& config,
    std::shared_ptr<TransferEngine> transfer_engine) {
    if (std::holds_alternative<DramBlockPoolConfig>(config)) {
        auto pool = std::make_shared<DramBlockPool>(
            std::get<DramBlockPoolConfig>(config), std::move(transfer_engine));
        auto initialized = pool->Init();
        if (!initialized) return tl::make_unexpected(initialized.error());
        return pool;
    }
    if (std::holds_alternative<SSDBlockPoolConfig>(config)) {
        auto pool = std::make_shared<SSDBlockPool>(
            std::get<SSDBlockPoolConfig>(config));
        auto initialized = pool->Init();
        if (!initialized) return tl::make_unexpected(initialized.error());
        return pool;
    }
    LOG(ERROR) << "CreateBlockPool: unknown pool configuration";
    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
}

}  // namespace mooncake::v2
