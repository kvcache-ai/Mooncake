#pragma once

// BlockPool: everything about physical storage, and nothing about keys.
//
// A pool hides its entire hardware topology. One DramBlockPool can span several
// NUMA arenas; one SSDBlockPool can span several devices, files and allocators.
// It picks the physical target, handles alignment, balances load and falls over
// between targets internally. Above it, TilerManager sees only
// Allocate/Free/Get/Capacity/Usage/Capabilities and never learns a NUMA node, a
// device path or a file offset.
//
// Architectural constraint (sections 3.5, 5.6, invariant 7.4.12): a pool NEVER
// reclaims on its own. Not inside Allocate, not from a background thread, not
// on a watermark. Out of space has exactly one legal answer,
// NO_AVAILABLE_HANDLE; deciding what to release is the EvictEngine's job, and
// releasing happens only when a BlockAllocation is destroyed. This is a
// deliberate departure from V1's StorageTier, which evicted whole buckets from
// under the metadata layer and then reported it upwards via
// TieredBackend::NotifyBucketEviction. V2 has no such upward channel.

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/block.h"
#include "types.h"

namespace mooncake {
class TransferEngine;
}  // namespace mooncake

namespace mooncake::v2 {

/**
 * @struct DramArenaConfig
 * @brief One contiguous DRAM region inside a DramBlockPool.
 */
struct DramArenaConfig {
    std::optional<int> numa_node;
    size_t capacity_bytes = 0;
    size_t alignment = 64;
};

struct DramBlockPoolConfig {
    std::vector<DramArenaConfig> arenas;
};

/**
 * @struct SSDDeviceConfig
 * @brief One file/device inside an SSDBlockPool.
 */
struct SSDDeviceConfig {
    std::string file_path;
    size_t capacity_bytes = 0;
    size_t alignment = 4096;
    bool fsync_on_commit = false;
};

struct SSDBlockPoolConfig {
    std::vector<SSDDeviceConfig> devices;
};

using BlockPoolConfig = std::variant<DramBlockPoolConfig, SSDBlockPoolConfig>;

/**
 * @class BlockPoolState
 * @brief The shared, reference-counted core of a pool.
 *
 * It owns every BlockDataHandle. Both the pool facade and every outstanding
 * BlockAllocation keep it alive, which is what lets a reader that obtained a
 * snapshot before Stop() finish safely afterwards.
 */
class BlockPoolState {
   public:
    virtual ~BlockPoolState() = default;

    /** Called exactly once per allocation, by ~BlockAllocation. */
    virtual tl::expected<void, ErrorCode> Free(PhysicalBlockId id) = 0;
    /** Borrowed; valid while an allocation for `id` is alive. */
    virtual BlockDataHandle* Get(PhysicalBlockId id) const = 0;
    virtual size_t Capacity() const = 0;
    virtual size_t Usage() const = 0;
    virtual BlockPoolCapabilities Capabilities() const = 0;
};

/**
 * @class BlockPool
 */
class BlockPool {
   public:
    virtual ~BlockPool() = default;

    /**
     * @param alignment 0 means "use the pool's minimum alignment".
     * @return NO_AVAILABLE_HANDLE when out of space. The pool must have tried
     *         all of its own targets first; it must not evict to make room.
     */
    virtual tl::expected<BlockAllocation, ErrorCode> Allocate(
        size_t size, size_t alignment) = 0;

    /**
     * @brief Pool-internal / test adaptor only.
     *
     * The normal path frees through ~BlockAllocation. Implementations must
     * detect an unknown id and a double free rather than corrupting state.
     */
    virtual tl::expected<void, ErrorCode> Free(PhysicalBlockId id) = 0;

    virtual BlockDataHandle* Get(PhysicalBlockId id) = 0;  // borrowed
    virtual size_t Capacity() const = 0;
    virtual size_t Usage() const = 0;
    virtual BlockPoolCapabilities Capabilities() const = 0;

    /** Identity used in PhysicalBlockId::pool_id. */
    virtual UUID Id() const = 0;
};

/**
 * @class DramBlockPool
 * @brief DRAM across one or more NUMA arenas.
 *
 * Arena selection, alignment and cross-arena fallback are private. When a
 * TransferEngine is supplied each arena is registered with it, so the pool
 * reports te_addressable and hands out TransferAddresses.
 */
class DramBlockPool final : public BlockPool {
   public:
    DramBlockPool(const DramBlockPoolConfig& config,
                  std::shared_ptr<TransferEngine> transfer_engine);
    ~DramBlockPool() override;

    tl::expected<void, ErrorCode> Init();

    tl::expected<BlockAllocation, ErrorCode> Allocate(
        size_t size, size_t alignment) override;
    tl::expected<void, ErrorCode> Free(PhysicalBlockId id) override;
    BlockDataHandle* Get(PhysicalBlockId id) override;
    size_t Capacity() const override;
    size_t Usage() const override;
    BlockPoolCapabilities Capabilities() const override;
    UUID Id() const override;

   private:
    std::shared_ptr<BlockPoolState> state_;
    DramBlockPoolConfig config_;
};

/**
 * @class SSDBlockPool
 * @brief A pure extent allocator over one or more device files.
 *
 * It never exposes an address, so a slow tier can hold data without anything
 * above it being able to hand a pointer to a peer. Out of space is reported
 * and nothing else: no whole-bucket eviction, no background GC, no upward
 * notification. Since the request path never allocates here, exhaustion can at
 * worst postpone an offload.
 *
 * Deployment constraint: because V2 manages this space completely differently
 * from V1's StorageTier, the two must not share a data directory or file.
 */
class SSDBlockPool final : public BlockPool {
   public:
    explicit SSDBlockPool(const SSDBlockPoolConfig& config);
    ~SSDBlockPool() override;

    tl::expected<void, ErrorCode> Init();

    tl::expected<BlockAllocation, ErrorCode> Allocate(
        size_t size, size_t alignment) override;
    tl::expected<void, ErrorCode> Free(PhysicalBlockId id) override;
    BlockDataHandle* Get(PhysicalBlockId id) override;
    size_t Capacity() const override;
    size_t Usage() const override;
    BlockPoolCapabilities Capabilities() const override;
    UUID Id() const override;

   private:
    std::shared_ptr<BlockPoolState> state_;
    SSDBlockPoolConfig config_;
};

/**
 * @brief Build a pool from its configuration.
 *
 * Construction is centralized here so TilerManager and DataManagerV2 only ever
 * see the BlockPool interface. Supporting a new medium means editing this one
 * function; no layer above changes.
 */
tl::expected<std::shared_ptr<BlockPool>, ErrorCode> CreateBlockPool(
    const BlockPoolConfig& config,
    std::shared_ptr<TransferEngine> transfer_engine);

}  // namespace mooncake::v2
