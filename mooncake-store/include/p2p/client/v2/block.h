#pragma once

// Block identity, physical handles and the three capability wrappers that
// encode a block's lifecycle stage (section 5.4 - 5.8).
//
//   BlockPool::Allocate --> MutableBlock --Complete--> CompletedBlock
//             |                  |                          |
//             +-----Drop/Free----+                  BlockIndex::Insert
//                                                            |
//                                                    Visible BlockEntry
//                                                            |
//                                     Lookup --> ImmutableBlock snapshot
//
// Whether a committed block exists is decided by one thing only: is there a
// canonical BlockEntry for its registration in the owning tiler's BlockIndex.
// The wrappers cannot express an illegal operation (you cannot read a
// MutableBlock, cannot write an ImmutableBlock, cannot register twice).

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>

#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/block_registry.h"
#include "types.h"

namespace mooncake::v2 {

class BlockPoolState;
class BlockDataHandle;
class BlockIndex;
class TilerManager;
class MutableBlock;

/**
 * @struct TransferAddress
 * @brief A locally addressable, TransferEngine-registered memory range.
 *        Only pools whose capability says te_addressable produce one.
 */
struct TransferAddress {
    uintptr_t addr = 0;
    size_t size = 0;
};

/**
 * @struct PhysicalBlockId
 * @brief Opaque to everything above the pool. `target_index` names an
 *        arena/device *within* the pool and is interpreted by the pool alone;
 *        `generation` makes slot reuse detectable (ABA guard).
 */
struct PhysicalBlockId {
    UUID pool_id{0, 0};
    uint32_t target_index = 0;
    uint64_t local_id = 0;
    uint64_t generation = 0;
    bool operator==(const PhysicalBlockId&) const = default;
};

struct PhysicalBlockIdHash {
    size_t operator()(const PhysicalBlockId& id) const noexcept;
};

/**
 * @struct BlockPoolCapabilities
 * @brief What a pool can do, as facts rather than a medium enum. Routing and
 *        placement branch on these, never on MemoryType.
 */
struct BlockPoolCapabilities {
    bool direct_cpu_access = false;
    bool te_addressable = false;
    bool persistent = false;
    size_t minimum_alignment = 1;
    size_t preferred_io_size = 1;
};

/**
 * @class BlockDataHandle
 * @brief The uniform physical handle. Owned by the pool state; the rest of V2
 *        only ever sees this interface, never a concrete subclass.
 */
class BlockDataHandle {
   public:
    virtual ~BlockDataHandle() = default;

    virtual size_t Size() const = 0;

    // Implementations must range- and overflow-check offset + span size.
    virtual tl::expected<void, ErrorCode> Read(
        size_t offset, std::span<std::byte> dst) const = 0;
    virtual tl::expected<void, ErrorCode> Write(
        size_t offset, std::span<const std::byte> src) = 0;

    /** Durability barrier. No-op for DRAM. */
    virtual tl::expected<void, ErrorCode> Commit() = 0;

    /**
     * @brief The TE-visible address, when there is one.
     *
     * Deliberately optional rather than "address 0 means unsupported": V1 hit
     * exactly that bug, handing peers a zero address that only failed much
     * later on the requester side.
     */
    virtual std::optional<TransferAddress> GetTransferAddress() const = 0;

    /**
     * @brief May the calling CPU dereference GetTransferAddress()?
     *
     * The copy layer routes on this, not on a medium enum. The default answers
     * it from the address itself, which is exact for host memory (an address
     * the CPU can use) and for a file-backed block (no address at all). A pool
     * whose memory is addressable by a device but not by the CPU must override
     * it, or a memcpy copier would happily dereference device memory.
     */
    virtual bool DirectCpuAccess() const {
        return GetTransferAddress().has_value();
    }

    /**
     * @brief Did this process actually register this memory with an engine?
     *
     * Deliberately not the same question as "is there an address". A DRAM
     * block always exposes a usable pointer, registered or not, so a copier
     * that took the address as proof would hand unregistered memory to RDMA --
     * which fails far away from here, on the peer.
     */
    virtual bool TeRegistered() const { return false; }
};

/**
 * @class BlockAllocation
 * @brief Move-only RAII lease on one physical allocation.
 *
 * This is the *only* thing that frees a physical block: pools never reclaim on
 * their own (section 3.5). The shared BlockPoolState keeps the pool's memory
 * alive for as long as any allocation exists, so a BlockEntry snapshot handed
 * out before a Stop() stays valid until the last reader drops it.
 */
class BlockAllocation {
   public:
    BlockAllocation() = default;
    BlockAllocation(const BlockAllocation&) = delete;
    BlockAllocation& operator=(const BlockAllocation&) = delete;
    BlockAllocation(BlockAllocation&& other) noexcept;
    BlockAllocation& operator=(BlockAllocation&& other) noexcept;
    ~BlockAllocation();

    PhysicalBlockId Id() const { return id_; }
    /** Valid while this allocation is armed. */
    BlockDataHandle& Data() const;
    size_t Size() const { return size_bytes_; }
    explicit operator bool() const { return armed_; }

    /** Idempotent; returns the physical allocation exactly once. */
    void Reset();

    /** Pools construct these; nothing else may. */
    static BlockAllocation MakeForPool(
        PhysicalBlockId id, size_t size_bytes, BlockDataHandle* data,
        std::shared_ptr<BlockPoolState> pool_state);

   private:
    BlockAllocation(PhysicalBlockId id, size_t size_bytes,
                    BlockDataHandle* data,
                    std::shared_ptr<BlockPoolState> pool_state);

    PhysicalBlockId id_{};
    size_t size_bytes_ = 0;
    BlockDataHandle* data_ = nullptr;  // stable while state_ is held
    std::shared_ptr<BlockPoolState> state_;
    bool armed_ = false;
};

/**
 * @struct BlockId
 * @brief Tiler-local block identity. `generation` is copied from the physical
 *        id so a recycled physical slot cannot be mistaken for the old block.
 */
struct BlockId {
    UUID tiler_id{0, 0};
    uint64_t local_id = 0;
    uint64_t generation = 0;
    bool operator==(const BlockId&) const = default;
};

struct BlockIdHash {
    size_t operator()(const BlockId& id) const noexcept;
};

/**
 * @struct Block
 * @brief A committed block: immutable identity plus the resources it owns.
 */
struct Block {
    BlockId id;
    size_t size_bytes = 0;
    std::string key;
    BlockAllocation allocation;            // move-only physical lease
    BlockRegistrationHandle registration;  // strong: keeps the key alive
};

/**
 * @struct BlockEntry
 * @brief What the BlockIndex stores. Business fields never change after
 *        insertion, so readers need no per-entry lock; `last_access_tick` is
 *        an approximate policy statistic, not authority.
 */
struct BlockEntry {
    Block block;
    mutable std::atomic<uint64_t> last_access_tick{0};

    explicit BlockEntry(Block&& b) : block(std::move(b)) {}
};

using BlockEntryPtr = std::shared_ptr<const BlockEntry>;

/**
 * @struct BlockToken
 * @brief A non-owning name for one committed block.
 *
 * Everything that reasons about a block without owning it -- the eviction
 * index, the evict engine, a movement proposal -- holds one of these. Every
 * field is a name, not a resource: the weak registration and the BlockId are
 * re-validated against the owning BlockIndex, under its shard lock, before
 * anything is removed or copied. That is what stops approximate state from
 * ever becoming a second source of truth about what exists.
 */
struct BlockToken {
    std::string key;
    RegistrationId registration_id;
    WeakBlockRegistrationHandle registration;
    UUID tiler_id{0, 0};
    BlockId block_id;
    size_t size_bytes = 0;
};

// ---------------------------------------------------------------------------
// Capability wrappers
// ---------------------------------------------------------------------------

/**
 * @class CompletedBlock
 * @brief Written and durable, not yet visible. Can only be registered or
 *        aborted. Dropping it frees the allocation.
 */
class CompletedBlock {
   public:
    CompletedBlock() = default;
    CompletedBlock(const CompletedBlock&) = delete;
    CompletedBlock& operator=(const CompletedBlock&) = delete;
    CompletedBlock(CompletedBlock&& other) noexcept;
    CompletedBlock& operator=(CompletedBlock&& other) noexcept;
    ~CompletedBlock();

    size_t Size() const { return allocation_.Size(); }
    const std::string& Key() const { return key_; }
    PhysicalBlockId PhysicalId() const { return allocation_.Id(); }
    explicit operator bool() const { return armed_; }

    void Abort();

   private:
    friend class MutableBlock;
    friend class TilerManager;
    friend class BlockIndex;
    // BlockIndex::Insert moves the allocation and the key straight into the
    // BlockEntry it builds, which is why it needs access here.
    CompletedBlock(BlockAllocation allocation, std::string key)
        : allocation_(std::move(allocation)),
          key_(std::move(key)),
          armed_(true) {}

    BlockAllocation allocation_;
    std::string key_;
    bool armed_ = false;
};

/**
 * @class MutableBlock
 * @brief Allocated and writable, invisible to any reader. Complete() consumes
 *        it; dropping it frees the allocation.
 */
class MutableBlock {
   public:
    MutableBlock() = default;
    MutableBlock(const MutableBlock&) = delete;
    MutableBlock& operator=(const MutableBlock&) = delete;
    MutableBlock(MutableBlock&& other) noexcept;
    MutableBlock& operator=(MutableBlock&& other) noexcept;
    ~MutableBlock();

    size_t Size() const;
    PhysicalBlockId PhysicalId() const { return allocation_.Id(); }
    explicit operator bool() const { return armed_; }

    tl::expected<void, ErrorCode> Write(size_t offset,
                                        std::span<const std::byte> src);
    std::optional<TransferAddress> GetTransferAddress() const;

    /**
     * @brief The physical handle, for the local copy engine only.
     *
     * Named for its single caller on purpose: everything else goes through
     * Write(), and the copy engine needs the handle itself so it can route on
     * the medium's capabilities instead of bouncing every byte.
     */
    BlockDataHandle* DataHandleForCopy();

    /** Flush, then hand over ownership as a CompletedBlock. */
    tl::expected<CompletedBlock, ErrorCode> Complete(std::string key) &&;

    void Abort();

    /** Tilers construct these; nothing else may. */
    static MutableBlock MakeForTiler(BlockAllocation allocation);

   private:
    explicit MutableBlock(BlockAllocation allocation)
        : allocation_(std::move(allocation)), armed_(true) {}

    BlockAllocation allocation_;
    bool armed_ = false;
};

/**
 * @class ImmutableBlock
 * @brief A read snapshot of a committed entry.
 *
 * Holding one keeps the physical resource alive but says nothing about
 * visibility: a concurrent Delete/Evict can detach the entry from the index
 * while readers finish. Reclaim happens when the last snapshot goes away.
 */
class ImmutableBlock {
   public:
    ImmutableBlock() = default;
    explicit ImmutableBlock(BlockEntryPtr entry) : entry_(std::move(entry)) {}

    const std::string& Key() const;
    RegistrationId Registration() const;
    BlockId Id() const;
    size_t Size() const;

    tl::expected<void, ErrorCode> Read(size_t offset,
                                       std::span<std::byte> dst) const;
    std::optional<TransferAddress> GetTransferAddress() const;

    /** See MutableBlock::DataHandleForCopy. Read side. */
    BlockDataHandle* DataHandleForCopy() const;

    /** Approximate policy statistic; not authority. */
    void RecordAccess(uint64_t tick) const;

    const BlockEntryPtr& Entry() const { return entry_; }
    explicit operator bool() const { return entry_ != nullptr; }

   private:
    BlockEntryPtr entry_;
};

}  // namespace mooncake::v2
