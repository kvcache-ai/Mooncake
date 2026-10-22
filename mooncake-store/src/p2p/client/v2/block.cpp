#include "p2p/client/v2/block.h"

#include <utility>

#include <glog/logging.h>

#include "p2p/client/v2/block_pool.h"

namespace mooncake::v2 {

size_t PhysicalBlockIdHash::operator()(
    const PhysicalBlockId& id) const noexcept {
    size_t seed = boost::hash<UUID>{}(id.pool_id);
    boost::hash_combine(seed, id.target_index);
    boost::hash_combine(seed, id.local_id);
    boost::hash_combine(seed, id.generation);
    return seed;
}

size_t BlockIdHash::operator()(const BlockId& id) const noexcept {
    size_t seed = boost::hash<UUID>{}(id.tiler_id);
    boost::hash_combine(seed, id.local_id);
    boost::hash_combine(seed, id.generation);
    return seed;
}

// ---------------------------------------------------------------------------
// BlockAllocation
// ---------------------------------------------------------------------------

BlockAllocation::BlockAllocation(PhysicalBlockId id, size_t size_bytes,
                                 BlockDataHandle* data,
                                 std::shared_ptr<BlockPoolState> pool_state)
    : id_(id),
      size_bytes_(size_bytes),
      data_(data),
      state_(std::move(pool_state)),
      armed_(data != nullptr && state_ != nullptr) {}

BlockAllocation BlockAllocation::MakeForPool(
    PhysicalBlockId id, size_t size_bytes, BlockDataHandle* data,
    std::shared_ptr<BlockPoolState> pool_state) {
    return BlockAllocation(id, size_bytes, data, std::move(pool_state));
}

BlockAllocation::BlockAllocation(BlockAllocation&& other) noexcept
    : id_(other.id_),
      size_bytes_(other.size_bytes_),
      data_(other.data_),
      state_(std::move(other.state_)),
      armed_(other.armed_) {
    other.data_ = nullptr;
    other.armed_ = false;
    other.size_bytes_ = 0;
}

BlockAllocation& BlockAllocation::operator=(BlockAllocation&& other) noexcept {
    if (this != &other) {
        Reset();
        id_ = other.id_;
        size_bytes_ = other.size_bytes_;
        data_ = other.data_;
        state_ = std::move(other.state_);
        armed_ = other.armed_;
        other.data_ = nullptr;
        other.armed_ = false;
        other.size_bytes_ = 0;
    }
    return *this;
}

BlockAllocation::~BlockAllocation() { Reset(); }

void BlockAllocation::Reset() {
    if (!armed_) {
        state_.reset();
        data_ = nullptr;
        return;
    }
    armed_ = false;
    // This is the one and only place a physical block goes back to its pool.
    auto freed = state_->Free(id_);
    if (!freed) {
        LOG(ERROR) << "BlockPool::Free failed for physical block, pool="
                   << id_.pool_id << ", target=" << id_.target_index
                   << ", local_id=" << id_.local_id
                   << ", error=" << toString(freed.error());
    }
    data_ = nullptr;
    state_.reset();
}

BlockDataHandle& BlockAllocation::Data() const {
    CHECK(armed_ && data_ != nullptr)
        << "BlockAllocation::Data() on a released allocation";
    return *data_;
}

// ---------------------------------------------------------------------------
// MutableBlock
// ---------------------------------------------------------------------------

MutableBlock MutableBlock::MakeForTiler(BlockAllocation allocation) {
    return MutableBlock(std::move(allocation));
}

MutableBlock::MutableBlock(MutableBlock&& other) noexcept
    : allocation_(std::move(other.allocation_)), armed_(other.armed_) {
    other.armed_ = false;
}

MutableBlock& MutableBlock::operator=(MutableBlock&& other) noexcept {
    if (this != &other) {
        Abort();
        allocation_ = std::move(other.allocation_);
        armed_ = other.armed_;
        other.armed_ = false;
    }
    return *this;
}

MutableBlock::~MutableBlock() { Abort(); }

void MutableBlock::Abort() {
    // Unconsumed wrappers return their allocation; there is no other cleanup
    // path, which is what makes every failure branch in section 6 a rollback.
    armed_ = false;
    allocation_.Reset();
}

size_t MutableBlock::Size() const { return allocation_.Size(); }

tl::expected<void, ErrorCode> MutableBlock::Write(
    size_t offset, std::span<const std::byte> src) {
    if (!armed_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    return allocation_.Data().Write(offset, src);
}

std::optional<TransferAddress> MutableBlock::GetTransferAddress() const {
    if (!armed_) return std::nullopt;
    return allocation_.Data().GetTransferAddress();
}

BlockDataHandle* MutableBlock::DataHandleForCopy() {
    if (!armed_) return nullptr;
    return &allocation_.Data();
}

tl::expected<CompletedBlock, ErrorCode> MutableBlock::Complete(
    std::string key) && {
    if (!armed_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);

    auto committed = allocation_.Data().Commit();
    if (!committed) {
        // Stay a MutableBlock: the caller still owns the rollback.
        return tl::make_unexpected(committed.error());
    }
    armed_ = false;
    return CompletedBlock(std::move(allocation_), std::move(key));
}

// ---------------------------------------------------------------------------
// CompletedBlock
// ---------------------------------------------------------------------------

CompletedBlock::CompletedBlock(CompletedBlock&& other) noexcept
    : allocation_(std::move(other.allocation_)),
      key_(std::move(other.key_)),
      armed_(other.armed_) {
    other.armed_ = false;
}

CompletedBlock& CompletedBlock::operator=(CompletedBlock&& other) noexcept {
    if (this != &other) {
        Abort();
        allocation_ = std::move(other.allocation_);
        key_ = std::move(other.key_);
        armed_ = other.armed_;
        other.armed_ = false;
    }
    return *this;
}

CompletedBlock::~CompletedBlock() { Abort(); }

void CompletedBlock::Abort() {
    armed_ = false;
    allocation_.Reset();
}

// ---------------------------------------------------------------------------
// ImmutableBlock
// ---------------------------------------------------------------------------

namespace {
const std::string& EmptyKey() {
    static const std::string kEmpty;
    return kEmpty;
}
}  // namespace

const std::string& ImmutableBlock::Key() const {
    return entry_ ? entry_->block.key : EmptyKey();
}

RegistrationId ImmutableBlock::Registration() const {
    return entry_ ? entry_->block.registration.Id() : RegistrationId{};
}

BlockId ImmutableBlock::Id() const {
    return entry_ ? entry_->block.id : BlockId{};
}

size_t ImmutableBlock::Size() const {
    return entry_ ? entry_->block.size_bytes : 0;
}

tl::expected<void, ErrorCode> ImmutableBlock::Read(
    size_t offset, std::span<std::byte> dst) const {
    if (!entry_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    return entry_->block.allocation.Data().Read(offset, dst);
}

std::optional<TransferAddress> ImmutableBlock::GetTransferAddress() const {
    if (!entry_) return std::nullopt;
    return entry_->block.allocation.Data().GetTransferAddress();
}

BlockDataHandle* ImmutableBlock::DataHandleForCopy() const {
    if (!entry_) return nullptr;
    return &entry_->block.allocation.Data();
}

void ImmutableBlock::RecordAccess(uint64_t tick) const {
    if (!entry_) return;
    entry_->last_access_tick.store(tick, std::memory_order_relaxed);
}

}  // namespace mooncake::v2
