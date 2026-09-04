#include "p2p/client/v2/tiler_manager.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {

TilerManager::TilerManager(const LogicalTilerConfig& logical_config,
                           const BlockIndexConfig& index_config,
                           std::shared_ptr<BlockPool> block_pool,
                           BlockRegistry block_registry,
                           EventPublisher event_publisher,
                           const EvictionIndexConfig& eviction_config)
    : tiler_id_(logical_config.tiler_id),
      memory_type_(logical_config.memory_type),
      priority_(logical_config.priority),
      tags_(logical_config.tags),
      block_pool_(std::move(block_pool)),
      block_index_(
          std::make_unique<BlockIndex>(logical_config.tiler_id, index_config)),
      block_registry_(std::move(block_registry)),
      event_publisher_(std::move(event_publisher)) {
    CHECK(block_pool_ != nullptr) << "TilerManager requires a BlockPool";
    auto eviction = CreateEvictionIndex(eviction_config, tiler_id_,
                                        block_pool_->Capacity());
    // The configuration is validated at Init, so a failure here is a
    // programming error rather than an operator one -- and continuing without
    // an ordering would leave the tier with no victims to offer and no way to
    // reclaim anything.
    CHECK(eviction.has_value())
        << "TilerManager could not build its eviction index: "
        << toString(eviction.error());
    eviction_index_ = std::move(eviction.value());
}

namespace {

/** The non-owning name of a committed block, for the eviction index. */
BlockToken TokenOf(const BlockRegistrationHandle& registration,
                   const UUID& tiler_id, const BlockId& block_id,
                   size_t size_bytes) {
    BlockToken token;
    token.key = registration.Key();
    token.registration_id = registration.Id();
    token.registration = registration.Downgrade();
    token.tiler_id = tiler_id;
    token.block_id = block_id;
    token.size_bytes = size_bytes;
    return token;
}

}  // namespace

tl::expected<MutableBlock, ErrorCode> TilerManager::Allocate(size_t size,
                                                             size_t alignment) {
    if (IsStopped()) return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    // alignment == 0 defers to the pool's minimum; this layer does not know
    // where that number comes from.
    auto allocation = block_pool_->Allocate(size, alignment);
    if (!allocation) {
        if (allocation.error() == ErrorCode::NO_AVAILABLE_HANDLE) {
            BlockEvent event;
            event.type = EventType::kAllocationFailure;
            event.tiler_id = tiler_id_;
            event.size_bytes = size;
            event.error = allocation.error();
            event_publisher_.Publish(std::move(event));
        }
        return tl::make_unexpected(allocation.error());
    }
    return MutableBlock::MakeForTiler(std::move(allocation.value()));
}

tl::expected<ImmutableBlock, ErrorCode> TilerManager::Register(
    std::string_view key, CompletedBlock&& block) {
    CompletedBlock owned = std::move(block);
    if (IsStopped()) return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);

    auto registration = block_registry_.Register(key);
    if (!registration) {
        return tl::make_unexpected(registration.error());
    }
    return InsertRegistered(std::move(owned), registration.value(),
                            /*defer_notify=*/false);
}

tl::expected<ImmutableBlock, ErrorCode> TilerManager::RegisterWithHandle(
    CompletedBlock&& block, const BlockRegistrationHandle& registration,
    bool defer_notify) {
    CompletedBlock owned = std::move(block);
    if (IsStopped()) return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    if (!registration) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    return InsertRegistered(std::move(owned), registration, defer_notify);
}

tl::expected<ImmutableBlock, ErrorCode> TilerManager::InsertRegistered(
    CompletedBlock&& block, const BlockRegistrationHandle& registration,
    bool defer_notify) {
    const size_t size_bytes = block.Size();
    auto inserted = block_index_->Insert(std::move(block), registration);
    if (!inserted) {
        return tl::make_unexpected(inserted.error());
    }
    // Synchronously, and only after the authoritative insert succeeded: an
    // ordering that lists a block the index does not have would offer a victim
    // that can never be removed, and one that misses a block the index does
    // have would leak that block's bytes for good. Outside the shard lock,
    // inside whatever mutation guard the caller holds.
    eviction_index_->OnCommit(
        TokenOf(registration, tiler_id_, inserted->Id(), size_bytes));
    if (!defer_notify) {
        NotifyRegistered(registration, inserted->Id(), size_bytes);
    }
    return inserted;
}

void TilerManager::NotifyRegistered(const BlockRegistrationHandle& registration,
                                    const BlockId& block_id,
                                    size_t size_bytes) const {
    // The insert is the linearization point; the presence marker is a hint
    // that follows it, and the event is a fact about something that already
    // happened. Neither may run while an index shard lock -- or the key's
    // mutation guard -- is held.
    registration.MarkPresent(tiler_id_);

    BlockEvent event;
    event.type = EventType::kCommit;
    event.key = registration.Key();
    event.tiler_id = tiler_id_;
    event.block_id = block_id;
    event.registration = registration.Downgrade();
    event.size_bytes = size_bytes;
    event_publisher_.Publish(std::move(event));
}

void TilerManager::NotifyAccess(const BlockRegistrationHandle& registration,
                                const ImmutableBlock& block) const {
    if (!registration || !block) return;
    // Applied here rather than carried through the event queue. The design
    // permits the asynchronous route, but the queue is allowed to drop and
    // coalesce access events under pressure -- and dropping exactly the reads
    // that arrive while the tier is busiest is when the ordering matters most.
    // This call takes only the index's own lock and holds nothing else.
    eviction_index_->OnAccess(
        TokenOf(registration, tiler_id_, block.Id(), block.Size()));

    BlockEvent event;
    event.type = EventType::kAccess;
    event.key = registration.Key();
    event.tiler_id = tiler_id_;
    event.block_id = block.Id();
    event.registration = registration.Downgrade();
    event.size_bytes = block.Size();
    // Best-effort by design: access is a sampling signal, so a full queue
    // folds or drops it rather than making a reader wait.
    event_publisher_.Publish(std::move(event));
}

tl::expected<ImmutableBlock, ErrorCode> TilerManager::Match(
    const BlockRegistrationHandle& registration) const {
    // Deliberately still served after Stop(). Stop() rejects new *work*
    // (Allocate, Register); the committed index is untouched until Destroy()
    // drains it. Refusing reads here would make Exist() -- which returns a
    // plain bool and cannot report "shutting down" -- answer "absent" for a
    // fully intact replica, diverging from V1 and letting RectifyReadRoute ask
    // Master to drop a good replica during shutdown.
    if (!registration) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    auto found = block_index_->Lookup(registration);
    if (!found) return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    return std::move(*found);
}

tl::expected<void, ErrorCode> TilerManager::Delete(
    const BlockRegistrationHandle& registration,
    std::optional<BlockId> expected_id, bool defer_notify) {
    if (!registration) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    // Detached under the shard lock, released here: if a reader still holds a
    // snapshot the physical block survives until that reader is done.
    BlockEntryPtr detached = block_index_->Erase(registration, expected_id);
    if (!detached) return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);

    const BlockId block_id = detached->block.id;
    const size_t size_bytes = detached->block.size_bytes;
    detached.reset();

    // Token-scoped inside the index: if a newer block for this registration
    // has already been committed, its record survives this removal.
    eviction_index_->OnDelete(
        TokenOf(registration, tiler_id_, block_id, size_bytes));

    if (!defer_notify) {
        NotifyDeleted(registration, block_id, size_bytes);
    }
    return {};
}

void TilerManager::NotifyDeleted(const BlockRegistrationHandle& registration,
                                 const BlockId& block_id,
                                 size_t size_bytes) const {
    registration.MarkAbsent(tiler_id_);

    BlockEvent event;
    event.type = EventType::kDelete;
    event.key = registration.Key();
    event.tiler_id = tiler_id_;
    event.block_id = block_id;
    event.registration = registration.Downgrade();
    event.size_bytes = size_bytes;
    event_publisher_.Publish(std::move(event));
}

std::vector<BlockEntryPtr> TilerManager::DrainAll() {
    // Both halves, or the ordering would keep naming victims for a tier that
    // no longer indexes anything.
    eviction_index_->Clear();
    return block_index_->Drain();
}

BlockIndexSnapshot TilerManager::SnapshotTokens(size_t shard_id) const {
    BlockIndexSnapshot snapshot;
    // One shard, so this never locks more than one at a time; the caller
    // assembles the whole tier with SnapshotAllTokens if it needs to.
    snapshot.complete = false;
    snapshot.shard_id = shard_id;
    OpenSnapshot(snapshot);
    AppendShardTokens(shard_id, snapshot);
    return snapshot;
}

BlockIndexSnapshot TilerManager::SnapshotAllTokens() const {
    BlockIndexSnapshot snapshot;
    // Complete in the sense the eviction index needs -- every shard was
    // read -- while still locking one shard at a time. A commit that lands
    // mid-walk is caught by the mutation sample below, which is what stops
    // Reconcile from treating this as proof that the ordering is whole.
    snapshot.complete = true;
    OpenSnapshot(snapshot);
    for (size_t shard_id = 0; shard_id < block_index_->ShardCount();
         ++shard_id) {
        AppendShardTokens(shard_id, snapshot);
    }
    return snapshot;
}

void TilerManager::OpenSnapshot(BlockIndexSnapshot& snapshot) const {
    snapshot.tiler_id = tiler_id_;
    // Which shards the drop side of Reconcile is allowed to judge: without
    // this a one-shard snapshot looks like proof that every block in the
    // other shards is gone.
    snapshot.shard_count = block_index_->ShardCount();
    // Sampled before a single entry is read. A commit landing between here
    // and the Reconcile call is missing from `entries` through no fault of
    // the eviction index, and must not be dropped from it.
    snapshot.observed_mutations = eviction_index_->MutationCount();
}

void TilerManager::AppendShardTokens(size_t shard_id,
                                     BlockIndexSnapshot& snapshot) const {
    for (const auto& entry : block_index_->SnapshotShard(shard_id)) {
        if (!entry) continue;
        BlockToken token;
        token.key = entry->block.key;
        token.registration_id = entry->block.registration.Id();
        token.registration = entry->block.registration.Downgrade();
        token.tiler_id = tiler_id_;
        token.block_id = entry->block.id;
        token.size_bytes = entry->block.size_bytes;
        snapshot.entries.push_back(std::move(token));
    }
}

std::vector<BlockEntryPtr> TilerManager::SnapshotShard(size_t shard_id) const {
    return block_index_->SnapshotShard(shard_id);
}

size_t TilerManager::ShardCount() const { return block_index_->ShardCount(); }

TierView TilerManager::GetView() const {
    TierView view;
    view.id = tiler_id_;
    // Display and capability label only: nothing routes on it.
    view.type = memory_type_;
    view.capacity = block_pool_->Capacity();
    view.usage = block_pool_->Usage();
    view.free_space = view.capacity - std::min(view.capacity, view.usage);
    view.priority = priority_;
    view.tags = tags_;
    return view;
}

BlockPoolCapabilities TilerManager::Capabilities() const {
    return block_pool_->Capabilities();
}

bool TilerManager::IsTeAddressable() const {
    return block_pool_->Capabilities().te_addressable;
}

size_t TilerManager::Capacity() const { return block_pool_->Capacity(); }

size_t TilerManager::Usage() const { return block_pool_->Usage(); }

size_t TilerManager::FreeBytes() const {
    const size_t capacity = block_pool_->Capacity();
    const size_t usage = block_pool_->Usage();
    return capacity - std::min(capacity, usage);
}

BlockIndexStats TilerManager::IndexStats() const {
    return block_index_->Stats();
}

void TilerManager::SetEventPublisher(EventPublisher publisher) {
    event_publisher_ = std::move(publisher);
}

void TilerManager::Stop() { stopped_.store(true, std::memory_order_release); }

bool TilerManager::IsStopped() const {
    return stopped_.load(std::memory_order_acquire);
}

// ---------------------------------------------------------------------------
// MultiTiler
// ---------------------------------------------------------------------------

TilerManager* MultiTiler::Find(const UUID& tiler_id) const {
    auto it = by_id.find(tiler_id);
    return it == by_id.end() ? nullptr : it->second;
}

std::vector<TilerManager*> MultiTiler::TeAddressable() const {
    std::vector<TilerManager*> out;
    out.reserve(by_priority.size());
    for (const auto& tiler : by_priority) {
        if (tiler->IsTeAddressable()) out.push_back(tiler.get());
    }
    return out;
}

std::vector<TilerManager*> MultiTiler::All() const {
    std::vector<TilerManager*> out;
    out.reserve(by_priority.size());
    for (const auto& tiler : by_priority) out.push_back(tiler.get());
    return out;
}

void MultiTiler::Rebuild() {
    std::stable_sort(by_priority.begin(), by_priority.end(),
                     [](const std::unique_ptr<TilerManager>& lhs,
                        const std::unique_ptr<TilerManager>& rhs) {
                         return lhs->Priority() > rhs->Priority();
                     });
    by_id.clear();
    by_id.reserve(by_priority.size());
    for (const auto& tiler : by_priority) {
        by_id.emplace(tiler->Id(), tiler.get());
    }
}

}  // namespace mooncake::v2
