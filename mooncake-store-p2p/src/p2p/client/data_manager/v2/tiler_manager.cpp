#include "p2p/client/data_manager/v2/tiler_manager.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {

TilerManager::TilerManager(const LogicalTilerConfig& logical_config,
                           std::unique_ptr<BlockIndex> block_index,
                           std::shared_ptr<BlockPool> block_pool,
                           BlockRegistry block_registry,
                           EventPublisher event_publisher,
                           double offload_high_watermark)
    : tiler_id_(logical_config.tiler_id),
      memory_type_(logical_config.memory_type),
      priority_(logical_config.priority),
      offload_high_watermark_(offload_high_watermark),
      tags_(logical_config.tags),
      block_pool_(std::move(block_pool)),
      block_index_(std::move(block_index)),
      block_registry_(std::move(block_registry)),
      event_publisher_(std::move(event_publisher)) {
    CHECK(block_pool_ != nullptr) << "TilerManager requires a BlockPool";
    CHECK(block_index_ != nullptr) << "TilerManager requires a BlockIndex";
    CHECK(block_index_->TilerId() == tiler_id_)
        << "TilerManager requires an index for its own tiler";
}

namespace {

/** The non-owning identity of a committed block. */
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
    std::shared_lock<std::shared_mutex> mutation(replica_mutation_mu_);
    const size_t size_bytes = block.Size();
    auto inserted = block_index_->Insert(std::move(block), registration);
    if (!inserted) {
        return tl::make_unexpected(inserted.error());
    }
    if (replica_observer_) replica_observer_(registration, true);
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
    block_index_->OnAccess(
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

    std::shared_lock<std::shared_mutex> mutation(replica_mutation_mu_);
    // Detached under the shard lock, released here: if a reader still holds a
    // snapshot the physical block survives until that reader is done.
    BlockEntryPtr detached = block_index_->Erase(registration, expected_id);
    if (!detached) return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);

    const BlockId block_id = detached->block.id;
    const size_t size_bytes = detached->block.size_bytes;
    detached.reset();

    if (replica_observer_) replica_observer_(registration, false);
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
    std::unique_lock<std::shared_mutex> mutation(replica_mutation_mu_);
    auto detached = block_index_->Drain();
    if (replica_observer_) {
        for (const auto& entry : detached) {
            replica_observer_(entry->block.registration, false);
        }
    }
    return detached;
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

std::function<size_t()> TilerManager::UsageReader() const {
    return [weak = std::weak_ptr<BlockPool>(block_pool_)] {
        auto pool = weak.lock();
        return pool ? pool->Usage() : 0;
    };
}

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
