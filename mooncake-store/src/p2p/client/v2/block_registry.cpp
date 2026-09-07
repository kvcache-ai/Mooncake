#include "p2p/client/v2/block_registry.h"

#include <algorithm>
#include <limits>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {

size_t RegistrationIdHash::operator()(const RegistrationId& id) const noexcept {
    size_t seed = std::hash<uint64_t>{}(id.shard_sequence);
    boost::hash_combine(seed, id.registry_shard);
    return seed;
}

RegistryState::RegistryState(size_t shard_count) {
    shards.reserve(shard_count);
    for (size_t i = 0; i < shard_count; ++i) {
        shards.push_back(std::make_unique<RegistryShard>());
    }
}

// ---------------------------------------------------------------------------
// BlockRegistrationHandleInner
// ---------------------------------------------------------------------------

BlockRegistrationHandleInner::~BlockRegistrationHandleInner() {
    // Last strong reference is gone, so the key can leave the registry -- but
    // only if the stored entry is still *this* registration. A concurrent
    // delete-then-recreate may already have replaced it, and erasing then
    // would silently drop a live registration. Both the id and the raw pointer
    // are compared; the pointer is never dereferenced (we are inside its own
    // destructor, and a recreated inner could in principle reuse the address,
    // which is why the id is checked too).
    auto state = registry.lock();
    if (!state) return;

    auto& shard = *state->shards[registration_id.registry_shard];
    std::unique_lock<std::shared_mutex> lock(shard.mu);
    auto it = shard.entries.find(key);
    if (it == shard.entries.end()) return;
    if (it->second.registration_id == registration_id &&
        it->second.identity_ptr == this) {
        shard.entries.erase(it);
    }
}

// ---------------------------------------------------------------------------
// RegistrationMutationGuard
// ---------------------------------------------------------------------------

RegistrationMutationGuard::RegistrationMutationGuard(
    std::shared_ptr<BlockRegistrationHandleInner> inner)
    : inner_(std::move(inner)) {
    if (inner_) {
        lock_ = std::unique_lock<std::mutex>(inner_->mutation_mu);
    }
}

bool RegistrationMutationGuard::IsRetired() const {
    return inner_ == nullptr || inner_->retired.load(std::memory_order_acquire);
}

// ---------------------------------------------------------------------------
// BlockRegistrationHandle
// ---------------------------------------------------------------------------

namespace {
const std::string& EmptyKey() {
    static const std::string kEmpty;
    return kEmpty;
}
}  // namespace

const std::string& BlockRegistrationHandle::Key() const {
    return inner_ ? inner_->key : EmptyKey();
}

RegistrationId BlockRegistrationHandle::Id() const {
    return inner_ ? inner_->registration_id : RegistrationId{};
}

bool BlockRegistrationHandle::IsRetired() const {
    return inner_ == nullptr || inner_->retired.load(std::memory_order_acquire);
}

RegistrationMutationGuard BlockRegistrationHandle::LockMutation() const {
    return RegistrationMutationGuard(inner_);
}

void BlockRegistrationHandle::Retire(
    const RegistrationMutationGuard& guard) const {
    // The guard argument is what documents (and, in a debug build, checks)
    // that retirement happens under this key's serialization point.
    DCHECK(guard.OwnsLock()) << "Retire requires a held mutation guard";
    (void)guard;
    if (inner_) {
        inner_->retired.store(true, std::memory_order_release);
    }
}

void BlockRegistrationHandle::MarkPresent(const UUID& tiler_id) const {
    if (!inner_) return;
    std::lock_guard<std::mutex> lock(inner_->attachments_mu);
    inner_->presence_tilers.insert(tiler_id);
}

void BlockRegistrationHandle::MarkAbsent(const UUID& tiler_id) const {
    if (!inner_) return;
    std::lock_guard<std::mutex> lock(inner_->attachments_mu);
    inner_->presence_tilers.erase(tiler_id);
}

std::vector<UUID> BlockRegistrationHandle::PresenceHint() const {
    std::vector<UUID> out;
    if (!inner_) return out;
    std::lock_guard<std::mutex> lock(inner_->attachments_mu);
    out.assign(inner_->presence_tilers.begin(), inner_->presence_tilers.end());
    return out;
}

WeakBlockRegistrationHandle BlockRegistrationHandle::Downgrade() const {
    if (!inner_) return WeakBlockRegistrationHandle();
    return WeakBlockRegistrationHandle(inner_->registration_id, inner_);
}

std::optional<BlockRegistrationHandle> WeakBlockRegistrationHandle::Lock()
    const {
    auto strong = inner_.lock();
    if (!strong) return std::nullopt;
    return BlockRegistrationHandle(std::move(strong));
}

// ---------------------------------------------------------------------------
// BlockRegistry
// ---------------------------------------------------------------------------

BlockRegistry::BlockRegistry(const BlockRegistryConfig& config)
    : state_(std::make_shared<RegistryState>(
          std::max<size_t>(1, config.shard_count))) {}

size_t BlockRegistry::ShardCount() const {
    return state_ ? state_->shards.size() : 0;
}

size_t BlockRegistry::ShardFor(std::string_view key) const {
    return StringHash{}(key) % state_->shards.size();
}

tl::expected<BlockRegistrationHandle, ErrorCode> BlockRegistry::Register(
    std::string_view key) const {
    if (!state_) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (key.empty()) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    const size_t shard_id = ShardFor(key);
    auto& shard = *state_->shards[shard_id];

    // Declared before the lock so that, if this is the last strong reference
    // to a retired inner, the destructor (which takes the same shard lock)
    // runs after the lock below has been released.
    std::shared_ptr<BlockRegistrationHandleInner> existing;
    std::shared_ptr<BlockRegistrationHandleInner> created;
    {
        std::unique_lock<std::shared_mutex> lock(shard.mu);
        auto it = shard.entries.find(key);
        if (it != shard.entries.end()) {
            existing = it->second.handle.lock();
            if (existing &&
                !existing->retired.load(std::memory_order_acquire)) {
                return BlockRegistrationHandle(existing);
            }
        }

        // Sequences are never reused inside one RegistryState: an async
        // command holding an old identity must stay distinguishable forever.
        // Refusing is the only safe answer on overflow.
        if (shard.next_local_sequence == std::numeric_limits<uint64_t>::max()) {
            LOG(ERROR) << "BlockRegistry shard " << shard_id
                       << " exhausted its registration sequence";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        RegistrationId id;
        id.registry_shard = static_cast<uint32_t>(shard_id);
        id.shard_sequence = shard.next_local_sequence++;

        created = std::make_shared<BlockRegistrationHandleInner>(
            std::string(key), id, state_);

        RegistryEntry entry;
        entry.registration_id = id;
        entry.handle = created;  // weak only: the registry never owns identity
        entry.identity_ptr = created.get();
        shard.entries.insert_or_assign(std::string(key), std::move(entry));
    }
    return BlockRegistrationHandle(std::move(created));
}

std::optional<BlockRegistrationHandle> BlockRegistry::Match(
    std::string_view key) const {
    if (!state_ || key.empty()) return std::nullopt;

    const size_t shard_id = ShardFor(key);
    auto& shard = *state_->shards[shard_id];

    // Same ordering rule as Register: upgrade under the lock, but let the
    // strong pointer die outside it.
    std::shared_ptr<BlockRegistrationHandleInner> strong;
    {
        std::shared_lock<std::shared_mutex> lock(shard.mu);
        auto it = shard.entries.find(key);
        if (it == shard.entries.end()) return std::nullopt;
        strong = it->second.handle.lock();
    }
    if (!strong || strong->retired.load(std::memory_order_acquire)) {
        return std::nullopt;
    }
    return BlockRegistrationHandle(std::move(strong));
}

bool BlockRegistry::IsCanonical(const BlockRegistrationHandle& handle) const {
    if (!state_ || !handle) return false;

    const size_t shard_id = handle.Id().registry_shard;
    if (shard_id >= state_->shards.size()) return false;
    auto& shard = *state_->shards[shard_id];

    std::shared_lock<std::shared_mutex> lock(shard.mu);
    auto it = shard.entries.find(handle.Key());
    if (it == shard.entries.end()) return false;
    return it->second.registration_id == handle.Id() &&
           it->second.identity_ptr == handle.IdentityPtr();
}

std::vector<RegistryKeySnapshot> BlockRegistry::SnapshotShard(
    size_t shard_id) const {
    std::vector<RegistryKeySnapshot> out;
    if (!state_ || shard_id >= state_->shards.size()) return out;

    auto& shard = *state_->shards[shard_id];
    std::shared_lock<std::shared_mutex> lock(shard.mu);
    out.reserve(shard.entries.size());
    for (const auto& [key, entry] : shard.entries) {
        // Weak only: the snapshot must not keep a registration alive, and the
        // caller upgrades after releasing this lock.
        out.push_back(RegistryKeySnapshot{
            key,
            WeakBlockRegistrationHandle(entry.registration_id, entry.handle)});
    }
    return out;
}

size_t BlockRegistry::SizeForTest() const {
    if (!state_) return 0;
    size_t total = 0;
    for (const auto& shard : state_->shards) {
        std::shared_lock<std::shared_mutex> lock(shard->mu);
        total += shard->entries.size();
    }
    return total;
}

}  // namespace mooncake::v2
