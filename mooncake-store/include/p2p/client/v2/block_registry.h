#pragma once

// BlockRegistry: the cross-tiler coordination layer for key registrations.
//
// It owns *identity*, never data. A registration is the canonical, deduplicated
// identity of a key; Blocks, replicas and physical resources belong to each
// TilerManager's BlockIndex. The Registry stores only weak pointers, so the
// last strong handle disappearing removes the key automatically.
//
// Mechanism mirrors Dynamo's kvbm-logical registry (see section 3.3):
//   - Register(key) reuses a live, non-retired registration or mints a new
//     identity (registry_shard, shard_sequence++).
//   - Deleting every replica retires the registration, so Match() stops
//     returning it and the next Register() mints a fresh identity. An async
//     command that still carries the old weak handle is therefore detectably
//     stale.
//   - The inner destructor removes the shard entry only when both the
//     registration id and the raw pointer identity still match, which is what
//     makes concurrent delete-then-recreate safe.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "types.h"
#include "utils.h"

namespace mooncake::v2 {

struct RegistryState;
struct BlockRegistrationHandleInner;
class BlockRegistry;
class WeakBlockRegistrationHandle;

/**
 * @struct BlockRegistryConfig
 */
struct BlockRegistryConfig {
    size_t shard_count = 64;
};

/**
 * @struct RegistrationId
 * @brief Identity of one registration. Unique for the lifetime of a
 *        RegistryState: `shard_sequence` must never wrap or be reused, so a
 *        stale async command can always be recognized.
 */
struct RegistrationId {
    uint32_t registry_shard = 0;
    uint64_t shard_sequence = 0;
    bool operator==(const RegistrationId&) const = default;
};

struct RegistrationIdHash {
    size_t operator()(const RegistrationId& id) const noexcept;
};

/**
 * @class RegistrationMutationGuard
 * @brief Serializes Register / Delete / Migrate on one key.
 *
 * Holding it keeps the identity alive (it owns a strong reference) and excludes
 * every other mutation of the same key. Lock order: this guard is taken before
 * any BlockIndex shard lock and released after it (section 7.3).
 */
class RegistrationMutationGuard {
   public:
    RegistrationMutationGuard(RegistrationMutationGuard&&) noexcept = default;
    RegistrationMutationGuard& operator=(RegistrationMutationGuard&&) noexcept =
        default;
    RegistrationMutationGuard(const RegistrationMutationGuard&) = delete;
    RegistrationMutationGuard& operator=(const RegistrationMutationGuard&) =
        delete;

    /** True once the registration has been retired by a delete. */
    bool IsRetired() const;
    bool OwnsLock() const { return lock_.owns_lock(); }

   private:
    friend class BlockRegistrationHandle;
    explicit RegistrationMutationGuard(
        std::shared_ptr<BlockRegistrationHandleInner> inner);

    std::shared_ptr<BlockRegistrationHandleInner> inner_;  // keeps identity
    std::unique_lock<std::mutex> lock_;
};

/**
 * @class BlockRegistrationHandle
 * @brief A strong reference to a key's registration identity.
 */
class BlockRegistrationHandle {
   public:
    BlockRegistrationHandle() = default;

    const std::string& Key() const;
    RegistrationId Id() const;
    bool IsRetired() const;

    /** Take the per-key mutation lock. */
    RegistrationMutationGuard LockMutation() const;

    /**
     * @brief Mark the registration dead. Must be called under a guard obtained
     *        from this same handle.
     */
    void Retire(const RegistrationMutationGuard& guard) const;

    // Presence markers are a hint ("which tilers might hold this key"), never
    // an authority. Anything needing an exact answer must ask the TilerManager.
    void MarkPresent(const UUID& tiler_id) const;
    void MarkAbsent(const UUID& tiler_id) const;
    std::vector<UUID> PresenceHint() const;

    WeakBlockRegistrationHandle Downgrade() const;
    explicit operator bool() const { return inner_ != nullptr; }

    /** Raw identity, compared but never dereferenced. */
    const BlockRegistrationHandleInner* IdentityPtr() const {
        return inner_.get();
    }

   private:
    friend class BlockRegistry;
    friend class WeakBlockRegistrationHandle;
    explicit BlockRegistrationHandle(
        std::shared_ptr<BlockRegistrationHandleInner> inner)
        : inner_(std::move(inner)) {}

    std::shared_ptr<BlockRegistrationHandleInner> inner_;
};

/**
 * @class WeakBlockRegistrationHandle
 * @brief What asynchronous commands carry. Upgrading can fail, which is the
 *        first of the three staleness checks (upgrade / retired / canonical).
 */
class WeakBlockRegistrationHandle {
   public:
    WeakBlockRegistrationHandle() = default;

    std::optional<BlockRegistrationHandle> Lock() const;
    RegistrationId Id() const { return registration_id_; }

   private:
    friend class BlockRegistrationHandle;
    // SnapshotShard hands out weak handles taken straight from the shard map.
    friend class BlockRegistry;
    WeakBlockRegistrationHandle(
        RegistrationId id, std::weak_ptr<BlockRegistrationHandleInner> inner)
        : registration_id_(id), inner_(std::move(inner)) {}

    RegistrationId registration_id_;
    std::weak_ptr<BlockRegistrationHandleInner> inner_;
};

/**
 * @struct BlockRegistrationHandleInner
 * @brief The reference-counted identity itself.
 */
struct BlockRegistrationHandleInner {
    std::string key;
    const RegistrationId registration_id;
    std::weak_ptr<RegistryState> registry;
    std::atomic<bool> retired{false};
    // Serializes Register / Delete / Migrate for this key.
    mutable std::mutex mutation_mu;
    // Guards presence_tilers only; taken after the mutation guard and the
    // BlockIndex shard lock have been released.
    mutable std::mutex attachments_mu;
    std::unordered_set<UUID, boost::hash<UUID>> presence_tilers;

    BlockRegistrationHandleInner(std::string key_in, RegistrationId id,
                                 std::weak_ptr<RegistryState> registry_in)
        : key(std::move(key_in)),
          registration_id(id),
          registry(std::move(registry_in)) {}

    ~BlockRegistrationHandleInner();
};

/**
 * @struct RegistryEntry
 */
struct RegistryEntry {
    RegistrationId registration_id;
    std::weak_ptr<BlockRegistrationHandleInner> handle;
    // Compared for identity only; never dereferenced. Guards against a late
    // destructor erasing the entry of a newer registration for the same key.
    const BlockRegistrationHandleInner* identity_ptr = nullptr;
};

struct RegistryShard {
    mutable std::shared_mutex mu;
    uint64_t next_local_sequence = 1;  // only under this shard's unique lock
    std::unordered_map<std::string, RegistryEntry, StringHash, std::equal_to<>>
        entries;
};

struct RegistryState {
    explicit RegistryState(size_t shard_count);
    std::vector<std::unique_ptr<RegistryShard>> shards;
};

/**
 * @struct RegistryKeySnapshot
 * @brief One entry of a shard snapshot, taken so a walk never holds the shard
 *        lock while touching a TilerManager.
 */
struct RegistryKeySnapshot {
    std::string key;
    WeakBlockRegistrationHandle registration;
};

/**
 * @class BlockRegistry
 * @brief A cheap, copyable handle onto a shared RegistryState. Every
 *        TilerManager holds one; there is no global lock and no global counter.
 */
class BlockRegistry {
   public:
    BlockRegistry() = default;
    explicit BlockRegistry(const BlockRegistryConfig& config);

    /**
     * @brief Return the canonical registration for `key`, creating it if the
     *        key is unknown or its previous registration has been retired.
     * @return INTERNAL_ERROR if this shard's sequence would overflow. Reusing
     *         a sequence could collide with an in-flight async command, so the
     *         registry refuses rather than wraps.
     */
    tl::expected<BlockRegistrationHandle, ErrorCode> Register(
        std::string_view key) const;

    /** @return the live, non-retired registration for `key`, if any. */
    std::optional<BlockRegistrationHandle> Match(std::string_view key) const;

    /** True if `handle` is still the registration stored for its key. */
    bool IsCanonical(const BlockRegistrationHandle& handle) const;

    std::vector<RegistryKeySnapshot> SnapshotShard(size_t shard_id) const;
    size_t ShardCount() const;

    /** Live entries across all shards. Test/metric use only. */
    size_t SizeForTest() const;

    explicit operator bool() const { return state_ != nullptr; }

   private:
    size_t ShardFor(std::string_view key) const;

    std::shared_ptr<RegistryState> state_;
};

}  // namespace mooncake::v2
