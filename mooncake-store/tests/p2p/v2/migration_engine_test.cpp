// Component tests for MigrationEngine.
//
// A movement command is a proposal, built from approximate policy state and
// executed some time later. Most of what is pinned here is therefore refusal:
// each of the three staleness checks must abort the command before a single
// byte is written, leaving the source exactly as it was and nothing at all on
// the destination. The two successful shapes differ in one point only --
// kReplicate keeps the source, kMigrate drops it once the destination is
// matchable -- and that difference is visible in the metadata callbacks too.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <chrono>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block_pool.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/event_center.h"
#include "p2p/client/v2/local_copy_engine.h"
#include "p2p/client/v2/migration_engine.h"
#include "p2p/client/v2/tiler_manager.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

/**
 * @class ManualClock
 * @brief Time only moves when a test says so, which is what makes deadline
 *        behaviour assertable instead of timing-dependent.
 */
class ManualClock final : public Clock {
   public:
    time_point Now() const override {
        std::lock_guard<std::mutex> lock(mu_);
        return now_;
    }
    void Advance(std::chrono::milliseconds delta) {
        std::lock_guard<std::mutex> lock(mu_);
        now_ += delta;
    }

   private:
    mutable std::mutex mu_;
    time_point now_{std::chrono::steady_clock::time_point{} +
                    std::chrono::hours(1)};
};

constexpr size_t kBlockSize = 8192;
constexpr size_t kTierCapacity = 8ULL * 1024 * 1024;

std::vector<uint8_t> Pattern(size_t size, uint8_t seed) {
    std::vector<uint8_t> out(size);
    for (size_t i = 0; i < size; ++i) {
        out[i] = static_cast<uint8_t>((i * 31 + seed) & 0xff);
    }
    return out;
}

std::span<const std::byte> AsBytes(const std::vector<uint8_t>& data) {
    return {reinterpret_cast<const std::byte*>(data.data()), data.size()};
}

std::span<std::byte> AsWritableBytes(std::vector<uint8_t>& data) {
    return {reinterpret_cast<std::byte*>(data.data()), data.size()};
}

/** One metadata-callback invocation, recorded verbatim. */
struct ReplicaCall {
    std::string key;
    UUID tiler_id{0, 0};
    size_t size_bytes = 0;
};

/** One trip through the policy-aware allocation path. */
struct AllocateCall {
    UUID tiler_id{0, 0};
    size_t size_bytes = 0;
    AllocationSource source = AllocationSource::kPut;
};

}  // namespace

class MigrationEngineTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("MigrationEngineTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        storage_dir_ = std::filesystem::temp_directory_path() /
                       ("mooncake_v2_migration_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(storage_dir_);
        std::filesystem::create_directories(storage_dir_);

        registry_ = BlockRegistry(BlockRegistryConfig{/*shard_count=*/8});

        auto fast = MakeDramTiler(/*priority=*/100, kTierCapacity);
        auto slow = MakeSsdTiler(/*priority=*/10, kTierCapacity);
        ASSERT_NE(fast, nullptr);
        ASSERT_NE(slow, nullptr);
        const UUID fast_id = fast->Id();
        const UUID slow_id = slow->Id();
        tilers_.by_priority.push_back(std::move(fast));
        tilers_.by_priority.push_back(std::move(slow));
        tilers_.Rebuild();
        fast_ = tilers_.Find(fast_id);
        slow_ = tilers_.Find(slow_id);
        ASSERT_NE(fast_, nullptr);
        ASSERT_NE(slow_, nullptr);
        // The offload direction under test: an addressable tier to one that
        // exposes no address at all.
        ASSERT_TRUE(fast_->IsTeAddressable());
        ASSERT_FALSE(slow_->IsTeAddressable());

        callbacks_.add_replica =
            [this](std::string_view key, const UUID& tier_id,
                   size_t size) -> tl::expected<void, ErrorCode> {
            added_.push_back(ReplicaCall{std::string(key), tier_id, size});
            if (add_replica_probe_) add_replica_probe_();
            return {};
        };
        callbacks_.remove_replica =
            [this](std::string_view key,
                   const UUID& tier_id) -> tl::expected<void, ErrorCode> {
            removed_.push_back(ReplicaCall{std::string(key), tier_id, 0});
            return {};
        };

        engine_ = std::make_unique<MigrationEngine>(
            &tilers_, &registry_, &copier_, &callbacks_,
            [this](const UUID& tiler_id, size_t size, size_t alignment,
                   AllocationSource source)
                -> tl::expected<MutableBlock, ErrorCode> {
                allocations_.push_back(AllocateCall{tiler_id, size, source});
                // A seam for making time pass mid-execution, which is the
                // only way to test the post-copy deadline check without a
                // real sleep.
                if (on_allocate_) on_allocate_();
                if (allocate_failure_.has_value()) {
                    return tl::make_unexpected(*allocate_failure_);
                }
                TilerManager* tiler = tilers_.Find(tiler_id);
                if (tiler == nullptr) {
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                return tiler->Allocate(size, alignment);
            },
            clock_);
    }

    void TearDown() override {
        engine_.reset();
        fast_ = nullptr;
        slow_ = nullptr;
        tilers_.by_id.clear();
        tilers_.by_priority.clear();
        std::filesystem::remove_all(storage_dir_);
    }

    std::unique_ptr<TilerManager> MakeDramTiler(int32_t priority,
                                                size_t capacity) {
        DramArenaConfig arena;
        arena.capacity_bytes = capacity;
        DramBlockPoolConfig pool_config;
        pool_config.arenas.push_back(arena);

        auto pool = CreateBlockPool(BlockPoolConfig(pool_config),
                                    std::shared_ptr<TransferEngine>{});
        if (!pool) {
            ADD_FAILURE() << "CreateBlockPool(DRAM) failed: "
                          << toString(pool.error());
            return nullptr;
        }
        LogicalTilerConfig logical;
        logical.tiler_id = generate_uuid();
        logical.memory_type = MemoryType::DRAM;
        logical.priority = priority;
        logical.tags = {"fast"};
        // No event sink: nothing in this file asserts on facts, and the engine
        // publishes none of its own.
        return std::make_unique<TilerManager>(logical, BlockIndexConfig{},
                                              std::move(pool.value()),
                                              registry_, EventPublisher());
    }

    std::unique_ptr<TilerManager> MakeSsdTiler(int32_t priority,
                                               size_t capacity) {
        SSDDeviceConfig device;
        device.file_path = (storage_dir_ / "slow.data").string();
        device.capacity_bytes = capacity;
        SSDBlockPoolConfig pool_config;
        pool_config.devices.push_back(device);

        auto pool = CreateBlockPool(BlockPoolConfig(pool_config),
                                    std::shared_ptr<TransferEngine>{});
        if (!pool) {
            ADD_FAILURE() << "CreateBlockPool(SSD) failed: "
                          << toString(pool.error());
            return nullptr;
        }
        LogicalTilerConfig logical;
        logical.tiler_id = generate_uuid();
        logical.memory_type = MemoryType::NVME;
        logical.priority = priority;
        logical.tags = {"slow"};
        return std::make_unique<TilerManager>(logical, BlockIndexConfig{},
                                              std::move(pool.value()),
                                              registry_, EventPublisher());
    }

    /** Allocate, fill and register `key` on `tiler`. */
    ImmutableBlock Commit(TilerManager& tiler, const std::string& key,
                          const std::vector<uint8_t>& payload) {
        auto allocated = tiler.Allocate(payload.size());
        EXPECT_TRUE(allocated.has_value());
        if (!allocated) return ImmutableBlock();
        EXPECT_TRUE(allocated->Write(0, AsBytes(payload)).has_value());
        auto completed = std::move(allocated.value()).Complete(key);
        EXPECT_TRUE(completed.has_value());
        if (!completed) return ImmutableBlock();
        auto registered = tiler.Register(key, std::move(completed.value()));
        EXPECT_TRUE(registered.has_value());
        if (!registered) return ImmutableBlock();
        return std::move(registered.value());
    }

    std::shared_ptr<ManualClock> clock_ = std::make_shared<ManualClock>();
    std::function<void()> on_allocate_;

    MovementRequest MakeRequest(MovementKind kind, const std::string& key,
                                const BlockRegistrationHandle& registration,
                                BlockId source_block_id, TilerManager* source,
                                TilerManager* destination) {
        MovementRequest request;
        request.kind = kind;
        request.key = key;
        request.source_tiler = source->Id();
        request.destination_tiler = destination->Id();
        request.source_block_id = source_block_id;
        // Weak, like every queued command: upgrading it is staleness check 1.
        request.registration = registration.Downgrade();
        return request;
    }

    std::vector<uint8_t> ReadAll(const ImmutableBlock& block) {
        std::vector<uint8_t> out(block.Size(), 0);
        EXPECT_TRUE(block.Read(0, AsWritableBytes(out)).has_value());
        return out;
    }

    /** What every rejected command must leave behind: nothing. */
    void ExpectDestinationUntouched() const {
        EXPECT_EQ(slow_->IndexStats().entry_count, 0U);
        EXPECT_EQ(slow_->Usage(), 0U);
        EXPECT_TRUE(added_.empty());
        EXPECT_TRUE(removed_.empty());
    }

    std::filesystem::path storage_dir_;
    BlockRegistry registry_;
    MultiTiler tilers_;
    TilerManager* fast_ = nullptr;
    TilerManager* slow_ = nullptr;
    LocalCopyEngine copier_{LocalTransferConfig{}};
    MetadataCallbacks callbacks_;
    std::vector<ReplicaCall> added_;
    std::vector<ReplicaCall> removed_;
    std::vector<AllocateCall> allocations_;
    std::optional<ErrorCode> allocate_failure_;
    std::function<void()> add_replica_probe_;
    std::unique_ptr<MigrationEngine> engine_;
};

TEST_F(MigrationEngineTest, ReplicateCopiesTheBytesAndKeepsBothReplicas) {
    const std::string key = "migration_replicate";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x11);

    ImmutableBlock source_block = Commit(*fast_, key, payload);
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    const BlockId source_id = source_block.Id();
    source_block = ImmutableBlock();

    auto result = engine_->Execute(MakeRequest(
        MovementKind::kReplicate, key, *registration, source_id, fast_, slow_));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    auto on_fast = fast_->Match(*registration);
    ASSERT_TRUE(on_fast.has_value()) << "a replicate must keep its source";
    auto on_slow = slow_->Match(*registration);
    ASSERT_TRUE(on_slow.has_value()) << toString(on_slow.error());
    EXPECT_EQ(on_fast->Id(), source_id);

    // One identity, two tiers: a later command naming this registration is
    // still unambiguous, which is what RegisterWithHandle exists for.
    EXPECT_EQ(on_slow->Registration(), registration->Id());
    EXPECT_EQ(on_slow->Registration(), on_fast->Registration());
    EXPECT_NE(on_slow->Id(), on_fast->Id());
    EXPECT_EQ(on_slow->Id().tiler_id, slow_->Id());

    EXPECT_EQ(ReadAll(*on_fast), payload);
    EXPECT_EQ(ReadAll(*on_slow), payload);

    // kMigration is the only allocation source permitted to target a tier
    // that exposes no address, so the engine must not borrow a request-path
    // source to get its destination block.
    ASSERT_EQ(allocations_.size(), 1U);
    EXPECT_EQ(allocations_[0].tiler_id, slow_->Id());
    EXPECT_EQ(allocations_[0].size_bytes, payload.size());
    EXPECT_EQ(allocations_[0].source, AllocationSource::kMigration);

    const MigrationStats stats = engine_->Stats();
    EXPECT_EQ(stats.executed, 1U);
    EXPECT_EQ(stats.succeeded, 1U);
}

TEST_F(MigrationEngineTest, MigrateRemovesTheSourceOnceTheDestinationIsUp) {
    const std::string key = "migration_migrate";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x22);

    ImmutableBlock source_block = Commit(*fast_, key, payload);
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    const BlockId source_id = source_block.Id();
    source_block = ImmutableBlock();

    // Probed from inside the callback, which runs after the index mutations:
    // the object must never stop existing somewhere, so by the time the move
    // is published the destination has to be matchable already.
    bool destination_live_when_published = false;
    bool source_gone_when_published = false;
    add_replica_probe_ = [&] {
        destination_live_when_published =
            slow_->Match(*registration).has_value();
        source_gone_when_published = !fast_->Match(*registration).has_value();
    };

    auto result = engine_->Execute(MakeRequest(
        MovementKind::kMigrate, key, *registration, source_id, fast_, slow_));
    ASSERT_TRUE(result.has_value()) << toString(result.error());
    EXPECT_TRUE(destination_live_when_published);
    EXPECT_TRUE(source_gone_when_published);

    auto on_slow = slow_->Match(*registration);
    ASSERT_TRUE(on_slow.has_value()) << toString(on_slow.error());
    EXPECT_EQ(ReadAll(*on_slow), payload);

    auto on_fast = fast_->Match(*registration);
    ASSERT_FALSE(on_fast.has_value());
    EXPECT_EQ(on_fast.error(), ErrorCode::OBJECT_NOT_FOUND);
    // The source replica is gone, but the key itself survives on the slow
    // tier: a migrate must not retire the registration.
    EXPECT_TRUE(registry_.Match(key).has_value());
    EXPECT_EQ(engine_->Stats().succeeded, 1U);
}

TEST_F(MigrationEngineTest, ReplicateAnnouncesTheNewReplicaAndRemovesNone) {
    const std::string key = "migration_replicate_callbacks";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x33);

    ImmutableBlock source_block = Commit(*fast_, key, payload);
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    const BlockId source_id = source_block.Id();
    source_block = ImmutableBlock();

    auto result = engine_->Execute(MakeRequest(
        MovementKind::kReplicate, key, *registration, source_id, fast_, slow_));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    ASSERT_EQ(added_.size(), 1U);
    EXPECT_EQ(added_[0].key, key);
    EXPECT_EQ(added_[0].tiler_id, slow_->Id());
    EXPECT_EQ(added_[0].size_bytes, payload.size());
    // Master would drop a route to a replica that is still there.
    EXPECT_TRUE(removed_.empty());
}

TEST_F(MigrationEngineTest, MigrateAnnouncesTheDestinationAndTheLostSource) {
    const std::string key = "migration_migrate_callbacks";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x44);

    ImmutableBlock source_block = Commit(*fast_, key, payload);
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    const BlockId source_id = source_block.Id();
    source_block = ImmutableBlock();

    auto result = engine_->Execute(MakeRequest(
        MovementKind::kMigrate, key, *registration, source_id, fast_, slow_));
    ASSERT_TRUE(result.has_value()) << toString(result.error());

    ASSERT_EQ(added_.size(), 1U);
    EXPECT_EQ(added_[0].key, key);
    EXPECT_EQ(added_[0].tiler_id, slow_->Id());
    EXPECT_EQ(added_[0].size_bytes, payload.size());
    ASSERT_EQ(removed_.size(), 1U);
    EXPECT_EQ(removed_[0].key, key);
    // Naming the source tier, not the destination: swapping them would strand
    // the only surviving route.
    EXPECT_EQ(removed_[0].tiler_id, fast_->Id());
}

// Check 1. The registry owns identity weakly, so losing the last replica also
// loses the last strong handle; a command still carrying the weak one has to
// notice rather than copy from a block that no longer exists.
TEST_F(MigrationEngineTest, ARegistrationThatCannotBeUpgradedIsRejected) {
    const std::string key = "migration_upgrade_fails";
    const std::string neighbour_key = "migration_neighbour";
    const std::vector<uint8_t> neighbour_payload = Pattern(kBlockSize, 0x56);

    MovementRequest request;
    {
        ImmutableBlock source_block =
            Commit(*fast_, key, Pattern(kBlockSize, 0x55));
        ASSERT_TRUE(static_cast<bool>(source_block));
        auto registration = registry_.Match(key);
        ASSERT_TRUE(registration.has_value());
        request = MakeRequest(MovementKind::kMigrate, key, *registration,
                              source_block.Id(), fast_, slow_);
        source_block = ImmutableBlock();
        ASSERT_TRUE(fast_->Delete(*registration).has_value());
    }
    ImmutableBlock neighbour = Commit(*fast_, neighbour_key, neighbour_payload);
    ASSERT_TRUE(static_cast<bool>(neighbour));
    ASSERT_FALSE(request.registration.Lock().has_value());

    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);

    const MigrationStats stats = engine_->Stats();
    EXPECT_EQ(stats.executed, 1U);
    EXPECT_EQ(stats.stale, 1U);
    EXPECT_EQ(stats.succeeded, 0U);
    EXPECT_TRUE(allocations_.empty());
    ExpectDestinationUntouched();

    // The rest of the source tier is exactly as it was.
    auto neighbour_registration = registry_.Match(neighbour_key);
    ASSERT_TRUE(neighbour_registration.has_value());
    auto still_there = fast_->Match(*neighbour_registration);
    ASSERT_TRUE(still_there.has_value());
    EXPECT_EQ(ReadAll(*still_there), neighbour_payload);
}

// Check 2. Delete-then-recreate mints a fresh identity for the same key. The
// old one still upgrades while anything holds it, so only the retired flag and
// the canonical comparison can tell that it now names a detached object.
TEST_F(MigrationEngineTest, ARetiredAndRecreatedRegistrationIsRejected) {
    const std::string key = "migration_recreated";
    const std::vector<uint8_t> replacement = Pattern(kBlockSize, 0x67);

    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0x66));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto old_registration = registry_.Match(key);
    ASSERT_TRUE(old_registration.has_value());
    const BlockId old_id = source_block.Id();
    source_block = ImmutableBlock();
    auto request = MakeRequest(MovementKind::kReplicate, key, *old_registration,
                               old_id, fast_, slow_);

    // A full delete of the last replica: retire under the mutation guard
    // first, then drop the block.
    {
        auto mutation = old_registration->LockMutation();
        old_registration->Retire(mutation);
    }
    ASSERT_TRUE(fast_->Delete(*old_registration, old_id).has_value());

    ImmutableBlock recreated = Commit(*fast_, key, replacement);
    ASSERT_TRUE(static_cast<bool>(recreated));
    auto new_registration = registry_.Match(key);
    ASSERT_TRUE(new_registration.has_value());
    EXPECT_NE(new_registration->Id(), old_registration->Id());

    // This test deliberately keeps the old identity alive, so the upgrade
    // still succeeds and the rejection is attributable to check 2 alone.
    ASSERT_TRUE(request.registration.Lock().has_value());
    EXPECT_TRUE(old_registration->IsRetired());
    EXPECT_FALSE(registry_.IsCanonical(*old_registration));

    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(engine_->Stats().stale, 1U);
    EXPECT_TRUE(allocations_.empty());
    ExpectDestinationUntouched();

    // Copying here would have published the deleted object's bytes under the
    // recreated key; the recreated one is untouched instead.
    auto current = fast_->Match(*new_registration);
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(ReadAll(*current), replacement);
}

// Check 3. The identity can be alive and canonical while the block under it
// has been replaced, and only the BlockId comparison catches that.
TEST_F(MigrationEngineTest, ASourceBlockIdThatNoLongerMatchesIsRejected) {
    const std::string key = "migration_rewritten";
    const std::vector<uint8_t> rewritten = Pattern(kBlockSize, 0x78);

    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0x77));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    const BlockId stale_id = source_block.Id();
    source_block = ImmutableBlock();
    auto request = MakeRequest(MovementKind::kMigrate, key, *registration,
                               stale_id, fast_, slow_);

    // Rewritten in place: same key, same registration, a different block.
    ASSERT_TRUE(fast_->Delete(*registration, stale_id).has_value());
    auto allocated = fast_->Allocate(rewritten.size());
    ASSERT_TRUE(allocated.has_value());
    ASSERT_TRUE(allocated->Write(0, AsBytes(rewritten)).has_value());
    auto completed = std::move(allocated.value()).Complete(key);
    ASSERT_TRUE(completed.has_value());
    auto fresh =
        fast_->RegisterWithHandle(std::move(completed.value()), *registration);
    ASSERT_TRUE(fresh.has_value()) << toString(fresh.error());
    EXPECT_NE(fresh->Id(), stale_id);

    // Nothing about the identity changed, so checks 1 and 2 both pass.
    EXPECT_FALSE(registration->IsRetired());
    EXPECT_TRUE(registry_.IsCanonical(*registration));

    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(engine_->Stats().stale, 1U);
    EXPECT_TRUE(allocations_.empty());
    ExpectDestinationUntouched();

    auto current = fast_->Match(*registration);
    ASSERT_TRUE(current.has_value());
    EXPECT_EQ(current->Id(), fresh->Id());
    EXPECT_EQ(ReadAll(*current), rewritten);
}

// The tiler set is fixed at initialization, so an unknown tier means the
// command outlived the topology it was planned against.
TEST_F(MigrationEngineTest, AnUnknownSourceTierIsRejected) {
    const std::string key = "migration_unknown_source";
    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0x88));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    auto request = MakeRequest(MovementKind::kMigrate, key, *registration,
                               source_block.Id(), fast_, slow_);
    request.source_tiler = UUID{0xdead, 0xbeef};

    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(engine_->Stats().stale, 1U);
    EXPECT_TRUE(fast_->Match(*registration).has_value());
    ExpectDestinationUntouched();
}

TEST_F(MigrationEngineTest, AnUnknownDestinationTierIsRejected) {
    const std::string key = "migration_unknown_dest";
    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0x99));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    auto request = MakeRequest(MovementKind::kMigrate, key, *registration,
                               source_block.Id(), fast_, slow_);
    request.destination_tiler = UUID{0xdead, 0xbeef};

    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(engine_->Stats().stale, 1U);
    EXPECT_TRUE(allocations_.empty());
    EXPECT_TRUE(fast_->Match(*registration).has_value());
    ExpectDestinationUntouched();
}

// A destination-less command is a planner bug, not a lost race: counting it as
// stale would hide the bug behind an expected outcome.
TEST_F(MigrationEngineTest, ACommandWithoutADestinationIsRejected) {
    const std::string key = "migration_no_dest";
    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0xaa));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    auto request = MakeRequest(MovementKind::kReplicate, key, *registration,
                               source_block.Id(), fast_, slow_);
    request.destination_tiler = UUID{0, 0};

    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);

    const MigrationStats stats = engine_->Stats();
    EXPECT_EQ(stats.executed, 1U);
    EXPECT_EQ(stats.stale, 0U);
    EXPECT_TRUE(fast_->Match(*registration).has_value());
    ExpectDestinationUntouched();
}

// A full destination tier is routine; it must cost the caller nothing beyond
// the skipped offload.
TEST_F(MigrationEngineTest, ADestinationAllocationFailureLeavesTheSourceAlone) {
    const std::string key = "migration_alloc_failure";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0xbb);

    ImmutableBlock source_block = Commit(*fast_, key, payload);
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    const BlockId source_id = source_block.Id();
    source_block = ImmutableBlock();

    allocate_failure_ = ErrorCode::NO_AVAILABLE_HANDLE;
    auto result = engine_->Execute(MakeRequest(
        MovementKind::kMigrate, key, *registration, source_id, fast_, slow_));
    ASSERT_FALSE(result.has_value());
    // The allocator's own error is reported, not a generic failure: the
    // caller distinguishes "no room" from "the object is gone".
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);

    const MigrationStats stats = engine_->Stats();
    EXPECT_EQ(stats.executed, 1U);
    EXPECT_EQ(stats.allocate_failed, 1U);
    EXPECT_EQ(stats.stale, 0U);
    EXPECT_EQ(stats.succeeded, 0U);
    EXPECT_EQ(allocations_.size(), 1U);

    auto on_fast = fast_->Match(*registration);
    ASSERT_TRUE(on_fast.has_value());
    EXPECT_EQ(on_fast->Id(), source_id);
    EXPECT_EQ(ReadAll(*on_fast), payload);
    ExpectDestinationUntouched();
}

TEST_F(MigrationEngineTest, StatsCountEveryOutcomeExactlyOnce) {
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0xcc);

    const std::string moved_key = "migration_stats_moved";
    ImmutableBlock moved = Commit(*fast_, moved_key, payload);
    ASSERT_TRUE(static_cast<bool>(moved));
    auto moved_registration = registry_.Match(moved_key);
    ASSERT_TRUE(moved_registration.has_value());
    const BlockId moved_id = moved.Id();
    moved = ImmutableBlock();
    auto moved_result = engine_->Execute(
        MakeRequest(MovementKind::kReplicate, moved_key, *moved_registration,
                    moved_id, fast_, slow_));
    ASSERT_TRUE(moved_result.has_value()) << toString(moved_result.error());

    MovementRequest stale;
    {
        const std::string stale_key = "migration_stats_stale";
        ImmutableBlock block = Commit(*fast_, stale_key, payload);
        ASSERT_TRUE(static_cast<bool>(block));
        auto registration = registry_.Match(stale_key);
        ASSERT_TRUE(registration.has_value());
        stale = MakeRequest(MovementKind::kReplicate, stale_key, *registration,
                            block.Id(), fast_, slow_);
        block = ImmutableBlock();
        ASSERT_TRUE(fast_->Delete(*registration).has_value());
    }
    EXPECT_FALSE(engine_->Execute(stale).has_value());

    const std::string starved_key = "migration_stats_starved";
    ImmutableBlock starved = Commit(*fast_, starved_key, payload);
    ASSERT_TRUE(static_cast<bool>(starved));
    auto starved_registration = registry_.Match(starved_key);
    ASSERT_TRUE(starved_registration.has_value());
    const BlockId starved_id = starved.Id();
    starved = ImmutableBlock();
    allocate_failure_ = ErrorCode::NO_AVAILABLE_HANDLE;
    auto starved_result = engine_->Execute(
        MakeRequest(MovementKind::kReplicate, starved_key,
                    *starved_registration, starved_id, fast_, slow_));
    EXPECT_FALSE(starved_result.has_value());
    allocate_failure_.reset();

    auto malformed =
        MakeRequest(MovementKind::kReplicate, starved_key,
                    *starved_registration, starved_id, fast_, slow_);
    malformed.destination_tiler = UUID{0, 0};
    EXPECT_FALSE(engine_->Execute(malformed).has_value());

    // Every command is counted once, and each failure lands in exactly one
    // bucket -- otherwise the counters cannot be used to tell a busy tier
    // apart from a racy one.
    const MigrationStats stats = engine_->Stats();
    EXPECT_EQ(stats.executed, 4U);
    EXPECT_EQ(stats.succeeded, 1U);
    EXPECT_EQ(stats.stale, 1U);
    EXPECT_EQ(stats.allocate_failed, 1U);
    EXPECT_EQ(stats.copy_failed, 0U);
    EXPECT_EQ(stats.register_failed, 0U);
}

// Publishing the copy after the key was deleted would resurrect data the
// caller removed, and the caller has no way to delete it a second time.
TEST_F(MigrationEngineTest, DeletingTheKeyInFlightCreatesNothingDownstream) {
    const std::string key = "migration_deleted_in_flight";

    MovementRequest request;
    {
        ImmutableBlock source_block =
            Commit(*fast_, key, Pattern(kBlockSize, 0xdd));
        ASSERT_TRUE(static_cast<bool>(source_block));
        auto registration = registry_.Match(key);
        ASSERT_TRUE(registration.has_value());
        request = MakeRequest(MovementKind::kReplicate, key, *registration,
                              source_block.Id(), fast_, slow_);
        source_block = ImmutableBlock();

        // The command is already queued when the caller deletes the key.
        {
            auto mutation = registration->LockMutation();
            registration->Retire(mutation);
        }
        ASSERT_TRUE(fast_->Delete(*registration).has_value());
    }
    ASSERT_FALSE(registry_.Match(key).has_value());

    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_EQ(engine_->Stats().stale, 1U);
    EXPECT_TRUE(allocations_.empty());
    ExpectDestinationUntouched();
    EXPECT_EQ(fast_->IndexStats().entry_count, 0U);
    // The identity itself is gone once its last replica and handle are.
    EXPECT_EQ(registry_.SizeForTest(), 0U);
}

// ---------------------------------------------------------------------------
// Deadlines
// ---------------------------------------------------------------------------

// The deadline field was populated with now+30s by the planner and read
// nowhere, so a command queued behind a long backlog would still allocate,
// copy and publish however late it ran.
TEST_F(MigrationEngineTest, AnExpiredCommandIsAbandonedBeforeItAllocates) {
    const std::string key = "migrate/deadline/before";
    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0x31));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    auto request = MakeRequest(MovementKind::kReplicate, key, *registration,
                               source_block.Id(), fast_, slow_);
    request.deadline = clock_->Now() + std::chrono::seconds(5);
    clock_->Advance(std::chrono::seconds(6));

    const size_t allocations_before = allocations_.size();
    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::TRANSFER_FAIL);
    // Nothing was attempted: no allocation, no copy, no replica anywhere else.
    EXPECT_EQ(allocations_.size(), allocations_before);
    EXPECT_FALSE(slow_->Match(*registration).has_value());
    EXPECT_TRUE(fast_->Match(*registration).has_value());
    EXPECT_EQ(engine_->Stats().deadline_exceeded, 1U);
    // An expired command is not a stale one: the proposal was still valid,
    // it simply ran too late, and an operator needs to tell those apart.
    EXPECT_EQ(engine_->Stats().stale, 0U);
}

// A deadline that passes while a large block is being copied must still stop
// the move before it becomes visible, or the caller has already given up and
// may have arranged the move some other way.
TEST_F(MigrationEngineTest, ADeadlineThatPassesDuringTheCopyStopsTheMove) {
    const std::string key = "migrate/deadline/during";
    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0x32));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    auto request = MakeRequest(MovementKind::kMigrate, key, *registration,
                               source_block.Id(), fast_, slow_);
    // Still in the future when Execute starts; the allocate callback pushes
    // the clock past it, standing in for a copy that took too long.
    request.deadline = clock_->Now() + std::chrono::seconds(5);
    on_allocate_ = [this] { clock_->Advance(std::chrono::seconds(6)); };

    auto result = engine_->Execute(request);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(engine_->Stats().deadline_exceeded, 1U);
    // The destination never became visible and, this being a migrate, the
    // source was never removed -- the object still exists exactly once.
    EXPECT_FALSE(slow_->Match(*registration).has_value());
    EXPECT_TRUE(fast_->Match(*registration).has_value());
    EXPECT_TRUE(added_.empty());
    EXPECT_TRUE(removed_.empty());
}

// A command with no deadline is unbounded on purpose: background warming has
// no caller waiting on it, and inventing a deadline would silently cancel work
// the planner never limited.
TEST_F(MigrationEngineTest, ACommandWithNoDeadlineIsNeverExpired) {
    const std::string key = "migrate/deadline/none";
    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0x33));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    auto request = MakeRequest(MovementKind::kReplicate, key, *registration,
                               source_block.Id(), fast_, slow_);
    ASSERT_FALSE(request.deadline.has_value());
    clock_->Advance(std::chrono::hours(24));

    ASSERT_TRUE(engine_->Execute(request).has_value());
    EXPECT_EQ(engine_->Stats().deadline_exceeded, 0U);
    EXPECT_TRUE(slow_->Match(*registration).has_value());
}

// ---------------------------------------------------------------------------
// Route queues, batching and fairness
// ---------------------------------------------------------------------------

class MigrationSchedulerTest : public MigrationEngineTest {
   protected:
    void SetUp() override {
        MigrationEngineTest::SetUp();
        tracker_ =
            std::make_unique<MovementTracker>(MovementTrackerConfig{}, clock_);
        // Small bounds so a handful of requests reaches a trigger. The
        // production defaults are sized for a real workload, and a test that
        // queued four items against a batch of sixteen would simply block:
        // RunOnce is supposed to wait for a batch, and on a manual clock the
        // delay trigger never arrives on its own.
        MigrationSchedulerConfig scheduler;
        scheduler.max_batch_items = 4;
        scheduler.max_batch_bytes = 64ULL * 1024 * 1024;
        scheduler.max_batch_delay = std::chrono::milliseconds(20);
        Rebuild(scheduler);
    }

    /** Replace the engine with one using `scheduler`. */
    void Rebuild(const MigrationSchedulerConfig& scheduler) {
        engine_ = std::make_unique<MigrationEngine>(
            &tilers_, &registry_, &copier_, &callbacks_,
            [this](const UUID& tiler_id, size_t size, size_t alignment,
                   AllocationSource source)
                -> tl::expected<MutableBlock, ErrorCode> {
                allocations_.push_back(AllocateCall{tiler_id, size, source});
                if (on_allocate_) on_allocate_();
                if (allocate_failure_.has_value()) {
                    return tl::make_unexpected(*allocate_failure_);
                }
                TilerManager* tiler = tilers_.Find(tiler_id);
                if (tiler == nullptr) {
                    return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
                }
                return tiler->Allocate(size, alignment);
            },
            clock_, scheduler);
    }

    /** A lease for a request, taken the way a consumer takes one. */
    MovementLease LeaseFor(const MovementRequest& request) {
        MovementDedupKey dedup;
        dedup.registration_id = request.registration.Id();
        dedup.source_block_id = request.source_block_id;
        dedup.source_tiler = request.source_tiler;
        dedup.destination_tiler = request.destination_tiler;
        auto lease = tracker_->TryAcquire(request.key, dedup,
                                          MovementDirection::kOffload);
        CHECK(lease.has_value()) << "test setup: the tracker refused a lease";
        return std::move(lease.value());
    }

    /** A request that will be rejected as stale, so no data has to exist. */
    MovementRequest StaleRequest(const std::string& key, size_t length,
                                 MovementPriority priority, const UUID& source,
                                 const UUID& destination) {
        MovementRequest request;
        request.kind = MovementKind::kMigrate;
        request.key = key;
        request.source_tiler = source;
        request.destination_tiler = destination;
        request.source_block_id = BlockId{source, next_block_++, 1};
        request.length = length;
        request.priority = priority;
        request.route.kind = MovementKind::kMigrate;
        request.route.source_tiler = source;
        request.route.destination_tiler = destination;
        return request;
    }

    bool Submit(MovementRequest request) {
        MovementLease lease = LeaseFor(request);
        return engine_->Enqueue(std::move(request), std::move(lease));
    }

    std::unique_ptr<MovementTracker> tracker_;
    uint64_t next_block_ = 1;
};

// The three triggers the design names, each on its own. Without all three a
// route either waits for company that never comes or submits one item at a
// time forever.
TEST_F(MigrationSchedulerTest, ABatchFormsWhenEnoughItemsAreQueued) {
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(Submit(StaleRequest("items/" + std::to_string(i), 16,
                                        MovementPriority::kBackground,
                                        fast_->Id(), slow_->Id())));
    }
    EXPECT_EQ(engine_->QueuedCount(), 4U);
    EXPECT_EQ(engine_->RunOnce(), 4U);
    EXPECT_EQ(engine_->QueuedCount(), 0U);
    EXPECT_EQ(engine_->Stats().batches, 1U);
    EXPECT_EQ(engine_->Stats().batches_by_items, 1U);
}

TEST_F(MigrationSchedulerTest, ABatchFormsWhenEnoughBytesAreQueued) {
    // Two large items reach the byte bound before the item bound of four.
    ASSERT_TRUE(Submit(StaleRequest("bytes/0", 40ULL * 1024 * 1024,
                                    MovementPriority::kBackground, fast_->Id(),
                                    slow_->Id())));
    ASSERT_TRUE(Submit(StaleRequest("bytes/1", 40ULL * 1024 * 1024,
                                    MovementPriority::kBackground, fast_->Id(),
                                    slow_->Id())));
    EXPECT_GT(engine_->RunOnce(), 0U);
    EXPECT_EQ(engine_->Stats().batches_by_bytes, 1U);
    EXPECT_EQ(engine_->Stats().batches_by_items, 0U);
}

// The one that keeps a quiet route alive. The delay is measured from the
// oldest request's arrival; timing it from the end of the previous batch is
// what makes a lone request wait behind whatever the busy routes are doing.
TEST_F(MigrationSchedulerTest, ASingleRequestGoesOnceItsDelayExpires) {
    ASSERT_TRUE(Submit(StaleRequest("lonely", 16, MovementPriority::kBackground,
                                    fast_->Id(), slow_->Id())));
    // Not ready yet on the manual clock.
    EXPECT_EQ(engine_->QueuedCount(), 1U);
    clock_->Advance(std::chrono::milliseconds(100));
    EXPECT_EQ(engine_->RunOnce(), 1U);
    EXPECT_EQ(engine_->Stats().batches_by_delay, 1U);
}

// Foreground work has a caller blocked on capacity; a warm-up batch must not
// go first just because its route happened to be next in the rotation.
TEST_F(MigrationSchedulerTest, ForegroundWorkIsServedBeforeBackgroundWork) {
    // Background first, and on a different route so both are ready.
    ASSERT_TRUE(
        Submit(StaleRequest("background", 16, MovementPriority::kBackground,
                            fast_->Id(), slow_->Id())));
    ASSERT_TRUE(
        Submit(StaleRequest("foreground", 16, MovementPriority::kForeground,
                            slow_->Id(), fast_->Id())));
    clock_->Advance(std::chrono::milliseconds(100));

    // The engine must pick the foreground route even though the background one
    // was queued first and sits earlier in the rotation.
    EXPECT_EQ(engine_->RunOnce(), 1U);
    ASSERT_EQ(engine_->QueuedCount(), 1U);

    // Named by route, not by kind: both of these are kMigrate, so a check on
    // the kind alone would pass whatever the engine chose.
    bool background_still_queued = false;
    bool foreground_still_queued = false;
    for (const auto& route : engine_->Routes()) {
        if (route.queued_items == 0) continue;
        if (route.route.source_tiler == fast_->Id()) {
            background_still_queued = true;
        }
        if (route.route.source_tiler == slow_->Id()) {
            foreground_still_queued = true;
        }
    }
    EXPECT_TRUE(background_still_queued)
        << "the background route was served first";
    EXPECT_FALSE(foreground_still_queued)
        << "the foreground route is still waiting behind background work";
}

// One busy route must not hold every worker while its neighbours wait.
TEST_F(MigrationSchedulerTest, RoutesAreServedInRotation) {
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(Submit(StaleRequest("busy/" + std::to_string(i), 16,
                                        MovementPriority::kBackground,
                                        fast_->Id(), slow_->Id())));
    }
    ASSERT_TRUE(Submit(StaleRequest("quiet", 16, MovementPriority::kBackground,
                                    slow_->Id(), fast_->Id())));
    clock_->Advance(std::chrono::milliseconds(100));

    // First pass takes one route, second pass must take the other.
    const size_t first = engine_->RunOnce();
    const size_t second = engine_->RunOnce();
    EXPECT_GT(first, 0U);
    EXPECT_GT(second, 0U);
    EXPECT_EQ(engine_->QueuedCount(), 0U)
        << "one route was served twice while the other waited";
}

// Every queued command holds a dedup claim. Dropping the queue at shutdown
// without settling them would leave those keys permanently "in flight", so
// nothing would ever propose them again.
TEST_F(MigrationSchedulerTest, StopSettlesEveryQueuedLease) {
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(Submit(StaleRequest("shutdown/" + std::to_string(i), 16,
                                        MovementPriority::kBackground,
                                        fast_->Id(), slow_->Id())));
    }
    EXPECT_EQ(tracker_->Stats().inflight, 3U);

    engine_->Stop();
    EXPECT_EQ(engine_->QueuedCount(), 0U);
    EXPECT_EQ(tracker_->Stats().inflight, 0U)
        << "a discarded command left its dedup claim behind";
    // A discarded move did not happen, so it must not start a cooldown either.
    EXPECT_EQ(tracker_->Stats().settled_moved, 0U);
    EXPECT_EQ(tracker_->Stats().settled_unmoved, 3U);
    EXPECT_EQ(engine_->RunOnce(), 0U);
}

// A refused submission is an exit path like any other.
TEST_F(MigrationSchedulerTest, ARefusedSubmissionSettlesItsLease) {
    engine_->Stop();
    MovementRequest request = StaleRequest(
        "refused", 16, MovementPriority::kBackground, fast_->Id(), slow_->Id());
    MovementLease lease = LeaseFor(request);
    EXPECT_FALSE(engine_->Enqueue(std::move(request), std::move(lease)));
    EXPECT_EQ(tracker_->Stats().inflight, 0U);
}

// Executed items report independently: a stale one must not abort the batch.
TEST_F(MigrationSchedulerTest, EachItemInABatchGetsItsOwnOutcome) {
    const std::string key = "batch/live";
    ImmutableBlock source_block =
        Commit(*fast_, key, Pattern(kBlockSize, 0x91));
    ASSERT_TRUE(static_cast<bool>(source_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    std::vector<MovementRequest> batch;
    batch.push_back(MakeRequest(MovementKind::kReplicate, key, *registration,
                                source_block.Id(), fast_, slow_));
    batch.push_back(StaleRequest("batch/stale", 16,
                                 MovementPriority::kBackground, fast_->Id(),
                                 slow_->Id()));

    const auto results = engine_->ExecuteBatch(batch);
    ASSERT_EQ(results.size(), 2U);
    EXPECT_TRUE(results[0].has_value()) << "a stale neighbour aborted a live "
                                           "item";
    EXPECT_FALSE(results[1].has_value());
    EXPECT_TRUE(slow_->Match(*registration).has_value());
}

// Route labels are a metric dimension, so their cardinality has to be bounded
// by the topology rather than by the workload.
TEST_F(MigrationSchedulerTest, RouteStatsAreLabelledByRouteNotByKey) {
    ASSERT_TRUE(Submit(StaleRequest("secret-key-name", 4096,
                                    MovementPriority::kBackground, fast_->Id(),
                                    slow_->Id())));
    const auto routes = engine_->Routes();
    ASSERT_EQ(routes.size(), 1U);
    EXPECT_EQ(routes[0].queued_items, 1U);
    EXPECT_EQ(routes[0].queued_bytes, 4096U);
    EXPECT_EQ(routes[0].label.find("secret-key-name"), std::string::npos)
        << "a key reached a metric label: " << routes[0].label;
}

}  // namespace mooncake::v2
