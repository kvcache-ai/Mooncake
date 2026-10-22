// Component tests for TilerManager and MultiTiler.
//
// The point of a logical tiler is that it knows nothing about hardware. Note
// that no assertion below mentions a NUMA node, a device path or a file
// offset: those appear only where the pool configuration is built, and never
// cross the TilerManager surface. A cross-medium replica is created here
// through the generic Read/Write path, which is exactly why a slow tier can
// hold data without ever exposing an address.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "p2p/client/v2/block_pool.h"
#include "p2p/client/v2/event_center.h"
#include "p2p/client/v2/local_copy_engine.h"
#include "p2p/client/v2/tiler_manager.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

constexpr size_t kBlockSize = 8192;

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

/**
 * @class RecordingSink
 * @brief Captures the facts a tiler publishes, so a test can assert on them
 *        without any queue or worker being involved.
 */
class RecordingSink final : public EventSink {
   public:
    QueuePushResult Publish(BlockEvent event) override {
        std::lock_guard<std::mutex> lock(mu_);
        events_.push_back(std::move(event));
        return QueuePushResult::kEnqueued;
    }

    std::vector<BlockEvent> Snapshot() const {
        std::lock_guard<std::mutex> lock(mu_);
        return events_;
    }

    size_t CountOf(EventType type) const {
        std::lock_guard<std::mutex> lock(mu_);
        size_t count = 0;
        for (const auto& event : events_) {
            if (event.type == type) ++count;
        }
        return count;
    }

   private:
    mutable std::mutex mu_;
    std::vector<BlockEvent> events_;
};

}  // namespace

class TilerManagerTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("TilerManagerTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        storage_dir_ = std::filesystem::temp_directory_path() /
                       ("mooncake_v2_tiler_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(storage_dir_);
        std::filesystem::create_directories(storage_dir_);

        registry_ = BlockRegistry(BlockRegistryConfig{/*shard_count=*/8});
        sink_ = std::make_shared<RecordingSink>();

        fast_ = MakeDramTiler(/*priority=*/100, 8ULL * 1024 * 1024);
        slow_ = MakeSsdTiler(/*priority=*/10, 8ULL * 1024 * 1024);
        ASSERT_NE(fast_, nullptr);
        ASSERT_NE(slow_, nullptr);
    }

    void TearDown() override {
        slow_.reset();
        fast_.reset();
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
        return std::make_unique<TilerManager>(logical, BlockIndexConfig{},
                                              std::move(pool.value()),
                                              registry_, EventPublisher(sink_));
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
                                              registry_, EventPublisher(sink_));
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

    std::filesystem::path storage_dir_;
    BlockRegistry registry_;
    std::shared_ptr<RecordingSink> sink_;
    std::unique_ptr<TilerManager> fast_;
    std::unique_ptr<TilerManager> slow_;
    LocalCopyEngine copier_{LocalTransferConfig{}};
};

// ---------------------------------------------------------------------------
// Identity and capabilities
// ---------------------------------------------------------------------------

// A tiler's identity is its UUID, not its medium: two tilers of the same type
// are independent layers, and MemoryType is only a label on the view.
TEST_F(TilerManagerTest, IdentityIsTheUuidAndMediumIsOnlyALabel) {
    auto second_dram = MakeDramTiler(/*priority=*/50, 1024 * 1024);
    ASSERT_NE(second_dram, nullptr);
    EXPECT_NE(fast_->Id(), second_dram->Id());
    EXPECT_EQ(fast_->Medium(), second_dram->Medium());

    const TierView view = fast_->GetView();
    EXPECT_EQ(view.id, fast_->Id());
    EXPECT_EQ(view.type, MemoryType::DRAM);
    EXPECT_EQ(view.capacity, 8ULL * 1024 * 1024);
    EXPECT_EQ(view.priority, 100);
    EXPECT_EQ(view.GetName(), MakeTierSegmentName(fast_->Id()));
    ASSERT_EQ(view.tags.size(), 1U);
    EXPECT_EQ(view.tags[0], "fast");
}

TEST_F(TilerManagerTest, OnlyTheFastTierCanExposeAnAddress) {
    EXPECT_TRUE(fast_->IsTeAddressable());
    EXPECT_TRUE(fast_->Capabilities().direct_cpu_access);
    EXPECT_FALSE(fast_->Capabilities().persistent);

    EXPECT_FALSE(slow_->IsTeAddressable());
    EXPECT_FALSE(slow_->Capabilities().direct_cpu_access);
    EXPECT_TRUE(slow_->Capabilities().persistent);
}

// ---------------------------------------------------------------------------
// Cross-tier replicas under one registration
// ---------------------------------------------------------------------------

// The whole point of RegisterWithHandle: replicas on different tiers share a
// single registration identity, so an async command that names that identity
// refers unambiguously to one logical object.
TEST_F(TilerManagerTest, ReplicasAcrossTiersShareOneRegistration) {
    const std::string key = "tiler_shared_registration";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x21);

    ImmutableBlock fast_block = Commit(*fast_, key, payload);
    ASSERT_TRUE(static_cast<bool>(fast_block));

    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    EXPECT_EQ(fast_block.Registration(), registration->Id());

    // Copy across the medium boundary using only the generic handle
    // interface: the destination never has to be addressable.
    auto slow_allocation = slow_->Allocate(payload.size());
    ASSERT_TRUE(slow_allocation.has_value())
        << toString(slow_allocation.error());
    ASSERT_TRUE(copier_.Copy(fast_block, slow_allocation.value()).has_value());
    auto slow_completed = std::move(slow_allocation.value()).Complete(key);
    ASSERT_TRUE(slow_completed.has_value());

    auto slow_block = slow_->RegisterWithHandle(
        std::move(slow_completed.value()), *registration);
    ASSERT_TRUE(slow_block.has_value()) << toString(slow_block.error());

    // One identity, two blocks, two tiers.
    EXPECT_EQ(slow_block->Registration(), fast_block.Registration());
    EXPECT_NE(slow_block->Id(), fast_block.Id());
    EXPECT_EQ(slow_block->Id().tiler_id, slow_->Id());
    EXPECT_EQ(fast_block.Id().tiler_id, fast_->Id());

    // The bytes really made it across.
    std::vector<uint8_t> readback(payload.size(), 0);
    ASSERT_TRUE(slow_block->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);

    EXPECT_TRUE(fast_->Match(*registration).has_value());
    EXPECT_TRUE(slow_->Match(*registration).has_value());
}

TEST_F(TilerManagerTest, DeletingOneReplicaLeavesTheOtherAndTheRegistration) {
    const std::string key = "tiler_partial_delete";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x33);

    ImmutableBlock fast_block = Commit(*fast_, key, payload);
    ASSERT_TRUE(static_cast<bool>(fast_block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    auto slow_allocation = slow_->Allocate(payload.size());
    ASSERT_TRUE(slow_allocation.has_value());
    ASSERT_TRUE(copier_.Copy(fast_block, slow_allocation.value()).has_value());
    auto slow_completed = std::move(slow_allocation.value()).Complete(key);
    ASSERT_TRUE(slow_completed.has_value());
    ASSERT_TRUE(slow_
                    ->RegisterWithHandle(std::move(slow_completed.value()),
                                         *registration)
                    .has_value());

    ASSERT_TRUE(fast_->Delete(*registration).has_value());
    EXPECT_FALSE(fast_->Match(*registration).has_value());

    // The registration survives because a replica does, and the surviving copy
    // is still readable.
    ASSERT_TRUE(registry_.Match(key).has_value());
    auto surviving = slow_->Match(*registration);
    ASSERT_TRUE(surviving.has_value());
    std::vector<uint8_t> readback(payload.size(), 0);
    ASSERT_TRUE(surviving->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);
}

// A stale policy token names a block that may already have been replaced;
// acting on it would drop live data.
TEST_F(TilerManagerTest, DeleteWithAMismatchedBlockIdIsRefused) {
    const std::string key = "tiler_stale_token";
    ImmutableBlock block = Commit(*fast_, key, Pattern(kBlockSize, 0x44));
    ASSERT_TRUE(static_cast<bool>(block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    BlockId wrong = block.Id();
    wrong.local_id += 1;
    auto refused = fast_->Delete(*registration, wrong);
    ASSERT_FALSE(refused.has_value());
    EXPECT_EQ(refused.error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_TRUE(fast_->Match(*registration).has_value());

    ASSERT_TRUE(fast_->Delete(*registration, block.Id()).has_value());
    EXPECT_FALSE(fast_->Match(*registration).has_value());
}

TEST_F(TilerManagerTest, RegisteringTheSameRegistrationTwiceIsRefused) {
    const std::string key = "tiler_duplicate";
    ImmutableBlock block = Commit(*fast_, key, Pattern(kBlockSize, 0x55));
    ASSERT_TRUE(static_cast<bool>(block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    const size_t before = fast_->Usage();
    auto allocated = fast_->Allocate(kBlockSize);
    ASSERT_TRUE(allocated.has_value());
    auto completed = std::move(allocated.value()).Complete(key);
    ASSERT_TRUE(completed.has_value());

    auto duplicate =
        fast_->RegisterWithHandle(std::move(completed.value()), *registration);
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    // The rejected block's space went straight back to the pool.
    EXPECT_EQ(fast_->Usage(), before);
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

TEST_F(TilerManagerTest, CommitAndDeletePublishFactsNamingTheirTier) {
    const std::string key = "tiler_events";
    ImmutableBlock block = Commit(*fast_, key, Pattern(kBlockSize, 0x66));
    ASSERT_TRUE(static_cast<bool>(block));

    EXPECT_EQ(sink_->CountOf(EventType::kCommit), 1U);
    for (const auto& event : sink_->Snapshot()) {
        if (event.type != EventType::kCommit) continue;
        EXPECT_EQ(event.key, key);
        EXPECT_EQ(event.tiler_id, fast_->Id());
        EXPECT_EQ(event.size_bytes, kBlockSize);
        ASSERT_TRUE(event.block_id.has_value());
        EXPECT_EQ(*event.block_id, block.Id());
        ASSERT_TRUE(event.registration.has_value());
        EXPECT_EQ(event.registration->Id(), block.Registration());
    }

    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());
    ASSERT_TRUE(fast_->Delete(*registration).has_value());
    EXPECT_EQ(sink_->CountOf(EventType::kDelete), 1U);
}

// Exhaustion is a fact worth reporting, but it must not be turned into a
// different answer: the allocation still fails.
TEST_F(TilerManagerTest, AllocationFailurePublishesAFactAndStillFails) {
    auto tiny = MakeDramTiler(/*priority=*/1, 1024 * 1024);
    ASSERT_NE(tiny, nullptr);

    auto oversized = tiny->Allocate(64ULL * 1024 * 1024);
    ASSERT_FALSE(oversized.has_value());
    EXPECT_EQ(oversized.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_GE(sink_->CountOf(EventType::kAllocationFailure), 1U);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Stop() rejects new work but keeps serving the committed index: Exist() above
// returns a bool and cannot say "shutting down", so a read that answered
// "absent" here would be a wrong answer, not a rejection.
TEST_F(TilerManagerTest, StopRejectsNewWorkButKeepsServingReads) {
    const std::string key = "tiler_stop";
    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x77);
    ImmutableBlock block = Commit(*fast_, key, payload);
    ASSERT_TRUE(static_cast<bool>(block));
    auto registration = registry_.Match(key);
    ASSERT_TRUE(registration.has_value());

    fast_->Stop();
    EXPECT_TRUE(fast_->IsStopped());

    auto allocated = fast_->Allocate(kBlockSize);
    ASSERT_FALSE(allocated.has_value());
    EXPECT_EQ(allocated.error(), ErrorCode::SHUTTING_DOWN);

    auto matched = fast_->Match(*registration);
    ASSERT_TRUE(matched.has_value());
    std::vector<uint8_t> readback(payload.size(), 0);
    ASSERT_TRUE(matched->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, payload);
}

TEST_F(TilerManagerTest, DrainAllEmptiesTheIndexAndReleasesTheSpace) {
    for (int i = 0; i < 8; ++i) {
        ImmutableBlock block =
            Commit(*fast_, "tiler_drain_" + std::to_string(i),
                   Pattern(kBlockSize, static_cast<uint8_t>(i)));
        ASSERT_TRUE(static_cast<bool>(block));
    }
    EXPECT_EQ(fast_->IndexStats().entry_count, 8U);
    EXPECT_GT(fast_->Usage(), 0u);

    auto drained = fast_->DrainAll();
    EXPECT_EQ(drained.size(), 8U);
    EXPECT_EQ(fast_->IndexStats().entry_count, 0U);
    // Still held by the returned pointers: the caller decides when the space
    // actually goes back.
    EXPECT_GT(fast_->Usage(), 0u);
    drained.clear();
    EXPECT_EQ(fast_->Usage(), 0u);
}

// ---------------------------------------------------------------------------
// The eviction index beside the BlockIndex
// ---------------------------------------------------------------------------

// The safety-net scan the evict engine runs: one shard at a time, so it never
// holds more than one index lock. A per-shard snapshot is authoritative for
// its own shard only -- if Reconcile treats it as proof about the whole tier,
// walking the shards in turn leaves just the last shard's blocks, and a block
// the ordering has forgotten is one nothing offers as a victim again.
TEST_F(TilerManagerTest, ReconcilingShardByShardKeepsTheWholeTierOrdered) {
    constexpr size_t kKeys = 12;
    for (size_t i = 0; i < kKeys; ++i) {
        ImmutableBlock block =
            Commit(*fast_, "tiler_snapshot_" + std::to_string(i),
                   Pattern(kBlockSize, static_cast<uint8_t>(i)));
        ASSERT_TRUE(static_cast<bool>(block));
    }
    ASSERT_EQ(fast_->Eviction()->Stats().tracked_blocks, kKeys);

    for (size_t shard = 0; shard < fast_->ShardCount(); ++shard) {
        const BlockIndexSnapshot snapshot = fast_->SnapshotTokens(shard);
        EXPECT_EQ(snapshot.shard_id, shard);
        EXPECT_EQ(snapshot.shard_count, fast_->ShardCount());
        EXPECT_FALSE(snapshot.complete);
        fast_->Eviction()->Reconcile(snapshot);
    }
    EXPECT_EQ(fast_->Eviction()->Stats().tracked_blocks, kKeys);

    // And the whole-tier form, which is the only one that can report the
    // ordering whole again.
    const BlockIndexSnapshot everything = fast_->SnapshotAllTokens();
    EXPECT_TRUE(everything.complete);
    EXPECT_EQ(everything.entries.size(), kKeys);
    fast_->Eviction()->Reconcile(everything);
    EXPECT_EQ(fast_->Eviction()->Stats().tracked_blocks, kKeys);
    EXPECT_FALSE(fast_->Eviction()->NeedsReconcile());
}

// ---------------------------------------------------------------------------
// MultiTiler
// ---------------------------------------------------------------------------

TEST_F(TilerManagerTest, MultiTilerOrdersByPriorityAndIndexesByUuid) {
    MultiTiler tilers;
    // Deliberately inserted low-priority first, to prove Rebuild() sorts.
    const UUID slow_id = slow_->Id();
    const UUID fast_id = fast_->Id();
    tilers.by_priority.push_back(std::move(slow_));
    tilers.by_priority.push_back(std::move(fast_));
    tilers.Rebuild();

    ASSERT_EQ(tilers.Size(), 2U);
    EXPECT_EQ(tilers.by_priority[0]->Id(), fast_id);
    EXPECT_EQ(tilers.by_priority[1]->Id(), slow_id);

    ASSERT_NE(tilers.Find(fast_id), nullptr);
    ASSERT_NE(tilers.Find(slow_id), nullptr);
    EXPECT_EQ(tilers.Find(UUID{0xdead, 0xbeef}), nullptr);

    // Only the addressable tier may receive a request-path allocation, which
    // is what keeps slow-tier capacity an offload destination rather than a
    // write fallback.
    auto addressable = tilers.TeAddressable();
    ASSERT_EQ(addressable.size(), 1U);
    EXPECT_EQ(addressable[0]->Id(), fast_id);
    EXPECT_EQ(tilers.All().size(), 2U);
}

}  // namespace mooncake::v2
