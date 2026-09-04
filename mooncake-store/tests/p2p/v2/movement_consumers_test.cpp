// Component tests for the two stateless movement consumers.
//
// These carry the offload and onboard cases that used to live in
// placement_policy_test.cpp, restated for one decision per event and for a
// frequency that comes from the tracker rather than from a counter the policy
// kept for itself. The single most important case here is the one that could
// not be written before: a write must not count as demand.

#include "p2p/client/v2/movement_consumers.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "p2p/client/v2/block_pool.h"
#include "p2p/client/v2/migration_engine.h"
#include "p2p/client/v2/tiler_manager.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

using namespace std::chrono_literals;

constexpr size_t kBlockSize = 64 * 1024;
constexpr size_t kFastCapacity = 512 * 1024;
constexpr size_t kSlowCapacity = 4 * 1024 * 1024;

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
    time_point now_{std::chrono::steady_clock::time_point{} + 1h};
};

/** Records what the consumers decided, without executing anything. */
class RecordingSink final : public MovementSink {
   public:
    bool Enqueue(MovementRequest request, MovementLease lease) override {
        std::lock_guard<std::mutex> lock(mu_);
        requests_.push_back(std::move(request));
        // Held, not settled: a queued command is still in flight, and letting
        // it settle here would hide every dedup effect the tests check.
        leases_.push_back(std::move(lease));
        return true;
    }

    std::vector<MovementRequest> Requests() const {
        std::lock_guard<std::mutex> lock(mu_);
        return requests_;
    }
    size_t Count() const {
        std::lock_guard<std::mutex> lock(mu_);
        return requests_.size();
    }
    void SettleAll(bool moved) {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& lease : leases_) lease.Settle(moved);
        leases_.clear();
    }

   private:
    mutable std::mutex mu_;
    std::vector<MovementRequest> requests_;
    std::vector<MovementLease> leases_;
};

class AcceptingSink final : public EventSink {
   public:
    QueuePushResult Publish(BlockEvent) override {
        return QueuePushResult::kEnqueued;
    }
};

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

}  // namespace

class MovementConsumersTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("MovementConsumersTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        storage_dir_ = std::filesystem::temp_directory_path() /
                       ("mooncake_v2_consumers_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(storage_dir_);
        std::filesystem::create_directories(storage_dir_);

        clock_ = std::make_shared<ManualClock>();
        registry_ = BlockRegistry(BlockRegistryConfig{/*shard_count=*/8});
        sink_ = std::make_shared<AcceptingSink>();

        auto fast = MakeDramTiler(/*priority=*/100, kFastCapacity);
        auto slow = MakeSsdTiler(/*priority=*/10, kSlowCapacity);
        ASSERT_NE(fast, nullptr);
        ASSERT_NE(slow, nullptr);
        fast_ = fast.get();
        slow_ = slow.get();
        tilers_.by_priority.push_back(std::move(fast));
        tilers_.by_priority.push_back(std::move(slow));
        tilers_.Rebuild();
        for (auto* tiler : tilers_.All()) {
            tiler->SetEventPublisher(EventPublisher(sink_));
        }

        std::vector<TierNode> nodes;
        for (auto* tiler : tilers_.All()) {
            TierNode node;
            node.tiler_id = tiler->Id();
            node.priority = tiler->Priority();
            node.capacity = tiler->Capacity();
            node.addressable = tiler->IsTeAddressable();
            node.domain = tiler->Capabilities().direct_cpu_access
                              ? CopyDomain::kHostMemory
                              : CopyDomain::kFileOrBlock;
            nodes.push_back(node);
        }
        auto graph = TierGraph::FromPriorityChain(std::move(nodes));
        ASSERT_TRUE(graph.has_value()) << toString(graph.error());
        graph_ = std::make_shared<const TierGraph>(std::move(graph.value()));

        auto placement =
            CreateTierPlacementPolicy(TierPlacementPolicyConfig{}, graph_);
        ASSERT_TRUE(placement.has_value());
        placement_ = std::move(placement.value());

        frequency_ = std::make_shared<FrequencyTracker>(
            FrequencyTrackerConfig{}, clock_);
        movement_ =
            std::make_unique<MovementTracker>(MovementTrackerConfig{}, clock_);
        commands_ = std::make_shared<RecordingSink>();
        // Half full is "above the watermark" here. The production default of
        // 0.9 would need the tier filled to within one block of capacity,
        // which makes the test about allocator padding rather than about the
        // threshold it is supposed to exercise.
        MovementConsumerConfig config;
        config.offload_high_watermark = 0.5;
        Build(config);
    }

    void TearDown() override {
        offload_.reset();
        onboard_.reset();
        tilers_ = MultiTiler{};
        std::filesystem::remove_all(storage_dir_);
    }

    /** Keeps the fixture's relaxed watermark when a test overrides onboard. */
    void BuildWithOnboardHeat(double heat) {
        MovementConsumerConfig config;
        config.offload_high_watermark = 0.5;
        config.onboard_min_read_heat = heat;
        Build(config);
    }

    void Build(const MovementConsumerConfig& config) {
        MovementConsumerDeps deps;
        deps.tilers = &tilers_;
        deps.registry = &registry_;
        deps.placement = placement_.get();
        deps.frequency = frequency_.get();
        deps.movement = movement_.get();
        deps.sink = commands_.get();
        deps.clock = clock_;

        auto offload = CreateOffloadConsumer(config, deps);
        CHECK(offload.has_value()) << toString(offload.error());
        offload_ = std::move(offload.value());
        auto onboard = CreateOnboardConsumer(config, deps);
        CHECK(onboard.has_value()) << toString(onboard.error());
        onboard_ = std::move(onboard.value());
    }

    std::unique_ptr<TilerManager> MakeDramTiler(int32_t priority,
                                                size_t capacity) {
        DramArenaConfig arena;
        arena.capacity_bytes = capacity;
        DramBlockPoolConfig pool_config;
        pool_config.arenas.push_back(arena);
        auto pool = CreateBlockPool(BlockPoolConfig(pool_config),
                                    std::shared_ptr<TransferEngine>{});
        if (!pool) return nullptr;
        LogicalTilerConfig logical;
        logical.tiler_id = generate_uuid();
        logical.memory_type = MemoryType::DRAM;
        logical.priority = priority;
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
        if (!pool) return nullptr;
        LogicalTilerConfig logical;
        logical.tiler_id = generate_uuid();
        logical.memory_type = MemoryType::NVME;
        logical.priority = priority;
        return std::make_unique<TilerManager>(logical, BlockIndexConfig{},
                                              std::move(pool.value()),
                                              registry_, EventPublisher());
    }

    ImmutableBlock Commit(TilerManager& tiler, const std::string& key,
                          uint8_t seed) {
        const std::vector<uint8_t> payload = Pattern(kBlockSize, seed);
        auto allocated = tiler.Allocate(payload.size());
        CHECK(allocated.has_value());
        CHECK(allocated->Write(0, AsBytes(payload)).has_value());
        auto completed = std::move(allocated.value()).Complete(key);
        CHECK(completed.has_value());
        auto registered = tiler.Register(key, std::move(completed.value()));
        CHECK(registered.has_value()) << toString(registered.error());
        // The commit is also a fact the tracker must see, exactly as the data
        // plane records it before publishing.
        auto handle = registry_.Match(key);
        CHECK(handle.has_value());
        frequency_->OnCommit(handle->Id(), key);
        return std::move(registered.value());
    }

    /** The commit fact for a key, as TilerManager would publish it. */
    BlockEvent CommitEvent(TilerManager& tiler, const std::string& key,
                           const ImmutableBlock& block) {
        auto handle = registry_.Match(key);
        CHECK(handle.has_value());
        BlockEvent event;
        event.type = EventType::kCommit;
        event.key = key;
        event.tiler_id = tiler.Id();
        event.block_id = block.Id();
        event.registration = handle->Downgrade();
        event.size_bytes = block.Size();
        return event;
    }

    BlockEvent AccessEvent(TilerManager& tiler, const std::string& key,
                           const ImmutableBlock& block) {
        BlockEvent event = CommitEvent(tiler, key, block);
        event.type = EventType::kAccess;
        return event;
    }

    /** A read, recorded the way the data plane records one. */
    void Read(const std::string& key) {
        auto handle = registry_.Match(key);
        CHECK(handle.has_value());
        frequency_->RecordAccess(handle->Id(), key);
    }

    /** Fill the fast tier so its usage crosses the offload watermark. */
    void FillFastTier(int blocks) {
        for (int i = 0; i < blocks; ++i) {
            Commit(*fast_, "filler/" + std::to_string(i),
                   static_cast<uint8_t>(i));
        }
    }

    std::filesystem::path storage_dir_;
    std::shared_ptr<ManualClock> clock_;
    BlockRegistry registry_{BlockRegistryConfig{}};
    std::shared_ptr<AcceptingSink> sink_;
    MultiTiler tilers_;
    TilerManager* fast_ = nullptr;
    TilerManager* slow_ = nullptr;
    std::shared_ptr<const TierGraph> graph_;
    std::unique_ptr<TierPlacementPolicy> placement_;
    std::shared_ptr<FrequencyTracker> frequency_;
    std::unique_ptr<MovementTracker> movement_;
    std::shared_ptr<RecordingSink> commands_;
    std::unique_ptr<EventConsumer> offload_;
    std::unique_ptr<EventConsumer> onboard_;
};

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

// Each consumer reacts to exactly one kind of fact. The monolith subscribed to
// everything, which is why one bug in it could affect every path.
TEST_F(MovementConsumersTest, EachConsumerSubscribesToOneTypeOnly) {
    const Subscription offload = offload_->SubscriptionInfo();
    EXPECT_EQ(offload.name, "offload");
    EXPECT_TRUE(offload.Wants(EventType::kCommit));
    EXPECT_FALSE(offload.Wants(EventType::kAccess));

    const Subscription onboard = onboard_->SubscriptionInfo();
    EXPECT_EQ(onboard.name, "onboard");
    EXPECT_TRUE(onboard.Wants(EventType::kAccess));
    EXPECT_FALSE(onboard.Wants(EventType::kCommit));
}

// ---------------------------------------------------------------------------
// Offload
// ---------------------------------------------------------------------------

TEST_F(MovementConsumersTest, ACommitBelowTheWatermarkMovesNothing) {
    ImmutableBlock block = Commit(*fast_, "cold", 0x11);
    ASSERT_TRUE(static_cast<bool>(block));
    EXPECT_EQ(offload_->Consume(CommitEvent(*fast_, "cold", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kIgnored);
    EXPECT_EQ(commands_->Count(), 0U);
}

TEST_F(MovementConsumersTest, ACommitAboveTheWatermarkOffloadsOneBlockDown) {
    FillFastTier(5);  // 5/8 of the fast tier, past the 0.5 watermark
    ImmutableBlock block = Commit(*fast_, "trigger", 0x22);
    ASSERT_TRUE(static_cast<bool>(block));

    EXPECT_EQ(offload_->Consume(CommitEvent(*fast_, "trigger", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kCommandEnqueued);
    const auto requests = commands_->Requests();
    ASSERT_EQ(requests.size(), 1U) << "one fact, one decision";
    EXPECT_EQ(requests[0].kind, MovementKind::kMigrate);
    EXPECT_EQ(requests[0].source_tiler, fast_->Id());
    EXPECT_EQ(requests[0].destination_tiler, slow_->Id());
    EXPECT_GT(requests[0].length, 0U);
    EXPECT_EQ(requests[0].priority, MovementPriority::kBackground);
    ASSERT_TRUE(requests[0].deadline.has_value());
}

// The block that arrived is not necessarily the one that leaves: what to shed
// is a property of the tier's contents.
TEST_F(MovementConsumersTest, TheVictimComesFromTheTiersOwnOrdering) {
    FillFastTier(5);
    ImmutableBlock block = Commit(*fast_, "trigger", 0x22);
    ASSERT_TRUE(static_cast<bool>(block));

    ASSERT_EQ(offload_->Consume(CommitEvent(*fast_, "trigger", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kCommandEnqueued);
    const auto requests = commands_->Requests();
    ASSERT_EQ(requests.size(), 1U);
    EXPECT_NE(requests[0].key, "trigger")
        << "the newest block was chosen, so the ordering was not consulted";
}

TEST_F(MovementConsumersTest, TheSlowestTierHasNowhereToOffloadTo) {
    ImmutableBlock block = Commit(*slow_, "bottom", 0x33);
    ASSERT_TRUE(static_cast<bool>(block));
    // Even above its watermark there is no slower neighbour.
    EXPECT_EQ(offload_->Consume(CommitEvent(*slow_, "bottom", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kIgnored);
    EXPECT_EQ(commands_->Count(), 0U);
}

// On a writer's thread the consumer applies the fact and produces nothing:
// queueing there would put scheduling into the write path.
TEST_F(MovementConsumersTest, InlineDeliveryProducesNoCommand) {
    FillFastTier(5);
    ImmutableBlock block = Commit(*fast_, "trigger", 0x22);
    ASSERT_TRUE(static_cast<bool>(block));
    EXPECT_EQ(offload_->Consume(CommitEvent(*fast_, "trigger", block),
                                DeliveryMode::kInline),
              ConsumeResult::kApplied);
    EXPECT_EQ(commands_->Count(), 0U);
}

// ---------------------------------------------------------------------------
// Onboard
// ---------------------------------------------------------------------------

// The case that could not be written before. The old policy read a sketch that
// commits also incremented, so writing a key eight times looked exactly like
// reading it eight times -- and a key nobody had ever read would be promoted
// to the fast tier.
TEST_F(MovementConsumersTest, WritesDoNotCountAsDemandForOnboarding) {
    BuildWithOnboardHeat(3.0);

    // Committed repeatedly, never read. Each commit raises heat but not
    // read_heat.
    ImmutableBlock block = Commit(*slow_, "written_often", 0x44);
    ASSERT_TRUE(static_cast<bool>(block));
    auto handle = registry_.Match("written_often");
    ASSERT_TRUE(handle.has_value());
    for (int i = 0; i < 10; ++i) {
        frequency_->OnCommit(handle->Id(), "written_often");
    }
    ASSERT_GT(frequency_->Get(handle->Id(), "written_often").heat, 3.0)
        << "the fixture did not actually make the key look busy";

    EXPECT_EQ(onboard_->Consume(AccessEvent(*slow_, "written_often", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kIgnored);
    EXPECT_EQ(commands_->Count(), 0U)
        << "a key nobody has read was promoted to the fast tier";
}

TEST_F(MovementConsumersTest, AFrequentlyReadSlowBlockIsReplicatedUp) {
    BuildWithOnboardHeat(3.0);

    ImmutableBlock block = Commit(*slow_, "hot", 0x55);
    ASSERT_TRUE(static_cast<bool>(block));
    for (int i = 0; i < 4; ++i) Read("hot");

    EXPECT_EQ(onboard_->Consume(AccessEvent(*slow_, "hot", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kCommandEnqueued);
    const auto requests = commands_->Requests();
    ASSERT_EQ(requests.size(), 1U);
    // Replicate, not migrate: the slow copy is the durable one, and with a
    // tier-local reclaim path dropping it would leave the object only where it
    // can be destroyed.
    EXPECT_EQ(requests[0].kind, MovementKind::kReplicate);
    EXPECT_EQ(requests[0].source_tiler, slow_->Id());
    EXPECT_EQ(requests[0].destination_tiler, fast_->Id());
}

TEST_F(MovementConsumersTest, AnAccessBelowTheThresholdMovesNothing) {
    BuildWithOnboardHeat(10.0);

    ImmutableBlock block = Commit(*slow_, "lukewarm", 0x66);
    ASSERT_TRUE(static_cast<bool>(block));
    Read("lukewarm");
    EXPECT_EQ(onboard_->Consume(AccessEvent(*slow_, "lukewarm", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kIgnored);
    EXPECT_EQ(commands_->Count(), 0U);
}

TEST_F(MovementConsumersTest, ZeroThresholdDisablesOnboarding) {
    BuildWithOnboardHeat(0.0);

    ImmutableBlock block = Commit(*slow_, "ignored", 0x77);
    ASSERT_TRUE(static_cast<bool>(block));
    for (int i = 0; i < 20; ++i) Read("ignored");
    EXPECT_EQ(onboard_->Consume(AccessEvent(*slow_, "ignored", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kIgnored);
    EXPECT_EQ(commands_->Count(), 0U);
}

// Only the destination's own index can say whether the block is already there,
// and a proposal to copy it again is pure waste.
TEST_F(MovementConsumersTest, ATargetThatAlreadyHoldsTheBlockIsSkipped) {
    BuildWithOnboardHeat(1.0);

    const std::vector<uint8_t> payload = Pattern(kBlockSize, 0x88);
    ImmutableBlock block = Commit(*slow_, "both", 0x88);
    ASSERT_TRUE(static_cast<bool>(block));
    auto handle = registry_.Match("both");
    ASSERT_TRUE(handle.has_value());

    // The same registration also present on the fast tier.
    auto allocated = fast_->Allocate(payload.size());
    ASSERT_TRUE(allocated.has_value());
    ASSERT_TRUE(allocated->Write(0, AsBytes(payload)).has_value());
    auto completed = std::move(allocated.value()).Complete("both");
    ASSERT_TRUE(completed.has_value());
    ASSERT_TRUE(fast_->RegisterWithHandle(std::move(completed.value()), *handle)
                    .has_value());

    for (int i = 0; i < 4; ++i) Read("both");
    EXPECT_EQ(onboard_->Consume(AccessEvent(*slow_, "both", block),
                                DeliveryMode::kQueued),
              ConsumeResult::kIgnored);
    EXPECT_EQ(commands_->Count(), 0U);
}

// ---------------------------------------------------------------------------
// Dedup, which the consumers do not implement themselves
// ---------------------------------------------------------------------------

// The second identical decision is refused because the first is still in
// flight -- by the tracker, not by anything the consumer remembers.
TEST_F(MovementConsumersTest, ASecondIdenticalProposalIsRefusedWhileInFlight) {
    BuildWithOnboardHeat(1.0);

    ImmutableBlock block = Commit(*slow_, "repeat", 0x99);
    ASSERT_TRUE(static_cast<bool>(block));
    for (int i = 0; i < 4; ++i) Read("repeat");

    const BlockEvent event = AccessEvent(*slow_, "repeat", block);
    EXPECT_EQ(onboard_->Consume(event, DeliveryMode::kQueued),
              ConsumeResult::kCommandEnqueued);
    EXPECT_EQ(onboard_->Consume(event, DeliveryMode::kQueued),
              ConsumeResult::kIgnored);
    EXPECT_EQ(commands_->Count(), 1U);
    EXPECT_EQ(movement_->Stats().rejected_inflight, 1U);
}

// And once it settles, a cooldown -- also the tracker's -- still holds it back
// for a while, so a steady read stream cannot become a replicate storm.
TEST_F(MovementConsumersTest, ASettledMoveIsHeldBackByTheCooldown) {
    BuildWithOnboardHeat(1.0);

    ImmutableBlock block = Commit(*slow_, "storm", 0xAA);
    ASSERT_TRUE(static_cast<bool>(block));
    for (int i = 0; i < 4; ++i) Read("storm");

    const BlockEvent event = AccessEvent(*slow_, "storm", block);
    ASSERT_EQ(onboard_->Consume(event, DeliveryMode::kQueued),
              ConsumeResult::kCommandEnqueued);
    commands_->SettleAll(/*moved=*/true);

    EXPECT_EQ(onboard_->Consume(event, DeliveryMode::kQueued),
              ConsumeResult::kIgnored);
    EXPECT_EQ(movement_->Stats().rejected_cooldown, 1U);
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

TEST_F(MovementConsumersTest, ConfigValidationRejectsUnusableValues) {
    EXPECT_TRUE(ValidateMovementConsumerConfig({}).has_value());

    MovementConsumerConfig full;
    full.offload_high_watermark = 1.0;
    EXPECT_FALSE(ValidateMovementConsumerConfig(full).has_value());

    MovementConsumerConfig negative;
    negative.onboard_min_read_heat = -1.0;
    EXPECT_FALSE(ValidateMovementConsumerConfig(negative).has_value());

    MovementConsumerConfig no_deadline;
    no_deadline.movement_deadline = 0ms;
    EXPECT_FALSE(ValidateMovementConsumerConfig(no_deadline).has_value());
}

TEST_F(MovementConsumersTest, MissingDependenciesAreRejected) {
    MovementConsumerDeps empty;
    EXPECT_FALSE(CreateOffloadConsumer({}, empty).has_value());
    EXPECT_FALSE(CreateOnboardConsumer({}, empty).has_value());
}

}  // namespace mooncake::v2
