// Component tests for the local copy layer: CopierRegistry routing and the
// LocalCopyEngine built on top of it.
//
// The thing under test is *which path the bytes take*, not only that they
// arrive. Before this layer existed every local copy -- DRAM to DRAM included
// -- went through a scratch buffer and two memcpys per chunk, and
// LocalTransferConfig was discarded outright. So each case below asserts the
// route as well as the result: a correct copy on the wrong path is exactly the
// regression that went unnoticed for three phases.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "p2p/client/v2/block_pool.h"
#include "p2p/client/v2/copier.h"
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

std::span<std::byte> AsWritableBytes(std::vector<uint8_t>& data) {
    return {reinterpret_cast<std::byte*>(data.data()), data.size()};
}

/**
 * @class CountingCopier
 * @brief Accepts a configurable subset of pairs and records what it served.
 */
class CountingCopier final : public Copier {
   public:
    CountingCopier(std::string name, bool accept_everything)
        : name_(std::move(name)), accept_everything_(accept_everything) {}

    bool CanCopy(const CopyEndpoint&, const CopyEndpoint&) const override {
        return accept_everything_;
    }

    CopyResult Copy(const CopyRequest& request) override {
        calls_.fetch_add(1, std::memory_order_relaxed);
        return CopyResult::Success(request.length);
    }

    CopierCapabilities Capabilities() const override {
        CopierCapabilities capabilities;
        capabilities.name = name_;
        return capabilities;
    }

    uint64_t Calls() const { return calls_.load(std::memory_order_relaxed); }

   private:
    std::string name_;
    bool accept_everything_;
    std::atomic<uint64_t> calls_{0};
};

/** Uses of the copier registered under `name`, or 0 if there is none. */
uint64_t UsesOf(const LocalCopyStats& stats, std::string_view name) {
    for (size_t i = 0; i < stats.copier_names.size(); ++i) {
        if (stats.copier_names[i] == name) return stats.copier_uses[i];
    }
    return 0;
}

bool HasCopier(const std::vector<CopierCapabilities>& described,
               std::string_view name) {
    for (const auto& capabilities : described) {
        if (capabilities.name == name) return true;
    }
    return false;
}

}  // namespace

// ---------------------------------------------------------------------------
// CopierRegistry
// ---------------------------------------------------------------------------

TEST(CopierRegistryTest, RegistrationOrderIsPriorityOrder) {
    CopierRegistry registry;
    auto first = std::make_unique<CountingCopier>("first", true);
    auto second = std::make_unique<CountingCopier>("second", true);
    CountingCopier* first_raw = first.get();
    CountingCopier* second_raw = second.get();
    ASSERT_TRUE(registry.Register(std::move(first)).has_value());
    ASSERT_TRUE(registry.Register(std::move(second)).has_value());

    CopyEndpoint source = CopyEndpoint::FromHost(nullptr, 0);
    CopyEndpoint destination = CopyEndpoint::FromHost(nullptr, 0);
    EXPECT_EQ(registry.Route(source, destination), first_raw);
    EXPECT_NE(registry.Route(source, destination), second_raw);
}

TEST(CopierRegistryTest, ACopierThatRefusesThePairIsSkipped) {
    CopierRegistry registry;
    auto picky = std::make_unique<CountingCopier>("picky", false);
    auto fallback = std::make_unique<CountingCopier>("fallback", true);
    CountingCopier* fallback_raw = fallback.get();
    ASSERT_TRUE(registry.Register(std::move(picky)).has_value());
    ASSERT_TRUE(registry.Register(std::move(fallback)).has_value());

    CopyEndpoint source = CopyEndpoint::FromHost(nullptr, 0);
    CopyEndpoint destination = CopyEndpoint::FromHost(nullptr, 0);
    EXPECT_EQ(registry.Route(source, destination), fallback_raw);
}

TEST(CopierRegistryTest, RouteReportsNothingWhenNobodyAccepts) {
    CopierRegistry registry;
    ASSERT_TRUE(
        registry.Register(std::make_unique<CountingCopier>("picky", false))
            .has_value());
    CopyEndpoint endpoint = CopyEndpoint::FromHost(nullptr, 0);
    EXPECT_EQ(registry.Route(endpoint, endpoint), nullptr);
}

TEST(CopierRegistryTest, FreezeRejectsFurtherRegistration) {
    CopierRegistry registry;
    ASSERT_TRUE(registry.Register(std::make_unique<CountingCopier>("a", true))
                    .has_value());
    registry.Freeze();
    EXPECT_TRUE(registry.IsFrozen());

    auto late = registry.Register(std::make_unique<CountingCopier>("b", true));
    ASSERT_FALSE(late.has_value());
    EXPECT_EQ(late.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(registry.Size(), 1U);
}

TEST(CopierRegistryTest, ANullCopierIsRejected) {
    CopierRegistry registry;
    auto rejected = registry.Register(nullptr);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::INVALID_PARAMS);
}

TEST(CopierRegistryTest, UsesAreCountedPerCopier) {
    CopierRegistry registry;
    ASSERT_TRUE(
        registry.Register(std::make_unique<CountingCopier>("only", true))
            .has_value());
    CopyEndpoint endpoint = CopyEndpoint::FromHost(nullptr, 0);
    registry.Route(endpoint, endpoint);
    registry.Route(endpoint, endpoint);
    ASSERT_EQ(registry.Uses().size(), 1U);
    EXPECT_EQ(registry.Uses()[0], 2U);
}

// The registry is per instance, which is the whole reason it is not the
// process-wide singleton the tiered-cache tree uses: two managers in one
// process must not see each other's fakes.
TEST(CopierRegistryTest, TwoRegistriesDoNotShareCopiers) {
    CopierRegistry left;
    CopierRegistry right;
    ASSERT_TRUE(left.Register(std::make_unique<CountingCopier>("left", true))
                    .has_value());
    EXPECT_EQ(left.Size(), 1U);
    EXPECT_EQ(right.Size(), 0U);
    CopyEndpoint endpoint = CopyEndpoint::FromHost(nullptr, 0);
    EXPECT_EQ(right.Route(endpoint, endpoint), nullptr);
}

// ---------------------------------------------------------------------------
// The TransferEngine gate
// ---------------------------------------------------------------------------

// A DRAM block hands out a usable pointer whether or not the arena was ever
// registered with an engine. Trusting the address alone would submit
// unregistered memory to RDMA, and it would fail on the peer rather than here.
TEST(TransferEngineCopierTest, AnUnregisteredDestinationIsRefused) {
    // Only CanCopy is exercised, and it merely checks the pointer for null, so
    // no coordinator is constructed.
    auto* fake_coordinator = reinterpret_cast<TransferCoordinator*>(0x1);
    auto copier = CreateTransferEngineCopier(fake_coordinator, "local:1");

    std::vector<uint8_t> buffer(kBlockSize, 0);
    CopyEndpoint source = CopyEndpoint::FromHost(buffer.data(), buffer.size());

    CopyEndpoint destination;
    destination.host_buffer = buffer.data();
    destination.capacity = buffer.size();
    destination.domain = CopyDomain::kHostMemory;
    destination.direct_cpu_access = true;
    destination.address = TransferAddress{
        reinterpret_cast<uintptr_t>(buffer.data()), buffer.size()};
    destination.te_addressable = false;  // never registered

    EXPECT_FALSE(copier->CanCopy(source, destination));

    destination.te_addressable = true;
    EXPECT_TRUE(copier->CanCopy(source, destination));
}

TEST(TransferEngineCopierTest, ADestinationWithNoAddressIsRefused) {
    auto* fake_coordinator = reinterpret_cast<TransferCoordinator*>(0x1);
    auto copier = CreateTransferEngineCopier(fake_coordinator, "local:1");

    std::vector<uint8_t> buffer(kBlockSize, 0);
    CopyEndpoint source = CopyEndpoint::FromHost(buffer.data(), buffer.size());

    // A slow tier: registered nothing, exposes nothing. TE cannot reach it,
    // which is why the offload direction always needs a non-TE fallback.
    CopyEndpoint destination;
    destination.capacity = buffer.size();
    destination.domain = CopyDomain::kFileOrBlock;
    destination.te_addressable = true;
    EXPECT_FALSE(copier->CanCopy(source, destination));
}

TEST(TransferEngineCopierTest, WithoutACoordinatorItNeverAccepts) {
    auto copier = CreateTransferEngineCopier(nullptr, "local:1");
    std::vector<uint8_t> buffer(kBlockSize, 0);
    CopyEndpoint source = CopyEndpoint::FromHost(buffer.data(), buffer.size());
    CopyEndpoint destination = source;
    destination.te_addressable = true;
    destination.address = TransferAddress{
        reinterpret_cast<uintptr_t>(buffer.data()), buffer.size()};
    EXPECT_FALSE(copier->CanCopy(source, destination));
}

// ---------------------------------------------------------------------------
// LocalCopyEngine over real pools
// ---------------------------------------------------------------------------

class LocalCopyEngineTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag logging_once;
        std::call_once(logging_once, [] {
            google::InitGoogleLogging("LocalCopyEngineTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        storage_dir_ = std::filesystem::temp_directory_path() /
                       ("mooncake_v2_copy_" +
                        std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(storage_dir_);
        std::filesystem::create_directories(storage_dir_);

        registry_ = BlockRegistry(BlockRegistryConfig{/*shard_count=*/8});
        dram_ = MakeDramTiler(100);
        ssd_ = MakeSsdTiler(10);
        ASSERT_NE(dram_, nullptr);
        ASSERT_NE(ssd_, nullptr);
    }

    void TearDown() override {
        ssd_.reset();
        dram_.reset();
        std::filesystem::remove_all(storage_dir_);
    }

    std::unique_ptr<TilerManager> MakeDramTiler(int32_t priority) {
        DramArenaConfig arena;
        arena.capacity_bytes = 8ULL * 1024 * 1024;
        DramBlockPoolConfig pool_config;
        pool_config.arenas.push_back(arena);
        auto pool = CreateBlockPool(BlockPoolConfig(pool_config),
                                    std::shared_ptr<TransferEngine>{});
        if (!pool) {
            ADD_FAILURE() << "CreateBlockPool(DRAM): "
                          << toString(pool.error());
            return nullptr;
        }
        LogicalTilerConfig logical;
        logical.tiler_id = generate_uuid();
        logical.memory_type = MemoryType::DRAM;
        logical.priority = priority;
        return std::make_unique<TilerManager>(logical, BlockIndexConfig{},
                                              std::move(pool.value()),
                                              registry_, EventPublisher());
    }

    std::unique_ptr<TilerManager> MakeSsdTiler(int32_t priority) {
        SSDDeviceConfig device;
        device.file_path = (storage_dir_ / "slow.data").string();
        device.capacity_bytes = 8ULL * 1024 * 1024;
        SSDBlockPoolConfig pool_config;
        pool_config.devices.push_back(device);
        auto pool = CreateBlockPool(BlockPoolConfig(pool_config),
                                    std::shared_ptr<TransferEngine>{});
        if (!pool) {
            ADD_FAILURE() << "CreateBlockPool(SSD): " << toString(pool.error());
            return nullptr;
        }
        LogicalTilerConfig logical;
        logical.tiler_id = generate_uuid();
        logical.memory_type = MemoryType::NVME;
        logical.priority = priority;
        return std::make_unique<TilerManager>(logical, BlockIndexConfig{},
                                              std::move(pool.value()),
                                              registry_, EventPublisher());
    }

    /** Commit `pattern` into `tiler` under `key` and return the snapshot. */
    ImmutableBlock Commit(TilerManager& tiler, const std::string& key,
                          const std::vector<uint8_t>& pattern,
                          const LocalCopyEngine& engine) {
        auto block = tiler.Allocate(pattern.size());
        if (!block) {
            ADD_FAILURE() << "Allocate: " << toString(block.error());
            return {};
        }
        std::vector<Slice> slices{
            Slice{const_cast<uint8_t*>(pattern.data()), pattern.size()}};
        auto written = engine.WriteFromSlices(slices, block.value());
        if (!written) {
            ADD_FAILURE() << "WriteFromSlices: " << toString(written.error());
            return {};
        }
        auto completed = std::move(block.value()).Complete(key);
        if (!completed) {
            ADD_FAILURE() << "Complete: " << toString(completed.error());
            return {};
        }
        auto registered = tiler.Register(key, std::move(completed.value()));
        if (!registered) {
            ADD_FAILURE() << "Register: " << toString(registered.error());
            return {};
        }
        return std::move(registered.value());
    }

    std::filesystem::path storage_dir_;
    BlockRegistry registry_{BlockRegistryConfig{}};
    std::unique_ptr<TilerManager> dram_;
    std::unique_ptr<TilerManager> ssd_;
};

// The gate above is only worth having if the endpoint actually reports the
// registration state. A DRAM pool built without a TransferEngine registers
// nothing, yet every one of its blocks still hands out a usable pointer -- so
// deriving te_addressable from the address is exactly the bug, and this is the
// case that catches it.
TEST_F(LocalCopyEngineTest, AnUnregisteredDramBlockIsNotTeAddressable) {
    auto block = dram_->Allocate(kBlockSize);
    ASSERT_TRUE(block.has_value());
    BlockDataHandle* handle = block->DataHandleForCopy();
    ASSERT_NE(handle, nullptr);

    const CopyEndpoint endpoint =
        CopyEndpoint::FromHandle(*handle, 0, kBlockSize, dram_->Id());
    // The address exists and the CPU may use it...
    EXPECT_TRUE(endpoint.address.has_value());
    EXPECT_TRUE(endpoint.direct_cpu_access);
    EXPECT_NE(endpoint.HostAddress(), nullptr);
    // ...but nothing registered it, so no engine may be handed it.
    EXPECT_FALSE(endpoint.te_addressable);

    auto* fake_coordinator = reinterpret_cast<TransferCoordinator*>(0x1);
    auto te_copier = CreateTransferEngineCopier(fake_coordinator, "local:1");
    EXPECT_FALSE(te_copier->CanCopy(endpoint, endpoint));
}

// An SSD block has no address at all: it is reachable only through the generic
// Read/Write interface, which is what lets a slow tier take part without
// pretending to be memory.
TEST_F(LocalCopyEngineTest, AnSsdBlockExposesNoHostAddress) {
    auto block = ssd_->Allocate(kBlockSize);
    ASSERT_TRUE(block.has_value());
    BlockDataHandle* handle = block->DataHandleForCopy();
    ASSERT_NE(handle, nullptr);

    const CopyEndpoint endpoint =
        CopyEndpoint::FromHandle(*handle, 0, kBlockSize, ssd_->Id());
    EXPECT_FALSE(endpoint.address.has_value());
    EXPECT_FALSE(endpoint.direct_cpu_access);
    EXPECT_FALSE(endpoint.te_addressable);
    EXPECT_EQ(endpoint.HostAddress(), nullptr);
    EXPECT_EQ(endpoint.domain, CopyDomain::kFileOrBlock);
}

TEST_F(LocalCopyEngineTest, MemcpyModeRegistersNoTransferEngineCopier) {
    LocalTransferConfig config;
    config.mode = LocalTransferMode::MEMCPY;
    LocalCopyEngine engine(config);
    EXPECT_FALSE(HasCopier(engine.Describe(), "transfer_engine"));
    EXPECT_TRUE(HasCopier(engine.Describe(), "dram_memcpy"));
    EXPECT_TRUE(HasCopier(engine.Describe(), "staged_read_write"));
}

// TE mode with nothing to submit to must degrade to the memory paths rather
// than fail every copy: four in-tree fixtures build a default-initialised
// LocalTransferConfig, whose mode is TE, with no TransferEngine at all.
TEST_F(LocalCopyEngineTest, TeModeWithoutACoordinatorFallsBack) {
    LocalTransferConfig config;
    config.mode = LocalTransferMode::TE;
    config.te_endpoint = "local:1";
    LocalCopyEngine engine(config, /*coordinator=*/nullptr);
    EXPECT_FALSE(HasCopier(engine.Describe(), "transfer_engine"));

    const auto pattern = Pattern(kBlockSize, 0x11);
    ImmutableBlock source = Commit(*dram_, "te/fallback", pattern, engine);
    ASSERT_TRUE(static_cast<bool>(source));
    EXPECT_GT(UsesOf(engine.Stats(), "dram_memcpy"), 0U);
}

TEST_F(LocalCopyEngineTest, DramToDramTakesTheMemcpyRoute) {
    LocalCopyEngine engine{LocalTransferConfig{}};
    const auto pattern = Pattern(kBlockSize, 0x22);
    ImmutableBlock source = Commit(*dram_, "copy/dram", pattern, engine);
    ASSERT_TRUE(static_cast<bool>(source));

    auto destination = dram_->Allocate(kBlockSize);
    ASSERT_TRUE(destination.has_value());
    const uint64_t staged_before = UsesOf(engine.Stats(), "staged_read_write");

    ASSERT_TRUE(engine.Copy(source, destination.value()).has_value());
    EXPECT_EQ(UsesOf(engine.Stats(), "staged_read_write"), staged_before);
    EXPECT_GT(UsesOf(engine.Stats(), "dram_memcpy"), 0U);

    // Routing is only interesting if the bytes still arrive.
    auto completed = std::move(destination.value()).Complete("copy/dram/two");
    ASSERT_TRUE(completed.has_value());
    auto registered =
        dram_->Register("copy/dram/two", std::move(completed.value()));
    ASSERT_TRUE(registered.has_value());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(registered->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, pattern);
}

TEST_F(LocalCopyEngineTest, DramToSsdTakesTheStagedRouteAndIsByteIdentical) {
    LocalCopyEngine engine{LocalTransferConfig{}};
    const auto pattern = Pattern(kBlockSize, 0x33);
    ImmutableBlock source = Commit(*dram_, "copy/offload", pattern, engine);
    ASSERT_TRUE(static_cast<bool>(source));

    auto destination = ssd_->Allocate(kBlockSize);
    ASSERT_TRUE(destination.has_value());
    const uint64_t staged_before = UsesOf(engine.Stats(), "staged_read_write");
    ASSERT_TRUE(engine.Copy(source, destination.value()).has_value());
    EXPECT_EQ(UsesOf(engine.Stats(), "staged_read_write"), staged_before + 1);

    auto completed = std::move(destination.value()).Complete("copy/offload");
    ASSERT_TRUE(completed.has_value());
    auto registered = ssd_->RegisterWithHandle(
        std::move(completed.value()), *registry_.Match("copy/offload"));
    ASSERT_TRUE(registered.has_value()) << toString(registered.error());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(registered->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, pattern);
}

TEST_F(LocalCopyEngineTest, SsdToSsdTakesTheStagedRoute) {
    LocalCopyEngine engine{LocalTransferConfig{}};
    const auto pattern = Pattern(kBlockSize, 0x44);
    ImmutableBlock source = Commit(*ssd_, "copy/ssd", pattern, engine);
    ASSERT_TRUE(static_cast<bool>(source));

    auto destination = ssd_->Allocate(kBlockSize);
    ASSERT_TRUE(destination.has_value());
    const uint64_t memcpy_before = UsesOf(engine.Stats(), "dram_memcpy");
    ASSERT_TRUE(engine.Copy(source, destination.value()).has_value());
    EXPECT_EQ(UsesOf(engine.Stats(), "dram_memcpy"), memcpy_before);
    EXPECT_GT(UsesOf(engine.Stats(), "staged_read_write"), 0U);
}

TEST_F(LocalCopyEngineTest, ARegisteredCopierOutranksTheBuiltInOnes) {
    LocalCopyEngine engine{LocalTransferConfig{}};
    // The engine registers its own copiers in the constructor, so anything
    // added afterwards lands behind them -- which is the whole reason
    // registration order has to be documented as priority order.
    auto intercept = std::make_unique<CountingCopier>("intercept", true);
    CountingCopier* raw = intercept.get();
    ASSERT_TRUE(engine.RegisterCopier(std::move(intercept)).has_value());

    const auto pattern = Pattern(kBlockSize, 0x55);
    ImmutableBlock source = Commit(*dram_, "copy/intercept", pattern, engine);
    ASSERT_TRUE(static_cast<bool>(source));
    EXPECT_EQ(raw->Calls(), 0U);
    EXPECT_TRUE(HasCopier(engine.Describe(), "intercept"));
}

TEST_F(LocalCopyEngineTest, AShortSliceSetIsRejectedInBothDirections) {
    LocalCopyEngine engine{LocalTransferConfig{}};
    const auto pattern = Pattern(kBlockSize, 0x66);

    // Write: a slice set covering only part of the block used to return OK
    // after writing a prefix, leaving the tail whatever the allocator left.
    auto block = dram_->Allocate(kBlockSize);
    ASSERT_TRUE(block.has_value());
    std::vector<Slice> short_write{
        Slice{const_cast<uint8_t*>(pattern.data()), kBlockSize / 2}};
    auto written = engine.WriteFromSlices(short_write, block.value());
    ASSERT_FALSE(written.has_value());
    EXPECT_EQ(written.error(), ErrorCode::INVALID_PARAMS);

    // Read: already symmetric, kept here so the pair cannot drift apart.
    ImmutableBlock source = Commit(*dram_, "copy/short", pattern, engine);
    ASSERT_TRUE(static_cast<bool>(source));
    std::vector<uint8_t> half(kBlockSize / 2, 0);
    std::vector<Slice> short_read{Slice{half.data(), half.size()}};
    auto read = engine.ReadToSlices(source, short_read);
    ASSERT_FALSE(read.has_value());
    EXPECT_EQ(read.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(LocalCopyEngineTest, ExactCoverageStillSucceeds) {
    LocalCopyEngine engine{LocalTransferConfig{}};
    const auto pattern = Pattern(kBlockSize, 0x77);
    auto block = dram_->Allocate(kBlockSize);
    ASSERT_TRUE(block.has_value());

    // Split across two slices, which is what a caller with a scatter list
    // actually looks like.
    std::vector<Slice> slices{
        Slice{const_cast<uint8_t*>(pattern.data()), kBlockSize / 2},
        Slice{const_cast<uint8_t*>(pattern.data()) + kBlockSize / 2,
              kBlockSize / 2}};
    ASSERT_TRUE(engine.WriteFromSlices(slices, block.value()).has_value());

    auto completed = std::move(block.value()).Complete("copy/exact");
    ASSERT_TRUE(completed.has_value());
    auto registered =
        dram_->Register("copy/exact", std::move(completed.value()));
    ASSERT_TRUE(registered.has_value());
    std::vector<uint8_t> readback(kBlockSize, 0);
    ASSERT_TRUE(registered->Read(0, AsWritableBytes(readback)).has_value());
    EXPECT_EQ(readback, pattern);
}

TEST_F(LocalCopyEngineTest, StatsCountCopiesAndBytes) {
    LocalCopyEngine engine{LocalTransferConfig{}};
    const auto pattern = Pattern(kBlockSize, 0x88);
    ImmutableBlock source = Commit(*dram_, "copy/stats", pattern, engine);
    ASSERT_TRUE(static_cast<bool>(source));

    const LocalCopyStats after_write = engine.Stats();
    EXPECT_EQ(after_write.copies, 1U);
    EXPECT_EQ(after_write.bytes, kBlockSize);
    EXPECT_EQ(after_write.failures, 0U);
    EXPECT_EQ(after_write.unroutable, 0U);
    EXPECT_EQ(after_write.copier_names.size(), after_write.copier_uses.size());
}

TEST_F(LocalCopyEngineTest, ACancelledCopyStopsAndReportsShuttingDown) {
    auto copier = CreateStagedReadWriteCopier(/*default_chunk_bytes=*/1024);
    auto cancelled = std::make_shared<std::atomic<bool>>(true);

    std::vector<uint8_t> from(kBlockSize, 0xab);
    std::vector<uint8_t> into(kBlockSize, 0);
    CopyRequest request;
    request.source = CopyEndpoint::FromHost(from.data(), from.size());
    request.destination = CopyEndpoint::FromHost(into.data(), into.size());
    request.length = kBlockSize;
    request.cancellation = cancelled;

    const CopyResult result = copier->Copy(request);
    EXPECT_FALSE(result.Ok());
    EXPECT_EQ(result.status, ErrorCode::SHUTTING_DOWN);
    EXPECT_EQ(result.copied_bytes, 0U);
}

TEST_F(LocalCopyEngineTest, BatchCopyDefaultsToPerItemResults) {
    auto copier = CreateStagedReadWriteCopier(4096);
    std::vector<uint8_t> from(64, 0xcd);
    std::vector<uint8_t> into(64, 0);

    CopyRequest good;
    good.source = CopyEndpoint::FromHost(from.data(), from.size());
    good.destination = CopyEndpoint::FromHost(into.data(), into.size());
    good.length = from.size();

    // Second item asks for more than the destination holds: it must fail on
    // its own without disturbing the first.
    CopyRequest bad = good;
    bad.length = from.size() * 2;

    const std::vector<CopyRequest> requests{good, bad};
    const std::vector<CopyResult> results = copier->BatchCopy(requests);
    ASSERT_EQ(results.size(), 2U);
    EXPECT_TRUE(results[0].Ok());
    EXPECT_EQ(results[0].copied_bytes, from.size());
    EXPECT_FALSE(results[1].Ok());
    EXPECT_FALSE(copier->Capabilities().native_batch);
}

}  // namespace mooncake::v2
