// Copyright 2024 KVCache.AI

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "../src/config/egm_store_pool_config.h"
#include "real_client.h"
#include "embedded_master.h"

namespace mooncake {
namespace {

class EgmEnvGuard {
   public:
    EgmEnvGuard(const char* name, const char* value) : name_(name) {
        if (const char* old = std::getenv(name)) old_ = old;
        if (value)
            setenv(name, value, 1);
        else
            unsetenv(name);
    }
    ~EgmEnvGuard() {
        if (old_)
            setenv(name_.c_str(), old_->c_str(), 1);
        else
            unsetenv(name_.c_str());
    }

   private:
    std::string name_;
    std::optional<std::string> old_;
};

}  // namespace

class EgmStoreLifecycleTestPeer {
   public:
    struct Faults {
        std::vector<std::string> calls;
        int registration = 0;
        bool exported = true;
        std::optional<Segment> mounted_segment;
        ErrorCode mount = ErrorCode::OK;
        ErrorCode unmount = ErrorCode::OK;
        int unregister = 0;
        int drain = 0;
        int free_result = 0;
        int freed = 0;
    };
    static std::shared_ptr<Client> MakeClient(
        const std::shared_ptr<Faults>& faults) {
        EgmEnvGuard domain("MOONCAKE_FABRIC_DOMAIN", "test-fabric");
        EgmEnvGuard rack("MC_RACK", "rack1");
        EgmEnvGuard tray("MC_TRAY", "host1");
        EgmEnvGuard gpu("MC_PHYSICAL_GPU", "3");
        auto client = std::shared_ptr<Client>(
            new Client("egm-test", "test-metadata", "mnnvl", {}, "default"));
        client->transfer_engine_ = std::make_shared<TransferEngine>();
        auto ops = std::make_unique<Client::MnnvlLifecycleOps>();
        ops->register_memory = [faults](void*, size_t,
                                        const std::string& location) {
            faults->calls.push_back("register:" + location);
            return faults->registration;
        };
        ops->exported = [faults](void*, size_t) { return faults->exported; };
        ops->mount =
            [faults](const Segment& segment) -> tl::expected<void, ErrorCode> {
            faults->calls.push_back("mount:" + segment.protocol);
            faults->mounted_segment = segment;
            if (faults->mount != ErrorCode::OK)
                return tl::unexpected(faults->mount);
            return {};
        };
        ops->unmount = [faults](const UUID&) -> tl::expected<void, ErrorCode> {
            faults->calls.push_back("unmount");
            if (faults->unmount != ErrorCode::OK)
                return tl::unexpected(faults->unmount);
            return {};
        };
        ops->unregister_memory = [faults](void*) {
            faults->calls.push_back("unregister");
            return faults->unregister;
        };
        ops->drain = [faults] {
            faults->calls.push_back("drain");
            return faults->drain;
        };
        client->mnnvl_ops_for_test_ = std::move(ops);
        return client;
    }
    static std::shared_ptr<Client> MakeHotCacheClient(
        const std::string& protocol) {
        // CTest sets this before first use; the guard also covers direct runs
        // of this suite without modifying the client's immutable setting.
        EgmEnvGuard checksum("MOONCAKE_STORE_CHECKSUM", "0");
        return std::shared_ptr<Client>(new Client(
            "hot-cache-test", "P2PHANDSHAKE", protocol, {}, "default"));
    }
    static ErrorCode InitHotCache(Client& client) {
        return client.InitLocalHotCache();
    }
    static bool HasHotCacheResources(const Client& client) {
        return client.hot_cache_ || client.hot_cache_handler_ ||
               client.admission_sketch_ || client.hot_cache_memory_registered_;
    }
    static ErrorCode PreparePersistence(Client& client, const std::string& root,
                                        const std::string& subdir) {
        return client.PrepareStorageBackend(root, subdir, true, 0);
    }
    static bool HasStorageBackend(const Client& client) {
        return bool(client.storage_backend_);
    }
    static void StageGraceful(Client& client, const UUID& id,
                              std::function<void(const UUID&)> callback) {
        std::lock_guard<std::mutex> lock(client.mounted_segments_mutex_);
        client.gracefully_unmounting_segments_[id] =
            client.mounted_segments_.at(id);
        client.mounted_segments_.erase(id);
        client.graceful_unmount_cleanup_callbacks_[id] = std::move(callback);
    }
    static size_t MountedCount(const Client& client) {
        std::lock_guard<std::mutex> lock(client.mounted_segments_mutex_);
        return client.mounted_segments_.size();
    }
    static size_t MountStateCount(const Client& client) {
        std::lock_guard<std::mutex> lock(client.mounted_segments_mutex_);
        return client.mnnvl_mount_states_.size();
    }
};

namespace {

TEST(EgmStoreHotCacheTest, MnnvlSkipsRequestedCacheWithoutTransferEngine) {
    EgmEnvGuard size("MC_STORE_LOCAL_HOT_CACHE_SIZE", "8192");
    EgmEnvGuard block("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "4096");
    for (const char* shared : {"0", "1"}) {
        SCOPED_TRACE(shared);
        EgmEnvGuard shm("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", shared);
        auto client = EgmStoreLifecycleTestPeer::MakeHotCacheClient("mnnvl");
        for (int attempt = 0; attempt < 2; ++attempt) {
            SCOPED_TRACE(attempt);
            EXPECT_EQ(EgmStoreLifecycleTestPeer::InitHotCache(*client),
                      ErrorCode::OK);
            EXPECT_FALSE(client->IsHotCacheEnabled());
            EXPECT_EQ(client->GetLocalHotCacheBlockCount(), 0);
            EXPECT_FALSE(
                EgmStoreLifecycleTestPeer::HasHotCacheResources(*client));
        }
    }
}

TEST(EgmStoreHotCacheTest, UnrequestedCacheRemainsDisabled) {
    EgmEnvGuard size("MC_STORE_LOCAL_HOT_CACHE_SIZE", nullptr);
    for (const char* protocol : {"mnnvl", "tcp"}) {
        SCOPED_TRACE(protocol);
        auto client = EgmStoreLifecycleTestPeer::MakeHotCacheClient(protocol);
        EXPECT_EQ(EgmStoreLifecycleTestPeer::InitHotCache(*client),
                  ErrorCode::OK);
        EXPECT_FALSE(EgmStoreLifecycleTestPeer::HasHotCacheResources(*client));
    }
}

TEST(EgmStoreHotCacheTest, TcpStillValidatesRequestedCacheCapacity) {
    EgmEnvGuard size("MC_STORE_LOCAL_HOT_CACHE_SIZE", "4096");
    EgmEnvGuard block("MC_STORE_LOCAL_HOT_BLOCK_SIZE", "8192");
    EgmEnvGuard shm("MC_STORE_LOCAL_HOT_CACHE_USE_SHM", "0");
    auto client = EgmStoreLifecycleTestPeer::MakeHotCacheClient("tcp");
    // An undersized TCP cache still fails before registration; only MNNVL
    // ignores a requested cache, so no transfer engine is needed here.
    EXPECT_EQ(EgmStoreLifecycleTestPeer::InitHotCache(*client),
              ErrorCode::INVALID_PARAMS);
    EXPECT_FALSE(EgmStoreLifecycleTestPeer::HasHotCacheResources(*client));
}
TEST(EgmStorePoolConfigTest, DefaultOffIgnoresDependentSettings) {
    const ConfigDict config{{CONFIG_KEY_EGM_NUMA_NODES, "invalid"}};
    const auto parsed = ParseEgmStorePoolConfig(config, "tcp", 0);
    ASSERT_TRUE(parsed);
    EXPECT_FALSE(parsed->enabled);
    EXPECT_TRUE(parsed->auto_numa_nodes);
    EXPECT_TRUE(parsed->numa_nodes.empty());
}

TEST(EgmStorePoolConfigTest, AcceptsNarrowCommonBooleanSyntax) {
    for (const auto* value : {"true", " \tTrUe\r\n", "1"}) {
        SCOPED_TRACE(value);
        const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value}};
        const auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1);
        ASSERT_TRUE(parsed);
        EXPECT_TRUE(parsed->enabled);
    }
    for (const auto* value : {"false", " \tFaLsE\r\n", "0"}) {
        SCOPED_TRACE(value);
        const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value},
                                {CONFIG_KEY_EGM_NUMA_NODES, "invalid"}};
        const auto parsed = ParseEgmStorePoolConfig(config, "tcp", 0);
        ASSERT_TRUE(parsed);
        EXPECT_FALSE(parsed->enabled);
    }
}

TEST(EgmStorePoolConfigTest, RejectsOtherBooleanValues) {
    for (const auto* value : {"", " ", "2", "yes", "no", "on", "off", "enable",
                              "disable", "truex"}) {
        SCOPED_TRACE(value);
        const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value}};
        const auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST(EgmStorePoolConfigTest, AutoNodesAreDefaultAndCaseSensitive) {
    ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"}};
    auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1);
    ASSERT_TRUE(parsed);
    EXPECT_TRUE(parsed->auto_numa_nodes);
    EXPECT_TRUE(parsed->numa_nodes.empty());

    config[CONFIG_KEY_EGM_NUMA_NODES] = " \tauto\r\n";
    parsed = ParseEgmStorePoolConfig(config, "nvlink", 1);
    ASSERT_TRUE(parsed);
    EXPECT_TRUE(parsed->auto_numa_nodes);
    EXPECT_TRUE(parsed->numa_nodes.empty());

    config[CONFIG_KEY_EGM_NUMA_NODES] = "AUTO";
    EXPECT_FALSE(ParseEgmStorePoolConfig(config, "nvlink", 1));
}

TEST(EgmStorePoolConfigTest, ExplicitNodesAreTrimmedSortedAndDeduplicated) {
    const ConfigDict config{
        {CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"},
        {CONFIG_KEY_EGM_NUMA_NODES, " \t3, 1,3,-0,2147483647\r\n"}};
    const auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1);
    ASSERT_TRUE(parsed);
    EXPECT_FALSE(parsed->auto_numa_nodes);
    EXPECT_EQ(parsed->numa_nodes,
              (std::vector<int>{0, 1, 3, std::numeric_limits<int>::max()}));
}

TEST(EgmStorePoolConfigTest, RejectsInvalidNodeLists) {
    for (const auto* value : {"", " ", "1,,3", "1,", ",1", "-1", "+1", "1x",
                              "1 2", "2147483648", "4294967296"}) {
        SCOPED_TRACE(value);
        const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"},
                                {CONFIG_KEY_EGM_NUMA_NODES, value}};
        const auto parsed = ParseEgmStorePoolConfig(config, "nvlink", 1);
        ASSERT_FALSE(parsed);
        EXPECT_EQ(parsed.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST(EgmStorePoolConfigTest, EnabledRequiresNvlinkAndCapacity) {
    const ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"}};
    EXPECT_TRUE(ParseEgmStorePoolConfig(config, "nvlink", 1));
    EXPECT_FALSE(ParseEgmStorePoolConfig(config, "rdma", 1));
    EXPECT_FALSE(ParseEgmStorePoolConfig(config, "nvlink", 0));
}

TEST(EgmStorePoolCapacityTest, RoundsDownAndSplitsAlignedUnitsInInputOrder) {
    const auto capacity = CalculateEgmStorePoolCapacity(57, {4, 8}, 4, 20);
    ASSERT_TRUE(capacity);
    EXPECT_EQ(capacity->alignment, 8);
    EXPECT_EQ(capacity->max_chunk_size, 16);
    EXPECT_EQ(capacity->node_sizes, (std::vector<size_t>{32, 24}));
}

TEST(EgmStorePoolCapacityTest, UsesLargestPowerOfTwoGranularity) {
    const auto capacity = CalculateEgmStorePoolCapacity(143, {4, 16, 8}, 8, 63);
    ASSERT_TRUE(capacity);
    EXPECT_EQ(capacity->alignment, 16);
    EXPECT_EQ(capacity->max_chunk_size, 48);
    EXPECT_EQ(capacity->node_sizes, (std::vector<size_t>{48, 48, 32}));
}

TEST(EgmStorePoolCapacityTest, IncludesStoreSlabAlignment) {
    const auto capacity = CalculateEgmStorePoolCapacity(17, {2, 4}, 8, 16);
    ASSERT_TRUE(capacity);
    EXPECT_EQ(capacity->alignment, 8);
    EXPECT_EQ(capacity->node_sizes, (std::vector<size_t>{8, 8}));
}

TEST(EgmStorePoolCapacityTest, RejectsMissingNodesOrInvalidAlignment) {
    EXPECT_FALSE(CalculateEgmStorePoolCapacity(64, {}, 4, 64));
    for (const size_t invalid : {0, 3, 12}) {
        SCOPED_TRACE(invalid);
        EXPECT_FALSE(CalculateEgmStorePoolCapacity(64, {4}, invalid, 64));
        const auto capacity =
            CalculateEgmStorePoolCapacity(64, {4, invalid}, 4, 64);
        ASSERT_FALSE(capacity);
        EXPECT_EQ(capacity.error(), ErrorCode::INVALID_PARAMS);
    }
}

TEST(EgmStorePoolCapacityTest, RequiresOneUnitPerNodeAndAnAlignedMrCap) {
    for (const size_t requested : {0, 7, 15}) {
        SCOPED_TRACE(requested);
        EXPECT_FALSE(CalculateEgmStorePoolCapacity(requested, {8, 4}, 4, 64));
    }
    EXPECT_FALSE(CalculateEgmStorePoolCapacity(64, {4, 8}, 4, 0));
    EXPECT_FALSE(CalculateEgmStorePoolCapacity(64, {4, 8}, 4, 7));
    EXPECT_TRUE(CalculateEgmStorePoolCapacity(16, {4, 8}, 4, 8));
}

TEST(EgmStorePoolCapacityTest, MaximumSizeRoundsDownWithoutOverflow) {
    const size_t maximum = std::numeric_limits<size_t>::max();
    const auto capacity =
        CalculateEgmStorePoolCapacity(maximum, {8, 4}, 4, maximum);
    ASSERT_TRUE(capacity);
    ASSERT_EQ(capacity->node_sizes.size(), 2);
    EXPECT_EQ(capacity->max_chunk_size, maximum - maximum % 8);
    EXPECT_EQ(capacity->node_sizes[0] + capacity->node_sizes[1],
              maximum - maximum % 8);
    EXPECT_EQ(capacity->node_sizes[0] - capacity->node_sizes[1], 8);
}

TEST(EgmStorePoolConfigTest, MnnvlDefaultsToEgmWithPrivateLocalBuffers) {
    auto parsed = ParseEgmStorePoolConfig({}, "mnnvl", 64);
    ASSERT_TRUE(parsed);
    EXPECT_TRUE(parsed->enabled);
    EXPECT_TRUE(parsed->auto_numa_nodes);
    EXPECT_FALSE(parsed->nearest_numa_node);
    EXPECT_TRUE(ParseEgmStorePoolConfig({}, "mnnvl", 0));
    ConfigDict disabled{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "false"}};
    EXPECT_FALSE(ParseEgmStorePoolConfig(disabled, "mnnvl", 1));
    parsed = ParseEgmStorePoolConfig(disabled, "mnnvl", 0);
    ASSERT_TRUE(parsed);
    EXPECT_TRUE(parsed->enabled);
    EXPECT_EQ(NormalizeEgmStoreProtocol("nvlink_host"), "mnnvl");
    EXPECT_EQ(NormalizeEgmStoreProtocol("nvlink"), "nvlink");
}

TEST(EgmStorePoolConfigTest, AliasUsesNearestOrStrictFabricNumaOverride) {
    EgmEnvGuard backend("MC_NVLINK_HOST_BACKEND", "fabric");
    EgmEnvGuard unset_node("MC_FABRIC_NUMA", nullptr);
    auto parsed = ParseEgmStorePoolConfig({}, "nvlink_host", 0);
    ASSERT_TRUE(parsed);
    EXPECT_TRUE(parsed->nearest_numa_node);
    {
        EgmEnvGuard node("MC_FABRIC_NUMA", " 3 ");
        parsed = ParseEgmStorePoolConfig({}, "nvlink_host", 32);
        ASSERT_TRUE(parsed);
        EXPECT_EQ(parsed->numa_nodes, (std::vector<int>{3}));
        EXPECT_FALSE(parsed->auto_numa_nodes);
    }
    for (const auto* value : {"-1", " -1 "}) {
        EgmEnvGuard node("MC_FABRIC_NUMA", value);
        parsed = ParseEgmStorePoolConfig({}, "nvlink_host", 32);
        ASSERT_TRUE(parsed);
        EXPECT_TRUE(parsed->nearest_numa_node);
    }
    for (const auto* value : {"", "-2", "+1", "1x", "2147483648"}) {
        EgmEnvGuard node("MC_FABRIC_NUMA", value);
        EXPECT_FALSE(ParseEgmStorePoolConfig({}, "nvlink_host", 32));
        EXPECT_TRUE(ParseEgmStorePoolConfig({}, "mnnvl", 32));
        ConfigDict explicit_nodes{{CONFIG_KEY_EGM_NUMA_NODES, "2,0,2"}};
        parsed = ParseEgmStorePoolConfig(explicit_nodes, "nvlink_host", 32);
        ASSERT_TRUE(parsed);
        EXPECT_EQ(parsed->numa_nodes, (std::vector<int>{0, 2}));
    }
}

TEST(EgmStorePoolConfigTest, AliasRejectsNonFabricBackend) {
    for (const auto* value : {"shm", "", "auto", "FABRIC"}) {
        EgmEnvGuard backend("MC_NVLINK_HOST_BACKEND", value);
        EXPECT_FALSE(ParseEgmStorePoolConfig({}, "nvlink_host", 0));
        EXPECT_TRUE(ParseEgmStorePoolConfig({}, "mnnvl", 0));
    }
}

TEST(EgmStoreLifecycleTest,
     RejectsUnavailableExternalEngineWithoutReinitializing) {
    // Satisfy the placement gate first so rejection below is attributable
    // to the non-strict external engine, not missing identity env vars.
    EgmEnvGuard domain("MOONCAKE_FABRIC_DOMAIN", "test-fabric");
    EgmEnvGuard rack("MC_RACK", "rack1");
    EgmEnvGuard tray("MC_TRAY", "host1");
    EgmEnvGuard gpu("MC_PHYSICAL_GPU", "3");
    auto engine = std::make_shared<TransferEngine>();
    EXPECT_FALSE(engine->isStrictMnnvlEgm());
    EXPECT_FALSE(Client::Create("egm-test", "P2PHANDSHAKE", "mnnvl", {},
                                "127.0.0.1:1", engine));
    EXPECT_FALSE(engine->isStrictMnnvlEgm());
    void* base = reinterpret_cast<void*>(1);
    std::vector<int> nodes{1};
    size_t granularity = 1;
    EXPECT_NE(engine->allocateLocalMemory(&base, 64, "cpu:0"), 0);
    EXPECT_EQ(base, nullptr);
    EXPECT_NE(engine->getEgmNumaNodes(nodes), 0);
    EXPECT_TRUE(nodes.empty());
    EXPECT_NE(engine->getEgmGranularity(0, granularity), 0);
    EXPECT_EQ(granularity, 0);
}

TEST(EgmStoreLifecycleTest, RejectsMasterPersistenceBeforeCreatingStorage) {
    auto client = EgmStoreLifecycleTestPeer::MakeHotCacheClient("mnnvl");
    // These paths must never be opened, for either storage-config RPC path.
    for (const char* root :
         {"/no-such-mooncake-egm-storage", "s3://egm-test"}) {
        EXPECT_EQ(EgmStoreLifecycleTestPeer::PreparePersistence(*client, root,
                                                                "cluster"),
                  ErrorCode::INVALID_PARAMS);
        EXPECT_FALSE(EgmStoreLifecycleTestPeer::HasStorageBackend(*client));
    }
}

using Peer = EgmStoreLifecycleTestPeer;

size_t CountCalls(const Peer::Faults& faults, const std::string& prefix) {
    return std::count_if(
        faults.calls.begin(), faults.calls.end(),
        [&](const std::string& call) { return call.rfind(prefix, 0) == 0; });
}

TEST(EgmStoreLifecycleTest, StrictMountPublishesPlacementAndTracksState) {
    auto faults = std::make_shared<Peer::Faults>();
    auto client = Peer::MakeClient(faults);
    auto state = std::make_shared<Client::MnnvlMountState>();
    void* buffer = reinterpret_cast<void*>(0x10000);
    auto id =
        client->MountSegmentAndGetId(buffer, 4096, "mnnvl", "cpu:2", state);
    ASSERT_TRUE(id);
    EXPECT_EQ(state->id, *id);
    EXPECT_TRUE(state->remountable);
    EXPECT_FALSE(state->cleanup_confirmed.load());
    ASSERT_TRUE(faults->mounted_segment);
    EXPECT_EQ(faults->mounted_segment->name, "rack1.host1#g3/numa2");
    EXPECT_EQ(faults->mounted_segment->protocol, "mnnvl");
    ASSERT_TRUE(faults->mounted_segment->physical_placement);
    EXPECT_EQ(faults->mounted_segment->physical_placement->fabric_domain,
              "test-fabric");
    EXPECT_EQ(faults->mounted_segment->physical_placement->numa_node, 2);
    EXPECT_EQ(Peer::MountedCount(*client), 1u);
    EXPECT_EQ(Peer::MountStateCount(*client), 1u);
    EXPECT_TRUE(client->UnmountSegmentById(*id));
    EXPECT_TRUE(state->cleanup_confirmed.load());
    EXPECT_EQ(Peer::MountedCount(*client), 0u);
    EXPECT_EQ(Peer::MountStateCount(*client), 0u);
}

TEST(EgmStoreLifecycleTest, RegistrationFailureRollsBackWithoutPublishing) {
    auto faults = std::make_shared<Peer::Faults>();
    auto client = Peer::MakeClient(faults);
    faults->registration = -1;
    void* buffer = reinterpret_cast<void*>(0x10000);
    auto result = client->MountSegmentAndGetId(buffer, 4096, "mnnvl", "cpu:0");
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(CountCalls(*faults, "register:"), 1u);
    EXPECT_EQ(CountCalls(*faults, "mount:"), 0u);
    EXPECT_EQ(CountCalls(*faults, "unregister"), 1u);
    EXPECT_EQ(CountCalls(*faults, "drain"), 1u);
    EXPECT_EQ(Peer::MountedCount(*client), 0u);
    EXPECT_EQ(Peer::MountStateCount(*client), 0u);
}

TEST(EgmStoreLifecycleTest, ExportFailureRollsBackBeforePublication) {
    auto faults = std::make_shared<Peer::Faults>();
    auto client = Peer::MakeClient(faults);
    faults->exported = false;
    void* buffer = reinterpret_cast<void*>(0x10000);
    auto result = client->MountSegmentAndGetId(buffer, 4096, "mnnvl", "cpu:0");
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(CountCalls(*faults, "register:"), 1u);
    EXPECT_EQ(CountCalls(*faults, "mount:"), 0u);
    EXPECT_EQ(CountCalls(*faults, "unregister"), 1u);
    EXPECT_EQ(CountCalls(*faults, "drain"), 1u);
    EXPECT_EQ(Peer::MountedCount(*client), 0u);
    EXPECT_EQ(Peer::MountStateCount(*client), 0u);
}

TEST(EgmStoreLifecycleTest, MountFailureRollsBackAndAllowsRemount) {
    auto faults = std::make_shared<Peer::Faults>();
    auto client = Peer::MakeClient(faults);
    auto state = std::make_shared<Client::MnnvlMountState>();
    void* buffer = reinterpret_cast<void*>(0x10000);
    faults->mount = ErrorCode::INTERNAL_ERROR;
    auto result =
        client->MountSegmentAndGetId(buffer, 4096, "mnnvl", "cpu:0", state);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_FALSE(state->remountable);
    EXPECT_TRUE(state->cleanup_confirmed.load());
    EXPECT_EQ(CountCalls(*faults, "unmount"), 1u);
    EXPECT_EQ(Peer::MountedCount(*client), 0u);
    EXPECT_EQ(Peer::MountStateCount(*client), 0u);
    // A confirmed cleanup lets the same state back a fresh mount.
    faults->mount = ErrorCode::OK;
    auto id =
        client->MountSegmentAndGetId(buffer, 4096, "mnnvl", "cpu:0", state);
    ASSERT_TRUE(id);
    EXPECT_TRUE(state->remountable);
}

TEST(EgmStoreLifecycleTest, FailedCleanupRetainsStateAndBlocksRemount) {
    auto faults = std::make_shared<Peer::Faults>();
    auto client = Peer::MakeClient(faults);
    auto state = std::make_shared<Client::MnnvlMountState>();
    void* buffer = reinterpret_cast<void*>(0x10000);
    auto id =
        client->MountSegmentAndGetId(buffer, 4096, "mnnvl", "cpu:0", state);
    ASSERT_TRUE(id);
    faults->unmount = ErrorCode::INTERNAL_ERROR;
    auto result = client->UnmountSegmentById(*id);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_FALSE(state->remountable);
    EXPECT_FALSE(state->cleanup_confirmed.load());
    EXPECT_EQ(Peer::MountedCount(*client), 1u);
    EXPECT_EQ(Peer::MountStateCount(*client), 1u);
    // An unconfirmed cleanup must block reusing the state for a new range.
    auto blocked = client->MountSegmentAndGetId(
        reinterpret_cast<void*>(0x20000), 4096, "mnnvl", "cpu:0", state);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error(), ErrorCode::INVALID_PARAMS);
    // Retry tolerates a still-failing drain, then completes once it recovers.
    faults->unmount = ErrorCode::OK;
    faults->drain = -1;
    result = client->UnmountSegmentById(*id);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(Peer::MountStateCount(*client), 1u);
    faults->drain = 0;
    EXPECT_TRUE(client->UnmountSegmentById(*id));
    EXPECT_TRUE(state->cleanup_confirmed.load());
    EXPECT_EQ(Peer::MountedCount(*client), 0u);
    EXPECT_EQ(Peer::MountStateCount(*client), 0u);
    EXPECT_EQ(CountCalls(*faults, "unmount"), 2u);
}

TEST(EgmStoreLifecycleTest, GracefulUnmountChainsCallbacksAndFiresOnce) {
    auto faults = std::make_shared<Peer::Faults>();
    auto client = Peer::MakeClient(faults);
    void* buffer = reinterpret_cast<void*>(0x10000);
    auto id = client->MountSegmentAndGetId(buffer, 4096, "mnnvl", "cpu:0");
    ASSERT_TRUE(id);
    std::vector<std::string> fired;
    Peer::StageGraceful(*client, *id,
                        [&](const UUID&) { fired.push_back("first"); });
    // A repeated graceful request without a callback keeps the stored one.
    EXPECT_TRUE(client->UnmountSegmentById(*id, 1000));
    // A repeated graceful request chains its callback after the stored one.
    EXPECT_TRUE(client->UnmountSegmentById(
        *id, 1000, [&](const UUID&) { fired.push_back("second"); }));
    EXPECT_TRUE(fired.empty());
    // Immediate completion fires both exactly once, in registration order.
    EXPECT_TRUE(client->UnmountSegmentById(*id, 0));
    EXPECT_EQ(fired, (std::vector<std::string>{"first", "second"}));
    EXPECT_EQ(Peer::MountedCount(*client), 0u);
    EXPECT_EQ(Peer::MountStateCount(*client), 0u);
}

}  // namespace
}  // namespace mooncake
