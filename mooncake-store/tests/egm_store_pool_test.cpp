// Copyright 2024 KVCache.AI

#include <gtest/gtest.h>

#include <limits>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "../src/config/egm_store_pool_config.h"
#include "real_client.h"
#include "embedded_master.h"
#ifdef MOONCAKE_TEST_STORE_MNNVL
#include <cuda_runtime.h>
#endif

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
    static tl::expected<void, ErrorCode> SetupResolvedOffload(
        RealClient& real, const std::string& protocol) {
        return real.setup_internal(
            EgmStorePoolConfig{}, "egm-test", "P2PHANDSHAKE", 64 << 20, 0,
            protocol, "", "127.0.0.1:1", nullptr, "", 50052, true, true, "",
            "default", false, DEFAULT_CLIENT_HTTP_PORT, true);
    }
    static bool HasSetupResources(const RealClient& real) {
        return real.client_ || real.embedded_master_ ||
               real.offload_rpc_server_ || real.client_buffer_allocator_ ||
               real.port_binder_ || !real.engine_egm_segment_ptrs_.empty();
    }
    static std::shared_ptr<EngineEgmAllocation> MakeOwner(
        const std::shared_ptr<Faults>& faults,
        std::shared_ptr<TransferEngine> engine = {}) {
        auto owner = std::make_shared<EngineEgmAllocation>(std::move(engine));
        owner->base_ = new char[64];
        owner->size_ = 64;
        owner->drain_for_test_ = [faults] { return faults->drain; };
        owner->free_for_test_ = [faults](void* base) {
            if (faults->free_result) return faults->free_result;
            ++faults->freed;
            delete[] static_cast<char*>(base);
            return 0;
        };
        return owner;
    }
    static void StageGraceful(Client& client, const UUID& id,
                              std::function<void(const UUID&)> callback) {
        std::lock_guard<std::mutex> lock(client.mounted_segments_mutex_);
        client.gracefully_unmounting_segments_[id] =
            client.mounted_segments_.at(id);
        client.mounted_segments_.erase(id);
        client.graceful_unmount_cleanup_callbacks_[id] = std::move(callback);
    }
    static std::shared_ptr<Client> GetClient(RealClient& real) {
        return real.client_;
    }
    static std::string MasterAddress(RealClient& real) {
        return real.embedded_master_->master_address();
    }
    static void* LocalBuffer(RealClient& real) {
        return real.client_buffer_allocator_->getBase();
    }
    static const auto& Owners(RealClient& real) {
        return real.engine_egm_segment_ptrs_;
    }
    static void SetSetupState(RealClient& real, std::shared_ptr<Client> client,
                              std::shared_ptr<EngineEgmAllocation> owner) {
        real.protocol = "mnnvl";
        real.client_ = std::move(client);
        real.engine_egm_segment_ptrs_.clear();
        if (owner) real.engine_egm_segment_ptrs_.push_back(std::move(owner));
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

TEST(EgmStoreLifecycleTest, MountLabelsPhysicalGpuAndNumaNotEndpoint) {
    using Peer = EgmStoreLifecycleTestPeer;
    auto faults = std::make_shared<Peer::Faults>();
    auto client = Peer::MakeClient(faults);
    auto owner = Peer::MakeOwner(faults);
    ASSERT_TRUE(client->MountSegment(owner->base(), 64, "mnnvl", "cpu:2",
                                     owner->mount_state()));
    ASSERT_TRUE(faults->mounted_segment);
    const auto& segment = *faults->mounted_segment;
    EXPECT_EQ(segment.name, "rack1.host1#g3/numa2");
    EXPECT_EQ(segment.te_endpoint, "egm-test");
    ASSERT_TRUE(segment.physical_placement);
    EXPECT_EQ(segment.physical_placement->physical_gpu, 3);
    EXPECT_EQ(segment.physical_placement->numa_node, 2);
    EXPECT_EQ(segment.physical_placement->fabric_domain, "test-fabric");
    ASSERT_TRUE(client->UnmountSegment(owner->base(), 64));
    EXPECT_EQ(owner->Release(), 0);
}

TEST(EgmStoreLifecycleTest, NeverRegisteredAllocationCanReleaseAndRepeat) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    EXPECT_EQ(owner->Release(), 0);
    EXPECT_EQ(owner->Release(), 0);
    EXPECT_EQ(faults->freed, 1);
    EngineEgmAllocation unsupported(nullptr);
    EXPECT_NE(unsupported.Allocate(0, 64), 0);
    EXPECT_EQ(unsupported.base(), nullptr);
}

TEST(EgmStoreLifecycleTest, FreeFailureRetainsOwnerForRetry) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    void* base = owner->base();
    faults->free_result = -1;
    EXPECT_NE(owner->Release(), 0);
    EXPECT_EQ(owner->base(), base);
    EXPECT_EQ(faults->freed, 0);
    faults->free_result = 0;
    EXPECT_EQ(owner->Release(), 0);
    EXPECT_EQ(owner->Release(), 0);
    EXPECT_EQ(faults->freed, 1);
}

TEST(EgmStoreLifecycleTest,
     RejectsUnavailableExternalEngineWithoutReinitializing) {
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

TEST(EgmStoreLifecycleTest, RejectsOffloadBeforeSetupForEveryBackend) {
    for (const char* protocol : {"mnnvl", "nvlink_host"}) {
        for (const char* backend :
             {"bucket_storage_backend", "file_per_key_storage_backend",
              "offset_allocator_storage_backend", "distributed_storage_backend",
              "nvme_kv_storage_backend"}) {
            SCOPED_TRACE(protocol);
            SCOPED_TRACE(backend);
            EgmEnvGuard storage("MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR",
                                backend);
            RealClient real;
            const auto original_protocol = real.protocol;
            const bool original_closed = real.closed_.load();
            auto result = real.setup_internal(
                "egm-test", "P2PHANDSHAKE", 64 << 20, 0, protocol, "",
                "127.0.0.1:1", nullptr, "", 50052, true, true, "", "default",
                false, DEFAULT_CLIENT_HTTP_PORT, true);
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            result =
                EgmStoreLifecycleTestPeer::SetupResolvedOffload(real, protocol);
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            ConfigDict config{{CONFIG_KEY_LOCAL_HOSTNAME, "egm-test"},
                              {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
                              {CONFIG_KEY_PROTOCOL, protocol},
                              {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "64MB"},
                              {CONFIG_KEY_LOCAL_BUFFER_SIZE, "0"},
                              {CONFIG_KEY_ENABLE_EMBEDDED_MASTER, "true"},
                              {"enable_ssd_offload", "true"}};
            result = real.setup_internal(config);
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            EXPECT_EQ(real.protocol, original_protocol);
            EXPECT_EQ(real.closed_.load(), original_closed);
            EXPECT_FALSE(EgmStoreLifecycleTestPeer::HasSetupResources(real));
        }
    }
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

TEST(EgmStoreLifecycleTest, RejectedSetupPreservesLiveAndPendingOwners) {
    using Peer = EgmStoreLifecycleTestPeer;
    auto faults = std::make_shared<Peer::Faults>();
    auto client = Peer::MakeClient(faults);
    auto owner = Peer::MakeOwner(faults);
    RealClient real;
    for (bool has_client : {true, false}) {
        Peer::SetSetupState(real, has_client ? client : nullptr, owner);
        for (const char* protocol : {"mnnvl", "nvlink_host", "nvlink", "tcp"}) {
            auto result = real.setup_internal("egm-test", "P2PHANDSHAKE",
                                              64 << 20, 0, protocol);
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            ConfigDict config{{CONFIG_KEY_LOCAL_HOSTNAME, "egm-test"},
                              {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
                              {CONFIG_KEY_PROTOCOL, protocol},
                              {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "64MB"},
                              {CONFIG_KEY_LOCAL_BUFFER_SIZE, "0"}};
            result = real.setup_internal(config);
            ASSERT_FALSE(result);
            EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
            EXPECT_EQ(Peer::GetClient(real), has_client ? client : nullptr);
            ASSERT_EQ(Peer::Owners(real).size(), 1);
            EXPECT_EQ(Peer::Owners(real).front(), owner);
            EXPECT_EQ(real.protocol, "mnnvl");
            EXPECT_FALSE(real.closed_.load());
            EXPECT_EQ(faults->freed, 0);
            EXPECT_TRUE(faults->calls.empty());
        }
    }
    // Detach the fake setup state; it has no native engine to drain.
    Peer::SetSetupState(real, nullptr, nullptr);
    EXPECT_EQ(owner->Release(), 0);
}

TEST(EgmStoreLifecycleTest, FailedRegistrationRequiresConfirmedCleanup) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    faults->registration = -1;
    faults->unregister = -1;
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    EXPECT_FALSE(client->MountSegment(owner->base(), owner->length(), "mnnvl",
                                      "cpu:2", owner->mount_state()));
    EXPECT_NE(owner->Release(), 0);
    EXPECT_EQ(faults->freed, 0);
    EXPECT_EQ(faults->calls,
              (std::vector<std::string>{"register:cpu:2", "unregister"}));
    faults->unregister = 0;
    ASSERT_TRUE(client->UnmountSegment(owner->base(), owner->length()));
    EXPECT_EQ(owner->Release(), 0);
}

TEST(EgmStoreLifecycleTest, ExportFailureNeverPublishesAndRollsBack) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    faults->exported = false;
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    EXPECT_FALSE(client->MountSegment(owner->base(), owner->length(), "mnnvl",
                                      "cpu:1", owner->mount_state()));
    EXPECT_EQ(faults->calls, (std::vector<std::string>{"register:cpu:1",
                                                       "unregister", "drain"}));
    EXPECT_EQ(owner->Release(), 0);
}

TEST(EgmStoreLifecycleTest, UncertainMasterPublicationRetainsUuidAndBacking) {
    EgmEnvGuard backend("MC_NVLINK_HOST_BACKEND", "fabric");
    EgmEnvGuard numa("MC_FABRIC_NUMA", nullptr);
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    faults->mount = ErrorCode::RPC_FAIL;
    faults->unmount = ErrorCode::RPC_FAIL;
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    EXPECT_FALSE(client->MountSegment(owner->base(), owner->length(),
                                      "nvlink_host", "cpu:0",
                                      owner->mount_state()));
    EXPECT_NE(owner->Release(), 0);
    EXPECT_TRUE(owner->mount_state()->publication_pending);
    EXPECT_FALSE(owner->mount_state()->remountable);
    EXPECT_EQ(faults->calls.back(), "unmount");
    faults->unmount = ErrorCode::SEGMENT_NOT_FOUND;
    ASSERT_TRUE(client->UnmountSegmentById(owner->mount_state()->id));
    EXPECT_EQ(owner->Release(), 0);
    EXPECT_EQ(faults->freed, 1);
}

TEST(EgmStoreLifecycleTest, AliasMountExplicitNodeOverridesEnvironment) {
    EgmEnvGuard backend("MC_NVLINK_HOST_BACKEND", "fabric");
    EgmEnvGuard numa("MC_FABRIC_NUMA", "invalid");
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    EXPECT_FALSE(client->MountSegment(owner->base(), 64, "nvlink_host", "cpu",
                                      owner->mount_state()));
    EXPECT_TRUE(faults->calls.empty());
    ASSERT_TRUE(client->MountSegment(owner->base(), 64, "nvlink_host", "cpu:2",
                                     owner->mount_state()));
    EXPECT_EQ(faults->calls.front(), "register:cpu:2");
    ASSERT_TRUE(client->UnmountSegmentById(owner->mount_state()->id));
    EXPECT_EQ(owner->Release(), 0);
}

TEST(EgmStoreLifecycleTest, DrainFailureRetriesWithoutRepeatingUnregister) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    ASSERT_TRUE(client->MountSegment(owner->base(), owner->length(), "mnnvl",
                                     "cpu:0", owner->mount_state()));
    faults->drain = -1;
    EXPECT_FALSE(client->UnmountSegmentById(owner->mount_state()->id));
    EXPECT_NE(owner->Release(), 0);
    faults->calls.clear();
    faults->drain = 0;
    ASSERT_TRUE(client->UnmountSegmentById(owner->mount_state()->id));
    EXPECT_EQ(faults->calls, (std::vector<std::string>{"drain"}));
    EXPECT_EQ(owner->Release(), 0);
}

TEST(EgmStoreLifecycleTest, PartialChunksRetainOnlyFailedCleanup) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    auto first = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    auto second = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    ASSERT_TRUE(client->MountSegment(first->base(), 64, "mnnvl", "cpu:0",
                                     first->mount_state()));
    ASSERT_TRUE(client->MountSegment(second->base(), 64, "mnnvl", "cpu:1",
                                     second->mount_state()));
    faults->unmount = ErrorCode::RPC_FAIL;
    EXPECT_FALSE(client->UnmountSegmentById(first->mount_state()->id));
    EXPECT_NE(first->Release(), 0);
    faults->unmount = ErrorCode::OK;
    ASSERT_TRUE(client->UnmountSegmentById(second->mount_state()->id));
    EXPECT_EQ(second->Release(), 0);
    EXPECT_EQ(faults->freed, 1);
    ASSERT_TRUE(client->UnmountSegmentById(first->mount_state()->id));
    EXPECT_EQ(first->Release(), 0);
    EXPECT_EQ(faults->freed, 2);
}

TEST(EgmStoreLifecycleTest, CloseCleansExternalAndGracefulMountsAndRetries) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    auto first = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    auto second = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    ASSERT_TRUE(client->MountSegment(first->base(), 64, "mnnvl", "cpu:0",
                                     first->mount_state()));
    ASSERT_TRUE(client->MountSegment(second->base(), 64, "mnnvl", "cpu:1",
                                     second->mount_state()));
    int callbacks = 0;
    EgmStoreLifecycleTestPeer::StageGraceful(
        *client, second->mount_state()->id, [&, second](const UUID&) {
            ++callbacks;
            EXPECT_EQ(second->Release(), 0);
        });
    client->StopStoreWorkers();
    faults->unregister = -1;
    EXPECT_FALSE(client->CleanupMnnvlSegments());
    EXPECT_EQ(callbacks, 0);
    EXPECT_EQ(faults->freed, 0);
    faults->unregister = 0;
    ASSERT_TRUE(client->CleanupMnnvlSegments());
    EXPECT_EQ(callbacks, 1);
    EXPECT_EQ(first->Release(), 0);
    EXPECT_EQ(faults->freed, 2);
    EXPECT_TRUE(client->CleanupMnnvlSegments());
    EXPECT_EQ(callbacks, 1);
}

TEST(EgmStoreLifecycleTest, ImmediateGracefulCleanupInvokesCallbackOnce) {
    using Peer = EgmStoreLifecycleTestPeer;
    for (bool by_id : {true, false}) {
        auto faults = std::make_shared<Peer::Faults>();
        auto client = Peer::MakeClient(faults);
        auto owner = Peer::MakeOwner(faults);
        void* base = owner->base();
        ASSERT_TRUE(client->MountSegment(base, 64, "mnnvl", "cpu:0",
                                         owner->mount_state()));
        const UUID id = owner->mount_state()->id;
        int callbacks = 0;
        Peer::StageGraceful(*client, id, [&, owner](const UUID& removed) {
            EXPECT_EQ(removed, id);
            EXPECT_TRUE(owner->mount_state()->cleanup_confirmed.load());
            EXPECT_EQ(faults->calls.back(), "drain");
            // A callback can reenter Client after the segment lock is released.
            EXPECT_TRUE(client->CleanupMnnvlSegments());
            ++callbacks;
            EXPECT_EQ(owner->Release(), 0);
        });
        auto cleanup = [&] {
            return by_id ? client->UnmountSegmentById(id)
                         : client->UnmountSegment(base, 64);
        };
        faults->unregister = -1;
        EXPECT_FALSE(cleanup());
        EXPECT_EQ(callbacks, 0);
        EXPECT_EQ(faults->freed, 0);
        faults->unregister = 0;
        ASSERT_TRUE(cleanup());
        EXPECT_EQ(callbacks, 1);
        EXPECT_EQ(faults->freed, 1);
        EXPECT_TRUE(client->CleanupMnnvlSegments());
        client.reset();
        EXPECT_EQ(callbacks, 1);
    }
}

TEST(EgmStoreLifecycleTest, GracefulDestructorDoesNotFreeOnUnregisterFailure) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults);
    ASSERT_TRUE(client->MountSegment(owner->base(), 64, "mnnvl", "cpu:0",
                                     owner->mount_state()));
    bool callback_called = false;
    EgmStoreLifecycleTestPeer::StageGraceful(*client, owner->mount_state()->id,
                                             [&](const UUID&) {
                                                 callback_called = true;
                                                 EXPECT_EQ(owner->Release(), 0);
                                             });
    faults->unregister = -1;
    client.reset();
    EXPECT_FALSE(callback_called);
    EXPECT_EQ(faults->freed, 0);
    EXPECT_FALSE(owner->mount_state()->cleanup_confirmed.load());
    // The fake backend is now torn down; explicitly confirm safe test cleanup.
    owner->mount_state()->cleanup_confirmed = true;
    EXPECT_EQ(owner->Release(), 0);
}

TEST(EgmStoreLifecycleTest,
     GracefulCleanupPrecedesCallbackAndKeepsEngineAlive) {
    auto faults = std::make_shared<EgmStoreLifecycleTestPeer::Faults>();
    auto engine = std::make_shared<TransferEngine>();
    std::weak_ptr<TransferEngine> weak = engine;
    auto owner = EgmStoreLifecycleTestPeer::MakeOwner(faults, engine);
    engine.reset();
    EXPECT_FALSE(weak.expired());
    auto client = EgmStoreLifecycleTestPeer::MakeClient(faults);
    ASSERT_TRUE(client->MountSegment(owner->base(), 64, "mnnvl", "cpu:0",
                                     owner->mount_state()));
    bool callback_called = false;
    EgmStoreLifecycleTestPeer::StageGraceful(
        *client, owner->mount_state()->id, [&](const UUID&) {
            EXPECT_TRUE(owner->mount_state()->cleanup_confirmed.load());
            EXPECT_EQ(faults->calls.back(), "drain");
            callback_called = true;
            EXPECT_EQ(owner->Release(), 0);
        });
    client.reset();
    EXPECT_TRUE(callback_called);
    owner.reset();
    EXPECT_TRUE(weak.expired());
}

#ifdef MOONCAKE_TEST_STORE_MNNVL
TEST(MnnvlStoreFabricTest, HostGpuBatchDynamicAndReinstall) {
    if (!std::getenv("MOONCAKE_RUN_STORE_MNNVL"))
        GTEST_SKIP() << "Fabric opt-in required";
    EgmEnvGuard domain("MOONCAKE_FABRIC_DOMAIN", "local-test-fabric");
    EgmEnvGuard rack("MC_RACK", "test-rack");
    EgmEnvGuard tray("MC_TRAY", "test-host");
    EgmEnvGuard physical_gpu("MC_PHYSICAL_GPU",
                             std::getenv("MC_PHYSICAL_GPU")
                                 ? std::getenv("MC_PHYSICAL_GPU")
                                 : (std::getenv("CUDA_VISIBLE_DEVICES")
                                        ? std::getenv("CUDA_VISIBLE_DEVICES")
                                        : "0"));
    using Peer = EgmStoreLifecycleTestPeer;
    auto store = RealClient::create();
    auto initiator = RealClient::create();
    for (const char* protocol : {"mnnvl", "nvlink_host"}) {
        ConfigDict config{{CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
                          {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
                          {CONFIG_KEY_PROTOCOL, protocol},
                          {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "256MB"},
                          {CONFIG_KEY_LOCAL_BUFFER_SIZE, "8MB"},
                          {"enable_embedded_master", "true"}};
        ASSERT_TRUE(store->setup_internal(config))
            << "Requested Fabric/IMEX must work";
        auto storage_client = Peer::GetClient(*store);
        ASSERT_EQ(storage_client->GetProtocol(), "mnnvl");
        auto engine = storage_client->GetTransferEngine();
        ASSERT_TRUE(engine->isStrictMnnvlEgm());
        ASSERT_FALSE(Peer::Owners(*store).empty());
        for (const auto& owner : Peer::Owners(*store))
            ASSERT_TRUE(
                engine->isMnnvlExported(owner->base(), owner->length()));
        EXPECT_FALSE(store->setup_internal(config));
        ASSERT_EQ(Peer::GetClient(*store), storage_client);
        for (const auto& owner : Peer::Owners(*store))
            ASSERT_TRUE(
                engine->isMnnvlExported(owner->base(), owner->length()));
        config.erase("enable_embedded_master");
        config[CONFIG_KEY_MASTER_SERVER_ADDR] = Peer::MasterAddress(*store);
        config[CONFIG_KEY_GLOBAL_SEGMENT_SIZE] = "0";
        ASSERT_TRUE(initiator->setup_internal(config));
        auto client = Peer::GetClient(*initiator);
        ASSERT_FALSE(
            client->CanUseLocalMemcpy(storage_client->GetTransportEndpoint()));
        constexpr size_t bytes = 4096;
        auto* local = static_cast<char*>(Peer::LocalBuffer(*initiator));
        std::memset(local, 0x5a, bytes);
        std::vector<Slice> slices{{local, bytes}};
        ASSERT_TRUE(client->Put("host", slices, ReplicateConfig{}));
        std::memset(local, 0, bytes);
        ASSERT_TRUE(client->Get("host", slices));
        for (size_t i = 0; i < bytes; ++i) ASSERT_EQ(local[i], 0x5a);
        std::vector<std::string> keys{"batch-a", "batch-b"};
        std::vector<Slice> second_slices{{local + bytes, bytes}};
        std::memset(local + bytes, 0x3c, bytes);
        std::vector<std::vector<Slice>> batch{slices, second_slices};
        auto put_results = client->BatchPut(keys, batch, ReplicateConfig{});
        ASSERT_EQ(put_results.size(), keys.size());
        for (const auto& result : put_results) ASSERT_TRUE(result);
        std::memset(local, 0, bytes * 2);
        std::unordered_map<std::string, std::vector<Slice>> destinations{
            {keys[0], slices}, {keys[1], second_slices}};
        auto get_results = client->BatchGet(keys, destinations);
        ASSERT_EQ(get_results.size(), keys.size());
        for (const auto& result : get_results) ASSERT_TRUE(result);
        for (size_t i = 0; i < bytes; ++i) {
            ASSERT_EQ(local[i], 0x5a);
            ASSERT_EQ(local[bytes + i], 0x3c);
        }
        void* gpu = nullptr;
        ASSERT_EQ(cudaMalloc(&gpu, bytes), cudaSuccess);
        ASSERT_TRUE(
            client->RegisterLocalMemory(gpu, bytes, "cuda:0", false, true));
        ASSERT_FALSE(client->GetTransferEngine()->isMnnvlExported(gpu, bytes));
        cudaStream_t producer_stream = nullptr;
        cudaEvent_t producer_event = nullptr;
        ASSERT_EQ(
            cudaStreamCreateWithFlags(&producer_stream, cudaStreamNonBlocking),
            cudaSuccess);
        ASSERT_EQ(
            cudaEventCreateWithFlags(&producer_event, cudaEventDisableTiming),
            cudaSuccess);
        for (bool group_by_target : {false, true}) {
            SCOPED_TRACE(group_by_target);
            ASSERT_EQ(cudaMemsetAsync(gpu, 0x5a, bytes, producer_stream),
                      cudaSuccess);
            ASSERT_EQ(cudaEventRecord(producer_event, producer_stream),
                      cudaSuccess);
            ReplicateConfig write_config;
            write_config.prefer_alloc_in_same_node = group_by_target;
            const std::vector<std::string> event_keys =
                group_by_target
                    ? std::vector<std::string>{"gpu", "gpu-group"}
                    : std::vector<std::string>{"gpu-single-a", "gpu-single-b"};
            auto event_results = initiator->batch_put_from_multi_buffers(
                event_keys, {{gpu}, {gpu}}, {{bytes}, {bytes}}, write_config,
                false, producer_event);
            ASSERT_EQ(event_results, (std::vector<int>{0, 0}));
            for (const auto& event_key : event_keys) {
                std::memset(local, 0, bytes);
                ASSERT_TRUE(client->Get(event_key, slices));
                for (size_t i = 0; i < bytes; ++i) ASSERT_EQ(local[i], 0x5a);
            }
        }
        ASSERT_EQ(cudaEventDestroy(producer_event), cudaSuccess);
        ASSERT_EQ(cudaStreamDestroy(producer_stream), cudaSuccess);
        std::vector<Slice> gpu_slices{{gpu, bytes}};
        ASSERT_EQ(cudaMemset(gpu, 0, bytes), cudaSuccess);
        ASSERT_TRUE(client->Get("gpu", gpu_slices));
        std::memset(local, 0, bytes);
        ASSERT_EQ(cudaMemcpy(local, gpu, bytes, cudaMemcpyDeviceToHost),
                  cudaSuccess);
        for (size_t i = 0; i < bytes; ++i) ASSERT_EQ(local[i], 0x5a);
        ASSERT_TRUE(client->unregisterLocalMemory(gpu));
        ASSERT_EQ(client->GetTransferEngine()->quiesce(), 0);
        ASSERT_EQ(cudaFree(gpu), cudaSuccess);
        EXPECT_FALSE(
            storage_client->MountSegment(local, bytes, "mnnvl", "cpu:0"));
        std::vector<int> nodes;
        ASSERT_EQ(engine->getEgmNumaNodes(nodes), 0);
        ASSERT_FALSE(nodes.empty());
        EngineEgmAllocation external(engine);
        ASSERT_EQ(external.Allocate(nodes.front(), 64 << 20), 0);
        ASSERT_TRUE(storage_client->MountSegment(
            external.base(), external.length(), protocol,
            "cpu:" + std::to_string(nodes.front()), external.mount_state()));
        ASSERT_TRUE(
            engine->isMnnvlExported(external.base(), external.length()));
        ASSERT_TRUE(
            storage_client->UnmountSegment(external.base(), external.length()));
        ASSERT_EQ(external.Release(), 0);
        std::vector<std::string> ids;
        size_t allocated = 0;
        if (std::string(protocol) == "nvlink_host") {
            EgmEnvGuard numa("MC_FABRIC_NUMA", "invalid");
            ASSERT_EQ(store->allocateAndMountSegment(
                          64 << 20, protocol,
                          "cpu:" + std::to_string(nodes.front()), ids),
                      0);
            ASSERT_EQ(store->unmountAndFreeSegment(ids), 0);
        }
        ASSERT_EQ(store->allocateAndMountSegment(128 << 20, protocol, "*", ids,
                                                 &allocated),
                  0);
        ASSERT_GT(allocated, 0);
        auto mounted_ids = ids;
        EXPECT_NE(
            store->allocateAndMountSegment(128 << 20, protocol, "cuda:0", ids),
            0);
        EXPECT_TRUE(ids.empty());
        ASSERT_EQ(store->unmountAndFreeSegment(mounted_ids), 0);
        ASSERT_EQ(
            store->allocateAndMountSegment(128 << 20, protocol, "cpu", ids), 0);
        ASSERT_EQ(store->unmountAndFreeSegment(ids, 1), 0);
        ASSERT_EQ(initiator->tearDownAll(), 0);
        ASSERT_EQ(store->tearDownAll(), 0);
        EXPECT_EQ(store->tearDownAll(), 0);
    }
}

TEST(MnnvlStoreFabricTest, ExplicitMultiNumaPool) {
    if (!std::getenv("MOONCAKE_RUN_STORE_MNNVL"))
        GTEST_SKIP() << "Fabric opt-in required";
    EgmEnvGuard domain("MOONCAKE_FABRIC_DOMAIN", "local-test-fabric");
    EgmEnvGuard rack("MC_RACK", "test-rack");
    EgmEnvGuard tray("MC_TRAY", "test-host");
    EgmEnvGuard physical_gpu("MC_PHYSICAL_GPU",
                             std::getenv("MC_PHYSICAL_GPU")
                                 ? std::getenv("MC_PHYSICAL_GPU")
                                 : (std::getenv("CUDA_VISIBLE_DEVICES")
                                        ? std::getenv("CUDA_VISIBLE_DEVICES")
                                        : "0"));
    using Peer = EgmStoreLifecycleTestPeer;
    auto initiator = RealClient::create();
    ConfigDict config{{CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
                      {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
                      {CONFIG_KEY_PROTOCOL, "mnnvl"},
                      {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "0"},
                      {CONFIG_KEY_LOCAL_BUFFER_SIZE, "0"},
                      {"enable_embedded_master", "true"}};
    ASSERT_TRUE(initiator->setup_internal(config));
    std::vector<int> nodes;
    ASSERT_EQ(Peer::GetClient(*initiator)
                  ->GetTransferEngine()
                  ->getEgmNumaNodes(nodes),
              0);
    if (nodes.size() < 2) {
        ASSERT_EQ(initiator->tearDownAll(), 0);
        GTEST_SKIP() << "At least two GPU-visible host NUMA nodes required";
    }
    config.erase("enable_embedded_master");
    config[CONFIG_KEY_MASTER_SERVER_ADDR] = Peer::MasterAddress(*initiator);
    config[CONFIG_KEY_GLOBAL_SEGMENT_SIZE] = "256MB";
    config[CONFIG_KEY_EGM_NUMA_NODES] =
        std::to_string(nodes[1]) + "," + std::to_string(nodes[0]);
    auto store = RealClient::create();
    ASSERT_TRUE(store->setup_internal(config));
    ASSERT_GE(Peer::Owners(*store).size(), 2);
    auto engine = Peer::GetClient(*store)->GetTransferEngine();
    for (const auto& owner : Peer::Owners(*store))
        ASSERT_TRUE(engine->isMnnvlExported(owner->base(), owner->length()));
    ASSERT_EQ(store->tearDownAll(), 0);
    ASSERT_EQ(initiator->tearDownAll(), 0);
}
#endif

}  // namespace
}  // namespace mooncake
