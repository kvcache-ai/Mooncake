// Copyright 2024 KVCache.AI

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "egm_store_pool.h"
#include "real_client.h"

namespace mooncake {

class EgmStorePoolTestPeer {
   public:
    static void MarkCleanupPending(RealClient& client) {
        client.egm_store_pool_ =
            std::make_unique<EgmStorePool>(EgmStorePoolHooks{});
        client.ipc_socket_path_ = "cleanup-pending";
    }

    static bool HasPool(const RealClient& client) {
        return client.egm_store_pool_ != nullptr;
    }

    static const std::string& IpcSocketPath(const RealClient& client) {
        return client.ipc_socket_path_;
    }
};

namespace {

template <typename T>
EgmStorePoolResult<T> Fail(const std::string& message) {
    return tl::make_unexpected(message);
}

TEST(EgmStorePoolTest, ConfigIsDefaultOffAndParsesExplicitNodes) {
    ConfigDict defaults{{CONFIG_KEY_EGM_NUMA_NODES, "invalid"}};
    auto options = ParseEgmStorePoolOptions(defaults);
    ASSERT_TRUE(options);
    EXPECT_FALSE(options->enabled);

    ConfigDict invalid_enabled{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "maybe"}};
    EXPECT_FALSE(ParseEgmStorePoolOptions(invalid_enabled));

    ConfigDict explicit_nodes{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"},
                              {CONFIG_KEY_EGM_NUMA_NODES, " 3,1,3 "}};
    options = ParseEgmStorePoolOptions(explicit_nodes);
    ASSERT_TRUE(options);
    EXPECT_TRUE(options->enabled);
    EXPECT_FALSE(options->auto_nodes);
    EXPECT_EQ(options->nodes, (std::vector<int>{1, 3}));

    explicit_nodes[CONFIG_KEY_EGM_NUMA_NODES] = "1,,3";
    EXPECT_FALSE(ParseEgmStorePoolOptions(explicit_nodes));
}

TEST(EgmStorePoolTest, EnabledConfigurationHasOneNarrowContract) {
    EgmStorePoolOptions disabled;
    EXPECT_TRUE(ValidateEgmStorePoolOptions(disabled, "tcp", 0, 1));
    EgmStorePool pool({});
    EXPECT_TRUE(pool.Setup(disabled, "tcp", 0, 1, 0));
    EXPECT_FALSE(pool.hasOwnership());

    EgmStorePoolOptions enabled;
    enabled.enabled = true;
    EXPECT_TRUE(ValidateEgmStorePoolOptions(enabled, "nvlink", 1, 0));
    EXPECT_FALSE(ValidateEgmStorePoolOptions(enabled, "rdma", 1, 0));
    EXPECT_FALSE(ValidateEgmStorePoolOptions(enabled, "nvlink", 0, 0));
    EXPECT_FALSE(ValidateEgmStorePoolOptions(enabled, "nvlink", 1, 1));
}

TEST(EgmStorePoolTest, CleanupPendingPoolRejectsSetupBeforeMutation) {
    auto client = RealClient::create();
    EgmStorePoolTestPeer::MarkCleanupPending(*client);

    ConfigDict enabled{{CONFIG_KEY_LOCAL_HOSTNAME, "provider:12345"},
                       {CONFIG_KEY_METADATA_SERVER, "unused"},
                       {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "16777216"},
                       {CONFIG_KEY_LOCAL_BUFFER_SIZE, "0"},
                       {CONFIG_KEY_PROTOCOL, "nvlink"},
                       {CONFIG_KEY_IPC_SOCKET_PATH, "enabled-retry"},
                       {CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"}};
    auto enabled_retry = client->setup_internal(enabled);
    ASSERT_FALSE(enabled_retry);
    EXPECT_EQ(enabled_retry.error(), ErrorCode::INVALID_PARAMS);

    ConfigDict disabled{{CONFIG_KEY_LOCAL_HOSTNAME, "consumer:12345"},
                        {CONFIG_KEY_METADATA_SERVER, "unused"},
                        {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "0"},
                        {CONFIG_KEY_LOCAL_BUFFER_SIZE, "16777216"},
                        {CONFIG_KEY_PROTOCOL, "tcp"},
                        {CONFIG_KEY_IPC_SOCKET_PATH, "disabled-retry"}};
    auto disabled_retry = client->setup_internal(disabled);
    ASSERT_FALSE(disabled_retry);
    EXPECT_EQ(disabled_retry.error(), ErrorCode::INVALID_PARAMS);

    EXPECT_TRUE(EgmStorePoolTestPeer::HasPool(*client));
    EXPECT_EQ(EgmStorePoolTestPeer::IpcSocketPath(*client), "cleanup-pending");
    EXPECT_EQ(client->tearDownAll(), 0);
}

TEST(EgmStorePoolTest, PlannerAlignsDownDistributesRemainderAndChunks) {
    auto plan = PlanEgmStorePool(5 * 24 + 7, {{4, 12}, {0, 8}}, 2 * 24, 4);
    ASSERT_TRUE(plan);
    EXPECT_EQ(plan->alignment, 24);
    EXPECT_EQ(plan->effective_bytes, 5 * 24);
    ASSERT_EQ(plan->nodes.size(), 2);
    EXPECT_EQ(plan->nodes[0].node, 0);
    EXPECT_EQ(plan->nodes[0].bytes, 3 * 24);
    EXPECT_EQ(plan->nodes[1].node, 4);
    EXPECT_EQ(plan->nodes[1].bytes, 2 * 24);
    ASSERT_EQ(plan->chunks.size(), 3);
    for (const auto& chunk : plan->chunks) {
        EXPECT_LE(chunk.bytes, 2 * 24);
        EXPECT_EQ(chunk.bytes % plan->alignment, 0);
    }
    EXPECT_FALSE(PlanEgmStorePool(24, {{0, 8}}, 7, 4));
}

class FakeAllocation final : public EgmStorePoolAllocation {
   public:
    FakeAllocation(void* base, size_t length,
                   std::function<EgmStorePoolResult<void>()> release)
        : base_(base), length_(length), release_(std::move(release)) {}

    void* base() const override { return base_; }
    size_t length() const override { return length_; }
    EgmStorePoolResult<void> Release() override { return release_(); }

   private:
    void* base_;
    size_t length_;
    std::function<EgmStorePoolResult<void>()> release_;
};

struct FakeOperations {
    EgmStorePoolHooks Hooks() {
        EgmStorePoolHooks hooks;
        hooks.discover_nodes = [this] {
            events.push_back("discover");
            return EgmStorePoolResult<std::vector<int>>(
                std::vector<int>{3, 1, 3});
        };
        hooks.get_granularity = [](int) {
            return EgmStorePoolResult<size_t>(16);
        };
        hooks.allocate = [this](int, size_t length, size_t) {
            const int index = allocate_calls++;
            events.push_back("a" + std::to_string(index));
            if (fail_allocate == index) {
                return EgmStorePoolAllocationAttempt{nullptr, "allocate"};
            }
            void* base = reinterpret_cast<void*>(
                static_cast<uintptr_t>(0x1000 + index * 0x100));
            auto release = [this, index]() -> EgmStorePoolResult<void> {
                events.push_back("r" + std::to_string(index));
                if (fail_release == index) {
                    fail_release = -1;
                    return Fail<void>("release");
                }
                return {};
            };
            EgmStorePoolAllocationAttempt attempt;
            attempt.allocation =
                std::make_unique<FakeAllocation>(base, length, release);
            if (error_with_owner == index) attempt.error = "create";
            return attempt;
        };
        hooks.mount = [this](const UUID& id, void* base, size_t) {
            const int index =
                (reinterpret_cast<uintptr_t>(base) - 0x1000) / 0x100;
            events.push_back("m" + std::to_string(index));
            mounted_ids[id] = index;
            if (fail_mount == index) return Fail<void>("mount");
            return EgmStorePoolResult<void>();
        };
        hooks.unmount = [this](const UUID& id) {
            const int index = mounted_ids.at(id);
            events.push_back("u" + std::to_string(index));
            if (fail_unmount == index) {
                fail_unmount = -1;
                return Fail<void>("unmount");
            }
            return EgmStorePoolResult<void>();
        };
        return hooks;
    }

    int allocate_calls = 0;
    int fail_allocate = -1;
    int error_with_owner = -1;
    int fail_mount = -1;
    int fail_unmount = -1;
    int fail_release = -1;
    std::vector<std::string> events;
    std::map<UUID, int> mounted_ids;
};

EgmStorePoolOptions AutoOptions() {
    EgmStorePoolOptions options;
    options.enabled = true;
    return options;
}

TEST(EgmStorePoolTest, AutoNodesAllocateAllBeforeMountAndTeardownInReverse) {
    FakeOperations operations;
    EgmStorePool pool(operations.Hooks());
    ASSERT_TRUE(pool.Setup(AutoOptions(), "nvlink", 64, 0, 32, 16));
    EXPECT_EQ(pool.plan().nodes[0].node, 1);
    EXPECT_EQ(pool.plan().nodes[1].node, 3);
    EXPECT_EQ(operations.events,
              (std::vector<std::string>{"discover", "a0", "a1", "m0", "m1"}));

    ASSERT_TRUE(pool.Teardown());
    EXPECT_EQ(std::vector<std::string>(operations.events.end() - 4,
                                       operations.events.end()),
              (std::vector<std::string>{"u1", "u0", "r1", "r0"}));
}

TEST(EgmStorePoolTest, ExplicitNodesSkipDiscoveryAndAreSorted) {
    FakeOperations operations;
    EgmStorePool pool(operations.Hooks());
    EgmStorePoolOptions options;
    options.enabled = true;
    options.auto_nodes = false;
    options.nodes = {3, 1, 3};

    ASSERT_TRUE(pool.Setup(options, "nvlink", 64, 0, 32, 16));
    ASSERT_EQ(pool.plan().nodes.size(), 2);
    EXPECT_EQ(pool.plan().nodes[0].node, 1);
    EXPECT_EQ(pool.plan().nodes[1].node, 3);
    EXPECT_EQ(operations.events.front(), "a0");
    EXPECT_TRUE(pool.Teardown());
}

TEST(EgmStorePoolTest, SetupFailuresRollBackEveryCompletedStage) {
    FakeOperations allocation_failure;
    allocation_failure.fail_allocate = 1;
    EgmStorePool allocation_pool(allocation_failure.Hooks());
    EXPECT_FALSE(allocation_pool.Setup(AutoOptions(), "nvlink", 64, 0, 32, 16));
    EXPECT_EQ(allocation_failure.events,
              (std::vector<std::string>{"discover", "a0", "a1", "r0"}));
    EXPECT_FALSE(allocation_pool.hasOwnership());

    FakeOperations mount_failure;
    mount_failure.fail_mount = 1;
    EgmStorePool mount_pool(mount_failure.Hooks());
    EXPECT_FALSE(mount_pool.Setup(AutoOptions(), "nvlink", 64, 0, 32, 16));
    EXPECT_EQ(std::vector<std::string>(mount_failure.events.end() - 4,
                                       mount_failure.events.end()),
              (std::vector<std::string>{"u1", "u0", "r1", "r0"}));
    EXPECT_FALSE(mount_pool.hasOwnership());
}

TEST(EgmStorePoolTest, PartialCleanupRetryDoesNotRepeatCompletedWork) {
    FakeOperations operations;
    operations.fail_unmount = 1;
    EgmStorePool pool(operations.Hooks());
    ASSERT_TRUE(pool.Setup(AutoOptions(), "nvlink", 64, 0, 32, 16));
    EXPECT_FALSE(pool.Teardown());
    EXPECT_TRUE(pool.hasOwnership());

    const size_t first_attempt = operations.events.size();
    ASSERT_TRUE(pool.Teardown());
    EXPECT_EQ(
        std::vector<std::string>(operations.events.begin() + first_attempt,
                                 operations.events.end()),
        (std::vector<std::string>{"u1", "r1", "r0"}));

    FakeOperations release_failure;
    release_failure.fail_release = 0;
    EgmStorePool release_pool(release_failure.Hooks());
    ASSERT_TRUE(release_pool.Setup(AutoOptions(), "nvlink", 64, 0, 32, 16));
    EXPECT_FALSE(release_pool.Teardown());
    const size_t retry = release_failure.events.size();
    ASSERT_TRUE(release_pool.Teardown());
    EXPECT_EQ(std::vector<std::string>(release_failure.events.begin() + retry,
                                       release_failure.events.end()),
              (std::vector<std::string>{"r0"}));
}

TEST(EgmStorePoolTest, IncompleteSetupRollbackRetainsOnlyPendingOwner) {
    FakeOperations operations;
    operations.fail_mount = 1;
    operations.fail_unmount = 1;
    EgmStorePool pool(operations.Hooks());
    EXPECT_FALSE(pool.Setup(AutoOptions(), "nvlink", 64, 0, 32, 16));
    EXPECT_TRUE(pool.hasOwnership());

    const size_t retry = operations.events.size();
    ASSERT_TRUE(pool.Teardown());
    EXPECT_EQ(std::vector<std::string>(operations.events.begin() + retry,
                                       operations.events.end()),
              (std::vector<std::string>{"u1", "r1", "r0"}));
}

TEST(EgmStorePoolTest, CreateErrorRetainsPartialOwnerUntilReleaseCanRetry) {
    FakeOperations operations;
    operations.error_with_owner = 1;
    operations.fail_release = 1;
    EgmStorePool pool(operations.Hooks());

    EXPECT_FALSE(pool.Setup(AutoOptions(), "nvlink", 64, 0, 32, 16));
    EXPECT_TRUE(pool.hasOwnership());
    EXPECT_EQ(std::vector<std::string>(operations.events.end() - 2,
                                       operations.events.end()),
              (std::vector<std::string>{"r1", "r0"}));

    const size_t retry = operations.events.size();
    ASSERT_TRUE(pool.Teardown());
    EXPECT_EQ(std::vector<std::string>(operations.events.begin() + retry,
                                       operations.events.end()),
              (std::vector<std::string>{"r1"}));
}

}  // namespace
}  // namespace mooncake
