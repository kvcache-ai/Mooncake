// Copyright 2024 KVCache.AI

#include <gtest/gtest.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "client_internal.h"
#include "egm_store_pool.h"
#include "real_client.h"

namespace mooncake {

struct ManualNvlinkFactoryProbe {
    std::vector<std::string> events;
    AutoDiscoverConfig auto_discover;
    std::string metadata_server;
    std::string local_endpoint;
    std::string hostname;
    std::string installed_protocol;
    uint16_t port = 0;
};

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

    static tl::expected<std::shared_ptr<TransferEngine>, ErrorCode>
    CreateManualNvlinkTransferEngine(ManualNvlinkFactoryProbe& probe,
                                     bool use_tent = false, int init_result = 0,
                                     bool install_result = true) {
        RealClient::TransferEngineSetupOperations operations;
        operations.is_using_tent = [&probe, use_tent](TransferEngine&) {
            probe.events.push_back("tent");
            return use_tent;
        };
        operations.set_auto_discover =
            [&probe](TransferEngine&, const AutoDiscoverConfig& config) {
                probe.events.push_back("discover");
                probe.auto_discover = config;
            };
        operations.init = [&probe, init_result](
                              TransferEngine&, const std::string& metadata,
                              const std::string& local_endpoint,
                              const std::string& hostname, uint16_t port) {
            probe.events.push_back("init");
            probe.metadata_server = metadata;
            probe.local_endpoint = local_endpoint;
            probe.hostname = hostname;
            probe.port = port;
            return init_result;
        };
        operations.install_transport = [&probe, install_result](
                                           TransferEngine&,
                                           const std::string& protocol) {
            probe.events.push_back("install");
            probe.installed_protocol = protocol;
            return install_result;
        };
        return RealClient::CreateManualNvlinkTransferEngine(
            "provider:12345", "metadata", &operations);
    }

    static tl::expected<std::shared_ptr<TransferEngine>, ErrorCode>
    ResolveTransferEngine(
        const std::shared_ptr<TransferEngine>& supplied,
        const std::function<tl::expected<std::shared_ptr<TransferEngine>,
                                         ErrorCode>(const std::string&)>&
            factory,
        const std::string& endpoint) {
        return RealClient::ResolveTransferEngineForSetup(supplied, factory,
                                                         endpoint);
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

TEST(EgmStorePoolTest, ConfigUsesNarrowCommonParserSemantics) {
    for (const auto& value : {"true", " TRUE ", "1"}) {
        ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value}};
        auto options = ParseEgmStorePoolOptions(config);
        ASSERT_TRUE(options) << value;
        EXPECT_TRUE(options->enabled) << value;
    }

    for (const auto& value : {"false", " FALSE ", "0"}) {
        ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value}};
        auto options = ParseEgmStorePoolOptions(config);
        ASSERT_TRUE(options) << value;
        EXPECT_FALSE(options->enabled) << value;
    }

    for (const auto& value : {"yes", "on", "enable"}) {
        ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, value}};
        EXPECT_FALSE(ParseEgmStorePoolOptions(config)) << value;
    }

    ConfigDict config{{CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"},
                      {CONFIG_KEY_EGM_NUMA_NODES, " auto "}};
    auto options = ParseEgmStorePoolOptions(config);
    ASSERT_TRUE(options);
    EXPECT_TRUE(options->auto_nodes);

    for (const auto& value : {"1,,3", "-1", "+1", "1x", "2147483648"}) {
        config[CONFIG_KEY_EGM_NUMA_NODES] = value;
        EXPECT_FALSE(ParseEgmStorePoolOptions(config)) << value;
    }
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

TEST(EgmStorePoolTest, ManualNvlinkFactoryConfiguresOnlyNvlinkInOrder) {
    ManualNvlinkFactoryProbe probe;
    auto result = EgmStorePoolTestPeer::CreateManualNvlinkTransferEngine(probe);
    ASSERT_TRUE(result);
    EXPECT_EQ(probe.events, (std::vector<std::string>{"tent", "discover",
                                                      "init", "install"}));
    EXPECT_FALSE(probe.auto_discover.enabled);
    EXPECT_EQ(probe.auto_discover.protocol, "nvlink");
    EXPECT_EQ(probe.metadata_server, "metadata");
    EXPECT_EQ(probe.local_endpoint, "provider:12345");
    EXPECT_EQ(probe.hostname, "provider");
    EXPECT_EQ(probe.port, 12345);
    EXPECT_EQ(probe.installed_protocol, "nvlink");
}

TEST(EgmStorePoolTest, ManualNvlinkFactoryFailsClosed) {
    ManualNvlinkFactoryProbe tent_probe;
    auto tent = EgmStorePoolTestPeer::CreateManualNvlinkTransferEngine(
        tent_probe, true);
    ASSERT_FALSE(tent);
    EXPECT_EQ(tent.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(tent_probe.events, (std::vector<std::string>{"tent"}));

    ManualNvlinkFactoryProbe init_probe;
    auto init = EgmStorePoolTestPeer::CreateManualNvlinkTransferEngine(
        init_probe, false, -1);
    ASSERT_FALSE(init);
    EXPECT_EQ(init.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(init_probe.events,
              (std::vector<std::string>{"tent", "discover", "init"}));

    ManualNvlinkFactoryProbe install_probe;
    auto install = EgmStorePoolTestPeer::CreateManualNvlinkTransferEngine(
        install_probe, false, 0, false);
    ASSERT_FALSE(install);
    EXPECT_EQ(install.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(
        install_probe.events,
        (std::vector<std::string>{"tent", "discover", "init", "install"}));
}

TEST(EgmStorePoolTest, SuppliedTransferEngineBypassesFactory) {
    auto supplied = std::make_shared<TransferEngine>();
    int factory_calls = 0;
    auto factory = [&factory_calls](const std::string&)
        -> tl::expected<std::shared_ptr<TransferEngine>, ErrorCode> {
        ++factory_calls;
        return std::make_shared<TransferEngine>();
    };
    auto selected = EgmStorePoolTestPeer::ResolveTransferEngine(
        supplied, factory, "provider:12345");
    ASSERT_TRUE(selected);
    EXPECT_EQ(*selected, supplied);
    EXPECT_EQ(factory_calls, 0);

    auto ordinary = EgmStorePoolTestPeer::ResolveTransferEngine(
        nullptr, {}, "provider:12345");
    ASSERT_TRUE(ordinary);
    EXPECT_EQ(*ordinary, nullptr);
}

TEST(EgmStorePoolTest, TransferEngineFactoryReceivesSelectedEndpoint) {
    std::string selected_endpoint;
    auto created = std::make_shared<TransferEngine>();
    auto factory = [&selected_endpoint, created](const std::string& endpoint)
        -> tl::expected<std::shared_ptr<TransferEngine>, ErrorCode> {
        selected_endpoint = endpoint;
        return created;
    };
    auto selected = EgmStorePoolTestPeer::ResolveTransferEngine(
        nullptr, factory, "provider:13001");
    ASSERT_TRUE(selected);
    EXPECT_EQ(*selected, created);
    EXPECT_EQ(selected_endpoint, "provider:13001");

    auto null_factory = [](const std::string&)
        -> tl::expected<std::shared_ptr<TransferEngine>, ErrorCode> {
        return std::shared_ptr<TransferEngine>{};
    };
    auto null_result = EgmStorePoolTestPeer::ResolveTransferEngine(
        nullptr, null_factory, "provider:13002");
    ASSERT_FALSE(null_result);
    EXPECT_EQ(null_result.error(), ErrorCode::INTERNAL_ERROR);
}

class MountTransactionTestClient final : public Client {
   public:
    MountTransactionTestClient()
        : Client("provider:12345", "metadata", "nvlink") {}
};

struct ScriptedMountOperations {
    internal::SegmentMountOperations Hooks() {
        internal::SegmentMountOperations operations;
        operations.register_memory = [this](void*, size_t, const std::string&,
                                            bool, bool) {
            events.push_back("register");
            return register_result;
        };
        operations.mount_master = [this](const Segment&) {
            events.push_back("mount");
            if (mount_error) {
                return tl::expected<void, ErrorCode>(
                    tl::unexpected(*mount_error));
            }
            return tl::expected<void, ErrorCode>();
        };
        operations.unmount_master = [this](const UUID&) {
            events.push_back("unmount");
            if (unmount_failures-- > 0) {
                return tl::expected<void, ErrorCode>(
                    tl::unexpected(ErrorCode::INTERNAL_ERROR));
            }
            return tl::expected<void, ErrorCode>();
        };
        operations.unregister_memory = [this](void*) {
            events.push_back("unregister");
            return unregister_result;
        };
        return operations;
    }

    int register_result = 0;
    int unregister_result = 0;
    int unmount_failures = 0;
    std::optional<ErrorCode> mount_error;
    std::vector<std::string> events;
};

TEST(EgmStorePoolTest, CallerUuidMountUsesSharedTransaction) {
    MountTransactionTestClient client;
    ScriptedMountOperations scripted;
    auto operations = scripted.Hooks();
    const UUID segment_id = generate_uuid();
    const void* buffer = reinterpret_cast<void*>(0x100000);

    auto mount = internal::ClientAccess::MountSegmentWithId(
        client, segment_id, buffer, 4096, "nvlink", kWildcardLocation,
        &operations);
    ASSERT_TRUE(mount);
    EXPECT_EQ(scripted.events, (std::vector<std::string>{"register", "mount"}));

    auto duplicate = internal::ClientAccess::MountSegmentWithId(
        client, segment_id, reinterpret_cast<void*>(0x200000), 4096, "nvlink",
        kWildcardLocation, &operations);
    ASSERT_FALSE(duplicate);
    EXPECT_EQ(duplicate.error(), ErrorCode::SEGMENT_ALREADY_EXISTS);

    auto overlap = internal::ClientAccess::MountSegmentWithId(
        client, generate_uuid(), reinterpret_cast<void*>(0x100800), 4096,
        "nvlink", kWildcardLocation, &operations);
    ASSERT_FALSE(overlap);
    EXPECT_EQ(overlap.error(), ErrorCode::INVALID_PARAMS);

    auto cleanup = internal::ClientAccess::CleanupSegmentByIdIfPresent(
        client, segment_id, &operations);
    ASSERT_TRUE(cleanup);
    EXPECT_EQ(scripted.events,
              (std::vector<std::string>{"register", "mount", "unmount",
                                        "unregister"}));
    EXPECT_TRUE(internal::ClientAccess::CleanupSegmentByIdIfPresent(
        client, segment_id, &operations));
    EXPECT_EQ(scripted.events.size(), 4);
}

TEST(EgmStorePoolTest, MountFailuresReturnOriginalErrorAndRollBack) {
    MountTransactionTestClient client;
    ScriptedMountOperations register_failure;
    register_failure.register_result = -1;
    auto register_operations = register_failure.Hooks();
    auto register_result = internal::ClientAccess::MountSegmentWithId(
        client, generate_uuid(), reinterpret_cast<void*>(0x300000), 4096,
        "nvlink", kWildcardLocation, &register_operations);
    ASSERT_FALSE(register_result);
    EXPECT_EQ(register_result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(register_failure.events,
              (std::vector<std::string>{"register", "unmount", "unregister"}));

    ScriptedMountOperations mount_failure;
    mount_failure.mount_error = ErrorCode::BUFFER_OVERFLOW;
    auto mount_operations = mount_failure.Hooks();
    auto mount_result = internal::ClientAccess::MountSegmentWithId(
        client, generate_uuid(), reinterpret_cast<void*>(0x400000), 4096,
        "nvlink", kWildcardLocation, &mount_operations);
    ASSERT_FALSE(mount_result);
    EXPECT_EQ(mount_result.error(), ErrorCode::BUFFER_OVERFLOW);
    EXPECT_EQ(mount_failure.events,
              (std::vector<std::string>{"register", "mount", "unmount",
                                        "unregister"}));
}

TEST(EgmStorePoolTest, FailedMountRollbackRetainsRecordForRetry) {
    MountTransactionTestClient client;
    ScriptedMountOperations scripted;
    scripted.mount_error = ErrorCode::BUFFER_OVERFLOW;
    scripted.unmount_failures = 1;
    auto operations = scripted.Hooks();
    const UUID segment_id = generate_uuid();

    auto mount = internal::ClientAccess::MountSegmentWithId(
        client, segment_id, reinterpret_cast<void*>(0x500000), 4096, "nvlink",
        kWildcardLocation, &operations);
    ASSERT_FALSE(mount);
    EXPECT_EQ(mount.error(), ErrorCode::BUFFER_OVERFLOW);
    EXPECT_EQ(scripted.events,
              (std::vector<std::string>{"register", "mount", "unmount"}));

    scripted.mount_error.reset();
    auto cleanup = internal::ClientAccess::CleanupSegmentByIdIfPresent(
        client, segment_id, &operations);
    ASSERT_TRUE(cleanup);
    EXPECT_EQ(scripted.events,
              (std::vector<std::string>{"register", "mount", "unmount",
                                        "unmount", "unregister"}));
}

TEST(EgmStorePoolTest, AddressNotRegisteredCompletesCleanup) {
    MountTransactionTestClient client;
    ScriptedMountOperations scripted;
    scripted.unregister_result = ERR_ADDRESS_NOT_REGISTERED;
    auto operations = scripted.Hooks();
    const UUID segment_id = generate_uuid();

    ASSERT_TRUE(internal::ClientAccess::MountSegmentWithId(
        client, segment_id, reinterpret_cast<void*>(0x600000), 4096, "nvlink",
        kWildcardLocation, &operations));
    ASSERT_TRUE(internal::ClientAccess::CleanupSegmentByIdIfPresent(
        client, segment_id, &operations));
    EXPECT_TRUE(internal::ClientAccess::CleanupSegmentByIdIfPresent(
        client, segment_id, &operations));
    EXPECT_EQ(scripted.events,
              (std::vector<std::string>{"register", "mount", "unmount",
                                        "unregister"}));
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
