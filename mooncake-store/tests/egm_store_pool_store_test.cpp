// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <algorithm>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <future>
#include <glob.h>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <unistd.h>

#include "config.h"
#include "egm_store_pool.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "transport/nvlink_transport/nvlink_transport.h"

namespace mooncake {

class EgmStorePoolStoreTestPeer {
   public:
    using MasterMountFailureHook =
        std::function<std::optional<ErrorCode>(const Segment&)>;

    static void SetMasterMountFailureHook(RealClient& client,
                                          MasterMountFailureHook hook) {
        client.egm_store_pool_master_mount_failure_for_test_ = std::move(hook);
    }

    static void ClearMasterMountFailureHook(RealClient& client) {
        client.egm_store_pool_master_mount_failure_for_test_ = {};
    }

    static void MarkAllocatorCleanupPending(RealClient& client) {
        client.egm_store_pool_allocator_installed_ = true;
    }

    static tl::expected<void, ErrorCode> ReleaseAllocatorView(
        RealClient& client,
        const std::function<void()>& after_exchange_for_test = {}) {
        return client.ReleaseEgmStorePoolAllocatorView(after_exchange_for_test);
    }

    static void PublishAllocatorView(
        RealClient& client, std::shared_ptr<ClientBufferAllocator> allocator) {
        client.PublishClientBufferAllocator(std::move(allocator));
    }

    static std::shared_ptr<ClientBufferAllocator> SnapshotAllocatorView(
        const RealClient& client) {
        return client.SnapshotClientBufferAllocator();
    }

    static std::optional<BufferHandle> AllocateFromAllocatorView(
        RealClient& client, size_t size) {
        return client.AllocateClientBuffer(size);
    }

    static bool HasLocalTeBuffer(Client& client, uintptr_t base) {
        auto metadata = client.transfer_engine_->getMetadata();
        if (metadata == nullptr) return false;
        auto descriptor = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID, false);
        if (descriptor == nullptr) return false;
        return std::any_of(descriptor->buffers.begin(),
                           descriptor->buffers.end(),
                           [base](const TransferMetadata::BufferDesc& buffer) {
                               return buffer.addr == base;
                           });
    }
};

namespace {

using ::mooncake::InProcMasterConfigBuilder;
using testing::InProcMaster;

ConfigDict MinimalConfig(const std::string& protocol) {
    return {{CONFIG_KEY_LOCAL_HOSTNAME, "localhost:17991"},
            {CONFIG_KEY_METADATA_SERVER, P2PHANDSHAKE},
            {CONFIG_KEY_MASTER_SERVER_ADDR, "127.0.0.1:1"},
            {CONFIG_KEY_PROTOCOL, protocol},
            {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "0"},
            {CONFIG_KEY_LOCAL_BUFFER_SIZE, "0"}};
}

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        if (const char* previous = std::getenv(name); previous != nullptr) {
            previous_ = previous;
        }
        if (value != nullptr) {
            (void)::setenv(name_.c_str(), value, 1);
        } else {
            (void)::unsetenv(name_.c_str());
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

    ~ScopedEnvVar() {
        if (previous_) {
            (void)::setenv(name_.c_str(), previous_->c_str(), 1);
        } else {
            (void)::unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> previous_;
};

class ScopedMaxMrSize {
   public:
    explicit ScopedMaxMrSize(size_t value)
        : previous_(globalConfig().max_mr_size) {
        globalConfig().max_mr_size = static_cast<uint64_t>(value);
    }

    ScopedMaxMrSize(const ScopedMaxMrSize&) = delete;
    ScopedMaxMrSize& operator=(const ScopedMaxMrSize&) = delete;

    ~ScopedMaxMrSize() { globalConfig().max_mr_size = previous_; }

   private:
    uint64_t previous_;
};

struct StoreHardwareContext {
    EgmStorePoolPlan plan;
    size_t local_buffer_size = DEFAULT_LOCAL_BUFFER_SIZE;
};

class FakeQuarantineAllocation final : public EgmStorePoolAllocation {
   public:
    FakeQuarantineAllocation(void* base, size_t length)
        : base_(base), length_(length) {}

    void* base() const override { return base_; }
    size_t length() const override { return length_; }
    size_t granularity() const override { return 4096; }

   private:
    void* base_;
    size_t length_;
};

tl::expected<bool, std::string> StrictFabricRequired() {
    const char* value = std::getenv("MC_REQUIRE_MNNVL_FABRIC");
    if (value == nullptr || std::string_view(value) == "0") return false;
    if (std::string_view(value) == "1") return true;
    return tl::make_unexpected("MC_REQUIRE_MNNVL_FABRIC must be either 0 or 1");
}

#define REQUIRE_FABRIC_OR_SKIP(strict, reason) \
    do {                                       \
        if (strict) {                          \
            FAIL() << (reason);                \
        } else {                               \
            GTEST_SKIP() << (reason);          \
        }                                      \
    } while (false)

#if defined(USE_MNNVL) && defined(USE_CUDA)
bool HasAccessibleImexChannel(std::string& error) {
    glob_t channels{};
    const int result = ::glob("/dev/nvidia-caps-imex-channels/channel*", 0,
                              nullptr, &channels);
    if (result != 0) {
        if (result == GLOB_NOMATCH) {
            error =
                "no /dev/nvidia-caps-imex-channels/channel* device is "
                "present";
        } else {
            error = "failed to enumerate NVIDIA IMEX channel devices";
        }
        return false;
    }

    bool accessible = false;
    for (size_t index = 0; index < channels.gl_pathc; ++index) {
        if (::access(channels.gl_pathv[index], R_OK | W_OK) == 0) {
            accessible = true;
            break;
        }
    }
    ::globfree(&channels);
    if (!accessible) {
        error =
            "the launching user cannot read and write any NVIDIA IMEX "
            "channel device";
    }
    return accessible;
}
#endif

tl::expected<StoreHardwareContext, std::string> PrepareStoreHardware() {
#if !defined(USE_MNNVL) || !defined(USE_CUDA)
    return tl::make_unexpected(
        "Store hardware acceptance requires a USE_MNNVL+USE_CUDA build");
#else
    if (std::getenv("MC_USE_NVLINK_IPC") != nullptr) {
        return tl::make_unexpected(
            "MC_USE_NVLINK_IPC is set; legacy IPC mode disables Fabric "
            "allocation handles");
    }
    for (const char* incompatible_env :
         {"MC_USE_TENT", "MC_USE_TEV1", "MC_FORCE_TCP",
          "MC_INTRANODE_NVLINK"}) {
        if (std::getenv(incompatible_env) != nullptr) {
            return tl::make_unexpected(
                std::string(incompatible_env) +
                " is set; EGM Store Pool V1 requires the legacy cross-node "
                "NvlinkTransport as the only transport");
        }
    }

    std::string imex_error;
    if (!HasAccessibleImexChannel(imex_error)) {
        return tl::make_unexpected(std::move(imex_error));
    }

    Status capability = NvlinkVmmAllocation::CheckStrictFabricCapability();
    if (!capability.ok()) {
        return tl::make_unexpected(
            "CUDA Fabric capability prerequisite failed: " +
            capability.ToString());
    }

    EgmStorePoolOptions options;
    options.enabled = true;
    options.auto_nodes = true;
    auto environment = CreateProductionEgmStorePoolEnvironment();
    auto nodes = DiscoverEgmStorePoolNodes(options, 1, *environment);
    if (!nodes) {
        return tl::make_unexpected(
            "visible GPU PCI-to-NUMA discovery prerequisite failed: " +
            nodes.error());
    }
    if (nodes->empty()) {
        return tl::make_unexpected(
            "visible GPU PCI-to-NUMA discovery selected no online node");
    }

    auto local_node = ResolveEgmStorePoolLocalNode(
        options, DEFAULT_LOCAL_BUFFER_SIZE, *environment);
    if (!local_node || !local_node->has_value()) {
        return tl::make_unexpected(
            "setup CPU NUMA locality prerequisite failed: " +
            (local_node ? std::string("no local NUMA node")
                        : local_node.error()));
    }

    std::vector<std::pair<int, size_t> > granularities;
    granularities.reserve(nodes->size());
    for (int node : *nodes) {
        size_t granularity = 0;
        Status status = NvlinkVmmAllocation::GetAllocationGranularity(
            NvlinkVmmAllocation::LocationType::HOST_NUMA, node, true,
            granularity);
        if (!status.ok()) {
            return tl::make_unexpected(
                "HOST_NUMA Fabric granularity prerequisite failed for node " +
                std::to_string(node) + ": " + status.ToString());
        }
        granularities.emplace_back(node, granularity);
    }

    const uint64_t configured_max_mr = globalConfig().max_mr_size;
    const size_t max_mr_size = static_cast<size_t>(std::min<uint64_t>(
        configured_max_mr, std::numeric_limits<size_t>::max()));
    size_t per_node_bytes = DEFAULT_GLOBAL_SEGMENT_SIZE;
    std::optional<EgmStorePoolPlan> selected_plan;
    std::string plan_error;
    for (int attempt = 0; attempt != 7; ++attempt) {
        if (nodes->size() >
            std::numeric_limits<size_t>::max() / per_node_bytes) {
            return tl::make_unexpected(
                "hardware test capacity overflows size_t");
        }
        const size_t requested = nodes->size() * per_node_bytes;
        auto plan =
            PlanEgmStorePoolCapacity(requested, granularities, max_mr_size);
        if (plan) {
            selected_plan = std::move(*plan);
            break;
        }
        plan_error = plan.error();
        if (per_node_bytes > std::numeric_limits<size_t>::max() / 2) break;
        per_node_bytes *= 2;
    }
    if (!selected_plan) {
        return tl::make_unexpected(
            "HOST_NUMA capacity planning prerequisite failed: " + plan_error);
    }

    // Allocation validates that the selected HOST_NUMA location, allocation
    // granularity, and Fabric VMM path are usable before starting Store RPCs.
    NvlinkVmmAllocation::Options allocation_options;
    allocation_options.location_type =
        NvlinkVmmAllocation::LocationType::HOST_NUMA;
    allocation_options.location_id = selected_plan->nodes.front().node_id;
    allocation_options.requested_length = selected_plan->common_alignment;
    allocation_options.fabric_exportable = true;
    allocation_options.required_va_alignment = selected_plan->common_alignment;
    std::unique_ptr<NvlinkVmmAllocation> allocation;
    Status allocation_status =
        NvlinkVmmAllocation::Create(allocation_options, allocation);
    if (!allocation_status.ok() || allocation == nullptr) {
        return tl::make_unexpected(
            "HOST_NUMA Fabric VMM allocation prerequisite failed: " +
            (allocation_status.ok() ? std::string("null allocation")
                                    : allocation_status.ToString()));
    }

    StoreHardwareContext context;
    context.plan = std::move(*selected_plan);
    return context;
#endif
}

ConfigDict HardwareConfig(const InProcMaster& master,
                          const StoreHardwareContext& context,
                          const std::string& hostname) {
    return {{CONFIG_KEY_LOCAL_HOSTNAME, hostname},
            {CONFIG_KEY_METADATA_SERVER, P2PHANDSHAKE},
            {CONFIG_KEY_MASTER_SERVER_ADDR, master.master_address()},
            {CONFIG_KEY_PROTOCOL, "nvlink"},
            {CONFIG_KEY_GLOBAL_SEGMENT_SIZE,
             std::to_string(context.plan.requested_total)},
            {CONFIG_KEY_LOCAL_BUFFER_SIZE,
             std::to_string(context.local_buffer_size)},
            {CONFIG_KEY_ENABLE_EGM_STORE_POOL, "true"},
            {CONFIG_KEY_EGM_NUMA_NODES, "auto"}};
}

std::optional<uint64_t> FindMetricValue(const std::string& metrics,
                                        std::string_view name,
                                        std::string_view required_label = {}) {
    size_t line_start = 0;
    while (line_start < metrics.size()) {
        const size_t line_end = metrics.find('\n', line_start);
        const std::string_view line(
            metrics.data() + line_start,
            (line_end == std::string::npos ? metrics.size() : line_end) -
                line_start);
        if (line.starts_with(name) &&
            (line.size() == name.size() || line[name.size()] == '{' ||
             line[name.size()] == ' ') &&
            (required_label.empty() ||
             line.find(required_label) != std::string_view::npos)) {
            const size_t separator = line.rfind(' ');
            if (separator != std::string_view::npos) {
                try {
                    return std::stoull(std::string(line.substr(separator + 1)));
                } catch (...) {
                    return std::nullopt;
                }
            }
        }
        if (line_end == std::string::npos) break;
        line_start = line_end + 1;
    }
    return std::nullopt;
}

tl::expected<std::string, int> WaitForProviderVisibility(
    const InProcMaster& master, const std::string& hostname, bool visible) {
    for (int attempt = 0; attempt != 50; ++attempt) {
        auto segments = testing::GetAllSegments(master);
        if (!segments) return tl::make_unexpected(segments.error());
        const bool found = segments->find(hostname) != std::string::npos;
        if (found == visible) return *segments;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return testing::GetAllSegments(master);
}

TEST(EgmStorePoolStoreTest, EnabledConfigRejectsNonNvlinkBeforeSetup) {
    auto client = RealClient::create();
    ConfigDict config = MinimalConfig("tcp");
    config[CONFIG_KEY_ENABLE_EGM_STORE_POOL] = "true";

    auto result = client->setup_internal(config);
    EXPECT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST(EgmStorePoolStoreTest, FixedArgumentApiHasNoEgmStorePoolSwitch) {
    using FixedInternalSetupSignature =
        tl::expected<void, ErrorCode> (RealClient::*)(
            const std::string&, const std::string&, size_t, size_t,
            const std::string&, const std::string&, const std::string&,
            const std::shared_ptr<TransferEngine>&, const std::string&, int,
            bool, bool, const std::string&, const std::string&, bool, int);
    FixedInternalSetupSignature fixed_internal_setup =
        &RealClient::setup_internal;
    EXPECT_TRUE(fixed_internal_setup != nullptr);

    using FixedSetupSignature = int (RealClient::*)(
        const std::string&, const std::string&, size_t, size_t,
        const std::string&, const std::string&, const std::string&,
        const std::shared_ptr<TransferEngine>&, const std::string&, bool,
        const std::string&, const std::string&, bool, int);
    FixedSetupSignature fixed_setup = &RealClient::setup_real;
    EXPECT_TRUE(fixed_setup != nullptr);
}

TEST(EgmStorePoolStoreTest, EmptyPartialSetupCleanupIsIdempotent) {
    auto client = RealClient::create();
    client->egm_store_pool_enabled_ = true;

    EXPECT_TRUE(client->CleanupEgmStorePool(true));
    EXPECT_TRUE(client->CleanupEgmStorePool(true));
    EXPECT_FALSE(client->egm_store_pool_enabled_);
    EXPECT_TRUE(client->egm_store_pool_globals_.empty());
    EXPECT_FALSE(client->egm_store_pool_local_.has_value());
}

TEST(EgmStorePoolStoreTest,
     OutstandingBufferHandleBlocksAllocatorViewReleaseUntilRetry) {
    auto client = RealClient::create();
    std::vector<std::byte> storage(4096);
    EgmStorePoolStoreTestPeer::PublishAllocatorView(
        *client, ClientBufferAllocator::create(storage.data(), storage.size(),
                                               "nvlink"));
    auto allocation =
        EgmStorePoolStoreTestPeer::AllocateFromAllocatorView(*client, 1024);
    ASSERT_TRUE(allocation.has_value());
    EXPECT_EQ(allocation->ptr(), storage.data());

    auto blocked = EgmStorePoolStoreTestPeer::ReleaseAllocatorView(*client);
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error(), ErrorCode::INTERNAL_ERROR);
    ASSERT_NE(EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client),
              nullptr);
    EXPECT_EQ(allocation->ptr(), storage.data());

    allocation.reset();
    EXPECT_TRUE(EgmStorePoolStoreTestPeer::ReleaseAllocatorView(*client));
    EXPECT_EQ(EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client),
              nullptr);
}

TEST(EgmStorePoolStoreTest,
     CleanupFailureIsDegradedAndClearsAfterExplicitRetry) {
    auto client = RealClient::create();
    std::vector<std::byte> storage(4096);
    EgmStorePoolStoreTestPeer::PublishAllocatorView(
        *client, ClientBufferAllocator::create(storage.data(), storage.size(),
                                               "nvlink"));
    EgmStorePoolStoreTestPeer::MarkAllocatorCleanupPending(*client);
    client->egm_store_pool_enabled_ = true;
    auto allocation =
        EgmStorePoolStoreTestPeer::AllocateFromAllocatorView(*client, 1024);
    ASSERT_TRUE(allocation.has_value());

    EXPECT_FALSE(client->CleanupEgmStorePool(false));
    EXPECT_EQ(client->health_check(), HC_CLEANUP_PENDING);
    EXPECT_TRUE(client->egm_store_pool_enabled_);

    allocation.reset();
    EXPECT_TRUE(client->CleanupEgmStorePool(false));
    EXPECT_EQ(client->health_check(), HC_NOT_INITIALIZED);
    EXPECT_FALSE(client->egm_store_pool_enabled_);
}

TEST(EgmStorePoolStoreTest,
     DestructorQuarantinesOwnershipWhenOwningClientIsUnavailable) {
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT(
        {
            auto client = std::make_unique<RealClient>();
            std::vector<std::byte> storage(4096);
            client->egm_store_pool_enabled_ = true;

            EgmStorePoolGlobalRecord record;
            record.allocation = std::make_unique<FakeQuarantineAllocation>(
                storage.data(), storage.size());
            client->egm_store_pool_globals_.push_back(std::move(record));

            client.reset();
            const auto stats = GetEgmStorePoolProcessQuarantineStats();
            if (stats.clients != 1 || stats.allocations != 1 ||
                stats.bytes != storage.size()) {
                _exit(10);
            }
            RealClient health_probe;
            if (health_probe.health_check() != HC_PROCESS_QUARANTINED) {
                _exit(11);
            }
            _exit(0);
        },
        ::testing::ExitedWithCode(0), "");
}

TEST(EgmStorePoolStoreTest,
     AtomicExchangeRejectsOldLeaseAndHidesOwnerFromNewReaders) {
    auto client = RealClient::create();
    std::vector<std::byte> storage(4096);
    EgmStorePoolStoreTestPeer::PublishAllocatorView(
        *client, ClientBufferAllocator::create(storage.data(), storage.size(),
                                               "nvlink"));

    auto old_lease = EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client);
    ASSERT_NE(old_lease, nullptr);

    std::barrier exchange_reached(2);
    std::barrier new_reader_checked(2);
    auto release_future = std::async(std::launch::async, [&]() {
        return EgmStorePoolStoreTestPeer::ReleaseAllocatorView(*client, [&]() {
            exchange_reached.arrive_and_wait();
            new_reader_checked.arrive_and_wait();
        });
    });

    // Release has atomically removed the published owner but is paused before
    // inspecting the lease count. New readers must not attach to that owner.
    exchange_reached.arrive_and_wait();
    EXPECT_EQ(EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client),
              nullptr);
    EXPECT_FALSE(
        EgmStorePoolStoreTestPeer::AllocateFromAllocatorView(*client, 1024)
            .has_value());
    new_reader_checked.arrive_and_wait();

    auto blocked = release_future.get();
    ASSERT_FALSE(blocked);
    EXPECT_EQ(blocked.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client),
              old_lease);

    old_lease.reset();
    EXPECT_TRUE(EgmStorePoolStoreTestPeer::ReleaseAllocatorView(*client));
    EXPECT_EQ(EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client),
              nullptr);
}

TEST(EgmStorePoolStoreTest,
     CleanupPendingOwnershipRejectsEnabledAndDisabledSetupBeforeMutation) {
    auto client = RealClient::create();

    // This is the state CleanupEgmStorePool deliberately retains when any
    // unmount/unregister step fails. Null allocation records are sufficient for
    // this CPU-only guard test; no CUDA operation is reached.
    client->egm_store_pool_enabled_ = true;
    client->egm_store_pool_globals_.emplace_back();
    client->egm_store_pool_local_.emplace();
    EgmStorePoolStoreTestPeer::PublishAllocatorView(
        *client, ClientBufferAllocator::create(size_t{0}, "nvlink"));
    EgmStorePoolStoreTestPeer::MarkAllocatorCleanupPending(*client);
    client->protocol = "cleanup-pending-protocol";
    client->local_hostname = "cleanup-pending-host";

    auto retained_allocator =
        EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client);
    ConfigDict enabled = MinimalConfig("nvlink");
    enabled[CONFIG_KEY_ENABLE_EGM_STORE_POOL] = "true";
    auto enabled_retry = client->setup_internal(enabled);
    ASSERT_FALSE(enabled_retry);
    EXPECT_EQ(enabled_retry.error(), ErrorCode::INVALID_PARAMS);

    ConfigDict disabled = MinimalConfig("tcp");
    auto disabled_retry = client->setup_internal(disabled);
    ASSERT_FALSE(disabled_retry);
    EXPECT_EQ(disabled_retry.error(), ErrorCode::INVALID_PARAMS);

    EXPECT_EQ(client->client_, nullptr);
    EXPECT_TRUE(client->egm_store_pool_enabled_);
    EXPECT_EQ(client->egm_store_pool_globals_.size(), 1U);
    EXPECT_TRUE(client->egm_store_pool_local_.has_value());
    EXPECT_EQ(EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client),
              retained_allocator);
    EXPECT_EQ(client->protocol, "cleanup-pending-protocol");
    EXPECT_EQ(client->local_hostname, "cleanup-pending-host");

    retained_allocator.reset();
    EXPECT_TRUE(client->CleanupEgmStorePool(true));
}

#if !defined(USE_MNNVL) || !defined(USE_CUDA)
TEST(EgmStorePoolStoreTest, UnsupportedBuildFailsWithoutAllocations) {
    auto client = RealClient::create();
    ConfigDict config = MinimalConfig("nvlink");
    config[CONFIG_KEY_ENABLE_EGM_STORE_POOL] = "true";

    auto result = client->setup_internal(config);
    EXPECT_FALSE(result);
    EXPECT_TRUE(client->egm_store_pool_globals_.empty());
    EXPECT_FALSE(client->egm_store_pool_local_.has_value());
}
#endif

TEST(EgmStorePoolStoreHardwareTest,
     ConfigDictSetupPublishesMetricsAndTearsDown) {
    auto strict = StrictFabricRequired();
    ASSERT_TRUE(strict) << strict.error();
    auto hardware = PrepareStoreHardware();
    if (!hardware) REQUIRE_FABRIC_OR_SKIP(*strict, hardware.error());

    ScopedEnvVar auto_discovery("MC_MS_AUTO_DISC", "0");
    ScopedEnvVar metrics_enabled("MC_STORE_CLIENT_METRIC", "1");
    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));

    const std::string hostname =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = RealClient::create();
    auto setup =
        client->setup_internal(HardwareConfig(master, *hardware, hostname));
    if (!setup) {
        REQUIRE_FABRIC_OR_SKIP(
            *strict,
            "Store ConfigDict setup did not pass the Fabric export/IMEX "
            "prerequisite (error=" +
                toString(setup.error()) + ")");
    }

    ASSERT_NE(client->client_, nullptr);
    ASSERT_TRUE(client->client_->IsNvlinkFabricTransportReady());
    ASSERT_TRUE(client->egm_store_pool_enabled_);
    ASSERT_FALSE(client->egm_store_pool_globals_.empty());
    ASSERT_TRUE(client->egm_store_pool_local_.has_value());
    EXPECT_EQ(client->egm_store_pool_globals_.size(),
              hardware->plan.chunks.size());
    for (const auto& record : client->egm_store_pool_globals_) {
        EXPECT_TRUE(record.mounted_segment_id.has_value());
    }

    auto visible = WaitForProviderVisibility(master, hostname, true);
    ASSERT_TRUE(visible) << "failed to query Master segments";
    ASSERT_NE(visible->find(hostname), std::string::npos);

    auto metrics = client->client_->SerializeMetrics();
    ASSERT_TRUE(metrics) << toString(metrics.error());
    const auto requested = FindMetricValue(
        *metrics, "mooncake_egm_store_pool_requested_capacity_bytes");
    const auto effective = FindMetricValue(
        *metrics, "mooncake_egm_store_pool_effective_capacity_bytes");
    ASSERT_TRUE(requested.has_value());
    ASSERT_TRUE(effective.has_value());
    EXPECT_EQ(*requested, hardware->plan.requested_total);
    EXPECT_EQ(*effective, hardware->plan.effective_total);
    EXPECT_NE(metrics->find("stage=\"preflight\""), std::string::npos);
    EXPECT_NE(metrics->find("stage=\"mount\""), std::string::npos);

    ::testing::Test::RecordProperty(
        "numa_node_count", std::to_string(hardware->plan.nodes.size()));
    ::testing::Test::RecordProperty(
        "provider_chunk_count", std::to_string(hardware->plan.chunks.size()));
    ::testing::Test::RecordProperty(
        "effective_capacity_bytes",
        std::to_string(hardware->plan.effective_total));

    ASSERT_TRUE(client->tearDownAll_internal());
    EXPECT_FALSE(client->egm_store_pool_enabled_);
    EXPECT_TRUE(client->egm_store_pool_globals_.empty());
    EXPECT_FALSE(client->egm_store_pool_local_.has_value());
    EXPECT_FALSE(client->local_buffer_region_.has_value());

    auto absent = WaitForProviderVisibility(master, hostname, false);
    ASSERT_TRUE(absent) << "failed to query Master segments after teardown";
    EXPECT_EQ(absent->find(hostname), std::string::npos);
}

TEST(EgmStorePoolStoreHardwareTest,
     FailNthMountRollsBackMasterTransferEngineAndLocalWorkspace) {
    auto strict = StrictFabricRequired();
    ASSERT_TRUE(strict) << strict.error();
    auto hardware = PrepareStoreHardware();
    if (!hardware) REQUIRE_FABRIC_OR_SKIP(*strict, hardware.error());

    ScopedEnvVar auto_discovery("MC_MS_AUTO_DISC", "0");
    ScopedEnvVar metrics_enabled("MC_STORE_CLIENT_METRIC", "1");
    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));

    const std::string hostname =
        "127.0.0.1:" + std::to_string(getFreeTcpPort());
    auto client = RealClient::create();

    // Force at least two plan chunks even when only one NUMA node is visible:
    // the first mount reaches the real Master and the second fails after its
    // real TE registration. This makes earlier-success rollback substantive on
    // every supported topology.
    ASSERT_LE(hardware->plan.requested_total,
              std::numeric_limits<size_t>::max() / 2);
    StoreHardwareContext failure_context = *hardware;
    failure_context.plan.requested_total *= 2;
    ScopedMaxMrSize max_mr_size(failure_context.plan.common_alignment);
    constexpr size_t fail_nth = 2;

    auto mount_calls = std::make_shared<size_t>(0);
    auto failed_global_base = std::make_shared<void*>(nullptr);
    auto local_base = std::make_shared<void*>(nullptr);
    auto te_descriptor_visible_before_failure = std::make_shared<bool>(false);
    RealClient* client_ptr = client.get();
    EgmStorePoolStoreTestPeer::SetMasterMountFailureHook(
        *client,
        [client_ptr, mount_calls, failed_global_base, local_base,
         te_descriptor_visible_before_failure,
         fail_nth](const Segment& segment) -> std::optional<ErrorCode> {
            ++*mount_calls;
            if (*mount_calls == fail_nth) {
                *failed_global_base = reinterpret_cast<void*>(segment.base);
                if (client_ptr->egm_store_pool_local_) {
                    *local_base =
                        client_ptr->egm_store_pool_local_->allocation->base();
                }
                *te_descriptor_visible_before_failure =
                    EgmStorePoolStoreTestPeer::HasLocalTeBuffer(
                        *client_ptr->client_, segment.base);
                return ErrorCode::INTERNAL_ERROR;
            }
            return std::nullopt;
        });

    auto setup = client->setup_internal(
        HardwareConfig(master, failure_context, hostname));
    if (*mount_calls < fail_nth) {
        REQUIRE_FABRIC_OR_SKIP(
            *strict,
            "Store setup failed before reaching the injected mount, "
            "indicating a "
            "Fabric export/IMEX prerequisite failure (error=" +
                (setup ? std::string("unexpected success")
                       : toString(setup.error())) +
                ")");
    }
    ASSERT_FALSE(setup);
    EXPECT_EQ(setup.error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(*mount_calls, fail_nth);
    EXPECT_TRUE(*te_descriptor_visible_before_failure)
        << "fault injection must run after real TE registration";
    ASSERT_NE(*failed_global_base, nullptr);
    ASSERT_NE(*local_base, nullptr);
    EXPECT_FALSE(client->egm_store_pool_enabled_);
    EXPECT_TRUE(client->egm_store_pool_globals_.empty());
    EXPECT_FALSE(client->egm_store_pool_local_.has_value());
    EXPECT_EQ(EgmStorePoolStoreTestPeer::SnapshotAllocatorView(*client),
              nullptr);
    EXPECT_FALSE(client->local_buffer_region_.has_value());

    auto absent = WaitForProviderVisibility(master, hostname, false);
    ASSERT_TRUE(absent) << "failed to query Master after injected rollback";
    EXPECT_EQ(absent->find(hostname), std::string::npos);

    ASSERT_NE(client->client_, nullptr);
    TransferEngine metadata_observer(false);
    ASSERT_EQ(
        metadata_observer.init(P2PHANDSHAKE, "127.0.0.1:0", "127.0.0.1", 0), 0);
    ASSERT_NE(metadata_observer.installTransport("nvlink", nullptr), nullptr);
    const SegmentID observed_segment =
        metadata_observer.openSegment(client->client_->GetTransportEndpoint());
    ASSERT_NE(observed_segment, static_cast<SegmentID>(-1));
    auto observed_desc = metadata_observer.getMetadata()->getSegmentDescByID(
        observed_segment, false);
    ASSERT_NE(observed_desc, nullptr);
    EXPECT_TRUE(observed_desc->buffers.empty())
        << "rollback must remove every global TE BufferDesc";
    EXPECT_EQ(metadata_observer.closeSegment(observed_segment), 0);

    // These intentionally use the non-idempotent API: success would prove a
    // registration survived rollback. The addresses are ownership tokens only;
    // NvlinkTransport does not dereference them during an absent lookup.
    EXPECT_FALSE(
        client->client_->unregisterLocalMemory(*failed_global_base, true)
            .has_value())
        << "failed current-chunk TE registration survived compensation";
    EXPECT_FALSE(
        client->client_->unregisterLocalMemory(*local_base, false).has_value())
        << "local-only TE registration survived rollback";

    auto metrics = client->client_->SerializeMetrics();
    ASSERT_TRUE(metrics) << toString(metrics.error());
    EXPECT_EQ(
        FindMetricValue(*metrics,
                        "mooncake_egm_store_pool_initialization_failures_total",
                        "stage=\"mount\""),
        1U);
    EXPECT_EQ(FindMetricValue(
                  *metrics, "mooncake_egm_store_pool_rollback_attempts_total"),
              1U);
    const auto rollback_failures = FindMetricValue(
        *metrics, "mooncake_egm_store_pool_rollback_failures_total");
    EXPECT_TRUE(!rollback_failures || *rollback_failures == 0U)
        << "an unchanged zero counter may be omitted from serialization";

    ::testing::Test::RecordProperty("injected_mount_ordinal",
                                    std::to_string(fail_nth));
    ::testing::Test::RecordProperty("rollback_result", "success");
    EgmStorePoolStoreTestPeer::ClearMasterMountFailureHook(*client);
    EXPECT_TRUE(client->tearDownAll_internal());
}

#undef REQUIRE_FABRIC_OR_SKIP

}  // namespace
}  // namespace mooncake
