#include <gtest/gtest.h>

#include <limits>
#include <map>
#include <set>

#include "egm_store_pool.h"

namespace mooncake {
namespace {

class FakeEgmStorePoolEnvironment final : public EgmStorePoolEnvironment {
   public:
    EgmStorePoolResult<std::vector<int>> VisibleCudaDevices() const override {
        ++visible_calls;
        if (!visible_error.empty()) return tl::make_unexpected(visible_error);
        return visible_devices;
    }

    EgmStorePoolResult<std::string> PciBdfForCudaDevice(
        int device_id) const override {
        ++pci_calls;
        auto it = device_pci.find(device_id);
        if (it == device_pci.end()) {
            return tl::make_unexpected("missing fake PCI BDF");
        }
        return it->second;
    }

    EgmStorePoolResult<int> ReadPciNumaNode(
        const std::string& pci_bdf) const override {
        ++sysfs_calls;
        auto it = pci_node.find(pci_bdf);
        if (it == pci_node.end()) {
            return tl::make_unexpected("missing fake sysfs NUMA node");
        }
        return it->second;
    }

    bool IsNumaNodeOnline(int node_id) const override {
        ++online_calls;
        return online_nodes.count(node_id) != 0;
    }

    EgmStorePoolResult<int> CurrentCpu() const override {
        ++cpu_calls;
        if (!cpu_error.empty()) return tl::make_unexpected(cpu_error);
        return current_cpu;
    }

    EgmStorePoolResult<int> NumaNodeForCpu(int cpu_id) const override {
        ++cpu_node_calls;
        if (!cpu_node_error.empty()) return tl::make_unexpected(cpu_node_error);
        auto it = cpu_nodes.find(cpu_id);
        if (it == cpu_nodes.end()) {
            return tl::make_unexpected("missing fake CPU NUMA node");
        }
        return it->second;
    }

    std::vector<int> visible_devices;
    std::string visible_error;
    std::map<int, std::string> device_pci;
    std::map<std::string, int> pci_node;
    std::set<int> online_nodes;
    int current_cpu = 0;
    std::string cpu_error;
    std::map<int, int> cpu_nodes;
    std::string cpu_node_error;

    mutable int visible_calls = 0;
    mutable int pci_calls = 0;
    mutable int sysfs_calls = 0;
    mutable int online_calls = 0;
    mutable int cpu_calls = 0;
    mutable int cpu_node_calls = 0;
};

EgmStorePoolOptions EnabledAutoOptions() {
    EgmStorePoolOptions options;
    options.enabled = true;
    return options;
}

TEST(EgmStorePoolDiscoveryTest, AutoDiscoveryDeduplicatesAndSorts) {
    FakeEgmStorePoolEnvironment environment;
    environment.visible_devices = {2, 0, 1};
    environment.device_pci = {
        {0, "0000:01:00.0"}, {1, "0000:02:00.0"}, {2, "0000:03:00.0"}};
    environment.pci_node = {
        {"0000:01:00.0", 2}, {"0000:02:00.0", 0}, {"0000:03:00.0", 2}};
    environment.online_nodes = {0, 2};

    auto nodes =
        DiscoverEgmStorePoolNodes(EnabledAutoOptions(), 4096, environment);
    ASSERT_TRUE(nodes);
    EXPECT_EQ(*nodes, (std::vector<int>{0, 2}));
    EXPECT_EQ(environment.visible_calls, 1);
    EXPECT_EQ(environment.pci_calls, 3);
    EXPECT_EQ(environment.sysfs_calls, 3);
}

TEST(EgmStorePoolDiscoveryTest, ExplicitNodesBypassGpuDiscovery) {
    FakeEgmStorePoolEnvironment environment;
    environment.online_nodes = {1, 3};
    EgmStorePoolOptions options;
    options.enabled = true;
    options.auto_nodes = false;
    options.nodes = {3, 1, 3};

    auto nodes = DiscoverEgmStorePoolNodes(options, 4096, environment);
    ASSERT_TRUE(nodes);
    EXPECT_EQ(*nodes, (std::vector<int>{1, 3}));
    EXPECT_EQ(environment.visible_calls, 0);
    EXPECT_EQ(environment.pci_calls, 0);
    EXPECT_EQ(environment.sysfs_calls, 0);
}

TEST(EgmStorePoolDiscoveryTest, RejectsDiscoveryFailures) {
    FakeEgmStorePoolEnvironment no_devices;
    EXPECT_FALSE(
        DiscoverEgmStorePoolNodes(EnabledAutoOptions(), 4096, no_devices));

    FakeEgmStorePoolEnvironment missing_sysfs;
    missing_sysfs.visible_devices = {0};
    missing_sysfs.device_pci = {{0, "0000:01:00.0"}};
    EXPECT_FALSE(
        DiscoverEgmStorePoolNodes(EnabledAutoOptions(), 4096, missing_sysfs));

    FakeEgmStorePoolEnvironment negative;
    negative.visible_devices = {0};
    negative.device_pci = {{0, "0000:01:00.0"}};
    negative.pci_node = {{"0000:01:00.0", -1}};
    EXPECT_FALSE(
        DiscoverEgmStorePoolNodes(EnabledAutoOptions(), 4096, negative));

    FakeEgmStorePoolEnvironment offline;
    offline.visible_devices = {0};
    offline.device_pci = {{0, "0000:01:00.0"}};
    offline.pci_node = {{"0000:01:00.0", 4}};
    EXPECT_FALSE(
        DiscoverEgmStorePoolNodes(EnabledAutoOptions(), 4096, offline));
}

TEST(EgmStorePoolDiscoveryTest, RejectsInvalidExplicitNodes) {
    FakeEgmStorePoolEnvironment environment;
    environment.online_nodes = {0};
    EgmStorePoolOptions options;
    options.enabled = true;
    options.auto_nodes = false;
    options.nodes = {0, 1};
    EXPECT_FALSE(DiscoverEgmStorePoolNodes(options, 4096, environment));

    options.nodes.clear();
    EXPECT_FALSE(DiscoverEgmStorePoolNodes(options, 4096, environment));
}

TEST(EgmStorePoolDiscoveryTest, ZeroOrDisabledGlobalSkipsDiscovery) {
    FakeEgmStorePoolEnvironment environment;
    auto enabled_zero =
        DiscoverEgmStorePoolNodes(EnabledAutoOptions(), 0, environment);
    ASSERT_TRUE(enabled_zero);
    EXPECT_TRUE(enabled_zero->empty());

    EgmStorePoolOptions disabled;
    auto disabled_nonzero =
        DiscoverEgmStorePoolNodes(disabled, 4096, environment);
    ASSERT_TRUE(disabled_nonzero);
    EXPECT_TRUE(disabled_nonzero->empty());
    EXPECT_EQ(environment.visible_calls, 0);
}

TEST(EgmStorePoolLocalNodeTest, UsesSetupCpuLocality) {
    FakeEgmStorePoolEnvironment environment;
    environment.current_cpu = 17;
    environment.cpu_nodes = {{17, 3}};
    environment.online_nodes = {3};

    auto node =
        ResolveEgmStorePoolLocalNode(EnabledAutoOptions(), 4096, environment);
    ASSERT_TRUE(node);
    ASSERT_TRUE(node->has_value());
    EXPECT_EQ(**node, 3);
    EXPECT_EQ(environment.cpu_calls, 1);
    EXPECT_EQ(environment.cpu_node_calls, 1);
}

TEST(EgmStorePoolLocalNodeTest, RejectsCpuAndNodeFailures) {
    FakeEgmStorePoolEnvironment cpu_failure;
    cpu_failure.cpu_error = "injected sched_getcpu failure";
    EXPECT_FALSE(
        ResolveEgmStorePoolLocalNode(EnabledAutoOptions(), 4096, cpu_failure));

    FakeEgmStorePoolEnvironment node_failure;
    node_failure.current_cpu = 4;
    node_failure.cpu_node_error = "injected numa_node_of_cpu failure";
    EXPECT_FALSE(
        ResolveEgmStorePoolLocalNode(EnabledAutoOptions(), 4096, node_failure));

    FakeEgmStorePoolEnvironment offline;
    offline.current_cpu = 4;
    offline.cpu_nodes = {{4, 1}};
    EXPECT_FALSE(
        ResolveEgmStorePoolLocalNode(EnabledAutoOptions(), 4096, offline));
}

TEST(EgmStorePoolLocalNodeTest, ZeroOrDisabledLocalSkipsDiscovery) {
    FakeEgmStorePoolEnvironment environment;
    auto enabled_zero =
        ResolveEgmStorePoolLocalNode(EnabledAutoOptions(), 0, environment);
    ASSERT_TRUE(enabled_zero);
    EXPECT_FALSE(enabled_zero->has_value());

    EgmStorePoolOptions disabled;
    auto disabled_nonzero =
        ResolveEgmStorePoolLocalNode(disabled, 4096, environment);
    ASSERT_TRUE(disabled_nonzero);
    EXPECT_FALSE(disabled_nonzero->has_value());
    EXPECT_EQ(environment.cpu_calls, 0);
}

TEST(EgmStorePoolPlannerTest, FloorsAndBalancesInStableNodeOrder) {
    auto plan =
        PlanEgmStorePoolCapacity(11 * 12 + 7, {{4, 3}, {0, 4}}, 10 * 12, 4);
    ASSERT_TRUE(plan);
    EXPECT_EQ(plan->common_alignment, 12u);
    EXPECT_EQ(plan->requested_total, 139u);
    EXPECT_EQ(plan->effective_total, 132u);
    ASSERT_EQ(plan->nodes.size(), 2u);
    EXPECT_EQ(plan->nodes[0].node_id, 0);
    EXPECT_EQ(plan->nodes[0].effective_bytes, 6 * 12u);
    EXPECT_EQ(plan->nodes[1].node_id, 4);
    EXPECT_EQ(plan->nodes[1].effective_bytes, 5 * 12u);
}

TEST(EgmStorePoolPlannerTest, SplitsEveryNodeAtCommonAlignedMaxMr) {
    auto plan =
        PlanEgmStorePoolCapacity(16 * 12, {{0, 3}, {1, 4}}, 5 * 12 + 11, 4);
    ASSERT_TRUE(plan);
    ASSERT_EQ(plan->chunks.size(), 4u);
    EXPECT_EQ(plan->chunks[0].node_id, 0);
    EXPECT_EQ(plan->chunks[0].chunk_bytes, 5 * 12u);
    EXPECT_EQ(plan->chunks[1].node_id, 0);
    EXPECT_EQ(plan->chunks[1].chunk_bytes, 3 * 12u);
    EXPECT_EQ(plan->chunks[2].node_id, 1);
    EXPECT_EQ(plan->chunks[2].chunk_bytes, 5 * 12u);
    EXPECT_EQ(plan->chunks[3].node_id, 1);
    EXPECT_EQ(plan->chunks[3].chunk_bytes, 3 * 12u);
    for (size_t index = 0; index < plan->chunks.size(); ++index) {
        EXPECT_EQ(plan->chunks[index].plan_index, index);
        EXPECT_EQ(plan->chunks[index].chunk_bytes % plan->common_alignment, 0u);
    }
}

TEST(EgmStorePoolPlannerTest, HandlesMaxMrBoundaries) {
    EXPECT_FALSE(PlanEgmStorePoolCapacity(24, {{0, 3}}, 11, 4));

    auto equal = PlanEgmStorePoolCapacity(24, {{0, 3}}, 12, 4);
    ASSERT_TRUE(equal);
    EXPECT_EQ(equal->chunks.size(), 2u);

    auto above = PlanEgmStorePoolCapacity(24, {{0, 3}}, 23, 4);
    ASSERT_TRUE(above);
    EXPECT_EQ(above->chunks.size(), 2u);
    EXPECT_EQ(above->chunks[0].chunk_bytes, 12u);
}

TEST(EgmStorePoolPlannerTest, RejectsInsufficientOrInvalidInputs) {
    EXPECT_FALSE(PlanEgmStorePoolCapacity(0, {{0, 4}}, 4, 4));
    EXPECT_FALSE(PlanEgmStorePoolCapacity(16, {}, 16, 4));
    EXPECT_FALSE(PlanEgmStorePoolCapacity(4, {{0, 4}, {1, 4}}, 4, 4));
    EXPECT_FALSE(PlanEgmStorePoolCapacity(16, {{-1, 4}}, 16, 4));
    EXPECT_FALSE(PlanEgmStorePoolCapacity(16, {{0, 0}}, 16, 4));
    EXPECT_FALSE(PlanEgmStorePoolCapacity(16, {{0, 4}, {0, 4}}, 16, 4));
}

TEST(EgmStorePoolPlannerTest, RejectsCommonAlignmentOverflow) {
    const size_t largest = std::numeric_limits<size_t>::max();
    EXPECT_FALSE(PlanEgmStorePoolCapacity(largest, {{0, 2}}, largest, largest));
}

TEST(EgmStorePoolPlannerTest, UsesCachelibSlabAlignmentByDefault) {
    auto plan =
        PlanEgmStorePoolCapacity(facebook::cachelib::Slab::kSize * 2,
                                 {{0, 4096}}, facebook::cachelib::Slab::kSize);
    ASSERT_TRUE(plan);
    EXPECT_EQ(plan->common_alignment, facebook::cachelib::Slab::kSize);
    ASSERT_EQ(plan->chunks.size(), 2u);
    EXPECT_EQ(plan->chunks[0].chunk_bytes, facebook::cachelib::Slab::kSize);
}

}  // namespace
}  // namespace mooncake
