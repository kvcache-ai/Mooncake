#include "topology.h"

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <cstdlib>
#include <string>
#include <unordered_map>

#include "config.h"
#include "cuda_alike.h"
#include "transfer_metadata.h"
#include "memory_location.h"

TEST(ToplogyTest, GetTopologyMatrix) {
    mooncake::Topology topology;
    topology.discover();
    std::string json_str = topology.toString();
    LOG(INFO) << json_str;
    topology.clear();
    topology.parse(json_str);
    ASSERT_EQ(topology.toString(), json_str);
}

TEST(ToplogyTest, DiscoverWithMcTeFilters) {
    ASSERT_EQ(::setenv("MC_TE_FILTERS", "erdma_0", 1), 0);
    mooncake::Topology topology;
    EXPECT_EQ(topology.discover(), 0);
    ::unsetenv("MC_TE_FILTERS");
}

TEST(ToplogyTest, TestEmpty) {
    mooncake::Topology topology;
    std::string json_str =
        "{\"cpu:0\" : [[\"erdma_0\"],[\"erdma_1\"]],\"cpu:1\" "
        ": [[\"erdma_1\"],[\"erdma_0\"]]}";
    topology.clear();
    topology.parse(json_str);
    ASSERT_TRUE(!topology.empty());
}

TEST(ToplogyTest, TestHcaList) {
    mooncake::Topology topology;
    std::string json_str =
        "{\"cpu:0\" : [[\"erdma_0\"],[\"erdma_0\"]],\"cpu:1\" "
        ": [[\"erdma_0\"],[\"erdma_0\"]]}";
    topology.clear();
    topology.parse(json_str);
    ASSERT_EQ(topology.getHcaList().size(), static_cast<size_t>(1));
    std::set<std::string> HcaList = {"erdma_0"};
    for (auto &hca : topology.getHcaList()) {
        ASSERT_TRUE(HcaList.count(hca));
    }
}

TEST(ToplogyTest, TestHcaListSize) {
    mooncake::Topology topology;
    std::string json_str =
        "{\"cpu:0\" : [[\"erdma_0\"],[\"erdma_1\"]],\"cpu:1\" "
        ": [[\"erdma_2\"],[\"erdma_3\"]]}";
    topology.clear();
    topology.parse(json_str);
    ASSERT_EQ(topology.getHcaList().size(), static_cast<size_t>(4));
}

TEST(ToplogyTest, TestHcaList2) {
    mooncake::Topology topology;
    std::string json_str =
        "{\"cpu:0\" : [[\"erdma_0\"],[\"erdma_1\"]],\"cpu:1\" "
        ": [[\"erdma_1\"],[\"erdma_0\"]]}";
    topology.clear();
    topology.parse(json_str);
    ASSERT_EQ(topology.getHcaList().size(), static_cast<size_t>(2));
    std::set<std::string> HcaList = {"erdma_0", "erdma_1"};
    for (auto &hca : topology.getHcaList()) {
        ASSERT_TRUE(HcaList.count(hca));
    }
}

TEST(ToplogyTest, TestMatrix) {
    mooncake::Topology topology;
    std::string json_str = "{\"cpu:0\" : [[\"erdma_0\"],[\"erdma_1\"]]}";
    topology.clear();
    topology.parse(json_str);
    auto matrix = topology.getMatrix();
    ASSERT_TRUE(matrix.size() == 1);
    ASSERT_TRUE(matrix.count("cpu:0"));
}

TEST(ToplogyTest, TestSelectDevice) {
    mooncake::Topology topology;
    std::string json_str = "{\"cpu:0\" : [[\"erdma_0\"],[\"erdma_1\"]]}";
    topology.clear();
    topology.parse(json_str);
    std::set<int> items = {0, 1};
    int device;
    device = topology.selectDevice("cpu:0", 2);
    ASSERT_TRUE(items.count(device));
    items.erase(device);
    device = topology.selectDevice("cpu:0", 1);
    ASSERT_TRUE(items.count(device));
    items.erase(device);
    ASSERT_TRUE(items.empty());
}

TEST(ToplogyTest, TestSelectDeviceAny) {
    mooncake::Topology topology;
    std::string json_str = "{\"cpu:0\" : [[\"erdma_0\"],[\"erdma_1\"]]}";
    topology.clear();
    topology.parse(json_str);
    std::set<int> items = {0, 1};
    int device;
    device = topology.selectDevice(mooncake::kWildcardLocation, 2);
    ASSERT_TRUE(items.count(device));
    items.erase(device);
    device = topology.selectDevice(mooncake::kWildcardLocation, 1);
    ASSERT_TRUE(items.count(device));
    items.erase(device);
    ASSERT_TRUE(items.empty());
}

TEST(ToplogyTest, TestSelectDeviceEmptyEntry) {
    mooncake::Topology topology;
    std::string json_str = "{\"gpu:0\" : [[],[]]}";
    topology.clear();
    ASSERT_EQ(topology.parse(json_str), 0);

    ASSERT_EQ(topology.selectDevice("gpu:0", 0), ERR_DEVICE_NOT_FOUND);
    ASSERT_EQ(topology.selectDevice("gpu:0", 1), ERR_DEVICE_NOT_FOUND);
}

TEST(ToplogyTest, TestDisableOnlyPreferredDeviceLeavesNoSelection) {
    mooncake::Topology topology;
    std::string json_str = "{\"gpu:0\" : [[\"mlx5_2\"],[]]}";
    topology.clear();
    ASSERT_EQ(topology.parse(json_str), 0);
    ASSERT_EQ(topology.selectDevice("gpu:0", 0), 0);

    ASSERT_EQ(topology.disableDevice("mlx5_2"), 0);
    ASSERT_EQ(topology.selectDevice("gpu:0", 0), ERR_DEVICE_NOT_FOUND);
    ASSERT_EQ(topology.selectDevice("gpu:0", 1), ERR_DEVICE_NOT_FOUND);
}

TEST(ToplogyTest, TestDisableDeviceRemovesLocalHcaAffinityCandidate) {
    mooncake::Topology topology;
    std::string json_str =
        "{\"cpu:0\" : [[\"mlx5_1\"],[\"mlx5_2\"]],"
        "\"cpu:1\" : [[\"mlx5_2\"],[\"mlx5_1\"]]}";
    topology.clear();
    ASSERT_EQ(topology.parse(json_str), 0);

    const auto &hca_list = topology.getHcaList();
    auto disabled_iter = std::find(hca_list.begin(), hca_list.end(), "mlx5_2");
    ASSERT_NE(disabled_iter, hca_list.end());
    const int disabled_index =
        static_cast<int>(std::distance(hca_list.begin(), disabled_iter));

    ASSERT_EQ(topology.disableDevice("mlx5_2"), 0);
    ASSERT_NE(topology.selectDeviceByLocalHca("cpu:0", "mlx5_2", 0),
              disabled_index);
}

// HCA peer affinity must key off GPU_PREFIX (cuda:/hip:/...), not a
// hardcoded "cuda:" string — otherwise USE_HIP builds never resolve affinity
// for discovered hip:N topology entries.
TEST(ToplogyTest, HcaPeerAffinityAppliesToGpuPrefixEntries) {
    auto &cfg = mooncake::globalConfig();
    const bool old_enable = cfg.enable_hca_peer_affinity;
    const auto old_map = cfg.nic_peer_affinity;
    cfg.enable_hca_peer_affinity = true;
    cfg.nic_peer_affinity = {{"L", {"P0"}}};

    const std::string gpu_loc = GPU_PREFIX + "0";
    const std::string json_str = "{\"" + gpu_loc + "\" : [[\"P0\",\"P1\"],[]]}";

    mooncake::Topology topology;
    ASSERT_EQ(topology.parse(json_str), 0);
    ASSERT_EQ(topology.getHcaList().size(), static_cast<size_t>(2));
    ASSERT_EQ(topology.getHcaList()[0], "P0");

    std::unordered_map<int, int> hist;
    for (int i = 0; i < 64; ++i) {
        int id = topology.selectDeviceByLocalHca(gpu_loc, "L", 0);
        hist[id]++;
    }

    cfg.enable_hca_peer_affinity = old_enable;
    cfg.nic_peer_affinity = old_map;

    ASSERT_EQ(hist.size(), static_cast<size_t>(1));
    EXPECT_EQ(hist[0], 64) << "peer affinity should pin " << gpu_loc
                           << " to P0 for local HCA L";
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
