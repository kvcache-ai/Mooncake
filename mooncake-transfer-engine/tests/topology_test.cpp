#include "topology.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

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

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
