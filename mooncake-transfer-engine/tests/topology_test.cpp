#include "topology.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "transfer_metadata.h"

TEST(ToplogyTest, GetTopologyMatrix)
{
    mooncake::Topology topology;
    topology.discover();
    std::string json_str = topology.toString();
    LOG(INFO) << json_str;
    topology.clear();
    topology.parse(json_str);
    ASSERT_EQ(topology.toString(), json_str);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
