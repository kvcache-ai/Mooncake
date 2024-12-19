#include "topology.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "transfer_metadata.h"

TEST(ToplogyTest, GetTopologyMatrix)
{
    std::string topo = mooncake::discoverTopologyMatrix();
    LOG(INFO) << topo;
    mooncake::TransferMetadata::PriorityMatrix matrix;
    std::vector<std::string> rnic_list;
    mooncake::TransferMetadata::parseNicPriorityMatrix(topo, matrix, rnic_list);
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
