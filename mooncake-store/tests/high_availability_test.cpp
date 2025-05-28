#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <numa.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "allocator.h"
#include "client.h"
#include "types.h"
#include "utils.h"
#include "ha_helper.h"

namespace mooncake {
namespace testing {

DEFINE_string(etcd_endpoints, "0.0.0.0:2379", "Etcd endpoints");

class HighAvailabilityTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTest");

        // Set VLOG level to 1 for detailed logs
        google::SetVLOGLevel("*", 1);
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() {
        google::ShutdownGoogleLogging();
    }
};

// Test leader election and master address discovery
TEST_F(HighAvailabilityTest, LeaderElectionAndMasterAddressDiscovery) {
    ClientHaHelper client_ha_helper;
    client_ha_helper.ConnectToEtcd(FLAGS_etcd_endpoints);
    std::string master_address;
    client_ha_helper.GetMasterAddress(master_address);
    LOG(INFO) << "Master address: " << master_address;
}

}  // namespace testing

}  // namespace mooncake

int main(int argc, char** argv) {
    // Initialize Google's flags library
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // Initialize Google Test
    ::testing::InitGoogleTest(&argc, argv);

    // Run all tests
    return RUN_ALL_TESTS();
}
