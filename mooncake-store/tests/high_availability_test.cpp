#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "types.h"
#include "utils.h"
#include "ha_helper.h"
#include "etcd_helper.h"

namespace mooncake {
namespace testing {

DEFINE_string(etcd_endpoints, "0.0.0.0:2379", "Etcd endpoints");
DEFINE_string(etcd_test_key_prefix, "mooncake-store/test/", "The prefix of the test keys in ETCD");

class HighAvailabilityTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("ClientIntegrationTest");

        // Set VLOG level to 1 for detailed logs
        google::SetVLOGLevel("*", 1);
        FLAGS_logtostderr = 1;

        // Initialize etcd client
        char* err_msg = nullptr;
        auto ret = NewStoreEtcdClient((char*)FLAGS_etcd_endpoints.c_str(), &err_msg);
        ASSERT_EQ(0, ret) << err_msg;
        free(err_msg);
        err_msg = nullptr;
    }

    static void TearDownTestSuite() {
        google::ShutdownGoogleLogging();
    }
};

TEST_F(HighAvailabilityTest, EtcdBasicOperations) {
    std::map<std::string, std::string> kvs;
    std::string prefix(FLAGS_etcd_test_key_prefix);
/*
    // Create and get a ordinary key-value pair
    std::string key(prefix + "test_key");
    std::string value("test_value");
    ASSERT_EQ(ErrorCode::OK, EtcdPut(key.c_str(), key.size(), value.c_str(), value.size()));
    std::string get_value;
    ASSERT_EQ(ErrorCode::OK, EtcdGet(key.c_str(), key.size(), get_value));
    ASSERT_EQ(value, get_value);
    kvs[key] = value;

    // Create and get a key-value pair with null bytes
    key = std::string(prefix + "test_\0\0key2", 11);
    value = std::string("test_\0\0value2", 13);
    ASSERT_EQ(ErrorCode::OK, EtcdPut(key.c_str(), key.size(), value.c_str(), value.size()));
    ASSERT_EQ(ErrorCode::OK, EtcdGet(key.c_str(), key.size(), get_value));
    ASSERT_EQ(value, get_value);
    kvs[key] = value;
*/
}

TEST_F(HighAvailabilityTest, EtcdPutGetMeta) {
    std::string key1("test_meta_segment");
    std::string segment_name1("test_segment");
    uint64_t base1 = 300000000;
    uint64_t size1 = 1024 * 1024 * 32;
}

// Test leader election and master address discovery
TEST_F(HighAvailabilityTest, LeaderElectionAndMasterAddressDiscovery) {
    MasterViewHelper mv_helper;
    mv_helper.ConnectToEtcd(FLAGS_etcd_endpoints);
    std::string master_address;
    ViewVersion version = 0;
    mv_helper.GetMasterView(master_address, version);
    LOG(INFO) << "Master address: " << master_address << ", version: " << version;
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
