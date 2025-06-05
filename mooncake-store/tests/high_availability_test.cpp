#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "types.h"
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
        ASSERT_EQ(ErrorCode::OK, EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints));
    }

    static void TearDownTestSuite() {
        google::ShutdownGoogleLogging();
    }
};

TEST_F(HighAvailabilityTest, EtcdBasicOperations) {
    // Test grant lease, create kv and get kv
    const int64_t lease_ttl = 10;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    // Ordinary key-value pair
    keys.push_back(FLAGS_etcd_test_key_prefix + std::string("test_key1"));
    values.push_back("test_value1");
    // Key-value pair with null bytes in the middle
    keys.push_back(FLAGS_etcd_test_key_prefix + std::string("test_\0\0key2"));
    values.push_back("test_\0\0value2");
    // Key-value pair with null bytes at the end
    keys.push_back(FLAGS_etcd_test_key_prefix + std::string("test_key3\0\0"));
    values.push_back("test_value3\0\0");
    // Key-value pair with null bytes at the beginning
    keys.push_back(FLAGS_etcd_test_key_prefix + std::string("\0\0test_key4"));
    values.push_back("\0\0test_value4");

    for (size_t i = 0; i < keys.size(); i++) {
        auto &key = keys[i];
        auto &value = values[i];
        EtcdLeaseId lease_id;
        EtcdRevisionId version = 0;

        ASSERT_EQ(ErrorCode::OK, EtcdHelper::EtcdGrantLease(lease_ttl, lease_id));
        ASSERT_EQ(ErrorCode::OK, EtcdHelper::EtcdCreateWithLease(key.c_str(), key.size(),
            value.c_str(), value.size(), lease_id, version));
        std::string get_value;
        EtcdRevisionId get_version;
        ASSERT_EQ(ErrorCode::OK, EtcdHelper::EtcdGet(key.c_str(), key.size(), get_value, get_version));
        ASSERT_EQ(value, get_value);
        ASSERT_EQ(version, get_version);
    }
}

// Test leader election and master address discovery
TEST_F(HighAvailabilityTest, LeaderElectionAndMasterAddressDiscovery) {
    MasterViewHelper mv_helper;
    mv_helper.ConnectToEtcd(FLAGS_etcd_endpoints);
    std::string master_address;
    ViewVersionId version = 0;
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
