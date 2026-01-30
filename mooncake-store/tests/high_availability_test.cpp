#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "etcd_helper.h"
#include "ha_helper.h"
#include "types.h"

namespace mooncake {
namespace testing {

DEFINE_string(etcd_endpoints, "127.0.0.1:2379", "Etcd endpoints");
DEFINE_string(etcd_test_key_prefix, "mooncake-store/test/",
              "The prefix of the test keys in ETCD");

class HighAvailabilityTest : public ::testing::Test {
   protected:
    static bool etcd_available_;

    static void SetUpTestSuite() {
        // Initialize glog
        google::InitGoogleLogging("HighAvailabilityTest");

        // Set VLOG level to 1 for detailed logs
        google::SetVLOGLevel("*", 1);
        FLAGS_logtostderr = 1;

        // Initialize etcd client
        ErrorCode err =
            EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "Failed to initialize etcd client, skipping tests.";
            etcd_available_ = false;
            return;
        }

        // Probe connectivity: Try to get a non-existent key
        // We use a short timeout check implicitly via the wrapper's timeout
        // (default 5s) If this fails, the etcd server is likely down.
        std::string val;
        EtcdRevisionId rev;
        // Using a key that likely doesn't exist
        err = EtcdHelper::Get("probe_connection_key", 20, val, rev);

        if (err == ErrorCode::ETCD_OPERATION_ERROR) {
            LOG(WARNING) << "Failed to connect to Etcd at "
                         << FLAGS_etcd_endpoints << " (Error: " << (int)err
                         << "). Integration tests will be S K I P P E D.";
            etcd_available_ = false;
        } else {
            // OK or KEY_NOT_EXIST means connection is good
            etcd_available_ = true;
        }
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        if (!etcd_available_) {
            GTEST_SKIP() << "Etcd server not reachable at "
                         << FLAGS_etcd_endpoints;
        }
    }
};

bool HighAvailabilityTest::etcd_available_ = false;

TEST_F(HighAvailabilityTest, EtcdBasicOperations) {
    // == Test grant lease, create kv and get kv ==
    int64_t lease_ttl = 10;
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
        auto& key = keys[i];
        auto& value = values[i];
        EtcdLeaseId lease_id;
        EtcdRevisionId version = 0;

        ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));
        ASSERT_EQ(ErrorCode::OK, EtcdHelper::CreateWithLease(
                                     key.c_str(), key.size(), value.c_str(),
                                     value.size(), lease_id, version));
        std::string get_value;
        EtcdRevisionId get_version;
        ASSERT_EQ(ErrorCode::OK, EtcdHelper::Get(key.c_str(), key.size(),
                                                 get_value, get_version));
        ASSERT_EQ(value, get_value);
        ASSERT_EQ(version, get_version);
    }

    // == Test keep alive and cancel keep alive ==
    lease_ttl = 2;
    EtcdLeaseId lease_id;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));

    std::promise<ErrorCode> promise;
    std::future<ErrorCode> future = promise.get_future();

    std::thread keep_alive_thread([&]() {
        ErrorCode result = EtcdHelper::KeepAlive(lease_id);
        promise.set_value(result);
    });
    // Check if keep alive can extend the lease's life time
    ASSERT_NE(future.wait_for(std::chrono::seconds(lease_ttl * 3)),
              std::future_status::ready);
    std::string key =
        FLAGS_etcd_test_key_prefix + std::string("keep_alive_key");
    std::string value = "keep_alive_value";
    EtcdRevisionId version = 0;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::CreateWithLease(
                                 key.c_str(), key.size(), value.c_str(),
                                 value.size(), lease_id, version));
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Get(key.c_str(), key.size(), value, version));

    // Test cancel keep alive
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::CancelKeepAlive(lease_id));
    ASSERT_EQ(future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    ASSERT_EQ(future.get(), ErrorCode::ETCD_CTX_CANCELLED);
    keep_alive_thread.join();

    // == Test watch key and cancel watch ==
    lease_ttl = 2;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));
    std::string watch_key =
        FLAGS_etcd_test_key_prefix + std::string("watch_key");
    std::string watch_value = "watch_value";
    EtcdRevisionId watch_version = 0;
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CreateWithLease(
                  watch_key.c_str(), watch_key.size(), watch_value.c_str(),
                  watch_value.size(), lease_id, watch_version));

    promise = std::promise<ErrorCode>();
    future = promise.get_future();
    keep_alive_thread = std::thread([&]() { EtcdHelper::KeepAlive(lease_id); });
    std::thread watch_thread([&]() {
        ErrorCode result =
            EtcdHelper::WatchUntilDeleted(watch_key.c_str(), watch_key.size());
        promise.set_value(result);
    });
    // Check the watch thread is blocked if the key is not deleted
    ASSERT_NE(future.wait_for(std::chrono::seconds(lease_ttl * 3)),
              std::future_status::ready);
    // Check the watch thread returns after the key is deleted
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::CancelKeepAlive(lease_id));
    ASSERT_EQ(future.wait_for(std::chrono::seconds(lease_ttl * 3)),
              std::future_status::ready);
    ASSERT_EQ(future.get(), ErrorCode::OK);
    watch_thread.join();
    keep_alive_thread.join();

    // Test cancel watch
    lease_ttl = 10;
    int64_t watch_wait_time = 2;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));
    watch_key = FLAGS_etcd_test_key_prefix + std::string("watch_key2");
    watch_value = "watch_value2";
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CreateWithLease(
                  watch_key.c_str(), watch_key.size(), watch_value.c_str(),
                  watch_value.size(), lease_id, watch_version));

    promise = std::promise<ErrorCode>();
    future = promise.get_future();
    watch_thread = std::thread([&]() {
        ErrorCode result =
            EtcdHelper::WatchUntilDeleted(watch_key.c_str(), watch_key.size());
        promise.set_value(result);
    });
    // Wait for the watch thread to call WatchUntilDeleted
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Cancel the watch
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::CancelWatch(watch_key.c_str(), watch_key.size()));
    ASSERT_EQ(future.wait_for(std::chrono::seconds(watch_wait_time)),
              std::future_status::ready);
    ASSERT_EQ(future.get(), ErrorCode::ETCD_CTX_CANCELLED);
    watch_thread.join();
}

TEST_F(HighAvailabilityTest, BasicMasterViewOperations) {
    MasterViewHelper mv_helper;
    mv_helper.ConnectToEtcd(FLAGS_etcd_endpoints);
    std::string master_address = "0.0.0.0:8888";
    ViewVersionId version = 0;

    // Initially, the master view is not set
    ASSERT_NE(ErrorCode::OK, mv_helper.GetMasterView(master_address, version));

    // Elect and keep leader
    EtcdLeaseId lease_id = 0;
    mv_helper.ElectLeader(master_address, version, lease_id);
    std::thread keep_alive_thread([&]() { mv_helper.KeepLeader(lease_id); });

    // Check the master view is correctly set
    std::string get_master_address;
    ASSERT_EQ(ErrorCode::OK,
              mv_helper.GetMasterView(get_master_address, version));
    ASSERT_EQ(get_master_address, master_address);

    // Check the master view does not change
    std::this_thread::sleep_for(
        std::chrono::seconds(ETCD_MASTER_VIEW_LEASE_TTL + 2));
    ASSERT_EQ(ErrorCode::OK,
              mv_helper.GetMasterView(get_master_address, version));
    ASSERT_EQ(get_master_address, master_address);

    EtcdHelper::CancelKeepAlive(lease_id);
    keep_alive_thread.join();
}

TEST_F(HighAvailabilityTest, OpLogPersistenceInterfaces) {
    // 1. Basic Put & Get
    std::string key = FLAGS_etcd_test_key_prefix + "oplog_test_1";
    std::string val = "v1";
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::Put(key.c_str(), key.size(),
                                             val.c_str(), val.size()));

    std::string got_val;
    EtcdRevisionId rev;
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::Get(key.c_str(), key.size(), got_val, rev));
    ASSERT_EQ(got_val, val);

    // 2. CAS Create
    std::string cas_key = FLAGS_etcd_test_key_prefix + "oplog_cas_1";
    EtcdHelper::DeleteRange(cas_key.c_str(), cas_key.size(),
                            (cas_key + "\0").c_str(), cas_key.size() + 1);

    // First create success
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::Create(cas_key.c_str(), cas_key.size(),
                                                "initial", 7));
    // Second create fails
    ASSERT_EQ(
        ErrorCode::ETCD_TRANSACTION_FAIL,
        EtcdHelper::Create(cas_key.c_str(), cas_key.size(), "conflict", 8));

    // 3. Range Operations
    std::string prefix = FLAGS_etcd_test_key_prefix + "range/";
    std::string k1 = prefix + "a";
    std::string k2 = prefix + "b";
    std::string k3 = prefix + "c";

    // Clean up
    std::string prefix_end = prefix;
    if (!prefix_end.empty()) prefix_end.back()++;
    EtcdHelper::DeleteRange(prefix.c_str(), prefix.size(), prefix_end.c_str(),
                            prefix_end.size());

    EtcdHelper::Put(k1.c_str(), k1.size(), "val_a", 5);
    EtcdHelper::Put(k2.c_str(), k2.size(), "val_b", 5);
    EtcdHelper::Put(k3.c_str(), k3.size(), "val_c", 5);

    std::string first, last;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GetFirstKeyWithPrefix(
                                 prefix.c_str(), prefix.size(), first));
    EXPECT_EQ(first, k1);
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GetLastKeyWithPrefix(
                                 prefix.c_str(), prefix.size(), last));
    EXPECT_EQ(last, k3);

    // GetRangeAsJson
    std::string json;
    EtcdRevisionId json_rev;
    // Get k1 and k2 (limit 2), range [k1, k3) -> k1, k2
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::GetRangeAsJson(k1.c_str(), k1.size(), k3.c_str(),
                                         k3.size(), 0, json, json_rev));
    // Should contain val_a and val_b but NOT val_c
    EXPECT_NE(json.find("val_a"), std::string::npos);
    EXPECT_NE(json.find("val_b"), std::string::npos);
    EXPECT_EQ(json.find("val_c"), std::string::npos);

    // CreateWithLease & DeleteRange
    int64_t lease_ttl = 10;
    EtcdLeaseId lease_id;
    EtcdRevisionId lease_rev;
    ASSERT_EQ(ErrorCode::OK, EtcdHelper::GrantLease(lease_ttl, lease_id));

    // Use DeleteRange to clear k1-k3
    ASSERT_EQ(ErrorCode::OK,
              EtcdHelper::DeleteRange(k1.c_str(), k1.size(),
                                      (k3 + "\0").c_str(), k3.size() + 1));

    std::string dummy_val;
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              EtcdHelper::Get(k1.c_str(), k1.size(), dummy_val, rev));
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              EtcdHelper::Get(k2.c_str(), k2.size(), dummy_val, rev));
    EXPECT_EQ(ErrorCode::ETCD_KEY_NOT_EXIST,
              EtcdHelper::Get(k3.c_str(), k3.size(), dummy_val, rev));
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
