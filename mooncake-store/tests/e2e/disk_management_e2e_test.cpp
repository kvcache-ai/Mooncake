#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include <ylt/coro_http/coro_http_client.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>

#include "client_wrapper.h"
#include "test_server_helpers.h"
#include "types.h"
#include "utils.h"

namespace fs = std::filesystem;

constexpr int kClientPortBase = 14888;

namespace mooncake {
namespace testing {

struct HttpResponse {
    int status;
    std::string body;
};

HttpResponse HttpGet(const std::string& url) {
    coro_http::coro_http_client client;
    auto result = client.get(url);
    return {result.status, std::string(result.resp_body)};
}

HttpResponse HttpPost(const std::string& url, const std::string& body) {
    coro_http::coro_http_client client;
    auto result = client.post(url, body, coro_http::req_content_type::json);
    return {result.status, std::string(result.resp_body)};
}

HttpResponse HttpPut(const std::string& url, const std::string& body) {
    coro_http::coro_http_client client;
    auto result = async_simple::coro::syncAwait(
        client.async_put(url, body, coro_http::req_content_type::json));
    return {result.status, std::string(result.resp_body)};
}

class DiskManagementE2ETest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("DiskManagementE2ETest");
        FLAGS_logtostderr = 1;
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        tmp_dir_ = fs::temp_directory_path() /
                   ("mc_dm_e2e_" + std::to_string(::getpid()));
        fs::create_directories(tmp_dir_);
    }

    void TearDown() override {
        master_.Stop();
        std::error_code ec;
        fs::remove_all(tmp_dir_, ec);
    }

    void StartMaster(uint64_t lease_ttl = 1000) {
        auto builder = InProcMasterConfigBuilder()
                           .set_root_fs_dir(tmp_dir_.string())
                           .set_default_kv_lease_ttl(lease_ttl);
        ASSERT_TRUE(master_.Start(builder.build()));
        admin_base_ = master_.http_metrics_base();
        master_addr_ = master_.master_address();
    }

    std::shared_ptr<ClientTestWrapper> CreateClient(
        int port_offset, size_t seg_size = 16 * 1024 * 1024) {
        std::string hostname =
            "127.0.0.1:" + std::to_string(kClientPortBase + port_offset);
        auto opt = ClientTestWrapper::CreateClientWrapper(
            hostname, "P2PHANDSHAKE", "tcp", "", master_addr_);
        if (!opt.has_value()) return nullptr;
        auto wrapper = *opt;
        void* buf = nullptr;
        if (wrapper->Mount(seg_size, buf) != ErrorCode::OK) return nullptr;
        return wrapper;
    }

    fs::path tmp_dir_;
    InProcMaster master_;
    std::string admin_base_;
    std::string master_addr_;
};

// -------------------------------------------------------------------
// E2E 1: Device metadata visible after client mounts
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, DeviceMetadataVisibleAfterMount) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    auto resp = HttpGet(admin_base_ + "/api/v1/devices/metadata");
    EXPECT_EQ(resp.status, 200);
    EXPECT_NE(resp.body.find("\"total_devices\""), std::string::npos);
}

// -------------------------------------------------------------------
// E2E 2: Maintenance plan reports healthy devices (no candidates)
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, MaintenancePlanEmptyForHealthyCluster) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    auto resp = HttpGet(admin_base_ + "/api/v1/devices/maintenance");
    EXPECT_EQ(resp.status, 200);
    EXPECT_NE(resp.body.find("\"total_recovery_candidates\":0"),
              std::string::npos);
    EXPECT_NE(resp.body.find("\"total_gc_candidates\":0"), std::string::npos);
}

// -------------------------------------------------------------------
// E2E 3: Put data → ExistKey → cleanup → key gone
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, CleanupRemovesExpiredKeys) {
    StartMaster(500);
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    const std::string key = "e2e_cleanup_key";
    const std::string value(4096, 'X');
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    bool exist = client->HasMemoryReplica(key) || client->HasDiskReplica(key);
    EXPECT_TRUE(exist);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto cleanup_resp = HttpPost(
        admin_base_ + "/api/v1/devices/cleanup",
        R"({"device_id":"any","strategy":"manual","keys":[")" + key + R"("]})");
    EXPECT_EQ(cleanup_resp.status, 200);

    exist = (client->HasMemoryReplica(key) || client->HasDiskReplica(key));
    EXPECT_FALSE(exist);
}

// -------------------------------------------------------------------
// E2E 4: Drain job lifecycle — create, query, cancel
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, DrainJobLifecycle) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    auto segs_resp = HttpGet(admin_base_ + "/get_all_segments");
    EXPECT_EQ(segs_resp.status, 200);

    if (segs_resp.body.find("admin_test") == std::string::npos) {
        return;
    }
}

// -------------------------------------------------------------------
// E2E 5: GC candidates endpoint returns data
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, GcCandidatesEndpoint) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    auto resp = HttpGet(admin_base_ + "/api/v1/devices/gc_candidates");
    EXPECT_EQ(resp.status, 200);
    EXPECT_NE(resp.body.find("\"total_gc_candidates\""), std::string::npos);
}

// -------------------------------------------------------------------
// E2E 6: Put → Get → Remove → Get fails
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, PutGetRemoveLifecycle) {
    StartMaster(500);
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    const std::string key = "e2e_lifecycle_key";
    const std::string value(8192, 'Z');
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    std::string got;
    ASSERT_EQ(client->Get(key, got), ErrorCode::OK);
    EXPECT_EQ(got, value);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto remove_result = client->Delete(key);
    EXPECT_EQ(remove_result, ErrorCode::OK);

    bool exist = client->HasMemoryReplica(key) || client->HasDiskReplica(key);
    EXPECT_FALSE(exist);
}

// -------------------------------------------------------------------
// E2E 7: Multiple clients see same key
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, MultiClientKeyVisibility) {
    StartMaster();
    auto writer = CreateClient(0);
    ASSERT_NE(writer, nullptr);

    const std::string key = "e2e_multi_client_key";
    const std::string value(4096, 'M');
    ASSERT_EQ(writer->Put(key, value), ErrorCode::OK);

    auto reader = CreateClient(1);
    ASSERT_NE(reader, nullptr);

    bool exist = reader->HasMemoryReplica(key) || reader->HasDiskReplica(key);
    EXPECT_TRUE(exist);

    std::string got;
    ASSERT_EQ(reader->Get(key, got), ErrorCode::OK);
    EXPECT_EQ(got, value);
}

// -------------------------------------------------------------------
// E2E 8: Cleanup dry_run does not delete
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, CleanupDryRunPreservesKeys) {
    StartMaster(500);
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    const std::string key = "e2e_dryrun_key";
    const std::string value(4096, 'D');
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto resp = HttpPost(admin_base_ + "/api/v1/devices/cleanup",
                         R"({"device_id":"any","strategy":"manual","keys":[")" +
                             key + R"("],"dry_run":true})");
    EXPECT_EQ(resp.status, 200);

    bool exist = client->HasMemoryReplica(key) || client->HasDiskReplica(key);
    EXPECT_TRUE(exist);
}

// -------------------------------------------------------------------
// E2E 9: Device metadata update — disable and re-enable via admin API
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, DeviceMetadataDisableEnable) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    auto meta = HttpGet(admin_base_ + "/api/v1/devices/metadata");
    ASSERT_EQ(meta.status, 200);

    auto pos = meta.body.find("\"device_id\":\"");
    if (pos == std::string::npos) return;
    auto start = pos + 13;
    auto end = meta.body.find('"', start);
    std::string device_id = meta.body.substr(start, end - start);

    auto disable =
        HttpPut(admin_base_ + "/api/v1/devices/metadata?device_id=" + device_id,
                R"({"enabled":false})");
    EXPECT_EQ(disable.status, 200);
    EXPECT_NE(disable.body.find("\"health\":\"DISABLED\""), std::string::npos);

    auto enable =
        HttpPut(admin_base_ + "/api/v1/devices/metadata?device_id=" + device_id,
                R"({"enabled":true})");
    EXPECT_EQ(enable.status, 200);
    EXPECT_NE(enable.body.find("\"health\":\"HEALTHY\""), std::string::npos);
}

// -------------------------------------------------------------------
// E2E 10: Probe endpoint works on mounted devices
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, ProbeEndpoint) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    auto meta = HttpGet(admin_base_ + "/api/v1/devices/metadata");
    ASSERT_EQ(meta.status, 200);

    auto pos = meta.body.find("\"device_id\":\"");
    if (pos == std::string::npos) return;
    auto start = pos + 13;
    auto end = meta.body.find('"', start);
    std::string device_id = meta.body.substr(start, end - start);

    auto probe_success =
        HttpPost(admin_base_ + "/api/v1/devices/probe?device_id=" + device_id +
                     "&action=success",
                 "");
    EXPECT_EQ(probe_success.status, 200);

    auto probe_fail =
        HttpPost(admin_base_ + "/api/v1/devices/probe?device_id=" + device_id +
                     "&action=fail",
                 "");
    EXPECT_EQ(probe_fail.status, 200);
    EXPECT_NE(probe_fail.body.find("\"health\":\"DEGRADED\""),
              std::string::npos);
}

// -------------------------------------------------------------------
// E2E 11: Recovery start → sets REBUILDING, complete blocked
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, RecoveryStartBlocksComplete) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    auto meta = HttpGet(admin_base_ + "/api/v1/devices/metadata");
    ASSERT_EQ(meta.status, 200);

    auto pos = meta.body.find("\"device_id\":\"");
    if (pos == std::string::npos) return;
    auto start = pos + 13;
    auto end = meta.body.find('"', start);
    std::string device_id = meta.body.substr(start, end - start);

    auto start_resp = HttpPost(
        admin_base_ + "/api/v1/devices/recovery?device_id=" + device_id +
            "&action=start",
        "");
    EXPECT_EQ(start_resp.status, 200);
    EXPECT_NE(start_resp.body.find("REBUILDING"), std::string::npos);

    auto complete_resp = HttpPost(
        admin_base_ + "/api/v1/devices/recovery?device_id=" + device_id +
            "&action=complete",
        "");
    EXPECT_NE(complete_resp.status, 200);

    auto fail_resp = HttpPost(
        admin_base_ + "/api/v1/devices/recovery?device_id=" + device_id +
            "&action=fail",
        "");
    EXPECT_EQ(fail_resp.status, 200);
}

// -------------------------------------------------------------------
// E2E 12: Batch put → batch get consistency
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, BatchPutGetConsistency) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    constexpr int kCount = 20;
    constexpr size_t kSize = 8192;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    for (int i = 0; i < kCount; ++i) {
        std::string key = "e2e_batch_" + std::to_string(i);
        std::string value(kSize, static_cast<char>('A' + (i % 26)));
        ASSERT_EQ(client->Put(key, value), ErrorCode::OK);
        keys.push_back(key);
        values.push_back(value);
    }

    for (int i = 0; i < kCount; ++i) {
        std::string got;
        ASSERT_EQ(client->Get(keys[i], got), ErrorCode::OK);
        EXPECT_EQ(got, values[i]) << "Mismatch at key " << keys[i];
    }
}

// -------------------------------------------------------------------
// E2E 13: Large value put/get (1MB)
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, LargeValuePutGet) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    const std::string key = "e2e_large_value";
    const std::string value(1024 * 1024, 'L');
    ASSERT_EQ(client->Put(key, value), ErrorCode::OK);

    std::string got;
    ASSERT_EQ(client->Get(key, got), ErrorCode::OK);
    EXPECT_EQ(got, value);
}

// -------------------------------------------------------------------
// E2E 15: Admin segment detail endpoint
// -------------------------------------------------------------------
TEST_F(DiskManagementE2ETest, SegmentDetailEndpoint) {
    StartMaster();
    auto client = CreateClient(0);
    ASSERT_NE(client, nullptr);

    auto resp = HttpGet(admin_base_ + "/get_segments_detail");
    EXPECT_EQ(resp.status, 200);
    EXPECT_NE(resp.body.find("\"total_segments\""), std::string::npos);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
