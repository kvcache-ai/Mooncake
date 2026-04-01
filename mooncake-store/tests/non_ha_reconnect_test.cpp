#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <csignal>

#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog.hpp>
#include <ylt/easylog/record.hpp>

#include "client_service.h"
#include "master_metric_manager.h"
#include "utils.h"
#include "test_server_helpers.h"
#include "default_config.h"

namespace mooncake {
namespace testing {

TEST(NonHAReconnectTest, ZeroSegmentClientStartsHeartbeatOnlyAfterMount) {
    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));

    std::string local_hostname = "127.0.0.1:18011";
    std::string master_addr = master.master_address();
    const auto ping_before =
        MasterMetricManager::instance().get_ping_requests();

    auto client_opt = Client::Create(local_hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_addr);
    ASSERT_TRUE(client_opt.has_value());
    auto client = client_opt.value();

    // Zero-segment clients should not start storage heartbeat immediately.
    std::this_thread::sleep_for(std::chrono::milliseconds(2200));
    const auto ping_without_mount =
        MasterMetricManager::instance().get_ping_requests();
    EXPECT_EQ(ping_without_mount, ping_before);

    size_t ram_buffer_size = 16 * 1024 * 1024;
    void* seg_ptr = allocate_buffer_allocator_memory(ram_buffer_size);
    ASSERT_NE(seg_ptr, nullptr);

    auto mount_res = client->MountSegment(seg_ptr, ram_buffer_size);
    ASSERT_TRUE(mount_res.has_value()) << toString(mount_res.error());

    std::this_thread::sleep_for(std::chrono::milliseconds(2200));
    const auto ping_after_mount =
        MasterMetricManager::instance().get_ping_requests();
    EXPECT_GT(ping_after_mount, ping_without_mount);

    auto unmount_res = client->UnmountSegment(seg_ptr, ram_buffer_size);
    ASSERT_TRUE(unmount_res.has_value()) << toString(unmount_res.error());
    free(seg_ptr);
}

TEST(NonHAReconnectTest, ZeroSegmentClientStartsHeartbeatAfterLocalDiskMount) {
    const auto tmp_dir =
        std::filesystem::temp_directory_path() /
        ("mc_zero_seg_local_disk_" +
         std::to_string(
             std::chrono::steady_clock::now().time_since_epoch().count()));
    ASSERT_TRUE(std::filesystem::create_directories(tmp_dir));

    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder()
                                 .set_root_fs_dir(tmp_dir.string())
                                 .set_enable_offload(true)
                                 .build()));

    std::string local_hostname = "127.0.0.1:18012";
    std::string master_addr = master.master_address();
    const auto ping_before =
        MasterMetricManager::instance().get_ping_requests();

    auto client_opt = Client::Create(local_hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_addr);
    ASSERT_TRUE(client_opt.has_value());
    auto client = client_opt.value();

    std::this_thread::sleep_for(std::chrono::milliseconds(2200));
    const auto ping_without_mount =
        MasterMetricManager::instance().get_ping_requests();
    EXPECT_EQ(ping_without_mount, ping_before);

    auto mount_res = client->MountLocalDiskSegment(false);
    ASSERT_TRUE(mount_res.has_value()) << toString(mount_res.error());

    std::this_thread::sleep_for(std::chrono::milliseconds(2200));
    const auto ping_after_mount =
        MasterMetricManager::instance().get_ping_requests();
    EXPECT_GT(ping_after_mount, ping_without_mount);

    client.reset();
    std::error_code ec;
    std::filesystem::remove_all(tmp_dir, ec);
}

// Non-HA: client auto-reconnects to master and remounts segments
TEST(NonHAReconnectTest, ClientAutoReconnectAndRemount) {
    // Start master (auto-pick ports) without HTTP metadata server
    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));

    // Create client (non-HA), mount a segment
    std::string local_hostname = "127.0.0.1:18001";
    std::string master_addr = master.master_address();
    auto client_opt = Client::Create(local_hostname, "P2PHANDSHAKE", "tcp",
                                     std::nullopt, master_addr);
    ASSERT_TRUE(client_opt.has_value());
    auto client = client_opt.value();

    size_t ram_buffer_size = 16 * 1024 * 1024;  // 16MB
    void* seg_ptr = allocate_buffer_allocator_memory(ram_buffer_size);
    ASSERT_NE(seg_ptr, nullptr);
    auto mount_res = client->MountSegment(seg_ptr, ram_buffer_size);
    ASSERT_TRUE(mount_res.has_value()) << toString(mount_res.error());

    // Verify segment is visible via helper (expected-based)
    {
        auto vis = CheckSegmentVisible(master, local_hostname);
        if (!vis.has_value()) {
            LOG(INFO) << "Initial segment not visible: " << vis.error();
        }
        ASSERT_TRUE(vis.value()) << "Initial segment not found in master";
    }

    // Stop master, wait for ping failures to accumulate
    master.Stop();
    std::this_thread::sleep_for(std::chrono::seconds(3));

    auto vis_result = CheckSegmentVisible(master, local_hostname);
    ASSERT_FALSE(vis_result.has_value())
        << "Segment should not be visible after master stop";

    // Restart master on the same ports
    ASSERT_TRUE(
        master.Start(InProcMasterConfigBuilder()
                         .set_rpc_port(master.rpc_port())
                         .set_http_metrics_port(master.http_metrics_port())
                         .build()));

    // Wait until remount is reflected in master
    bool found = false;
    for (int i = 0; i < 30; ++i) {  // up to ~15s
        auto vis = CheckSegmentVisible(master, local_hostname);
        if (vis.value()) {
            LOG(INFO) << "Segment found in master after restart in " << i
                      << " attempts";
            found = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    ASSERT_TRUE(found) << "Client did not reconnect and remount in time";

    // Cleanup
    auto unmount_res = client->UnmountSegment(seg_ptr, ram_buffer_size);
    ASSERT_TRUE(unmount_res.has_value()) << toString(unmount_res.error());
    // Free the aligned memory allocated for the segment to avoid leaks
    free(seg_ptr);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    mooncake::init_ylt_log_level();
    return RUN_ALL_TESTS();
}
