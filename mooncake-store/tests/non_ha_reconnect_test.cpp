#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>

#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog.hpp>
#include <ylt/easylog/record.hpp>

#include "client.h"
#include "utils.h"
#include "test_server_helpers.h"

namespace mooncake {
namespace testing {

// Non-HA: client auto-reconnects to master and remounts segments
TEST(NonHAReconnectTest, ClientAutoReconnectAndRemount) {
    // Start master (auto-pick ports) and embedded HTTP metadata server
    InProcMaster master;
    ASSERT_TRUE(master.Start());

    // Create client (non-HA), mount a segment
    std::string metadata_url = master.metadata_url();
    std::string local_hostname = "127.0.0.1:18001";
    std::string master_addr = master.master_address();
    auto client_opt = Client::Create(local_hostname, metadata_url, "tcp",
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
    ASSERT_TRUE(master.Start(master.rpc_port(), master.http_metrics_port(),
                             /*enable_http_metadata=*/true,
                             master.http_metadata_port()));

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
    easylog::set_min_severity(easylog::Severity::WARNING);
    return RUN_ALL_TESTS();
}
