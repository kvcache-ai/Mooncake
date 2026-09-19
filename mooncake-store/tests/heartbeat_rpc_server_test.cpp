#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "common/network.h"
#include "master_client.h"
#include "rpc_service.h"
#include "types.h"

namespace mooncake {
namespace {

std::atomic<bool> main_io_thread_held{false};
std::shared_future<void> main_io_thread_release;

// Stands in for any long handler: it keeps the main server's only io thread.
void HoldMainIoThread() {
    main_io_thread_held.store(true);
    main_io_thread_release.wait();
}

class HeartbeatRpcServerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        const auto ports = getFreeTcpPorts(2);
        ASSERT_EQ(ports.size(), 2u);
        main_port_ = ports[0];
        heartbeat_port_ = ports[1];

        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL;
        config.enable_metric_reporting = false;
        wrapped_ = std::make_unique<WrappedMasterService>(config);
        main_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/1, main_port_, "127.0.0.1", std::chrono::seconds(0),
            /*tcp_no_delay=*/true);
        RegisterRpcService(*main_server_, *wrapped_);
        main_server_->register_handler<HoldMainIoThread>();
        ASSERT_FALSE(main_server_->async_start().hasResult());

        heartbeat_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/1, heartbeat_port_, "127.0.0.1",
            std::chrono::seconds(0), /*tcp_no_delay=*/true);
        RegisterHeartbeatRpcService(*heartbeat_server_, *wrapped_);
        ASSERT_FALSE(heartbeat_server_->async_start().hasResult());
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    void TearDown() override {
        ReleaseMainIoThread();
        if (hold_caller_.joinable()) {
            hold_caller_.join();
        }
        if (heartbeat_server_) {
            heartbeat_server_->stop();
        }
        if (main_server_) {
            main_server_->stop();
        }
    }

    std::string MainAddress() const {
        return "127.0.0.1:" + std::to_string(main_port_);
    }

    void HoldTheMainIoThread() {
        main_io_thread_held.store(false);
        release_ = std::promise<void>();
        main_io_thread_release = release_.get_future().share();
        hold_caller_ = std::thread([port = main_port_] {
            coro_rpc::coro_rpc_client client;
            async_simple::coro::syncAwait(
                client.connect("127.0.0.1", std::to_string(port)));
            async_simple::coro::syncAwait(client.call<HoldMainIoThread>());
        });
        while (!main_io_thread_held.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    void ReleaseMainIoThread() {
        if (!released_) {
            released_ = true;
            release_.set_value();
        }
    }

    int main_port_ = 0;
    int heartbeat_port_ = 0;
    std::unique_ptr<WrappedMasterService> wrapped_;
    std::unique_ptr<coro_rpc::coro_rpc_server> main_server_;
    std::unique_ptr<coro_rpc::coro_rpc_server> heartbeat_server_;
    std::promise<void> release_;
    bool released_ = false;
    std::thread hold_caller_;
};

TEST_F(HeartbeatRpcServerTest, PingIsServedWhileTheMainIoThreadIsBusy) {
    wrapped_->SetHeartbeatRpcPort(heartbeat_port_);
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect(MainAddress()), ErrorCode::OK);

    HoldTheMainIoThread();
    auto ping = std::async(std::launch::async, [&] { return client.Ping(); });
    const bool answered_while_busy =
        ping.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    ReleaseMainIoThread();

    EXPECT_TRUE(answered_while_busy);
    EXPECT_TRUE(ping.get().has_value());
}

TEST_F(HeartbeatRpcServerTest, PingWaitsForTheMainIoThreadWithoutTheServer) {
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect(MainAddress()), ErrorCode::OK);

    HoldTheMainIoThread();
    auto ping = std::async(std::launch::async, [&] { return client.Ping(); });
    const bool answered_while_busy =
        ping.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    ReleaseMainIoThread();

    EXPECT_FALSE(answered_while_busy);
    EXPECT_TRUE(ping.get().has_value());
}

TEST_F(HeartbeatRpcServerTest, PingFallsBackToTheMainServerWhenUnreachable) {
    wrapped_->SetHeartbeatRpcPort(getFreeTcpPorts(1).at(0));
    MasterClient client(generate_uuid());
    ASSERT_EQ(client.Connect(MainAddress()), ErrorCode::OK);

    EXPECT_TRUE(client.Ping().has_value());
}

}  // namespace
}  // namespace mooncake
