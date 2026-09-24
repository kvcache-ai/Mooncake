// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "tent/rpc/rpc.h"
#include "tent/runtime/control_plane.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;

template <typename TestBody>
void WithFreshCaller(TestBody body) {
    // ControlClient pools are thread-local. Isolate cases even if a later
    // server randomly selects a port used by an earlier test or repetition.
    std::thread caller(std::move(body));
    caller.join();
}

class SegmentDescRetryTest : public ::testing::Test {
   protected:
    struct Result {
        Status status;
        std::string response;
    };

    Result Read() {
        Result result;
        result.status = ControlClient::getSegmentDesc(addr_, result.response);
        return result;
    }

    Status StartServer(const std::string& epoch) {
        server_ = std::make_unique<CoroRpcAgent>();
        CHECK_STATUS(server_->registerFunction(
            GetSegmentDesc,
            [this, epoch](const std::string_view&, std::string& response) {
                metadata_calls_.fetch_add(1);
                if (fail_metadata_.load()) {
                    throw std::runtime_error("metadata handler failed");
                }
                response = epoch;
            }));
        CHECK_STATUS(server_->registerFunction(
            Notify, [this](const std::string_view&, std::string&) {
                notifications_.fetch_add(1);
                throw std::runtime_error("after notification side effect");
            }));
        const auto requested_port = port_;
        CHECK_STATUS(server_->start(port_));
        if (requested_port != 0 && port_ != requested_port) {
            return Status::InvalidArgument("restart changed the peer port");
        }
        addr_ = "127.0.0.1:" + std::to_string(port_);
        return Status::OK();
    }

    Status RestartServer() {
        server_.reset();
        // Match rpc_reconnect_test: let the old client sockets observe FIN.
        std::this_thread::sleep_for(100ms);
        return StartServer("epoch2");
    }

    void TearDown() override {
        // A fatal assertion must still release and join the parked worker.
        if (worker_.joinable()) {
            if (!released_) resume_.set_value();
            worker_.join();
        }
        server_.reset();
    }

    std::unique_ptr<CoroRpcAgent> server_;
    uint16_t port_ = 0;
    std::string addr_;
    std::atomic<int> metadata_calls_{0};
    std::atomic<int> notifications_{0};
    std::atomic<bool> fail_metadata_{false};
    std::promise<void> warmed_;
    std::promise<void> resume_;
    std::thread worker_;
    bool released_ = false;
    Result worker_warm_;
    Result worker_after_restart_;
    bool worker_resumed_ = false;
};

TEST_F(SegmentDescRetryTest, FirstMetadataCallRecoversAfterSamePortRestart) {
    WithFreshCaller([&] {
        ASSERT_TRUE(StartServer("epoch1").ok());
        const auto warm = Read();
        ASSERT_TRUE(warm.status.ok()) << warm.status.ToString();
        ASSERT_EQ(warm.response, "epoch1");
        ASSERT_TRUE(RestartServer().ok());

        const auto result = Read();
        EXPECT_TRUE(result.status.ok()) << result.status.ToString();
        EXPECT_EQ(result.response, "epoch2");
        EXPECT_EQ(metadata_calls_.load(), 2);
    });
}

TEST_F(SegmentDescRetryTest, WorkerRecoversAfterMainRefreshesItsOwnPool) {
    WithFreshCaller([&] {
        ASSERT_TRUE(StartServer("epoch1").ok());
        const auto warm = Read();
        ASSERT_TRUE(warm.status.ok()) << warm.status.ToString();
        ASSERT_EQ(warm.response, "epoch1");

        auto warmed = warmed_.get_future();
        auto resume = resume_.get_future();
        worker_ = std::thread([this, resume = std::move(resume)] {
            worker_warm_ = Read();
            warmed_.set_value();
            if (resume.wait_for(10s) != std::future_status::ready) return;
            worker_resumed_ = true;
            worker_after_restart_ = Read();
        });
        ASSERT_EQ(warmed.wait_for(10s), std::future_status::ready);
        ASSERT_TRUE(worker_warm_.status.ok()) << worker_warm_.status.ToString();
        ASSERT_EQ(worker_warm_.response, "epoch1");
        ASSERT_TRUE(RestartServer().ok());

        // Recover main even on the original implementation, so a worker failure
        // specifically demonstrates its independent thread-local stale pool.
        auto main_result = Read();
        if (!main_result.status.ok()) main_result = Read();
        ASSERT_TRUE(main_result.status.ok()) << main_result.status.ToString();
        ASSERT_EQ(main_result.response, "epoch2");

        released_ = true;
        resume_.set_value();
        worker_.join();
        ASSERT_TRUE(worker_resumed_);
        EXPECT_TRUE(worker_after_restart_.status.ok())
            << worker_after_restart_.status.ToString();
        EXPECT_EQ(worker_after_restart_.response, "epoch2");
        EXPECT_EQ(metadata_calls_.load(), 4);
    });
}

TEST_F(SegmentDescRetryTest, HealthyMetadataHandlerRunsOnce) {
    WithFreshCaller([&] {
        ASSERT_TRUE(StartServer("epoch1").ok());
        const auto result = Read();
        EXPECT_TRUE(result.status.ok()) << result.status.ToString();
        EXPECT_EQ(result.response, "epoch1");
        EXPECT_EQ(metadata_calls_.load(), 1);
    });
}

TEST_F(SegmentDescRetryTest, ThrowingMetadataHandlerRunsExactlyTwice) {
    WithFreshCaller([&] {
        ASSERT_TRUE(StartServer("epoch1").ok());
        fail_metadata_.store(true);
        const auto result = Read();
        EXPECT_TRUE(result.status.IsRpcServiceError())
            << result.status.ToString();
        EXPECT_TRUE(result.response.empty());
        EXPECT_EQ(metadata_calls_.load(), 2);
    });
}

TEST_F(SegmentDescRetryTest, DeadPeerStillReturnsFailure) {
    WithFreshCaller([&] {
        ASSERT_TRUE(StartServer("epoch1").ok());
        const auto warm = Read();
        ASSERT_TRUE(warm.status.ok()) << warm.status.ToString();
        server_.reset();
        std::this_thread::sleep_for(100ms);

        const auto result = Read();
        EXPECT_TRUE(result.status.IsRpcServiceError())
            << result.status.ToString();
        EXPECT_TRUE(result.response.empty());
        EXPECT_EQ(metadata_calls_.load(), 1);
    });
}

TEST_F(SegmentDescRetryTest, NotificationSideEffectIsNotReplayed) {
    WithFreshCaller([&] {
        ASSERT_TRUE(StartServer("epoch1").ok());
        const auto status =
            ControlClient::notify(addr_, Notification{"test", "once"});
        EXPECT_TRUE(status.IsRpcServiceError()) << status.ToString();
        EXPECT_EQ(notifications_.load(), 1);
    });
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
