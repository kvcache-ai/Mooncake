#include <gtest/gtest.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/easylog.hpp>
#include <ylt/easylog/record.hpp>

#define protected public
#include "client_service.h"
#include "centralized_client_service.h"
#include "p2p_client_service.h"
#include "test_server_helpers.h"
#include "test_p2p_server_helpers.h"
#undef protected

#include "utils.h"
#include "default_config.h"

namespace mooncake {
namespace testing {

enum class TestMode { CENTRALIZED, P2P };

static std::ostream& operator<<(std::ostream& os, const TestMode& mode) {
    if (mode == TestMode::CENTRALIZED)
        os << "CENTRALIZED";
    else if (mode == TestMode::P2P)
        os << "P2P";
    return os;
}

// Wrapper to abstract away Master and Client differences between Centralized
// and P2P
struct MasterTestWrapper {
    TestMode mode_;
    std::variant<std::unique_ptr<InProcMaster>,
                 std::unique_ptr<InProcP2PMaster>>
        master_;
    std::string master_address_;

    MasterTestWrapper(TestMode mode) : mode_(mode) {
        if (mode_ == TestMode::CENTRALIZED) {
            master_ = std::make_unique<InProcMaster>();
        } else {
            master_ = std::make_unique<InProcP2PMaster>();
        }
    }

    bool Start(std::optional<int> rpc_port = std::nullopt,
               std::optional<int> client_live_ttl = std::nullopt,
               std::optional<int> client_crashed_ttl = std::nullopt) {
        InProcMasterConfigBuilder builder;
        if (rpc_port) builder.set_rpc_port(rpc_port.value());
        if (client_live_ttl)
            builder.set_client_live_ttl_sec(client_live_ttl.value());
        if (client_crashed_ttl)
            builder.set_client_crashed_ttl_sec(client_crashed_ttl.value());

        InProcMasterConfig config = builder.build();

        bool success = false;
        if (mode_ == TestMode::CENTRALIZED) {
            auto& m = std::get<0>(master_);
            success = m->Start(config);
            if (success) master_address_ = m->master_address();
        } else {
            auto& m = std::get<1>(master_);
            success = m->Start(config);
            if (success) master_address_ = m->master_address();
        }
        return success;
    }

    void Stop() {
        if (mode_ == TestMode::CENTRALIZED) {
            std::get<0>(master_)->Stop();
        } else {
            std::get<1>(master_)->Stop();
        }
    }

    int rpc_port() const {
        if (mode_ == TestMode::CENTRALIZED) {
            return std::get<0>(master_)->rpc_port();
        } else {
            return std::get<1>(master_)->rpc_port();
        }
    }

    std::string master_address() const { return master_address_; }

    tl::expected<QueryClientStatusResponse, ErrorCode> QueryClientStatus(
        const UUID& client_id) const {
        QueryClientStatusRequest req;
        req.client_id = client_id;
        if (mode_ == TestMode::CENTRALIZED) {
            return std::get<0>(master_)
                ->GetWrapped()
                .GetMasterService()
                .QueryClientStatus(req);
        } else {
            return std::get<1>(master_)
                ->GetWrapped()
                .GetMasterService()
                .QueryClientStatus(req);
        }
    }

    std::vector<std::string> GetClientSegments(const UUID& client_id) const {
        tl::expected<std::vector<std::string>, ErrorCode> res;
        if (mode_ == TestMode::CENTRALIZED) {
            res = std::get<0>(master_)
                      ->GetWrapped()
                      .GetMasterService()
                      .GetClientSegments(client_id);
        } else {
            res = std::get<1>(master_)
                      ->GetWrapped()
                      .GetMasterService()
                      .GetClientSegments(client_id);
        }
        if (res) return res.value();
        return {};
    }

    std::vector<std::string> GetMasterSegments() const {
        tl::expected<std::vector<std::string>, ErrorCode> res;
        if (mode_ == TestMode::CENTRALIZED) {
            res = std::get<0>(master_)
                      ->GetWrapped()
                      .GetMasterService()
                      .GetAllSegments();
        } else {
            res = std::get<1>(master_)
                      ->GetWrapped()
                      .GetMasterService()
                      .GetAllSegments();
        }
        if (res) return res.value();
        return {};
    }

    // Unified helper to check if expected segments are visible to master
    bool CheckSegmentsMatch(
        const std::vector<std::string>& expected_segments) const {
        auto master_segments = GetMasterSegments();
        if (master_segments.size() != expected_segments.size()) {
            std::cerr << "Segment count mismatch: expected "
                      << expected_segments.size() << ", got "
                      << master_segments.size() << "\n";
            return false;
        }

        for (const auto& expected : expected_segments) {
            bool found = false;
            for (const auto& ms : master_segments) {
                if (ms.find(expected) != std::string::npos) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                std::cerr << "Segment not found: " << expected
                          << "\nCurrent Master segments:\n";
                for (const auto& ms : master_segments)
                    std::cerr << " - " << ms << "\n";
                return false;
            }
        }
        return true;
    }
};

class NonHAReconnectTest : public ::testing::TestWithParam<TestMode> {
   protected:
    TestMode mode_;
    std::unique_ptr<MasterTestWrapper> master_;
    std::shared_ptr<ClientService> client_;
    std::string local_hostname_ = "127.0.0.1:18001";
    std::vector<std::pair<void*, std::string>> allocated_buffers_;

    void SetUp() override {
        mode_ = GetParam();
        master_ = std::make_unique<MasterTestWrapper>(mode_);
    }

    void TearDown() override {
        if (client_) {
            client_->Stop();
            client_->Destroy();
            client_.reset();
        }
        if (master_) {
            master_->Stop();
            master_.reset();
        }
        for (auto& buffer : allocated_buffers_) {
            free_memory(buffer.second, buffer.first);
        }
        allocated_buffers_.clear();
    }

    void CreateClientAndMount(std::shared_ptr<ClientService>& client_out,
                              std::vector<std::string>& segments_out,
                              const std::string& local_hostname,
                              size_t num_segments = 3,
                              std::string master_addr = "") {
        if (master_addr.empty()) master_addr = master_->master_address();

        if (mode_ == TestMode::CENTRALIZED) {
            auto config = ClientConfigBuilder::build_centralized_real_client(
                local_hostname, "P2PHANDSHAKE", "tcp", std::nullopt,
                master_addr);
            auto client_opt = ClientService::Create(config);
            EXPECT_TRUE(client_opt.has_value());
            client_out = client_opt.value();

            // Wait a bit for heartbeat registration to establish
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            for (size_t i = 0; i < num_segments; ++i) {
                size_t ram_buffer_size = 16 * 1024 * 1024;  // 16MB
                void* seg_ptr =
                    allocate_buffer_allocator_memory(ram_buffer_size);
                EXPECT_NE(seg_ptr, nullptr);
                allocated_buffers_.push_back({seg_ptr, ""});

                auto mount_res =
                    std::dynamic_pointer_cast<CentralizedClientService>(
                        client_out)
                        ->MountSegment(seg_ptr, ram_buffer_size);
                EXPECT_TRUE(mount_res.has_value())
                    << toString(mount_res.error());
            }
        } else {
            // For P2P we need local_buffer_size = 0 and te_port resolving.
            std::string tiers_json = R"({"tiers": [)";
            for (size_t i = 0; i < num_segments; ++i) {
                tiers_json +=
                    R"({"type": "DRAM", "capacity": 67108864, "priority": 100})";
                if (i < num_segments - 1) tiers_json += ",";
            }
            tiers_json += "]}";

            auto config = ClientConfigBuilder::build_p2p_real_client(
                local_hostname, "P2PHANDSHAKE", "tcp", std::nullopt,
                master_addr, tiers_json, 0, nullptr, "", 0);
            auto client_opt = ClientService::Create(config);
            EXPECT_TRUE(client_opt.has_value());
            client_out = client_opt.value();
        }

        // Wait for segments to be fully registered in Master.
        // P2P segments are registered asynchronously during InitStorage.
        bool ready = false;
        for (int i = 0; i < 100; ++i) {
            segments_out =
                master_->GetClientSegments(client_out->GetClientID());
            if (segments_out.size() == num_segments) {
                ready = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        EXPECT_TRUE(ready)
            << "Timed out waiting for segments to register at Master. "
            << "Expected " << num_segments << ", got " << segments_out.size();
    }
};

// When master restarts, client should be able to reconnect and remount segments
TEST_P(NonHAReconnectTest, MasterRestartAndRecover) {
    ASSERT_TRUE(master_->Start());

    std::vector<std::string> segments;
    CreateClientAndMount(client_, segments, local_hostname_);

    tl::expected<QueryClientStatusResponse, ErrorCode> res;
    res = master_->QueryClientStatus(client_->GetClientID());
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().status, ClientStatus::HEALTH)
        << "Initial registration to master failed";

    // verify segments are visible
    ASSERT_TRUE(master_->CheckSegmentsMatch(segments))
        << "Segments doesn't match when initializing";

    int rpc_port = master_->rpc_port();

    // Stop master, wait for ping failures to accumulate
    master_->Stop();
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Restart master on the same ports
    ASSERT_TRUE(master_->Start(rpc_port));

    // Wait until remount is reflected in master
    int max_attempts = 100;
    for (int i = 0; i < max_attempts; ++i) {  // up to 50s
        res = master_->QueryClientStatus(client_->GetClientID());
        if (res.has_value() && res.value().status == ClientStatus::HEALTH) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().status, ClientStatus::HEALTH)
        << "Client did not reconnect and remount in time";

    // verify segments re-mounted and visible
    ASSERT_TRUE(master_->CheckSegmentsMatch(segments))
        << "Segments doesn't match after recovery";
}

// When client disconnect, it can't be seen in master.
// When client reconnect, it can be seen in master.
TEST_P(NonHAReconnectTest, ClientDisconnectAndRecover) {
    ASSERT_TRUE(master_->Start(std::nullopt, 2, 20));

    std::shared_ptr<ClientService> client1, client2;
    std::vector<std::string> segments1, segments2;
    std::string host1 = "127.0.0.1:18001";
    std::string host2 = "127.0.0.1:18002";

    CreateClientAndMount(client1, segments1, host1);
    CreateClientAndMount(client2, segments2, host2);
    std::vector<std::string> all_segments = segments1;
    all_segments.insert(all_segments.end(), segments2.begin(), segments2.end());

    tl::expected<QueryClientStatusResponse, ErrorCode> res;
    res = master_->QueryClientStatus(client1->GetClientID());
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().status, ClientStatus::HEALTH)
        << "Initial registration to master failed";

    ASSERT_TRUE(master_->CheckSegmentsMatch(all_segments))
        << "Initial segments match failed";

    // Pause client1 heartbeats
    client1->StopHeartbeat();

    // Wait for disconnected state (live TTL = 2s)
    std::this_thread::sleep_for(std::chrono::seconds(3));
    res = master_->QueryClientStatus(client1->GetClientID());
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().status, ClientStatus::DISCONNECTION)
        << "Master did not transition client1 to DISCONNECTED state";

    // verify client1 segments are NOT seen, but client2 segments are still seen
    ASSERT_FALSE(master_->CheckSegmentsMatch(segments1))
        << "Client1 segments should be hidden";
    ASSERT_TRUE(master_->CheckSegmentsMatch(segments2))
        << "Client2 segments should remain visible";

    // Resume heartbeats by making the manual heartbeat calls
    HeartbeatRequest req;
    req.client_id = client1->GetClientID();
    auto hb_res = client1->GetMasterClient().Heartbeat(req);
    ASSERT_TRUE(hb_res.has_value()) << "Heartbeat failed";
    // In mooncake-store design, a heartbeat from a DISCONNECTION client
    // immediately recovers its state to HEALTH and returns HEALTH.
    ASSERT_EQ(hb_res.value().status, ClientStatus::HEALTH)
        << "Client should have been recovered to HEALTH state by heartbeat";

    res = master_->QueryClientStatus(client1->GetClientID());
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().status, ClientStatus::HEALTH)
        << "Client did not recover to HEALTH state after manually sending "
           "heartbeat";

    // verify client1 segments are restored
    ASSERT_TRUE(master_->CheckSegmentsMatch(all_segments))
        << "Client1 segments not fully visible after recovery";

    client1->Destroy();
    client2->Destroy();
}

// When client crashes, it will be evicted from master.
TEST_P(NonHAReconnectTest, ClientCrash) {
    ASSERT_TRUE(master_->Start(std::nullopt, 2, 8));

    std::shared_ptr<ClientService> client1, client2;
    std::vector<std::string> segments1, segments2;
    std::string host1 = "127.0.0.1:18001";
    std::string host2 = "127.0.0.1:18002";

    CreateClientAndMount(client1, segments1, host1);
    CreateClientAndMount(client2, segments2, host2);
    std::vector<std::string> all_segments = segments1;
    all_segments.insert(all_segments.end(), segments2.begin(), segments2.end());

    tl::expected<QueryClientStatusResponse, ErrorCode> res;
    res = master_->QueryClientStatus(client1->GetClientID());
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().status, ClientStatus::HEALTH)
        << "Initial registration to master failed";

    // verify visibility
    ASSERT_TRUE(master_->CheckSegmentsMatch(all_segments));

    // Pause client1 heartbeats
    client1->Stop();

    // Wait for crash state (crashed TTL = 4s)
    std::this_thread::sleep_for(std::chrono::seconds(3));
    res = master_->QueryClientStatus(client1->GetClientID());
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().status, ClientStatus::DISCONNECTION)
        << "Client should have been disconnected";

    std::this_thread::sleep_for(std::chrono::seconds(6));
    res = master_->QueryClientStatus(client1->GetClientID());
    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res.value().status, ClientStatus::UNDEFINED)
        << "Master should delete client after crashed_ttl expires";

    // Now send a manual heartbeat and expect UNDEFINED
    HeartbeatRequest req;
    req.client_id = client1->GetClientID();
    auto hb_res = client1->GetMasterClient().Heartbeat(req);
    ASSERT_TRUE(hb_res.has_value() &&
                hb_res.value().status == ClientStatus::UNDEFINED)
        << "Master should report UNDEFINED for an evicted client";

    // verify fully evicted
    ASSERT_FALSE(master_->CheckSegmentsMatch(segments1))
        << "Client1 segments should be completely removed";
    ASSERT_TRUE(master_->CheckSegmentsMatch(segments2))
        << "Client2 segments should remain visible";

    client1->Destroy();
    client2->Destroy();
}

INSTANTIATE_TEST_SUITE_P(AllModes, NonHAReconnectTest,
                         ::testing::Values(TestMode::CENTRALIZED,
                                           TestMode::P2P));

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    mooncake::init_ylt_log_level();
    return RUN_ALL_TESTS();
}
