#include <glog/logging.h>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <asio.hpp>
#include <async_simple/coro/SyncAwait.h>

#include "p2p/client/data_manager/v2/data_manager_v2.h"
#include "utils.h"

namespace mooncake::v2 {
namespace {
using namespace std::chrono_literals;

class ManualClock final : public Clock {
   public:
    time_point Now() const override {
        return time_point(std::chrono::milliseconds(now_ms_.load()));
    }
    void Advance(std::chrono::milliseconds delta) {
        now_ms_.fetch_add(delta.count());
    }

   private:
    std::atomic<int64_t> now_ms_{3600000};
};

// A legacy TCP READ peer that withholds the response until Release(). The
// transfer really submits through TE, but its lifetime is controlled by the
// test rather than by the speed of a local memcpy.
class DelayedReadPeer {
   public:
    explicit DelayedReadPeer(std::string payload)
        : work_(asio::make_work_guard(io_)),
          acceptor_(io_, {asio::ip::address_v4::loopback(), 0}),
          socket_(io_),
          payload_(std::move(payload)),
          request_seen_(request_.get_future()) {
        acceptor_.async_accept(socket_, [this](const asio::error_code& ec) {
            if (ec) return;
            asio::async_read(socket_, asio::buffer(header_),
                             [this](const asio::error_code& error, size_t) {
                                 if (error) return;
                                 received_ = true;
                                 request_.set_value();
                                 SendIfReleased();
                             });
        });
        worker_ = std::thread([this] { io_.run(); });
    }

    ~DelayedReadPeer() {
        io_.stop();
        worker_.join();
    }

    uint16_t Port() const { return acceptor_.local_endpoint().port(); }
    std::string Endpoint() const {
        return "127.0.0.1:" + std::to_string(Port());
    }
    bool WaitForRequest() {
        return request_seen_.wait_for(3s) == std::future_status::ready;
    }
    void Release(bool fail = false) {
        asio::post(io_, [this, fail] {
            released_ = true;
            fail_ = fail;
            SendIfReleased();
        });
    }

   private:
    void SendIfReleased() {
        if (!received_ || !released_ || sent_) return;
        sent_ = true;
        if (fail_) {
            asio::error_code ignored;
            socket_.close(ignored);
            return;
        }
        asio::async_write(socket_, asio::buffer(payload_),
                          [](const asio::error_code&, size_t) {});
    }

    asio::io_context io_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    asio::ip::tcp::acceptor acceptor_;
    asio::ip::tcp::socket socket_;
    // SessionHeader: uint64_t size, uint64_t address, uint8_t opcode + padding.
    std::array<char, 24> header_{};
    std::string payload_;
    std::promise<void> request_;
    std::future<void> request_seen_;
    bool received_ = false;
    bool released_ = false;
    bool sent_ = false;
    bool fail_ = false;
    std::thread worker_;
};

enum class LeaseState {
    kLive,
    kExpired,
    kScanned,
    kReplaced,
    kTransferFailed,
    kStopping
};

}  // namespace

class DataManagerTransferTest : public ::testing::TestWithParam<LeaseState> {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag once;
        std::call_once(once, [] {
            google::InitGoogleLogging("DataManagerTransferTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        if (std::getenv("MC_USE_TENT") || std::getenv("MC_USE_TEV1")) {
            GTEST_SKIP() << "This test uses the Classic TE TCP transport";
        }
        engine_ = std::make_shared<TransferEngine>(false);
        ASSERT_EQ(
            engine_->init("P2PHANDSHAKE",
                          "127.0.0.1:" + std::to_string(getFreeTcpPort())),
            0);
        ASSERT_NE(engine_->installTransport("tcp", nullptr), nullptr);

        Json::Value tiers;
        Json::Value tier;
        tier["type"] = "DRAM";
        tier["capacity"] = 8u << 20;
        tier["priority"] = 10;
        tiers["tiers"].append(tier);
        auto config = ParseDataManagerV2Config(tiers, LocalTransferConfig{},
                                               KeyLeaseConfig{});
        ASSERT_TRUE(config.has_value());
        config->local_transfer.local_memcpy_async_worker_num = 0;
        config->local_transfer.te_async_poll_worker_num = 1;
        config->stop_drain_timeout = 20ms;
        config->key_lease.duration_ms = 1000;
        config->key_lease.scan_interval_ms = 3600000;
        clock_ = std::make_shared<ManualClock>();
        manager_ = std::make_unique<DataManagerV2>(
            *config, engine_, MetadataCallbacks{}, nullptr, nullptr, clock_);
        ASSERT_TRUE(manager_->Init().has_value());
    }

    size_t Usage() const { return manager_->GetTierViews().front().usage; }

    std::shared_ptr<TransferEngine> engine_;
    std::shared_ptr<ManualClock> clock_;
    std::unique_ptr<DataManagerV2> manager_;
};

TEST_P(DataManagerTransferTest, ReverseWriteOwnsTargetUntilTransferDrains) {
    const std::string key = "reverse-write";
    const std::string payload(4096, 'x');
    DelayedReadPeer peer(payload);
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = peer.Endpoint();
    desc->protocol = "tcp";
    desc->tcp_data_port = peer.Port();
    desc->tcp_proto_version = 1;
    ASSERT_EQ(engine_->getMetadata()->addLocalSegment(42, peer.Endpoint(),
                                                      std::move(desc)),
              0);
    const std::vector<RemoteBufferDesc> sources = {
        {peer.Endpoint(), 0x1000, payload.size()}};
    auto completed = std::async(std::launch::async, [&] {
        return async_simple::coro::syncAwait(
            manager_->WriteRemoteDataAsync(key, sources, std::nullopt));
    });
    // Release before the future's destructor waits, including ASSERT failures.
    struct ReleasePeer {
        DelayedReadPeer& peer;
        ~ReleasePeer() { peer.Release(); }
    } release{peer};
    ASSERT_TRUE(peer.WaitForRequest());
    const size_t retained = Usage();
    EXPECT_GE(retained, payload.size());
    EXPECT_FALSE(manager_->Exist(key));

    if (GetParam() == LeaseState::kExpired ||
        GetParam() == LeaseState::kScanned ||
        GetParam() == LeaseState::kReplaced) {
        clock_->Advance(1001ms);
        if (GetParam() == LeaseState::kScanned ||
            GetParam() == LeaseState::kReplaced) {
            manager_->DrainForTest();
        }
        // The expiry scanner may drop the claim, but must not own the block
        // being written by TE. Its usage cannot disappear before completion.
        EXPECT_EQ(Usage(), retained);
    }
    std::optional<PreWriteResponse> replacement;
    if (GetParam() == LeaseState::kReplaced) {
        auto reserved = manager_->PreWrite(key, payload.size(), std::nullopt);
        ASSERT_TRUE(reserved.has_value());
        replacement = reserved.value();
        EXPECT_EQ(Usage(), retained * 2);
    }

    std::optional<std::future<void>> stopping;
    if (GetParam() == LeaseState::kStopping) {
        stopping.emplace(
            std::async(std::launch::async, [&] { manager_->Stop(); }));
        EXPECT_EQ(stopping->wait_for(100ms), std::future_status::timeout);
        EXPECT_EQ(Usage(), retained);
    }
    EXPECT_EQ(completed.wait_for(0ms), std::future_status::timeout);
    peer.Release(GetParam() == LeaseState::kTransferFailed);
    EXPECT_EQ(completed.wait_for(3s), std::future_status::ready);
    auto result = completed.get();
    if (GetParam() == LeaseState::kLive) {
        ASSERT_TRUE(result.has_value()) << toString(result.error());
        EXPECT_EQ(*result, manager_->GetTierViews().front().id);
        std::string out(payload.size(), '\0');
        const std::vector<mooncake::Slice> slices = {{out.data(), out.size()}};
        auto read = manager_->Get(key, slices);
        ASSERT_TRUE(read.has_value());
        ASSERT_TRUE(read->task_handle->Wait().has_value());
        EXPECT_EQ(out, payload);
        EXPECT_TRUE(manager_->Delete(key).has_value());
    } else {
        ASSERT_FALSE(result.has_value());
        if (GetParam() == LeaseState::kStopping) {
            EXPECT_EQ(result.error(), ErrorCode::SHUTTING_DOWN);
        } else if (GetParam() == LeaseState::kTransferFailed) {
            EXPECT_EQ(result.error(), ErrorCode::TRANSFER_FAIL);
        } else if (GetParam() == LeaseState::kExpired) {
            EXPECT_EQ(result.error(), ErrorCode::LEASE_EXPIRED);
        } else {
            EXPECT_EQ(result.error(), ErrorCode::INVALID_WRITE);
        }
        EXPECT_FALSE(manager_->Exist(key));
    }
    if (replacement.has_value()) {
        // Failure of the old transfer must not consume the replacement claim.
        EXPECT_EQ(Usage(), retained);
        EXPECT_TRUE(manager_->WriteRevoke(key, replacement->write_operation_id)
                        .has_value());
    }
    if (stopping) {
        EXPECT_EQ(stopping->wait_for(2s), std::future_status::ready);
        stopping->get();
    }
    EXPECT_EQ(Usage(), 0u);
}

INSTANTIATE_TEST_SUITE_P(
    LeaseLifetime, DataManagerTransferTest,
    ::testing::Values(LeaseState::kLive, LeaseState::kExpired,
                      LeaseState::kScanned, LeaseState::kReplaced,
                      LeaseState::kTransferFailed, LeaseState::kStopping),
    [](const ::testing::TestParamInfo<LeaseState>& info) {
        switch (info.param) {
            case LeaseState::kLive:
                return "Live";
            case LeaseState::kExpired:
                return "Expired";
            case LeaseState::kScanned:
                return "Scanned";
            case LeaseState::kReplaced:
                return "Replaced";
            case LeaseState::kStopping:
                return "Stopping";
            case LeaseState::kTransferFailed:
                return "TransferFailed";
        }
        return "Unknown";
    });

}  // namespace mooncake::v2
