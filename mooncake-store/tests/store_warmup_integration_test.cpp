#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "client_buffer.h"
#include "client_service.h"
#include "test_server_helpers.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "types.h"
#include "utils.h"

namespace mooncake::test {
namespace {

constexpr size_t kProbeSize = 64;
constexpr size_t kProbeAllocatorSize = 4096;
constexpr size_t kSegmentSize = 4 * 1024 * 1024;
constexpr auto kTransferTimeout = std::chrono::seconds(5);

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        if (const char* old_value = std::getenv(name)) {
            old_value_ = old_value;
        }
        setenv(name, value, 1);
    }

    ~ScopedEnvVar() {
        if (old_value_) {
            setenv(name_.c_str(), old_value_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> old_value_;
};

struct ClientRuntime {
    std::shared_ptr<TransferEngine> transfer_engine;
    std::shared_ptr<Client> client;
    std::shared_ptr<ClientBufferAllocator> probe_allocator;
    void* segment = nullptr;
    bool probe_memory_registered = false;
};

struct ReadResult {
    bool completed = false;
    bool batch_freed = false;
    std::array<uint8_t, kProbeSize> data{};
};

std::array<uint8_t, kProbeSize> MakeTestPattern() {
    std::array<uint8_t, kProbeSize> pattern{};
    for (size_t i = 0; i < pattern.size(); ++i) {
        pattern[i] = static_cast<uint8_t>(0x40 + i);
    }
    return pattern;
}

size_t CountPatternCopies(
    const std::shared_ptr<ClientBufferAllocator>& allocator,
    const std::array<uint8_t, kProbeSize>& pattern) {
    const auto* base = static_cast<const uint8_t*>(allocator->getBase());
    size_t copies = 0;
    for (size_t offset = 0; offset + pattern.size() <= allocator->size();
         ++offset) {
        if (std::memcmp(base + offset, pattern.data(), pattern.size()) == 0) {
            ++copies;
            offset += pattern.size() - 1;
        }
    }
    return copies;
}

ReadResult ReadRemotePrefix(ClientRuntime& requester,
                            const std::string& remote_endpoint) {
    ReadResult result;
    auto buffer = requester.probe_allocator->allocate(kProbeSize);
    if (!buffer) return result;
    std::memset(buffer->ptr(), 0, buffer->size());

    const auto target_id =
        requester.transfer_engine->openSegment(remote_endpoint);
    if (target_id == static_cast<SegmentHandle>(-1) ||
        target_id == LOCAL_SEGMENT_ID) {
        return result;
    }

    auto metadata = requester.transfer_engine->getMetadata();
    if (!metadata) return result;
    auto segment = metadata->getSegmentDescByID(target_id);
    if (!segment || segment->buffers.empty()) return result;

    const auto batch_id = requester.transfer_engine->allocateBatchID(1);
    if (batch_id == INVALID_BATCH_ID) return result;
    bool batch_release_attempted = false;
    auto release_batch = [&]() {
        if (batch_release_attempted) return;
        result.batch_freed =
            requester.transfer_engine->freeBatchID(batch_id).ok();
        batch_release_attempted = true;
    };

    Transport::TransferRequest request;
    request.opcode = Transport::TransferRequest::READ;
    request.source = buffer->ptr();
    request.target_id = target_id;
    request.target_offset = segment->buffers.front().addr;
    request.length = kProbeSize;

    auto submit =
        requester.transfer_engine->submitTransfer(batch_id, {request});
    if (!submit.ok()) {
        release_batch();
        return result;
    }

    Transport::TransferStatus status{};
    const auto deadline = std::chrono::steady_clock::now() + kTransferTimeout;
    while (std::chrono::steady_clock::now() < deadline) {
        auto get_status =
            requester.transfer_engine->getTransferStatus(batch_id, 0, status);
        if (!get_status.ok()) break;
        if (status.s == Transport::COMPLETED || status.s == Transport::FAILED ||
            status.s == Transport::INVALID) {
            result.completed = status.s == Transport::COMPLETED;
            break;
        }
        std::this_thread::yield();
    }

    release_batch();
    if (result.completed) {
        std::memcpy(result.data.data(), buffer->ptr(), result.data.size());
    }
    return result;
}

}  // namespace

class StoreWarmupIntegrationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
        const auto ports = getFreeTcpPorts(2);
        ASSERT_EQ(ports.size(), 2);
        requester_endpoint_ = "127.0.0.1:" + std::to_string(ports[0]);
        remote_endpoint_ = "127.0.0.1:" + std::to_string(ports[1]);
    }

    void TearDown() override {
        for (auto it = concurrent_remotes_.rbegin();
             it != concurrent_remotes_.rend(); ++it) {
            CleanupRuntime(*it);
        }
        for (auto it = concurrent_requesters_.rbegin();
             it != concurrent_requesters_.rend(); ++it) {
            CleanupRuntime(*it);
        }
        CleanupRuntime(remote_);
        CleanupRuntime(requester_);
        master_.Stop();
    }

    void CreateRuntime(ClientRuntime& runtime, const std::string& endpoint,
                       bool create_probe_allocator) {
        const auto separator = endpoint.rfind(':');
        ASSERT_NE(separator, std::string::npos);
        const auto port =
            static_cast<uint64_t>(std::stoul(endpoint.substr(separator + 1)));

        runtime.transfer_engine = std::make_shared<TransferEngine>(false);
        ASSERT_EQ(runtime.transfer_engine->init(P2PHANDSHAKE, endpoint,
                                                "127.0.0.1", port),
                  0);
        ASSERT_NE(runtime.transfer_engine->installTransport("tcp", nullptr),
                  nullptr);

        auto client =
            Client::Create(endpoint, P2PHANDSHAKE, "tcp", std::nullopt,
                           master_.master_address(), runtime.transfer_engine);
        ASSERT_TRUE(client.has_value());
        runtime.client = std::move(*client);

        if (!create_probe_allocator) return;
        runtime.probe_allocator =
            ClientBufferAllocator::create(kProbeAllocatorSize, "tcp");
        ASSERT_NE(runtime.probe_allocator, nullptr);
        ASSERT_TRUE(
            runtime.client
                ->RegisterLocalMemory(runtime.probe_allocator->getBase(),
                                      runtime.probe_allocator->size(), "cpu:0",
                                      /*remote_accessible=*/false,
                                      /*update_metadata=*/false)
                .has_value());
        runtime.probe_memory_registered = true;
    }

    ::testing::AssertionResult MountSegment(
        ClientRuntime& runtime, const std::array<uint8_t, kProbeSize>& prefix) {
        void* memory = nullptr;
        const int ret = posix_memalign(&memory, 4096, kSegmentSize);
        if (ret != 0 || memory == nullptr) {
            return ::testing::AssertionFailure()
                   << "posix_memalign failed: ret=" << ret;
        }

        std::memset(memory, 0, kSegmentSize);
        std::memcpy(memory, prefix.data(), prefix.size());
        auto mount_result =
            runtime.client->MountSegment(memory, kSegmentSize, "tcp");
        if (!mount_result.has_value()) {
            std::free(memory);
            return ::testing::AssertionFailure()
                   << "Client::MountSegment failed: "
                   << toString(mount_result.error());
        }

        runtime.segment = memory;
        return ::testing::AssertionSuccess();
    }

    void ExpectProbeBufferReleased(ClientRuntime& runtime) {
        // TransferEngine does not expose an active-batch count. A full-size
        // allocation directly checks that all temporary probe handles returned.
        auto whole_allocator =
            runtime.probe_allocator->allocate(runtime.probe_allocator->size());
        EXPECT_TRUE(whole_allocator.has_value());
    }

    void CleanupRuntime(ClientRuntime& runtime) {
        if (runtime.client && runtime.segment) {
            EXPECT_TRUE(
                runtime.client->UnmountSegment(runtime.segment, kSegmentSize)
                    .has_value());
        }
        if (runtime.client && runtime.probe_memory_registered) {
            EXPECT_TRUE(runtime.client
                            ->unregisterLocalMemory(
                                runtime.probe_allocator->getBase(), false)
                            .has_value());
        }

        runtime.client.reset();
        runtime.probe_allocator.reset();
        runtime.transfer_engine.reset();
        runtime.probe_memory_registered = false;
        if (runtime.segment) {
            std::free(runtime.segment);
            runtime.segment = nullptr;
        }
    }

    testing::InProcMaster master_;
    ClientRuntime requester_;
    ClientRuntime remote_;
    std::vector<ClientRuntime> concurrent_requesters_;
    std::vector<ClientRuntime> concurrent_remotes_;
    std::string requester_endpoint_;
    std::string remote_endpoint_;
    ScopedEnvVar read_size_{"MC_STORE_WARMUP_READ_SIZE", "64"};
    ScopedEnvVar timeout_{"MC_STORE_WARMUP_TIMEOUT_MS", "5000"};
    ScopedEnvVar concurrency_{"MC_STORE_WARMUP_CONCURRENCY", "1"};
    ScopedEnvVar max_targets_{"MC_STORE_WARMUP_MAX_TARGETS", "0"};
};

TEST_F(StoreWarmupIntegrationTest,
       SingleClientWarmupCompletesWithoutRemoteProbe) {
    CreateRuntime(requester_, requester_endpoint_, true);
    ASSERT_NE(requester_.client, nullptr);

    std::array<uint8_t, kProbeSize> local_prefix{};
    local_prefix.fill(0x11);
    ASSERT_TRUE(MountSegment(requester_, local_prefix))
        << "requester local Segment registration failed";

    std::memset(requester_.probe_allocator->getBase(), 0xA5,
                requester_.probe_allocator->size());
    ASSERT_TRUE(
        requester_.client->warmup(requester_.probe_allocator).has_value());

    const auto* probe_bytes =
        static_cast<const uint8_t*>(requester_.probe_allocator->getBase());
    EXPECT_TRUE(std::all_of(probe_bytes, probe_bytes + kProbeSize,
                            [](uint8_t byte) { return byte == 0xA5; }));
    ExpectProbeBufferReleased(requester_);
}

TEST_F(StoreWarmupIntegrationTest, TwoClientTcpWarmupSubmitsReadProbe) {
    CreateRuntime(requester_, requester_endpoint_, true);
    CreateRuntime(remote_, remote_endpoint_, false);
    ASSERT_NE(requester_.client, nullptr);
    ASSERT_NE(remote_.client, nullptr);
    ASSERT_NE(requester_.client->getClientId(), remote_.client->getClientId());
    ASSERT_NE(requester_.client->GetTransportEndpoint(),
              remote_.client->GetTransportEndpoint());

    std::array<uint8_t, kProbeSize> requester_pattern{};
    requester_pattern.fill(0xA5);
    const auto pattern = MakeTestPattern();
    ASSERT_TRUE(MountSegment(requester_, requester_pattern))
        << "requester local Segment registration failed";
    ASSERT_TRUE(MountSegment(remote_, pattern))
        << "remote Segment registration failed";
    std::memset(requester_.probe_allocator->getBase(), 0,
                requester_.probe_allocator->size());

    ASSERT_TRUE(
        requester_.client->warmup(requester_.probe_allocator).has_value());
    EXPECT_EQ(std::memcmp(requester_.probe_allocator->getBase(), pattern.data(),
                          pattern.size()),
              0);
    ExpectProbeBufferReleased(requester_);

    // Validate that the data path remains healthy after warmup. This does not
    // assert that the TCP connection established by warmup was reused.
    const auto normal_read =
        ReadRemotePrefix(requester_, remote_.client->GetSegmentEndpoint());
    ASSERT_TRUE(normal_read.completed);
    EXPECT_TRUE(normal_read.batch_freed);
    EXPECT_EQ(normal_read.data, pattern);
    ExpectProbeBufferReleased(requester_);
}

TEST_F(StoreWarmupIntegrationTest,
       MultipleClientsWarmupRemoteSegmentsConcurrently) {
    constexpr size_t kClientsPerGroup = 2;
    ScopedEnvVar group_concurrency("MC_STORE_WARMUP_CONCURRENCY", "2");
    ScopedEnvVar group_max_targets("MC_STORE_WARMUP_MAX_TARGETS", "2");

    const auto ports = getFreeTcpPorts(2 * kClientsPerGroup);
    ASSERT_EQ(ports.size(), 2 * kClientsPerGroup);
    concurrent_requesters_.reserve(kClientsPerGroup);
    concurrent_remotes_.reserve(kClientsPerGroup);

    // Model two requesters starting while two established remote clients
    // expose readable segments through distinct loopback endpoints.
    const auto pattern = MakeTestPattern();
    for (size_t i = 0; i < kClientsPerGroup; ++i) {
        concurrent_requesters_.emplace_back();
        CreateRuntime(concurrent_requesters_.back(),
                      "127.0.0.1:" + std::to_string(ports[i]), true);
        ASSERT_NE(concurrent_requesters_.back().client, nullptr);
        std::array<uint8_t, kProbeSize> requester_pattern{};
        requester_pattern.fill(static_cast<uint8_t>(0xA0 + i));
        ASSERT_TRUE(
            MountSegment(concurrent_requesters_.back(), requester_pattern))
            << "requester local Segment registration failed: index=" << i;

        concurrent_remotes_.emplace_back();
        CreateRuntime(
            concurrent_remotes_.back(),
            "127.0.0.1:" + std::to_string(ports[kClientsPerGroup + i]), false);
        ASSERT_NE(concurrent_remotes_.back().client, nullptr);
        ASSERT_TRUE(MountSegment(concurrent_remotes_.back(), pattern))
            << "remote Segment registration failed: index=" << i;

        EXPECT_NE(concurrent_requesters_.back().client->getClientId(),
                  concurrent_remotes_.back().client->getClientId());
        EXPECT_NE(concurrent_requesters_.back().client->GetTransportEndpoint(),
                  concurrent_remotes_.back().client->GetTransportEndpoint());
    }
    ASSERT_EQ(concurrent_requesters_.size(), kClientsPerGroup);
    ASSERT_EQ(concurrent_remotes_.size(), kClientsPerGroup);

    std::vector<int> warmup_results(kClientsPerGroup, -1);
    std::vector<std::thread> startup_threads;
    startup_threads.reserve(kClientsPerGroup);
    for (size_t i = 0; i < kClientsPerGroup; ++i) {
        startup_threads.emplace_back([&, i]() {
            auto result = concurrent_requesters_[i].client->warmup(
                concurrent_requesters_[i].probe_allocator);
            warmup_results[i] = result.has_value() ? 0 : 1;
        });
    }
    for (auto& thread : startup_threads) {
        thread.join();
    }
    for (const auto& thread : startup_threads) {
        EXPECT_FALSE(thread.joinable());
    }

    for (size_t i = 0; i < kClientsPerGroup; ++i) {
        EXPECT_EQ(warmup_results[i], 0);
        EXPECT_GE(CountPatternCopies(concurrent_requesters_[i].probe_allocator,
                                     pattern),
                  1);
        ExpectProbeBufferReleased(concurrent_requesters_[i]);
    }
}

}  // namespace mooncake::test
