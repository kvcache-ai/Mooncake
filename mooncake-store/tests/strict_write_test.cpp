#include "transfer_task.h"
#include "transport/tcp_transport/tcp_transport.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <endian.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
void tcpTransportSetLaneConnectPauseHookForTest(
    bool (*hook)() noexcept) noexcept;
void tcpTransportSetSessionProgressHookForTest(
    int (*hook)(int, bool) noexcept) noexcept;
#endif

namespace {

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
std::atomic<size_t> paused_connects{0};

bool pauseStrictConnect() noexcept {
    paused_connects.fetch_add(1, std::memory_order_relaxed);
    return true;
}

struct PauseConnectScope {
    PauseConnectScope() {
        paused_connects.store(0, std::memory_order_relaxed);
        tcpTransportSetLaneConnectPauseHookForTest(pauseStrictConnect);
    }
    ~PauseConnectScope() {
        tcpTransportSetLaneConnectPauseHookForTest(nullptr);
    }
};

constexpr int kStatusDeadlineArmed = 8;
constexpr int kStatusTimeoutCommitted = 9;
constexpr int kStatusTimeoutStale = 10;
constexpr int kServerWriteAckReady = 11;
constexpr int kStatusReplayComplete = 12;
constexpr int kReplayPreviousStatusAfterArm = 3;
std::atomic<int> status_deadline_arms{0};
std::atomic<int> status_timeout_commits{0};
std::atomic<int> stale_status_expiries{0};
std::atomic<int> status_expiry_replays{0};
std::atomic<bool> release_server_ack{false};
std::atomic<bool> server_ack_wait_expired{false};

int replayHelloStatusExpiryDuringWriteAck(int event, bool detail) noexcept {
    if (event == kServerWriteAckReady) {
        // The real receiver has applied the body. Hold only its ACK until the
        // sender re-arms the timer and dispatches the queued old-phase expiry.
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!release_server_ack.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() >= deadline) {
                server_ack_wait_expired.store(true, std::memory_order_release);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    if (event == kStatusTimeoutCommitted)
        status_timeout_commits.fetch_add(1, std::memory_order_relaxed);
    if (event == kStatusTimeoutStale && detail)
        stale_status_expiries.fetch_add(1, std::memory_order_relaxed);
    if (event == kStatusReplayComplete) {
        status_expiry_replays.fetch_add(1, std::memory_order_relaxed);
        release_server_ack.store(true, std::memory_order_release);
    }
    if (event == kStatusDeadlineArmed &&
        status_deadline_arms.fetch_add(1, std::memory_order_relaxed) == 1)
        return kReplayPreviousStatusAfterArm;
    return 0;
}

struct ScopedStatusDeadlineRace {
    ScopedStatusDeadlineRace() {
        status_deadline_arms.store(0, std::memory_order_relaxed);
        status_timeout_commits.store(0, std::memory_order_relaxed);
        stale_status_expiries.store(0, std::memory_order_relaxed);
        status_expiry_replays.store(0, std::memory_order_relaxed);
        release_server_ack.store(false, std::memory_order_relaxed);
        server_ack_wait_expired.store(false, std::memory_order_relaxed);
        tcpTransportSetSessionProgressHookForTest(
            replayHelloStatusExpiryDuringWriteAck);
    }
    ~ScopedStatusDeadlineRace() {
        release_server_ack.store(true, std::memory_order_release);
        tcpTransportSetSessionProgressHookForTest(nullptr);
    }
};
#endif

class StrictWriteTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("StrictWriteTest");
        for (const auto& name :
             {"MC_USE_TENT", "MC_USE_TEV1", "MC_TCP_PROTO",
              "MC_TCP_ENABLE_CONNECTION_POOL", "MC_TCP_PROGRESS_TIMEOUT_SEC",
              "MC_TCP_LANES_PER_PEER", "MC_NUM_TCP_IO_THREADS"}) {
            const char* value = std::getenv(name);
            environment_.push_back(
                {name,
                 value ? std::optional<std::string>(value) : std::nullopt});
        }
        setenv("MC_USE_TENT", "0", 1);
        setenv("MC_USE_TEV1", "0", 1);
        setenv("MC_TCP_PROTO", "2", 1);
        setenv("MC_TCP_ENABLE_CONNECTION_POOL", "1", 1);
        setenv("MC_TCP_LANES_PER_PEER", "1", 1);
        configureTcpEnvironment();
        source_.assign(1024 * 1024, 'S');
        target_.assign(source_.size(), 0);
        sender_ = std::make_unique<TransferEngine>(false);
        receiver_ = std::make_unique<TransferEngine>(false);
        for (auto* engine : {sender_.get(), receiver_.get()}) {
            ASSERT_EQ(engine->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
            ASSERT_NE(engine->installTransport("tcp", nullptr), nullptr);
        }
        ASSERT_EQ(sender_->registerLocalMemory(source_.data(), source_.size(),
                                               "cpu:0"),
                  0);
        ASSERT_EQ(receiver_->registerLocalMemory(target_.data(), target_.size(),
                                                 "cpu:0"),
                  0);
        submitter_ = std::make_unique<TransferSubmitter>(
            *sender_, backend_, sender_->getLocalIpAndPort());
    }

    virtual void configureTcpEnvironment() {}

    void TearDown() override {
        submitter_.reset();
        sender_.reset();
        receiver_.reset();
        for (const auto& [name, value] : environment_) {
            if (value)
                setenv(name.c_str(), value->c_str(), 1);
            else
                unsetenv(name.c_str());
        }
        google::ShutdownGoogleLogging();
    }

    Replica::Descriptor descriptor(size_t offset, size_t length) const {
        Replica::Descriptor result;
        result.status = ReplicaStatus::PROCESSING;
        result.descriptor_variant =
            MemoryDescriptor{AllocatedBuffer::Descriptor{
                length, reinterpret_cast<uintptr_t>(target_.data()) + offset,
                "tcp", receiver_->getLocalIpAndPort()}};
        return result;
    }

    std::vector<Slice> slices() {
        const size_t half = source_.size() / 2;
        return {{source_.data(), half},
                {source_.data() + half, source_.size() - half}};
    }

    std::vector<std::pair<std::string, std::optional<std::string>>>
        environment_;
    std::vector<char> source_;
    std::vector<char> target_;
    std::unique_ptr<TransferEngine> sender_;
    std::unique_ptr<TransferEngine> receiver_;
    std::shared_ptr<StorageBackend> backend_;
    std::unique_ptr<TransferSubmitter> submitter_;
};

TEST_F(StrictWriteTest, OrdinaryDescriptorSelectsStrictAcknowledgedWrite) {
    auto replica = descriptor(0, target_.size());
    replica.get_memory_descriptor().strict_tcp_instance =
        receiver_->getTcpInstanceId();
    auto fragments = slices();
    auto future =
        submitter_->submit(replica, fragments, TransferRequest::WRITE);
    ASSERT_TRUE(future);
    EXPECT_EQ(future->get(), ErrorCode::OK);
    EXPECT_EQ(target_, source_);
}

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
TEST_F(StrictWriteTest, QueuedHelloStatusExpiryCannotCancelTheWriteAckPhase) {
    ScopedStatusDeadlineRace race;
    // One slice gives one strict Hello followed by one WRITE ACK phase.
    std::vector<Slice> fragments{{source_.data(), source_.size()}};
    auto future =
        submitter_->submitStrictWrite(descriptor(0, target_.size()), fragments,
                                      receiver_->getTcpInstanceId());
    ASSERT_TRUE(future);
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!future->isReady() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    const bool completed_without_retirement = future->isReady();
    if (!completed_without_retirement) {
        release_server_ack.store(true, std::memory_order_release);
        EXPECT_TRUE(sender_->retireStrictWrites());
    }
    ASSERT_TRUE(completed_without_retirement);
    EXPECT_EQ(status_expiry_replays.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(stale_status_expiries.load(std::memory_order_relaxed), 1);
    EXPECT_EQ(status_timeout_commits.load(std::memory_order_relaxed), 0);
    EXPECT_FALSE(server_ack_wait_expired.load(std::memory_order_relaxed));
    EXPECT_EQ(future->get(), ErrorCode::OK);
    EXPECT_EQ(target_, source_);

    // A discarded old timer must also leave the verified connection usable.
    std::fill(target_.begin(), target_.end(), 0);
    auto next =
        submitter_->submitStrictWrite(descriptor(0, target_.size()), fragments,
                                      receiver_->getTcpInstanceId());
    ASSERT_TRUE(next);
    EXPECT_EQ(next->get(), ErrorCode::OK);
    EXPECT_EQ(target_, source_);
}
#endif

TEST_F(StrictWriteTest, OrdinaryCapabilityCannotFallBackToLocalMemcpy) {
    auto replica = descriptor(0, target_.size());
    auto& memory = replica.get_memory_descriptor();
    memory.buffer_descriptor.transport_endpoint_ = sender_->getLocalIpAndPort();
    auto wrong = sender_->getTcpInstanceId();
    wrong[0] = wrong[0] == '0' ? '1' : '0';
    memory.strict_tcp_instance = wrong;
    auto fragments = slices();
    auto future =
        submitter_->submit(replica, fragments, TransferRequest::WRITE);
    ASSERT_TRUE(future);
    EXPECT_EQ(future->get(), ErrorCode::TRANSFER_FAIL);
    EXPECT_TRUE(std::all_of(target_.begin(), target_.end(),
                            [](char value) { return value == 0; }));
}

TEST_F(StrictWriteTest, OrdinaryBatchPreservesStrictInstanceValidation) {
    const size_t half = source_.size() / 2;
    std::vector<Replica::Descriptor> replicas{descriptor(0, half),
                                              descriptor(half, half)};
    const auto instance = receiver_->getTcpInstanceId();
    for (auto& replica : replicas) {
        replica.get_memory_descriptor().strict_tcp_instance = instance;
    }
    std::vector<std::vector<Slice>> fragments{{{source_.data(), half}},
                                              {{source_.data() + half, half}}};
    auto future =
        submitter_->submit_batch(replicas, fragments, TransferRequest::WRITE);
    ASSERT_TRUE(future);
    EXPECT_EQ(future->get(), ErrorCode::OK);
    EXPECT_EQ(target_, source_);
}

TEST_F(StrictWriteTest, OrdinaryBatchSettlesEveryInstanceAfterPartialFailure) {
    const size_t half = source_.size() / 2;
    std::vector<Replica::Descriptor> replicas{descriptor(0, half),
                                              descriptor(half, half)};
    const auto instance = receiver_->getTcpInstanceId();
    auto wrong = instance;
    wrong[0] = wrong[0] == '0' ? '1' : '0';
    replicas[0].get_memory_descriptor().strict_tcp_instance = wrong;
    replicas[1].get_memory_descriptor().strict_tcp_instance = instance;
    std::vector<std::vector<Slice>> fragments{{{source_.data(), half}},
                                              {{source_.data() + half, half}}};
    auto future =
        submitter_->submit_batch(replicas, fragments, TransferRequest::WRITE);
    ASSERT_TRUE(future);
    EXPECT_EQ(future->get(), ErrorCode::TRANSFER_FAIL);
    EXPECT_TRUE(std::all_of(target_.begin(), target_.begin() + half,
                            [](char value) { return value == 0; }));
    EXPECT_TRUE(std::equal(target_.begin() + half, target_.end(),
                           source_.begin() + half));
}

TEST_F(StrictWriteTest, OrdinaryRangeWritePreservesStrictInstanceValidation) {
    const size_t half = source_.size() / 2;
    auto replica = descriptor(0, target_.size());
    auto wrong = receiver_->getTcpInstanceId();
    wrong[0] = wrong[0] == '0' ? '1' : '0';
    replica.get_memory_descriptor().strict_tcp_instance = wrong;
    std::vector<Slice> fragments{{source_.data(), half}};
    auto future = submitter_->submitRangeWrite(replica, fragments, half);
    ASSERT_TRUE(future);
    EXPECT_EQ(future->get(), ErrorCode::TRANSFER_FAIL);
    EXPECT_TRUE(std::all_of(target_.begin(), target_.end(),
                            [](char value) { return value == 0; }));
}

TEST_F(StrictWriteTest, AppliesAllFragmentsAndReusesVerifiedConnections) {
    auto fragments = slices();
    const auto instance = receiver_->getTcpInstanceId();
    ASSERT_EQ(instance.size(), 32u);
    for (int attempt = 0; attempt < 3; ++attempt) {
        std::fill(target_.begin(), target_.end(), 0);
        auto future = submitter_->submitStrictWrite(
            descriptor(0, target_.size()), fragments, instance);
        ASSERT_TRUE(future);
        EXPECT_EQ(future->get(), ErrorCode::OK);
        EXPECT_EQ(target_, source_);
    }
}

TEST_F(StrictWriteTest, BoundListenerRejectsUnverifiedWritesAndAllowsReads) {
    std::vector<char> backing(source_.size(), 0);
    auto target = std::make_unique<TransferEngine>(false);
    ASSERT_EQ(target->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    ASSERT_NE(target->installTransport("tcp", nullptr), nullptr);
    const auto segment = sender_->openSegment(target->getLocalIpAndPort());
    const auto peer = sender_->getMetadata()->getSegmentDescByID(segment);
    ASSERT_TRUE(peer);
    asio::io_context io;
    asio::ip::tcp::resolver resolver(io);
    const auto endpoints = resolver.resolve(
        peer->tcp_data_host, std::to_string(peer->tcp_data_port));
    asio::ip::tcp::socket socket(io);
    // A connection may already be open when the fresh listener opts in.
    asio::connect(socket, endpoints);
    ASSERT_TRUE(target->bindTcpMasterInstance("bound-write-test"));
    ASSERT_EQ(
        target->registerLocalMemory(backing.data(), backing.size(), "cpu:0"),
        0);

    struct Header {
        uint64_t size;
        uint64_t addr;
        uint8_t opcode;
    };
    static_assert(sizeof(Header) == 24);
    // The third attempt shares a socket with a successful Hello, just as an
    // ordinary request can follow strict work on a reused connection lane.
    for (int mode : {0, 1, 2}) {
        const bool v2 = mode != 0;
        if (!socket.is_open()) asio::connect(socket, endpoints);
        if (mode == 2) {
            Header hello{};
            hello.opcode = 0xfe;
            const auto instance = target->getTcpInstanceId();
            ASSERT_EQ(instance.size(), 32u);
            const std::array<asio::const_buffer, 2> hello_buffers{
                asio::buffer(&hello, sizeof(hello)), asio::buffer(instance)};
            asio::write(socket, hello_buffers);
            socket.non_blocking(true);
            std::array<char, 40> hello_reply{};
            size_t received = 0;
            asio::error_code error;
            const auto deadline =
                std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (received < hello_reply.size() &&
                   std::chrono::steady_clock::now() < deadline) {
                received += socket.read_some(
                    asio::buffer(hello_reply.data() + received,
                                 hello_reply.size() - received),
                    error);
                if (error && error != asio::error::would_block &&
                    error != asio::error::try_again)
                    break;
                if (error)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            ASSERT_EQ(received, hello_reply.size()) << error.message();
            uint64_t status = 0;
            std::memcpy(&status, hello_reply.data(), sizeof(status));
            ASSERT_EQ(le64toh(status), 0x4D43545600000000ull);
            ASSERT_EQ(std::string(hello_reply.data() + sizeof(status), 32),
                      instance);
            socket.non_blocking(false);
        }
        Header header{};
        header.size = htole64(64);
        header.addr = htole64(reinterpret_cast<uintptr_t>(backing.data()));
        header.opcode =
            static_cast<uint8_t>(TransferRequest::WRITE) | (v2 ? 0x80 : 0);
        std::array<char, 64> payload{};
        payload.fill('L');
        const std::array<asio::const_buffer, 2> buffers{
            asio::buffer(&header, sizeof(header)), asio::buffer(payload)};
        asio::error_code error;
        asio::write(socket, buffers, error);
        // A v1 sender can finish its local send before rejection. Confirm the
        // receiver closes the stream and never applies the valid-address body.
        socket.non_blocking(true);
        bool closed = false;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        std::array<char, 16> reply{};
        while (std::chrono::steady_clock::now() < deadline) {
            socket.read_some(asio::buffer(reply), error);
            if (error == asio::error::eof ||
                error == asio::error::connection_reset) {
                closed = true;
                break;
            }
            ASSERT_TRUE(!error || error == asio::error::would_block ||
                        error == asio::error::try_again)
                << error.message();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_TRUE(closed)
            << "ordinary WRITE stream was not rejected, mode=" << mode;
        EXPECT_TRUE(std::all_of(backing.begin(), backing.end(),
                                [](char value) { return value == 0; }));
        socket.close(error);
    }

    auto replica = descriptor(0, backing.size());
    auto& handle = replica.get_memory_descriptor().buffer_descriptor;
    handle.buffer_address_ = reinterpret_cast<uintptr_t>(backing.data());
    handle.transport_endpoint_ = target->getLocalIpAndPort();
    auto fragments = slices();
    auto written = submitter_->submitStrictWrite(replica, fragments,
                                                 target->getTcpInstanceId());
    ASSERT_TRUE(written);
    ASSERT_EQ(written->get(), ErrorCode::OK);
    EXPECT_EQ(backing, source_);

    // A different engine's ordinary READ has no prior Hello on its connection.
    TransferSubmitter reader(*receiver_, backend_,
                             receiver_->getLocalIpAndPort());
    std::vector<Slice> destination{{target_.data(), target_.size()}};
    auto read = reader.submit(replica, destination, TransferRequest::READ);
    ASSERT_TRUE(read);
    ASSERT_EQ(read->get(), ErrorCode::OK);
    EXPECT_EQ(target_, source_);
    ASSERT_TRUE(target->retireStrictWrites());
}

TEST_F(StrictWriteTest,
       ListenerMasterBindingIsImmutableAndCannotReopenRetired) {
    // Binding must precede any memory exposure. The fixture receiver has
    // already published its direct-TE target region for transport-only tests.
    auto listener_engine = std::make_unique<TransferEngine>(false);
    ASSERT_EQ(listener_engine->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    ASSERT_NE(listener_engine->installTransport("tcp", nullptr), nullptr);
    const auto listener = listener_engine->getTcpInstanceId();
    ASSERT_FALSE(listener.empty());
    EXPECT_FALSE(listener_engine->bindTcpMasterInstance(""));
    EXPECT_TRUE(listener_engine->bindTcpMasterInstance("master-instance-A"));
    EXPECT_TRUE(listener_engine->bindTcpMasterInstance("master-instance-A"));
    EXPECT_FALSE(listener_engine->bindTcpMasterInstance("master-instance-B"));
    EXPECT_EQ(listener_engine->getTcpInstanceId(), listener);
    ASSERT_TRUE(listener_engine->retireStrictWrites());
    EXPECT_FALSE(listener_engine->bindTcpMasterInstance("master-instance-A"));
    EXPECT_FALSE(listener_engine->bindTcpMasterInstance("master-instance-B"));
}

TEST_F(StrictWriteTest, WrongIncarnationNeverSendsPayload) {
    auto fragments = slices();
    auto wrong = receiver_->getTcpInstanceId();
    ASSERT_EQ(wrong.size(), 32u);
    wrong[0] = wrong[0] == '0' ? '1' : '0';
    auto future = submitter_->submitStrictWrite(descriptor(0, target_.size()),
                                                fragments, wrong);
    ASSERT_TRUE(future);
    EXPECT_EQ(future->get(), ErrorCode::TRANSFER_FAIL);
    EXPECT_TRUE(std::all_of(target_.begin(), target_.end(),
                            [](char value) { return value == 0; }));
}

TEST_F(StrictWriteTest, RejectsLegacyBeforeSubmitting) {
    auto fragments = slices();
    setenv("MC_TCP_PROTO", "1", 1);
    EXPECT_FALSE(submitter_->submitStrictWrite(descriptor(0, target_.size()),
                                               fragments,
                                               receiver_->getTcpInstanceId()));
    EXPECT_TRUE(std::all_of(target_.begin(), target_.end(),
                            [](char value) { return value == 0; }));
}

TEST_F(StrictWriteTest, IndependentPieceOutcomesSurvivePeerFailure) {
    const auto half = source_.size() / 2;
    std::vector<Slice> first{{source_.data(), half}};
    std::vector<Slice> second{{source_.data() + half, half}};
    const auto instance = receiver_->getTcpInstanceId();
    auto wrong = instance;
    wrong[0] = wrong[0] == '0' ? '1' : '0';
    auto failed =
        submitter_->submitStrictWrite(descriptor(0, half), first, wrong);
    auto applied =
        submitter_->submitStrictWrite(descriptor(half, half), second, instance);
    ASSERT_TRUE(failed);
    ASSERT_TRUE(applied);
    EXPECT_EQ(failed->get(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(applied->get(), ErrorCode::OK);
    EXPECT_TRUE(std::all_of(target_.begin(), target_.begin() + half,
                            [](char value) { return value == 0; }));
    EXPECT_TRUE(std::equal(target_.begin() + half, target_.end(),
                           source_.begin() + half));
}

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
TEST_F(StrictWriteTest,
       ConnectDeadlineTerminatesPausedConnectionBeforePayload) {
    setenv("MC_TCP_PROGRESS_TIMEOUT_SEC", "1", 1);
    // The injected pause concerns the data connection, not metadata discovery.
    ASSERT_NE(sender_->openSegment(receiver_->getLocalIpAndPort()),
              static_cast<uint64_t>(ERR_INVALID_ARGUMENT));
    PauseConnectScope pause;
    auto fragments = slices();
    const auto begin = std::chrono::steady_clock::now();
    auto future =
        submitter_->submitStrictWrite(descriptor(0, target_.size()), fragments,
                                      receiver_->getTcpInstanceId());
    ASSERT_TRUE(future);
    EXPECT_EQ(future->get(), ErrorCode::TRANSFER_FAIL);
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    EXPECT_GT(paused_connects.load(std::memory_order_relaxed), 0u);
    EXPECT_GE(elapsed, std::chrono::milliseconds(900));
    EXPECT_LT(elapsed, std::chrono::seconds(8));
    EXPECT_TRUE(std::all_of(target_.begin(), target_.end(),
                            [](char value) { return value == 0; }));
}

std::atomic<size_t> allocation_test_body_starts{0};

int observeAllocationTestBody(int event, bool) noexcept {
    if (event == 7) {
        allocation_test_body_starts.fetch_add(1, std::memory_order_relaxed);
    }
    return 0;
}

class StrictWriteAllocationTest : public StrictWriteTest {
   protected:
    void configureTcpEnvironment() override {
        setenv("MC_NUM_TCP_IO_THREADS", "2", 1);
        setenv("MC_TCP_LANES_PER_PEER", "2", 1);
        setenv("MC_TCP_PROGRESS_TIMEOUT_SEC", "1", 1);
    }

    void TearDown() override {
        tcpTransportFailNextAllocationForTest(TcpAllocationFailurePoint::NONE);
        tcpTransportSetSessionProgressHookForTest(nullptr);
        StrictWriteTest::TearDown();
    }

    void checkFailure(TcpAllocationFailurePoint point, bool verified = false,
                      bool header_failure = false) {
        // Resolve metadata before the data-path injection, so discovery cannot
        // consume the test deadline or be mistaken for the injected fault.
        ASSERT_NE(sender_->openSegment(receiver_->getLocalIpAndPort()),
                  static_cast<uint64_t>(ERR_INVALID_ARGUMENT));
        std::vector<Slice> fragments{{source_.data(), source_.size()}};
        const auto instance = receiver_->getTcpInstanceId();
        if (verified) {
            auto warm_fragments = slices();
            auto warmup = submitter_->submitStrictWrite(
                descriptor(0, target_.size()), warm_fragments, instance);
            ASSERT_TRUE(warmup);
            ASSERT_EQ(warmup->get(), ErrorCode::OK);
            ASSERT_EQ(target_, source_);
            std::fill(target_.begin(), target_.end(), 0);
        }
        allocation_test_body_starts.store(0, std::memory_order_relaxed);
        if (header_failure) {
            tcpTransportSetSessionProgressHookForTest(
                observeAllocationTestBody);
        }
        tcpTransportFailNextAllocationForTest(point);
        EXPECT_FALSE(tcpTransportAllocationFailureConsumedForTest());
        auto future = submitter_->submitStrictWrite(
            descriptor(0, target_.size()), fragments, instance);
        ASSERT_TRUE(future);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!future->isReady() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        const bool completed_without_retirement = future->isReady();
        EXPECT_TRUE(completed_without_retirement);
        EXPECT_TRUE(tcpTransportAllocationFailureConsumedForTest());
        if (completed_without_retirement) {
            EXPECT_EQ(future->get(), ErrorCode::TRANSFER_FAIL);
            // Keep the batch and its source alive while observing late header
            // callbacks. Retirement is cleanup, never the completion oracle.
            const auto at_completion = target_;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            EXPECT_EQ(target_, at_completion);
            if (header_failure) {
                EXPECT_EQ(
                    allocation_test_body_starts.load(std::memory_order_relaxed),
                    0u);
                EXPECT_TRUE(std::all_of(target_.begin(), target_.end(),
                                        [](char value) { return value == 0; }));
            }
        }
        // Even a regressed implementation keeps source and batch memory alive
        // until all asynchronous handlers have drained.
        EXPECT_TRUE(sender_->retireStrictWrites());
    }
};

TEST_F(StrictWriteAllocationTest, PostFailureDrainsAcceptedQueue) {
    checkFailure(TcpAllocationFailurePoint::POST_GROUP_PUMP);
}

TEST_F(StrictWriteAllocationTest, PumpFailureDoesNotStrandScheduledWork) {
    checkFailure(TcpAllocationFailurePoint::PUMP_HANDLER);
}

TEST_F(StrictWriteAllocationTest, ConnectDispatchFailureDoesNotStrandLane) {
    checkFailure(TcpAllocationFailurePoint::DISPATCH_CONNECT);
}

TEST_F(StrictWriteAllocationTest, SessionDispatchFailureDoesNotStrandLane) {
    checkFailure(TcpAllocationFailurePoint::DISPATCH_SESSION, true);
}

TEST_F(StrictWriteAllocationTest, InitialHeaderTimerFailureCannotStartBody) {
    checkFailure(TcpAllocationFailurePoint::HEADER_PROGRESS_TIMER, false, true);
}

TEST_F(StrictWriteAllocationTest, ReusedHeaderTimerFailureCannotStartBody) {
    checkFailure(TcpAllocationFailurePoint::HEADER_PROGRESS_TIMER, true, true);
}

TEST_F(StrictWriteAllocationTest, HelloTimerFailureTerminatesBeforePayload) {
    checkFailure(TcpAllocationFailurePoint::HELLO_STATUS_TIMER, false, true);
}

TEST_F(StrictWriteAllocationTest, AckReadFailureTerminatesBeforePayload) {
    checkFailure(TcpAllocationFailurePoint::WRITE_ACK_READ, true, true);
}

#endif

TEST_F(StrictWriteTest, ConcurrentRetirementsBothObserveCompletedDrain) {
    auto fragments = slices();
    auto future =
        submitter_->submitStrictWrite(descriptor(0, target_.size()), fragments,
                                      receiver_->getTcpInstanceId());
    ASSERT_TRUE(future);
    std::promise<void> start;
    const auto ready = start.get_future().share();
    auto retire = [&] {
        ready.wait();
        return receiver_->retireStrictWrites();
    };
    auto first = std::async(std::launch::async, retire);
    auto second = std::async(std::launch::async, retire);
    start.set_value();
    EXPECT_TRUE(first.get());
    EXPECT_TRUE(second.get());
    const auto at_retirement = target_;
    const auto result = future->get();
    EXPECT_TRUE(result == ErrorCode::OK || result == ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(target_, at_retirement);
}

TEST_F(StrictWriteTest,
       RetirementDrainsAcceptedBodyAndPermanentlyClosesEndpoint) {
    auto fragments = slices();
    const auto instance = receiver_->getTcpInstanceId();
    auto future = submitter_->submitStrictWrite(descriptor(0, target_.size()),
                                                fragments, instance);
    ASSERT_TRUE(future);
    ASSERT_TRUE(receiver_->retireStrictWrites());
    const auto at_retirement = target_;
    const auto result = future->get();
    EXPECT_TRUE(result == ErrorCode::OK || result == ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(target_, at_retirement);
    EXPECT_EQ(receiver_->getTcpInstanceId(), instance);
    EXPECT_TRUE(receiver_->isTcpRetired());
    EXPECT_TRUE(receiver_->retireStrictWrites());

    auto late = submitter_->submitStrictWrite(descriptor(0, target_.size()),
                                              fragments, instance);
    ASSERT_TRUE(late);
    EXPECT_EQ(late->get(), ErrorCode::TRANSFER_FAIL);
    EXPECT_EQ(target_, at_retirement);
}

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
