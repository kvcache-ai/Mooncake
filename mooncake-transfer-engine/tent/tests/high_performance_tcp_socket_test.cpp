// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include <asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <optional>
#include <thread>

#include "tent/transport/tcp/high_performance_tcp_client.h"
#include "tent/transport/tcp/high_performance_tcp_server.h"

namespace mooncake::tent {
namespace {
using namespace std::chrono_literals;
constexpr char kIncarnation[] = "00112233445566778899aabbccddeeff";

struct Completion {
    std::atomic<bool> done{false};
    TransferStatusEnum status{PENDING};
    size_t bytes{0};
    std::optional<HighPerformanceTcpStatus> protocol_status;

    auto callback() {
        return [this](TransferStatusEnum value, size_t count,
                      std::optional<HighPerformanceTcpStatus> result) {
            status = value;
            bytes = count;
            protocol_status = result;
            done.store(true, std::memory_order_release);
        };
    }
    bool wait() {
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (!done.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(1ms);
        }
        return done.load(std::memory_order_acquire);
    }
};

template <class Predicate>
bool WaitUntil(Predicate predicate) {
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!predicate() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

// Kernel TCP orders the remote write before its response, but that ordering is
// not visible to ThreadSanitizer as a C++ happens-before edge.
template <size_t N>
__attribute__((no_sanitize("thread"))) bool SocketOrderedEqual(
    const std::array<uint8_t, N>& left, const std::array<uint8_t, N>& right) {
    return left == right;
}

class Runtime {
   public:
    explicit Runtime(size_t max_connections = 16, uint64_t timeout_ms = 500)
        : workers({.worker_count = 2}),
          client({.max_transfer_bytes = 1 << 20,
                  .chunk_size = 128,
                  .connect_timeout_ms = 500,
                  .progress_timeout_ms = timeout_ms,
                  .connections_per_peer = 2},
                 &workers),
          server({.bind_address = "127.0.0.1",
                  .port = 0,
                  .max_transfer_bytes = 1 << 20,
                  .chunk_size = 128,
                  .progress_timeout_ms = timeout_ms,
                  .max_connections = max_connections},
                 &registry, &workers) {}

    ~Runtime() {
        (void)server.stopAccepting();
        (void)client.cancelAll();
        (void)server.stop();
        (void)workers.stop();
    }
    void start() {
        ASSERT_TRUE(workers.start().ok());
        ASSERT_TRUE(server.start(&port).ok());
    }
    Status submit(HighPerformanceTcpClient::Operation operation) {
        const size_t owner =
            workers.affinityOwner(operation.peer_id, operation.lane_id);
        std::vector<HighPerformanceTcpWorkers::Command> commands;
        commands.push_back({.worker_id = owner,
                            .run =
                                [this, owner, operation = std::move(operation)](
                                    size_t) mutable {
                                    client.enqueueOnOwner(owner,
                                                          std::move(operation));
                                },
                            .cancel = {}});
        return workers.tryCommitBatch(commands, nullptr, 0, 0, [] {});
    }

    HighPerformanceTcpWorkers workers;
    HighPerformanceTcpBufferRegistry registry;
    HighPerformanceTcpClient client;
    HighPerformanceTcpServer server;
    uint16_t port{0};
};

HighPerformanceTcpClient::Operation Operation(
    void* local, size_t length, uint64_t remote, uint64_t registration,
    uint64_t request_id, HighPerformanceTcpOpcode opcode,
    Completion* completion, uint16_t port) {
    HighPerformanceTcpClient::Operation operation;
    operation.peer_id = 7;
    operation.incarnation = kIncarnation;
    operation.host = "127.0.0.1";
    operation.port = port;
    operation.registration_id = registration;
    operation.remote_addr = remote;
    operation.local_addr = local;
    operation.length = length;
    operation.opcode = opcode;
    operation.request_id = request_id;
    operation.complete = completion->callback();
    return operation;
}

TEST(HighPerformanceTcpSocketTest, WriteReadAndReuseConnection) {
    Runtime runtime(/*max_connections=*/16, /*timeout_ms=*/100);
    runtime.start();
    std::array<uint8_t, 1024> remote{};
    std::array<uint8_t, 1024> source{};
    std::array<uint8_t, 1024> destination{};
    source.fill(0x5a);
    uint64_t registration = 0;
    ASSERT_TRUE(runtime.registry
                    .add(reinterpret_cast<uint64_t>(remote.data()),
                         remote.size(), kGlobalReadWrite, &registration)
                    .ok());

    Completion write;
    ASSERT_TRUE(
        runtime
            .submit(Operation(source.data(), source.size(),
                              reinterpret_cast<uint64_t>(remote.data()),
                              registration, 1, HighPerformanceTcpOpcode::kWrite,
                              &write, runtime.port))
            .ok());
    ASSERT_TRUE(write.wait());
    EXPECT_EQ(write.status, COMPLETED);
    EXPECT_TRUE(SocketOrderedEqual(remote, source));

    std::this_thread::sleep_for(300ms);
    ASSERT_EQ(runtime.server.activeSessionsForTest(), 1u);

    Completion read;
    ASSERT_TRUE(
        runtime
            .submit(Operation(destination.data(), destination.size(),
                              reinterpret_cast<uint64_t>(remote.data()),
                              registration, 2, HighPerformanceTcpOpcode::kRead,
                              &read, runtime.port))
            .ok());
    ASSERT_TRUE(read.wait());
    EXPECT_EQ(read.status, COMPLETED);
    EXPECT_TRUE(SocketOrderedEqual(destination, source));
    EXPECT_EQ(runtime.client.connectionsCreatedForTest(), 1u);
}

TEST(HighPerformanceTcpSocketTest, RejectedWriteBodyCannotBecomeNextFrame) {
    Runtime runtime;
    runtime.start();
    std::array<uint8_t, 64> remote{};
    uint64_t registration = 0;
    ASSERT_TRUE(runtime.registry
                    .add(reinterpret_cast<uint64_t>(remote.data()),
                         remote.size(), kGlobalReadWrite, &registration)
                    .ok());

    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    socket.connect({asio::ip::make_address("127.0.0.1"), runtime.port});
    const auto rejected = EncodeHighPerformanceTcpRequest(
        {HighPerformanceTcpOpcode::kWrite, 20, registration + 1,
         reinterpret_cast<uint64_t>(remote.data()),
         kHighPerformanceTcpRequestSize + 1});
    const auto hidden = EncodeHighPerformanceTcpRequest(
        {HighPerformanceTcpOpcode::kWrite, 21, registration,
         reinterpret_cast<uint64_t>(remote.data()), 1});
    const uint8_t hidden_body = 0x5a;
    asio::write(socket, asio::buffer(rejected));
    asio::write(socket, asio::buffer(hidden));
    asio::write(socket, asio::buffer(&hidden_body, 1));

    std::array<uint8_t, kHighPerformanceTcpResponseSize> response_bytes{};
    ASSERT_EQ(asio::read(socket, asio::buffer(response_bytes)),
              response_bytes.size());
    HighPerformanceTcpResponseFrame response;
    ASSERT_TRUE(DecodeHighPerformanceTcpResponse(
                    response_bytes.data(), response_bytes.size(), &response)
                    .ok());
    EXPECT_EQ(response.status, HighPerformanceTcpStatus::kStaleRegistration);
    EXPECT_EQ(response.request_id, 20u);
    EXPECT_EQ(response.committed_bytes, 0u);
    EXPECT_EQ(remote[0], 0u);
    EXPECT_TRUE(
        WaitUntil([&] { return runtime.server.activeSessionsForTest() == 0; }));
}

TEST(HighPerformanceTcpSocketTest,
     RejectedWritePartialPayloadTimesOutAndReleasesSlot) {
    Runtime runtime(/*max_connections=*/1, /*timeout_ms=*/100);
    runtime.start();
    std::array<uint8_t, 1> remote{};
    uint64_t registration = 0;
    ASSERT_TRUE(runtime.registry
                    .add(reinterpret_cast<uint64_t>(remote.data()),
                         remote.size(), kGlobalReadWrite, &registration)
                    .ok());

    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    socket.connect({asio::ip::make_address("127.0.0.1"), runtime.port});
    const auto rejected = EncodeHighPerformanceTcpRequest(
        {HighPerformanceTcpOpcode::kWrite, 22, registration + 1,
         reinterpret_cast<uint64_t>(remote.data()), 1024});
    const uint8_t one_body_byte = 0;
    asio::write(socket, asio::buffer(rejected));
    asio::write(socket, asio::buffer(&one_body_byte, 1));
    ASSERT_TRUE(
        WaitUntil([&] { return runtime.server.activeSessionsForTest() == 1; }));
    EXPECT_TRUE(
        WaitUntil([&] { return runtime.server.activeSessionsForTest() == 0; }));
    EXPECT_EQ(remote[0], 0u);
}

TEST(HighPerformanceTcpSocketTest, ClientProgressTimeoutCompletesTask) {
    asio::io_context peer_io;
    asio::ip::tcp::acceptor acceptor(peer_io, {asio::ip::tcp::v4(), 0});
    std::thread peer([&] {
        asio::ip::tcp::socket socket(peer_io);
        acceptor.accept(socket);
        std::array<uint8_t, kHighPerformanceTcpRequestSize> request{};
        asio::read(socket, asio::buffer(request));
        std::this_thread::sleep_for(200ms);
    });

    HighPerformanceTcpWorkers workers({.worker_count = 1});
    ASSERT_TRUE(workers.start().ok());
    HighPerformanceTcpClient client({4096, 128, 100, 30, 1}, &workers);
    std::array<uint8_t, 64> local{};
    Completion completion;
    auto operation = Operation(local.data(), local.size(), 0x1000, 1, 3,
                               HighPerformanceTcpOpcode::kRead, &completion,
                               acceptor.local_endpoint().port());
    std::vector<HighPerformanceTcpWorkers::Command> commands;
    commands.push_back({.worker_id = 0,
                        .run =
                            [&](size_t) mutable {
                                client.enqueueOnOwner(0, std::move(operation));
                            },
                        .cancel = {}});
    ASSERT_TRUE(workers.tryCommitBatch(commands, nullptr, 0, 0, [] {}).ok());
    ASSERT_TRUE(completion.wait());
    EXPECT_EQ(completion.status, TIMEOUT);
    EXPECT_FALSE(completion.protocol_status.has_value());
    EXPECT_TRUE(client.cancelAll().ok());
    EXPECT_TRUE(workers.stop().ok());
    peer.join();
}

TEST(HighPerformanceTcpSocketTest,
     WriteWithoutCompletionAckMarksRemoteOutcomeUnknown) {
    asio::io_context peer_io;
    asio::ip::tcp::acceptor acceptor(peer_io, {asio::ip::tcp::v4(), 0});
    std::thread peer([&] {
        asio::ip::tcp::socket socket(peer_io);
        acceptor.accept(socket);
        std::array<uint8_t, kHighPerformanceTcpRequestSize> request{};
        std::array<uint8_t, 64> body{};
        asio::read(socket, asio::buffer(request));
        asio::read(socket, asio::buffer(body));
        socket.close();  // The payload arrived, but no completion ACK did.
    });

    HighPerformanceTcpWorkers workers({.worker_count = 1});
    ASSERT_TRUE(workers.start().ok());
    HighPerformanceTcpClient client({4096, 128, 100, 100, 1}, &workers);
    std::array<uint8_t, 64> local{};
    Completion completion;
    auto operation = Operation(local.data(), local.size(), 0x1000, 1, 31,
                               HighPerformanceTcpOpcode::kWrite, &completion,
                               acceptor.local_endpoint().port());
    std::vector<HighPerformanceTcpWorkers::Command> commands;
    commands.push_back({.worker_id = 0,
                        .run =
                            [&](size_t) mutable {
                                client.enqueueOnOwner(0, std::move(operation));
                            },
                        .cancel = {}});
    ASSERT_TRUE(workers.tryCommitBatch(commands, nullptr, 0, 0, [] {}).ok());
    ASSERT_TRUE(completion.wait());
    EXPECT_EQ(completion.status, FAILED);
    EXPECT_EQ(completion.protocol_status,
              HighPerformanceTcpStatus::kInternalError);
    EXPECT_TRUE(client.cancelAll().ok());
    EXPECT_TRUE(workers.stop().ok());
    peer.join();
}

TEST(HighPerformanceTcpSocketTest, EmptyAndPartialHeadersReleaseSlot) {
    Runtime runtime(/*max_connections=*/1, /*timeout_ms=*/100);
    runtime.start();
    asio::io_context io;
    {
        asio::ip::tcp::socket socket(io);
        socket.connect({asio::ip::make_address("127.0.0.1"), runtime.port});
        ASSERT_TRUE(WaitUntil(
            [&] { return runtime.server.activeSessionsForTest() == 1; }));
        ASSERT_TRUE(WaitUntil(
            [&] { return runtime.server.activeSessionsForTest() == 0; }));
    }
    {
        asio::ip::tcp::socket socket(io);
        socket.connect({asio::ip::make_address("127.0.0.1"), runtime.port});
        const uint8_t byte = 0;
        asio::write(socket, asio::buffer(&byte, 1));
        ASSERT_TRUE(WaitUntil(
            [&] { return runtime.server.activeSessionsForTest() == 1; }));
        ASSERT_TRUE(WaitUntil(
            [&] { return runtime.server.activeSessionsForTest() == 0; }));
    }

    std::array<uint8_t, 8> remote{};
    std::array<uint8_t, 8> local{};
    uint64_t registration = 0;
    ASSERT_TRUE(runtime.registry
                    .add(reinterpret_cast<uint64_t>(remote.data()),
                         remote.size(), kGlobalReadOnly, &registration)
                    .ok());
    Completion completion;
    ASSERT_TRUE(
        runtime
            .submit(Operation(local.data(), local.size(),
                              reinterpret_cast<uint64_t>(remote.data()),
                              registration, 4, HighPerformanceTcpOpcode::kRead,
                              &completion, runtime.port))
            .ok());
    ASSERT_TRUE(completion.wait());
    EXPECT_EQ(completion.status, COMPLETED);
}

TEST(HighPerformanceTcpSocketTest, ClosedSessionsAreReaped) {
    Runtime runtime(/*max_connections=*/2);
    runtime.start();
    for (int i = 0; i < 8; ++i) {
        asio::io_context io;
        asio::ip::tcp::socket socket(io);
        socket.connect({asio::ip::make_address("127.0.0.1"), runtime.port});
        ASSERT_TRUE(WaitUntil(
            [&] { return runtime.server.activeSessionsForTest() == 1; }));
        socket.close();
        ASSERT_TRUE(WaitUntil(
            [&] { return runtime.server.activeSessionsForTest() == 0; }));
    }
}

}  // namespace
}  // namespace mooncake::tent
