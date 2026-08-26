// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include <asio.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <functional>
#include <thread>
#include <vector>

#include "tent/transport/tcp/high_performance_tcp_buffer_registry.h"
#include "tent/transport/tcp/high_performance_tcp_client.h"
#include "tent/transport/tcp/high_performance_tcp_protocol.h"
#include "tent/transport/tcp/high_performance_tcp_server.h"
#include "tent/transport/tcp/high_performance_tcp_workers.h"

namespace mooncake::tent {
namespace {
using namespace std::chrono_literals;

struct Completion {
    std::atomic<bool> done{false};
    TransferStatusEnum status{PENDING};
    size_t bytes{0};

    std::function<void(TransferStatusEnum, size_t)> callback() {
        return [this](TransferStatusEnum s, size_t n) {
            status = s;
            bytes = n;
            done.store(true, std::memory_order_release);
        };
    }

    bool wait(std::chrono::milliseconds timeout = 2s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (done.load(std::memory_order_acquire)) {
                return true;
            }
            std::this_thread::sleep_for(1ms);
        }
        return done.load(std::memory_order_acquire);
    }
};

template <typename Predicate>
bool WaitUntil(Predicate&& predicate, std::chrono::milliseconds timeout = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

// The server and client intentionally share an in-process backing buffer in
// these socket tests. The protocol ACK provides the ordering, but
// ThreadSanitizer cannot infer a C++ happens-before edge through the kernel TCP
// stack.
template <size_t Size>
#if defined(__clang__) || defined(__GNUC__)
__attribute__((no_sanitize("thread")))
#endif
bool SocketOrderedEqual(const std::array<uint8_t, Size>& left,
                        const std::array<uint8_t, Size>& right) {
    return left == right;
}

class CoreRuntime {
   public:
    explicit CoreRuntime(size_t max_connections = 32)
        : workers({.worker_count = 2, .queue_capacity = 32}),
          client({.max_transfer_bytes = 1 << 20,
                  .chunk_size = 128,
                  .connect_timeout_ms = 500,
                  .progress_timeout_ms = 500,
                  .connections_per_peer = 2},
                 &workers),
          server({.bind_address = "127.0.0.1",
                  .port = 0,
                  .max_transfer_bytes = 1 << 20,
                  .chunk_size = 128,
                  .progress_timeout_ms = 500,
                  .max_connections = max_connections},
                 &registry, &workers) {}

    ~CoreRuntime() {
        (void)server.stopAccepting();
        (void)client.cancelAll(CANCELED);
        (void)server.stop();
        (void)workers.stop();
    }

    void start() {
        ASSERT_TRUE(workers.start().ok());
        ASSERT_TRUE(server.start(&port).ok());
        ASSERT_NE(port, 0);
    }

    size_t owner(uint32_t lane, const std::string& incarnation) const {
        return workers.affinityOwner({7, 0, lane, incarnation});
    }

    Status submit(HighPerformanceTcpClient::Operation op) {
        const size_t worker = owner(op.lane_id, op.incarnation);
        return workers.submitToWorker(
            worker, [this, worker, op = std::move(op)](size_t) mutable {
                client.enqueueOnOwner(worker, std::move(op));
            });
    }

    HighPerformanceTcpWorkers workers;
    HighPerformanceTcpBufferRegistry registry;
    HighPerformanceTcpClient client;
    HighPerformanceTcpServer server;
    uint16_t port{0};
};

HighPerformanceTcpClient::Operation MakeOperation(
    void* local, size_t length, uint64_t remote, uint64_t registration,
    uint64_t request_id, uint32_t lane, const std::string& incarnation,
    HighPerformanceTcpOpcode opcode,
    std::function<void(TransferStatusEnum, size_t)> complete, uint16_t port) {
    HighPerformanceTcpClient::Operation op;
    op.peer_id = 7;
    op.peer_name = "peer-seven";
    op.incarnation = incarnation;
    op.host = "127.0.0.1";
    op.port = port;
    op.lane_id = lane;
    op.registration_id = registration;
    op.remote_addr = remote;
    op.local_addr = local;
    op.length = length;
    op.opcode = opcode;
    op.request_id = request_id;
    op.complete = std::move(complete);
    return op;
}

TEST(HighPerformanceTcpSocketTest, WriteAckThenReadAndReuseConnection) {
    CoreRuntime runtime;
    runtime.start();

    std::array<uint8_t, 1024> remote{};
    uint64_t registration = 0;
    ASSERT_TRUE(runtime.registry
                    .add(reinterpret_cast<uint64_t>(remote.data()),
                         remote.size(), kGlobalReadWrite, &registration)
                    .ok());

    std::array<uint8_t, 1024> source{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<uint8_t>((i * 17) & 0xff);
    }
    Completion write;
    ASSERT_TRUE(runtime
                    .submit(MakeOperation(
                        source.data(), source.size(),
                        reinterpret_cast<uint64_t>(remote.data()), registration,
                        1, 0, "00112233445566778899aabbccddeeff",
                        HighPerformanceTcpOpcode::kWrite, write.callback(),
                        runtime.port))
                    .ok());
    ASSERT_TRUE(write.wait());
    EXPECT_EQ(write.status, COMPLETED);
    EXPECT_EQ(write.bytes, source.size());
    EXPECT_TRUE(SocketOrderedEqual(remote, source));

    std::array<uint8_t, 1024> destination{};
    Completion read;
    ASSERT_TRUE(
        runtime
            .submit(MakeOperation(
                destination.data(), destination.size(),
                reinterpret_cast<uint64_t>(remote.data()), registration, 2, 0,
                "00112233445566778899aabbccddeeff",
                HighPerformanceTcpOpcode::kRead, read.callback(), runtime.port))
            .ok());
    ASSERT_TRUE(read.wait());
    EXPECT_EQ(read.status, COMPLETED);
    EXPECT_EQ(read.bytes, destination.size());
    EXPECT_TRUE(SocketOrderedEqual(destination, remote));
    EXPECT_EQ(runtime.client.connectionsCreatedForTest(), 1u);
    EXPECT_GE(runtime.client.cleanReusesForTest(), 1u);

    EXPECT_TRUE(
        runtime.registry
            .remove(reinterpret_cast<uint64_t>(remote.data()), remote.size())
            .ok());
}

TEST(HighPerformanceTcpSocketTest, StaleRegistrationFailsAndDirtiesLane) {
    CoreRuntime runtime;
    runtime.start();

    std::array<uint8_t, 64> remote{};
    std::array<uint8_t, 64> local{};
    uint64_t registration = 0;
    ASSERT_TRUE(runtime.registry
                    .add(reinterpret_cast<uint64_t>(remote.data()),
                         remote.size(), kGlobalReadWrite, &registration)
                    .ok());

    Completion stale;
    ASSERT_TRUE(
        runtime
            .submit(MakeOperation(local.data(), local.size(),
                                  reinterpret_cast<uint64_t>(remote.data()),
                                  registration + 1, 11, 0,
                                  "00112233445566778899aabbccddeeff",
                                  HighPerformanceTcpOpcode::kRead,
                                  stale.callback(), runtime.port))
            .ok());
    ASSERT_TRUE(stale.wait());
    EXPECT_EQ(stale.status, FAILED);
    EXPECT_EQ(stale.bytes, 0u);

    Completion valid;
    ASSERT_TRUE(runtime
                    .submit(MakeOperation(
                        local.data(), local.size(),
                        reinterpret_cast<uint64_t>(remote.data()), registration,
                        12, 0, "00112233445566778899aabbccddeeff",
                        HighPerformanceTcpOpcode::kRead, valid.callback(),
                        runtime.port))
                    .ok());
    ASSERT_TRUE(valid.wait());
    EXPECT_EQ(valid.status, COMPLETED);
    EXPECT_EQ(runtime.client.connectionsCreatedForTest(), 2u);
}

TEST(HighPerformanceTcpSocketTest, IncarnationReplacementRetiresOldConnection) {
    CoreRuntime runtime;
    runtime.start();

    std::array<uint8_t, 64> remote{};
    std::array<uint8_t, 64> local{};
    uint64_t registration = 0;
    ASSERT_TRUE(runtime.registry
                    .add(reinterpret_cast<uint64_t>(remote.data()),
                         remote.size(), kGlobalReadWrite, &registration)
                    .ok());

    const std::string old_incarnation = "00112233445566778899aabbccddeeff";
    const std::string new_incarnation = "ffeeddccbbaa99887766554433221100";
    EXPECT_EQ(runtime.owner(0, old_incarnation),
              runtime.owner(0, new_incarnation));

    Completion first;
    ASSERT_TRUE(runtime
                    .submit(MakeOperation(
                        local.data(), local.size(),
                        reinterpret_cast<uint64_t>(remote.data()), registration,
                        20, 0, old_incarnation, HighPerformanceTcpOpcode::kRead,
                        first.callback(), runtime.port))
                    .ok());
    ASSERT_TRUE(first.wait());
    ASSERT_EQ(first.status, COMPLETED);

    Completion second;
    ASSERT_TRUE(runtime
                    .submit(MakeOperation(
                        local.data(), local.size(),
                        reinterpret_cast<uint64_t>(remote.data()), registration,
                        21, 0, new_incarnation, HighPerformanceTcpOpcode::kRead,
                        second.callback(), runtime.port))
                    .ok());
    ASSERT_TRUE(second.wait());
    EXPECT_EQ(second.status, COMPLETED);
    EXPECT_EQ(runtime.client.connectionsCreatedForTest(), 2u);
}

TEST(HighPerformanceTcpSocketTest,
     ProgressTimeoutIsStickyUntilIoCallbackRetires) {
    asio::io_context server_io;
    asio::ip::tcp::acceptor acceptor(
        server_io, asio::ip::tcp::endpoint(asio::ip::tcp::v4(), 0));
    const uint16_t port = acceptor.local_endpoint().port();
    std::thread peer([&] {
        asio::ip::tcp::socket socket(server_io);
        acceptor.accept(socket);
        std::array<uint8_t, kHighPerformanceTcpRequestSize> request{};
        asio::read(socket, asio::buffer(request));
        std::this_thread::sleep_for(200ms);
        std::error_code ignored;
        socket.close(ignored);
    });

    HighPerformanceTcpWorkers workers({.worker_count = 1, .queue_capacity = 8});
    ASSERT_TRUE(workers.start().ok());
    HighPerformanceTcpClient client({.max_transfer_bytes = 4096,
                                     .chunk_size = 128,
                                     .connect_timeout_ms = 100,
                                     .progress_timeout_ms = 30,
                                     .connections_per_peer = 1},
                                    &workers);

    std::array<uint8_t, 64> local{};
    Completion completion;
    auto op = MakeOperation(local.data(), local.size(), 0x1000, 1, 30, 0,
                            "00112233445566778899aabbccddeeff",
                            HighPerformanceTcpOpcode::kRead,
                            completion.callback(), port);
    ASSERT_TRUE(
        workers
            .submitToWorker(0,
                            [&client, op = std::move(op)](size_t) mutable {
                                client.enqueueOnOwner(0, std::move(op));
                            })
            .ok());
    ASSERT_TRUE(completion.wait(1s));
    EXPECT_EQ(completion.status, TIMEOUT);
    EXPECT_EQ(completion.bytes, 0u);
    EXPECT_TRUE(client.cancelAll(CANCELED).ok());
    EXPECT_TRUE(workers.stop().ok());
    peer.join();
}

TEST(HighPerformanceTcpSocketTest, MalformedFrameGetsErrorAndConnectionCloses) {
    CoreRuntime runtime;
    runtime.start();

    asio::io_context io;
    asio::ip::tcp::socket socket(io);
    socket.connect({asio::ip::make_address("127.0.0.1"), runtime.port});
    std::array<uint8_t, kHighPerformanceTcpRequestSize> frame{};
    // Correct magic, deliberately invalid version/opcode fields are otherwise
    // zero. The server must answer with a bounded error response then close.
    frame[0] = 0x4d;
    frame[1] = 0x43;
    frame[2] = 0x54;
    frame[3] = 0x50;
    asio::write(socket, asio::buffer(frame));

    std::array<uint8_t, kHighPerformanceTcpResponseSize> response_bytes{};
    ASSERT_EQ(asio::read(socket, asio::buffer(response_bytes)),
              response_bytes.size());
    HighPerformanceTcpResponseFrame response;
    ASSERT_TRUE(DecodeHighPerformanceTcpResponse(
                    response_bytes.data(), response_bytes.size(), &response)
                    .ok());
    EXPECT_NE(response.status, HighPerformanceTcpStatus::kOk);

    std::array<uint8_t, 1> extra{};
    std::error_code error;
    socket.read_some(asio::buffer(extra), error);
    EXPECT_TRUE(error == asio::error::eof ||
                error == asio::error::connection_reset);
}

TEST(HighPerformanceTcpSocketTest,
     ClosedSessionsAreReapedAndConnectionBudgetIsReusable) {
    constexpr size_t kMaxConnections = 2;
    constexpr size_t kConnectionCycles = 16;
    CoreRuntime runtime(kMaxConnections);
    runtime.start();

    for (size_t cycle = 0; cycle < kConnectionCycles; ++cycle) {
        SCOPED_TRACE(cycle);
        asio::io_context io;
        asio::ip::tcp::socket socket(io);
        socket.connect({asio::ip::make_address("127.0.0.1"), runtime.port});

        ASSERT_TRUE(WaitUntil(
            [&] { return runtime.server.activeSessionsForTest() == 1u; }));

        std::error_code ignored;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        socket.close(ignored);

        ASSERT_TRUE(WaitUntil(
            [&] { return runtime.server.activeSessionsForTest() == 0u; }));
    }
}

}  // namespace
}  // namespace mooncake::tent
