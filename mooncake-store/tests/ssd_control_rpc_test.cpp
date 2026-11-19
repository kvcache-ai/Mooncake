// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_control_rpc.h"
#include "ssd_transfer_registration.h"
#include "transfer_engine.h"
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <poll.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <unistd.h>
#include <array>
#include <atomic>
#include <thread>
#include <future>
#include <condition_variable>
#include <ylt/struct_pack.hpp>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
std::shared_ptr<TransferEngine> Engine() {
    auto result = std::make_shared<TransferEngine>(false);
    if (result->init("P2PHANDSHAKE", "127.0.0.1:0", "127.0.0.1", 0) != 0 ||
        !result->installTransport("tcp", nullptr))
        return {};
    return result;
}
struct Socket {
    int fd{-1};
    ~Socket() {
        if (fd >= 0) close(fd);
    }
};
Socket Connect(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(port);
    if (connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) !=
        0) {
        close(fd);
        return {};
    }
    return {fd};
}
// Fault injection is on the network reply, after the actual owner has
// produced its terminal result. Owner I/O threads/workers remain available.
struct DelayedReplyProxy {
    Socket listener{socket(AF_INET, SOCK_STREAM, 0)};
    uint16_t port{0};
    std::thread thread;
    explicit DelayedReplyProxy(uint16_t upstream) {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (listener.fd < 0 ||
            bind(listener.fd, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) != 0 ||
            listen(listener.fd, 1) != 0)
            return;
        socklen_t size = sizeof(address);
        if (getsockname(listener.fd, reinterpret_cast<sockaddr*>(&address),
                        &size) != 0)
            return;
        port = ntohs(address.sin_port);
        thread = std::thread([this, upstream] {
            pollfd descriptor{listener.fd, POLLIN, 0};
            if (poll(&descriptor, 1, 2000) != 1) return;
            Socket incoming{accept(listener.fd, nullptr, nullptr)};
            auto outgoing = Connect(upstream);
            if (incoming.fd < 0 || outgoing.fd < 0) return;
            std::array<char, 20> head;
            if (recv(incoming.fd, head.data(), head.size(), MSG_WAITALL) !=
                ssize_t(head.size()))
                return;
            coro_rpc::protocol::coro_rpc_protocol::req_header request{};
            if (struct_pack::deserialize_to<
                    struct_pack::sp_config::DISABLE_ALL_META_INFO>(
                    request, std::string_view(head.data(), head.size())) ||
                request.length > kSsdControlMaxRpcBody)
                return;
            std::string body(request.length, '\0');
            if (recv(incoming.fd, body.data(), body.size(), MSG_WAITALL) !=
                ssize_t(body.size()))
                return;
            if (send(outgoing.fd, head.data(), head.size(), MSG_NOSIGNAL) !=
                    ssize_t(head.size()) ||
                send(outgoing.fd, body.data(), body.size(), MSG_NOSIGNAL) !=
                    ssize_t(body.size()))
                return;
            std::array<char, 16> reply_head;
            if (recv(outgoing.fd, reply_head.data(), reply_head.size(),
                     MSG_WAITALL) != ssize_t(reply_head.size()))
                return;
            coro_rpc::protocol::coro_rpc_protocol::resp_header response{};
            if (struct_pack::deserialize_to<
                    struct_pack::sp_config::DISABLE_ALL_META_INFO>(
                    response,
                    std::string_view(reply_head.data(), reply_head.size())) ||
                response.length > kSsdControlMaxRpcBody)
                return;
            std::string reply(response.length, '\0');
            if (recv(outgoing.fd, reply.data(), reply.size(), MSG_WAITALL) !=
                ssize_t(reply.size()))
                return;
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
            send(incoming.fd, reply_head.data(), reply_head.size(),
                 MSG_NOSIGNAL);
            send(incoming.fd, reply.data(), reply.size(), MSG_NOSIGNAL);
        });
    }
    ~DelayedReplyProxy() {
        if (thread.joinable()) thread.join();
    }
};
class SsdControlRpcTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("ssd_control_rpc_test");
        FLAGS_logtostderr = 1;
        unsetenv("MC_USE_TENT");
        unsetenv("MC_USE_TEV1");
        unsetenv("MC_TCP_PROTO");
    }
    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }
};
class PooledRpcTest : public SsdControlRpcTest,
                      public ::testing::WithParamInterface<bool> {};
// One bounded local event loop keeps many independent Owner routes alive.
// It never closes an idle stream unless the test explicitly rotates the peer.
class PoolRoutes {
   public:
    explicit PoolRoutes(size_t count) : accepts_(count) {
        for (size_t i = 0; i < count; ++i) {
            int fd = socket(AF_INET, SOCK_STREAM, 0);
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            socklen_t size = sizeof(address);
            if (fd < 0 ||
                bind(fd, reinterpret_cast<sockaddr*>(&address), size) ||
                listen(fd, 8) ||
                getsockname(fd, reinterpret_cast<sockaddr*>(&address), &size)) {
                if (fd >= 0) close(fd);
                failed = true;
                return;
            }
            listeners_.push_back(fd);
            ports.push_back(ntohs(address.sin_port));
        }
        worker_ = std::thread([this] { Run(); });
    }
    ~PoolRoutes() {
        stop_ = true;
        if (worker_.joinable()) worker_.join();
        for (int fd : connections_)
            if (fd >= 0) close(fd);
        for (int fd : listeners_) close(fd);
    }
    size_t accepts(size_t route) const { return accepts_[route]; }
    void RotateOwner() {
        rotate_ = true;
        while (rotate_) std::this_thread::yield();
    }
    std::vector<uint16_t> ports;
    std::atomic<bool> failed{false};

   private:
    static bool Exact(int fd, char* data, size_t size, bool write) {
        size_t done = 0;
        while (done < size) {
            const auto n =
                write ? send(fd, data + done, size - done, MSG_NOSIGNAL)
                      : recv(fd, data + done, size - done, 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            done += size_t(n);
        }
        return true;
    }
    void Run() {
        using Protocol = coro_rpc::protocol::coro_rpc_protocol;
        while (!stop_) {
            if (rotate_) {
                for (auto& fd : connections_) {
                    if (fd >= 0) close(fd);
                    fd = -1;
                }
                rotate_ = false;
            }
            std::vector<pollfd> wait;
            for (int fd : listeners_) wait.push_back({fd, POLLIN, 0});
            for (int fd : connections_) wait.push_back({fd, POLLIN, 0});
            if (poll(wait.data(), wait.size(), 10) <= 0) continue;
            for (size_t i = 0; i < wait.size() && !stop_; ++i) {
                if (!wait[i].revents) continue;
                if (i < listeners_.size()) {
                    int fd = accept(listeners_[i], nullptr, nullptr);
                    if (fd < 0) {
                        failed = true;
                        continue;
                    }
                    timeval timeout{1, 0};
                    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                               sizeof(timeout));
                    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                               sizeof(timeout));
                    connections_.push_back(fd);
                    ++accepts_[i];
                    continue;
                }
                auto& fd = connections_[i - listeners_.size()];
                std::array<char, 20> head{};
                Protocol::req_header request{};
                bool ok =
                    Exact(fd, head.data(), head.size(), false) &&
                    !struct_pack::deserialize_to<
                        struct_pack::sp_config::DISABLE_ALL_META_INFO>(
                        request, std::string_view(head.data(), head.size())) &&
                    request.length <= kSsdControlMaxRpcBody;
                std::string body(ok ? request.length : 0, '\0');
                ok = ok && Exact(fd, body.data(), body.size(), false);
                if (ok) {
                    auto wire =
                        struct_pack::deserialize<std::string_view>(body);
                    SsdControlMessage message;
                    ok = wire && DecodeSsdControlMessage(*wire, message) ==
                                     DestinationError::kOk;
                    if (ok) {
                        auto encoded =
                            EncodeSsdControlReply({message.command,
                                                   DestinationError::kNotFound,
                                                   {},
                                                   {}});
                        auto payload = struct_pack::serialize<std::string>(
                            std::string_view(encoded.bytes));
                        Protocol::resp_header response{};
                        response.magic = Protocol::magic_number;
                        response.seq_num = request.seq_num;
                        response.length = uint32_t(payload.size());
                        auto reply_head = struct_pack::serialize<
                            struct_pack::sp_config::DISABLE_ALL_META_INFO,
                            std::string>(response);
                        ok = Exact(fd, reply_head.data(), reply_head.size(),
                                   true) &&
                             Exact(fd, payload.data(), payload.size(), true);
                    }
                    if (!ok) failed = true;
                }
                if (!ok) {
                    close(fd);
                    fd = -1;
                }
            }
        }
    }
    std::vector<int> listeners_, connections_;
    std::vector<std::atomic<size_t>> accepts_;
    std::atomic<bool> stop_{false}, rotate_{false};
    std::thread worker_;
};
SsdRequesterCallResult QueryRoute(const SsdRequesterControlCall& call,
                                  uint16_t port, uint64_t id = 1) {
    return call(
        "127.0.0.1", port,
        {SsdControlCommand::kQuery, {requester, {owner, id, 1}, {11, 12}}, {}},
        std::chrono::seconds(1));
}
TEST_F(SsdControlRpcTest, IdleCacheEvictsOldestRouteAt128Connections) {
    PoolRoutes routes(129);
    ASSERT_FALSE(routes.failed);
    ASSERT_EQ(routes.ports.size(), 129u);
    auto call = MakeSsdRequesterControlCall();
    for (auto port : routes.ports)
        ASSERT_EQ(QueryRoute(call, port).error, DestinationError::kOk);
    EXPECT_EQ(routes.accepts(0), 1u);
    EXPECT_EQ(QueryRoute(call, routes.ports[0], 2).error,
              DestinationError::kOk);
    EXPECT_EQ(routes.accepts(0), 2u);
    // A recent route survives while the oldest stream is evicted.
    EXPECT_EQ(QueryRoute(call, routes.ports[128], 2).error,
              DestinationError::kOk);
    EXPECT_EQ(routes.accepts(128), 1u);
    EXPECT_FALSE(routes.failed);
}
TEST_F(SsdControlRpcTest, IdleExpirationIsPrunedOnTheNextTake) {
    PoolRoutes routes(1);
    ASSERT_FALSE(routes.failed);
    auto call = MakeSsdRequesterControlCall();
    ASSERT_EQ(QueryRoute(call, routes.ports[0]).error, DestinationError::kOk);
    ASSERT_EQ(QueryRoute(call, routes.ports[0], 2).error,
              DestinationError::kOk);
    ASSERT_EQ(routes.accepts(0), 1u);
    std::this_thread::sleep_for(std::chrono::seconds(31));
    EXPECT_EQ(routes.accepts(0), 1u);  // No timer evicts the live idle stream.
    EXPECT_EQ(QueryRoute(call, routes.ports[0], 3).error,
              DestinationError::kOk);
    EXPECT_EQ(routes.accepts(0), 2u);
    EXPECT_FALSE(routes.failed);
}
TEST_F(SsdControlRpcTest, ClosedOwnerStreamIsDiscardedBeforeNextSubmission) {
    PoolRoutes routes(1);
    ASSERT_FALSE(routes.failed);
    auto call = MakeSsdRequesterControlCall();
    ASSERT_EQ(QueryRoute(call, routes.ports[0]).error, DestinationError::kOk);
    routes.RotateOwner();
    EXPECT_EQ(QueryRoute(call, routes.ports[0], 2).error,
              DestinationError::kOk);
    EXPECT_EQ(routes.accepts(0), 2u);
    EXPECT_FALSE(routes.failed);
}
TEST_P(PooledRpcTest, ReusesHealthyStreamAndNeverReplaysAfterPartialReply) {
    const bool drop_second_response = GetParam();
    Socket listener{socket(AF_INET, SOCK_STREAM, 0)};
    ASSERT_GE(listener.fd, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(bind(listener.fd, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)),
              0);
    ASSERT_EQ(listen(listener.fd, 8), 0);
    socklen_t size = sizeof(address);
    ASSERT_EQ(
        getsockname(listener.fd, reinterpret_cast<sockaddr*>(&address), &size),
        0);
    constexpr size_t calls = 256;
    std::atomic<size_t> connections{0}, requests{0};
    std::atomic<bool> failed{false};
    auto exact = [](int fd, char* data, size_t bytes, bool send_bytes) {
        size_t done = 0;
        while (done < bytes) {
            const auto n =
                send_bytes ? send(fd, data + done, bytes - done, MSG_NOSIGNAL)
                           : recv(fd, data + done, bytes - done, 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            done += size_t(n);
        }
        return true;
    };
    std::thread server([&] {
        using Protocol = coro_rpc::protocol::coro_rpc_protocol;
        while (requests < calls) {
            pollfd wait{listener.fd, POLLIN, 0};
            if (poll(&wait, 1, 2000) != 1) {
                failed = true;
                return;
            }
            Socket incoming{accept(listener.fd, nullptr, nullptr)};
            if (incoming.fd < 0) {
                failed = true;
                return;
            }
            ++connections;
            const int no_delay = 1;
            setsockopt(incoming.fd, IPPROTO_TCP, TCP_NODELAY, &no_delay,
                       sizeof(no_delay));
            timeval timeout{2, 0};
            setsockopt(incoming.fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout));
            uint32_t previous = 0;
            while (requests < calls) {
                std::array<char, 20> head{};
                if (!exact(incoming.fd, head.data(), head.size(), false)) break;
                Protocol::req_header request{};
                if (struct_pack::deserialize_to<
                        struct_pack::sp_config::DISABLE_ALL_META_INFO>(
                        request, std::string_view(head.data(), head.size())) ||
                    request.seq_num <= previous ||
                    request.length > kSsdControlMaxRpcBody) {
                    failed = true;
                    return;
                }
                previous = request.seq_num;
                std::string body(request.length, '\0');
                if (!exact(incoming.fd, body.data(), body.size(), false)) {
                    failed = true;
                    return;
                }
                auto wire = struct_pack::deserialize<std::string_view>(body);
                SsdControlMessage message;
                if (!wire || DecodeSsdControlMessage(*wire, message) !=
                                 DestinationError::kOk) {
                    failed = true;
                    return;
                }
                ++requests;
                SsdControlReply reply{
                    message.command, DestinationError::kNotFound, {}, {}};
                auto encoded = EncodeSsdControlReply(reply);
                auto payload = struct_pack::serialize<std::string>(
                    std::string_view(encoded.bytes));
                Protocol::resp_header response{};
                response.magic = Protocol::magic_number;
                response.seq_num = request.seq_num;
                response.length = uint32_t(payload.size());
                auto response_head = struct_pack::serialize<
                    struct_pack::sp_config::DISABLE_ALL_META_INFO, std::string>(
                    response);
                if (drop_second_response && requests == 129) {
                    // The request reached the server. A partial reply leaves
                    // its outcome uncertain, so replay on a fresh stream is
                    // forbidden. Only the next explicit call may reconnect.
                    if (!exact(incoming.fd, response_head.data(), 4, true))
                        failed = true;
                    break;
                }
                if (!exact(incoming.fd, response_head.data(),
                           response_head.size(), true) ||
                    !exact(incoming.fd, payload.data(), payload.size(), true)) {
                    failed = true;
                    return;
                }
            }
        }
    });
    auto call = MakeSsdRequesterControlCall();
    bool valid = true;
    for (size_t i = 0; i < calls && valid; ++i) {
        SsdControlMessage message{SsdControlCommand::kQuery,
                                  {requester, {owner, i + 1, 1}, {11, 12}},
                                  {}};
        // Endpoint fan-out creates short-lived workers. The requester-scoped
        // cache must survive those workers instead of using thread-local FDs.
        auto result = std::async(std::launch::async, [&] {
                          return call("127.0.0.1", ntohs(address.sin_port),
                                      message, std::chrono::seconds(1));
                      }).get();
        if (drop_second_response && i == 128) {
            valid = result.error == DestinationError::kTransportError &&
                    !result.reply;
        } else {
            valid = result.error == DestinationError::kOk && result.reply &&
                    result.reply->command == message.command &&
                    result.reply->error == DestinationError::kNotFound;
        }
    }
    server.join();
    EXPECT_TRUE(valid);
    EXPECT_FALSE(failed);
    EXPECT_EQ(requests, calls);
    EXPECT_EQ(connections, drop_second_response ? 2u : 1u);
}
INSTANTIATE_TEST_SUITE_P(HealthyAndBrokenConnection, PooledRpcTest,
                         ::testing::Bool());
TEST_F(SsdControlRpcTest, HeaderPolicyBoundsBothBodyAndAttachments) {
    EXPECT_EQ(ValidateSsdControlRpcRequestHeader(128, 0),
              DestinationError::kOk);
    EXPECT_EQ(ValidateSsdControlRpcRequestHeader(kSsdControlMaxRpcBody, 0),
              DestinationError::kOk);
    EXPECT_EQ(ValidateSsdControlRpcRequestHeader(kSsdControlMaxRpcBody + 1, 0),
              DestinationError::kLimit);
    EXPECT_EQ(ValidateSsdControlRpcRequestHeader(128, 1),
              DestinationError::kLimit);
}
TEST_F(SsdControlRpcTest, ActualRpcAndTcpWriteRecoverAfterLostTerminalReply) {
    auto destination_engine = Engine(), source_engine = Engine();
    ASSERT_TRUE(destination_engine);
    ASSERT_TRUE(source_engine);
    auto backing = std::make_shared<std::array<std::byte, 128>>();
    backing->fill(std::byte{0xA5});
    auto transport = std::make_shared<TransferEngineDestinationRegistration>(
        destination_engine);
    auto registration = std::make_shared<DestinationRegistration>(
        requester, destination_engine->getLocalIpAndPort(), transport);
    const auto base = reinterpret_cast<uint64_t>(backing->data());
    auto region = registration->Register({base + 16, 96, base, backing->size()},
                                         "cpu:0", backing);
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto grants = std::make_shared<SsdRequesterGrants>(requester, registration);
    auto ledger = std::make_shared<SsdOwnerRequestLedger>(owner);
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 1, 1},
        "default",
        1000,
        0,
        {{"key", 4096, {}, {{123, {{region.region, 4, 32}}}}}}};
    auto prepared = grants->Prepare(request);
    ASSERT_EQ(prepared.error, DestinationError::kOk);
    SsdControlRpcEndpoint requester_endpoint(grants, {});
    SsdControlRpcServer requester_server(1, 0, "127.0.0.1");
    requester_server.register_handler<&SsdControlRpcEndpoint::Call>(
        &requester_endpoint);
    requester_server.async_start();
    ASSERT_FALSE(requester_server.get_errc());
    std::atomic<size_t> writers{0};
    std::promise<void> drained;
    auto drained_future = drained.get_future();
    SsdControlRpcEndpoint owner_endpoint(
        {}, ledger,
        [&](const SsdReadRequestV1& req, const SsdGrantReferenceV1& ref,
            SsdOwnerRequestLedger::Operation operation) {
            EXPECT_EQ(operation.Start(), DestinationError::kOk);
            auto resolved =
                CallSsdControlRpc("127.0.0.1", requester_server.port(),
                                  {SsdControlCommand::kResolve, ref, req},
                                  std::chrono::seconds(1));
            ASSERT_EQ(resolved.error, DestinationError::kOk);
            ASSERT_TRUE(resolved.reply);
            ASSERT_EQ(resolved.reply->error, DestinationError::kOk);
            ASSERT_EQ(resolved.reply->destinations.size(), 1);
            const auto& view = resolved.reply->destinations[0];
            std::array<std::byte, 32> slot;
            slot.fill(std::byte{0x3B});
            ASSERT_EQ(source_engine->registerLocalMemory(
                          slot.data(), slot.size(), "cpu:0", false, false),
                      0);
            std::array<size_t, 1> offsets{0}, lengths{32};
            TransferEngine::ScatterTransferRange range{TransferRequest::WRITE,
                                                       view.transport_endpoint,
                                                       view.address,
                                                       size_t(view.length),
                                                       slot.data(),
                                                       slot.size(),
                                                       offsets,
                                                       offsets,
                                                       lengths,
                                                       {}};
            auto transfer = source_engine->submitScatter({range});
            ASSERT_TRUE(transfer.wait().ok());
            ASSERT_EQ(source_engine->unregisterLocalMemory(slot.data(), false),
                      0);
            ++writers;
            EXPECT_EQ(
                operation.Complete({1,
                                    req.attempt,
                                    true,
                                    {{0, AttemptOutcome::kSucceeded, 32, {}}},
                                    requester}),
                DestinationError::kOk);
            drained.set_value();
        });
    SsdControlRpcServer owner_server(1, 0, "127.0.0.1");
    owner_server.register_handler<&SsdControlRpcEndpoint::Call>(
        &owner_endpoint);
    owner_server.async_start();
    ASSERT_FALSE(owner_server.get_errc());
    auto submitted = CallSsdControlRpc(
        "127.0.0.1", owner_server.port(),
        {SsdControlCommand::kSubmit, prepared.reference, request},
        std::chrono::seconds(1));
    ASSERT_EQ(submitted.error, DestinationError::kOk);
    ASSERT_EQ(drained_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    DelayedReplyProxy proxy(owner_server.port());
    ASSERT_GT(proxy.port, 0);
    auto lost =
        CallSsdControlRpc("127.0.0.1", proxy.port,
                          {SsdControlCommand::kQuery, prepared.reference, {}},
                          std::chrono::milliseconds(75));
    EXPECT_EQ(lost.transport_error, std::make_error_code(std::errc::timed_out));
    EXPECT_FALSE(lost.reply);
    ASSERT_EQ(grants->Timeout(prepared.reference), DestinationError::kOk);
    EXPECT_EQ(registration->Revoke(region.region), DestinationError::kBusy);
    auto recovered =
        CallSsdControlRpc("127.0.0.1", owner_server.port(),
                          {SsdControlCommand::kQuery, prepared.reference, {}},
                          std::chrono::seconds(1));
    ASSERT_EQ(recovered.error, DestinationError::kOk);
    ASSERT_TRUE(recovered.reply);
    ASSERT_TRUE(recovered.reply->snapshot);
    ASSERT_TRUE(recovered.reply->snapshot->result);
    EXPECT_EQ(grants->Complete(prepared.reference,
                               *recovered.reply->snapshot->result),
              DestinationError::kOk);
    EXPECT_FALSE(grants->Inspect(prepared.reference).publishable);
    auto duplicate = CallSsdControlRpc(
        "127.0.0.1", owner_server.port(),
        {SsdControlCommand::kSubmit, prepared.reference, request},
        std::chrono::seconds(1));
    ASSERT_TRUE(duplicate.reply);
    EXPECT_EQ(duplicate.reply->error, DestinationError::kDuplicate);
    EXPECT_EQ(writers.load(), 1);
    for (size_t i = 0; i < backing->size(); ++i)
        EXPECT_EQ((*backing)[i],
                  i >= 20 && i < 52 ? std::byte{0x3B} : std::byte{0xA5});
    auto ack =
        CallSsdControlRpc("127.0.0.1", owner_server.port(),
                          {SsdControlCommand::kForget, prepared.reference, {}},
                          std::chrono::seconds(1));
    ASSERT_TRUE(ack.reply);
    EXPECT_EQ(ack.reply->error, DestinationError::kOk);
    EXPECT_EQ(grants->Forget(owner, 1), DestinationError::kOk);
    EXPECT_EQ(registration->Revoke(region.region), DestinationError::kOk);
}
TEST_F(SsdControlRpcTest, RejectsOversizedRequestHeaderBeforeBodyAllocation) {
    auto ledger = std::make_shared<SsdOwnerRequestLedger>(owner);
    SsdControlRpcEndpoint endpoint({}, ledger);
    SsdControlRpcServer server(1, 0, "127.0.0.1");
    server.register_handler<&SsdControlRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    auto socket = Connect(server.port());
    ASSERT_GE(socket.fd, 0);
    coro_rpc::protocol::coro_rpc_protocol::req_header header{};
    header.magic = 21;
    header.length = UINT32_MAX;
    auto wire =
        struct_pack::serialize<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
            header);
    ASSERT_EQ(send(socket.fd, wire.data(), wire.size(), MSG_NOSIGNAL),
              ssize_t(wire.size()));
    pollfd p{socket.fd, POLLIN, 0};
    ASSERT_EQ(poll(&p, 1, 1000), 1);
    char byte;
    EXPECT_EQ(recv(socket.fd, &byte, 1, 0), 0);
    EXPECT_EQ(ledger->stats().records, 0);
}
TEST_F(SsdControlRpcTest,
       ClientRejectsOversizedResponseHeaderWithoutReadingBody) {
    Socket listener{socket(AF_INET, SOCK_STREAM, 0)};
    ASSERT_GE(listener.fd, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ASSERT_EQ(bind(listener.fd, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)),
              0);
    ASSERT_EQ(listen(listener.fd, 1), 0);
    socklen_t length = sizeof(address);
    ASSERT_EQ(getsockname(listener.fd, reinterpret_cast<sockaddr*>(&address),
                          &length),
              0);
    std::thread peer([&] {
        pollfd p{listener.fd, POLLIN, 0};
        if (poll(&p, 1, 2000) != 1) return;
        Socket connection{accept(listener.fd, nullptr, nullptr)};
        if (connection.fd < 0) return;
        std::array<char, 20> head;
        if (recv(connection.fd, head.data(), head.size(), MSG_WAITALL) !=
            ssize_t(head.size()))
            return;
        coro_rpc::protocol::coro_rpc_protocol::req_header request;
        if (struct_pack::deserialize_to<
                struct_pack::sp_config::DISABLE_ALL_META_INFO>(
                request, std::string_view(head.data(), head.size())))
            return;
        std::string body(request.length, 'x');
        if (recv(connection.fd, body.data(), body.size(), MSG_WAITALL) !=
            ssize_t(body.size()))
            return;
        coro_rpc::protocol::coro_rpc_protocol::resp_header response{};
        response.magic = 21;
        response.seq_num = request.seq_num;
        response.length = UINT32_MAX;
        auto wire = struct_pack::serialize<
            struct_pack::sp_config::DISABLE_ALL_META_INFO>(response);
        send(connection.fd, wire.data(), wire.size(), MSG_NOSIGNAL);
    });
    SsdGrantReferenceV1 reference{requester, {owner, 1, 1}, {11, 12}};
    auto result = CallSsdControlRpc("127.0.0.1", ntohs(address.sin_port),
                                    {SsdControlCommand::kQuery, reference, {}},
                                    std::chrono::seconds(1));
    peer.join();
    EXPECT_EQ(result.error, DestinationError::kLimit);
    EXPECT_FALSE(result.reply);
}

class HostRegistration final : public DestinationRegistrationTransport {
   public:
    TransportRegistrationResult RegisterWritable(
        const DestinationBinding&, const std::string&) noexcept override {
        return {DestinationError::kOk, true, true};
    }
    DestinationError Unregister(uint64_t) noexcept override {
        return DestinationError::kOk;
    }
};
TEST_F(SsdControlRpcTest,
       CombinedRoleServerCanResolveAndQueryWhileWorkerIsBlocked) {
    auto buffer = std::make_shared<std::array<std::byte, 128>>();
    const auto base = reinterpret_cast<uint64_t>(buffer->data());
    auto registration = std::make_shared<DestinationRegistration>(
        requester, "cpu-test", std::make_shared<HostRegistration>());
    auto region =
        registration->Register({base, 128, base, 128}, "cpu:0", buffer);
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto grants = std::make_shared<SsdRequesterGrants>(requester, registration);
    auto ledger = std::make_shared<SsdOwnerRequestLedger>(owner);
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 1, 1},
        "default",
        1000,
        0,
        {{"key", 4096, {}, {{0, {{region.region, 0, 32}}}}}}};
    auto prepared = grants->Prepare(request);
    ASSERT_EQ(prepared.error, DestinationError::kOk);
    std::mutex mutex;
    std::condition_variable changed;
    bool release = false;
    std::promise<bool> resolved;
    auto resolved_future = resolved.get_future();
    std::promise<void> finished;
    auto finished_future = finished.get_future();
    uint16_t port = 0;
    SsdControlRpcEndpoint endpoint(
        grants, ledger,
        [&](const SsdReadRequestV1& req, const SsdGrantReferenceV1& ref,
            SsdOwnerRequestLedger::Operation operation) {
            EXPECT_EQ(operation.Start(), DestinationError::kOk);
            auto grant = CallSsdControlRpc(
                "127.0.0.1", port, {SsdControlCommand::kResolve, ref, req},
                std::chrono::milliseconds(250));
            const bool ok = grant.error == DestinationError::kOk &&
                            grant.reply &&
                            grant.reply->error == DestinationError::kOk;
            resolved.set_value(ok);
            if (!ok) {
                finished.set_value();
                return;
            }
            std::unique_lock lock(mutex);
            changed.wait(lock, [&] { return release; });
            lock.unlock();
            EXPECT_EQ(
                operation.Complete({1,
                                    req.attempt,
                                    true,
                                    {{0, AttemptOutcome::kCancelled, 0, {}}},
                                    requester}),
                DestinationError::kOk);
            finished.set_value();
        });
    SsdControlRpcServer server(1, 0, "127.0.0.1");
    server.register_handler<&SsdControlRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    port = server.port();
    auto submitted = CallSsdControlRpc(
        "127.0.0.1", port,
        {SsdControlCommand::kSubmit, prepared.reference, request},
        std::chrono::seconds(1));
    EXPECT_EQ(submitted.error, DestinationError::kOk);
    const auto ready = resolved_future.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(ready, std::future_status::ready);
    if (ready == std::future_status::ready) {
        EXPECT_TRUE(resolved_future.get());
    }
    auto running = CallSsdControlRpc(
        "127.0.0.1", port, {SsdControlCommand::kQuery, prepared.reference, {}},
        std::chrono::milliseconds(250));
    EXPECT_EQ(running.error, DestinationError::kOk);
    if (running.reply && running.reply->snapshot) {
        EXPECT_EQ(running.reply->snapshot->state,
                  SsdOwnerRequestState::kRunning);
        EXPECT_FALSE(running.reply->snapshot->result);
    } else
        ADD_FAILURE() << "Missing running snapshot";
    EXPECT_EQ(grants->Timeout(prepared.reference), DestinationError::kOk);
    {
        std::lock_guard lock(mutex);
        release = true;
    }
    changed.notify_all();
    EXPECT_EQ(finished_future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    auto terminal = ledger->Query(prepared.reference);
    if (terminal.result)
        EXPECT_EQ(grants->Complete(prepared.reference, *terminal.result),
                  DestinationError::kOk);
    else {
        ADD_FAILURE() << "Missing terminal drain";
        // Test worker submitted no data I/O and has exited; clean the RED path.
        grants->Complete(prepared.reference,
                         {1,
                          request.attempt,
                          true,
                          {{0, AttemptOutcome::kCancelled, 0, {}}},
                          requester});
    }
    EXPECT_EQ(registration->Revoke(region.region), DestinationError::kOk);
}

TEST_F(SsdControlRpcTest, NumericAddressPolicyCannotBlockOnDns) {
    SsdGrantReferenceV1 ref{requester, {owner, 1, 1}, {11, 12}};
    auto call =
        CallSsdControlRpc("localhost", 9, {SsdControlCommand::kQuery, ref, {}},
                          std::chrono::milliseconds(100));
    EXPECT_EQ(call.transport_error,
              std::make_error_code(std::errc::invalid_argument));
    EXPECT_FALSE(call.reply);
}
TEST_F(
    SsdControlRpcTest,
    SaturatedWorkerQueueRecordsZeroByteRejectionWithoutStartingAnotherWriter) {
    auto buffer = std::make_shared<std::array<std::byte, 128>>();
    const auto base = reinterpret_cast<uint64_t>(buffer->data());
    auto registration = std::make_shared<DestinationRegistration>(
        requester, "cpu-test", std::make_shared<HostRegistration>());
    auto region =
        registration->Register({base, 128, base, 128}, "cpu:0", buffer);
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto grants = std::make_shared<SsdRequesterGrants>(requester, registration);
    auto ledger = std::make_shared<SsdOwnerRequestLedger>(owner);
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 1, 1},
        "default",
        1000,
        0,
        {{"key", 4096, {}, {{0, {{region.region, 0, 32}}}}}}};
    auto first = grants->Prepare(request);
    ASSERT_EQ(first.error, DestinationError::kOk);
    auto second_request = request;
    second_request.attempt.request_id = 2;
    second_request.objects[0].ranges[0].destinations[0].offset = 32;
    auto second = grants->Prepare(second_request);
    ASSERT_EQ(second.error, DestinationError::kOk);
    std::mutex mutex;
    std::condition_variable changed;
    bool release = false;
    std::promise<void> entered, finished;
    auto entered_future = entered.get_future(),
         finished_future = finished.get_future();
    std::atomic<size_t> writers{0};
    SsdControlRpcEndpoint endpoint(
        grants, ledger,
        [&](const SsdReadRequestV1& req, const SsdGrantReferenceV1& ref,
            SsdOwnerRequestLedger::Operation operation) {
            ++writers;
            EXPECT_EQ(operation.Start(), DestinationError::kOk);
            EXPECT_EQ(grants->Resolve(ref, req).error, DestinationError::kOk);
            entered.set_value();
            std::unique_lock lock(mutex);
            changed.wait(lock, [&] { return release; });
            lock.unlock();
            EXPECT_EQ(
                operation.Complete({1,
                                    req.attempt,
                                    true,
                                    {{0, AttemptOutcome::kCancelled, 0, {}}},
                                    requester}),
                DestinationError::kOk);
            finished.set_value();
        },
        {}, 1, 1);
    SsdControlRpcServer server(1, 0, "127.0.0.1");
    server.register_handler<&SsdControlRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    auto started = CallSsdControlRpc(
        "127.0.0.1", server.port(),
        {SsdControlCommand::kSubmit, first.reference, request},
        std::chrono::seconds(1));
    EXPECT_EQ(started.error, DestinationError::kOk);
    EXPECT_EQ(entered_future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    auto rejected = CallSsdControlRpc(
        "127.0.0.1", server.port(),
        {SsdControlCommand::kSubmit, second.reference, second_request},
        std::chrono::seconds(1));
    EXPECT_EQ(rejected.error, DestinationError::kOk);
    if (rejected.reply && rejected.reply->snapshot &&
        rejected.reply->snapshot->result) {
        EXPECT_EQ(rejected.reply->snapshot->result->objects[0].outcome,
                  AttemptOutcome::kRejected);
        EXPECT_EQ(rejected.reply->snapshot->result->objects[0].valid_bytes, 0);
        EXPECT_EQ(grants->Complete(second.reference,
                                   *rejected.reply->snapshot->result),
                  DestinationError::kOk);
    } else {
        ADD_FAILURE() << "Queue rejection must cache a no-I/O terminal";
        grants->Timeout(second.reference);
    }
    EXPECT_EQ(writers.load(), 1);
    EXPECT_EQ(ledger->stats().active_payload_bytes, 32);
    {
        std::lock_guard lock(mutex);
        release = true;
    }
    changed.notify_all();
    EXPECT_EQ(finished_future.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    auto terminal = ledger->Query(first.reference);
    if (terminal.result) grants->Complete(first.reference, *terminal.result);
    EXPECT_EQ(registration->Revoke(region.region), DestinationError::kOk);
}
TEST_F(SsdControlRpcTest,
       WorkerExceptionKeepsQuarantineAndCannotFabricateDrain) {
    auto buffer = std::make_shared<std::array<std::byte, 128>>();
    const auto base = reinterpret_cast<uint64_t>(buffer->data());
    auto registration = std::make_shared<DestinationRegistration>(
        requester, "cpu-test", std::make_shared<HostRegistration>());
    auto region =
        registration->Register({base, 128, base, 128}, "cpu:0", buffer);
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto grants = std::make_shared<SsdRequesterGrants>(requester, registration);
    auto ledger = std::make_shared<SsdOwnerRequestLedger>(owner);
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 1, 1},
        "default",
        1000,
        0,
        {{"key", 4096, {}, {{0, {{region.region, 0, 32}}}}}}};
    auto prepared = grants->Prepare(request);
    ASSERT_EQ(prepared.error, DestinationError::kOk);
    SsdControlRpcEndpoint endpoint(
        grants, ledger,
        [&](const SsdReadRequestV1& req, const SsdGrantReferenceV1& ref,
            SsdOwnerRequestLedger::Operation operation) {
            EXPECT_EQ(operation.Start(), DestinationError::kOk);
            EXPECT_EQ(grants->Resolve(ref, req).error, DestinationError::kOk);
            throw std::runtime_error("injected worker failure before data I/O");
        });
    SsdControlRpcServer server(1, 0, "127.0.0.1");
    server.register_handler<&SsdControlRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    auto submit = CallSsdControlRpc(
        "127.0.0.1", server.port(),
        {SsdControlCommand::kSubmit, prepared.reference, request},
        std::chrono::seconds(1));
    EXPECT_EQ(submit.error, DestinationError::kOk);
    SsdOwnerRequestSnapshot state;
    const auto until =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    do {
        state = ledger->Query(prepared.reference);
        if (state.state == SsdOwnerRequestState::kQuarantined) break;
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < until);
    EXPECT_EQ(state.state, SsdOwnerRequestState::kQuarantined);
    EXPECT_FALSE(state.result);
    EXPECT_EQ(ledger->stats().active_payload_bytes, 32);
    EXPECT_EQ(grants->Timeout(prepared.reference), DestinationError::kOk);
    EXPECT_EQ(registration->Revoke(region.region), DestinationError::kBusy);
    // The injected worker never submitted data I/O and has exited. This local
    // test-only proof cleans the destination; the RPC path returned no drain.
    EXPECT_EQ(grants->Complete(prepared.reference,
                               {1,
                                request.attempt,
                                true,
                                {{0, AttemptOutcome::kCancelled, 0, {}}},
                                requester}),
              DestinationError::kOk);
    EXPECT_EQ(registration->Revoke(region.region), DestinationError::kOk);
}
}  // namespace
}  // namespace mooncake::ssd_offload

namespace mooncake::ssd_offload {
namespace {
TEST_F(SsdControlRpcTest,
       DirectReadRepliesOnceAfterActualWriteWithoutBlockingRpcIo) {
    struct Copy final : DrainedWriteTarget {
        explicit Copy(std::vector<ResolvedDestination> v)
            : views(std::move(v)) {}
        Error WriteAndDrain(const ReadChunk& chunk,
                            SlotBuffer slot) noexcept override {
            for (const auto& f : chunk.fragments)
                std::memcpy(reinterpret_cast<void*>(
                                views[f.destination.region_id - 1].address),
                            slot.data + f.buffer_offset,
                            size_t(f.destination.length));
            return Error::kOk;
        }
        std::vector<ResolvedDestination> views;
    };
    std::array<std::byte, 32> destination{};
    std::promise<void> entered, resume;
    auto ready = entered.get_future();
    auto resumed = resume.get_future().share();
    std::atomic<size_t> reads{0};
    auto direct = std::make_shared<SsdDirectReadOwner>(
        owner,
        std::vector<SsdFixedL2Peer>{
            {requester, "default", "requester-te", {7, 8}}},
        [](auto views) { return std::make_shared<Copy>(std::move(views)); },
        [&](const auto& request, auto target,
            auto) -> std::optional<SsdReadResultV1> {
            ++reads;
            entered.set_value();
            resumed.wait();
            std::array<std::byte, 32> staging;
            staging.fill(std::byte{0x3B});
            ReadChunk chunk{0, 0, 32, 0, 32, {{0, {1, 1, 0, 32}}}};
            EXPECT_EQ(
                target->WriteAndDrain(chunk, {staging.data(), staging.size()}),
                Error::kOk);
            return SsdReadResultV1{1,
                                   request.attempt,
                                   true,
                                   {{0, AttemptOutcome::kSucceeded, 32, {}}},
                                   requester};
        });
    SsdReadIntoRpcEndpoint endpoint(direct, {}, 1, 2);
    SsdControlRpcServer server(1, 0, "127.0.0.1");
    server.register_handler<&SsdReadIntoRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 1, 1},
        "default",
        2000,
        0,
        {{"key", 32, {}, {{0, {{{requester, 1, 1, {9, 10}}, 0, 32}}}}}}};
    SsdControlMessage message{
        SsdControlCommand::kReadInto,
        {requester, request.attempt, {7, 8}},
        request,
        {{"requester-te", reinterpret_cast<uint64_t>(destination.data()), 32,
          DestinationMemory::kHost}}};
    auto result = std::async(std::launch::async, [&] {
        return CallSsdControlRpc("127.0.0.1", server.port(), message,
                                 std::chrono::seconds(2));
    });
    const auto started = ready.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(started, std::future_status::ready);
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    auto recovery_message = message;
    recovery_message.command = SsdControlCommand::kRecoverReadInto;
    auto recovery =
        CallSsdControlRpc("127.0.0.1", server.port(), recovery_message,
                          std::chrono::milliseconds(100));
    // A saturated read worker must not block the control I/O/recovery path.
    EXPECT_EQ(recovery.error, DestinationError::kOk);
    EXPECT_TRUE(recovery.reply);
    resume.set_value();
    auto call = result.get();
    ASSERT_EQ(call.error, DestinationError::kOk);
    ASSERT_TRUE(call.reply);
    ASSERT_EQ(call.reply->error, DestinationError::kOk);
    ASSERT_TRUE(call.reply->snapshot);
    ASSERT_TRUE(call.reply->snapshot->result);
    EXPECT_TRUE(call.reply->snapshot->result->dma_drained);
    EXPECT_EQ(destination[0], std::byte{0x3B});
    EXPECT_EQ(reads, 1u);
    endpoint.CloseAndDrain();
    server.stop();
}
}  // namespace
}  // namespace mooncake::ssd_offload
