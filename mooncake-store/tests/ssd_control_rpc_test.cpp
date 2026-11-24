// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_control_rpc.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstring>
#include <future>
#include <mutex>
#include <thread>
#include <vector>
#include <ylt/struct_pack.hpp>

#include "ssd_fixed_l2_requester.h"

namespace mooncake::ssd_offload {
namespace {

constexpr PeerIdentity kRequester{0x1122334455667788ULL, 0x8877665544332211ULL,
                                  3};
constexpr PeerIdentity kOwner{0xAABBCCDDEEFF0011ULL, 0x1100FFEEDDCCBBAAULL, 6};
constexpr WriteCredential kSecret{0xCAFEBABE12345678ULL, 0xDEADBEEF87654321ULL};

struct ScopedSocket {
    int fd{-1};
    ~ScopedSocket() {
        if (fd >= 0) close(fd);
    }
};

ScopedSocket ConnectLoopback(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return {};
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return {};
    }
    return {fd};
}

bool SendAll(int fd, const void* data, size_t n) {
    const auto* p = static_cast<const char*>(data);
    size_t off = 0;
    while (off < n) {
        ssize_t w = send(fd, p + off, n - off,
#ifdef MSG_NOSIGNAL
                         MSG_NOSIGNAL
#else
                         0
#endif
        );
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return false;
        off += size_t(w);
    }
    return true;
}

bool RecvAll(int fd, void* data, size_t n) {
    auto* p = static_cast<char*>(data);
    size_t off = 0;
    while (off < n) {
        ssize_t r = recv(fd, p + off, n - off, 0);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return false;
        off += size_t(r);
    }
    return true;
}

// TCP MITM proxy that captures raw wire bytes, optionally flips a bit on the
// first connection, and forwards subsequent connections cleanly (for recovery).
struct MitmProxy {
    ScopedSocket listener{socket(AF_INET, SOCK_STREAM, 0)};
    uint16_t port{0};
    bool tamper_request{false};
    bool tamper_reply{false};
    size_t max_connections{1};
    std::string captured_request;
    std::string captured_reply;
    std::thread thread;

    MitmProxy(uint16_t upstream_port, bool flip_req, bool flip_rep,
              size_t connections = 1)
        : tamper_request(flip_req),
          tamper_reply(flip_rep),
          max_connections(connections) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (listener.fd < 0 ||
            bind(listener.fd, reinterpret_cast<sockaddr*>(&addr),
                 sizeof(addr)) != 0 ||
            listen(listener.fd, 4) != 0) {
            return;
        }
        socklen_t len = sizeof(addr);
        if (getsockname(listener.fd, reinterpret_cast<sockaddr*>(&addr),
                        &len) != 0) {
            return;
        }
        port = ntohs(addr.sin_port);
        thread = std::thread([this, upstream_port] {
            for (size_t conn = 0; conn < max_connections; ++conn) {
                pollfd pfd{listener.fd, POLLIN, 0};
                if (poll(&pfd, 1, 3000) != 1) return;
                ScopedSocket in{accept(listener.fd, nullptr, nullptr)};
                ScopedSocket out = ConnectLoopback(upstream_port);
                if (in.fd < 0 || out.fd < 0) return;

                std::array<char, 20> req_head{};
                if (!RecvAll(in.fd, req_head.data(), req_head.size())) return;
                coro_rpc::protocol::coro_rpc_protocol::req_header req{};
                if (struct_pack::deserialize_to<
                        struct_pack::sp_config::DISABLE_ALL_META_INFO>(
                        req,
                        std::string_view(req_head.data(), req_head.size())) ||
                    req.length > kSsdControlMaxRpcBody) {
                    return;
                }
                std::string req_body(req.length, '\0');
                if (!RecvAll(in.fd, req_body.data(), req_body.size())) return;
                if (conn == 0) {
                    captured_request = req_body;
                    if (tamper_request && !req_body.empty()) {
                        req_body.back() ^= 0x01;
                    }
                }
                if (!SendAll(out.fd, req_head.data(), req_head.size()) ||
                    !SendAll(out.fd, req_body.data(), req_body.size())) {
                    return;
                }

                std::array<char, 16> rep_head{};
                if (!RecvAll(out.fd, rep_head.data(), rep_head.size())) return;
                coro_rpc::protocol::coro_rpc_protocol::resp_header rep{};
                if (struct_pack::deserialize_to<
                        struct_pack::sp_config::DISABLE_ALL_META_INFO>(
                        rep,
                        std::string_view(rep_head.data(), rep_head.size())) ||
                    rep.length > kSsdControlMaxRpcBody) {
                    return;
                }
                std::string rep_body(rep.length, '\0');
                if (!RecvAll(out.fd, rep_body.data(), rep_body.size())) return;
                if (conn == 0) {
                    captured_reply = rep_body;
                    if (tamper_reply && !rep_body.empty()) {
                        rep_body.back() ^= 0x01;
                    }
                }
                SendAll(in.fd, rep_head.data(), rep_head.size());
                SendAll(in.fd, rep_body.data(), rep_body.size());
            }
        });
    }

    ~MitmProxy() {
        if (thread.joinable()) thread.join();
    }
};

struct NoopWriteTarget final : SourceDrainedWriteTarget {
    Error WriteAndDrain(const ReadChunk&, SlotBuffer) noexcept override {
        return Error::kOk;
    }
    bool DestinationsDrained() const noexcept override { return true; }
};

SsdPushPeerInfo MakeRoute(uint16_t port) {
    return {kOwner,
            "default",
            "owner-legacy",
            "127.0.0.1",
            port,
            "owner-te",
            {1,
             kOwner,
             15,
             64,
             1024ULL * 1024 * 1024,
             65536,
             1024ULL * 1024 * 1024,
             1024 * 1024,
             {"tcp"}}};
}

class SsdControlRpcTest : public ::testing::Test {
   protected:
    SsdFixedL2Peer req_peer_{kRequester, "default", "req-te", kSecret};

    std::shared_ptr<SsdDirectReadOwner> MakeOwner(
        SsdOwnerCallbacks::Reader reader) {
        return std::make_shared<SsdDirectReadOwner>(
            kOwner, std::vector<SsdFixedL2Peer>{req_peer_},
            [](std::vector<ResolvedDestination>) {
                return std::make_shared<NoopWriteTarget>();
            },
            std::move(reader));
    }
};

TEST_F(SsdControlRpcTest, ValidateHeaderBoundsBeforeAllocation) {
    EXPECT_EQ(ValidateSsdControlRpcRequestHeader(1024, 0),
              DestinationError::kOk);
    EXPECT_EQ(ValidateSsdControlRpcRequestHeader(kSsdControlMaxRpcBody, 0),
              DestinationError::kOk);
    EXPECT_EQ(ValidateSsdControlRpcRequestHeader(kSsdControlMaxRpcBody + 1, 0),
              DestinationError::kLimit);
    EXPECT_EQ(ValidateSsdControlRpcRequestHeader(128, 1),
              DestinationError::kLimit);
}

TEST_F(SsdControlRpcTest, AuthenticatedReadIntoAndPlaintextNeverOnWire) {
    auto owner = MakeOwner([](const SsdReadRequestV1& req,
                              std::shared_ptr<SourceDrainedWriteTarget>,
                              std::chrono::steady_clock::time_point)
                               -> std::optional<SsdReadResultV1> {
        SsdReadResultV1 res{1, req.attempt, true, {}, req.requester};
        for (size_t i = 0; i < req.objects.size(); ++i) {
            res.objects.push_back({i, AttemptOutcome::kSucceeded,
                                   req.objects[i].expected_object_size,
                                   std::nullopt});
        }
        return res;
    });

    SsdControlRpcServer server(2, 0, "127.0.0.1", std::chrono::seconds(30));
    SsdReadIntoRpcEndpoint endpoint(owner);
    server.register_handler<&SsdReadIntoRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());

    MitmProxy observer(server.port(), /*flip_req=*/false, /*flip_rep=*/false);
    ASSERT_NE(observer.port, 0);

    SsdFixedL2Requester requester(req_peer_, "tcp",
                                  MakeSsdRequesterControlCall());
    auto buf = std::make_shared<std::vector<char>>(4096, 0);
    const auto addr = reinterpret_cast<uint64_t>(buf->data());
    auto reg = requester.Register(
        {addr, buf->size(), addr, buf->size(), DestinationMemory::kHost}, buf);
    ASSERT_EQ(reg.error, DestinationError::kOk);

    std::vector<SsdObjectReadV1> objects{
        {"k1", 1024, {}, {{0, {{reg.region, 0, 1024}}}}}};
    auto res = requester.ReadWithRecovery(
        MakeRoute(observer.port), std::move(objects), std::chrono::seconds(5));
    ASSERT_EQ(res.error, DestinationError::kOk);
    ASSERT_TRUE(res.result);
    EXPECT_EQ(res.result->objects[0].outcome, AttemptOutcome::kSucceeded);
    EXPECT_EQ(endpoint.read_requests(), 1u);

    // Wait for proxy thread to finish capturing bytes.
    if (observer.thread.joinable()) observer.thread.join();
    ASSERT_FALSE(observer.captured_request.empty());
    ASSERT_FALSE(observer.captured_reply.empty());

    // Raw 128-bit credential must NEVER appear in plaintext on the wire.
    std::string raw_high(8, '\0'), raw_low(8, '\0');
    std::memcpy(raw_high.data(), &kSecret.high, 8);
    std::memcpy(raw_low.data(), &kSecret.low, 8);
    EXPECT_EQ(observer.captured_request.find(raw_high), std::string::npos);
    EXPECT_EQ(observer.captured_request.find(raw_low), std::string::npos);
    EXPECT_EQ(observer.captured_reply.find(raw_high), std::string::npos);
    EXPECT_EQ(observer.captured_reply.find(raw_low), std::string::npos);

    EXPECT_EQ(requester.Revoke(reg.region), DestinationError::kOk);
    EXPECT_EQ(requester.Close(), DestinationError::kOk);
    endpoint.CloseAndDrain();
    server.stop();
}

TEST_F(SsdControlRpcTest, TamperedRequestAndForgedReplyFailClosed) {
    auto owner = MakeOwner([](const SsdReadRequestV1& req,
                              std::shared_ptr<SourceDrainedWriteTarget>,
                              std::chrono::steady_clock::time_point)
                               -> std::optional<SsdReadResultV1> {
        SsdReadResultV1 res{1, req.attempt, true, {}, req.requester};
        res.objects.push_back(
            {0, AttemptOutcome::kSucceeded, 1024, std::nullopt});
        return res;
    });

    SsdControlRpcServer server(2, 0, "127.0.0.1", std::chrono::seconds(30));
    SsdReadIntoRpcEndpoint endpoint(owner);
    server.register_handler<&SsdReadIntoRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());

    for (const bool flip_req : {true, false}) {
        SCOPED_TRACE(flip_req ? "tamper_request" : "tamper_reply");
        MitmProxy proxy(server.port(), flip_req, !flip_req, /*connections=*/2);
        ASSERT_NE(proxy.port, 0);

        SsdFixedL2Requester requester(req_peer_, "tcp",
                                      MakeSsdRequesterControlCall());
        auto buf = std::make_shared<std::vector<char>>(4096, 0);
        const auto addr = reinterpret_cast<uint64_t>(buf->data());
        auto reg = requester.Register(
            {addr, buf->size(), addr, buf->size(), DestinationMemory::kHost},
            buf);
        ASSERT_EQ(reg.error, DestinationError::kOk);

        std::vector<SsdObjectReadV1> objects{
            {"k1", 1024, {}, {{0, {{reg.region, 0, 1024}}}}}};
        auto res = requester.Read(MakeRoute(proxy.port), std::move(objects),
                                  std::chrono::seconds(2));
        EXPECT_EQ(res.error, DestinationError::kUnauthorized);
        EXPECT_FALSE(res.result.has_value());
        EXPECT_FALSE(res.allow_fallback);

        // Recover over the clean second proxy connection so the buffer drains.
        auto rec = requester.Recover(res.attempt, std::chrono::seconds(2));
        // Recovery never turns a tampered/failed read into a publishable kOk
        // result, but it does observe a signed drained terminal snapshot so
        // the destination buffer can be safely revoked and closed.
        EXPECT_TRUE(rec.result.has_value());
        EXPECT_TRUE(rec.result->dma_drained);
        EXPECT_EQ(requester.Revoke(reg.region), DestinationError::kOk);
        EXPECT_EQ(requester.Close(), DestinationError::kOk);
        if (proxy.thread.joinable()) proxy.thread.join();
    }

    endpoint.CloseAndDrain();
    server.stop();
}

TEST_F(SsdControlRpcTest,
       RecoverySucceedsOffIoLoopWhenReadWorkersAreSaturated) {
    std::promise<void> blocked_started, release_blocked;
    auto release_future = release_blocked.get_future().share();
    std::atomic<int> calls{0};

    auto owner = MakeOwner([&](const SsdReadRequestV1& req,
                               std::shared_ptr<SourceDrainedWriteTarget>,
                               std::chrono::steady_clock::time_point)
                               -> std::optional<SsdReadResultV1> {
        if (calls.fetch_add(1) == 0) {
            blocked_started.set_value();
            release_future.wait();
        }
        SsdReadResultV1 res{1, req.attempt, true, {}, req.requester};
        res.objects.push_back(
            {0, AttemptOutcome::kSucceeded, 1024, std::nullopt});
        return res;
    });

    SsdControlRpcServer server(2, 0, "127.0.0.1", std::chrono::seconds(30));
    // Single read worker thread so one blocked read saturates the read pool.
    SsdReadIntoRpcEndpoint endpoint(owner, {}, /*worker_threads=*/1,
                                    /*max_outstanding=*/4);
    server.register_handler<&SsdReadIntoRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());

    SsdFixedL2Requester requester(req_peer_, "tcp",
                                  MakeSsdRequesterControlCall());
    auto buf = std::make_shared<std::vector<char>>(8192, 0);
    const auto addr = reinterpret_cast<uint64_t>(buf->data());
    auto reg = requester.Register(
        {addr, buf->size(), addr, buf->size(), DestinationMemory::kHost}, buf);
    ASSERT_EQ(reg.error, DestinationError::kOk);

    // Trigger a read that times out on the client while holding the sole read
    // worker busy on the server.
    auto blocked_read = std::async(std::launch::async, [&] {
        return requester.Read(
            MakeRoute(server.port()),
            {{"k1", 1024, {}, {{0, {{reg.region, 0, 1024}}}}}},
            std::chrono::milliseconds(150));
    });
    ASSERT_EQ(blocked_started.get_future().wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    auto timed_out = blocked_read.get();
    EXPECT_TRUE(timed_out.may_have_submitted);
    EXPECT_FALSE(timed_out.result.has_value());

    // While the sole read worker is still blocked, kRecoverReadInto must still
    // run on the dedicated recovery pool and return promptly (kInProgress).
    auto rec_in_flight =
        requester.Recover(timed_out.attempt, std::chrono::seconds(2));
    EXPECT_FALSE(rec_in_flight.result.has_value());

    release_blocked.set_value();

    // Once the read finishes, recovery observes the signed terminal result.
    bool drained = false;
    for (int i = 0; i < 50; ++i) {
        auto rec =
            requester.Recover(timed_out.attempt, std::chrono::seconds(2));
        if (rec.result) {
            drained = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    EXPECT_TRUE(drained);
    EXPECT_EQ(requester.Revoke(reg.region), DestinationError::kOk);
    EXPECT_EQ(requester.Close(), DestinationError::kOk);
    endpoint.CloseAndDrain();
    server.stop();
}

TEST_F(SsdControlRpcTest, RejectsWildcardConnectAddress) {
    auto call = MakeSsdRequesterControlCall();
    SsdControlMessage msg;
    msg.command = SsdControlCommand::kReadInto;
    msg.reference = {kRequester, {kOwner, 1, 1}, kSecret};
    msg.request = SsdReadRequestV1{
        1,
        kRequester,
        {kOwner, 1, 1},
        "default",
        1000,
        0,
        {{"k1", 64, {}, {{0, {{{kRequester, 1, 1, {9, 10}}, 0, 64}}}}}}};
    msg.destinations = {{"req-te", 0x10000, 64, DestinationMemory::kHost}};
    auto res = call("0.0.0.0", 12345, msg, std::chrono::milliseconds(200));
    EXPECT_EQ(res.error, DestinationError::kTransportError);
    auto res6 = call("::", 12345, msg, std::chrono::milliseconds(200));
    EXPECT_EQ(res6.error, DestinationError::kTransportError);
}

TEST_F(SsdControlRpcTest,
       UnauthenticatedFramesDoNotIncrementReadRequestsAndQueueFullSignsLimit) {
    std::promise<void> entered, release;
    auto release_future = release.get_future().share();
    auto owner = MakeOwner([&](const SsdReadRequestV1& req,
                               std::shared_ptr<SourceDrainedWriteTarget>,
                               std::chrono::steady_clock::time_point)
                               -> std::optional<SsdReadResultV1> {
        entered.set_value();
        release_future.wait();
        SsdReadResultV1 res{1, req.attempt, true, {}, req.requester};
        res.objects.push_back(
            {0, AttemptOutcome::kSucceeded, 1024, std::nullopt});
        return res;
    });

    SsdControlRpcServer server(2, 0, "127.0.0.1", std::chrono::seconds(30));
    // Max 1 queued/running request on workers_ so the 2nd request triggers
    // backpressure kLimit.
    SsdReadIntoRpcEndpoint endpoint(owner, {}, /*worker_threads=*/1,
                                    /*max_outstanding=*/1);
    server.register_handler<&SsdReadIntoRpcEndpoint::Call>(&endpoint);
    server.async_start();
    ASSERT_FALSE(server.get_errc());

    // 1. Unauthenticated request (wrong secret) must NOT increment
    // read_requests_.
    {
        auto bad_peer = req_peer_;
        bad_peer.credential.high ^= 0x1;
        SsdFixedL2Requester bad_req(bad_peer, "tcp",
                                    MakeSsdRequesterControlCall());
        auto buf = std::make_shared<std::vector<char>>(4096, 0);
        const auto addr = reinterpret_cast<uint64_t>(buf->data());
        auto reg = bad_req.Register(
            {addr, buf->size(), addr, buf->size(), DestinationMemory::kHost},
            buf);
        ASSERT_EQ(reg.error, DestinationError::kOk);
        auto res =
            bad_req.Read(MakeRoute(server.port()),
                         {{"k1", 1024, {}, {{0, {{reg.region, 0, 1024}}}}}},
                         std::chrono::seconds(2));
        EXPECT_EQ(res.error, DestinationError::kUnauthorized);
        EXPECT_EQ(endpoint.read_requests(), 0u);
        (void)bad_req.Close();
    }

    // 2. Saturate workers_ with 1 authenticated read, then send a 2nd
    // authenticated read and verify it receives a signed kLimit (NOT
    // kUnauthorized).
    SsdFixedL2Requester requester(req_peer_, "tcp",
                                  MakeSsdRequesterControlCall());
    auto buf = std::make_shared<std::vector<char>>(8192, 0);
    const auto addr = reinterpret_cast<uint64_t>(buf->data());
    auto reg = requester.Register(
        {addr, buf->size(), addr, buf->size(), DestinationMemory::kHost}, buf);
    ASSERT_EQ(reg.error, DestinationError::kOk);

    auto first = std::async(std::launch::async, [&] {
        return requester.Read(
            MakeRoute(server.port()),
            {{"k1", 1024, {}, {{0, {{reg.region, 0, 1024}}}}}},
            std::chrono::seconds(5));
    });
    ASSERT_EQ(entered.get_future().wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto second =
        requester.Read(MakeRoute(server.port()),
                       {{"k2", 1024, {}, {{0, {{reg.region, 2048, 1024}}}}}},
                       std::chrono::seconds(2));
    EXPECT_EQ(second.error, DestinationError::kLimit);
    EXPECT_EQ(endpoint.read_requests(), 1u);

    release.set_value();
    auto first_res = first.get();
    EXPECT_EQ(first_res.error, DestinationError::kOk);
    EXPECT_EQ(endpoint.read_requests(), 1u);

    // Drain second's quarantined record via recovery so Revoke/Close succeed.
    auto rec = requester.Recover(second.attempt, std::chrono::seconds(2));
    EXPECT_TRUE(rec.result.has_value());
    EXPECT_EQ(requester.Revoke(reg.region), DestinationError::kOk);
    EXPECT_EQ(requester.Close(), DestinationError::kOk);
    endpoint.CloseAndDrain();
    server.stop();
}

}  // namespace
}  // namespace mooncake::ssd_offload
