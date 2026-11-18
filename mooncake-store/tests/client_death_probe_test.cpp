#include "master_service_test_fixture.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;

// Claims a loopback port and releases it before returning, so connections to
// that port are refused: the death probe's definition of a dead owner.
uint32_t TakeFreeLocalPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    EXPECT_GE(fd, 0);
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    EXPECT_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t len = sizeof(addr);
    EXPECT_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    const uint32_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

// A loopback listener that accepts and immediately closes every connection:
// the death probe's definition of a live owner.
class LoopbackListener {
   public:
    LoopbackListener() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(listen_fd_, 0);
        int one = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        EXPECT_EQ(::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr),
                         sizeof(addr)),
                  0);
        socklen_t len = sizeof(addr);
        EXPECT_EQ(
            ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len),
            0);
        port_ = ntohs(addr.sin_port);
        EXPECT_EQ(::listen(listen_fd_, 8), 0);
        acceptor_ = std::thread([this] {
            while (running_) {
                const int conn = ::accept(listen_fd_, nullptr, nullptr);
                if (conn < 0) break;
                ::close(conn);
            }
        });
    }

    ~LoopbackListener() { Stop(); }

    LoopbackListener(const LoopbackListener&) = delete;
    LoopbackListener& operator=(const LoopbackListener&) = delete;

    uint32_t Port() const { return port_; }

    void Stop() {
        if (!running_.exchange(false)) {
            return;
        }
        // Wake accept(); it then fails and the loop exits.
        ::shutdown(listen_fd_, SHUT_RDWR);
        ::close(listen_fd_);
        if (acceptor_.joinable()) {
            acceptor_.join();
        }
    }

   private:
    uint32_t port_ = 0;
    int listen_fd_ = -1;
    std::atomic<bool> running_{true};
    std::thread acceptor_;
};

// The death probe tuned so a dead client is retired within seconds: one
// active-TTL window to become SUSPECTED, then fast probe rounds. The
// suspicion TTL is kept an hour away so only the probe, never the TTL, can
// retire the session during the test.
MasterServiceConfig DeathProbeConfig(int64_t active_ttl_sec = 1,
                                     int64_t suspicion_ttl_sec = 3600,
                                     int64_t interval_ms = 100,
                                     uint32_t timeout_ms = 200,
                                     uint32_t failures_threshold = 2) {
    return MasterServiceConfig::builder()
        .set_memory_allocator(BufferAllocatorType::OFFSET)
        .set_client_active_ttl_sec(active_ttl_sec)
        .set_client_suspicion_ttl_sec(suspicion_ttl_sec)
        .set_client_death_probe_enable(true)
        .set_client_death_probe_interval_ms(interval_ms)
        .set_client_death_probe_timeout_ms(timeout_ms)
        .set_client_death_probe_failures_threshold(failures_threshold)
        .build();
}

TEST_F(MasterServiceTest, DeathProbeClassifiesEndpointOutcomes) {
    MasterService service(DeathProbeConfig());
    MasterServiceTestPeer peer(service);

    EXPECT_EQ(peer.ProbeClientEndpointsForTesting(
                  {"127.0.0.1:" + std::to_string(TakeFreeLocalPort())}, 300),
              MasterService::ClientDeathProbeOutcome::kRefused);

    LoopbackListener listener;
    EXPECT_EQ(peer.ProbeClientEndpointsForTesting(
                  {"127.0.0.1:" + std::to_string(listener.Port())}, 300),
              MasterService::ClientDeathProbeOutcome::kReachable);

    // RFC 5737 documentation range: no route answers, so the probe times
    // out.
    EXPECT_EQ(peer.ProbeClientEndpointsForTesting({"192.0.2.1:8341"}, 300),
              MasterService::ClientDeathProbeOutcome::kTimeout);

    // Unparsable endpoints are skipped; nothing is refused, so the outcome
    // is a timeout rather than a reachable or refused verdict.
    EXPECT_EQ(peer.ProbeClientEndpointsForTesting(
                  {"", "localhost", "127.0.0.1"}, 300),
              MasterService::ClientDeathProbeOutcome::kTimeout);
}

TEST_F(MasterServiceTest, DeathProbeRetiresDeadClientAndFreesSegmentName) {
    MasterService service(DeathProbeConfig());
    const uint32_t dead_port = TakeFreeLocalPort();
    Segment segment = MakeSegment("death_probe_refused_segment");
    segment.te_endpoint = "127.0.0.1:" + std::to_string(dead_port);
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    // The monitor marks the client SUSPECTED after one active-TTL window and
    // the death probe retires it on the first refused connection, long before
    // the suspicion TTL (one hour) would expire.
    WaitUntil(
        [&] {
            return FindClientLivenessForTest(service, client_id) == nullptr;
        },
        std::chrono::seconds(15));
    EXPECT_FALSE(service.QuerySegmentStatusById(segment.id).has_value());

    // The original bug: a restarted client could not re-claim its segment
    // name while the dead client's registration lingered.
    Segment remount = MakeSegment(segment.name, segment.base, segment.size);
    remount.te_endpoint = segment.te_endpoint;
    ASSERT_TRUE(service.MountSegment(remount, generate_uuid()).has_value());
}

TEST_F(MasterServiceTest, DeathProbeKeepsReachableSuspectedClient) {
    MasterService service(DeathProbeConfig());
    LoopbackListener listener;
    Segment segment = MakeSegment("death_probe_reachable_segment");
    segment.te_endpoint = "127.0.0.1:" + std::to_string(listener.Port());
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    WaitUntil([&] {
        const auto record = FindClientLivenessForTest(service, client_id);
        return record != nullptr &&
               record->state() == ClientLivenessState::SUSPECTED;
    });
    // Several probe rounds pass while the endpoint keeps answering.
    std::this_thread::sleep_for(1500ms);
    const auto record = FindClientLivenessForTest(service, client_id);
    ASSERT_NE(record, nullptr);
    EXPECT_EQ(record->state(), ClientLivenessState::SUSPECTED);
    EXPECT_TRUE(service.QuerySegmentStatusById(segment.id).has_value());
}

TEST_F(MasterServiceTest, DeathProbeRetiresSegmentlessSuspectedClient) {
    MasterService service(DeathProbeConfig(/*interval_ms=*/50));
    Segment segment = MakeSegment("death_probe_segmentless_segment");
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());
    ASSERT_TRUE(service.UnmountSegment(segment.id, client_id));

    const auto liveness = FindClientLivenessForTest(service, client_id);
    ASSERT_TRUE(liveness);
    ASSERT_EQ(
        liveness->Evaluate(ClientLivenessRecord::Clock::now(),
                           std::chrono::seconds::zero(), std::chrono::hours(1)),
        ClientLivenessTransition::BECAME_SUSPECTED);

    // One round suffices: with no segment left to protect, a suspected client
    // is retired immediately instead of being probed.
    MasterServiceTestPeer(service).RunClientDeathProbeRoundForTesting();
    WaitUntil(
        [&] {
            return FindClientLivenessForTest(service, client_id) == nullptr;
        },
        std::chrono::seconds(5));
}

TEST_F(MasterServiceTest, DeathProbeRetiresSuspectedClientAfterTimeouts) {
    MasterService service(
        DeathProbeConfig(/*active_ttl_sec=*/1, /*suspicion_ttl_sec=*/3600,
                         /*interval_ms=*/100, /*timeout_ms=*/150,
                         /*failures_threshold=*/2));
    Segment segment = MakeSegment("death_probe_timeout_segment");
    // RFC 5737 documentation range: no route answers, so every probe times
    // out and the failure threshold retires the client.
    segment.te_endpoint = "192.0.2.1:8341";
    const UUID client_id = generate_uuid();
    ASSERT_TRUE(service.MountSegment(segment, client_id).has_value());

    WaitUntil(
        [&] {
            return FindClientLivenessForTest(service, client_id) == nullptr;
        },
        std::chrono::seconds(15));
    EXPECT_FALSE(service.QuerySegmentStatusById(segment.id).has_value());
}

}  // namespace
}  // namespace mooncake::test
