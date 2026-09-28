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

// PeerHealth, the control-plane calls that consult it, and the prober.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/types.h"
#include "tent/runtime/control_plane.h"
#include "tent/rpc/rpc.h"

namespace mooncake {
namespace tent {
namespace {

using namespace std::chrono_literals;
using Clock = PeerHealth::Clock;

PeerHealth::Config config(std::chrono::milliseconds cooldown = 5000ms) {
    PeerHealth::Config c;
    c.cooldown = cooldown;
    c.probe_interval = 0ms;
    return c;
}

// A fresh caller thread, so no pooled connection from an earlier test is used.
void onFreshThread(const std::function<void()>& fn) { std::thread(fn).join(); }

std::string startServer(
    CoroRpcAgent& server, int func_id,
    CoroRpcAgent::Function fn = [](const std::string_view&, std::string&) {}) {
    EXPECT_TRUE(server.registerFunction(func_id, fn).ok());
    uint16_t port = 0;
    EXPECT_TRUE(server.start(port).ok());
    return "127.0.0.1:" + std::to_string(port);
}

TEST(PeerHealthTest, MarkedPeerFailsFastUntilTheCooldownExpires) {
    PeerHealth health;
    health.configure(config());
    const auto t0 = Clock::now();

    EXPECT_FALSE(health.shouldFailFast("10.0.0.1:1", t0));
    health.markUnreachable("10.0.0.1:1", t0);
    EXPECT_TRUE(health.shouldFailFast("10.0.0.1:1", t0));
    EXPECT_TRUE(health.shouldFailFast("10.0.0.1:1", t0 + 4999ms));
    EXPECT_FALSE(health.shouldFailFast("10.0.0.1:1", t0 + 5000ms));
    EXPECT_FALSE(health.shouldFailFast("10.0.0.2:1", t0));
}

TEST(PeerHealthTest, RepeatedFailuresDoubleTheCooldownUpToTheCap) {
    PeerHealth health;
    health.configure(config());
    const auto t0 = Clock::now();

    health.markUnreachable("p", t0);           // 5 s
    health.markUnreachable("p", t0 + 5000ms);  // 10 s
    EXPECT_TRUE(health.shouldFailFast("p", t0 + 5000ms + 9999ms));
    EXPECT_FALSE(health.shouldFailFast("p", t0 + 5000ms + 10000ms));

    health.configure(config(40000ms));
    health.markUnreachable("q", t0);            // 40 s
    health.markUnreachable("q", t0 + 40000ms);  // 80 s, capped
    const auto capped = t0 + 40000ms + PeerHealth::kMaxCooldown;
    EXPECT_TRUE(health.shouldFailFast("q", capped - 1ms));
    EXPECT_FALSE(health.shouldFailFast("q", capped));
}

// Forgotten after kMaxAge even while its cooldown is still running.
TEST(PeerHealthTest, OldEntriesAreForgotten) {
    PeerHealth health;
    health.configure(config());
    const auto t0 = Clock::now();
    const auto age = PeerHealth::kMaxAge;

    health.markUnreachable("p", t0);
    health.markUnreachable("p", t0 + age - 1000ms);  // dead for 10 s more
    EXPECT_EQ(health.unreachablePeers(t0 + age).size(), 1u);
    EXPECT_TRUE(health.shouldFailFast("p", t0 + age));
    EXPECT_TRUE(health.unreachablePeers(t0 + age + 1ms).empty());
    EXPECT_FALSE(health.shouldFailFast("p", t0 + age + 1ms));
}

// Also without the prober: a new mark forgets the old entries.
TEST(PeerHealthTest, ANewMarkForgetsOldEntries) {
    PeerHealth health;
    health.configure(config());
    const auto t0 = Clock::now();
    const auto later = t0 + PeerHealth::kMaxAge + 1ms;

    health.markUnreachable("p", t0);
    health.markUnreachable("p", t0);  // 10 s
    health.markUnreachable("q", later);
    health.markUnreachable("p", later);  // forgotten: a first failure, 5 s
    EXPECT_FALSE(health.shouldFailFast("p", later + 5000ms));
}

TEST(PeerHealthTest, GuardedControlPlaneCallsFailFastOnceMarked) {
    auto& health = PeerHealth::instance();
    health.clear();
    health.configure(config());
    const std::string addr = "127.0.0.1:1";
    health.markUnreachable(addr);

    const auto started = Clock::now();
    std::string response;
    const Status desc = ControlClient::getSegmentDesc(addr, response);
    BootstrapDesc request, reply;
    const Status boot = ControlClient::bootstrap(addr, request, reply);
    Notification notifi;
    notifi.name = "n";
    notifi.msg = "m";
    const Status notify = ControlClient::notify(addr, notifi);
    EXPECT_LT(Clock::now() - started, 100ms);
    for (const Status* s : {&desc, &boot, &notify}) {
        EXPECT_TRUE(s->IsRpcServiceError()) << s->ToString();
        EXPECT_NE(s->message().find("marked unreachable"),
                  std::string_view::npos)
            << s->ToString();
    }
    health.clear();
}

TEST(PeerHealthTest, ExplicitProbeDialsAMarkedPeer) {
    auto& health = PeerHealth::instance();
    health.clear();
    health.configure(config());

    CoroRpcAgent server;
    const std::string addr = startServer(server, Probe);

    health.markUnreachable(addr);
    ASSERT_TRUE(health.shouldFailFast(addr));
    onFreshThread([&] { EXPECT_TRUE(ControlClient::probe(addr).ok()); });
    EXPECT_FALSE(health.shouldFailFast(addr));
    health.clear();
}

TEST(PeerHealthTest, ARequestTimeoutMarksThePeer) {
    auto& health = PeerHealth::instance();
    health.clear();
    health.configure(config());
    auto slow = [](const std::string_view&, std::string&) {
        std::this_thread::sleep_for(1500ms);
    };
    CoroRpcAgent server;
    const std::string addr = startServer(server, Notify, slow);
    ASSERT_TRUE(server.registerFunction(GetSegmentDesc, slow).ok());
    ASSERT_TRUE(server.registerFunction(Probe, slow).ok());
    ASSERT_TRUE(server.registerFunction(BootstrapRdma, slow).ok());
    const auto saved = ControlClient::requestTimeout();
    ControlClient::setRequestTimeout(200ms);

    const std::vector<std::function<Status()>> calls = {
        [&] { return ControlClient::notify(addr, Notification{}); },
        [&] {
            std::string response;
            return ControlClient::getSegmentDesc(addr, response);
        },
        [&] { return ControlClient::probe(addr); },
        [&] {
            BootstrapDesc request, reply;
            return ControlClient::bootstrap(addr, request, reply);
        },
    };
    for (const auto& call : calls) {
        onFreshThread([&] {
            const auto started = Clock::now();
            const Status status = call();
            EXPECT_LT(Clock::now() - started, 1000ms);
            EXPECT_TRUE(status.IsRpcServiceError()) << status.ToString();
        });
        EXPECT_TRUE(health.shouldFailFast(addr));
        health.clear();
    }
    ControlClient::setRequestTimeout(saved);
    std::this_thread::sleep_for(5000ms);  // let the handlers finish
}

TEST(PeerHealthTest, AReplyClearsTheMark) {
    PeerHealth health;
    health.configure(config());
    const auto t0 = Clock::now();

    health.markUnreachable("p", t0);
    ASSERT_TRUE(health.shouldFailFast("p", t0));
    EXPECT_EQ(health.unreachablePeers().size(), 1u);
    health.markReachable("p");
    EXPECT_FALSE(health.shouldFailFast("p", t0));
    EXPECT_TRUE(health.unreachablePeers().empty());
}

TEST(PeerHealthTest, ExpiredMarksStayListedForTheProber) {
    PeerHealth health;
    health.configure(config());
    const auto t0 = Clock::now();

    health.markUnreachable("p", t0);
    EXPECT_FALSE(health.shouldFailFast("p", t0 + 10s));
    EXPECT_EQ(health.unreachablePeers(t0 + 10s).size(), 1u);
}

TEST(PeerHealthTest, DisabledNeverFailsFast) {
    PeerHealth health;
    auto c = config();
    c.enabled = false;
    health.configure(c);
    const auto t0 = Clock::now();

    health.markUnreachable("p", t0);
    EXPECT_FALSE(health.shouldFailFast("p", t0));
    EXPECT_TRUE(health.unreachablePeers().empty());

    health.configure(config());
    health.markUnreachable("p", t0);
    health.configure(c);  // disabling drops existing marks
    EXPECT_TRUE(health.unreachablePeers().empty());
}

// A refused connect marks the peer; a probe of it still dials.
TEST(PeerHealthTest, RefusedConnectMarksThePeer) {
    auto& health = PeerHealth::instance();
    health.clear();
    health.configure(config());
    const std::string addr = "127.0.0.1:1";

    const Status first = ControlClient::probe(addr);
    EXPECT_TRUE(first.IsRpcServiceError()) << first.ToString();
    EXPECT_TRUE(health.shouldFailFast(addr));
    const Status again = ControlClient::probe(addr);
    EXPECT_EQ(again.message().find("marked unreachable"),
              std::string_view::npos)
        << again.ToString();
    health.clear();
}

TEST(PeerHealthTest, ProberClearsAMarkedPeerThatAnswers) {
    auto& health = PeerHealth::instance();
    health.clear();
    auto c = config(PeerHealth::kMaxCooldown);  // only the prober can clear it
    c.probe_interval = 50ms;
    health.configure(c);

    CoroRpcAgent server;
    const std::string addr = startServer(server, Probe);

    health.markUnreachable(addr);
    ASSERT_TRUE(health.shouldFailFast(addr));

    ControlService service("p2p", "", nullptr);
    uint16_t service_port = 0;
    ASSERT_TRUE(service.start(service_port).ok());

    const auto deadline = Clock::now() + 2s;
    while (health.shouldFailFast(addr) && Clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_FALSE(health.shouldFailFast(addr));
    EXPECT_TRUE(health.unreachablePeers().empty());

    health.clear();
    c.probe_interval = 0ms;
    health.configure(c);
}

// A prober already running idles once a later configuration sets the
// interval to zero.
TEST(PeerHealthTest, ProberIdlesOnceTheIntervalIsSetToZero) {
    auto& health = PeerHealth::instance();
    health.clear();
    auto c = config(PeerHealth::kMaxCooldown);
    c.probe_interval = 20ms;
    c.probe_timeout = 100ms;
    health.configure(c);

    std::atomic<int> probes{0};
    CoroRpcAgent server;
    const std::string addr =
        startServer(server, Probe, [&](const std::string_view&, std::string&) {
            ++probes;
            std::this_thread::sleep_for(300ms);  // times out: stays marked
        });
    health.markUnreachable(addr);

    ControlService service("p2p", "", nullptr);
    uint16_t service_port = 0;
    ASSERT_TRUE(service.start(service_port).ok());
    const auto deadline = Clock::now() + 2s;
    while (probes < 2 && Clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
    }
    ASSERT_GE(probes.load(), 2);

    c.probe_interval = 0ms;
    health.configure(c);
    std::this_thread::sleep_for(500ms);  // a dial in flight finishes
    const int seen = probes.load();
    std::this_thread::sleep_for(1500ms);  // longer than the idle loop's 1 s
    EXPECT_EQ(probes.load(), seen);

    health.clear();
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
