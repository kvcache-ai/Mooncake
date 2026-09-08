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
//
// Hardware-free tests of the TENT side of the MPComm transport. Everything the
// transport does to the provider goes through MpcommAdapter, so an injected
// fake is enough to drive the boundary: endpoint publication and parsing,
// single-flight connection, key-query retry, dynamic buffer refresh, the
// request and completion mapping, short-transfer handling, releasing each
// handle exactly once, and teardown.
//
// These need neither RDMA devices nor libmpcomm and therefore run in CI. What
// they deliberately do not cover is MPComm's own behaviour - slicing, NIC and
// QP selection, worker scheduling - which is the provider's responsibility and
// is exercised by mpcomm_transport_test.cpp on real hardware.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/transport/mpcomm/mpcomm_peer_registry.h"
#include "tent/transport/mpcomm/mpcomm_task_mapping.h"

namespace mooncake {
namespace tent {
namespace {

// Programmable stand-in for libmpcomm. Counts what the transport asked for and
// can be told to fail a given number of times, which is how the recovery paths
// are reached without a peer.
class FakeMpcommAdapter final : public MpcommAdapter {
   public:
    bool available() const noexcept override { return true; }

    Status init(const std::string &, const std::string &, int port) override {
        bound_port = port;
        return Status::OK();
    }
    int tcpPort() const override { return bound_port; }
    Status startAcceptThread() override {
        ++accept_thread_starts;
        return Status::OK();
    }
    void stopAcceptThread() override { ++accept_thread_stops; }
    void shutdown() override { ++shutdowns; }

    Status registerMemory(void *, size_t) override { return Status::OK(); }
    void unregisterMemory(void *) override {}
    Status publishBuffer(void *, size_t, int) override { return Status::OK(); }
    void unpublishBuffer(void *) override {}

    Status connect(const std::string &host_id, const std::string &tcp_addr,
                   int tcp_port) override {
        {
            std::lock_guard<std::mutex> lock(mutex);
            ++connect_calls;
            last_connect_host = host_id;
            last_connect_addr = tcp_addr;
            last_connect_port = tcp_port;
        }
        // Held open long enough that every thread in the single-flight test is
        // inside ensure() while the first connect is still running.
        if (connect_delay.count() > 0) {
            std::this_thread::sleep_for(connect_delay);
        }
        if (connect_failures > 0) {
            --connect_failures;
            return Status::InternalError("injected connect failure");
        }
        return Status::OK();
    }

    Status queryRemoteBuffer(const std::string &host_id,
                             const std::string &tcp_addr,
                             int tcp_port) override {
        std::lock_guard<std::mutex> lock(mutex);
        ++query_calls;
        last_query_host = host_id;
        last_query_addr = tcp_addr;
        last_query_port = tcp_port;
        if (query_failures > 0) {
            --query_failures;
            return Status::InternalError("injected query failure");
        }
        return Status::OK();
    }

    MpcommTransferHandle putAsync(uintptr_t local_addr,
                                  const std::string &host_id,
                                  uintptr_t remote_addr,
                                  size_t length) override {
        ++put_calls;
        return record(local_addr, host_id, remote_addr, length);
    }
    MpcommTransferHandle getAsync(uintptr_t local_addr,
                                  const std::string &host_id,
                                  uintptr_t remote_addr,
                                  size_t length) override {
        ++get_calls;
        return record(local_addr, host_id, remote_addr, length);
    }

    bool isTransferComplete(MpcommTransferHandle) override {
        return transfer_complete;
    }
    MpcommTransferOutcome getTransferResult(MpcommTransferHandle) override {
        ++result_calls;
        return outcome;
    }
    void releaseTransfer(MpcommTransferHandle handle) override {
        ++release_calls;
        last_released = handle;
    }

    // --- knobs ---
    int connect_failures{0};
    int query_failures{0};
    std::chrono::milliseconds connect_delay{0};
    bool issue_fails{false};
    bool transfer_complete{true};
    MpcommTransferOutcome outcome{};

    // --- observations ---
    mutable std::mutex mutex;
    int connect_calls{0};
    int query_calls{0};
    int accept_thread_starts{0};
    int accept_thread_stops{0};
    int shutdowns{0};
    int put_calls{0};
    int get_calls{0};
    int result_calls{0};
    int release_calls{0};
    int bound_port{0};
    std::string last_connect_host;
    std::string last_connect_addr;
    int last_connect_port{0};
    std::string last_query_host;
    std::string last_query_addr;
    int last_query_port{0};
    uintptr_t last_local_addr{0};
    uintptr_t last_remote_addr{0};
    size_t last_length{0};
    std::string last_transfer_host;
    MpcommTransferHandle last_released{kInvalidMpcommTransferHandle};

   private:
    MpcommTransferHandle record(uintptr_t local_addr,
                                const std::string &host_id,
                                uintptr_t remote_addr, size_t length) {
        last_local_addr = local_addr;
        last_remote_addr = remote_addr;
        last_length = length;
        last_transfer_host = host_id;
        if (issue_fails) return kInvalidMpcommTransferHandle;
        return ++next_handle;
    }

    MpcommTransferHandle next_handle{0};
};

std::vector<MpcommBufferRange> makeRanges(
    std::initializer_list<std::pair<uint64_t, uint64_t>> list) {
    std::vector<MpcommBufferRange> out;
    for (const auto &entry : list) {
        out.push_back(MpcommBufferRange{entry.first, entry.second});
    }
    return out;
}

// MpcommTask::status_word is volatile, which cannot bind to the const
// references gtest's matchers take, so read it out as a plain value first.
TransferStatusEnum statusOf(const MpcommTask &task) { return task.status_word; }

// ===================================================================
// Endpoint publication and parsing
// ===================================================================

TEST(MpcommEndpointAttr, PublishedFormatRoundTripsThroughTheParser) {
    // The published attribute is derived from the segment name, whose port is
    // the tent RPC port and must not be mistaken for MPComm's.
    auto attr = buildMpcommEndpointAttr("10.0.0.1:12345", 13579);
    EXPECT_EQ(attr, "v1:10.0.0.1:13579");

    std::string addr;
    int port = 0;
    ASSERT_TRUE(parseMpcommEndpointAttr(attr, addr, port).ok());
    EXPECT_EQ(addr, "10.0.0.1");
    EXPECT_EQ(port, 13579);
}

TEST(MpcommEndpointAttr, UnversionedAttributeIsReadAsV1) {
    // Interoperability with a peer that predates the version prefix.
    std::string addr;
    int port = 0;
    ASSERT_TRUE(parseMpcommEndpointAttr("10.0.0.1:13579", addr, port).ok());
    EXPECT_EQ(addr, "10.0.0.1");
    EXPECT_EQ(port, 13579);
}

TEST(MpcommEndpointAttr, UnknownVersionIsRejectedRatherThanMisparsed) {
    std::string addr;
    int port = 0;
    EXPECT_FALSE(parseMpcommEndpointAttr("v2:10.0.0.1:13579", addr, port).ok());
}

TEST(MpcommEndpointAttr, HostnameStartingWithVIsNotTakenForAVersion) {
    // The reason the prefix check requires 'v' + digits + ':' rather than just
    // a leading 'v'.
    std::string addr;
    int port = 0;
    ASSERT_TRUE(parseMpcommEndpointAttr("vm-node1:13579", addr, port).ok());
    EXPECT_EQ(addr, "vm-node1");
    EXPECT_EQ(port, 13579);

    ASSERT_TRUE(parseMpcommEndpointAttr("v1:vm-node1:13579", addr, port).ok());
    EXPECT_EQ(addr, "vm-node1");
}

TEST(MpcommEndpointAttr, Ipv6LiteralIsRejected) {
    // MPComm's handshake sockets are AF_INET, and rfind(':') would otherwise
    // split an IPv6 literal at the wrong colon.
    std::string addr;
    int port = 0;
    EXPECT_FALSE(parseMpcommEndpointAttr("fe80::1:13579", addr, port).ok());
    EXPECT_FALSE(parseMpcommEndpointAttr("v1:fe80::1:13579", addr, port).ok());
}

TEST(MpcommEndpointAttr, MalformedAttributesAreRejected) {
    std::string addr;
    int port = 0;
    EXPECT_FALSE(parseMpcommEndpointAttr("", addr, port).ok());
    EXPECT_FALSE(parseMpcommEndpointAttr("10.0.0.1", addr, port).ok());
    EXPECT_FALSE(parseMpcommEndpointAttr(":13579", addr, port).ok());
    EXPECT_FALSE(parseMpcommEndpointAttr("10.0.0.1:", addr, port).ok());
}

TEST(MpcommTcpPort, RejectsEverythingAtoiWouldAccept) {
    // atoi() turns each of these into 0 or a truncated value, which MPComm
    // would then try to bind.
    int port = 0;
    EXPECT_FALSE(parseMpcommTcpPort("", port).ok());
    EXPECT_FALSE(parseMpcommTcpPort("abc", port).ok());
    EXPECT_FALSE(parseMpcommTcpPort("13579x", port).ok());
    EXPECT_FALSE(parseMpcommTcpPort("0", port).ok());
    EXPECT_FALSE(parseMpcommTcpPort("-1", port).ok());
    EXPECT_FALSE(parseMpcommTcpPort("65536", port).ok());
    EXPECT_FALSE(parseMpcommTcpPort("123456", port).ok());

    ASSERT_TRUE(parseMpcommTcpPort("1", port).ok());
    EXPECT_EQ(port, 1);
    ASSERT_TRUE(parseMpcommTcpPort("65535", port).ok());
    EXPECT_EQ(port, 65535);
}

// ===================================================================
// Peer registry: single flight, retry, refresh, teardown
// ===================================================================

TEST(MpcommPeerRegistryTest, FirstEnsureConnectsThenQueries) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    MpcommPeerRegistry registry(adapter);

    ASSERT_TRUE(
        registry.ensure("peer", "10.0.0.1", 13579, makeRanges({{0x1000, 4096}}))
            .ok());
    EXPECT_EQ(adapter->connect_calls, 1);
    EXPECT_EQ(adapter->query_calls, 1);
    EXPECT_EQ(adapter->last_connect_addr, "10.0.0.1");
    EXPECT_EQ(adapter->last_connect_port, 13579);

    MpcommPeerState state{};
    ASSERT_TRUE(registry.stateOf("peer", state));
    EXPECT_EQ(state, MpcommPeerState::READY);
}

TEST(MpcommPeerRegistryTest, CachedPeerIsNotContactedAgain) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    MpcommPeerRegistry registry(adapter);
    auto buffers = makeRanges({{0x1000, 4096}});

    ASSERT_TRUE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());
    ASSERT_TRUE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());
    ASSERT_TRUE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());

    EXPECT_EQ(adapter->connect_calls, 1);
    EXPECT_EQ(adapter->query_calls, 1);
}

TEST(MpcommPeerRegistryTest, ConcurrentCallersConnectThePeerOnce) {
    // A second connect() to a connected host replaces MPComm's connection
    // record wholesale, dropping its keys and leaking its queue pairs, so this
    // is the property the whole ownership scheme exists to guarantee.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->connect_delay = std::chrono::milliseconds(100);
    MpcommPeerRegistry registry(adapter);
    auto buffers = makeRanges({{0x1000, 4096}});

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;
    std::atomic<int> failures{0};
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&] {
            if (!registry.ensure("peer", "10.0.0.1", 13579, buffers).ok()) {
                ++failures;
            }
        });
    }
    for (auto &thread : threads) thread.join();

    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(adapter->connect_calls, 1);
    EXPECT_EQ(adapter->query_calls, 1);
    EXPECT_EQ(registry.size(), 1u);
}

TEST(MpcommPeerRegistryTest, ConnectFailureLeavesNothingCached) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->connect_failures = 1;
    MpcommPeerRegistry registry(adapter);
    auto buffers = makeRanges({{0x1000, 4096}});

    EXPECT_FALSE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());
    // Nothing was established, so the entry must not linger and block a later
    // attempt from connecting.
    EXPECT_FALSE(registry.contains("peer"));
    EXPECT_EQ(adapter->query_calls, 0);

    ASSERT_TRUE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());
    EXPECT_EQ(adapter->connect_calls, 2);
}

TEST(MpcommPeerRegistryTest, QueryFailureKeepsTheConnectionAndRetriesTheQuery) {
    // The behaviour the CONNECTED_NO_KEYS state exists for: the connection
    // cannot be closed, so a failed key query must not lead to a reconnect.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->query_failures = 1;
    MpcommPeerRegistry registry(adapter);
    auto buffers = makeRanges({{0x1000, 4096}});

    EXPECT_FALSE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());
    MpcommPeerState state{};
    ASSERT_TRUE(registry.stateOf("peer", state));
    EXPECT_EQ(state, MpcommPeerState::CONNECTED_NO_KEYS);

    ASSERT_TRUE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());
    EXPECT_EQ(adapter->connect_calls, 1);  // not reconnected
    EXPECT_EQ(adapter->query_calls, 2);    // query alone was retried
    ASSERT_TRUE(registry.stateOf("peer", state));
    EXPECT_EQ(state, MpcommPeerState::READY);
}

TEST(MpcommPeerRegistryTest, NewlyRegisteredRemoteBufferRefreshesKeys) {
    // TENT allows a peer to register memory after a segment is opened, and the
    // provider snapshots rkeys at query time, so the keys have to be refetched.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    MpcommPeerRegistry registry(adapter);

    ASSERT_TRUE(
        registry.ensure("peer", "10.0.0.1", 13579, makeRanges({{0x1000, 4096}}))
            .ok());
    ASSERT_EQ(adapter->query_calls, 1);

    ASSERT_TRUE(registry
                    .ensure("peer", "10.0.0.1", 13579,
                            makeRanges({{0x1000, 4096}, {0x8000, 4096}}))
                    .ok());
    EXPECT_EQ(adapter->query_calls, 2);
    EXPECT_EQ(adapter->connect_calls, 1);  // refreshed, not reconnected
}

TEST(MpcommPeerRegistryTest, UnregisteredRemoteBufferDoesNotRefreshKeys) {
    // Keys for memory the peer no longer publishes are never used, and the
    // decision has to stay monotonic: descriptors are cached per thread, so
    // threads routinely observe smaller buffer sets than one another.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    MpcommPeerRegistry registry(adapter);

    ASSERT_TRUE(registry
                    .ensure("peer", "10.0.0.1", 13579,
                            makeRanges({{0x1000, 4096}, {0x8000, 4096}}))
                    .ok());
    ASSERT_EQ(adapter->query_calls, 1);

    ASSERT_TRUE(
        registry.ensure("peer", "10.0.0.1", 13579, makeRanges({{0x1000, 4096}}))
            .ok());
    EXPECT_EQ(adapter->query_calls, 1);
}

TEST(MpcommPeerRegistryTest, RefreshQueriesTheConnectedEndpoint) {
    // Keys must come from the process the connection was made to, not from
    // whatever endpoint the peer advertises now.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    MpcommPeerRegistry registry(adapter);

    ASSERT_TRUE(
        registry.ensure("peer", "10.0.0.1", 13579, makeRanges({{0x1000, 4096}}))
            .ok());
    ASSERT_TRUE(registry
                    .ensure("peer", "10.0.0.9", 20000,
                            makeRanges({{0x1000, 4096}, {0x8000, 4096}}))
                    .ok());

    EXPECT_EQ(adapter->connect_calls, 1);
    EXPECT_EQ(adapter->last_query_addr, "10.0.0.1");
    EXPECT_EQ(adapter->last_query_port, 13579);
}

TEST(MpcommPeerRegistryTest, DistinctPeersAreConnectedIndependently) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    MpcommPeerRegistry registry(adapter);
    auto buffers = makeRanges({{0x1000, 4096}});

    ASSERT_TRUE(registry.ensure("peer_a", "10.0.0.1", 13579, buffers).ok());
    ASSERT_TRUE(registry.ensure("peer_b", "10.0.0.2", 13579, buffers).ok());

    EXPECT_EQ(adapter->connect_calls, 2);
    EXPECT_EQ(registry.size(), 2u);
}

TEST(MpcommPeerRegistryTest, ClearDropsEveryPeer) {
    // uninstall() clears the cache after the provider has been shut down; a
    // later install() must be able to connect again.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    MpcommPeerRegistry registry(adapter);
    auto buffers = makeRanges({{0x1000, 4096}});

    ASSERT_TRUE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());
    ASSERT_EQ(registry.size(), 1u);

    registry.clear();
    EXPECT_EQ(registry.size(), 0u);
    EXPECT_FALSE(registry.contains("peer"));

    ASSERT_TRUE(registry.ensure("peer", "10.0.0.1", 13579, buffers).ok());
    EXPECT_EQ(adapter->connect_calls, 2);
}

// ===================================================================
// Request and completion mapping
// ===================================================================

Request makeRequest(Request::OpCode opcode, size_t length) {
    Request request{};
    request.opcode = opcode;
    request.source = reinterpret_cast<void *>(0x4000);
    request.target_offset = 0x9000;
    request.length = length;
    return request;
}

TEST(MpcommTaskMapping, WriteBecomesPutAndReadBecomesGet) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();

    auto write = makeRequest(Request::WRITE, 1024);
    auto handle = issueMpcommTransfer(*adapter, write, "peer");
    EXPECT_NE(handle, kInvalidMpcommTransferHandle);
    EXPECT_EQ(adapter->put_calls, 1);
    EXPECT_EQ(adapter->get_calls, 0);
    EXPECT_EQ(adapter->last_local_addr, 0x4000u);
    EXPECT_EQ(adapter->last_remote_addr, 0x9000u);
    EXPECT_EQ(adapter->last_length, 1024u);
    EXPECT_EQ(adapter->last_transfer_host, "peer");

    auto read = makeRequest(Request::READ, 2048);
    EXPECT_NE(issueMpcommTransfer(*adapter, read, "peer"),
              kInvalidMpcommTransferHandle);
    EXPECT_EQ(adapter->put_calls, 1);
    EXPECT_EQ(adapter->get_calls, 1);
    EXPECT_EQ(adapter->last_length, 2048u);
}

TEST(MpcommTaskMapping, RejectedIssueReportsAnInvalidHandle) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->issue_fails = true;
    auto request = makeRequest(Request::WRITE, 1024);
    EXPECT_EQ(issueMpcommTransfer(*adapter, request, "peer"),
              kInvalidMpcommTransferHandle);
}

MpcommTask makeTask(size_t length, MpcommTransferHandle handle) {
    MpcommTask task;
    task.request = makeRequest(Request::WRITE, length);
    task.status_word = TransferStatusEnum::PENDING;
    task.mpcomm_handle = handle;
    return task;
}

TEST(MpcommTaskMapping, FullTransferCompletesAndReportsItsBytes) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->outcome = MpcommTransferOutcome{true, 1024, 0};
    auto task = makeTask(1024, 7);

    pollMpcommTask(*adapter, task);

    EXPECT_EQ(statusOf(task), TransferStatusEnum::COMPLETED);
    EXPECT_EQ(task.transferred_bytes, 1024u);
    EXPECT_EQ(task.mpcomm_handle, kInvalidMpcommTransferHandle);
    EXPECT_EQ(adapter->release_calls, 1);
}

TEST(MpcommTaskMapping, ProviderFailureFailsTheTask) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->outcome = MpcommTransferOutcome{false, 0, -4};
    auto task = makeTask(1024, 7);

    pollMpcommTask(*adapter, task);

    EXPECT_EQ(statusOf(task), TransferStatusEnum::FAILED);
    EXPECT_EQ(adapter->release_calls, 1);
}

TEST(MpcommTaskMapping, SuccessfulButShortTransferIsDemotedToFailed) {
    // The engine accumulates request.length for completed tasks, so a success
    // that moved fewer bytes must not be reported as complete.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->outcome = MpcommTransferOutcome{true, 512, 0};
    auto task = makeTask(1024, 7);

    pollMpcommTask(*adapter, task);

    EXPECT_EQ(statusOf(task), TransferStatusEnum::FAILED);
    EXPECT_EQ(task.transferred_bytes, 512u);
    EXPECT_EQ(adapter->release_calls, 1);
}

TEST(MpcommTaskMapping, ByteCountAboveRequestLengthStillCompletes) {
    // The provider reports posted bytes, so a larger count is not an error;
    // only a short one is.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->outcome = MpcommTransferOutcome{true, 2048, 0};
    auto task = makeTask(1024, 7);

    pollMpcommTask(*adapter, task);

    EXPECT_EQ(statusOf(task), TransferStatusEnum::COMPLETED);
}

TEST(MpcommTaskMapping, IncompleteTransferIsLeftPendingAndNotReleased) {
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->transfer_complete = false;
    auto task = makeTask(1024, 7);

    pollMpcommTask(*adapter, task);

    EXPECT_EQ(statusOf(task), TransferStatusEnum::PENDING);
    EXPECT_EQ(task.mpcomm_handle, 7u);
    EXPECT_EQ(adapter->release_calls, 0);
}

TEST(MpcommTaskMapping, HandleIsReleasedExactlyOnceAcrossRepeatedPolls) {
    // getTransferStatus() may be called any number of times per task; a second
    // release of the same handle would corrupt the provider's handle table.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    adapter->outcome = MpcommTransferOutcome{true, 1024, 0};
    auto task = makeTask(1024, 7);

    pollMpcommTask(*adapter, task);
    pollMpcommTask(*adapter, task);
    pollMpcommTask(*adapter, task);

    EXPECT_EQ(adapter->release_calls, 1);
    EXPECT_EQ(adapter->result_calls, 1);
    EXPECT_EQ(statusOf(task), TransferStatusEnum::COMPLETED);
}

TEST(MpcommTaskMapping, TaskWithoutHandleIsIgnored) {
    // A task whose issue failed is already terminal and owns no handle.
    auto adapter = std::make_shared<FakeMpcommAdapter>();
    auto task = makeTask(1024, kInvalidMpcommTransferHandle);
    task.status_word = TransferStatusEnum::FAILED;

    pollMpcommTask(*adapter, task);

    EXPECT_EQ(adapter->result_calls, 0);
    EXPECT_EQ(adapter->release_calls, 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
