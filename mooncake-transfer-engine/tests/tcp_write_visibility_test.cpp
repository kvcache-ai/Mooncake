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

// Contract tests for TcpTransport completion semantics (issue #2086).
//
// Under the legacy (v1) framing, a WRITE was reported COMPLETED when the
// final chunk reached the initiator's kernel socket buffer: destination
// memory could still be mutating megabytes later (measured 166/400
// iterations torn, worst case the entire 2.4 MB descriptor undelivered),
// and a server-side rejection was invisible to the initiator (a
// single-chunk WRITE to an unregistered address "succeeded"). The v2
// acknowledged framing makes COMPLETED mean "applied at the destination"
// and failures mean failures; these tests pin that contract and the
// mixed-version behavior. The main visibility test fails against the
// legacy framing (which the MC_TCP_PROTO=1 escape hatch still selects).

#include <arpa/inet.h>
#include <endian.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;

namespace {

constexpr size_t kBigLength = 2432 * 1024;  // ~2.4 MB, as reported in #2086
constexpr size_t kSmallLength = 16 * 1024;  // 16 KB control size
constexpr size_t kRegionAlign = 4 * 1024 * 1024;
constexpr int kIterations = 400;
constexpr int kNoiseThreads = 3;

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        if (const char* old = std::getenv(name)) {
            had_old_value_ = true;
            old_value_ = old;
        }
        setenv(name_.c_str(), value, 1);
    }

    ~ScopedEnvVar() {
        if (had_old_value_)
            setenv(name_.c_str(), old_value_.c_str(), 1);
        else
            unsetenv(name_.c_str());
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

   private:
    std::string name_;
    std::string old_value_;
    bool had_old_value_ = false;
};

struct TestSessionHeader {
    uint64_t size;
    uint64_t addr;
    uint8_t opcode;
};

static_assert(sizeof(TestSessionHeader) == 24,
              "legacy TCP header ABI changed unexpectedly");

// Minimal legacy-server behavior for a flagged WRITE: v1 does not recognize
// opcode 0x81 as WRITE, so it treats the request as READ and streams payload
// bytes without consuming the initiator's body. A sequential v2 client then
// deadlocks once both socket directions fill; the concurrent status read must
// reject the non-status bytes and cancel the body promptly.
class LegacyReadServer {
   public:
    // keep_open=true models the real v1 server loop: after streaming the
    // "READ payload" it does NOT close, it waits for the next 24-byte
    // header. For flagged requests shorter than a status frame this is the
    // configuration that used to hang a v2 initiator forever (fewer than 8
    // payload bytes ever arrive, no EOF, no deadline).
    explicit LegacyReadServer(bool keep_open = false) : keep_open_(keep_open) {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) return;
        int one = 1;
        if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one,
                       sizeof(one)) != 0)
            return;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        // The production HTTP-metadata path advertises the engine-selected
        // LAN address in RPC metadata even when local_server_name is a
        // loopback test name. Listen on every local interface so the stale
        // descriptor can redirect either that path or P2PHANDSHAKE's
        // loopback path to this fake legacy peer.
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr),
                 sizeof(addr)) != 0)
            return;
        if (listen(listen_fd_, 1) != 0) return;

        socklen_t len = sizeof(addr);
        if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len) !=
            0)
            return;
        port_ = ntohs(addr.sin_port);
        ok_ = true;
        thread_ = std::thread([this] { serve(); });
    }

    ~LegacyReadServer() { join(); }

    uint16_t port() const { return port_; }
    bool ok() const { return ok_; }

    void join() {
        // Wake a blocked accept if the client failed before connecting. This
        // keeps a failed assertion from turning into a hung test process.
        if (listen_fd_ >= 0) (void)shutdown(listen_fd_, SHUT_RDWR);
        if (thread_.joinable()) thread_.join();
        if (listen_fd_ >= 0) {
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    bool sawFlaggedWrite() const { return saw_flagged_write_.load(); }

   private:
    static bool recvExact(int fd, void* buffer, size_t size) {
        char* out = static_cast<char*>(buffer);
        while (size) {
            ssize_t n = recv(fd, out, size, 0);
            if (n <= 0) return false;
            out += n;
            size -= static_cast<size_t>(n);
        }
        return true;
    }

    void serve() {
        int fd = accept(listen_fd_, nullptr, nullptr);
        if (fd < 0) return;

        timeval timeout{8, 0};
        (void)setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout,
                         sizeof(timeout));
        (void)setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                         sizeof(timeout));

        TestSessionHeader header{};
        if (!recvExact(fd, &header, sizeof(header))) {
            close(fd);
            return;
        }
        saw_flagged_write_.store(header.opcode == 0x81);

        const uint64_t total = le64toh(header.size);
        std::vector<char> payload(64 * 1024, static_cast<char>(0xA5));
        uint64_t sent = 0;
        while (sent < total) {
            size_t chunk = std::min<uint64_t>(payload.size(), total - sent);
            ssize_t n = send(fd, payload.data(), chunk, MSG_NOSIGNAL);
            if (n <= 0) break;
            sent += static_cast<uint64_t>(n);
        }
        if (keep_open_) {
            // v1 loop: wait for the next header; leaves only when the
            // client drops the connection (or the test tears down the
            // listener, which shuts the accepted fd's peer down too).
            TestSessionHeader next{};
            (void)recvExact(fd, &next, sizeof(next));
        }
        close(fd);
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    bool ok_ = false;
    bool keep_open_ = false;
    std::thread thread_;
    std::atomic<bool> saw_flagged_write_{false};
};

struct EngineHandle {
    std::unique_ptr<TransferEngine> engine;
    void* pool = nullptr;

    ~EngineHandle() {
        engine.reset();  // unregisters memory before the pool goes away
        free(pool);
    }
    Transport::SegmentID segment_id = 0;
    uint64_t remote_base = 0;
    bool ok = false;  // ASSERT_* in a helper only aborts the helper

    void init(const std::string& metadata_server,
              const std::string& server_name, size_t pool_size) {
        // Exercise the pooled-connection path (default off): connection
        // reuse vs. discard-on-unclean-exchange is part of the contract
        // under test.
        setenv("MC_TCP_ENABLE_CONNECTION_POOL", "1", 1);
        engine = std::make_unique<TransferEngine>(false);
        auto hp = parseHostNameWithPort(server_name);
        int rc = engine->init(metadata_server, server_name, hp.first.c_str(),
                              hp.second);
        ASSERT_EQ(rc, 0);
        ASSERT_NE(engine->installTransport("tcp", nullptr), nullptr);
        pool = malloc(pool_size);
        ASSERT_NE(pool, nullptr);
        memset(pool, 0, pool_size);
        rc = engine->registerLocalMemory(pool, pool_size, "cpu:0");
        ASSERT_EQ(rc, 0);
        // The descriptor is fetchable under the name it was registered
        // with: in P2P-handshake mode the RPC port is auto-assigned, so
        // that is the engine-reported ip:port; against a real metadata
        // service (CI runs one at http://...) it is the requested
        // server_name, matching how production callers open segments.
        std::string segment_name = (metadata_server == P2PHANDSHAKE)
                                       ? engine->getLocalIpAndPort()
                                       : server_name;
        segment_id = engine->openSegment(segment_name);
        auto desc = engine->getMetadata()->getSegmentDescByID(segment_id);
        ASSERT_NE(desc, nullptr);
        remote_base = (uint64_t)desc->buffers[0].addr;
        ok = true;
    }
};

// Submit one request and poll until terminal state; returns final status.
TransferStatusEnum runOne(TransferEngine* engine, TransferRequest entry) {
    auto batch_id = engine->allocateBatchID(1);
    Status s = engine->submitTransfer(batch_id, {entry});
    if (!s.ok()) return TransferStatusEnum::FAILED;
    TransferStatus status;
    status.s = TransferStatusEnum::WAITING;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (status.s != TransferStatusEnum::COMPLETED &&
           status.s != TransferStatusEnum::FAILED) {
        if (std::chrono::steady_clock::now() >= deadline)
            return TransferStatusEnum::TIMEOUT;
        s = engine->getTransferStatus(batch_id, 0, status);
        if (!s.ok()) return TransferStatusEnum::FAILED;
        std::this_thread::yield();
    }
    (void)engine->freeBatchID(batch_id);
    return status.s;
}

}  // namespace

TEST(TcpWriteVisibilityTest, CompletedWriteIsVisibleToSubsequentRead) {
    const char* env = std::getenv("MC_METADATA_SERVER");
    std::string metadata_server = env ? env : "P2PHANDSHAKE";
    const char* name_env = std::getenv("MC_LOCAL_SERVER_NAME");
    std::string server_name = name_env ? name_env : "127.0.0.2:17901";

    const size_t pool_size = 64ull << 20;
    EngineHandle h;
    h.init(metadata_server, server_name, pool_size);
    ASSERT_TRUE(h.ok) << "engine/segment setup failed";

    // Region layout inside the registered pool (all offsets from pool base):
    //   [0, kBigLength)                       : WRITE target region
    //   [kRegionAlign, +kBigLength)           : local staging for WRITE
    //   [3*kRegionAlign + t*kRegionAlign, ...): per-noise-thread scratch
    char* base = (char*)h.pool;
    char* write_src = base + kRegionAlign;

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> noise_failures{0};
    std::vector<std::thread> noise;
    for (int t = 0; t < kNoiseThreads; ++t) {
        noise.emplace_back([&, t] {
            char* src = base + (3 + 2 * t) * kRegionAlign;
            uint64_t dst_off = (4 + 2 * t) * kRegionAlign;
            memset(src, 0x5A + t, kSmallLength);
            while (!stop.load(std::memory_order_relaxed)) {
                TransferRequest entry;
                entry.opcode = TransferRequest::WRITE;
                entry.length = kSmallLength;
                entry.source = src;
                entry.target_id = h.segment_id;
                entry.target_offset = h.remote_base + dst_off;
                if (runOne(h.engine.get(), entry) !=
                    TransferStatusEnum::COMPLETED)
                    noise_failures++;
            }
        });
    }

    uint64_t torn_reads = 0;
    uint64_t torn_bytes_worst = 0;
    int first_bad_iter = -1;
    for (int iter = 1; iter <= kIterations; ++iter) {
        // Generation-stamped pattern: every byte identifies the iteration.
        memset(write_src, iter & 0xFF, kBigLength);

        TransferRequest w;
        w.opcode = TransferRequest::WRITE;
        w.length = kBigLength;
        w.source = write_src;
        w.target_id = h.segment_id;
        w.target_offset = h.remote_base;  // region at pool offset 0
        ASSERT_EQ(runOne(h.engine.get(), w), TransferStatusEnum::COMPLETED)
            << "WRITE failed at iteration " << iter;

        // The WRITE is COMPLETED. The API contract (matching RDMA WRITE
        // semantics, which disaggregated-serving integrations rely on when
        // they notify the consumer out-of-band) is that destination memory
        // is now fully written. Verify by direct local inspection of the
        // destination region — this is exactly what a decode instance does
        // after the prefill side signals transfer completion. Scan backwards:
        // the tail chunks are the ones still in flight when the initiator's
        // final local send completes.
        size_t bad = 0;
        for (size_t i = kBigLength; i-- > 0;) {
            if ((unsigned char)base[i] != (unsigned char)(iter & 0xFF)) {
                bad = i + 1;  // bytes [0, i] not yet guaranteed; count prefix
                break;
            }
        }
        if (bad) {
            torn_reads++;
            torn_bytes_worst = std::max<uint64_t>(torn_bytes_worst, bad);
            if (first_bad_iter < 0) first_bad_iter = iter;
            // Show it is a visibility delay, not data loss: wait for the
            // server-side drain to finish before the next iteration so
            // generations do not overlap.
            while (memcmp(base, write_src, kBigLength) != 0)
                std::this_thread::yield();
        }
    }

    stop = true;
    for (auto& t : noise) t.join();

    EXPECT_EQ(torn_reads, 0u)
        << torn_reads << "/" << kIterations
        << " reads observed destination bytes not matching the COMPLETED "
           "write (worst: "
        << torn_bytes_worst << " stale bytes; first at iteration "
        << first_bad_iter << "; noise failures: " << noise_failures.load()
        << ")";
}

// A server-side rejection must surface as FAILED, not silent success: under
// v1 framing a single-chunk WRITE to an unregistered address reported
// COMPLETED because the protocol had no channel for the server to say no.
TEST(TcpWriteVisibilityTest, RejectedWriteMustFail) {
    const char* env = std::getenv("MC_METADATA_SERVER");
    std::string metadata_server = env ? env : "P2PHANDSHAKE";
    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17902", 8ull << 20);
    ASSERT_TRUE(h.ok) << "engine/segment setup failed";

    char* src = (char*)h.pool;
    memset(src, 0xAB, kSmallLength);
    TransferRequest w;
    w.opcode = TransferRequest::WRITE;
    w.length = kSmallLength;
    w.source = src;
    w.target_id = h.segment_id;
    // One page past the registered pool: the server rejects it in address
    // validation.
    w.target_offset = h.remote_base + (8ull << 20) + 4096;
    EXPECT_EQ(runOne(h.engine.get(), w), TransferStatusEnum::FAILED);

    // The connection that carried the rejected request must not poison
    // subsequent transfers.
    w.target_offset = h.remote_base + kRegionAlign;
    EXPECT_EQ(runOne(h.engine.get(), w), TransferStatusEnum::COMPLETED);
}

// A v2 server rejects an invalid destination immediately after the header,
// while a large client body is still in flight. FAILED is also the caller's
// source-buffer lifetime boundary, so it must not be published until closing
// the socket has quiesced the outstanding async_write.
TEST(TcpWriteVisibilityTest, LargeRejectedWriteQuiescesSourceBeforeFailure) {
    const char* env = std::getenv("MC_METADATA_SERVER");
    std::string metadata_server = env ? env : "P2PHANDSHAKE";
    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17906", 8ull << 20);
    ASSERT_TRUE(h.ok) << "engine/segment setup failed";

    constexpr size_t kLength = 32ull << 20;  // exceed the socket buffers
    void* source = mmap(nullptr, kLength, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(source, MAP_FAILED);
    memset(source, 0x6D, kLength);

    TransferRequest w;
    w.opcode = TransferRequest::WRITE;
    w.length = kLength;
    w.source = source;
    w.target_id = h.segment_id;
    w.target_offset = h.remote_base + (8ull << 20) + 4096;

    auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(runOne(h.engine.get(), w), TransferStatusEnum::FAILED);
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(3));

    ASSERT_EQ(mprotect(source, kLength, PROT_NONE), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_EQ(mprotect(source, kLength, PROT_READ | PROT_WRITE), 0);
    ASSERT_EQ(munmap(source, kLength), 0);

    // The rejected exchange is unclean, so the next request must use a fresh
    // connection rather than inheriting the server's mid-session state.
    char* small_source = static_cast<char*>(h.pool);
    memset(small_source, 0x42, kSmallLength);
    w.length = kSmallLength;
    w.source = small_source;
    w.target_offset = h.remote_base + kRegionAlign;
    EXPECT_EQ(runOne(h.engine.get(), w), TransferStatusEnum::COMPLETED);
}

// v2 READ round-trip: exercises the status-frame-then-data framing in both
// directions, including content integrity and a rejected READ surfacing as
// FAILED (v1 could only signal that by dropping the connection).
TEST(TcpWriteVisibilityTest, V2ReadRoundTripAndRejectedRead) {
    const char* env = std::getenv("MC_METADATA_SERVER");
    std::string metadata_server = env ? env : "P2PHANDSHAKE";
    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17904", 16ull << 20);
    ASSERT_TRUE(h.ok) << "engine/segment setup failed";

    char* base = (char*)h.pool;
    char* src = base + kRegionAlign;
    char* dst = base + 2 * kRegionAlign;
    for (size_t i = 0; i < kBigLength; ++i) src[i] = (char)(i * 131 + 7);

    TransferRequest w;
    w.opcode = TransferRequest::WRITE;
    w.length = kBigLength;
    w.source = src;
    w.target_id = h.segment_id;
    w.target_offset = h.remote_base;
    ASSERT_EQ(runOne(h.engine.get(), w), TransferStatusEnum::COMPLETED);

    TransferRequest r;
    r.opcode = TransferRequest::READ;
    r.length = kBigLength;
    r.source = dst;
    r.target_id = h.segment_id;
    r.target_offset = h.remote_base;
    ASSERT_EQ(runOne(h.engine.get(), r), TransferStatusEnum::COMPLETED);
    // v2 WRITE completion means the destination was already applied, so the
    // read-back must match immediately — no drain wait.
    EXPECT_EQ(memcmp(dst, src, kBigLength), 0);

    // A READ of an unregistered range must fail via the error status frame.
    r.target_offset = h.remote_base + (16ull << 20) + 4096;
    EXPECT_EQ(runOne(h.engine.get(), r), TransferStatusEnum::FAILED);

    // And the pool must still be usable afterwards.
    r.target_offset = h.remote_base;
    EXPECT_EQ(runOne(h.engine.get(), r), TransferStatusEnum::COMPLETED);
}

// Mixed-version quadrant: a legacy (v1) initiator against the v2 server —
// selected via the MC_TCP_PROTO=1 escape hatch — still transfers data
// correctly (with the old weaker completion semantics).
TEST(TcpWriteVisibilityTest, LegacyInitiatorInteropWithV2Server) {
    const char* env = std::getenv("MC_METADATA_SERVER");
    std::string metadata_server = env ? env : "P2PHANDSHAKE";
    // Set the process environment before the engine starts any threads, and
    // restore it only after those threads have stopped. POSIX does not require
    // setenv()/unsetenv() to synchronize with concurrent getenv() calls.
    ScopedEnvVar legacy_proto("MC_TCP_PROTO", "1");
    {
        EngineHandle h;
        h.init(metadata_server, "127.0.0.2:17903", 16ull << 20);
        ASSERT_TRUE(h.ok) << "engine/segment setup failed";

        char* base = (char*)h.pool;
        char* src = base + kRegionAlign;
        memset(src, 0x3C, kSmallLength);
        TransferRequest w;
        w.opcode = TransferRequest::WRITE;
        w.length = kSmallLength;
        w.source = src;
        w.target_id = h.segment_id;
        w.target_offset = h.remote_base;
        EXPECT_EQ(runOne(h.engine.get(), w), TransferStatusEnum::COMPLETED);

        // v1 completion does not guarantee destination visibility; wait for
        // the server drain before checking content.
        auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (memcmp(base, src, kSmallLength) != 0 &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::yield();
        EXPECT_EQ(memcmp(base, src, kSmallLength), 0);

        // Read-back over the transport under v1 framing.
        char* dst = base + 2 * kRegionAlign;
        TransferRequest r;
        r.opcode = TransferRequest::READ;
        r.length = kSmallLength;
        r.source = dst;
        r.target_id = h.segment_id;
        r.target_offset = h.remote_base;
        EXPECT_EQ(runOne(h.engine.get(), r), TransferStatusEnum::COMPLETED);
        EXPECT_EQ(memcmp(dst, src, kSmallLength), 0);
    }
}

// A cached v2 descriptor can briefly outlive a server downgrade/restart. A
// legacy server interprets the flagged WRITE opcode as READ and sends payload
// while the client sends its body. The concurrent status read must break this
// full-duplex deadlock, and FAILED must not become visible until asio has
// released the caller-owned source buffer.
TEST(TcpWriteVisibilityTest,
     StaleV2DescriptorAgainstLegacyServerQuiescesWriteBeforeFailure) {
    const char* env = std::getenv("MC_METADATA_SERVER");
    std::string metadata_server = env ? env : "P2PHANDSHAKE";
    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17905", 8ull << 20);
    ASSERT_TRUE(h.ok) << "engine/segment setup failed";

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);

    LegacyReadServer legacy_server;
    ASSERT_TRUE(legacy_server.ok());
    desc->tcp_data_port = legacy_server.port();
    desc->tcp_proto_version = 2;  // deliberately stale capability advertisement

    constexpr size_t kLength = 32ull << 20;  // exceed both socket buffers
    void* source = mmap(nullptr, kLength, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    ASSERT_NE(source, MAP_FAILED);
    memset(source, 0x5C, kLength);

    TransferRequest w;
    w.opcode = TransferRequest::WRITE;
    w.length = kLength;
    w.source = source;
    w.target_id = h.segment_id;
    w.target_offset = h.remote_base;

    auto start = std::chrono::steady_clock::now();
    EXPECT_EQ(runOne(h.engine.get(), w), TransferStatusEnum::FAILED);
    auto elapsed = std::chrono::steady_clock::now() - start;
    // The fake legacy peer waits up to 8s for the old mutual-write deadlock.
    // Leave generous CI headroom while still proving the concurrent-read path.
    EXPECT_LT(elapsed, std::chrono::seconds(3));

    // Terminal status is the source-buffer lifetime boundary. Protecting the
    // pages immediately after FAILED would crash if an async_write still owned
    // them and attempted further progress.
    ASSERT_EQ(mprotect(source, kLength, PROT_NONE), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_EQ(mprotect(source, kLength, PROT_READ | PROT_WRITE), 0);
    ASSERT_EQ(munmap(source, kLength), 0);

    legacy_server.join();
    EXPECT_TRUE(legacy_server.sawFlaggedWrite());
}

// A stale v2 descriptor can also point at a legacy server with a request
// SHORTER than a status frame (1-7 bytes). The v1 peer treats the flagged
// opcode as READ, streams fewer than 8 "payload" bytes, and then keeps the
// connection open waiting for the next header — no EOF ever arrives, and for
// WRITE it is symmetrically stuck parsing our body bytes as a partial
// header. Without a status-frame deadline both directions wait forever;
// with it they must fail, and only after actually waiting the deadline out
// (an early failure would mean something else broke).
TEST(TcpWriteVisibilityTest, StaleV2DescriptorShortRequestFailsWithinDeadline) {
    ScopedEnvVar fast_deadline("MC_TCP_STATUS_TIMEOUT_SEC", "2");
    const char* env = std::getenv("MC_METADATA_SERVER");
    std::string metadata_server = env ? env : "P2PHANDSHAKE";
    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17906", 8ull << 20);
    ASSERT_TRUE(h.ok) << "engine/segment setup failed";

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_proto_version = 2;  // deliberately stale capability advertisement

    char buf[4] = {0x11, 0x22, 0x33, 0x44};
    for (auto opcode : {TransferRequest::WRITE, TransferRequest::READ}) {
        // One server per direction: the previous connection was (correctly)
        // discarded rather than re-pooled, so each request dials anew.
        LegacyReadServer legacy_server(/*keep_open=*/true);
        ASSERT_TRUE(legacy_server.ok());
        desc->tcp_data_port = legacy_server.port();

        TransferRequest r;
        r.opcode = opcode;
        r.length = sizeof(buf);
        r.source = buf;
        r.target_id = h.segment_id;
        r.target_offset = h.remote_base;

        auto start = std::chrono::steady_clock::now();
        EXPECT_EQ(runOne(h.engine.get(), r), TransferStatusEnum::FAILED)
            << "opcode " << static_cast<int>(opcode);
        auto elapsed = std::chrono::steady_clock::now() - start;
        EXPECT_GE(elapsed, std::chrono::seconds(1))
            << "failure arrived before the status deadline could have fired; "
               "the wrong path failed (opcode "
            << static_cast<int>(opcode) << ")";
        EXPECT_LT(elapsed, std::chrono::seconds(6))
            << "deadline did not bound the stale-descriptor wait (opcode "
            << static_cast<int>(opcode) << ")";
        legacy_server.join();
    }
}
