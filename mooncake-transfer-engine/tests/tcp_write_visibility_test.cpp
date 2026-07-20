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
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
namespace mooncake {
void tcpTransportSetLaneConnectHandlerHookForTest(
    void (*hook)() noexcept) noexcept;
void tcpTransportSetLaneObserverHookForTest(
    void (*hook)(int, size_t, uint64_t, size_t, bool) noexcept) noexcept;
void tcpTransportSetLaneRetryHandlerHookForTest(
    void (*hook)() noexcept) noexcept;
void tcpTransportSetLaneFailureReasonHookForTest(
    void (*hook)(int) noexcept) noexcept;
bool tcpTransportLaneTypesAreMoveOnlyForTest() noexcept;
}  // namespace mooncake
#endif

using namespace mooncake;

namespace {

constexpr size_t kBigLength = 2432 * 1024;  // ~2.4 MB, as reported in #2086
constexpr size_t kSmallLength = 16 * 1024;  // 16 KB control size
constexpr size_t kRegionAlign = 4 * 1024 * 1024;
constexpr int kIterations = 400;
constexpr int kNoiseThreads = 3;

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
enum LaneTestEvent {
    kLaneQueueAdmitted = 1,
    kLaneQueueRejected = 2,
    kLaneConnecting = 3,
    kLaneBusy = 4,
    kLaneTerminal = 5,
    kLaneShutdownClean = 6,
    kLaneLateHandler = 7,
    kLaneRetryArmed = 8,
    kLaneRetryFired = 9,
    kLaneRetryLate = 10,
    kLaneCooldownStarted = 11,
};

enum WorkFailureReasonForTest {
    kWorkQueueFull = 0,
    kWorkRuntimeUnavailable = 1,
    kWorkConnectFailed = 2,
    kWorkSessionFailed = 3,
    kWorkShutdown = 4,
};

std::atomic<bool> lane_connect_handler_entered{false};
std::atomic<bool> release_lane_connect_handler{false};
std::atomic<bool> hold_lane_connect_handler{false};
std::atomic<bool> hold_lane_connect_after_busy{false};
std::atomic<bool> connecting_lane_had_current{false};
std::atomic<bool> retry_handler_entered{false};
std::atomic<bool> release_retry_handler{false};
std::atomic<bool> hold_retry_handler{false};
std::atomic<size_t> maximum_observed_queue_depth{0};
std::atomic<size_t> maximum_observed_socket_count{0};
std::atomic<int> queue_rejection_count{0};
std::atomic<int> lane_connecting_count{0};
std::atomic<int> lane_busy_count{0};
std::atomic<int> lane_terminal_count{0};
std::atomic<int> lane_shutdown_clean_count{0};
std::atomic<int> late_lane_handler_count{0};
std::atomic<int> retry_armed_count{0};
std::atomic<int> retry_fired_count{0};
std::atomic<int> retry_late_count{0};
std::atomic<int> cooldown_started_count{0};
std::atomic<int> queue_full_failure_count{0};
std::atomic<int> connect_failure_count{0};
std::atomic<int> runtime_unavailable_failure_count{0};
std::atomic<int> shutdown_failure_count{0};

template <typename Predicate, typename Rep, typename Period>
bool waitForPredicate(Predicate&& predicate,
                      std::chrono::duration<Rep, Period> timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return predicate();
}

template <typename T>
void updateMaximum(std::atomic<T>& value, T candidate) noexcept {
    T previous = value.load(std::memory_order_relaxed);
    while (previous < candidate &&
           !value.compare_exchange_weak(previous, candidate,
                                        std::memory_order_relaxed)) {
    }
}

void resetLaneTestState() noexcept {
    lane_connect_handler_entered.store(false, std::memory_order_release);
    release_lane_connect_handler.store(false, std::memory_order_release);
    hold_lane_connect_handler.store(false, std::memory_order_release);
    hold_lane_connect_after_busy.store(false, std::memory_order_release);
    connecting_lane_had_current.store(false, std::memory_order_release);
    retry_handler_entered.store(false, std::memory_order_release);
    release_retry_handler.store(false, std::memory_order_release);
    hold_retry_handler.store(false, std::memory_order_release);
    maximum_observed_queue_depth.store(0, std::memory_order_release);
    maximum_observed_socket_count.store(0, std::memory_order_release);
    queue_rejection_count.store(0, std::memory_order_release);
    lane_connecting_count.store(0, std::memory_order_release);
    lane_busy_count.store(0, std::memory_order_release);
    lane_terminal_count.store(0, std::memory_order_release);
    lane_shutdown_clean_count.store(0, std::memory_order_release);
    late_lane_handler_count.store(0, std::memory_order_release);
    retry_armed_count.store(0, std::memory_order_release);
    retry_fired_count.store(0, std::memory_order_release);
    retry_late_count.store(0, std::memory_order_release);
    cooldown_started_count.store(0, std::memory_order_release);
    queue_full_failure_count.store(0, std::memory_order_release);
    connect_failure_count.store(0, std::memory_order_release);
    runtime_unavailable_failure_count.store(0, std::memory_order_release);
    shutdown_failure_count.store(0, std::memory_order_release);
}

void releaseLaneConnectHandler() noexcept {
    release_lane_connect_handler.store(true, std::memory_order_release);
}

void blockLaneConnectHandler() noexcept {
    if (!hold_lane_connect_handler.load(std::memory_order_acquire)) return;
    lane_connect_handler_entered.store(true, std::memory_order_release);
    while (!release_lane_connect_handler.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void releaseRetryHandler() noexcept {
    release_retry_handler.store(true, std::memory_order_release);
}

void blockRetryHandler() noexcept {
    if (!hold_retry_handler.load(std::memory_order_acquire)) return;
    retry_handler_entered.store(true, std::memory_order_release);
    while (!release_retry_handler.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

void observeLaneState(int event, size_t queue_depth, uint64_t,
                      size_t active_sockets, bool lane_has_current) noexcept {
    updateMaximum(maximum_observed_queue_depth, queue_depth);
    updateMaximum(maximum_observed_socket_count, active_sockets);
    if (event == kLaneConnecting && lane_has_current)
        connecting_lane_had_current.store(true, std::memory_order_release);
    if (event == kLaneConnecting)
        lane_connecting_count.fetch_add(1, std::memory_order_relaxed);
    if (event == kLaneBusy) {
        lane_busy_count.fetch_add(1, std::memory_order_relaxed);
        if (hold_lane_connect_after_busy.load(std::memory_order_acquire))
            hold_lane_connect_handler.store(true, std::memory_order_release);
    }
    if (event == kLaneQueueRejected)
        queue_rejection_count.fetch_add(1, std::memory_order_relaxed);
    if (event == kLaneTerminal)
        lane_terminal_count.fetch_add(1, std::memory_order_relaxed);
    if (event == kLaneShutdownClean)
        lane_shutdown_clean_count.fetch_add(1, std::memory_order_relaxed);
    if (event == kLaneLateHandler)
        late_lane_handler_count.fetch_add(1, std::memory_order_relaxed);
    if (event == kLaneRetryArmed)
        retry_armed_count.fetch_add(1, std::memory_order_relaxed);
    if (event == kLaneRetryFired)
        retry_fired_count.fetch_add(1, std::memory_order_relaxed);
    if (event == kLaneRetryLate)
        retry_late_count.fetch_add(1, std::memory_order_relaxed);
    if (event == kLaneCooldownStarted)
        cooldown_started_count.fetch_add(1, std::memory_order_relaxed);
}

void observeWorkFailureReason(int reason) noexcept {
    if (reason == kWorkQueueFull)
        queue_full_failure_count.fetch_add(1, std::memory_order_relaxed);
    if (reason == kWorkConnectFailed)
        connect_failure_count.fetch_add(1, std::memory_order_relaxed);
    if (reason == kWorkRuntimeUnavailable)
        runtime_unavailable_failure_count.fetch_add(1,
                                                    std::memory_order_relaxed);
    if (reason == kWorkShutdown)
        shutdown_failure_count.fetch_add(1, std::memory_order_relaxed);
}

class ScopedLaneHooks {
   public:
    explicit ScopedLaneHooks(bool block_first_connect_handler = false,
                             bool block_after_busy = false,
                             bool block_retry = false) {
        resetLaneTestState();
        tcpTransportSetLaneObserverHookForTest(observeLaneState);
        tcpTransportSetLaneFailureReasonHookForTest(observeWorkFailureReason);
        if (block_first_connect_handler || block_after_busy) {
            hold_lane_connect_handler.store(block_first_connect_handler,
                                            std::memory_order_release);
            hold_lane_connect_after_busy.store(block_after_busy,
                                               std::memory_order_release);
            tcpTransportSetLaneConnectHandlerHookForTest(
                blockLaneConnectHandler);
        }
        if (block_retry) {
            hold_retry_handler.store(true, std::memory_order_release);
            tcpTransportSetLaneRetryHandlerHookForTest(blockRetryHandler);
        }
    }

    ~ScopedLaneHooks() { reset(); }

    void reset() noexcept {
        if (!active_) return;
        releaseLaneConnectHandler();
        releaseRetryHandler();
        tcpTransportSetLaneConnectHandlerHookForTest(nullptr);
        tcpTransportSetLaneRetryHandlerHookForTest(nullptr);
        tcpTransportSetLaneObserverHookForTest(nullptr);
        tcpTransportSetLaneFailureReasonHookForTest(nullptr);
        active_ = false;
    }

   private:
    bool active_ = true;
};
#endif

void reclaimBatchDescAfterEngineShutdownForTest(Transport::BatchID batch_id) {
#ifndef CONFIG_USE_BATCH_DESC_SET
    // These shutdown tests deliberately destroy TransferEngine with work still
    // outstanding, so normal freeBatchID cannot be called. The caller must
    // wait for engine destruction to return (and thus for its worker to join)
    // before reclaiming a descriptor that no callback can touch anymore.
    delete &Transport::toBatchDesc(batch_id);
#else
    // The batch descriptor registry owns (and may already reclaim) this object.
    (void)batch_id;
#endif
}

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

// Accepts TCP connections but deliberately never reads request headers or
// bodies. Large writes therefore remain in progress until closePeer() drops
// the accepted sockets.
class HoldingWriteServer {
   public:
    HoldingWriteServer() {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) return;
        int one = 1;
        if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one,
                       sizeof(one)) != 0)
            return;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr),
                 sizeof(addr)) != 0)
            return;
        if (listen(listen_fd_, 64) != 0) return;

        socklen_t len = sizeof(addr);
        if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len) !=
            0)
            return;
        port_ = ntohs(addr.sin_port);
        ok_ = true;
        thread_ = std::thread([this] { acceptLoop(); });
    }

    ~HoldingWriteServer() {
        closePeer();
        if (listen_fd_ >= 0) close(listen_fd_);
    }

    bool ok() const { return ok_; }
    uint16_t port() const { return port_; }
    int acceptedCount() const { return accepted_count_.load(); }
    int maxAcceptedCount() const { return max_accepted_count_.load(); }

    bool waitForAccepted(int count, std::chrono::seconds timeout) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (acceptedCount() < count &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return acceptedCount() >= count;
    }

    void closePeer() {
        if (closed_.exchange(true)) return;
        if (listen_fd_ >= 0) (void)shutdown(listen_fd_, SHUT_RDWR);
        if (thread_.joinable()) thread_.join();

        std::vector<int> accepted;
        {
            std::lock_guard<std::mutex> lock(accepted_mutex_);
            accepted.swap(accepted_fds_);
        }
        for (int fd : accepted) {
            (void)shutdown(fd, SHUT_RDWR);
            close(fd);
        }
    }

   private:
    void acceptLoop() {
        while (!closed_.load()) {
            int fd = accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) break;
            if (closed_.load()) {
                close(fd);
                break;
            }

            {
                std::lock_guard<std::mutex> lock(accepted_mutex_);
                accepted_fds_.push_back(fd);
            }
            const int count = accepted_count_.fetch_add(1) + 1;
            int previous_max = max_accepted_count_.load();
            while (previous_max < count &&
                   !max_accepted_count_.compare_exchange_weak(previous_max,
                                                              count)) {
            }
        }
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    bool ok_ = false;
    std::thread thread_;
    mutable std::mutex accepted_mutex_;
    std::vector<int> accepted_fds_;
    std::atomic<bool> closed_{false};
    std::atomic<int> accepted_count_{0};
    std::atomic<int> max_accepted_count_{0};
};

// Keeps a local TCP port bound but deliberately never listens. Linux rejects
// connect attempts deterministically, without blackhole-address timing.
class UnavailableTcpPeer {
   public:
    UnavailableTcpPeer() {
        fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) return;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
            return;

        socklen_t len = sizeof(addr);
        if (getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0)
            return;
        port_ = ntohs(addr.sin_port);
        ok_ = true;
    }

    ~UnavailableTcpPeer() {
        if (fd_ >= 0) close(fd_);
    }

    bool ok() const { return ok_; }
    uint16_t port() const { return port_; }

   private:
    int fd_ = -1;
    uint16_t port_ = 0;
    bool ok_ = false;
};

// Minimal v2 WRITE server that keeps accepted connections open and processes
// multiple request/ack exchanges on each one.
class ReusingWriteServer {
   public:
    ReusingWriteServer() {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) return;
        int one = 1;
        if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one,
                       sizeof(one)) != 0)
            return;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr),
                 sizeof(addr)) != 0)
            return;
        if (listen(listen_fd_, 16) != 0) return;

        socklen_t len = sizeof(addr);
        if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len) !=
            0)
            return;
        port_ = ntohs(addr.sin_port);
        ok_ = true;
        accept_thread_ = std::thread([this] { acceptLoop(); });
    }

    ~ReusingWriteServer() { stop(); }

    bool ok() const { return ok_; }
    uint16_t port() const { return port_; }
    int acceptedCount() const { return accepted_count_.load(); }

    bool waitForRequests(int count, std::chrono::seconds timeout) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (request_count_.load() < count &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return request_count_.load() >= count;
    }

    std::vector<uint64_t> requestAddresses() const {
        std::lock_guard<std::mutex> lock(request_mutex_);
        return request_addresses_;
    }

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

    static bool sendExact(int fd, const void* buffer, size_t size) {
        const char* in = static_cast<const char*>(buffer);
        while (size) {
            ssize_t n = send(fd, in, size, MSG_NOSIGNAL);
            if (n <= 0) return false;
            in += n;
            size -= static_cast<size_t>(n);
        }
        return true;
    }

    void acceptLoop() {
        while (!stopped_.load()) {
            int fd = accept(listen_fd_, nullptr, nullptr);
            if (fd < 0) break;
            if (stopped_.load()) {
                close(fd);
                break;
            }
            {
                std::lock_guard<std::mutex> lock(connection_mutex_);
                accepted_fds_.push_back(fd);
                connection_threads_.emplace_back(
                    [this, fd] { serveConnection(fd); });
            }
            accepted_count_.fetch_add(1);
        }
    }

    void serveConnection(int fd) {
        std::vector<char> payload(64 * 1024);
        bool keep_serving = true;
        while (!stopped_.load()) {
            TestSessionHeader header{};
            if (!recvExact(fd, &header, sizeof(header))) break;
            {
                std::lock_guard<std::mutex> lock(request_mutex_);
                request_addresses_.push_back(le64toh(header.addr));
            }
            uint64_t remaining = le64toh(header.size);
            while (remaining) {
                const size_t chunk =
                    std::min<uint64_t>(payload.size(), remaining);
                if (!recvExact(fd, payload.data(), chunk)) {
                    keep_serving = false;
                    break;
                }
                remaining -= chunk;
            }
            if (!keep_serving) break;

            constexpr uint64_t kStatusOk = 0x4D435456ull << 32;
            const uint64_t status = htole64(kStatusOk);
            if (!sendExact(fd, &status, sizeof(status))) break;
            request_count_.fetch_add(1);
        }
        {
            std::lock_guard<std::mutex> lock(connection_mutex_);
            auto it = std::find(accepted_fds_.begin(), accepted_fds_.end(), fd);
            if (it != accepted_fds_.end()) accepted_fds_.erase(it);
        }
        close(fd);
    }

    void stop() {
        if (stopped_.exchange(true)) return;
        if (listen_fd_ >= 0) (void)shutdown(listen_fd_, SHUT_RDWR);
        if (accept_thread_.joinable()) accept_thread_.join();

        {
            std::lock_guard<std::mutex> lock(connection_mutex_);
            for (int fd : accepted_fds_) (void)shutdown(fd, SHUT_RDWR);
        }
        for (auto& thread : connection_threads_)
            if (thread.joinable()) thread.join();
        if (listen_fd_ >= 0) close(listen_fd_);
    }

    int listen_fd_ = -1;
    uint16_t port_ = 0;
    bool ok_ = false;
    std::thread accept_thread_;
    std::mutex connection_mutex_;
    mutable std::mutex request_mutex_;
    std::vector<int> accepted_fds_;
    std::vector<std::thread> connection_threads_;
    std::vector<uint64_t> request_addresses_;
    std::atomic<bool> stopped_{false};
    std::atomic<int> accepted_count_{0};
    std::atomic<int> request_count_{0};
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

bool waitForBatchTerminal(TransferEngine* engine, Transport::BatchID batch_id,
                          size_t task_count, std::chrono::seconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        bool all_terminal = true;
        for (size_t task_id = 0; task_id < task_count; ++task_id) {
            TransferStatus status;
            status.s = TransferStatusEnum::WAITING;
            if (!engine->getTransferStatus(batch_id, task_id, status).ok() ||
                (status.s != TransferStatusEnum::COMPLETED &&
                 status.s != TransferStatusEnum::FAILED)) {
                all_terminal = false;
            }
        }
        if (all_terminal) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    for (size_t task_id = 0; task_id < task_count; ++task_id) {
        TransferStatus status;
        status.s = TransferStatusEnum::WAITING;
        if (!engine->getTransferStatus(batch_id, task_id, status).ok() ||
            (status.s != TransferStatusEnum::COMPLETED &&
             status.s != TransferStatusEnum::FAILED)) {
            return false;
        }
    }
    return true;
}

void expectEverySliceCompletedExactlyOnceAfterShutdown(
    Transport::BatchID batch_id) {
    const auto& batch = Transport::toBatchDesc(batch_id);
    for (size_t task_id = 0; task_id < batch.task_list.size(); ++task_id) {
        const auto& task = batch.task_list[task_id];
        const uint64_t success =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_RELAXED);
        const uint64_t failed =
            __atomic_load_n(&task.failed_slice_count, __ATOMIC_RELAXED);
        const uint64_t slices =
            __atomic_load_n(&task.slice_count, __ATOMIC_RELAXED);
        EXPECT_EQ(success + failed, slices) << "task " << task_id;
        for (const auto* slice : task.slice_list) {
            EXPECT_TRUE(slice->status == Transport::Slice::SUCCESS ||
                        slice->status == Transport::Slice::FAILED)
                << "task " << task_id;
        }
    }
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

TEST(TcpWriteVisibilityTest, PerPeerLaneAndQueueBoundsHoldUnderLoad) {
    constexpr int kRounds = 3;
    constexpr int kRequestCount = 32;
    constexpr size_t kLength = 64 * 1024;
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "2");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "64");
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    for (int round = 0; round < kRounds; ++round) {
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        ScopedLaneHooks hooks;
#endif
        HoldingWriteServer fake_peer;
        ASSERT_TRUE(fake_peer.ok()) << "round " << round;

        EngineHandle h;
        h.init(metadata_server, "127.0.0.2:" + std::to_string(17907 + round),
               kLength);
        ASSERT_TRUE(h.ok) << "engine/segment setup failed in round " << round;

        auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
        ASSERT_NE(desc, nullptr);
        desc->tcp_data_port = fake_peer.port();
        desc->tcp_proto_version = 2;

        memset(h.pool, 0x4D + round, kLength);
        std::vector<TransferRequest> requests(kRequestCount);
        for (auto& request : requests) {
            request.opcode = TransferRequest::WRITE;
            request.length = kLength;
            request.source = h.pool;
            request.target_id = h.segment_id;
            request.target_offset = h.remote_base;
        }

        auto batch_id = h.engine->allocateBatchID(kRequestCount);
        Status submission = h.engine->submitTransfer(batch_id, requests);
        ASSERT_TRUE(submission.ok()) << "round " << round;

        EXPECT_TRUE(fake_peer.waitForAccepted(2, std::chrono::seconds(5)))
            << "round " << round << " accepted only "
            << fake_peer.acceptedCount() << " connections";
        // Give any incorrectly uncapped attempts time to reach accept().
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        EXPECT_LE(fake_peer.maxAcceptedCount(), 2)
            << "per-peer lane count exceeded in round " << round;
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
        EXPECT_LE(maximum_observed_queue_depth.load(std::memory_order_acquire),
                  64u);
        EXPECT_LE(maximum_observed_socket_count.load(std::memory_order_acquire),
                  2u);
        EXPECT_FALSE(
            connecting_lane_had_current.load(std::memory_order_acquire));
#endif

        fake_peer.closePeer();

        std::vector<TransferStatusEnum> final_states(
            kRequestCount, TransferStatusEnum::WAITING);
        bool status_query_failed = false;
        bool all_terminal = false;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (!all_terminal && std::chrono::steady_clock::now() < deadline) {
            all_terminal = true;
            for (int i = 0; i < kRequestCount; ++i) {
                if (final_states[i] == TransferStatusEnum::COMPLETED ||
                    final_states[i] == TransferStatusEnum::FAILED)
                    continue;

                TransferStatus status;
                status.s = TransferStatusEnum::WAITING;
                Status query = h.engine->getTransferStatus(batch_id, i, status);
                if (!query.ok()) {
                    status_query_failed = true;
                    all_terminal = false;
                    continue;
                }
                final_states[i] = status.s;
                if (status.s != TransferStatusEnum::COMPLETED &&
                    status.s != TransferStatusEnum::FAILED)
                    all_terminal = false;
            }
            if (!all_terminal)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        EXPECT_FALSE(status_query_failed) << "round " << round;
        EXPECT_TRUE(all_terminal) << "round " << round;
        for (int i = 0; i < kRequestCount; ++i) {
            EXPECT_NE(final_states[i], TransferStatusEnum::WAITING)
                << "request " << i << " remained WAITING in round " << round;
            EXPECT_TRUE(final_states[i] == TransferStatusEnum::COMPLETED ||
                        final_states[i] == TransferStatusEnum::FAILED)
                << "request " << i << " was not terminal in round " << round;
        }
        if (all_terminal) (void)h.engine->freeBatchID(batch_id);
    }
}

TEST(TcpWriteVisibilityTest,
     QueuedWorkAndBusyLanesCompleteExactlyOnceDuringShutdown) {
    constexpr int kRequestCount = 16;
    constexpr size_t kLength = 64 * 1024;
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "2");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "64");
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    ScopedLaneHooks hooks;
#endif
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    HoldingWriteServer fake_peer;
    ASSERT_TRUE(fake_peer.ok());

    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17920", kLength);
    ASSERT_TRUE(h.ok);

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_data_port = fake_peer.port();
    desc->tcp_proto_version = 2;

    memset(h.pool, 0x5A, kLength);
    std::vector<TransferRequest> requests(kRequestCount);
    for (auto& request : requests) {
        request.opcode = TransferRequest::WRITE;
        request.length = kLength;
        request.source = h.pool;
        request.target_id = h.segment_id;
        request.target_offset = h.remote_base;
    }

    auto batch_id = h.engine->allocateBatchID(kRequestCount);
    ASSERT_TRUE(h.engine->submitTransfer(batch_id, requests).ok());
    ASSERT_TRUE(fake_peer.waitForAccepted(2, std::chrono::seconds(5)));
    EXPECT_LE(fake_peer.maxAcceptedCount(), 2);

    const auto start = std::chrono::steady_clock::now();
    h.engine.reset();
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(5));
    expectEverySliceCompletedExactlyOnceAfterShutdown(batch_id);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    EXPECT_EQ(lane_shutdown_clean_count.load(std::memory_order_acquire), 1);
    EXPECT_LE(maximum_observed_queue_depth.load(std::memory_order_acquire),
              64u);
    EXPECT_LE(maximum_observed_socket_count.load(std::memory_order_acquire),
              2u);
    hooks.reset();
#endif
    reclaimBatchDescAfterEngineShutdownForTest(batch_id);
}

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
TEST(TcpWriteVisibilityTest,
     ConnectingLaneShutdownDetachesOwnershipAndIgnoresLateHandler) {
    constexpr size_t kLength = 64 * 1024;
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "1");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "8");
    ScopedLaneHooks hooks(/*block_first_connect_handler=*/true);
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    HoldingWriteServer fake_peer;
    ASSERT_TRUE(fake_peer.ok());

    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17921", kLength);
    ASSERT_TRUE(h.ok);

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_data_port = fake_peer.port();
    desc->tcp_proto_version = 2;

    TransferRequest request;
    request.opcode = TransferRequest::WRITE;
    request.length = kLength;
    request.source = h.pool;
    request.target_id = h.segment_id;
    request.target_offset = h.remote_base;
    auto batch_id = h.engine->allocateBatchID(1);
    ASSERT_TRUE(h.engine->submitTransfer(batch_id, {request}).ok());

    ASSERT_TRUE(waitForPredicate(
        [] {
            return lane_connect_handler_entered.load(std::memory_order_acquire);
        },
        std::chrono::seconds(5)));
    EXPECT_FALSE(connecting_lane_had_current.load(std::memory_order_acquire));

    auto engine = std::move(h.engine);
    auto destruction =
        std::async(std::launch::async,
                   [engine = std::move(engine)]() mutable { engine.reset(); });

    // Let shutdown invalidate the lane epoch before the blocked resolve
    // handler returns. io_context::stop() cannot interrupt that handler, so
    // destruction must wait for this explicit release and then join it.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_EQ(destruction.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    releaseLaneConnectHandler();

    ASSERT_EQ(destruction.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    destruction.get();

    expectEverySliceCompletedExactlyOnceAfterShutdown(batch_id);
    EXPECT_EQ(lane_shutdown_clean_count.load(std::memory_order_acquire), 1);
    EXPECT_GE(late_lane_handler_count.load(std::memory_order_acquire), 1);
    EXPECT_LE(maximum_observed_socket_count.load(std::memory_order_acquire),
              1u);
    hooks.reset();
    reclaimBatchDescAfterEngineShutdownForTest(batch_id);
}

TEST(TcpWriteVisibilityTest,
     ReconnectRoundsAreRateLimitedAndCooldownQueueIsBounded) {
    constexpr int kCooldownRequestCount = 4;
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "1");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "3");
    ScopedLaneHooks hooks;
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    UnavailableTcpPeer unavailable_peer;
    ASSERT_TRUE(unavailable_peer.ok());

    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17925", 64 * 1024);
    ASSERT_TRUE(h.ok);

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_data_port = unavailable_peer.port();
    desc->tcp_proto_version = 2;

    TransferRequest request;
    request.opcode = TransferRequest::WRITE;
    request.length = 1;
    request.source = h.pool;
    request.target_id = h.segment_id;
    request.target_offset = h.remote_base;

    const auto first_batch = h.engine->allocateBatchID(1);
    ASSERT_TRUE(h.engine->submitTransfer(first_batch, {request}).ok());
    ASSERT_TRUE(waitForBatchTerminal(h.engine.get(), first_batch, 1,
                                     std::chrono::seconds(5)));
    ASSERT_TRUE(waitForPredicate(
        [] {
            return cooldown_started_count.load(std::memory_order_acquire) >= 1;
        },
        std::chrono::seconds(2)));
    EXPECT_EQ(lane_connecting_count.load(std::memory_order_acquire), 1);

    std::vector<TransferRequest> cooldown_requests(kCooldownRequestCount,
                                                   request);
    const auto cooldown_batch =
        h.engine->allocateBatchID(kCooldownRequestCount);
    ASSERT_TRUE(
        h.engine->submitTransfer(cooldown_batch, cooldown_requests).ok());
    ASSERT_TRUE(waitForPredicate(
        [] { return retry_armed_count.load(std::memory_order_acquire) == 1; },
        std::chrono::seconds(2)));

    // The first three requests are admitted during cooldown; the fourth is
    // rejected by the bounded queue. No second round starts before the timer.
    for (int task_id = 0; task_id < 3; ++task_id) {
        TransferStatus status;
        status.s = TransferStatusEnum::WAITING;
        ASSERT_TRUE(
            h.engine->getTransferStatus(cooldown_batch, task_id, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::WAITING);
    }
    TransferStatus overflow_status;
    overflow_status.s = TransferStatusEnum::WAITING;
    ASSERT_TRUE(
        h.engine->getTransferStatus(cooldown_batch, 3, overflow_status).ok());
    EXPECT_EQ(overflow_status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(queue_full_failure_count.load(std::memory_order_acquire), 1);
    EXPECT_LE(maximum_observed_queue_depth.load(std::memory_order_acquire), 3u);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(lane_connecting_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(retry_armed_count.load(std::memory_order_acquire), 1);

    ASSERT_TRUE(waitForBatchTerminal(h.engine.get(), cooldown_batch,
                                     kCooldownRequestCount,
                                     std::chrono::seconds(5)));
    EXPECT_EQ(retry_fired_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(retry_armed_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(lane_connecting_count.load(std::memory_order_acquire), 2);
    EXPECT_EQ(connect_failure_count.load(std::memory_order_acquire), 4);
    expectEverySliceCompletedExactlyOnceAfterShutdown(first_batch);
    expectEverySliceCompletedExactlyOnceAfterShutdown(cooldown_batch);

    hooks.reset();
    (void)h.engine->freeBatchID(first_batch);
    (void)h.engine->freeBatchID(cooldown_batch);
}

TEST(TcpWriteVisibilityTest,
     ShutdownWithPendingRetryTimerCompletesAcceptedWorkOnce) {
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "1");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "4");
    ScopedLaneHooks hooks;
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    UnavailableTcpPeer unavailable_peer;
    ASSERT_TRUE(unavailable_peer.ok());

    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17927", 64 * 1024);
    ASSERT_TRUE(h.ok);

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_data_port = unavailable_peer.port();
    desc->tcp_proto_version = 2;

    TransferRequest request;
    request.opcode = TransferRequest::WRITE;
    request.length = 1;
    request.source = h.pool;
    request.target_id = h.segment_id;
    request.target_offset = h.remote_base;

    const auto exhausted_batch = h.engine->allocateBatchID(1);
    ASSERT_TRUE(h.engine->submitTransfer(exhausted_batch, {request}).ok());
    ASSERT_TRUE(waitForPredicate(
        [] {
            return cooldown_started_count.load(std::memory_order_acquire) >=
                       1 &&
                   connect_failure_count.load(std::memory_order_acquire) >= 1;
        },
        std::chrono::seconds(2)));

    const auto pending_batch = h.engine->allocateBatchID(1);
    ASSERT_TRUE(h.engine->submitTransfer(pending_batch, {request}).ok());
    ASSERT_TRUE(waitForPredicate(
        [] { return retry_armed_count.load(std::memory_order_acquire) == 1; },
        std::chrono::seconds(2)));

    const auto shutdown_start = std::chrono::steady_clock::now();
    h.engine.reset();
    EXPECT_LT(std::chrono::steady_clock::now() - shutdown_start,
              std::chrono::seconds(5));

    expectEverySliceCompletedExactlyOnceAfterShutdown(exhausted_batch);
    expectEverySliceCompletedExactlyOnceAfterShutdown(pending_batch);
    EXPECT_EQ(shutdown_failure_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(retry_fired_count.load(std::memory_order_acquire), 0);
    EXPECT_EQ(lane_connecting_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(lane_shutdown_clean_count.load(std::memory_order_acquire), 1);

    hooks.reset();
    reclaimBatchDescAfterEngineShutdownForTest(exhausted_batch);
    reclaimBatchDescAfterEngineShutdownForTest(pending_batch);
}

TEST(TcpWriteVisibilityTest,
     ShutdownInvalidatesPendingRetryAndLateHandlerCannotReconnect) {
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "1");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "4");
    ScopedLaneHooks hooks(/*block_first_connect_handler=*/false,
                          /*block_after_busy=*/false,
                          /*block_retry=*/true);
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    UnavailableTcpPeer unavailable_peer;
    ASSERT_TRUE(unavailable_peer.ok());

    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17926", 64 * 1024);
    ASSERT_TRUE(h.ok);

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_data_port = unavailable_peer.port();
    desc->tcp_proto_version = 2;

    TransferRequest request;
    request.opcode = TransferRequest::WRITE;
    request.length = 1;
    request.source = h.pool;
    request.target_id = h.segment_id;
    request.target_offset = h.remote_base;

    const auto exhausted_batch = h.engine->allocateBatchID(1);
    ASSERT_TRUE(h.engine->submitTransfer(exhausted_batch, {request}).ok());
    ASSERT_TRUE(waitForPredicate(
        [] {
            return cooldown_started_count.load(std::memory_order_acquire) >=
                       1 &&
                   connect_failure_count.load(std::memory_order_acquire) >= 1;
        },
        std::chrono::seconds(2)));

    const auto pending_batch = h.engine->allocateBatchID(1);
    ASSERT_TRUE(h.engine->submitTransfer(pending_batch, {request}).ok());
    ASSERT_TRUE(waitForPredicate(
        [] { return retry_armed_count.load(std::memory_order_acquire) == 1; },
        std::chrono::seconds(2)));
    ASSERT_TRUE(waitForPredicate(
        [] { return retry_handler_entered.load(std::memory_order_acquire); },
        std::chrono::seconds(3)));

    auto engine = std::move(h.engine);
    auto destruction =
        std::async(std::launch::async,
                   [engine = std::move(engine)]() mutable { engine.reset(); });
    ASSERT_TRUE(waitForPredicate(
        [] {
            return shutdown_failure_count.load(std::memory_order_acquire) == 1;
        },
        std::chrono::seconds(2)));
    EXPECT_EQ(destruction.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);

    const int connects_before_release =
        lane_connecting_count.load(std::memory_order_acquire);
    releaseRetryHandler();
    ASSERT_EQ(destruction.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    destruction.get();

    expectEverySliceCompletedExactlyOnceAfterShutdown(exhausted_batch);
    expectEverySliceCompletedExactlyOnceAfterShutdown(pending_batch);
    EXPECT_EQ(lane_connecting_count.load(std::memory_order_acquire),
              connects_before_release);
    EXPECT_EQ(retry_fired_count.load(std::memory_order_acquire), 0);
    EXPECT_GE(retry_late_count.load(std::memory_order_acquire), 1);
    EXPECT_EQ(lane_shutdown_clean_count.load(std::memory_order_acquire), 1);

    hooks.reset();
    reclaimBatchDescAfterEngineShutdownForTest(exhausted_batch);
    reclaimBatchDescAfterEngineShutdownForTest(pending_batch);
}

TEST(TcpWriteVisibilityTest, LaneTerminalTypesAreMoveOnly) {
    EXPECT_TRUE(tcpTransportLaneTypesAreMoveOnlyForTest());
}
#endif

TEST(TcpWriteVisibilityTest, OneLaneReusesCleanSocketInFifoOrder) {
    constexpr int kRequestCount = 8;
    constexpr size_t kLength = 64 * 1024;
    constexpr size_t kPoolSize = kRequestCount * kLength;
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "1");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "16");
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    ReusingWriteServer fake_peer;
    ASSERT_TRUE(fake_peer.ok());

    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17922", kPoolSize);
    ASSERT_TRUE(h.ok);

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_data_port = fake_peer.port();
    desc->tcp_proto_version = 2;

    memset(h.pool, 0x6B, kLength);
    std::vector<TransferRequest> requests(kRequestCount);
    std::vector<uint64_t> expected_addresses;
    expected_addresses.reserve(kRequestCount);
    for (int i = 0; i < kRequestCount; ++i) {
        auto& request = requests[i];
        request.opcode = TransferRequest::WRITE;
        request.length = kLength;
        request.source = h.pool;
        request.target_id = h.segment_id;
        request.target_offset = h.remote_base + i * kLength;
        expected_addresses.push_back(request.target_offset);
    }

    auto batch_id = h.engine->allocateBatchID(kRequestCount);
    ASSERT_TRUE(h.engine->submitTransfer(batch_id, requests).ok());
    ASSERT_TRUE(
        fake_peer.waitForRequests(kRequestCount, std::chrono::seconds(10)));

    for (int i = 0; i < kRequestCount; ++i) {
        TransferStatus status;
        status.s = TransferStatusEnum::WAITING;
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (status.s == TransferStatusEnum::WAITING &&
               std::chrono::steady_clock::now() < deadline) {
            ASSERT_TRUE(h.engine->getTransferStatus(batch_id, i, status).ok());
            if (status.s == TransferStatusEnum::WAITING)
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED) << "request " << i;
    }

    EXPECT_EQ(fake_peer.acceptedCount(), 1);
    EXPECT_EQ(fake_peer.requestAddresses(), expected_addresses);
    (void)h.engine->freeBatchID(batch_id);
}

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
TEST(TcpWriteVisibilityTest,
     QueueOverflowFailsSlicesImmediatelyWithoutGivingConnectingLaneWork) {
    constexpr int kRequestCount = 4;
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "1");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "2");
    ScopedLaneHooks hooks(/*block_first_connect_handler=*/true);
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    HoldingWriteServer fake_peer;
    ASSERT_TRUE(fake_peer.ok());

    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17923", 64 * 1024);
    ASSERT_TRUE(h.ok);

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_data_port = fake_peer.port();
    desc->tcp_proto_version = 2;

    std::vector<TransferRequest> requests(kRequestCount);
    for (auto& request : requests) {
        request.opcode = TransferRequest::WRITE;
        request.length = 1;
        request.source = h.pool;
        request.target_id = h.segment_id;
        request.target_offset = h.remote_base;
    }

    const auto batch_id = h.engine->allocateBatchID(kRequestCount);
    ASSERT_TRUE(h.engine->submitTransfer(batch_id, requests).ok());
    ASSERT_TRUE(waitForPredicate(
        [] {
            return lane_connect_handler_entered.load(std::memory_order_acquire);
        },
        std::chrono::seconds(5)));

    EXPECT_EQ(queue_rejection_count.load(std::memory_order_acquire), 2);
    EXPECT_EQ(queue_full_failure_count.load(std::memory_order_acquire), 2);
    EXPECT_EQ(connect_failure_count.load(std::memory_order_acquire), 0);
    EXPECT_EQ(maximum_observed_queue_depth.load(std::memory_order_acquire), 2u);
    EXPECT_FALSE(connecting_lane_had_current.load(std::memory_order_acquire));
    for (int task_id = 2; task_id < kRequestCount; ++task_id) {
        TransferStatus status;
        status.s = TransferStatusEnum::WAITING;
        ASSERT_TRUE(
            h.engine->getTransferStatus(batch_id, task_id, status).ok());
        EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    }

    releaseLaneConnectHandler();
    ASSERT_TRUE(fake_peer.waitForAccepted(1, std::chrono::seconds(5)));
    fake_peer.closePeer();
    ASSERT_TRUE(waitForBatchTerminal(h.engine.get(), batch_id, kRequestCount,
                                     std::chrono::seconds(10)));
    hooks.reset();
    (void)h.engine->freeBatchID(batch_id);
}

TEST(TcpWriteVisibilityTest,
     ConcurrentAdmissionShutdownStressPreservesLaneOwnership) {
    constexpr size_t kSubmissionCount = 40;
    constexpr size_t kSubmitterCount = 4;
    constexpr size_t kQueueCapacity = 8;
    ScopedEnvVar lanes("MC_TCP_LANES_PER_PEER", "2");
    ScopedEnvVar queue_capacity("MC_TCP_MAX_QUEUED_TRANSFERS_PER_PEER", "8");
    ScopedLaneHooks hooks(/*block_first_connect_handler=*/false,
                          /*block_after_busy=*/true);
    const char* env = std::getenv("MC_METADATA_SERVER");
    const std::string metadata_server = env ? env : "P2PHANDSHAKE";

    HoldingWriteServer fake_peer;
    ASSERT_TRUE(fake_peer.ok());

    EngineHandle h;
    h.init(metadata_server, "127.0.0.2:17924", 64 * 1024);
    ASSERT_TRUE(h.ok);

    auto desc = h.engine->getMetadata()->getSegmentDescByID(h.segment_id);
    ASSERT_NE(desc, nullptr);
    desc->tcp_data_port = fake_peer.port();
    desc->tcp_proto_version = 2;

    TransferRequest request;
    request.opcode = TransferRequest::WRITE;
    request.length = 1;
    request.source = h.pool;
    request.target_id = h.segment_id;
    request.target_offset = h.remote_base;

    const auto busy_batch_id = h.engine->allocateBatchID(2);
    ASSERT_TRUE(
        h.engine->submitTransfer(busy_batch_id, {request, request}).ok());
    ASSERT_TRUE(waitForPredicate(
        [] {
            return lane_connect_handler_entered.load(
                       std::memory_order_acquire) &&
                   lane_busy_count.load(std::memory_order_acquire) >= 1;
        },
        std::chrono::seconds(5)));
    ASSERT_TRUE(fake_peer.waitForAccepted(1, std::chrono::seconds(5)));

    std::vector<Transport::BatchID> batch_ids(kSubmissionCount);
    for (auto& batch_id : batch_ids) batch_id = h.engine->allocateBatchID(1);
    std::vector<std::atomic<int>> submit_results(kSubmissionCount);
    for (auto& result : submit_results) result.store(0);

    std::vector<std::thread> submitters;
    for (size_t thread_id = 0; thread_id < kSubmitterCount; ++thread_id) {
        submitters.emplace_back([&, thread_id] {
            for (size_t i = thread_id; i < kSubmissionCount;
                 i += kSubmitterCount) {
                submit_results[i].store(
                    h.engine->submitTransfer(batch_ids[i], {request}).ok() ? 1
                                                                           : -1,
                    std::memory_order_release);
            }
        });
    }
    for (auto& submitter : submitters) submitter.join();
    for (const auto& result : submit_results)
        EXPECT_EQ(result.load(std::memory_order_acquire), 1);

    EXPECT_EQ(maximum_observed_queue_depth.load(std::memory_order_acquire),
              kQueueCapacity);
    EXPECT_LE(maximum_observed_socket_count.load(std::memory_order_acquire),
              2u);
    EXPECT_FALSE(connecting_lane_had_current.load(std::memory_order_acquire));
    EXPECT_EQ(queue_rejection_count.load(std::memory_order_acquire),
              static_cast<int>(kSubmissionCount - (kQueueCapacity - 1)));

    auto engine = std::move(h.engine);
    auto destruction =
        std::async(std::launch::async,
                   [engine = std::move(engine)]() mutable { engine.reset(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    releaseLaneConnectHandler();
    ASSERT_EQ(destruction.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    destruction.get();

    expectEverySliceCompletedExactlyOnceAfterShutdown(busy_batch_id);
    for (const auto batch_id : batch_ids)
        expectEverySliceCompletedExactlyOnceAfterShutdown(batch_id);
    EXPECT_EQ(lane_shutdown_clean_count.load(std::memory_order_acquire), 1);
    EXPECT_GE(late_lane_handler_count.load(std::memory_order_acquire), 1);

    hooks.reset();
    reclaimBatchDescAfterEngineShutdownForTest(busy_batch_id);
    for (const auto batch_id : batch_ids)
        reclaimBatchDescAfterEngineShutdownForTest(batch_id);
}
#endif
