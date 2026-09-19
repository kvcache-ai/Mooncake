// Copyright 2024 KVCache.AI
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

#ifndef TCP_TRANSPORT_H_
#define TCP_TRANSPORT_H_

#include <infiniband/verbs.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <asio/dispatch.hpp>
#include <asio/executor_work_guard.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/steady_timer.hpp>
#include <glog/logging.h>
#include <pthread.h>

#include "transfer_metadata.h"
#include "transport/transport.h"
#include "ylt/coro_io/coro_io.hpp"

namespace mooncake {
class TransferMetadata;
struct ClientSession;
class TcpContext;
class TcpTransport;

// A bounded pool of asio::io_context instances, each driven by its own thread.
// TCP lanes and accepted server sockets are round-robin distributed across the
// shards so a single hot peer can use more than one CPU core. With size 1 the
// behavior is identical to the historical single-io_context transport.
class TcpIoPool {
   public:
    explicit TcpIoPool(size_t num_threads) {
        if (num_threads < 1) num_threads = 1;
        contexts_.reserve(num_threads);
        guards_.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            contexts_.push_back(std::make_unique<asio::io_context>());
            guards_.push_back(
                std::make_unique<work_guard_t>(contexts_[i]->get_executor()));
        }
    }

    ~TcpIoPool() { stop(); }

    size_t size() const { return contexts_.size(); }

    asio::io_context &context(size_t i) {
        return *contexts_[i % contexts_.size()];
    }

    asio::io_context::executor_type executor(size_t i) {
        return contexts_[i % contexts_.size()]->get_executor();
    }

    // Group coordination (pump/admission/retry/retirement timers) is pinned to
    // one shard chosen by hash, so a group's serialized state stays on a single
    // executor while its lanes spread across shards.
    asio::io_context::executor_type coordinatorExecutor(size_t hash) {
        return executor(hash % contexts_.size());
    }

    // Round-robin executor for a new lane / accepted socket.
    asio::io_context::executor_type nextLaneExecutor() {
        return executor(rr_.fetch_add(1, std::memory_order_relaxed));
    }

    size_t nextShardIndex() {
        return rr_.fetch_add(1, std::memory_order_relaxed) % contexts_.size();
    }

    // Spawns one thread per shard. before_run(i) runs on shard i's thread
    // before each io_context.run() (used to arm the acceptor on shard 0 and to
    // re-arm it after an exception-driven restart).
    void start(std::function<void(size_t)> before_run) {
        for (size_t i = 0; i < contexts_.size(); ++i) {
            threads_.emplace_back([this, i, before_run] {
#ifdef __linux__
                std::string name = "mc-tcp-io-" + std::to_string(i);
                pthread_setname_np(pthread_self(), name.c_str());
#endif
                while (!stopping_.load(std::memory_order_acquire)) {
                    try {
                        if (before_run) before_run(i);
                        contexts_[i]->run();
                        break;
                    } catch (const std::exception &e) {
                        LOG(ERROR) << "TcpIoPool shard " << i
                                   << " encountered an exception during run: "
                                   << e.what();
                        // Do not restart once stop() has published its intent:
                        // re-arming (e.g. shard 0's async_accept) after stop()
                        // stopped the context would keep run() alive and hang
                        // join().
                        if (stopping_.load(std::memory_order_acquire)) break;
                        contexts_[i]->restart();
                    }
                }
            });
        }
    }

    void stop() {
        stopping_.store(true, std::memory_order_release);
        guards_.clear();
        for (auto &c : contexts_) c->stop();
        for (auto &t : threads_)
            if (t.joinable()) t.join();
        threads_.clear();
    }

   private:
    using work_guard_t =
        asio::executor_work_guard<asio::io_context::executor_type>;
    std::vector<std::unique_ptr<asio::io_context>> contexts_;
    std::vector<std::unique_ptr<work_guard_t>> guards_;
    std::vector<std::thread> threads_;
    std::atomic<size_t> rr_{0};
    std::atomic<bool> stopping_{false};
};

class TcpTransport : public Transport {
   public:
    using BufferDesc = TransferMetadata::BufferDesc;
    using SegmentDesc = TransferMetadata::SegmentDesc;
    using HandShakeDesc = TransferMetadata::HandShakeDesc;

   public:
    TcpTransport();

    ~TcpTransport();

    Status submitTransfer(BatchID batch_id,
                          const std::vector<TransferRequest> &entries) override;

    Status submitTransferTask(
        const std::vector<TransferTask *> &task_list) override;

    Status submitTransferTaskGroup(
        const std::vector<TransferTask *> &task_list) override;

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus &status) override;

   private:
    int install(std::string &local_server_name,
                std::shared_ptr<TransferMetadata> meta,
                std::shared_ptr<Topology> topo);

    int startHandshakeDaemon();

    int allocateLocalSegmentID(int tcp_data_port);

    int registerLocalMemory(void *addr, size_t length,
                            const std::string &location, bool remote_accessible,
                            bool update_metadata);

    int unregisterLocalMemory(void *addr, bool update_metadata = false);

    int registerLocalMemoryBatch(
        const std::vector<Transport::BufferEntry> &buffer_list,
        const std::string &location);

    int unregisterLocalMemoryBatch(
        const std::vector<void *> &addr_list) override;

    Slice *prepareTransfer(TransferTask *task, const TransferRequest &request);

    void startTransfer(Slice *slice,
                       std::function<void()> continuation = nullptr,
                       bool reuse_connection = false);

    void startTransferSequence(std::vector<Slice *> slices);

    bool validateAddress(uint64_t addr, uint64_t size) const;

    const char *getName() const override { return "tcp"; }

   private:
    // Opaque identity generated once per transport lifetime and published in
    // the local TCP segment descriptor.
    std::string tcp_instance_id_;
    TcpContext *context_;
    std::atomic_bool running_;
    std::unique_ptr<TcpIoPool> io_pool_;
    size_t num_io_threads_ = 1;
    bool enable_connection_pool_ = true;

    // Client-side bounded work queues and fixed connection lanes.
    struct ConnectionKey {
        std::string host;
        uint16_t port;
        std::string tcp_instance_id;

        bool operator==(const ConnectionKey &other) const {
            return host == other.host && port == other.port &&
                   tcp_instance_id == other.tcp_instance_id;
        }
    };

    struct ConnectionKeyHash {
        std::size_t operator()(const ConnectionKey &key) const {
            const auto host_hash = std::hash<std::string>()(key.host);
            const auto port_hash = std::hash<uint16_t>()(key.port);
            const auto instance_hash =
                std::hash<std::string>()(key.tcp_instance_id);
            return host_hash ^ (port_hash << 1) ^ (instance_hash << 2);
        }
    };

    enum class WorkFailureReason {
        QUEUE_FULL,
        QUEUE_TIMEOUT,
        RUNTIME_UNAVAILABLE,
        CONNECT_FAILED,
        SESSION_FAILED,
        SHUTDOWN,
    };

    struct FailureCounters {
        std::atomic<uint64_t> queue_full{0};
        std::atomic<uint64_t> queue_timeout{0};
        std::atomic<uint64_t> connect_failed{0};
        std::atomic<uint64_t> runtime_unavailable{0};
        std::atomic<uint64_t> session_failed{0};
        std::atomic<uint64_t> shutdown{0};
    };

    struct TcpWorkItem {
        Slice *slice = nullptr;
        bool use_v2 = false;
        std::function<void()> continuation;
        std::chrono::steady_clock::time_point admission_deadline;

        TcpWorkItem() = default;
        TcpWorkItem(Slice *slice_arg, bool use_v2_arg,
                    std::function<void()> continuation_arg = nullptr)
            : slice(slice_arg),
              use_v2(use_v2_arg),
              continuation(std::move(continuation_arg)) {}
        TcpWorkItem(TcpWorkItem &&other) noexcept
            : slice(other.slice),
              use_v2(other.use_v2),
              continuation(std::move(other.continuation)),
              admission_deadline(other.admission_deadline) {
            other.slice = nullptr;
        }
        TcpWorkItem &operator=(TcpWorkItem &&) = delete;
        TcpWorkItem(const TcpWorkItem &) = delete;
        TcpWorkItem &operator=(const TcpWorkItem &) = delete;
    };

    struct TerminalAction {
        TcpWorkItem work;
        TransferStatusEnum status;
        bool connection_clean;

        TerminalAction(TcpWorkItem &&work_arg, TransferStatusEnum status_arg,
                       bool clean_arg)
            : work(std::move(work_arg)),
              status(status_arg),
              connection_clean(clean_arg) {}
        TerminalAction(TerminalAction &&) noexcept = default;
        TerminalAction &operator=(TerminalAction &&) = delete;
        TerminalAction(const TerminalAction &) = delete;
        TerminalAction &operator=(const TerminalAction &) = delete;
    };

    static_assert(!std::is_copy_constructible<TcpWorkItem>::value,
                  "TcpWorkItem must remain move-only");
    static_assert(!std::is_copy_assignable<TcpWorkItem>::value,
                  "TcpWorkItem must remain move-only");
    static_assert(!std::is_copy_constructible<TerminalAction>::value,
                  "TerminalAction must remain move-only");
    static_assert(!std::is_copy_assignable<TerminalAction>::value,
                  "TerminalAction must remain move-only");

    enum class GroupState { OPEN, CLOSING, CLOSED };
    enum class LaneState {
        DISCONNECTED,
        CONNECTING,
        IDLE,
        BUSY,
        COMPLETING,
        CLOSING,
        CLOSED,
    };
    enum class LaneConnectStage { NONE, RESOLVING, CONNECTING };

    struct ConnectionLaneRuntime {
        explicit ConnectionLaneRuntime(TcpIoPool &pool_arg) : pool(&pool_arg) {}

        asio::io_context::executor_type coordinatorExecutor(size_t hash) {
            return pool->coordinatorExecutor(hash);
        }
        asio::io_context::executor_type nextLaneExecutor() {
            return pool->nextLaneExecutor();
        }

        TcpIoPool *pool;
    };

    struct ConnectionLaneState;
    struct PeerConnectionGroup;

    struct ConnectionLane {
        ConnectionLane(size_t lane_id_arg,
                       const std::shared_ptr<PeerConnectionGroup> &group_arg,
                       const asio::io_context::executor_type &executor_arg)
            : lane_id(lane_id_arg), group(group_arg), executor(executor_arg) {}

        size_t lane_id;
        std::weak_ptr<PeerConnectionGroup> group;
        // Shard executor this lane's socket/resolver/session/timers are bound
        // to. Round-robin assigned so a group's lanes spread across io threads.
        asio::io_context::executor_type executor;
        LaneState state = LaneState::DISCONNECTED;
        LaneConnectStage connect_stage = LaneConnectStage::NONE;
        uint64_t operation_epoch = 0;
        uint64_t last_connect_round = 0;
        std::shared_ptr<asio::ip::tcp::resolver> resolver;
        std::shared_ptr<asio::ip::tcp::socket> socket;
        std::shared_ptr<ClientSession> session;
        std::optional<TcpWorkItem> current;
    };

    struct PeerConnectionGroup {
        PeerConnectionGroup(ConnectionKey key_arg,
                            const asio::io_context::executor_type &executor_arg,
                            size_t queue_capacity_arg,
                            size_t pending_admission_capacity_arg,
                            std::chrono::milliseconds admission_timeout_arg,
                            std::shared_ptr<FailureCounters> counters_arg)
            : key(std::move(key_arg)),
              executor(executor_arg),
              queue_capacity(queue_capacity_arg),
              pending_admission_capacity(pending_admission_capacity_arg),
              admission_timeout(admission_timeout_arg),
              failure_counters(std::move(counters_arg)) {}

        std::mutex mutex;
        GroupState state = GroupState::OPEN;
        ConnectionKey key;
        asio::io_context::executor_type executor;
        size_t queue_capacity;
        std::deque<TcpWorkItem> queue;
        std::deque<TcpWorkItem> pending_admissions;
        size_t pending_admission_capacity;
        std::chrono::milliseconds admission_timeout;
        std::shared_ptr<asio::steady_timer> admission_timer;
        uint64_t admission_epoch = 0;
        uint64_t queued_bytes = 0;
        bool queued_bytes_saturated = false;
        std::vector<std::shared_ptr<ConnectionLane>> lanes;
        bool pump_scheduled = false;
        uint64_t pump_epoch = 0;
        uint64_t connect_round = 1;
        size_t probes_in_flight = 0;
        bool connect_round_had_success = false;
        std::chrono::steady_clock::time_point next_probe_not_before{};
        std::shared_ptr<asio::steady_timer> retry_timer;
        uint64_t retry_epoch = 0;
        uint64_t connect_failure_log_count = 0;
        bool retiring = false;
        bool retirement_scheduled = false;
        std::weak_ptr<ConnectionLaneState> owner_state;
        std::shared_ptr<FailureCounters> failure_counters;
    };

    struct ConnectionLaneState {
        std::mutex mutex;
        bool shutting_down = false;
        size_t lanes_per_peer = 4;
        size_t max_queued_transfers_per_peer = 1024;
        size_t max_pending_admissions_per_peer = 1024;
        std::chrono::milliseconds admission_timeout{1000};
        std::unordered_map<ConnectionKey, std::shared_ptr<PeerConnectionGroup>,
                           ConnectionKeyHash>
            groups;
        std::unordered_map<std::string, ConnectionKey> current_key_by_peer;
        std::vector<std::shared_ptr<PeerConnectionGroup>> retiring_groups;
        std::weak_ptr<ConnectionLaneRuntime> runtime;
        std::shared_ptr<FailureCounters> failure_counters =
            std::make_shared<FailureCounters>();
    };

    std::shared_ptr<ConnectionLaneRuntime> lane_runtime_;
    std::shared_ptr<ConnectionLaneState> lane_state_;

    // TODO(#2930): The queue item bound does not bound queued source bytes,
    // and a connected payload without a progress deadline can still stall a
    // lane indefinitely. Peer-generation recovery is also a separate phase.

    std::shared_ptr<asio::ip::tcp::socket> getConnection(
        const std::string &host, uint16_t port);
    void enqueuePooledTransfer(const std::string &logical_peer,
                               const ConnectionKey &key, TcpWorkItem work);
    static uint64_t requestGroupPumpLocked(PeerConnectionGroup &group);
    static void postGroupPump(const std::shared_ptr<PeerConnectionGroup> &group,
                              uint64_t pump_epoch);
    static bool requestGroupRetirementLocked(PeerConnectionGroup &group);
    static void postGroupRetirement(
        const std::shared_ptr<PeerConnectionGroup> &group);
    static void scheduleGroupRetirement(
        const std::shared_ptr<PeerConnectionGroup> &group);
    static void runGroupRetirement(
        const std::shared_ptr<PeerConnectionGroup> &group);
    static void runGroupPump(const std::shared_ptr<PeerConnectionGroup> &group,
                             uint64_t pump_epoch);
    static void startLaneConnect(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<ConnectionLane> &lane, uint64_t epoch);
    static void handleLaneResolved(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<ConnectionLane> &lane, uint64_t epoch,
        asio::error_code ec, asio::ip::tcp::resolver::results_type results);
    static void handleLaneConnected(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<ConnectionLane> &lane, uint64_t epoch,
        asio::error_code ec);
    static void handleLaneConnectFailure(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<ConnectionLane> &lane, uint64_t epoch,
        const std::string &error);
    static bool armRetryTimerLocked(
        const std::shared_ptr<PeerConnectionGroup> &group);
    static void handleRetryTimer(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<asio::steady_timer> &timer, uint64_t retry_epoch,
        asio::error_code ec);
    static size_t expirePendingAdmissionsLocked(
        PeerConnectionGroup &group, std::chrono::steady_clock::time_point now,
        std::deque<TcpWorkItem> &expired);
    static size_t promotePendingAdmissionsLocked(PeerConnectionGroup &group);
    static void refreshAdmissionTimerLocked(
        const std::shared_ptr<PeerConnectionGroup> &group,
        std::deque<TcpWorkItem> &runtime_failed,
        std::shared_ptr<asio::steady_timer> &timer_to_cancel,
        bool &timer_armed);
    static void handleAdmissionTimer(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<asio::steady_timer> &timer,
        uint64_t admission_epoch, asio::error_code ec);
    static void startLaneSession(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<ConnectionLane> &lane, uint64_t epoch);
    static void handleLaneTerminal(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<ConnectionLane> &lane, uint64_t epoch,
        TransferStatusEnum status, bool connection_clean) noexcept;
    static void completeTerminalAction(TerminalAction action) noexcept;
    static void failWorkItem(
        TcpWorkItem work, WorkFailureReason reason,
        const std::shared_ptr<FailureCounters> &counters) noexcept;
    static void failWorkItems(
        std::deque<TcpWorkItem> work, WorkFailureReason reason,
        const std::shared_ptr<FailureCounters> &counters) noexcept;
    static uint64_t recordWorkFailure(
        WorkFailureReason reason,
        const std::shared_ptr<FailureCounters> &counters) noexcept;
    static bool hasUsableLaneLocked(const PeerConnectionGroup &group);
    static bool hasDisconnectedLaneLocked(const PeerConnectionGroup &group);
    static bool hasUntriedDisconnectedLaneLocked(
        const PeerConnectionGroup &group);
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    static size_t activeSocketCountLocked(const PeerConnectionGroup &group);
#endif
    static void beginConnectRoundLocked(PeerConnectionGroup &group);
    static void enterReconnectCooldownLocked(PeerConnectionGroup &group);
    static void addQueuedBytesLocked(PeerConnectionGroup &group,
                                     uint64_t length);
    static void removeQueuedBytesLocked(PeerConnectionGroup &group,
                                        uint64_t length);
    static void clearQueuedBytesLocked(PeerConnectionGroup &group);
    static void closeSocketNoThrow(
        const std::shared_ptr<asio::ip::tcp::socket> &socket) noexcept;
    void shutdownConnectionLanes();
    void startTransferWithSocket(
        TcpWorkItem work,
        std::shared_ptr<asio::ip::tcp::socket> socket) noexcept;

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    friend bool tcpTransportLaneTypesAreMoveOnlyForTest() noexcept;
#endif
};
}  // namespace mooncake

#endif
