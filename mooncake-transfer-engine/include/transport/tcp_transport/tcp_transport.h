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
#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
// Each setting injects one bad_alloc at the selected ownership boundary.
enum class TcpAllocationFailurePoint {
    NONE = 0,
    POST_GROUP_PUMP = 1,
    PUMP_HANDLER = 2,
    DISPATCH_CONNECT = 3,
    DISPATCH_SESSION = 4,
    HEADER_PROGRESS_TIMER = 5,
    HELLO_STATUS_TIMER = 6,
    WRITE_ACK_READ = 7,
};
void tcpTransportFailNextAllocationForTest(
    TcpAllocationFailurePoint point) noexcept;
bool tcpTransportAllocationFailureConsumedForTest() noexcept;
#endif

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

    bool supportsGroupedScatter() const override { return true; }

    std::string instanceId() const {
        return (running_.load(std::memory_order_acquire) ||
                retired_.load(std::memory_order_acquire)) &&
                       enable_connection_pool_
                   ? instance_id_
                   : "";
    }
    bool isRetired() const { return retired_.load(std::memory_order_acquire); }
    bool retireStrictWrites();
    bool bindTcpMasterInstance(const std::string &master_instance);
    bool reserveLegacyTcpMount();
    Status submitStrictWrite(BatchID batch_id,
                             const std::vector<TransferRequest> &entries,
                             const std::string &expected_tcp_instance);

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
    TcpContext *context_;
    std::string instance_id_;
    std::atomic<bool> retired_{false};
    std::mutex retirement_mutex_;
    bool retirement_complete_ = false;
    // Monotonic under retirement_mutex_. Unregistering memory cannot revoke
    // addresses already handed to another process by a legacy Store writer.
    bool legacy_memory_exposed_ = false;
    std::string master_instance_;
    std::atomic_bool running_;
    std::unique_ptr<TcpIoPool> io_pool_;
    size_t num_io_threads_ = 1;
    bool enable_connection_pool_ = true;

    // Client-side bounded work queues and fixed connection lanes.
    struct ConnectionKey {
        std::string host;
        uint16_t port;

        bool operator==(const ConnectionKey &other) const {
            return host == other.host && port == other.port;
        }
    };

    struct ConnectionKeyHash {
        std::size_t operator()(const ConnectionKey &key) const {
            return std::hash<std::string>()(key.host) ^
                   (std::hash<uint16_t>()(key.port) << 1);
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

    // A failure list stays allocation-free on the successful hot path. If
    // detaching more work throws, its destructor still settles every item it
    // already owns; the scheduler guard handles work left in the group.
    struct FailureQueue {
        explicit FailureQueue(
            std::shared_ptr<FailureCounters> counters_arg) noexcept
            : counters(std::move(counters_arg)) {}
        ~FailureQueue() {
            if (items)
                TcpTransport::failWorkItems(
                    *items, WorkFailureReason::RUNTIME_UNAVAILABLE, counters);
        }
        bool empty() const { return !items || items->empty(); }
        void emplace_back(TcpWorkItem &&work) {
            if (!items) items.emplace();
            items->emplace_back(std::move(work));
        }
        void swap(std::deque<TcpWorkItem> &source) {
            if (!items && source.empty()) return;
            if (!items) items.emplace();
            items->swap(source);
        }
        void fail(WorkFailureReason reason) noexcept {
            if (items) TcpTransport::failWorkItems(*items, reason, counters);
        }
        std::optional<std::deque<TcpWorkItem>> items;
        std::shared_ptr<FailureCounters> counters;
    };

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
        std::shared_ptr<asio::steady_timer> connect_timer;
        std::shared_ptr<asio::ip::tcp::socket> socket;
        std::shared_ptr<ClientSession> session;
        // Verification belongs to a socket, never merely to its host/port.
        std::weak_ptr<asio::ip::tcp::socket> verified_socket;
        std::string verified_instance;
        std::optional<TcpWorkItem> current;
    };

    struct PeerConnectionGroup {
        PeerConnectionGroup(ConnectionKey key_arg,
                            const asio::io_context::executor_type &executor_arg,
                            size_t queue_capacity_arg,
                            size_t pending_admission_capacity_arg,
                            uint64_t queued_byte_capacity_arg,
                            std::chrono::milliseconds admission_timeout_arg,
                            std::shared_ptr<FailureCounters> counters_arg)
            : key(std::move(key_arg)),
              executor(executor_arg),
              queue_capacity(queue_capacity_arg),
              pending_admission_capacity(pending_admission_capacity_arg),
              queued_byte_capacity(queued_byte_capacity_arg),
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
        // Bytes whose source buffers remain owned by the queue or pending
        // admission. Active lanes are excluded and already bounded by
        // lanes_per_peer.
        uint64_t queued_byte_capacity;
        uint64_t admitted_bytes = 0;
        std::chrono::milliseconds admission_timeout;
        std::shared_ptr<asio::steady_timer> admission_timer;
        uint64_t admission_epoch = 0;
        uint64_t queued_bytes = 0;
        bool queued_bytes_saturated = false;
        std::vector<std::shared_ptr<ConnectionLane>> lanes;
        // A scheduler fault temporarily closes admission while queued and
        // not-yet-started work is failed without allocating. Active sessions
        // keep their current work until their physical terminal callback.
        bool recovering_scheduling = false;
        bool pump_scheduled = false;
        uint64_t pump_epoch = 0;
        uint64_t connect_round = 1;
        size_t probes_in_flight = 0;
        bool connect_round_had_success = false;
        // Only peers that admit strict work acquire an application-level
        // connect bound. Mixed ordinary/strict requests share these lanes.
        bool bound_connect_deadline = false;
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
        uint64_t max_queued_bytes_per_peer = 0;
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

    // TODO(#2930): The byte bound is opt-in. Select a workload-informed
    // default after sustained slow/stalled-peer measurements.

    std::shared_ptr<asio::ip::tcp::socket> getConnection(
        const std::string &host, uint16_t port);
    void enqueuePooledTransfer(const std::string &logical_peer,
                               const ConnectionKey &key, TcpWorkItem work);
    static uint64_t requestGroupPumpLocked(PeerConnectionGroup &group);
    static void postGroupPump(const std::shared_ptr<PeerConnectionGroup> &group,
                              uint64_t pump_epoch) noexcept;
    static void recoverGroupScheduling(
        const std::shared_ptr<PeerConnectionGroup> &group) noexcept;
    static bool requestGroupRetirementLocked(PeerConnectionGroup &group);
    static void postGroupRetirement(
        const std::shared_ptr<PeerConnectionGroup> &group);
    static void scheduleGroupRetirement(
        const std::shared_ptr<PeerConnectionGroup> &group);
    static void runGroupRetirement(
        const std::shared_ptr<PeerConnectionGroup> &group);
    static void runGroupPump(const std::shared_ptr<PeerConnectionGroup> &group,
                             uint64_t pump_epoch) noexcept;
    // Called with group->mutex held; each connect epoch is armed once.
    static void armConnectDeadlineLocked(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<ConnectionLane> &lane, uint64_t epoch);
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
        const char *error) noexcept;
    static bool armRetryTimerLocked(
        const std::shared_ptr<PeerConnectionGroup> &group);
    static void handleRetryTimer(
        const std::shared_ptr<PeerConnectionGroup> &group,
        const std::shared_ptr<asio::steady_timer> &timer, uint64_t retry_epoch,
        asio::error_code ec);
    static size_t expirePendingAdmissionsLocked(
        PeerConnectionGroup &group, std::chrono::steady_clock::time_point now,
        FailureQueue &expired);
    static size_t promotePendingAdmissionsLocked(PeerConnectionGroup &group);
    static bool hasQueuedByteCapacityLocked(const PeerConnectionGroup &group,
                                            uint64_t length);
    static void addAdmittedBytesLocked(PeerConnectionGroup &group,
                                       uint64_t length);
    static void removeAdmittedBytesLocked(PeerConnectionGroup &group,
                                          uint64_t length);
    static void recomputeAdmittedBytesLocked(PeerConnectionGroup &group);
    static void refreshAdmissionTimerLocked(
        const std::shared_ptr<PeerConnectionGroup> &group,
        FailureQueue &runtime_failed,
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
        std::deque<TcpWorkItem> &work, WorkFailureReason reason,
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
