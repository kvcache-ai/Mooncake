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
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/steady_timer.hpp>

#include "transfer_metadata.h"
#include "transport/transport.h"
#include "ylt/coro_io/coro_io.hpp"

namespace mooncake {
class TransferMetadata;
struct ClientSession;
class TcpContext;
class TcpTransport;

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

    void worker();

    void startTransfer(Slice *slice);

    bool validateAddress(uint64_t addr, uint64_t size) const;

    const char *getName() const override { return "tcp"; }

   private:
    TcpContext *context_;
    std::atomic_bool running_;
    std::thread thread_;
    bool enable_connection_pool_ = false;

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
        RUNTIME_UNAVAILABLE,
        CONNECT_FAILED,
        SESSION_FAILED,
        SHUTDOWN,
    };

    struct FailureCounters {
        std::atomic<uint64_t> queue_full{0};
        std::atomic<uint64_t> connect_failed{0};
        std::atomic<uint64_t> runtime_unavailable{0};
        std::atomic<uint64_t> session_failed{0};
        std::atomic<uint64_t> shutdown{0};
    };

    struct TcpWorkItem {
        Slice *slice = nullptr;
        bool use_v2 = false;
        uint64_t admission_sequence = 0;
        std::chrono::steady_clock::time_point enqueued_at;

        TcpWorkItem() = default;
        TcpWorkItem(Slice *slice_arg, bool use_v2_arg)
            : slice(slice_arg), use_v2(use_v2_arg) {}
        TcpWorkItem(TcpWorkItem &&other) noexcept
            : slice(other.slice),
              use_v2(other.use_v2),
              admission_sequence(other.admission_sequence),
              enqueued_at(other.enqueued_at) {
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
        explicit ConnectionLaneRuntime(asio::io_context &io_context)
            : executor(io_context.get_executor()) {}

        asio::io_context::executor_type executor;
    };

    struct PeerConnectionGroup;

    struct ConnectionLane {
        ConnectionLane(size_t lane_id_arg,
                       const std::shared_ptr<PeerConnectionGroup> &group_arg)
            : lane_id(lane_id_arg), group(group_arg) {}

        size_t lane_id;
        std::weak_ptr<PeerConnectionGroup> group;
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
                            size_t lane_count_arg, size_t queue_capacity_arg,
                            std::shared_ptr<FailureCounters> counters_arg)
            : key(std::move(key_arg)),
              executor(executor_arg),
              lane_count(lane_count_arg),
              queue_capacity(queue_capacity_arg),
              failure_counters(std::move(counters_arg)) {}

        std::mutex mutex;
        GroupState state = GroupState::OPEN;
        ConnectionKey key;
        asio::io_context::executor_type executor;
        size_t lane_count;
        size_t queue_capacity;
        std::deque<TcpWorkItem> queue;
        uint64_t queued_bytes = 0;
        bool queued_bytes_saturated = false;
        uint64_t next_admission_sequence = 1;
        std::vector<std::shared_ptr<ConnectionLane>> lanes;
        bool pump_scheduled = false;
        uint64_t pump_epoch = 0;
        uint64_t connect_round = 1;
        size_t connect_attempts_in_round = 0;
        size_t probes_in_flight = 0;
        bool connect_round_had_success = false;
        std::chrono::steady_clock::time_point next_probe_not_before{};
        std::shared_ptr<asio::steady_timer> retry_timer;
        uint64_t retry_epoch = 0;
        uint64_t connect_failure_log_count = 0;
        std::shared_ptr<FailureCounters> failure_counters;
    };

    struct ConnectionLaneState {
        std::mutex mutex;
        bool shutting_down = false;
        size_t lanes_per_peer = 4;
        size_t max_queued_transfers_per_peer = 1024;
        std::unordered_map<ConnectionKey, std::shared_ptr<PeerConnectionGroup>,
                           ConnectionKeyHash>
            groups;
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
    void enqueuePooledTransfer(const ConnectionKey &key, TcpWorkItem work);
    static uint64_t requestGroupPumpLocked(PeerConnectionGroup &group);
    static void postGroupPump(const std::shared_ptr<PeerConnectionGroup> &group,
                              uint64_t pump_epoch);
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
        Slice *slice, bool use_v2,
        std::shared_ptr<asio::ip::tcp::socket> socket) noexcept;

#ifdef MOONCAKE_TCP_TRANSPORT_TEST_HOOKS
    friend bool tcpTransportLaneTypesAreMoveOnlyForTest() noexcept;
#endif
};
}  // namespace mooncake

#endif
