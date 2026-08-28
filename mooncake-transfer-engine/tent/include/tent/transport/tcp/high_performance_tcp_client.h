// Copyright 2026 KVCache.AI
#ifndef TENT_HIGH_PERFORMANCE_TCP_CLIENT_H_
#define TENT_HIGH_PERFORMANCE_TCP_CLIENT_H_

#include <asio.hpp>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "tent/common/status.h"
#include "tent/common/types.h"
#include "tent/transport/tcp/high_performance_tcp_protocol.h"
#include "tent/transport/tcp/high_performance_tcp_workers.h"

namespace mooncake::tent {

// Process-wide client runtime for the HP TCP transport. It owns no thread of
// its own: every lane is created, queued, connected and driven exclusively on
// the worker selected by AffinityKey.
class HighPerformanceTcpClient {
   public:
    struct Config {
        uint64_t max_transfer_bytes{1ULL << 30};
        size_t chunk_size{1ULL << 20};
        uint64_t connect_timeout_ms{2000};
        uint64_t progress_timeout_ms{30000};
        size_t connections_per_peer{4};
    };

    struct Operation {
        SegmentID peer_id{0};
        std::string incarnation;
        std::string host;
        uint16_t port{0};
        uint32_t lane_id{0};
        uint64_t registration_id{0};
        uint64_t remote_addr{0};
        void* local_addr{nullptr};
        uint64_t length{0};
        HighPerformanceTcpOpcode opcode{HighPerformanceTcpOpcode::kRead};
        uint64_t request_id{0};
        // The fourth argument is true only when the endpoint failed before any
        // request byte was written. The fifth is true when a WRITE may have
        // reached the peer without a valid completion ACK; such a WRITE must
        // never be replayed through another transport.
        std::function<void(TransferStatusEnum, size_t,
                           std::optional<HighPerformanceTcpStatus>, bool, bool)>
            complete;
    };

    HighPerformanceTcpClient(Config config, HighPerformanceTcpWorkers* workers);
    ~HighPerformanceTcpClient();

    HighPerformanceTcpClient(const HighPerformanceTcpClient&) = delete;
    HighPerformanceTcpClient& operator=(const HighPerformanceTcpClient&) =
        delete;

    // Must execute on owner_worker. The transport reaches this method through
    // the owner's ASIO event queue after global admission succeeds.
    void enqueueOnOwner(size_t owner_worker, Operation operation);

    // Non-worker quiesce barrier. Cancels every queued/connecting/in-flight
    // operation and waits until all operation callbacks have retired.
    Status cancelAll(TransferStatusEnum terminal = CANCELED);

    // Best-effort cancellation for one logical request. If the request is
    // still in the transport dispatch queue, the adapter's cancel flag settles
    // it; if it already reached a lane, this posts cancellation to that owner.
    Status cancelRequest(size_t owner_worker, uint64_t request_id);

    uint64_t connectionsCreatedForTest() const {
        return connections_created_.load(std::memory_order_acquire);
    }
    uint64_t activeOperations() const {
        return active_operations_.load(std::memory_order_acquire);
    }

   private:
    struct LaneKey {
        SegmentID peer_id{0};
        std::string incarnation;
        std::string host;
        uint16_t port{0};
        uint32_t lane_id{0};

        bool operator==(const LaneKey& other) const;
    };

    struct LaneKeyHash {
        size_t operator()(const LaneKey& key) const;
    };

    class Lane;

    struct WorkerState {
        std::unordered_map<LaneKey, std::shared_ptr<Lane>, LaneKeyHash> lanes;
    };

    void cancelWorker(size_t worker_id, TransferStatusEnum terminal);
    void cancelRequestOnWorker(size_t worker_id, uint64_t request_id);
    void operationStarted();
    void operationFinished();

    Config config_;
    HighPerformanceTcpWorkers* workers_{nullptr};
    std::vector<WorkerState> worker_states_;
    std::atomic<bool> stopping_{false};

    std::atomic<uint64_t> connections_created_{0};
    std::atomic<uint64_t> active_operations_{0};
    mutable std::mutex active_mutex_;
    std::condition_variable active_cv_;
};

}  // namespace mooncake::tent

#endif  // TENT_HIGH_PERFORMANCE_TCP_CLIENT_H_
