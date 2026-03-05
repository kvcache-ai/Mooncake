#ifndef MOONCAKE_PG_CONNECTION_POLLER_H
#define MOONCAKE_PG_CONNECTION_POLLER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ratio>
#include <thread>
#include <vector>

#include <mooncake_worker.cuh>
#include <torch/torch.h>
#include <transfer_engine.h>

namespace mooncake {

enum class PeerConnectionState {
    WAITING_STORE,
    WAITING_WARMUP_TRANSFER,
    WAITING_PEER_WARMUP,
    CONNECTED,
    EXPIRING,
};

struct PeerConnection {
    static constexpr size_t CHECK_STORE_INITIAL_BACKOFF_MS = 8;
    static constexpr size_t CHECK_STORE_MAX_BACKOFF_MS = 4096;

    PeerConnectionState state{PeerConnectionState::WAITING_STORE};
    std::optional<BatchID> warmupBatchId{std::nullopt};
    std::optional<TransferMetadata::SegmentID> segmentId{std::nullopt};

    // Back off to avoid frequently checking store.
    std::chrono::steady_clock::time_point last_check_store;
    size_t check_store_backoff_ms{CHECK_STORE_INITIAL_BACKOFF_MS};

    void increaseCheckStoreBackoff() {
        check_store_backoff_ms =
            (std::min)(check_store_backoff_ms * 2,
                       PeerConnection::CHECK_STORE_MAX_BACKOFF_MS);
    }

    void resetCheckStoreBackoff() {
        check_store_backoff_ms = CHECK_STORE_INITIAL_BACKOFF_MS;
    }
};

class ConnectionContext {
   private:
    friend class ConnectionPoller;

    int backendIndex_;
    int rank_;

    // TODO: make it atomic and add `expandSize` to handle runtime scaling-up?
    int size_;
    uint64_t* local2global_rank_map_;
    c10::intrusive_ptr<::c10d::Store> store_;

    std::shared_ptr<TransferGroupMeta> meta_;
    TransferEngine* engine_;

    std::atomic<int> totalConnectedPeers_{0};
    std::atomic<bool> isShutdown_{false};

    PeerConnection peerStates_[kMaxNumRanks];

    // warmup_send_region_ and warmup_recv_region_ are managed by
    // ConnectionContext.
    int32_t* warmup_send_region_;
    int32_t* warmup_recv_region_;

    std::mutex backend_wakeup_mutex_;
    std::condition_variable backend_wakeup_cv_;

   public:
    ConnectionContext(int backendIndex, int rank, int size,
                      uint64_t* local2global_rank_map,
                      c10::intrusive_ptr<::c10d::Store> store,
                      std::shared_ptr<TransferGroupMeta> meta,
                      TransferEngine* engine);
    ~ConnectionContext();

    int32_t* warmup_send_region() const { return warmup_send_region_; }
    int32_t* warmup_recv_region() const { return warmup_recv_region_; }

    int getTotalConnectedPeers() const {
        return totalConnectedPeers_.load(std::memory_order_acquire);
    }
    bool isAllPeerConnected() const { return totalConnectedPeers_ == size_; }

    void waitUntilAllConnected();
    void shutdown();

   private:
    // For ConnectionManager
    bool poll();
    bool tryStop();

    // Internal helpers
    bool pollPeer(int pollingRank);
};

class ConnectionPoller {
    static constexpr size_t CONNECTING_IDLE_SLEEP_MS = 50;
    static constexpr size_t ALL_CONNECTED_IDLE_SLEEP_MS = 200;

   public:
    static ConnectionPoller& GetInstance() {
        // leaky singleton to avoid destructor fiasco problem
        static ConnectionPoller* instance = new ConnectionPoller;
        return *instance;
    }

    void registerContext(const std::shared_ptr<ConnectionContext>& ctx);
    void removeContext(const std::shared_ptr<ConnectionContext>& ctx);

    void wakeup() {
        std::lock_guard<std::mutex> lock(wakeup_mutex_);
        wakeup_cv_.notify_all();
    }
    // the global ranks
    bool global_peerConnected_[kMaxNumRanks]{};

   private:
    ConnectionPoller();
    void pollerLoop();
    bool processContext(const std::shared_ptr<ConnectionContext>& ctx);
    bool processPeer(const std::shared_ptr<ConnectionContext>& ctx,
                     int pollingRank);

    std::mutex wakeup_mutex_;
    std::condition_variable wakeup_cv_;
    std::thread pollerThread_;

    std::mutex contexts_mutex_;
    std::atomic<uint64_t> contexts_version_{0};
    std::vector<std::shared_ptr<ConnectionContext>> contexts_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONNECTION_POLLER_H
