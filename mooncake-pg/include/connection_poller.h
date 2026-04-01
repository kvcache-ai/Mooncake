#ifndef MOONCAKE_PG_CONNECTION_POLLER_H
#define MOONCAKE_PG_CONNECTION_POLLER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <mooncake_worker.cuh>
#include <p2p_proxy.h>
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
    static constexpr size_t CHECK_STORE_MAX_BACKOFF_MS = 1024;

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

    std::atomic<int> groupSize_;

    bool isDummy_;

    // A mark tracking the group size for which all ranks
    // in [0, establishedGroupSize_) have been successfully
    // connected at least once (they may disconnect afterwards).
    // Mainly used in `waitUntilNewRanksConnected()`.
    std::atomic<int> establishedGroupSize_;

    uint64_t* local2global_rank_map_;
    c10::intrusive_ptr<::c10d::Store> store_;

    std::shared_ptr<TransferGroupMeta> meta_;
    std::shared_ptr<P2PProxy> p2p_proxy_;
    TransferEngine* engine_;

    std::atomic<int> totalConnectedPeers_{0};
    std::atomic<bool> isShutdown_{false};

    PeerConnection peerStates_[kMaxNumRanks];

    // On MNNVL, warmup is skipped because CPU heap buffers aren't
    // fabric-accessible for cross-node NVLink writes.
    bool skip_warmup_;

    // warmup_send_region_ and warmup_recv_region_ are managed by
    // ConnectionContext. nullptr when skip_warmup_ is true.
    int32_t* warmup_send_region_;
    int32_t* warmup_recv_region_;

    std::mutex backend_wakeup_mutex_;
    std::condition_variable backend_wakeup_cv_;

   public:
    ConnectionContext(int backendIndex, int rank, int size, bool isDummy,
                      uint64_t* local2global_rank_map,
                      c10::intrusive_ptr<::c10d::Store> store,
                      std::shared_ptr<TransferGroupMeta> meta,
                      std::shared_ptr<P2PProxy> p2p_proxy,
                      TransferEngine* engine);
    ~ConnectionContext();

    int32_t* warmup_send_region() const { return warmup_send_region_; }
    int32_t* warmup_recv_region() const { return warmup_recv_region_; }

    /**
     * @brief Get the total number of actively connected peers.
     * @return The count of peers currently in the CONNECTED state.
     */
    int getTotalConnectedPeers() const;

    /**
     * @brief Expands the group to a new size.
     *
     * @note This is a non-blocking operation. Callers must invoke
     *       `waitUntilNewRanksConnected()` prior to initiating any
     *       subsequent communications (e.g., send, recv, putTaskCpu,
     *       putTaskCuda) to ensure the new peers are ready.
     *
     * @param newGroupSize The target size for the extended group.
     */
    void extendGroupSizeTo(int newGroupSize);

    /**
     * @brief Checks whether all peers within the group have
     *        established connections.
     *
     * @return True if all peers are fully connected.
     */
    bool isAllPeerConnected() const;

    /**
     * @brief Blocks until all peers in the group are connected.
     *
     * This method is primarily used during backend initialization.
     * Upon completion, it set `establishedGroupSize_` to the
     * current `groupSize_`.
     */
    void waitUntilAllConnected();

    void bootstrapLocalPeer(const std::string& localServerName,
                            const SegmentInfo& localRankInfo);

    /**
     * @brief Blocks until all newly added ranks in the
     *        extended group are connected.
     *
     * Specifically, it waits for pending ranks in the range
     * `[establishedGroupSize_, groupSize_)` to reach the connected state.
     * This should be called before starting new communications if
     * `extendGroupSizeTo()` has been invoked.
     * Upon completion, it set `establishedGroupSize_` to the
     * current `groupSize_`.
     */
    void waitUntilNewRanksConnected();

    void shutdown();

    void setDummy(bool isDummy) { isDummy_ = isDummy; }

    static std::string getServerNameStoreKey(int backendIndex, int rank) {
        return "server_name_" + std::to_string(backendIndex) + "_" +
               std::to_string(rank);
    }
    static std::string getBufferStoreKey(int backendIndex, int rank) {
        return "buffer_" + std::to_string(backendIndex) + "_" +
               std::to_string(rank);
    }
    static std::string getExtensionTaskCountStoreKey(int backendIndex,
                                                     int rank) {
        return "extension_task_count_" + std::to_string(backendIndex) + "_" +
               std::to_string(rank);
    }
    static std::string getExtensionActiveRanksStoreKey(int backendIndex,
                                                       int rank) {
        return "extension_active_ranks_" + std::to_string(backendIndex) + "_" +
               std::to_string(rank);
    }

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
    void ensureThreadStarted();
    void pollerLoop();
    bool processContext(const std::shared_ptr<ConnectionContext>& ctx);
    bool processPeer(const std::shared_ptr<ConnectionContext>& ctx,
                     int pollingRank);

    std::mutex wakeup_mutex_;
    std::condition_variable wakeup_cv_;
    std::thread pollerThread_;
    std::atomic<bool> pollerThreadStarted_{false};

    std::mutex contexts_mutex_;
    std::atomic<uint64_t> contexts_version_{0};
    std::vector<std::shared_ptr<ConnectionContext>> contexts_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_CONNECTION_POLLER_H
