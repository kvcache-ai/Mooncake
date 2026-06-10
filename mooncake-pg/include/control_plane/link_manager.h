#ifndef MOONCAKE_PG_LINK_MANAGER_H
#define MOONCAKE_PG_LINK_MANAGER_H

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include <transfer_engine.h>

#include "types.h"

namespace mooncake {

// PeerReadHandle  - lock-free handle returned by resolvePeer().
//
// Only contains L4-level info needed to construct a TransferRequest.
// Worker combines this with GroupEndpointInfo from GroupView.

struct PeerReadHandle {
    TransferMetadata::SegmentID target_id;
};

// TELinkEvent  - emitted when a physical TE link transitions up/down.

struct TELinkEvent {
    enum class Kind { LinkUp, LinkDown } kind = Kind::LinkDown;
    GlobalRank peer = kInvalidGlobalRank;
    std::optional<TransferMetadata::SegmentID> target_id;
};

// LinkManager  - process-level manager for shared TE physical links.
// Owned by MooncakeProcessContext; one instance per process.
//
// Responsibilities:
//   1. Physical link lifecycle: openSegment, warmup handshake, closeSegment
//   2. Low-frequency candidate probe for idle/inactive peers
//   3. Lock-free worker read model: resolvePeer() / isGroupReady()
//
// Thread model:
//   - Resource state (PeerLink[]) is protected by peers_mutex_.
//   - Read model (PeerReadState[]) uses atomics; lock-free for worker threads.
//   - A dedicated poller thread drives candidate probes and warmup state
//     machine. It does NOT use SerializedExecutor because L4 operations
//     (openSegment, RDMA warmup) may block.

class LinkManager {
   public:
    LinkManager() = default;

    // Initialize.  Must be called once, before any backend uses it.
    // Idempotent  - second call is a no-op.
    void init(GlobalRank rank, int max_world_size, TransferEngine* engine);

    bool isInitialized() const {
        return initialized_.load(std::memory_order_acquire);
    }

    // Shut down the poller thread and release all TE connections.
    void shutdown();

    std::string localServerName() const;

    // Return the local warmup recv region base address (0 if warmup is
    // skipped, e.g. MNNVL fabric).  Published to peers via RegisterRequest
    // so they can RDMA-write the warmup handshake signal.
    uint64_t getWarmupRecvAddr() const;

    // Mark a peer as a connection candidate and start low-frequency probe.
    // Safe to call when the peer is already a candidate or connected.
    void enablePeerProbe(GlobalRank peer, const std::string& server_name,
                         uint64_t warmup_recv_addr = 0);

    // Tear down the TE link to this peer but keep candidate=true so the
    // peer will be re-probed.  Called on LinkDown events and explicit
    // disconnect commands.
    void disconnect(GlobalRank peer);

    // Stop candidate probe entirely.  Only called when the peer transitions
    // to OFFLINE (control-plane dead).
    void stopReconnect(GlobalRank peer);

    bool isConnected(GlobalRank peer) const;

    using EventCallback = std::function<void(TELinkEvent)>;
    void setEventCallback(EventCallback callback);

    // Publish the Coordinator-authoritative RankState snapshot for all peers.
    // Called by AgentHost from the executor thread.
    void setRankStates(const std::vector<uint8_t>& states);

    // Check whether the L4 link to peer is up.  Returns a handle with the
    // remote SegmentID on success.  Does NOT check RankState or endpoint
    // (endpoint is read directly from GroupView by the worker).
    std::optional<PeerReadHandle> resolvePeer(GlobalRank peer) const;

    // Check whether the peer is ready for group operations:
    // RankState == HEALTHY && L4 link is up.
    // Used by pg.get_peer_state().
    bool isRankReady(GlobalRank peer) const;

    void publishLinkUp(GlobalRank peer, TransferMetadata::SegmentID target_id);
    void publishLinkDown(GlobalRank peer);

    ~LinkManager() { shutdown(); }

    LinkManager(const LinkManager&) = delete;
    LinkManager& operator=(const LinkManager&) = delete;

   private:
    // Resource state (mutex-protected)

    enum class PeerLinkState : uint8_t {
        IDLE = 0,
        WAITING_WARMUP_TRANSFER,
        WAITING_PEER_WARMUP,
        CONNECTED,
        EXPIRING,
    };

    struct PeerLink {
        PeerLinkState state = PeerLinkState::IDLE;
        std::string server_name;
        bool candidate = false;
        bool skip_warmup = false;  // MNNVL fabric  - warmup skipped

        std::optional<TransferMetadata::SegmentID> target_id;
        std::optional<BatchID> warmup_batch_id;
        uint64_t warmup_recv_addr =
            0;  // remote warmup recv region base address

        // Low-frequency probe timing
        std::chrono::steady_clock::time_point next_probe_time;
        std::chrono::milliseconds probe_backoff{1000};
        static constexpr auto kProbeBackoffMin =
            std::chrono::milliseconds(1000);
        static constexpr auto kProbeBackoffMax =
            std::chrono::milliseconds(10000);
    };

    std::array<PeerLink, kMaxNumRanks> peers_;
    mutable std::mutex peers_mutex_;

    // Worker read model (atomic, lock-free)

    struct PeerReadState {
        std::atomic<uint8_t> rank_state{0};      // RankState cast to uint8_t
        std::atomic<uint64_t> version{0};        // incremented on every link
                                                 // state change; resolvePeer
                                                 // uses it to detect torn reads
        std::atomic<uint8_t> link_connected{0};  // 1 = L4 link is up
        std::atomic<TransferMetadata::SegmentID>
            target_id{};  // remote segment handle
    };

    std::array<PeerReadState, kMaxNumRanks> read_state_;

    // Poller infrastructure

    GlobalRank rank_ = kInvalidGlobalRank;
    int max_world_size_ = 0;
    TransferEngine* engine_ = nullptr;
    std::string local_server_name_;

    // Warmup region  - process-level, allocated once in init().
    // nullptr when skip_warmup_ is true (MNNVL fabric).
    std::unique_ptr<int32_t[]> warmup_send_region_;
    std::unique_ptr<int32_t[]> warmup_recv_region_;
    bool skip_warmup_ = false;

    EventCallback event_callback_;
    mutable std::mutex event_callback_mutex_;  // protects event_callback_

    // Poller thread
    std::thread poller_thread_;
    std::atomic<bool> poller_running_{false};
    std::mutex wakeup_mutex_;
    std::condition_variable wakeup_cv_;

    std::atomic<bool> initialized_{false};
    std::atomic<bool> shutdown_{false};

    void pollerLoop();
    bool probePeer(GlobalRank peer);
    void tearDownPeerLink(GlobalRank peer, bool stop_reconnect);
    void emit(TELinkEvent event);
    void wakeup();

    static bool supportFabricMem();

    static constexpr size_t kPollerIdleSleepMs = 200;
    static constexpr size_t kPollerActiveSleepMs = 10;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_LINK_MANAGER_H
