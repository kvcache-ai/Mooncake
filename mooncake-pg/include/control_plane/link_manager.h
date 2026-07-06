#ifndef MOONCAKE_PG_LINK_MANAGER_H
#define MOONCAKE_PG_LINK_MANAGER_H

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
#include <vector>

#include <transfer_engine.h>

#include "types.h"

namespace mooncake {

struct TELinkEvent {
    enum class Kind { LinkUp, LinkDown };
    Kind kind = Kind::LinkDown;
    GlobalRank peer = kInvalidGlobalRank;
};

// Process-level manager for shared TE states.
class LinkManager {
   public:
    LinkManager() = default;

    void init(GlobalRank rank, int max_world_size, TransferEngine* engine);

    bool isInitialized() const {
        return initialized_.load(std::memory_order_acquire);
    }

    void shutdown();

    std::string localServerName() const;

    uint64_t getWarmupRecvAddr() const;

    void enablePeerProbe(GlobalRank peer, const std::string& server_name,
                         uint64_t warmup_recv_addr = 0);

    void disconnect(GlobalRank peer);

    void stopReconnect(GlobalRank peer);

    bool isConnected(GlobalRank peer) const;

    using EventCallback = std::function<void(TELinkEvent)>;
    void setEventCallback(EventCallback callback);

    std::optional<TransferMetadata::SegmentID> resolvePeer(
        GlobalRank peer) const;

    void refreshPeerSegment(GlobalRank peer);

    void publishLinkUp(GlobalRank peer, TransferMetadata::SegmentID target_id);
    void publishLinkDown(GlobalRank peer);

    ~LinkManager() { shutdown(); }

    LinkManager(const LinkManager&) = delete;
    LinkManager& operator=(const LinkManager&) = delete;

   private:
    bool rankInRange(GlobalRank peer) const {
        return 0 <= peer && peer < max_world_size_;
    }

    enum class PeerLinkState : uint8_t {
        Idle = 0,
        WaitingWarmupTransfer,
        WaitingPeerWarmup,
        Connected,
    };

    struct PeerLink {
        PeerLinkState state = PeerLinkState::Idle;
        std::string server_name;
        bool is_candidate = false;
        bool skip_warmup = false;

        std::optional<TransferMetadata::SegmentID> target_id;
        std::optional<BatchID> warmup_batch_id;
        uint64_t warmup_recv_addr = 0;

        // Low-frequency probe timing
        std::chrono::steady_clock::time_point next_probe_time;
        std::chrono::milliseconds probe_backoff{1000};
        static constexpr auto kProbeBackoffMin =
            std::chrono::milliseconds(1000);
        static constexpr auto kProbeBackoffMax =
            std::chrono::milliseconds(10000);
    };

    std::vector<PeerLink> peers_;
    mutable std::mutex peers_mutex_;

    struct PeerReadState {
        std::atomic<uint64_t> version{0};        // incremented on every link
                                                 // state change; resolvePeer
                                                 // uses it to detect torn reads
        std::atomic<uint8_t> link_connected{0};  // 1 = TE link is up
        std::atomic<TransferMetadata::SegmentID>
            target_id{};  // remote segment handle
    };

    std::vector<PeerReadState> read_state_;

    GlobalRank rank_ = kInvalidGlobalRank;
    int max_world_size_ = 0;
    TransferEngine* engine_ = nullptr;
    std::string local_server_name_;

    // Warmup region
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
    void tearDownPeerLink(GlobalRank peer);
    void emit(TELinkEvent event);
    void wakeup();

    static bool supportFabricMem();

    static constexpr size_t kPollerIdleSleepMs = 200;
    static constexpr size_t kPollerActiveSleepMs = 10;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_LINK_MANAGER_H
