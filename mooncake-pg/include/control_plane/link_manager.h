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

struct TELinkUpEvent {
    GlobalRank peer = kInvalidGlobalRank;
    uint64_t target_rank_epoch = 0;
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

    void enablePeerProbe(GlobalRank peer, uint64_t target_rank_epoch,
                         const std::string& server_name,
                         uint64_t warmup_recv_addr = 0);

    void disconnect(GlobalRank peer);

    void requestHealthCheck(GlobalRank peer);

    void stopReconnect(GlobalRank peer);

    bool isConnected(GlobalRank peer) const;

    using EventCallback = std::function<void(TELinkUpEvent)>;
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
        uint64_t target_rank_epoch = 0;
        std::string server_name;
        bool is_candidate = false;
        bool skip_warmup = false;

        std::optional<TransferMetadata::SegmentID> target_id;
        uint64_t warmup_recv_addr = 0;

        // Only meaningful while state == Connected; it does not prevent the
        // application from resolving or using the existing target_id.
        bool health_check_requested = false;

        std::optional<BatchID> probe_batch_id;
        std::chrono::steady_clock::time_point next_probe_time;
        static constexpr auto kProbeBackoffMin =
            std::chrono::milliseconds(1000);
        static constexpr auto kProbeBackoffMax =
            std::chrono::milliseconds(10000);
        std::chrono::milliseconds probe_backoff{kProbeBackoffMin};
    };

    std::vector<PeerLink> peers_;
    mutable std::mutex peers_mutex_;

    struct PeerReadState {
        std::atomic<uint64_t> version{0};
        std::atomic<uint8_t> link_connected{0};
        std::atomic<TransferMetadata::SegmentID> target_id{};
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
    mutable std::mutex event_callback_mutex_;

    // Poller thread
    std::thread poller_thread_;
    std::atomic<bool> poller_running_{false};
    std::mutex wakeup_mutex_;
    std::condition_variable wakeup_cv_;

    std::atomic<bool> initialized_{false};
    std::atomic<bool> shutdown_{false};

    void pollerLoop();
    bool advanceConnection(GlobalRank peer);
    bool advanceHealthCheck(GlobalRank peer);
    void tearDownPeerLink(GlobalRank peer);
    void emit(TELinkUpEvent event);
    void wakeup();

    static bool supportFabricMem();

    static constexpr size_t kPollerIdleSleepMs = 200;
    static constexpr size_t kPollerActiveSleepMs = 10;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_LINK_MANAGER_H
