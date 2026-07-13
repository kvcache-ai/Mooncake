#include "control_plane/link_manager.h"

#include <cstring>

#ifndef MOONCAKE_EP_USE_MUSA
#include <cuda.h>
#include <cuda_runtime.h>
#endif

#include <cuda_alike.h>
#include <transfer_engine.h>

#include "memory_location.h"
#include "pg_utils.h"

namespace mooncake {

#ifndef MOONCAKE_EP_USE_MUSA
static bool checkSupportFabricMem() {
    const char* nvlink_ipc = getenv("MC_USE_NVLINK_IPC");
    bool fabric_enabled = nvlink_ipc && strcmp(nvlink_ipc, "0") == 0;
    if (!fabric_enabled) return false;

    int num_devices = 0;
    cudaError_t err = cudaGetDeviceCount(&num_devices);
    if (err != cudaSuccess || num_devices == 0) return false;

    for (int dev = 0; dev < num_devices; ++dev) {
        int supported = 0;
        cuDeviceGetAttribute(
            &supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED, dev);
        if (!supported) return false;
    }
    return true;
}
#else
static bool checkSupportFabricMem() { return false; }
#endif

bool LinkManager::supportFabricMem() {
    static bool cached = checkSupportFabricMem();
    return cached;
}

void LinkManager::init(GlobalRank rank, int max_world_size,
                       TransferEngine* engine) {
    // If already initialized, return.  Also reject init after shutdown
    // (re-init-after-shutdown is not supported  - create a new process).
    if (initialized_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    if (shutdown_.load(std::memory_order_acquire)) {
        initialized_.store(false, std::memory_order_release);
        LOG(ERROR) << "LinkManager: init() called after shutdown(); ignoring.";
        return;
    }

    rank_ = rank;
    max_world_size_ = max_world_size;
    CHECK_GT(max_world_size_, 0);
    CHECK_LE(max_world_size_, kMaxNumRanks)
        << "max_world_size " << max_world_size_ << " exceeds kMaxNumRanks ("
        << kMaxNumRanks << ")";
    engine_ = engine;
    local_server_name_ = engine_->getLocalIpAndPort();
    skip_warmup_ = supportFabricMem();

    peers_.resize(max_world_size_);
    read_state_ = std::vector<PeerReadState>(max_world_size_);

    if (!skip_warmup_) {
        warmup_send_region_ = std::make_unique<int32_t[]>(max_world_size_);
        std::memset(warmup_send_region_.get(), 0,
                    max_world_size_ * sizeof(int32_t));
        warmup_send_region_[0] = 1;
        int rc = engine_->registerLocalMemory(warmup_send_region_.get(),
                                              max_world_size_ * sizeof(int32_t),
                                              kWildcardLocation);
        if (rc != 0) {
            LOG(FATAL) << "LinkManager: failed to register warmup send region";
        }

        warmup_recv_region_ = std::make_unique<int32_t[]>(max_world_size_);
        std::memset(warmup_recv_region_.get(), 0,
                    max_world_size_ * sizeof(int32_t));
        rc = engine_->registerLocalMemory(warmup_recv_region_.get(),
                                          max_world_size_ * sizeof(int32_t),
                                          kWildcardLocation);
        if (rc != 0) {
            LOG(FATAL)
                << "LinkManager: failed to register warmup recv region, rc="
                << rc;
        }
    }

    // Bootstrap self: open local segment, mark Connected.
    TransferMetadata::SegmentID self_target_id{};
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto& link = peers_[rank_];
        link.state = PeerLinkState::Connected;
        link.is_candidate = false;
        link.target_id = engine_->openSegment(local_server_name_);
        self_target_id = link.target_id.value();
    }

    // Write read model for self.
    publishLinkUp(rank_, self_target_id);

    // Start poller thread.
    poller_running_.store(true, std::memory_order_release);
    poller_thread_ = std::thread([this] { pollerLoop(); });
}

void LinkManager::shutdown() {
    if (shutdown_.exchange(true, std::memory_order_acq_rel)) return;

    poller_running_.store(false, std::memory_order_release);
    wakeup();
    if (poller_thread_.joinable()) {
        poller_thread_.join();
    }

    // Tear down all peer links.
    std::lock_guard<std::mutex> lock(peers_mutex_);
    for (int i = 0; i < max_world_size_; ++i) {
        if (i == rank_) continue;
        auto& link = peers_[i];
        if (link.target_id.has_value()) {
            engine_->closeSegment(link.target_id.value());
            link.target_id = std::nullopt;
        }
        if (link.warmup_batch_id.has_value()) {
            engine_->freeBatchID(link.warmup_batch_id.value());
            link.warmup_batch_id = std::nullopt;
        }
        link.state = PeerLinkState::Idle;
        link.is_candidate = false;
    }

    // Release warmup regions.
    if (warmup_send_region_) {
        engine_->unregisterLocalMemory(warmup_send_region_.get());
        warmup_send_region_.reset();
    }
    if (warmup_recv_region_) {
        engine_->unregisterLocalMemory(warmup_recv_region_.get());
        warmup_recv_region_.reset();
    }
}

std::string LinkManager::localServerName() const { return local_server_name_; }

uint64_t LinkManager::getWarmupRecvAddr() const {
    if (skip_warmup_ || !warmup_recv_region_) return 0;
    return reinterpret_cast<uint64_t>(warmup_recv_region_.get());
}

void LinkManager::enablePeerProbe(GlobalRank peer,
                                  const std::string& server_name,
                                  uint64_t warmup_recv_addr) {
    if (peer == rank_) return;
    if (!rankInRange(peer)) return;

    std::lock_guard<std::mutex> lock(peers_mutex_);
    auto& link = peers_[peer];
    link.server_name = server_name;
    link.warmup_recv_addr = warmup_recv_addr;
    link.is_candidate = true;
    link.skip_warmup = skip_warmup_;

    // Reset probe backoff so connection attempt starts promptly.
    link.probe_backoff = PeerLink::kProbeBackoffMin;
    link.next_probe_time = std::chrono::steady_clock::now();

    wakeup();
}

void LinkManager::disconnect(GlobalRank peer) {
    if (peer == rank_) return;
    if (!rankInRange(peer)) return;

    std::lock_guard<std::mutex> lock(peers_mutex_);
    tearDownPeerLink(peer);
}

void LinkManager::stopReconnect(GlobalRank peer) {
    if (peer == rank_) return;
    if (!rankInRange(peer)) return;

    std::lock_guard<std::mutex> lock(peers_mutex_);
    peers_[peer].is_candidate = false;
}

bool LinkManager::isConnected(GlobalRank peer) const {
    if (!rankInRange(peer)) return false;
    return read_state_[peer].link_connected.load(std::memory_order_acquire) !=
           0;
}

void LinkManager::setEventCallback(EventCallback callback) {
    std::lock_guard<std::mutex> lock(event_callback_mutex_);
    event_callback_ = std::move(callback);
}

std::optional<TransferMetadata::SegmentID> LinkManager::resolvePeer(
    GlobalRank peer) const {
    if (!rankInRange(peer)) return std::nullopt;

    auto& rs = read_state_[peer];

    uint64_t v1 = rs.version.load(std::memory_order_acquire);
    if (rs.link_connected.load(std::memory_order_acquire) == 0)
        return std::nullopt;
    auto target_id = rs.target_id.load(std::memory_order_acquire);
    uint64_t v2 = rs.version.load(std::memory_order_acquire);

    if (v1 != v2) return std::nullopt;

    return target_id;
}

void LinkManager::refreshPeerSegment(GlobalRank peer) {
    if (peer == rank_) return;
    if (!rankInRange(peer)) return;

    std::lock_guard<std::mutex> lock(peers_mutex_);
    auto& link = peers_[peer];

    // Only meaningful for connected peers.
    if (link.state != PeerLinkState::Connected) return;
    if (!link.target_id.has_value()) return;

    engine_->closeSegment(link.target_id.value());
    engine_->removeLocalSegment(link.server_name);
    link.target_id = engine_->openSegment(link.server_name);

    // Atomically swap in the new target_id without touching link_connected.
    // Increment the version so resolvePeer() sees the new value.
    read_state_[peer].target_id.store(link.target_id.value(),
                                      std::memory_order_relaxed);
    read_state_[peer].version.fetch_add(1, std::memory_order_release);
}

void LinkManager::publishLinkUp(GlobalRank peer,
                                TransferMetadata::SegmentID target_id) {
    if (!rankInRange(peer)) return;
    // Order: target_id before link_connected before version increment.
    // resolvePeer uses the version counter to detect torn reads.
    read_state_[peer].target_id.store(target_id, std::memory_order_relaxed);
    read_state_[peer].link_connected.store(1, std::memory_order_release);
    read_state_[peer].version.fetch_add(1, std::memory_order_release);
}

void LinkManager::publishLinkDown(GlobalRank peer) {
    if (!rankInRange(peer)) return;
    read_state_[peer].link_connected.store(0, std::memory_order_release);
    read_state_[peer].version.fetch_add(1, std::memory_order_release);
}

void LinkManager::tearDownPeerLink(GlobalRank peer) {
    auto& link = peers_[peer];

    if (link.target_id.has_value()) {
        engine_->closeSegment(link.target_id.value());
        link.target_id = std::nullopt;
    }

    if (!link.server_name.empty()) {
        engine_->removeLocalSegment(link.server_name);
    }

    if (link.warmup_batch_id.has_value()) {
        engine_->freeBatchID(link.warmup_batch_id.value());
        link.warmup_batch_id = std::nullopt;
    }

    link.state = PeerLinkState::Idle;

    publishLinkDown(peer);

    emit(TELinkEvent{.kind = TELinkEvent::Kind::LinkDown, .peer = peer});
}

void LinkManager::emit(TELinkEvent event) {
    std::lock_guard<std::mutex> lock(event_callback_mutex_);
    if (event_callback_) {
        event_callback_(std::move(event));
    }
}

void LinkManager::wakeup() { wakeup_cv_.notify_one(); }

void LinkManager::pollerLoop() {
    while (poller_running_.load(std::memory_order_acquire)) {
        bool did_work = false;

        // Find the next eligible peer to probe under a single lock.
        GlobalRank to_probe = kInvalidGlobalRank;
        {
            std::lock_guard<std::mutex> lock(peers_mutex_);
            auto now = std::chrono::steady_clock::now();

            for (int peer = 0; peer < max_world_size_; ++peer) {
                if (peer == rank_) continue;

                auto& link = peers_[peer];
                if (!link.is_candidate) continue;
                if (link.state == PeerLinkState::Connected) continue;
                if (now < link.next_probe_time) continue;

                // Schedule next probe attempt with short delay.
                link.next_probe_time = now + std::chrono::milliseconds(100);
                to_probe = peer;
                break;  // probe one per iteration
            }
        }

        if (to_probe != kInvalidGlobalRank) {
            did_work = probePeer(to_probe);
        }

        // Sleep a bit if idle, or a tiny bit if active (to avoid busy-loop).
        {
            std::unique_lock<std::mutex> lock(wakeup_mutex_);
            auto sleep_ms =
                did_work ? kPollerActiveSleepMs : kPollerIdleSleepMs;
            wakeup_cv_.wait_for(lock, std::chrono::milliseconds(sleep_ms));
        }
    }
}

bool LinkManager::probePeer(GlobalRank peer) {
    std::lock_guard<std::mutex> lock(peers_mutex_);
    auto& link = peers_[peer];

    switch (link.state) {
        case PeerLinkState::Idle: {
            if (link.server_name.empty()) return false;

            auto segment_id = engine_->openSegment(link.server_name);
            if (segment_id == static_cast<SegmentHandle>(-1)) {
                LOG(WARNING)
                    << "[LINK] openSegment failed rank=" << rank_
                    << " peer=" << peer << " server_name=" << link.server_name;
                link.probe_backoff = std::min(link.probe_backoff * 2,
                                              PeerLink::kProbeBackoffMax);
                return false;
            }

            link.target_id = segment_id;

            if (link.skip_warmup) {
                link.state = PeerLinkState::Connected;
                link.probe_backoff = PeerLink::kProbeBackoffMin;
                publishLinkUp(peer, segment_id);
                emit(TELinkEvent{
                    .kind = TELinkEvent::Kind::LinkUp,
                    .peer = peer,
                });
                return true;
            }

            // Warmup handshake based on total ordering to avoid both sides
            // initiating warmup simultaneously.
            if (peer <= rank_) {
                // We initiate: write to peer's warmup recv region.
                auto batch_id = engine_->allocateBatchID(1);
                uint64_t target_offset =
                    link.warmup_recv_addr + rank_ * sizeof(int32_t);

                engine_->submitTransfer(batch_id,
                                        {TransferRequest{
                                            .opcode = TransferRequest::WRITE,
                                            .source = warmup_send_region_.get(),
                                            .target_id = segment_id,
                                            .target_offset = target_offset,
                                            .length = sizeof(int32_t),
                                        }});
                link.warmup_batch_id = batch_id;
                link.state = PeerLinkState::WaitingWarmupTransfer;
            } else {
                // Wait for the peer to warmup us.
                link.state = PeerLinkState::WaitingPeerWarmup;
            }
            return true;
        }

        case PeerLinkState::WaitingWarmupTransfer: {
            if (!link.warmup_batch_id.has_value()) {
                link.state = PeerLinkState::Idle;
                return false;
            }

            TransferStatus status;
            engine_->getTransferStatus(link.warmup_batch_id.value(), 0, status);

            if (status.s == TransferStatusEnum::COMPLETED) {
                engine_->freeBatchID(link.warmup_batch_id.value());
                link.warmup_batch_id = std::nullopt;
                link.state = PeerLinkState::Connected;
                link.probe_backoff = PeerLink::kProbeBackoffMin;
                publishLinkUp(peer, link.target_id.value());
                emit(TELinkEvent{
                    .kind = TELinkEvent::Kind::LinkUp,
                    .peer = peer,
                });
                return true;
            }

            if (status.s == TransferStatusEnum::FAILED) {
                LOG(WARNING)
                    << "LinkManager: warmup rank " << rank_ << " -> " << peer
                    << " FAILED"
                    << " warmup_recv_addr=" << (void*)link.warmup_recv_addr
                    << " target_offset="
                    << (void*)(link.warmup_recv_addr + rank_ * sizeof(int32_t));
                engine_->freeBatchID(link.warmup_batch_id.value());
                link.warmup_batch_id = std::nullopt;
                engine_->closeSegment(link.target_id.value());
                link.target_id = std::nullopt;
                link.state = PeerLinkState::Idle;
                link.probe_backoff = std::min(link.probe_backoff * 2,
                                              PeerLink::kProbeBackoffMax);
                return false;
            }
            return false;
        }

        case PeerLinkState::WaitingPeerWarmup: {
            if (!warmup_recv_region_) {
                link.state = PeerLinkState::Idle;
                return false;
            }

            if (*reinterpret_cast<volatile int32_t*>(
                    &warmup_recv_region_[peer])) {
                link.state = PeerLinkState::Connected;
                link.probe_backoff = PeerLink::kProbeBackoffMin;
                publishLinkUp(peer, link.target_id.value());
                emit(TELinkEvent{
                    .kind = TELinkEvent::Kind::LinkUp,
                    .peer = peer,
                });
                return true;
            }
            return false;
        }

        case PeerLinkState::Connected:
            break;
    }

    return false;
}

}  // namespace mooncake
